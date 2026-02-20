"""Checkpoint storage abstraction and in-memory implementation."""

from __future__ import annotations

import logging
import os
import pickle
import re
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .errors import BojValidationError

DEFAULT_CHECKPOINT_TTL_SECONDS = 24 * 60 * 60
_CHECKPOINT_ID_RE = re.compile(r"^[0-9a-f]{32}$")
logger = logging.getLogger("boj_api_client")


def validate_checkpoint_id(checkpoint_id: str) -> str:
    """Validate checkpoint id format."""

    if not isinstance(checkpoint_id, str) or _CHECKPOINT_ID_RE.fullmatch(checkpoint_id) is None:
        raise BojValidationError("checkpoint_id is invalid")
    return checkpoint_id


class CheckpointStore(Protocol):
    """Abstract checkpoint persistence contract."""

    def save(self, state: Mapping[str, Any]) -> str:
        """Persist state and return checkpoint_id."""

    def load(self, checkpoint_id: str) -> dict[str, Any]:
        """Load previously saved state."""

    def delete(self, checkpoint_id: str) -> None:
        """Delete a checkpoint."""


@dataclass(slots=True)
class _StoredCheckpoint:
    expires_at: float
    state: dict[str, Any]


class _CheckpointStoreBase(ABC):
    """Common save/load/delete flow shared by concrete checkpoint stores."""

    def __init__(
        self,
        *,
        ttl_seconds: float = DEFAULT_CHECKPOINT_TTL_SECONDS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self._ttl_seconds = ttl_seconds
        self._clock = clock or time.time
        self._lock = threading.RLock()

    def save(self, state: Mapping[str, Any]) -> str:
        now = self._clock()
        checkpoint_id = uuid.uuid4().hex
        stored = _StoredCheckpoint(
            expires_at=now + self._ttl_seconds,
            state=deepcopy(dict(state)),
        )
        with self._lock:
            self._purge_expired_locked(now)
            self._write_checkpoint_locked(checkpoint_id, stored)
        return checkpoint_id

    def load(self, checkpoint_id: str) -> dict[str, Any]:
        validate_checkpoint_id(checkpoint_id)
        now = self._clock()
        with self._lock:
            stored = self._read_checkpoint_locked(checkpoint_id)
            if stored is None:
                self._purge_expired_locked(now)
                raise BojValidationError("checkpoint_id not found")
            if stored.expires_at <= now:
                self._delete_checkpoint_locked(checkpoint_id)
                raise BojValidationError("checkpoint_id expired")
            self._purge_expired_locked(now, skip_id=checkpoint_id)
            return deepcopy(stored.state)

    def delete(self, checkpoint_id: str) -> None:
        validate_checkpoint_id(checkpoint_id)
        now = self._clock()
        with self._lock:
            self._purge_expired_locked(now)
            if not self._delete_checkpoint_locked(checkpoint_id):
                raise BojValidationError("checkpoint_id not found")

    @abstractmethod
    def _write_checkpoint_locked(self, checkpoint_id: str, stored: _StoredCheckpoint) -> None: ...

    @abstractmethod
    def _read_checkpoint_locked(self, checkpoint_id: str) -> _StoredCheckpoint | None: ...

    @abstractmethod
    def _delete_checkpoint_locked(self, checkpoint_id: str) -> bool: ...

    @abstractmethod
    def _purge_expired_locked(self, now: float, *, skip_id: str | None = None) -> None: ...


class MemoryCheckpointStore(_CheckpointStoreBase):
    """Process-local checkpoint store."""

    def __init__(
        self,
        *,
        ttl_seconds: float = DEFAULT_CHECKPOINT_TTL_SECONDS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        super().__init__(ttl_seconds=ttl_seconds, clock=clock)
        self._items: dict[str, _StoredCheckpoint] = {}

    def _write_checkpoint_locked(self, checkpoint_id: str, stored: _StoredCheckpoint) -> None:
        self._items[checkpoint_id] = stored

    def _read_checkpoint_locked(self, checkpoint_id: str) -> _StoredCheckpoint | None:
        return self._items.get(checkpoint_id)

    def _delete_checkpoint_locked(self, checkpoint_id: str) -> bool:
        if checkpoint_id not in self._items:
            return False
        del self._items[checkpoint_id]
        return True

    def _purge_expired_locked(self, now: float, *, skip_id: str | None = None) -> None:
        expired = [
            key
            for key, item in self._items.items()
            if key != skip_id and item.expires_at <= now
        ]
        for key in expired:
            del self._items[key]


class FileCheckpointStore(_CheckpointStoreBase):
    """Filesystem-backed checkpoint store with TTL and lazy GC."""

    def __init__(
        self,
        *,
        base_dir: str | Path,
        ttl_seconds: float = DEFAULT_CHECKPOINT_TTL_SECONDS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        super().__init__(ttl_seconds=ttl_seconds, clock=clock)
        self._base_dir = Path(base_dir).resolve()
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def _write_checkpoint_locked(self, checkpoint_id: str, stored: _StoredCheckpoint) -> None:
        self._write_atomic(self._path_for(checkpoint_id), stored)

    def _read_checkpoint_locked(self, checkpoint_id: str) -> _StoredCheckpoint | None:
        return self._read(self._path_for(checkpoint_id))

    def _delete_checkpoint_locked(self, checkpoint_id: str) -> bool:
        path = self._path_for(checkpoint_id)
        if not path.exists():
            return False
        self._unlink(path)
        return True

    def _path_for(self, checkpoint_id: str) -> Path:
        validate_checkpoint_id(checkpoint_id)
        resolved = (self._base_dir / f"{checkpoint_id}.pkl").resolve()
        if resolved.parent != self._base_dir:
            raise BojValidationError("checkpoint_id is invalid")
        return resolved

    def _write_atomic(self, path: Path, stored: _StoredCheckpoint) -> None:
        tmp_path = path.with_suffix(".tmp")
        with tmp_path.open("wb") as file_obj:
            pickle.dump(stored, file_obj, protocol=pickle.HIGHEST_PROTOCOL)
            file_obj.flush()
            os.fsync(file_obj.fileno())
        os.replace(tmp_path, path)

    def _read(self, path: Path) -> _StoredCheckpoint | None:
        if not path.exists():
            return None
        try:
            with path.open("rb") as file_obj:
                loaded = pickle.load(file_obj)
        except (pickle.PickleError, OSError, EOFError):
            logger.warning("corrupt checkpoint removed path=%s", path)
            self._unlink(path)
            return None
        if not isinstance(loaded, _StoredCheckpoint):
            logger.warning("invalid checkpoint payload removed path=%s", path)
            self._unlink(path)
            return None
        return loaded

    def _purge_expired_locked(self, now: float, *, skip_id: str | None = None) -> None:
        for path in self._base_dir.glob("*.pkl"):
            checkpoint_id = path.stem
            if skip_id is not None and checkpoint_id == skip_id:
                continue
            stored = self._read(path)
            if stored is None:
                continue
            if stored.expires_at <= now:
                self._unlink(path)

    def _unlink(self, path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            return


__all__ = [
    "DEFAULT_CHECKPOINT_TTL_SECONDS",
    "validate_checkpoint_id",
    "CheckpointStore",
    "MemoryCheckpointStore",
    "FileCheckpointStore",
]

