"""Async checkpoint store adapter."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol, TypeVar

from .checkpoint_store import CheckpointStore

T = TypeVar("T")


class AsyncCheckpointStore(Protocol):
    """Async checkpoint persistence contract."""

    async def save(self, state: Mapping[str, Any]) -> str:
        """Persist state and return checkpoint_id."""

    async def load(self, checkpoint_id: str) -> dict[str, Any]:
        """Load previously saved state."""

    async def delete(self, checkpoint_id: str) -> None:
        """Delete a checkpoint."""


class AsyncCheckpointStoreAdapter:
    """Wrap a sync store and execute operations in worker threads."""

    def __init__(
        self,
        store: CheckpointStore,
        *,
        run_sync: Callable[..., Awaitable[T]] | None = None,
    ) -> None:
        self._store = store
        self._run_sync: Callable[..., Awaitable[T]] = run_sync or _default_run_sync

    async def save(self, state: Mapping[str, Any]) -> str:
        return await self._run_sync(self._store.save, state)

    async def load(self, checkpoint_id: str) -> dict[str, Any]:
        return await self._run_sync(self._store.load, checkpoint_id)

    async def delete(self, checkpoint_id: str) -> None:
        await self._run_sync(self._store.delete, checkpoint_id)


async def _default_run_sync(func: Callable[..., T], *args: Any) -> T:
    return await asyncio.to_thread(func, *args)


__all__ = [
    "AsyncCheckpointStore",
    "AsyncCheckpointStoreAdapter",
]
