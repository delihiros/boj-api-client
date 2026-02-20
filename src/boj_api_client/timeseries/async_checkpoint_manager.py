"""Async checkpoint load/save orchestration."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Mapping
from typing import TypeGuard

from ..core.async_checkpoint_store import AsyncCheckpointStore, AsyncCheckpointStoreAdapter
from ..core.checkpoint_store import CheckpointStore
from ..core.errors import BojValidationError
from .checkpoint_models import (
    DataCodeCheckpointState,
    DataLayerAutoPartitionCheckpointState,
    DataLayerCheckpointState,
    DataLayerDirectCheckpointState,
)
from .checkpoint_shared import (
    decode_validated_data_code_state,
    decode_validated_data_layer_state,
    normalize_config_snapshot,
)
from .queries import DataCodeQuery, DataLayerQuery

logger = logging.getLogger("boj_api_client")


def _is_async_method(obj: object, method_name: str) -> bool:
    method = getattr(obj, method_name, None)
    return callable(method) and inspect.iscoroutinefunction(method)


def _is_async_checkpoint_store(store: object) -> TypeGuard[AsyncCheckpointStore]:
    return (
        _is_async_method(store, "save")
        and _is_async_method(store, "load")
        and _is_async_method(store, "delete")
    )


def _is_sync_checkpoint_store(store: object) -> TypeGuard[CheckpointStore]:
    if _is_async_checkpoint_store(store):
        return False
    return (
        callable(getattr(store, "save", None))
        and callable(getattr(store, "load", None))
        and callable(getattr(store, "delete", None))
    )


class AsyncCheckpointManager:
    """Typed async checkpoint gateway for async orchestrator."""

    def __init__(
        self,
        *,
        store: CheckpointStore | AsyncCheckpointStore | None,
        config_snapshot: Mapping[str, int | float | bool] | None = None,
    ) -> None:
        self._store = self._normalize_store(store)
        self._config_snapshot = normalize_config_snapshot(config_snapshot)

    @property
    def config_snapshot(self) -> dict[str, int | float | bool]:
        return dict(self._config_snapshot)

    @property
    def enabled(self) -> bool:
        return self._store is not None

    async def save_data_code(self, state: DataCodeCheckpointState) -> str:
        return await self._require_store().save(state.to_record())

    async def save_data_layer_direct(self, state: DataLayerDirectCheckpointState) -> str:
        return await self._require_store().save(state.to_record())

    async def save_data_layer_auto_partition(
        self,
        state: DataLayerAutoPartitionCheckpointState,
    ) -> str:
        return await self._require_store().save(state.to_record())

    async def load_data_code(
        self,
        *,
        checkpoint_id: str,
        normalized: DataCodeQuery,
    ) -> DataCodeCheckpointState:
        return decode_validated_data_code_state(
            record=await self._load_record(checkpoint_id),
            normalized=normalized,
            expected_snapshot=self._config_snapshot,
        )

    async def load_data_layer(
        self,
        *,
        checkpoint_id: str,
        normalized: DataLayerQuery,
    ) -> DataLayerCheckpointState:
        return decode_validated_data_layer_state(
            record=await self._load_record(checkpoint_id),
            normalized=normalized,
            expected_snapshot=self._config_snapshot,
        )

    async def cleanup(self, checkpoint_id: str) -> None:
        if self._store is None:
            return
        try:
            await self._store.delete(checkpoint_id)
        except BojValidationError:
            logger.debug("checkpoint cleanup skipped checkpoint_id=%s", checkpoint_id)

    def _require_store(self) -> AsyncCheckpointStore:
        if self._store is None:
            raise BojValidationError("checkpoint is disabled")
        return self._store

    async def _load_record(self, checkpoint_id: str) -> dict[str, object]:
        record = await self._require_store().load(checkpoint_id)
        if not isinstance(record, dict):
            raise BojValidationError("checkpoint payload is invalid")
        return record

    @staticmethod
    def _normalize_store(
        store: CheckpointStore | AsyncCheckpointStore | None,
    ) -> AsyncCheckpointStore | None:
        if store is None:
            return None
        if _is_async_checkpoint_store(store):
            return store
        if _is_sync_checkpoint_store(store):
            return AsyncCheckpointStoreAdapter(store)
        raise BojValidationError("checkpoint store is invalid")


__all__ = [
    "AsyncCheckpointManager",
]
