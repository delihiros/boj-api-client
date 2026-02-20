"""Checkpoint load/save orchestration."""

from __future__ import annotations

import logging
from collections.abc import Mapping

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


class CheckpointManager:
    """Typed checkpoint gateway for orchestrators."""

    def __init__(
        self,
        *,
        store: CheckpointStore | None,
        config_snapshot: Mapping[str, int | float | bool] | None = None,
    ) -> None:
        self._store = store
        self._config_snapshot = normalize_config_snapshot(config_snapshot)

    @property
    def config_snapshot(self) -> dict[str, int | float | bool]:
        return dict(self._config_snapshot)

    @property
    def enabled(self) -> bool:
        return self._store is not None

    def save_data_code(self, state: DataCodeCheckpointState) -> str:
        return self._require_store().save(state.to_record())

    def save_data_layer_direct(self, state: DataLayerDirectCheckpointState) -> str:
        return self._require_store().save(state.to_record())

    def save_data_layer_auto_partition(
        self,
        state: DataLayerAutoPartitionCheckpointState,
    ) -> str:
        return self._require_store().save(state.to_record())

    def load_data_code(
        self,
        *,
        checkpoint_id: str,
        normalized: DataCodeQuery,
    ) -> DataCodeCheckpointState:
        return decode_validated_data_code_state(
            record=self._load_record(checkpoint_id),
            normalized=normalized,
            expected_snapshot=self._config_snapshot,
        )

    def load_data_layer(
        self,
        *,
        checkpoint_id: str,
        normalized: DataLayerQuery,
    ) -> DataLayerCheckpointState:
        return decode_validated_data_layer_state(
            record=self._load_record(checkpoint_id),
            normalized=normalized,
            expected_snapshot=self._config_snapshot,
        )

    def cleanup(self, checkpoint_id: str) -> None:
        if self._store is None:
            return
        try:
            self._store.delete(checkpoint_id)
        except BojValidationError:
            logger.debug("checkpoint cleanup skipped checkpoint_id=%s", checkpoint_id)

    def _require_store(self) -> CheckpointStore:
        if self._store is None:
            raise BojValidationError("checkpoint is disabled")
        return self._store

    def _load_record(self, checkpoint_id: str) -> dict[str, object]:
        record = self._require_store().load(checkpoint_id)
        if not isinstance(record, dict):
            raise BojValidationError("checkpoint payload is invalid")
        return record


__all__ = [
    "CheckpointManager",
]
