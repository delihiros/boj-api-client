"""Shared checkpoint manager utilities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict

from ..core.errors import BojValidationError
from .checkpoint_models import (
    DataCodeCheckpointState,
    DataLayerAutoPartitionCheckpointState,
    DataLayerCheckpointState,
    DataLayerDirectCheckpointState,
)
from .queries import DataCodeQuery, DataLayerQuery

QueryType = DataCodeQuery | DataLayerQuery


def normalize_config_snapshot(
    config_snapshot: Mapping[str, int | float | bool] | None,
) -> dict[str, int | float | bool]:
    return dict(config_snapshot) if config_snapshot is not None else {}


def validate_query_match(
    *,
    saved_query: QueryType,
    normalized: QueryType,
) -> None:
    if asdict(saved_query) != asdict(normalized):
        raise BojValidationError("checkpoint query mismatch")


def validate_config_snapshot_match(
    *,
    saved_snapshot: Mapping[str, int | float | bool],
    expected_snapshot: Mapping[str, int | float | bool],
) -> None:
    if dict(saved_snapshot) != dict(expected_snapshot):
        raise BojValidationError("checkpoint config mismatch")


def decode_data_layer_record(record: dict[str, object]) -> DataLayerCheckpointState:
    if record.get("kind") != "data_layer":
        raise BojValidationError("checkpoint kind mismatch")
    path = record.get("path")
    if path == "direct":
        return DataLayerDirectCheckpointState.from_record(record)
    if path == "auto_partition":
        return DataLayerAutoPartitionCheckpointState.from_record(record)
    raise BojValidationError("checkpoint path mismatch")


def decode_validated_data_code_state(
    *,
    record: dict[str, object],
    normalized: DataCodeQuery,
    expected_snapshot: Mapping[str, int | float | bool],
) -> DataCodeCheckpointState:
    state = DataCodeCheckpointState.from_record(record)
    validate_query_match(saved_query=state.query, normalized=normalized)
    validate_config_snapshot_match(
        saved_snapshot=state.config_snapshot,
        expected_snapshot=expected_snapshot,
    )
    return state


def decode_validated_data_layer_state(
    *,
    record: dict[str, object],
    normalized: DataLayerQuery,
    expected_snapshot: Mapping[str, int | float | bool],
) -> DataLayerCheckpointState:
    state = decode_data_layer_record(record)
    validate_query_match(saved_query=state.query, normalized=normalized)
    validate_config_snapshot_match(
        saved_snapshot=state.config_snapshot,
        expected_snapshot=expected_snapshot,
    )
    return state


__all__ = [
    "QueryType",
    "normalize_config_snapshot",
    "validate_query_match",
    "validate_config_snapshot_match",
    "decode_data_layer_record",
    "decode_validated_data_code_state",
    "decode_validated_data_layer_state",
]
