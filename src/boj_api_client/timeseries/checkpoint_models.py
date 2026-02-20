"""Typed checkpoint state models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field

from ..core.errors import BojValidationError
from ..core.models import ApiEnvelope
from .checkpoint_codec import (
    parse_data_code_query,
    parse_data_layer_query,
    parse_envelope,
    parse_series_map,
    serialize_series_map,
)
from .checkpoint_validation import as_config_snapshot, as_int, as_int_or_none
from .queries import DataCodeQuery, DataLayerQuery
from .models import TimeSeries


@dataclass(slots=True, frozen=True)
class DataCodeCheckpointState:
    query: DataCodeQuery
    config_snapshot: dict[str, int | float | bool]
    by_code: dict[str, TimeSeries]
    last_envelope: ApiEnvelope
    chunk_index: int
    start_position: int

    def __post_init__(self) -> None:
        if self.chunk_index < 0:
            raise ValueError("chunk_index must be >= 0")
        if self.start_position < 1:
            raise ValueError("start_position must be >= 1")

    def to_record(self) -> dict[str, object]:
        return {
            "kind": "data_code",
            "query": asdict(self.query),
            "config_snapshot": dict(self.config_snapshot),
            "by_code": serialize_series_map(self.by_code),
            "last_envelope": asdict(self.last_envelope),
            "chunk_index": self.chunk_index,
            "start_position": self.start_position,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, object]) -> "DataCodeCheckpointState":
        if record.get("kind") != "data_code":
            raise BojValidationError("checkpoint kind mismatch")
        return cls(
            query=parse_data_code_query(record.get("query")),
            config_snapshot=as_config_snapshot(record.get("config_snapshot")),
            by_code=parse_series_map(record.get("by_code")),
            last_envelope=parse_envelope(record.get("last_envelope")),
            chunk_index=as_int(record.get("chunk_index"), field_name="chunk_index"),
            start_position=as_int(record.get("start_position"), field_name="start_position"),
        )


@dataclass(slots=True, frozen=True)
class DataLayerDirectCheckpointState:
    query: DataLayerQuery
    config_snapshot: dict[str, int | float | bool]
    by_code: dict[str, TimeSeries]
    last_envelope: ApiEnvelope
    start_position: int
    next_position: int | None

    def __post_init__(self) -> None:
        if self.start_position < 1:
            raise ValueError("start_position must be >= 1")

    def to_record(self) -> dict[str, object]:
        return {
            "kind": "data_layer",
            "path": "direct",
            "query": asdict(self.query),
            "config_snapshot": dict(self.config_snapshot),
            "by_code": serialize_series_map(self.by_code),
            "last_envelope": asdict(self.last_envelope),
            "start_position": self.start_position,
            "next_position": self.next_position,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, object]) -> "DataLayerDirectCheckpointState":
        if record.get("kind") != "data_layer":
            raise BojValidationError("checkpoint kind mismatch")
        if record.get("path") != "direct":
            raise BojValidationError("checkpoint path mismatch")
        return cls(
            query=parse_data_layer_query(record.get("query")),
            config_snapshot=as_config_snapshot(record.get("config_snapshot")),
            by_code=parse_series_map(record.get("by_code")),
            last_envelope=parse_envelope(record.get("last_envelope")),
            start_position=as_int(record.get("start_position"), field_name="start_position"),
            next_position=as_int_or_none(record.get("next_position"), field_name="next_position"),
        )


@dataclass(slots=True, frozen=True)
class DataLayerAutoPartitionCheckpointState:
    query: DataLayerQuery
    config_snapshot: dict[str, int | float | bool]
    selected_codes: tuple[str, ...] = field(default_factory=tuple)
    data_code_checkpoint_id: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.selected_codes, tuple):
            return
        object.__setattr__(self, "selected_codes", tuple(self.selected_codes))

    def to_record(self) -> dict[str, object]:
        return {
            "kind": "data_layer",
            "path": "auto_partition",
            "query": asdict(self.query),
            "config_snapshot": dict(self.config_snapshot),
            "selected_codes": tuple(self.selected_codes),
            "data_code_checkpoint_id": self.data_code_checkpoint_id,
        }

    @classmethod
    def from_record(
        cls,
        record: Mapping[str, object],
    ) -> "DataLayerAutoPartitionCheckpointState":
        if record.get("kind") != "data_layer":
            raise BojValidationError("checkpoint kind mismatch")
        if record.get("path") != "auto_partition":
            raise BojValidationError("checkpoint path mismatch")
        selected_codes = record.get("selected_codes")
        if not isinstance(selected_codes, (list, tuple)):
            raise BojValidationError("checkpoint selected_codes is invalid")
        checkpoint_id = record.get("data_code_checkpoint_id")
        if checkpoint_id is not None and not isinstance(checkpoint_id, str):
            raise BojValidationError("checkpoint data_code_checkpoint_id is invalid")
        return cls(
            query=parse_data_layer_query(record.get("query")),
            config_snapshot=as_config_snapshot(record.get("config_snapshot")),
            selected_codes=tuple(str(code) for code in selected_codes),
            data_code_checkpoint_id=checkpoint_id,
        )


DataLayerCheckpointState = DataLayerDirectCheckpointState | DataLayerAutoPartitionCheckpointState


__all__ = [
    "DataCodeCheckpointState",
    "DataLayerDirectCheckpointState",
    "DataLayerAutoPartitionCheckpointState",
    "DataLayerCheckpointState",
]
