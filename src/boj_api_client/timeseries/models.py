"""Timeseries domain and response models."""

from __future__ import annotations

from dataclasses import dataclass

from ..core.models import ApiEnvelope


@dataclass(slots=True, frozen=True)
class TimeSeriesPoint:
    survey_date: str
    value: int | float | None


@dataclass(slots=True, frozen=True)
class TimeSeries:
    series_code: str
    name: str | None
    unit: str | None
    frequency: str | None
    category: str | None
    last_update: str | None
    points: tuple[TimeSeriesPoint, ...] | list[TimeSeriesPoint] = ()

    def __post_init__(self) -> None:
        if isinstance(self.points, tuple):
            return
        object.__setattr__(self, "points", tuple(self.points))


@dataclass(slots=True, frozen=True)
class MetadataEntry:
    series_code: str
    name_ja: str | None
    name_en: str | None
    unit_ja: str | None
    unit_en: str | None
    frequency: str | None
    category_ja: str | None
    category_en: str | None
    layer1: str | None
    layer2: str | None
    layer3: str | None
    layer4: str | None
    layer5: str | None
    start_of_series: str | None
    end_of_series: str | None
    last_update: str | None
    notes_ja: str | None
    notes_en: str | None


@dataclass(slots=True, frozen=True)
class DataCodeResponse:
    envelope: ApiEnvelope
    series: tuple[TimeSeries, ...] | list[TimeSeries]

    def __post_init__(self) -> None:
        if isinstance(self.series, tuple):
            return
        object.__setattr__(self, "series", tuple(self.series))


@dataclass(slots=True, frozen=True)
class DataLayerResponse:
    envelope: ApiEnvelope
    series: tuple[TimeSeries, ...] | list[TimeSeries]
    next_position: int | None

    def __post_init__(self) -> None:
        if isinstance(self.series, tuple):
            return
        object.__setattr__(self, "series", tuple(self.series))


@dataclass(slots=True, frozen=True)
class MetadataResponse:
    envelope: ApiEnvelope
    entries: tuple[MetadataEntry, ...] | list[MetadataEntry]

    def __post_init__(self) -> None:
        if isinstance(self.entries, tuple):
            return
        object.__setattr__(self, "entries", tuple(self.entries))


def make_success_envelope() -> ApiEnvelope:
    return ApiEnvelope(status=200, message_id="M181000I", message="OK", date=None)


__all__ = [
    "TimeSeriesPoint",
    "TimeSeries",
    "MetadataEntry",
    "DataCodeResponse",
    "DataLayerResponse",
    "MetadataResponse",
    "make_success_envelope",
]
