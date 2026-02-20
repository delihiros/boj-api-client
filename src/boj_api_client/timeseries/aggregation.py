"""Result aggregation helpers for timeseries orchestration."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from ..core.errors import BojApiError, BojValidationError
from ..core.models import ApiEnvelope
from .models import DataCodeResponse, DataLayerResponse, TimeSeries


def cause_from_error(exc: Exception) -> str:
    if isinstance(exc, BojApiError) and exc.cause:
        return exc.cause
    if isinstance(exc, BojValidationError):
        return "validation"
    return "network"


def merge_series(existing: TimeSeries, incoming: TimeSeries) -> TimeSeries:
    by_date = {point.survey_date: point for point in existing.points}
    for point in incoming.points:
        by_date[point.survey_date] = point
    merged_points = tuple(sorted(by_date.values(), key=lambda p: p.survey_date))
    return TimeSeries(
        series_code=existing.series_code,
        name=incoming.name or existing.name,
        unit=incoming.unit or existing.unit,
        frequency=incoming.frequency or existing.frequency,
        category=incoming.category or existing.category,
        last_update=incoming.last_update or existing.last_update,
        points=merged_points,
    )


def merge_series_map(by_code: dict[str, TimeSeries], series_items: Iterable[TimeSeries]) -> None:
    for series in series_items:
        existing = by_code.get(series.series_code)
        by_code[series.series_code] = merge_series(existing, series) if existing else series


def sort_series_by_code(series_items: Iterable[TimeSeries]) -> tuple[TimeSeries, ...]:
    return tuple(sorted(series_items, key=lambda s: s.series_code))


def build_data_code_response(
    *,
    ordered_codes: tuple[str, ...] | list[str],
    by_code: Mapping[str, TimeSeries],
    envelope: ApiEnvelope,
) -> DataCodeResponse:
    ordered_series = tuple(by_code[code] for code in ordered_codes if code in by_code)
    return DataCodeResponse(envelope=envelope, series=ordered_series)


def build_data_layer_response_from_map(
    *,
    envelope: ApiEnvelope,
    by_code: Mapping[str, TimeSeries],
    next_position: int | None,
) -> DataLayerResponse:
    return DataLayerResponse(
        envelope=envelope,
        series=sort_series_by_code(by_code.values()),
        next_position=next_position,
    )


def build_data_layer_response_from_series(
    *,
    envelope: ApiEnvelope,
    series: Iterable[TimeSeries],
    next_position: int | None,
) -> DataLayerResponse:
    return DataLayerResponse(
        envelope=envelope,
        series=sort_series_by_code(series),
        next_position=next_position,
    )


__all__ = [
    "cause_from_error",
    "merge_series",
    "merge_series_map",
    "sort_series_by_code",
    "build_data_code_response",
    "build_data_layer_response_from_map",
    "build_data_layer_response_from_series",
]
