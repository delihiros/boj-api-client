"""Parsers from BOJ JSON payload into typed response objects."""

from __future__ import annotations

from collections.abc import Iterable

from ..core.errors import BojProtocolError, extract_message_id
from ..core.models import ApiEnvelope
from ..core.pagination import parse_next_position
from .models import (
    DataCodeResponse,
    DataLayerResponse,
    MetadataEntry,
    MetadataResponse,
    TimeSeries,
    TimeSeriesPoint,
)

JsonObject = dict[str, object]
_METADATA_FIELD_MAP: tuple[tuple[str, str], ...] = (
    ("name_ja", "NAME_OF_TIME_SERIES_J"),
    ("name_en", "NAME_OF_TIME_SERIES"),
    ("unit_ja", "UNIT_J"),
    ("unit_en", "UNIT"),
    ("frequency", "FREQUENCY"),
    ("category_ja", "CATEGORY_J"),
    ("category_en", "CATEGORY"),
    ("layer1", "LAYER1"),
    ("layer2", "LAYER2"),
    ("layer3", "LAYER3"),
    ("layer4", "LAYER4"),
    ("layer5", "LAYER5"),
    ("start_of_series", "START_OF_THE_TIME_SERIES"),
    ("end_of_series", "END_OF_THE_TIME_SERIES"),
    ("last_update", "LAST_UPDATE"),
    ("notes_ja", "NOTES_J"),
    ("notes_en", "NOTES"),
)


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _as_resultset(payload: JsonObject) -> list[JsonObject]:
    raw = payload.get("RESULTSET", [])
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise BojProtocolError("RESULTSET must be a list")
    for item in raw:
        if not isinstance(item, dict):
            raise BojProtocolError("RESULTSET element must be an object")
    return raw


def _parse_points(values_obj: JsonObject) -> tuple[TimeSeriesPoint, ...]:
    survey_dates = values_obj.get("SURVEY_DATES", [])
    values = values_obj.get("VALUES", [])
    if not isinstance(survey_dates, list) or not isinstance(values, list):
        raise BojProtocolError("VALUES.SURVEY_DATES and VALUES.VALUES must be lists")

    points: list[TimeSeriesPoint] = []
    for survey_date, value in _paired(survey_dates, values):
        points.append(
            TimeSeriesPoint(
                survey_date=_normalize_text(survey_date) or "",
                value=value,
            )
        )
    return tuple(points)


def _paired(left: list[object], right: list[object]) -> Iterable[tuple[object, object]]:
    limit = min(len(left), len(right))
    for idx in range(limit):
        yield left[idx], right[idx]


def _series_from_item(item: JsonObject) -> TimeSeries:
    values_obj = item.get("VALUES", {})
    if not isinstance(values_obj, dict):
        raise BojProtocolError("VALUES must be an object")

    return TimeSeries(
        series_code=_normalize_text(item.get("SERIES_CODE")) or "",
        name=_normalize_text(item.get("NAME_OF_TIME_SERIES_J"))
        or _normalize_text(item.get("NAME_OF_TIME_SERIES")),
        unit=_normalize_text(item.get("UNIT_J")) or _normalize_text(item.get("UNIT")),
        frequency=_normalize_text(item.get("FREQUENCY")),
        category=_normalize_text(item.get("CATEGORY_J")) or _normalize_text(item.get("CATEGORY")),
        last_update=_normalize_text(item.get("LAST_UPDATE")),
        points=_parse_points(values_obj),
    )


def parse_data_code_response(payload: JsonObject) -> DataCodeResponse:
    envelope = ApiEnvelope.from_payload(payload)
    message_id = extract_message_id(payload)
    if message_id == "M181030I":
        return DataCodeResponse(envelope=envelope, series=())

    series = tuple(_series_from_item(item) for item in _as_resultset(payload))
    return DataCodeResponse(envelope=envelope, series=series)


def parse_data_layer_response(payload: JsonObject) -> DataLayerResponse:
    envelope = ApiEnvelope.from_payload(payload)
    message_id = extract_message_id(payload)
    if message_id == "M181030I":
        return DataLayerResponse(
            envelope=envelope,
            series=(),
            next_position=parse_next_position(payload),
        )

    series = tuple(_series_from_item(item) for item in _as_resultset(payload))
    return DataLayerResponse(
        envelope=envelope,
        series=series,
        next_position=parse_next_position(payload),
    )


def _metadata_fields(item: JsonObject) -> dict[str, str | None]:
    return {
        field_name: _normalize_text(item.get(raw_key))
        for field_name, raw_key in _METADATA_FIELD_MAP
    }


def _metadata_from_item(item: JsonObject) -> MetadataEntry:
    fields = _metadata_fields(item)
    return MetadataEntry(
        series_code=_normalize_text(item.get("SERIES_CODE")) or "",
        name_ja=fields["name_ja"],
        name_en=fields["name_en"],
        unit_ja=fields["unit_ja"],
        unit_en=fields["unit_en"],
        frequency=fields["frequency"],
        category_ja=fields["category_ja"],
        category_en=fields["category_en"],
        layer1=fields["layer1"],
        layer2=fields["layer2"],
        layer3=fields["layer3"],
        layer4=fields["layer4"],
        layer5=fields["layer5"],
        start_of_series=fields["start_of_series"],
        end_of_series=fields["end_of_series"],
        last_update=fields["last_update"],
        notes_ja=fields["notes_ja"],
        notes_en=fields["notes_en"],
    )


def parse_metadata_response(payload: JsonObject) -> MetadataResponse:
    envelope = ApiEnvelope.from_payload(payload)
    entries = tuple(_metadata_from_item(item) for item in _as_resultset(payload))
    return MetadataResponse(envelope=envelope, entries=entries)


__all__ = [
    "parse_data_code_response",
    "parse_data_layer_response",
    "parse_metadata_response",
]

