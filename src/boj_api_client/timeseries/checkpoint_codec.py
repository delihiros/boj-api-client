"""Codec helpers for checkpoint record payloads."""

from __future__ import annotations

from dataclasses import asdict

from ..core.errors import BojValidationError
from ..core.models import ApiEnvelope
from .models import TimeSeries, TimeSeriesPoint
from .queries import DataCodeQuery, DataLayerQuery
from .checkpoint_validation import as_int_or_none, as_str, as_str_or_none


def serialize_series_map(by_code: dict[str, TimeSeries]) -> dict[str, dict[str, object]]:
    return {code: asdict(series) for code, series in by_code.items()}


def parse_points(value: object) -> tuple[TimeSeriesPoint, ...]:
    if not isinstance(value, (list, tuple)):
        raise BojValidationError("checkpoint points is invalid")
    points: list[TimeSeriesPoint] = []
    for item in value:
        if not isinstance(item, dict):
            raise BojValidationError("checkpoint points is invalid")
        raw_survey_date = item.get("survey_date")
        if raw_survey_date is None:
            raise BojValidationError("checkpoint points is invalid")
        survey_date = as_str_or_none(raw_survey_date)
        if survey_date is None:
            raise BojValidationError("checkpoint points is invalid")
        raw_value = item.get("value")
        if raw_value is not None and not isinstance(raw_value, (int, float)):
            raise BojValidationError("checkpoint points is invalid")
        points.append(TimeSeriesPoint(survey_date=survey_date, value=raw_value))
    return tuple(points)


def parse_series(value: object) -> TimeSeries:
    if isinstance(value, TimeSeries):
        return value
    if not isinstance(value, dict):
        raise BojValidationError("checkpoint series is invalid")
    return TimeSeries(
        series_code=as_str(value.get("series_code"), field_name="series_code"),
        name=as_str_or_none(value.get("name")),
        unit=as_str_or_none(value.get("unit")),
        frequency=as_str_or_none(value.get("frequency")),
        category=as_str_or_none(value.get("category")),
        last_update=as_str_or_none(value.get("last_update")),
        points=parse_points(value.get("points")),
    )


def parse_series_map(value: object) -> dict[str, TimeSeries]:
    if not isinstance(value, dict):
        raise BojValidationError("checkpoint by_code is invalid")
    parsed: dict[str, TimeSeries] = {}
    for code, raw_series in value.items():
        parsed[as_str(code, field_name="series_code")] = parse_series(raw_series)
    return parsed


def parse_envelope(value: object) -> ApiEnvelope:
    if isinstance(value, ApiEnvelope):
        return value
    if not isinstance(value, dict):
        raise BojValidationError("checkpoint envelope is invalid")
    return ApiEnvelope(
        status=as_int_or_none(value.get("status"), field_name="status"),
        message_id=as_str_or_none(value.get("message_id")),
        message=as_str_or_none(value.get("message")),
        date=as_str_or_none(value.get("date")),
    )


def parse_data_code_query(value: object) -> DataCodeQuery:
    if isinstance(value, DataCodeQuery):
        return value
    if not isinstance(value, dict):
        raise BojValidationError("checkpoint query is invalid")
    raw_codes = value.get("code")
    if not isinstance(raw_codes, (list, tuple)):
        raise BojValidationError("checkpoint query is invalid")
    return DataCodeQuery(
        db=as_str(value.get("db"), field_name="db"),
        code=tuple(str(code) for code in raw_codes),
        lang=as_str_or_none(value.get("lang")) or "JP",
        start_date=as_str_or_none(value.get("start_date")),
        end_date=as_str_or_none(value.get("end_date")),
        start_position=as_int_or_none(value.get("start_position"), field_name="start_position"),
    )


def parse_data_layer_query(value: object) -> DataLayerQuery:
    if isinstance(value, DataLayerQuery):
        return value
    if not isinstance(value, dict):
        raise BojValidationError("checkpoint query is invalid")
    return DataLayerQuery(
        db=as_str(value.get("db"), field_name="db"),
        frequency=as_str(value.get("frequency"), field_name="frequency"),
        layer1=as_str(value.get("layer1"), field_name="layer1"),
        lang=as_str_or_none(value.get("lang")) or "JP",
        layer2=as_str_or_none(value.get("layer2")),
        layer3=as_str_or_none(value.get("layer3")),
        layer4=as_str_or_none(value.get("layer4")),
        layer5=as_str_or_none(value.get("layer5")),
        start_date=as_str_or_none(value.get("start_date")),
        end_date=as_str_or_none(value.get("end_date")),
        start_position=as_int_or_none(value.get("start_position"), field_name="start_position"),
    )


__all__ = [
    "parse_data_code_query",
    "parse_data_layer_query",
    "parse_envelope",
    "parse_series_map",
    "serialize_series_map",
]
