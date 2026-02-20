from __future__ import annotations

import pytest

from boj_api_client.core.errors import BojServerError, BojValidationError
from boj_api_client.core.models import ApiEnvelope
from boj_api_client.timeseries.aggregation import (
    build_data_code_response,
    build_data_layer_response_from_map,
    build_data_layer_response_from_series,
    cause_from_error,
    merge_series_map,
)
from boj_api_client.timeseries.models import TimeSeries, TimeSeriesPoint


def _series(code: str, points: list[tuple[str, int | float | None]]) -> TimeSeries:
    return TimeSeries(
        series_code=code,
        name=code,
        unit="u",
        frequency="Q",
        category="c",
        last_update="20250101",
        points=[TimeSeriesPoint(survey_date=d, value=v) for d, v in points],
    )


def test_merge_series_map_merges_points_by_survey_date():
    by_code = {"A": _series("A", [("202401", 1)])}
    merge_series_map(by_code, [_series("A", [("202402", 2)]), _series("B", [("202401", 3)])])

    assert list(by_code) == ["A", "B"]
    assert [point.survey_date for point in by_code["A"].points] == ["202401", "202402"]


def test_build_data_code_response_keeps_input_order():
    envelope = ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None)
    by_code = {"B": _series("B", []), "A": _series("A", [])}
    response = build_data_code_response(
        ordered_codes=["A", "B"],
        by_code=by_code,
        envelope=envelope,
    )
    assert [series.series_code for series in response.series] == ["A", "B"]


def test_build_data_layer_response_from_map_sorts_by_series_code():
    envelope = ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None)
    by_code = {"B": _series("B", []), "A": _series("A", [])}
    response = build_data_layer_response_from_map(
        envelope=envelope,
        by_code=by_code,
        next_position=3,
    )
    assert [series.series_code for series in response.series] == ["A", "B"]
    assert response.next_position == 3


def test_build_data_layer_response_from_series_sorts_by_series_code():
    envelope = ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None)
    response = build_data_layer_response_from_series(
        envelope=envelope,
        series=[_series("B", []), _series("A", [])],
        next_position=None,
    )
    assert [series.series_code for series in response.series] == ["A", "B"]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (BojServerError("boom", cause="server_transient"), "server_transient"),
        (BojValidationError("bad input"), "validation"),
        (RuntimeError("network down"), "network"),
    ],
)
def test_cause_from_error(error: Exception, expected: str):
    assert cause_from_error(error) == expected
