from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from boj_api_client.core.models import ApiEnvelope
from boj_api_client.timeseries.models import DataCodeResponse, TimeSeries, TimeSeriesPoint
from boj_api_client.timeseries.queries import DataCodeQuery


def test_data_code_query_code_is_tuple():
    query = DataCodeQuery(db="CO", code=["A", "B"])
    assert query.code == ("A", "B")
    assert isinstance(query.code, tuple)

    replaced = query.with_codes(["X", "Y"])
    assert replaced.code == ("X", "Y")
    assert query.code == ("A", "B")


def test_time_series_points_are_tuple_and_immutable():
    series = TimeSeries(
        series_code="A",
        name="A",
        unit="u",
        frequency="Q",
        category="c",
        last_update="20250101",
        points=[TimeSeriesPoint(survey_date="202401", value=1)],
    )
    assert isinstance(series.points, tuple)
    with pytest.raises(FrozenInstanceError):
        series.points = ()  # type: ignore[misc]


def test_data_code_response_series_is_tuple_and_immutable():
    response = DataCodeResponse(
        envelope=ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None),
        series=[],
    )
    assert isinstance(response.series, tuple)
    with pytest.raises(FrozenInstanceError):
        response.series = ()  # type: ignore[misc]

