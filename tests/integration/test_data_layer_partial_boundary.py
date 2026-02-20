from __future__ import annotations

import copy

import pytest

from boj_api_client.core.errors import BojPartialResultError, BojServerError
from boj_api_client.timeseries.strict import StrictTimeSeriesService
from boj_api_client.timeseries.queries import DataLayerQuery
from boj_api_client.timeseries.orchestrator import TimeSeriesService


class _FailOnSecondLayerPageTransport:
    def __init__(self, fixture_loader):
        self._load = fixture_loader

    def request(self, endpoint: str, *, params: dict):
        if endpoint != "/getDataLayer":
            raise AssertionError(f"Unexpected endpoint: {endpoint}")

        start_position = str(params.get("startPosition", "1"))
        if start_position == "1":
            return copy.deepcopy(self._load("get_data_layer_page1.json"))
        if start_position == "255":
            raise BojServerError("transient", status=500, cause="server_transient")
        raise AssertionError(f"Unexpected startPosition: {start_position}")


def test_data_layer_partial_result_boundary_when_second_page_fails(fixture_loader):
    transport = _FailOnSecondLayerPageTransport(fixture_loader)
    service = TimeSeriesService(StrictTimeSeriesService(transport))

    with pytest.raises(BojPartialResultError) as exc:
        service.get_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="*", lang="EN"))

    partial = exc.value.partial_result
    first_page = fixture_loader("get_data_layer_page1.json")
    expected_codes = {item["SERIES_CODE"] for item in first_page["RESULTSET"]}

    assert exc.value.cause == "server_transient"
    assert partial.next_position == 255
    assert {series.series_code for series in partial.series} == expected_codes
