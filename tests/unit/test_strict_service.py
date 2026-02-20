from __future__ import annotations

import pytest

from boj_api_client.core.errors import BojValidationError
from boj_api_client.timeseries.strict import StrictTimeSeriesService
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery


class _SpyTransport:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def request(self, endpoint: str, *, params: dict):
        self.calls.append((endpoint, params))
        return {"STATUS": 200, "MESSAGEID": "M181000I", "RESULTSET": [], "NEXTPOSITION": ""}


def test_strict_data_code_raises_when_too_many_codes():
    strict = StrictTimeSeriesService(_SpyTransport())
    query = DataCodeQuery(db="CO", code=[f"C{i}" for i in range(251)])
    with pytest.raises(BojValidationError):
        strict.execute_data_code(query, code_subset=query.code)


def test_strict_builds_expected_params_for_layer():
    transport = _SpyTransport()
    strict = StrictTimeSeriesService(transport)
    query = DataLayerQuery(db="MD10", frequency="Q", layer1="*", lang="EN")
    strict.execute_data_layer(query, start_position=1)
    endpoint, params = transport.calls[0]
    assert endpoint == "/getDataLayer"
    assert params["format"] == "json"
    assert params["db"] == "MD10"
    assert params["frequency"] == "Q"
    assert "startPosition" not in params


def test_strict_layer_param_is_compact_and_contiguous():
    transport = _SpyTransport()
    strict = StrictTimeSeriesService(transport)
    query = DataLayerQuery(db="MD10", frequency="Q", layer1="*", layer2="A", layer3="B")
    strict.execute_data_layer(query, start_position=255)
    _, params = transport.calls[0]
    assert params["layer"] == "*,A,B"
    assert params["startPosition"] == "255"


def test_strict_rejects_non_contiguous_layer_query():
    strict = StrictTimeSeriesService(_SpyTransport())
    query = DataLayerQuery(db="MD10", frequency="Q", layer1="*", layer2="A", layer4="B")
    with pytest.raises(BojValidationError):
        strict.execute_data_layer(query)


def test_strict_omits_start_position_when_first_page_data_code():
    transport = _SpyTransport()
    strict = StrictTimeSeriesService(transport)
    query = DataCodeQuery(db="CO", code=["A"])
    strict.execute_data_code(query, code_subset=["A"], start_position=1)
    _, params = transport.calls[0]
    assert "startPosition" not in params


def test_strict_metadata_calls_endpoint():
    transport = _SpyTransport()
    strict = StrictTimeSeriesService(transport)
    strict.execute_metadata(MetadataQuery(db="FM08"))
    endpoint, params = transport.calls[0]
    assert endpoint == "/getMetadata"
    assert params["db"] == "FM08"
