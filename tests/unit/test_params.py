from __future__ import annotations

from boj_api_client.timeseries.params import (
    build_data_code_params,
    build_data_layer_params,
    build_layer_param,
    build_metadata_params,
)
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery


def test_build_layer_param_stops_at_first_empty_layer():
    query = DataLayerQuery(
        db="MD10",
        frequency="Q",
        layer1="*",
        layer2="A",
        layer3=None,
        layer4="B",
    )
    assert build_layer_param(query) == "*,A"


def test_build_data_code_params_omits_start_position_for_first_page():
    query = DataCodeQuery(db="CO", code=["A"], start_date="202401", end_date="202402")
    params = build_data_code_params(query, start_position=1)
    assert params["code"] == "A"
    assert params["startDate"] == "202401"
    assert params["endDate"] == "202402"
    assert "startPosition" not in params


def test_build_data_layer_params_includes_start_position_for_resume():
    query = DataLayerQuery(
        db="MD10",
        frequency="Q",
        layer1="*",
        layer2="A",
        start_date="202401",
    )
    params = build_data_layer_params(query, start_position=255)
    assert params["layer"] == "*,A"
    assert params["startPosition"] == "255"
    assert params["startDate"] == "202401"


def test_build_metadata_params():
    query = MetadataQuery(db="FM08", lang="EN")
    assert build_metadata_params(query) == {
        "format": "json",
        "lang": "EN",
        "db": "FM08",
    }
