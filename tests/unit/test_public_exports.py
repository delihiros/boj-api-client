from __future__ import annotations

import boj_api_client.timeseries as timeseries


def test_timeseries_package_exports_public_models_and_queries_only():
    expected = {
        "DataCodeQuery",
        "DataLayerQuery",
        "MetadataQuery",
        "DataCodeResponse",
        "DataLayerResponse",
        "MetadataResponse",
        "TimeSeries",
        "TimeSeriesPoint",
        "MetadataEntry",
    }
    assert expected.issubset(set(timeseries.__all__))
    assert "TimeSeriesService" not in timeseries.__all__
    assert "AsyncTimeSeriesService" not in timeseries.__all__
    assert not hasattr(timeseries, "TimeSeriesService")
    assert not hasattr(timeseries, "AsyncTimeSeriesService")

