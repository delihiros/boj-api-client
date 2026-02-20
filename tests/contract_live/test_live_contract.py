from __future__ import annotations

import os

import pytest

from boj_api_client import BojClient, BojClientConfig
from boj_api_client.config import RetryConfig, ThrottlingConfig
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery


pytestmark = pytest.mark.live


def _require_live_flag() -> None:
    if os.getenv("BOJ_RUN_LIVE") != "1":
        pytest.skip("Set BOJ_RUN_LIVE=1 to run live contract tests")


def _live_client() -> BojClient:
    cfg = BojClientConfig(
        throttling=ThrottlingConfig(min_wait_interval_seconds=1.0),
        retry=RetryConfig(
            max_attempts=3,
            max_backoff_seconds=5.0,
            total_retry_budget_seconds=30.0,
        ),
    )
    cfg.validate()
    return BojClient(config=cfg)


def test_live_get_metadata_contract_minimum():
    _require_live_flag()
    with _live_client() as client:
        result = client.timeseries.get_metadata(MetadataQuery(db="FM08", lang="JP"))

    assert result.envelope.status == 200
    assert isinstance(result.envelope.message_id, str)
    assert isinstance(result.entries, tuple)
    assert len(result.entries) > 0
    assert all(isinstance(entry.series_code, str) for entry in result.entries[:5])


def test_live_get_data_code_contract_minimum():
    _require_live_flag()
    with _live_client() as client:
        result = client.timeseries.get_data_code(
            DataCodeQuery(
                db="CO",
                code=["TK99F1000601GCQ01000"],
                lang="JP",
            )
        )

    assert result.envelope.status == 200
    assert isinstance(result.envelope.message_id, str)
    assert len(result.series) >= 1
    series = result.series[0]
    assert isinstance(series.series_code, str)
    assert isinstance(series.name, str) or series.name is None
    assert all(isinstance(point.survey_date, str) for point in series.points[:5])


def test_live_get_data_layer_contract_optional_by_env():
    _require_live_flag()
    layer1 = os.getenv("BOJ_LIVE_LAYER1")
    if not layer1:
        pytest.skip("Set BOJ_LIVE_LAYER1 for live getDataLayer contract test")

    db = os.getenv("BOJ_LIVE_LAYER_DB", "MD10")
    frequency = os.getenv("BOJ_LIVE_LAYER_FREQUENCY", "Q")

    with _live_client() as client:
        result = client.timeseries.get_data_layer(
            DataLayerQuery(
                db=db,
                frequency=frequency,
                layer1=layer1,
                lang="JP",
            )
        )

    assert result.envelope.status == 200
    assert isinstance(result.series, tuple)
