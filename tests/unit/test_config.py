from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from boj_api_client.config import (
    BojClientConfig,
    CheckpointConfig,
    RetryConfig,
    ThrottlingConfig,
    TimeSeriesConfig,
    TransportConfig,
)


def test_config_validate_rejects_empty_base_url():
    cfg = BojClientConfig(base_url="")
    with pytest.raises(ValueError):
        cfg.validate()


def test_config_is_immutable():
    cfg = BojClientConfig()
    with pytest.raises(FrozenInstanceError):
        cfg.retry = RetryConfig(max_attempts=10)


def test_config_default_layer_auto_partition_disabled():
    cfg = BojClientConfig()
    assert cfg.timeseries.enable_layer_auto_partition is False


def test_config_default_checkpoint_enabled():
    cfg = BojClientConfig()
    assert cfg.checkpoint.enabled is True


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("retry", "max_attempts", 0),
        ("retry", "max_backoff_seconds", -1.0),
        ("retry", "total_retry_budget_seconds", -1.0),
        ("throttling", "min_wait_interval_seconds", -1.0),
        ("checkpoint", "ttl_seconds", 0.0),
        ("transport", "timeout_connect_seconds", 0.0),
        ("transport", "timeout_read_seconds", 0.0),
        ("transport", "timeout_write_seconds", 0.0),
        ("transport", "timeout_pool_seconds", 0.0),
    ],
)
def test_config_validate_rejects_invalid_numeric_values(section, field, value):
    kwargs = {field: value}
    cfg = BojClientConfig(
        retry=RetryConfig(**kwargs) if section == "retry" else RetryConfig(),
        throttling=ThrottlingConfig(**kwargs) if section == "throttling" else ThrottlingConfig(),
        checkpoint=CheckpointConfig(**kwargs) if section == "checkpoint" else CheckpointConfig(),
        transport=TransportConfig(**kwargs) if section == "transport" else TransportConfig(),
    )
    with pytest.raises(ValueError):
        cfg.validate()


def test_config_validate_rejects_non_bool_checkpoint_enabled():
    cfg = BojClientConfig(checkpoint=CheckpointConfig(enabled="yes"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="checkpoint.enabled must be bool"):
        cfg.validate()


def test_config_validate_rejects_non_bool_layer_auto_partition():
    cfg = BojClientConfig(
        timeseries=TimeSeriesConfig(enable_layer_auto_partition="yes")  # type: ignore[arg-type]
    )
    with pytest.raises(ValueError, match="timeseries.enable_layer_auto_partition must be bool"):
        cfg.validate()
