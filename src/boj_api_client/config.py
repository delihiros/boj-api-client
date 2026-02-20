"""Client configuration."""

from __future__ import annotations

from dataclasses import dataclass, field

from .core.checkpoint_store import DEFAULT_CHECKPOINT_TTL_SECONDS


@dataclass(slots=True, frozen=True)
class TransportConfig:
    """Transport-related settings."""

    timeout_connect_seconds: float = 5.0
    timeout_read_seconds: float = 30.0
    timeout_write_seconds: float = 30.0
    timeout_pool_seconds: float = 5.0

    def validate(self) -> None:
        for field_name in (
            "timeout_connect_seconds",
            "timeout_read_seconds",
            "timeout_write_seconds",
            "timeout_pool_seconds",
        ):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"transport.{field_name} must be > 0")


@dataclass(slots=True, frozen=True)
class RetryConfig:
    """Retry-related settings."""

    max_attempts: int = 5
    max_backoff_seconds: float = 30.0
    total_retry_budget_seconds: float = 120.0

    def validate(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("retry.max_attempts must be >= 1")
        if self.max_backoff_seconds < 0:
            raise ValueError("retry.max_backoff_seconds must be >= 0")
        if self.total_retry_budget_seconds < 0:
            raise ValueError("retry.total_retry_budget_seconds must be >= 0")


@dataclass(slots=True, frozen=True)
class ThrottlingConfig:
    """Throttling-related settings."""

    min_wait_interval_seconds: float = 1.0

    def validate(self) -> None:
        if self.min_wait_interval_seconds < 0:
            raise ValueError("throttling.min_wait_interval_seconds must be >= 0")


@dataclass(slots=True, frozen=True)
class CheckpointConfig:
    """Checkpoint-related settings."""

    enabled: bool = True
    ttl_seconds: float = float(DEFAULT_CHECKPOINT_TTL_SECONDS)

    def validate(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError("checkpoint.enabled must be bool")
        if self.ttl_seconds <= 0:
            raise ValueError("checkpoint.ttl_seconds must be > 0")


@dataclass(slots=True, frozen=True)
class TimeSeriesConfig:
    """Timeseries feature settings."""

    enable_layer_auto_partition: bool = False

    def validate(self) -> None:
        if not isinstance(self.enable_layer_auto_partition, bool):
            raise ValueError("timeseries.enable_layer_auto_partition must be bool")


@dataclass(slots=True, frozen=True)
class BojClientConfig:
    """Runtime configuration for BOJ client."""

    base_url: str = "https://www.stat-search.boj.or.jp/api/v1"
    user_agent: str = "boj-api-client/0.1.0"

    transport: TransportConfig = field(default_factory=TransportConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    throttling: ThrottlingConfig = field(default_factory=ThrottlingConfig)
    checkpoint: CheckpointConfig = field(default_factory=CheckpointConfig)
    timeseries: TimeSeriesConfig = field(default_factory=TimeSeriesConfig)

    def to_checkpoint_snapshot(self) -> dict[str, int | float | bool]:
        return {
            "max_attempts": self.retry.max_attempts,
            "max_backoff_seconds": self.retry.max_backoff_seconds,
            "total_retry_budget_seconds": self.retry.total_retry_budget_seconds,
            "min_wait_interval_seconds": self.throttling.min_wait_interval_seconds,
            "enable_layer_auto_partition": self.timeseries.enable_layer_auto_partition,
            "checkpoint_enabled": self.checkpoint.enabled,
            "checkpoint_ttl_seconds": self.checkpoint.ttl_seconds,
        }

    def validate(self) -> None:
        if not self.base_url:
            raise ValueError("base_url must not be empty")
        self.transport.validate()
        self.retry.validate()
        self.throttling.validate()
        self.checkpoint.validate()
        self.timeseries.validate()


__all__ = [
    "TransportConfig",
    "RetryConfig",
    "ThrottlingConfig",
    "CheckpointConfig",
    "TimeSeriesConfig",
    "BojClientConfig",
]
