from __future__ import annotations

from boj_api_client.core.retry import can_retry, next_backoff_seconds
from boj_api_client.core.throttling import MinIntervalThrottler


def test_throttler_waits_for_remaining_interval():
    now = {"value": 0.0}
    sleeps: list[float] = []

    def clock() -> float:
        return now["value"]

    def sleeper(sec: float) -> None:
        sleeps.append(sec)
        now["value"] += sec

    throttler = MinIntervalThrottler(1.0, clock=clock, sleeper=sleeper)
    throttler.wait()  # first call, no wait
    now["value"] = 0.2
    throttler.wait()
    assert sleeps and abs(sleeps[0] - 0.8) < 1e-6


def test_retry_budget_and_attempt_count():
    assert can_retry(
        attempt=1,
        max_attempts=5,
        started_at=0.0,
        now=1.0,
        total_budget_seconds=120.0,
    )
    assert not can_retry(
        attempt=5,
        max_attempts=5,
        started_at=0.0,
        now=1.0,
        total_budget_seconds=120.0,
    )


def test_backoff_is_non_negative():
    value = next_backoff_seconds(attempt_index=3, max_backoff_seconds=30.0)
    assert value >= 0.0


def test_throttler_reset_clears_last_request_timestamp():
    now = {"value": 0.0}
    sleeps: list[float] = []

    def clock() -> float:
        return now["value"]

    def sleeper(sec: float) -> None:
        sleeps.append(sec)
        now["value"] += sec

    throttler = MinIntervalThrottler(1.0, clock=clock, sleeper=sleeper)
    throttler.wait()
    now["value"] = 0.1
    throttler.reset()
    throttler.wait()
    assert sleeps == []
