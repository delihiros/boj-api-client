"""Rate throttling utilities."""

from __future__ import annotations

import time
from typing import Callable


class MinIntervalThrottler:
    """Ensures minimum interval between outbound requests."""

    def __init__(
        self,
        min_interval_seconds: float,
        *,
        clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._clock = clock or time.monotonic
        self._sleep = sleeper or time.sleep
        self._last_request_at: float | None = None

    def wait(self) -> None:
        now = self._clock()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            remaining = self._min_interval_seconds - elapsed
            if remaining > 0:
                self._sleep(remaining)
                now = self._clock()
        self._last_request_at = now

    def reset(self) -> None:
        self._last_request_at = None


__all__ = [
    "MinIntervalThrottler",
]
