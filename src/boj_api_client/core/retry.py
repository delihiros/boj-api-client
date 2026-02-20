"""Retry helpers."""

from __future__ import annotations

import random
from typing import Callable


def is_retryable_api_status(status: int | None) -> bool:
    return status in {500, 503}


def next_backoff_seconds(
    *,
    attempt_index: int,
    max_backoff_seconds: float,
    rng: random.Random | None = None,
) -> float:
    """Exponential backoff with small jitter.

    attempt_index: 0-based retry index.
    """

    base = min(max_backoff_seconds, float(2**attempt_index))
    if base <= 0:
        return 0.0
    source = rng or random
    jitter = base * 0.1 * (source.random() * 2.0 - 1.0)
    return max(0.0, base + jitter)


def can_retry(
    *,
    attempt: int,
    max_attempts: int,
    started_at: float,
    now: float,
    total_budget_seconds: float,
) -> bool:
    if attempt >= max_attempts:
        return False
    return (now - started_at) <= total_budget_seconds


__all__ = [
    "is_retryable_api_status",
    "next_backoff_seconds",
    "can_retry",
]
