"""Shared helpers for sync/async transport implementations."""

from __future__ import annotations

import random
from collections.abc import Mapping

import httpx

from ..config import BojClientConfig
from .retry import can_retry, next_backoff_seconds


def build_default_headers(config: BojClientConfig) -> Mapping[str, str]:
    return {
        "Accept-Encoding": "gzip",
        "User-Agent": config.user_agent,
    }


def build_default_timeout(config: BojClientConfig) -> httpx.Timeout:
    return httpx.Timeout(
        connect=config.transport.timeout_connect_seconds,
        read=config.transport.timeout_read_seconds,
        write=config.transport.timeout_write_seconds,
        pool=config.transport.timeout_pool_seconds,
    )


def should_retry_attempt(
    *,
    config: BojClientConfig,
    attempt: int,
    started_at: float,
    now: float,
) -> bool:
    return can_retry(
        attempt=attempt,
        max_attempts=config.retry.max_attempts,
        started_at=started_at,
        now=now,
        total_budget_seconds=config.retry.total_retry_budget_seconds,
    )


def compute_backoff_seconds(
    *,
    config: BojClientConfig,
    attempt: int,
    rng: random.Random,
) -> float:
    return next_backoff_seconds(
        attempt_index=attempt - 1,
        max_backoff_seconds=config.retry.max_backoff_seconds,
        rng=rng,
    )


__all__ = [
    "build_default_headers",
    "build_default_timeout",
    "should_retry_attempt",
    "compute_backoff_seconds",
]
