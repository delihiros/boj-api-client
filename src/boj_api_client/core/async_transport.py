"""Async HTTP transport with retry, throttling, and status evaluation."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Protocol

import httpx

from ..config import BojClientConfig
from .async_throttling import AsyncMinIntervalThrottler
from .errors import (
    BojProtocolError,
    BojServerError,
    BojTransportError,
    BojUnavailableError,
    BojValidationError,
)
from .response_parsing import classify_payload_outcome, parse_json_payload
from .retry import is_retryable_api_status
from .transport_shared import (
    build_default_headers,
    build_default_timeout,
    compute_backoff_seconds,
    should_retry_attempt,
)

logger = logging.getLogger("boj_api_client")


class AsyncTransportClient(Protocol):
    async def get(self, endpoint: str, params: Mapping[str, str]) -> object: ...
    async def aclose(self) -> None: ...


class AsyncTransport:
    """Asynchronous transport for BOJ API."""

    def __init__(
        self,
        config: BojClientConfig,
        *,
        client: AsyncTransportClient | None = None,
        sleeper: Callable[[float], Awaitable[None]] | None = None,
        clock: Callable[[], float] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._config = config
        self._sleep = sleeper or _default_sleep
        self._clock = clock or time.monotonic
        self._rng = rng or random.Random()
        self._closed = False

        self._throttler = AsyncMinIntervalThrottler(
            config.throttling.min_wait_interval_seconds,
            clock=self._clock,
            sleeper=self._sleep,
        )
        self._owns_client = client is None
        normalized_base_url = config.base_url.rstrip("/") + "/"
        self._client = client or httpx.AsyncClient(
            base_url=normalized_base_url,
            headers=build_default_headers(config),
            timeout=build_default_timeout(config),
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_client and hasattr(self._client, "aclose"):
            await self._client.aclose()

    async def request(self, endpoint: str, *, params: Mapping[str, str]) -> dict[str, object]:
        if self._closed:
            raise BojTransportError("transport is already closed")

        started_at = self._clock()
        attempt = 0
        normalized_endpoint = self._normalize_endpoint(endpoint)

        while True:
            attempt += 1
            logger.debug("request start endpoint=%s attempt=%s", normalized_endpoint, attempt)
            await self._throttler.wait()

            try:
                response = await self._client.get(normalized_endpoint, params=params)
            except Exception as exc:
                if should_retry_attempt(
                    config=self._config,
                    attempt=attempt,
                    started_at=started_at,
                    now=self._clock(),
                ):
                    logger.warning(
                        "request network error; retrying endpoint=%s attempt=%s error=%s",
                        normalized_endpoint,
                        attempt,
                        exc.__class__.__name__,
                    )
                    await self._sleep(
                        compute_backoff_seconds(
                            config=self._config,
                            attempt=attempt,
                            rng=self._rng,
                        )
                    )
                    continue
                logger.error(
                    "request network error; giving up endpoint=%s attempt=%s error=%s",
                    normalized_endpoint,
                    attempt,
                    exc.__class__.__name__,
                )
                raise BojTransportError(
                    "network/transport error",
                    cause="network",
                ) from exc

            http_status = getattr(response, "status_code", None)
            logger.debug(
                "response received endpoint=%s attempt=%s http_status=%s",
                normalized_endpoint,
                attempt,
                http_status,
            )
            try:
                payload = parse_json_payload(response, http_status=http_status)
            except (
                BojProtocolError,
                BojValidationError,
                BojServerError,
                BojUnavailableError,
            ):
                logger.error(
                    "response parse error endpoint=%s attempt=%s http_status=%s",
                    normalized_endpoint,
                    attempt,
                    http_status,
                )
                raise
            mapped_error, status = classify_payload_outcome(
                payload,
                http_status=http_status,
            )
            if mapped_error is None:
                logger.info(
                    "request success endpoint=%s attempt=%s",
                    normalized_endpoint,
                    attempt,
                )
                return payload

            if (
                is_retryable_api_status(status)
                and should_retry_attempt(
                    config=self._config,
                    attempt=attempt,
                    started_at=started_at,
                    now=self._clock(),
                )
            ):
                logger.warning(
                    "request transient failure; retrying endpoint=%s attempt=%s status=%s",
                    normalized_endpoint,
                    attempt,
                    status,
                )
                await self._sleep(
                    compute_backoff_seconds(
                        config=self._config,
                        attempt=attempt,
                        rng=self._rng,
                    )
                )
                continue

            logger.error(
                "request failed endpoint=%s attempt=%s status=%s",
                normalized_endpoint,
                attempt,
                status,
            )
            raise mapped_error

    @staticmethod
    def _normalize_endpoint(endpoint: str) -> str:
        return endpoint.lstrip("/")


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


__all__ = [
    "AsyncTransport",
]
