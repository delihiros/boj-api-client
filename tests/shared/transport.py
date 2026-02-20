from __future__ import annotations

from collections.abc import Sequence

from boj_api_client.config import BojClientConfig, RetryConfig, ThrottlingConfig


class Response:
    def __init__(self, status_code: int, payload: object):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> object:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


Step = Response | Exception


class SyncSequencedClient:
    def __init__(self, steps: Sequence[Step]):
        self.steps = list(steps)
        self.calls = 0

    def get(self, endpoint: str, params: dict[str, str]):
        self.calls += 1
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    def close(self):
        return None


class AsyncSequencedClient:
    def __init__(self, steps: Sequence[Step]):
        self.steps = list(steps)
        self.calls = 0
        self.closed = False

    async def get(self, endpoint: str, params: dict[str, str]):
        self.calls += 1
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    async def aclose(self):
        self.closed = True


def build_config(*, max_attempts: int = 5) -> BojClientConfig:
    cfg = BojClientConfig(
        throttling=ThrottlingConfig(min_wait_interval_seconds=0.0),
        retry=RetryConfig(
            max_attempts=max_attempts,
            max_backoff_seconds=0.0,
            total_retry_budget_seconds=120.0,
        ),
    )
    cfg.validate()
    return cfg
