from __future__ import annotations

import pytest

from boj_api_client.core.async_checkpoint_store import AsyncCheckpointStoreAdapter
from boj_api_client.core.errors import BojValidationError


class _SpySyncStore:
    def __init__(self):
        self.state_by_id: dict[str, dict] = {}
        self.save_calls = 0
        self.load_calls = 0
        self.delete_calls = 0

    def save(self, state: dict) -> str:
        self.save_calls += 1
        checkpoint_id = "a" * 32
        self.state_by_id[checkpoint_id] = dict(state)
        return checkpoint_id

    def load(self, checkpoint_id: str) -> dict:
        self.load_calls += 1
        if checkpoint_id not in self.state_by_id:
            raise BojValidationError("checkpoint_id not found")
        return dict(self.state_by_id[checkpoint_id])

    def delete(self, checkpoint_id: str) -> None:
        self.delete_calls += 1
        if checkpoint_id not in self.state_by_id:
            raise BojValidationError("checkpoint_id not found")
        del self.state_by_id[checkpoint_id]


async def _run_immediately(func, *args):
    return func(*args)


@pytest.mark.asyncio
async def test_async_checkpoint_store_adapter_roundtrip():
    store = _SpySyncStore()
    adapter = AsyncCheckpointStoreAdapter(store, run_sync=_run_immediately)

    checkpoint_id = await adapter.save({"cursor": 1})
    loaded = await adapter.load(checkpoint_id)
    await adapter.delete(checkpoint_id)

    assert checkpoint_id == "a" * 32
    assert loaded == {"cursor": 1}
    assert store.save_calls == 1
    assert store.load_calls == 1
    assert store.delete_calls == 1


@pytest.mark.asyncio
async def test_async_checkpoint_store_adapter_propagates_errors():
    store = _SpySyncStore()
    adapter = AsyncCheckpointStoreAdapter(store, run_sync=_run_immediately)

    with pytest.raises(BojValidationError, match="checkpoint_id not found"):
        await adapter.load("a" * 32)
