from __future__ import annotations

import asyncio

import pytest

from boj_api_client.core.checkpoint_store import MemoryCheckpointStore


@pytest.mark.asyncio
async def test_memory_checkpoint_store_concurrent_save_load_delete_is_safe():
    store = MemoryCheckpointStore()

    async def _worker(index: int) -> None:
        state = {"index": index}
        checkpoint_id = await asyncio.to_thread(store.save, state)
        loaded = await asyncio.to_thread(store.load, checkpoint_id)
        assert loaded == state
        await asyncio.to_thread(store.delete, checkpoint_id)

    await asyncio.gather(*[_worker(i) for i in range(50)])

