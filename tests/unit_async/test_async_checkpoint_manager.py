from __future__ import annotations

import pytest

from boj_api_client.core.checkpoint_store import MemoryCheckpointStore
from boj_api_client.core.models import ApiEnvelope
from boj_api_client.timeseries.async_checkpoint_manager import AsyncCheckpointManager
from boj_api_client.timeseries.checkpoint_models import DataCodeCheckpointState
from boj_api_client.timeseries.models import TimeSeries
from boj_api_client.timeseries.queries import DataCodeQuery


def _series(code: str) -> TimeSeries:
    return TimeSeries(
        series_code=code,
        name=code,
        unit="u",
        frequency="Q",
        category="c",
        last_update="20250101",
        points=[],
    )


@pytest.mark.asyncio
async def test_async_checkpoint_manager_accepts_sync_store():
    query = DataCodeQuery(db="CO", code=["A"])
    manager = AsyncCheckpointManager(
        store=MemoryCheckpointStore(),
        config_snapshot={"max_attempts": 5},
    )

    checkpoint_id = await manager.save_data_code(
        DataCodeCheckpointState(
            query=query,
            config_snapshot={"max_attempts": 5},
            by_code={"A": _series("A")},
            last_envelope=ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None),
            chunk_index=0,
            start_position=1,
        )
    )
    loaded = await manager.load_data_code(
        checkpoint_id=checkpoint_id,
        normalized=query,
    )
    assert loaded.by_code["A"].series_code == "A"


class _AsyncInMemoryStore:
    def __init__(self) -> None:
        self._items: dict[str, dict] = {}

    async def save(self, state: dict) -> str:
        checkpoint_id = "a" * 32
        self._items[checkpoint_id] = dict(state)
        return checkpoint_id

    async def load(self, checkpoint_id: str) -> dict:
        return dict(self._items[checkpoint_id])

    async def delete(self, checkpoint_id: str) -> None:
        self._items.pop(checkpoint_id, None)


@pytest.mark.asyncio
async def test_async_checkpoint_manager_accepts_async_store():
    query = DataCodeQuery(db="CO", code=["A"])
    manager = AsyncCheckpointManager(
        store=_AsyncInMemoryStore(),
        config_snapshot={"max_attempts": 5},
    )

    checkpoint_id = await manager.save_data_code(
        DataCodeCheckpointState(
            query=query,
            config_snapshot={"max_attempts": 5},
            by_code={"A": _series("A")},
            last_envelope=ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None),
            chunk_index=0,
            start_position=1,
        )
    )
    loaded = await manager.load_data_code(
        checkpoint_id=checkpoint_id,
        normalized=query,
    )
    assert loaded.by_code["A"].series_code == "A"
