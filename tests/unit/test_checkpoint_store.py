from __future__ import annotations

import pytest

from boj_api_client.core.checkpoint_store import MemoryCheckpointStore
from boj_api_client.core.errors import BojValidationError


class _FakeClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_memory_checkpoint_store_save_and_load_roundtrip():
    store = MemoryCheckpointStore()
    state = {"cursor": 2, "series": {"A": [1, 2]}}

    checkpoint_id = store.save(state)
    loaded = store.load(checkpoint_id)

    assert checkpoint_id
    assert loaded == state


def test_memory_checkpoint_store_load_missing_raises_validation_error():
    store = MemoryCheckpointStore()
    with pytest.raises(BojValidationError, match="checkpoint_id not found"):
        store.load("a" * 32)


def test_memory_checkpoint_store_delete_missing_raises_validation_error():
    store = MemoryCheckpointStore()
    with pytest.raises(BojValidationError, match="checkpoint_id not found"):
        store.delete("b" * 32)


def test_memory_checkpoint_store_load_expired_raises_validation_error():
    clock = _FakeClock()
    store = MemoryCheckpointStore(ttl_seconds=1.0, clock=clock)
    checkpoint_id = store.save({"cursor": 1})

    clock.advance(2.0)
    with pytest.raises(BojValidationError, match="checkpoint_id expired"):
        store.load(checkpoint_id)


def test_memory_checkpoint_store_state_is_copied():
    store = MemoryCheckpointStore()
    original = {"nested": {"cursor": 1}}
    checkpoint_id = store.save(original)
    original["nested"]["cursor"] = 999

    loaded = store.load(checkpoint_id)
    assert loaded["nested"]["cursor"] == 1


@pytest.mark.parametrize("checkpoint_id", ["", "missing", "../escape", "A" * 32, "a" * 31])
def test_memory_checkpoint_store_rejects_invalid_checkpoint_id(checkpoint_id: str):
    store = MemoryCheckpointStore()

    with pytest.raises(BojValidationError, match="checkpoint_id is invalid"):
        store.load(checkpoint_id)

    with pytest.raises(BojValidationError, match="checkpoint_id is invalid"):
        store.delete(checkpoint_id)
