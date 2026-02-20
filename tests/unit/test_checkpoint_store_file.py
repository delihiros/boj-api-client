from __future__ import annotations

import logging
import pickle

import pytest

from boj_api_client.core.checkpoint_store import FileCheckpointStore
from boj_api_client.core.errors import BojValidationError


class _FakeClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_file_checkpoint_store_save_load_and_delete_roundtrip(tmp_path):
    store = FileCheckpointStore(base_dir=tmp_path)
    checkpoint_id = store.save({"cursor": 10, "series": {"A": [1, 2]}})

    loaded = store.load(checkpoint_id)
    assert loaded["cursor"] == 10

    store.delete(checkpoint_id)
    with pytest.raises(BojValidationError, match="checkpoint_id not found"):
        store.load(checkpoint_id)


def test_file_checkpoint_store_load_missing_raises_validation_error(tmp_path):
    store = FileCheckpointStore(base_dir=tmp_path)
    with pytest.raises(BojValidationError, match="checkpoint_id not found"):
        store.load("c" * 32)


def test_file_checkpoint_store_load_expired_raises_validation_error(tmp_path):
    clock = _FakeClock()
    store = FileCheckpointStore(base_dir=tmp_path, ttl_seconds=1.0, clock=clock)
    checkpoint_id = store.save({"cursor": 1})

    clock.advance(2.0)
    with pytest.raises(BojValidationError, match="checkpoint_id expired"):
        store.load(checkpoint_id)


def test_file_checkpoint_store_persists_across_instances(tmp_path):
    clock = _FakeClock()
    first = FileCheckpointStore(base_dir=tmp_path, ttl_seconds=60.0, clock=clock)
    checkpoint_id = first.save({"cursor": 5})

    second = FileCheckpointStore(base_dir=tmp_path, ttl_seconds=60.0, clock=clock)
    loaded = second.load(checkpoint_id)
    assert loaded["cursor"] == 5


def test_file_checkpoint_store_state_is_copied(tmp_path):
    store = FileCheckpointStore(base_dir=tmp_path)
    original = {"nested": {"cursor": 1}}
    checkpoint_id = store.save(original)
    original["nested"]["cursor"] = 999

    loaded = store.load(checkpoint_id)
    assert loaded["nested"]["cursor"] == 1


def test_file_checkpoint_store_rejects_non_positive_ttl(tmp_path):
    with pytest.raises(ValueError, match="ttl_seconds must be > 0"):
        FileCheckpointStore(base_dir=tmp_path, ttl_seconds=0.0)


def test_file_checkpoint_store_lazy_gc_removes_expired_files(tmp_path):
    clock = _FakeClock()
    store = FileCheckpointStore(base_dir=tmp_path, ttl_seconds=2.0, clock=clock)
    expired_id = store.save({"cursor": 1})

    clock.advance(1.0)
    alive_id = store.save({"cursor": 2})

    clock.advance(1.5)
    loaded = store.load(alive_id)
    assert loaded["cursor"] == 2
    assert not (tmp_path / f"{expired_id}.pkl").exists()
    assert (tmp_path / f"{alive_id}.pkl").exists()


def test_file_checkpoint_store_removes_corrupt_file_and_logs_warning(tmp_path, caplog):
    store = FileCheckpointStore(base_dir=tmp_path)
    corrupt_id = "deadbeef" * 4
    corrupt_path = tmp_path / f"{corrupt_id}.pkl"
    with corrupt_path.open("wb") as file_obj:
        file_obj.write(b"not-a-valid-pickle")

    caplog.set_level(logging.WARNING, logger="boj_api_client")
    with pytest.raises(BojValidationError, match="checkpoint_id not found"):
        store.load(corrupt_id)

    assert not corrupt_path.exists()
    assert "corrupt checkpoint removed" in caplog.text


def test_file_checkpoint_store_removes_invalid_payload_and_logs_warning(tmp_path, caplog):
    store = FileCheckpointStore(base_dir=tmp_path)
    invalid_id = "cafebabe" * 4
    invalid_path = tmp_path / f"{invalid_id}.pkl"
    with invalid_path.open("wb") as file_obj:
        pickle.dump({"unexpected": "payload"}, file_obj, protocol=pickle.HIGHEST_PROTOCOL)

    caplog.set_level(logging.WARNING, logger="boj_api_client")
    with pytest.raises(BojValidationError, match="checkpoint_id not found"):
        store.load(invalid_id)

    assert not invalid_path.exists()
    assert "invalid checkpoint payload removed" in caplog.text


@pytest.mark.parametrize("checkpoint_id", ["", "missing", "../escape", "A" * 32, "a" * 31])
def test_file_checkpoint_store_rejects_invalid_checkpoint_id(tmp_path, checkpoint_id: str):
    store = FileCheckpointStore(base_dir=tmp_path)

    with pytest.raises(BojValidationError, match="checkpoint_id is invalid"):
        store.load(checkpoint_id)

    with pytest.raises(BojValidationError, match="checkpoint_id is invalid"):
        store.delete(checkpoint_id)
