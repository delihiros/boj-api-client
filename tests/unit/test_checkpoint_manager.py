from __future__ import annotations

import pytest

from boj_api_client.core.checkpoint_store import MemoryCheckpointStore
from boj_api_client.core.errors import BojValidationError
from boj_api_client.core.models import ApiEnvelope
from boj_api_client.timeseries.checkpoint_manager import CheckpointManager
from boj_api_client.timeseries.checkpoint_state import (
    DataCodeCheckpointState,
    DataLayerAutoPartitionCheckpointState,
    DataLayerDirectCheckpointState,
)
from boj_api_client.timeseries.models import TimeSeries
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery


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


def test_checkpoint_manager_roundtrip_data_code_state():
    query = DataCodeQuery(db="CO", code=["A", "B"])
    manager = CheckpointManager(
        store=MemoryCheckpointStore(),
        config_snapshot={"max_attempts": 5},
    )
    checkpoint_id = manager.save_data_code(
        DataCodeCheckpointState(
            query=query,
            config_snapshot={"max_attempts": 5},
            by_code={"A": _series("A")},
            last_envelope=ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None),
            chunk_index=1,
            start_position=3,
        )
    )

    loaded = manager.load_data_code(
        checkpoint_id=checkpoint_id,
        normalized=query,
    )
    assert loaded.chunk_index == 1
    assert loaded.start_position == 3
    assert list(loaded.by_code) == ["A"]


def test_checkpoint_manager_rejects_query_mismatch():
    good_query = DataCodeQuery(db="CO", code=["A"])
    bad_query = DataCodeQuery(db="CO", code=["B"])
    manager = CheckpointManager(
        store=MemoryCheckpointStore(),
        config_snapshot={"max_attempts": 5},
    )
    checkpoint_id = manager.save_data_code(
        DataCodeCheckpointState(
            query=good_query,
            config_snapshot={"max_attempts": 5},
            by_code={"A": _series("A")},
            last_envelope=ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None),
            chunk_index=0,
            start_position=1,
        )
    )

    with pytest.raises(BojValidationError, match="checkpoint query mismatch"):
        manager.load_data_code(checkpoint_id=checkpoint_id, normalized=bad_query)


def test_checkpoint_manager_rejects_config_snapshot_mismatch():
    query = DataCodeQuery(db="CO", code=["A"])
    store = MemoryCheckpointStore()
    writer = CheckpointManager(
        store=store,
        config_snapshot={"max_attempts": 5},
    )
    checkpoint_id = writer.save_data_code(
        DataCodeCheckpointState(
            query=query,
            config_snapshot={"max_attempts": 5},
            by_code={"A": _series("A")},
            last_envelope=ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None),
            chunk_index=0,
            start_position=1,
        )
    )

    reader = CheckpointManager(
        store=store,
        config_snapshot={"max_attempts": 3},
    )
    with pytest.raises(BojValidationError, match="checkpoint config mismatch"):
        reader.load_data_code(checkpoint_id=checkpoint_id, normalized=query)


def test_checkpoint_manager_loads_data_layer_union_states():
    query = DataLayerQuery(db="MD10", frequency="Q", layer1="*")
    manager = CheckpointManager(
        store=MemoryCheckpointStore(),
        config_snapshot={"max_attempts": 5},
    )

    direct_id = manager.save_data_layer_direct(
        DataLayerDirectCheckpointState(
            query=query,
            config_snapshot={"max_attempts": 5},
            by_code={"A": _series("A")},
            last_envelope=ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None),
            start_position=2,
            next_position=3,
        )
    )
    auto_id = manager.save_data_layer_auto_partition(
        DataLayerAutoPartitionCheckpointState(
            query=query,
            config_snapshot={"max_attempts": 5},
            selected_codes=["A", "B"],
            data_code_checkpoint_id="a" * 32,
        )
    )

    loaded_direct = manager.load_data_layer(checkpoint_id=direct_id, normalized=query)
    loaded_auto = manager.load_data_layer(checkpoint_id=auto_id, normalized=query)
    assert isinstance(loaded_direct, DataLayerDirectCheckpointState)
    assert isinstance(loaded_auto, DataLayerAutoPartitionCheckpointState)


def test_checkpoint_manager_rejects_point_with_missing_survey_date():
    query = DataCodeQuery(db="CO", code=["A"])
    store = MemoryCheckpointStore()
    checkpoint_id = store.save(
        {
            "kind": "data_code",
            "query": {
                "db": query.db,
                "code": list(query.code),
                "lang": query.lang,
                "start_date": query.start_date,
                "end_date": query.end_date,
                "start_position": query.start_position,
            },
            "config_snapshot": {"max_attempts": 5},
            "by_code": {
                "A": {
                    "series_code": "A",
                    "name": "A",
                    "unit": "u",
                    "frequency": "Q",
                    "category": "c",
                    "last_update": "20250101",
                    "points": [{"survey_date": None, "value": 1}],
                }
            },
            "last_envelope": {
                "status": 200,
                "message_id": "M181000I",
                "message": "ok",
                "date": None,
            },
            "chunk_index": 0,
            "start_position": 1,
        }
    )
    manager = CheckpointManager(
        store=store,
        config_snapshot={"max_attempts": 5},
    )

    with pytest.raises(BojValidationError, match="checkpoint points is invalid"):
        manager.load_data_code(checkpoint_id=checkpoint_id, normalized=query)
