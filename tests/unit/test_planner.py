from __future__ import annotations

import pytest

from boj_api_client.core.errors import BojValidationError
from boj_api_client.timeseries.planner import (
    AUTO_PARTITION_LIMIT_MARKER,
    chunk_codes,
    next_position_or_raise,
    plan_data_code_chunks,
    should_use_auto_partition,
)


def test_chunk_codes_splits_with_chunk_size():
    codes = [f"C{i:03d}" for i in range(5)]
    assert chunk_codes(codes, chunk_size=2) == (("C000", "C001"), ("C002", "C003"), ("C004",))


def test_chunk_codes_rejects_non_positive_chunk_size():
    with pytest.raises(ValueError, match="chunk_size must be > 0"):
        chunk_codes(["A"], chunk_size=0)


def test_plan_data_code_chunks_supports_resume():
    plans = plan_data_code_chunks(
        codes=[f"C{i:03d}" for i in range(251)],
        chunk_size=250,
        resume_chunk_index=1,
        resume_start_position=3,
    )
    assert len(plans) == 1
    assert plans[0].chunk_index == 1
    assert plans[0].start_position == 3
    assert plans[0].codes == ("C250",)


def test_plan_data_code_chunks_rejects_out_of_range_resume_index():
    with pytest.raises(ValueError, match="resume_chunk_index is out of range"):
        plan_data_code_chunks(codes=["A"], resume_chunk_index=2)


def test_should_use_auto_partition():
    assert (
        should_use_auto_partition(BojValidationError(f"exceeds {AUTO_PARTITION_LIMIT_MARKER} series"))
        is True
    )
    assert should_use_auto_partition(BojValidationError("other validation")) is False


def test_next_position_or_raise_tracks_seen_positions():
    seen: set[int] = set()
    next_position = next_position_or_raise(
        payload={"NEXTPOSITION": 2},
        seen_positions=seen,
        context_name="data_code",
    )
    assert next_position == 2
    assert seen == {2}

    with pytest.raises(BojValidationError, match="NEXTPOSITION loop detected during data_code retrieval"):
        next_position_or_raise(
            payload={"NEXTPOSITION": 2},
            seen_positions=seen,
            context_name="data_code",
        )
