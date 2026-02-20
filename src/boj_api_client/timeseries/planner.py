"""Execution planning helpers for timeseries orchestration."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from ..core.errors import BojValidationError
from ..core.pagination import parse_next_position

AUTO_PARTITION_LIMIT_MARKER = "1,250"


def chunk_codes(
    codes: Sequence[str],
    *,
    chunk_size: int = 250,
) -> tuple[tuple[str, ...], ...]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    return tuple(
        tuple(codes[i : i + chunk_size])
        for i in range(0, len(codes), chunk_size)
    )


@dataclass(slots=True, frozen=True)
class DataCodeChunkPlan:
    chunk_index: int
    codes: tuple[str, ...]
    start_position: int


def plan_data_code_chunks(
    *,
    codes: Sequence[str],
    chunk_size: int = 250,
    resume_chunk_index: int = 0,
    resume_start_position: int = 1,
) -> list[DataCodeChunkPlan]:
    if resume_chunk_index < 0:
        raise ValueError("resume_chunk_index must be >= 0")
    if resume_start_position < 1:
        raise ValueError("resume_start_position must be >= 1")

    chunks = chunk_codes(codes, chunk_size=chunk_size)
    if resume_chunk_index > len(chunks):
        raise ValueError("resume_chunk_index is out of range")

    plans: list[DataCodeChunkPlan] = []
    for index in range(resume_chunk_index, len(chunks)):
        start_position = resume_start_position if index == resume_chunk_index else 1
        plans.append(
            DataCodeChunkPlan(
                chunk_index=index,
                codes=chunks[index],
                start_position=start_position,
            )
        )
    return plans


def should_use_auto_partition(
    error: BojValidationError,
    *,
    marker: str = AUTO_PARTITION_LIMIT_MARKER,
) -> bool:
    return marker in str(error)


def next_position_or_raise(
    *,
    payload: dict[str, object],
    seen_positions: set[int],
    context_name: str,
) -> int | None:
    next_position = parse_next_position(payload)
    if next_position is None:
        return None
    if next_position in seen_positions:
        raise BojValidationError(f"NEXTPOSITION loop detected during {context_name} retrieval")
    seen_positions.add(next_position)
    return next_position


__all__ = [
    "AUTO_PARTITION_LIMIT_MARKER",
    "DataCodeChunkPlan",
    "chunk_codes",
    "plan_data_code_chunks",
    "should_use_auto_partition",
    "next_position_or_raise",
]
