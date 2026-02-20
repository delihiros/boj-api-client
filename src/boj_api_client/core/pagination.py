"""Pagination helpers based on NEXTPOSITION."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

from .errors import BojProtocolError


def parse_next_position(payload: dict[str, Any]) -> int | None:
    raw = payload.get("NEXTPOSITION")
    if raw is None:
        return None
    if isinstance(raw, str):
        text = raw.strip()
        if text == "":
            return None
        if text.isdigit():
            return int(text)
        raise BojProtocolError("NEXTPOSITION is not a valid integer")
    if isinstance(raw, int):
        return raw
    raise BojProtocolError("NEXTPOSITION has unsupported type")


def iterate_pages(
    fetch_page: Callable[[int], dict[str, Any]],
    *,
    start_position: int = 1,
    max_pages: int = 10_000,
) -> Iterator[dict[str, Any]]:
    current = start_position
    seen_positions: set[int] = set()

    for _ in range(max_pages):
        payload = fetch_page(current)
        yield payload

        next_position = parse_next_position(payload)
        if next_position is None:
            return
        if next_position in seen_positions:
            raise BojProtocolError("NEXTPOSITION loop detected")
        seen_positions.add(next_position)
        current = next_position

    raise BojProtocolError("Exceeded pagination guardrail (max_pages)")


__all__ = [
    "parse_next_position",
    "iterate_pages",
]
