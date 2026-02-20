"""Async pagination helpers based on NEXTPOSITION."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable

from .errors import BojProtocolError
from .pagination import parse_next_position


async def aiterate_pages(
    fetch_page: Callable[[int], Awaitable[dict]],
    *,
    start_position: int = 1,
    max_pages: int = 10_000,
) -> AsyncIterator[dict]:
    current = start_position
    seen_positions: set[int] = set()

    for _ in range(max_pages):
        payload = await fetch_page(current)
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
    "aiterate_pages",
]
