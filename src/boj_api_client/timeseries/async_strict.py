"""Async strict fail-fast request execution (single request unit)."""

from __future__ import annotations

from collections.abc import Sequence

from ..core.async_transport import AsyncTransport
from .queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from .strict_shared import (
    build_strict_data_code_params,
    build_strict_data_layer_params,
    build_strict_metadata_params,
)


class AsyncStrictTimeSeriesService:
    """Single-request strict async executor."""

    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def execute_data_code(
        self,
        query: DataCodeQuery,
        *,
        code_subset: Sequence[str],
        start_position: int = 1,
    ) -> dict[str, object]:
        params = build_strict_data_code_params(
            query,
            code_subset=code_subset,
            start_position=start_position,
        )
        return await self._transport.request("/getDataCode", params=params)

    async def execute_data_layer(
        self,
        query: DataLayerQuery,
        *,
        start_position: int = 1,
    ) -> dict[str, object]:
        params = build_strict_data_layer_params(
            query,
            start_position=start_position,
        )
        return await self._transport.request("/getDataLayer", params=params)

    async def execute_metadata(self, query: MetadataQuery) -> dict[str, object]:
        params = build_strict_metadata_params(query)
        return await self._transport.request("/getMetadata", params=params)


__all__ = [
    "AsyncStrictTimeSeriesService",
]
