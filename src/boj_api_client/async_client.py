"""Public async client entrypoint."""

from __future__ import annotations

from collections.abc import AsyncIterator
from types import TracebackType

from .client_shared import resolve_checkpoint_store, validate_client_config
from .config import BojClientConfig
from .core.checkpoint_store import CheckpointStore
from .core.async_transport import AsyncTransport
from .core.errors import BojClientClosedError
from .timeseries.async_orchestrator import AsyncTimeSeriesService
from .timeseries.async_strict import AsyncStrictTimeSeriesService
from .timeseries.models import DataCodeResponse, DataLayerResponse, MetadataResponse
from .timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery


class _GuardedAsyncTimeSeriesService:
    """Guard wrapper to block usage after async client close."""

    def __init__(self, owner: "AsyncBojClient", delegate: AsyncTimeSeriesService) -> None:
        self._owner = owner
        self._delegate = delegate

    async def get_data_code(
        self,
        query: DataCodeQuery,
        *,
        checkpoint_id: str | None = None,
    ) -> DataCodeResponse:
        self._owner._ensure_open()
        return await self._delegate.get_data_code(query, checkpoint_id=checkpoint_id)

    async def get_data_layer(
        self,
        query: DataLayerQuery,
        *,
        checkpoint_id: str | None = None,
    ) -> DataLayerResponse:
        self._owner._ensure_open()
        return await self._delegate.get_data_layer(query, checkpoint_id=checkpoint_id)

    async def get_metadata(self, query: MetadataQuery) -> MetadataResponse:
        self._owner._ensure_open()
        return await self._delegate.get_metadata(query)

    def iter_data_code(self, query: DataCodeQuery) -> AsyncIterator[DataCodeResponse]:
        self._owner._ensure_open()
        return self._iter_data_code_guarded(query)

    def iter_data_layer(self, query: DataLayerQuery) -> AsyncIterator[DataLayerResponse]:
        self._owner._ensure_open()
        return self._iter_data_layer_guarded(query)

    async def _iter_data_code_guarded(self, query: DataCodeQuery) -> AsyncIterator[DataCodeResponse]:
        iterator = self._delegate.iter_data_code(query).__aiter__()
        try:
            while True:
                self._owner._ensure_open()
                try:
                    page = await anext(iterator)
                except StopAsyncIteration:
                    return
                self._owner._ensure_open()
                yield page
        finally:
            await iterator.aclose()

    async def _iter_data_layer_guarded(
        self,
        query: DataLayerQuery,
    ) -> AsyncIterator[DataLayerResponse]:
        iterator = self._delegate.iter_data_layer(query).__aiter__()
        try:
            while True:
                self._owner._ensure_open()
                try:
                    page = await anext(iterator)
                except StopAsyncIteration:
                    return
                self._owner._ensure_open()
                yield page
        finally:
            await iterator.aclose()


class AsyncBojClient:
    """Public async BOJ API client."""

    def __init__(
        self,
        *,
        config: BojClientConfig | None = None,
        transport: AsyncTransport | None = None,
        checkpoint_store: CheckpointStore | None = None,
        strict_service: AsyncStrictTimeSeriesService | None = None,
        timeseries_service: AsyncTimeSeriesService | None = None,
    ) -> None:
        self._config = config or BojClientConfig()
        validate_client_config(self._config)

        self._transport = transport or AsyncTransport(self._config)
        self._strict = strict_service or AsyncStrictTimeSeriesService(self._transport)
        resolved_checkpoint_store = resolve_checkpoint_store(
            config=self._config,
            checkpoint_store=checkpoint_store,
        )
        internal_timeseries = timeseries_service or AsyncTimeSeriesService(
            self._strict,
            enable_layer_auto_partition=self._config.timeseries.enable_layer_auto_partition,
            checkpoint_store=resolved_checkpoint_store,
            config_snapshot=self._config.to_checkpoint_snapshot(),
        )
        self._closed = False
        self.timeseries = _GuardedAsyncTimeSeriesService(self, internal_timeseries)

    def _ensure_open(self) -> None:
        if self._closed:
            raise BojClientClosedError("AsyncBojClient is already closed")

    async def close(self) -> None:
        if self._closed:
            return
        await self._transport.close()
        self._closed = True

    async def __aenter__(self) -> "AsyncBojClient":
        self._ensure_open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        await self.close()
        return False


__all__ = [
    "AsyncBojClient",
]
