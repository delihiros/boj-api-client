"""Public client entrypoint."""

from __future__ import annotations

from collections.abc import Iterator
from types import TracebackType

from .client_shared import resolve_checkpoint_store, validate_client_config
from .config import BojClientConfig
from .core.checkpoint_store import CheckpointStore
from .core.errors import BojClientClosedError
from .core.transport import SyncTransport
from .timeseries.models import DataCodeResponse, DataLayerResponse, MetadataResponse
from .timeseries.orchestrator import TimeSeriesService
from .timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from .timeseries.strict import StrictTimeSeriesService


class _GuardedTimeSeriesService:
    """Guard wrapper to block usage after client close."""

    def __init__(self, owner: "BojClient", delegate: TimeSeriesService) -> None:
        self._owner = owner
        self._delegate = delegate

    def get_data_code(
        self,
        query: DataCodeQuery,
        *,
        checkpoint_id: str | None = None,
    ) -> DataCodeResponse:
        self._owner._ensure_open()
        return self._delegate.get_data_code(query, checkpoint_id=checkpoint_id)

    def get_data_layer(
        self,
        query: DataLayerQuery,
        *,
        checkpoint_id: str | None = None,
    ) -> DataLayerResponse:
        self._owner._ensure_open()
        return self._delegate.get_data_layer(query, checkpoint_id=checkpoint_id)

    def get_metadata(self, query: MetadataQuery) -> MetadataResponse:
        self._owner._ensure_open()
        return self._delegate.get_metadata(query)

    def iter_data_code(self, query: DataCodeQuery) -> Iterator[DataCodeResponse]:
        iterator = iter(self._delegate.iter_data_code(query))
        while True:
            self._owner._ensure_open()
            try:
                page = next(iterator)
            except StopIteration:
                return
            self._owner._ensure_open()
            yield page

    def iter_data_layer(self, query: DataLayerQuery) -> Iterator[DataLayerResponse]:
        iterator = iter(self._delegate.iter_data_layer(query))
        while True:
            self._owner._ensure_open()
            try:
                page = next(iterator)
            except StopIteration:
                return
            self._owner._ensure_open()
            yield page


class BojClient:
    """Public BOJ API client."""

    def __init__(
        self,
        *,
        config: BojClientConfig | None = None,
        transport: SyncTransport | None = None,
        checkpoint_store: CheckpointStore | None = None,
        strict_service: StrictTimeSeriesService | None = None,
        timeseries_service: TimeSeriesService | None = None,
    ) -> None:
        self._config = config or BojClientConfig()
        validate_client_config(self._config)

        self._transport = transport or SyncTransport(self._config)
        self._strict = strict_service or StrictTimeSeriesService(self._transport)
        resolved_checkpoint_store = resolve_checkpoint_store(
            config=self._config,
            checkpoint_store=checkpoint_store,
        )
        internal_timeseries = timeseries_service or TimeSeriesService(
            self._strict,
            enable_layer_auto_partition=self._config.timeseries.enable_layer_auto_partition,
            checkpoint_store=resolved_checkpoint_store,
            config_snapshot=self._config.to_checkpoint_snapshot(),
        )
        self._closed = False
        self.timeseries = _GuardedTimeSeriesService(self, internal_timeseries)

    def _ensure_open(self) -> None:
        if self._closed:
            raise BojClientClosedError("BojClient is already closed")

    def close(self) -> None:
        if self._closed:
            return
        self._transport.close()
        self._closed = True

    def __enter__(self) -> "BojClient":
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        self.close()
        return False


__all__ = [
    "BojClient",
]
