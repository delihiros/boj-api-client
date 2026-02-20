from __future__ import annotations

import pytest

from boj_api_client.async_client import AsyncBojClient
from boj_api_client.config import BojClientConfig, CheckpointConfig, TimeSeriesConfig
from boj_api_client.core.errors import BojClientClosedError
from boj_api_client.timeseries.models import (
    DataCodeResponse,
    DataLayerResponse,
    MetadataResponse,
    make_success_envelope,
)
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from tests.shared.client_fakes import (
    CheckpointAwareAsyncTimeSeriesService,
    DummyAsyncTransport,
    PagedDummyAsyncTransport,
)


class _CloseAwareAsyncTimeSeriesService:
    def __init__(self):
        self.code_closed = False
        self.layer_closed = False

    async def get_data_code(self, query: DataCodeQuery) -> DataCodeResponse:
        return DataCodeResponse(envelope=make_success_envelope(), series=[])

    async def get_data_layer(self, query: DataLayerQuery) -> DataLayerResponse:
        return DataLayerResponse(envelope=make_success_envelope(), series=[], next_position=None)

    async def get_metadata(self, query: MetadataQuery) -> MetadataResponse:
        return MetadataResponse(envelope=make_success_envelope(), entries=[])

    async def iter_data_code(self, query: DataCodeQuery):
        try:
            yield DataCodeResponse(envelope=make_success_envelope(), series=[])
            yield DataCodeResponse(envelope=make_success_envelope(), series=[])
        finally:
            self.code_closed = True

    async def iter_data_layer(self, query: DataLayerQuery):
        try:
            yield DataLayerResponse(envelope=make_success_envelope(), series=[], next_position=2)
            yield DataLayerResponse(envelope=make_success_envelope(), series=[], next_position=None)
        finally:
            self.layer_closed = True


@pytest.mark.asyncio
async def test_async_client_context_manager_closes_transport():
    transport = DummyAsyncTransport()
    async with AsyncBojClient(transport=transport) as client:
        assert client is not None
    assert transport.closed is True


@pytest.mark.asyncio
async def test_async_client_raises_when_used_after_close():
    transport = DummyAsyncTransport()
    client = AsyncBojClient(transport=transport)
    await client.close()
    with pytest.raises(BojClientClosedError):
        await client.timeseries.get_data_code(DataCodeQuery(db="CO", code=["A"]))


@pytest.mark.asyncio
async def test_async_client_delegates_timeseries_methods():
    transport = DummyAsyncTransport()
    async with AsyncBojClient(transport=transport) as client:
        code_result = await client.timeseries.get_data_code(DataCodeQuery(db="CO", code=["A"]))
        layer_result = await client.timeseries.get_data_layer(
            DataLayerQuery(db="MD10", frequency="Q", layer1="*")
        )
        metadata_result = await client.timeseries.get_metadata(MetadataQuery(db="FM08"))

        code_pages = []
        async for page in client.timeseries.iter_data_code(DataCodeQuery(db="CO", code=["A"])):
            code_pages.append(page)

        layer_pages = []
        async for page in client.timeseries.iter_data_layer(
            DataLayerQuery(db="MD10", frequency="Q", layer1="*")
        ):
            layer_pages.append(page)

    assert code_result.envelope.status == 200
    assert layer_result.envelope.status == 200
    assert metadata_result.envelope.status == 200
    assert len(code_pages) == 1
    assert len(layer_pages) == 1


@pytest.mark.asyncio
async def test_async_client_iter_raises_client_closed_error_when_closed_mid_iteration():
    client = AsyncBojClient(transport=PagedDummyAsyncTransport())
    iterator = client.timeseries.iter_data_code(DataCodeQuery(db="CO", code=["A"]))
    first = await anext(iterator)
    assert first.envelope.status == 200
    await client.close()
    with pytest.raises(BojClientClosedError):
        await anext(iterator)


@pytest.mark.asyncio
async def test_async_client_iter_data_code_closes_inner_iterator_when_closed_mid_iteration():
    service = _CloseAwareAsyncTimeSeriesService()
    client = AsyncBojClient(transport=DummyAsyncTransport(), timeseries_service=service)
    iterator = client.timeseries.iter_data_code(DataCodeQuery(db="CO", code=["A"]))

    await anext(iterator)
    await client.close()
    with pytest.raises(BojClientClosedError):
        await anext(iterator)
    assert service.code_closed is True


@pytest.mark.asyncio
async def test_async_client_iter_data_layer_closes_inner_iterator_when_closed_mid_iteration():
    service = _CloseAwareAsyncTimeSeriesService()
    client = AsyncBojClient(transport=DummyAsyncTransport(), timeseries_service=service)
    iterator = client.timeseries.iter_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="*"))

    await anext(iterator)
    await client.close()
    with pytest.raises(BojClientClosedError):
        await anext(iterator)
    assert service.layer_closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("config", "field", "expected"),
    [
        (
            BojClientConfig(timeseries=TimeSeriesConfig(enable_layer_auto_partition=True)),
            "enable_layer_auto_partition",
            True,
        ),
        (
            BojClientConfig(checkpoint=CheckpointConfig(enabled=False)),
            "checkpoint_enabled",
            False,
        ),
    ],
    ids=["layer-auto-partition", "checkpoint-disabled"],
)
async def test_async_client_config_flags_are_applied(
    config: BojClientConfig,
    field: str,
    expected: bool,
):
    transport = DummyAsyncTransport()
    async with AsyncBojClient(transport=transport, config=config) as client:
        if field == "enable_layer_auto_partition":
            assert client.timeseries._delegate._enable_layer_auto_partition is expected
            return
        assert client.timeseries._delegate._checkpoint_manager.enabled is expected


@pytest.mark.asyncio
async def test_async_client_passes_checkpoint_id_to_delegate_service():
    transport = DummyAsyncTransport()
    delegate = CheckpointAwareAsyncTimeSeriesService()
    async with AsyncBojClient(transport=transport, timeseries_service=delegate) as client:
        await client.timeseries.get_data_code(
            DataCodeQuery(db="CO", code=["A"]),
            checkpoint_id="cp-code",
        )
        await client.timeseries.get_data_layer(
            DataLayerQuery(db="MD10", frequency="Q", layer1="*"),
            checkpoint_id="cp-layer",
        )

    assert delegate.code_checkpoint_id == "cp-code"
    assert delegate.layer_checkpoint_id == "cp-layer"

