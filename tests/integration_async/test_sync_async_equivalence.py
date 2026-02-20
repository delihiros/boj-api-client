from __future__ import annotations

import pytest

from boj_api_client.core.async_transport import AsyncTransport
from boj_api_client.core.errors import BojValidationError
from boj_api_client.core.transport import SyncTransport
from boj_api_client.timeseries.async_strict import AsyncStrictTimeSeriesService
from boj_api_client.timeseries.strict import StrictTimeSeriesService
from boj_api_client.timeseries.async_orchestrator import AsyncTimeSeriesService
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from boj_api_client.timeseries.orchestrator import TimeSeriesService
from tests.shared.fixture_transports import (
    AsyncErrorFixtureTransport,
    AsyncFixtureTransport,
    SyncErrorFixtureTransport,
    SyncFixtureTransport,
)
from tests.shared.transport import AsyncSequencedClient, Response, SyncSequencedClient, build_config


def _build_sync_async_services(fixture_loader):
    sync_service = TimeSeriesService(StrictTimeSeriesService(SyncFixtureTransport(fixture_loader)))
    async_service = AsyncTimeSeriesService(AsyncStrictTimeSeriesService(AsyncFixtureTransport(fixture_loader)))
    return sync_service, async_service


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    ["data_code", "data_layer", "metadata"],
    ids=["code", "layer", "metadata"],
)
async def test_sync_async_success_equivalence_with_fixtures(fixture_loader, scenario: str):
    sync_service, async_service = _build_sync_async_services(fixture_loader)

    if scenario == "data_code":
        query = DataCodeQuery(
            db="CO",
            code=["TK99F1000601GCQ01000", "TK99F2000601GCQ01000"],
            start_date="202401",
            end_date="202504",
            lang="JP",
        )
        sync_result = sync_service.get_data_code(query)
        async_result = await async_service.get_data_code(query)
        assert [s.series_code for s in sync_result.series] == [s.series_code for s in async_result.series]
        return

    if scenario == "data_layer":
        query = DataLayerQuery(db="MD10", frequency="Q", layer1="*")
        sync_result = sync_service.get_data_layer(query)
        async_result = await async_service.get_data_layer(query)
        assert [s.series_code for s in sync_result.series] == [s.series_code for s in async_result.series]
        return

    query = MetadataQuery(db="FM08")
    sync_result = sync_service.get_metadata(query)
    async_result = await async_service.get_metadata(query)
    assert [m.series_code for m in sync_result.entries] == [m.series_code for m in async_result.entries]


@pytest.mark.asyncio
async def test_sync_async_error_payload_equivalence_with_fixtures(fixture_loader):
    sync_service = TimeSeriesService(StrictTimeSeriesService(SyncErrorFixtureTransport(fixture_loader)))
    async_service = AsyncTimeSeriesService(AsyncStrictTimeSeriesService(AsyncErrorFixtureTransport(fixture_loader)))

    sync_result = sync_service.get_metadata(MetadataQuery(db="FM08"))
    async_result = await async_service.get_metadata(MetadataQuery(db="FM08"))
    assert sync_result.envelope.status == 400
    assert async_result.envelope.status == 400
    assert sync_result.envelope.message_id == async_result.envelope.message_id


@pytest.mark.asyncio
async def test_sync_async_iter_equivalence_with_fixtures(fixture_loader):
    sync_service, async_service = _build_sync_async_services(fixture_loader)

    sync_pages = list(sync_service.iter_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="*")))

    async_pages = []
    async for page in async_service.iter_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="*")):
        async_pages.append(page)

    assert len(sync_pages) == len(async_pages)
    assert [p.series[0].series_code for p in sync_pages] == [p.series[0].series_code for p in async_pages]


@pytest.mark.asyncio
async def test_sync_async_transport_error_equivalence():
    payload = {"STATUS": 400, "MESSAGEID": "X", "MESSAGE": "bad"}
    sync_transport = SyncTransport(build_config(), client=SyncSequencedClient([Response(200, payload)]))
    async_transport = AsyncTransport(
        build_config(),
        client=AsyncSequencedClient([Response(200, payload)]),
    )

    with pytest.raises(BojValidationError) as sync_exc:
        sync_transport.request("/getMetadata", params={"db": "FM08"})
    with pytest.raises(BojValidationError) as async_exc:
        await async_transport.request("/getMetadata", params={"db": "FM08"})

    assert sync_exc.value.status == async_exc.value.status
    assert sync_exc.value.message_id == async_exc.value.message_id

