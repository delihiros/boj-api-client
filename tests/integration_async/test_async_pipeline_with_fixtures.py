from __future__ import annotations

import pytest

from boj_api_client.timeseries.async_strict import AsyncStrictTimeSeriesService
from boj_api_client.timeseries.async_orchestrator import AsyncTimeSeriesService
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from tests.shared.fixture_transports import AsyncFixtureTransport


@pytest.mark.asyncio
async def test_async_pipeline_data_code_metadata_layer(fixture_loader):
    service = AsyncTimeSeriesService(AsyncStrictTimeSeriesService(AsyncFixtureTransport(fixture_loader)))

    code_result = await service.get_data_code(
        DataCodeQuery(
            db="CO",
            code=["TK99F1000601GCQ01000", "TK99F2000601GCQ01000"],
            start_date="202401",
            end_date="202504",
            lang="JP",
        )
    )
    assert len(code_result.series) == 2

    layer_result = await service.get_data_layer(
        DataLayerQuery(db="md10", frequency="q", layer1="*", lang="en")
    )
    assert len(layer_result.series) > 0

    metadata_result = await service.get_metadata(MetadataQuery(db="fm08", lang="jp"))
    assert len(metadata_result.entries) > 0

