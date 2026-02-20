from __future__ import annotations

from boj_api_client.timeseries.strict import StrictTimeSeriesService
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from boj_api_client.timeseries.orchestrator import TimeSeriesService
from tests.shared.fixture_transports import SyncFixtureTransport


def test_pipeline_data_code_metadata_layer(fixture_loader):
    service = TimeSeriesService(StrictTimeSeriesService(SyncFixtureTransport(fixture_loader)))

    code_result = service.get_data_code(
        DataCodeQuery(
            db="CO",
            code=["TK99F1000601GCQ01000", "TK99F2000601GCQ01000"],
            start_date="202401",
            end_date="202504",
            lang="JP",
        )
    )
    assert len(code_result.series) == 2

    layer_result = service.get_data_layer(
        DataLayerQuery(db="md10", frequency="q", layer1="*", lang="en")
    )
    assert len(layer_result.series) > 0

    metadata_result = service.get_metadata(MetadataQuery(db="fm08", lang="jp"))
    assert len(metadata_result.entries) > 0

