from __future__ import annotations

import pytest

from boj_api_client.client import BojClient
from boj_api_client.config import BojClientConfig, CheckpointConfig, TimeSeriesConfig
from boj_api_client.core.errors import BojClientClosedError
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from tests.shared.client_fakes import (
    CheckpointAwareTimeSeriesService,
    DummyTransport,
    PagedDummyTransport,
)


def test_client_context_manager_closes_transport():
    transport = DummyTransport()
    with BojClient(transport=transport) as client:
        assert client is not None
    assert transport.closed is True


def test_client_raises_when_used_after_close():
    transport = DummyTransport()
    client = BojClient(transport=transport)
    client.close()
    with pytest.raises(BojClientClosedError):
        client.timeseries.get_data_code(DataCodeQuery(db="CO", code=["A"]))


def test_client_delegates_all_timeseries_methods():
    transport = DummyTransport()
    with BojClient(transport=transport) as client:
        code_result = client.timeseries.get_data_code(DataCodeQuery(db="CO", code=["A"]))
        layer_result = client.timeseries.get_data_layer(
            DataLayerQuery(db="MD10", frequency="Q", layer1="*")
        )
        metadata_result = client.timeseries.get_metadata(MetadataQuery(db="FM08"))

    assert code_result.envelope.status == 200
    assert layer_result.envelope.status == 200
    assert metadata_result.envelope.status == 200


def test_client_exposes_iter_methods():
    transport = DummyTransport()
    with BojClient(transport=transport) as client:
        code_pages = list(client.timeseries.iter_data_code(DataCodeQuery(db="CO", code=["A"])))
        layer_pages = list(
            client.timeseries.iter_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="*"))
        )
    assert len(code_pages) == 1
    assert len(layer_pages) == 1


def test_client_iter_raises_client_closed_error_when_closed_mid_iteration():
    client = BojClient(transport=PagedDummyTransport())
    iterator = client.timeseries.iter_data_code(DataCodeQuery(db="CO", code=["A"]))
    first = next(iterator)
    assert first.envelope.status == 200
    client.close()
    with pytest.raises(BojClientClosedError):
        next(iterator)


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
def test_client_config_flags_are_applied(
    config: BojClientConfig,
    field: str,
    expected: bool,
):
    transport = DummyTransport()
    with BojClient(transport=transport, config=config) as client:
        if field == "enable_layer_auto_partition":
            assert client.timeseries._delegate._enable_layer_auto_partition is expected
            return
        assert client.timeseries._delegate._checkpoint_manager.enabled is expected


def test_client_passes_checkpoint_id_to_delegate_service():
    transport = DummyTransport()
    delegate = CheckpointAwareTimeSeriesService()
    with BojClient(transport=transport, timeseries_service=delegate) as client:
        client.timeseries.get_data_code(DataCodeQuery(db="CO", code=["A"]), checkpoint_id="cp-code")
        client.timeseries.get_data_layer(
            DataLayerQuery(db="MD10", frequency="Q", layer1="*"),
            checkpoint_id="cp-layer",
        )

    assert delegate.code_checkpoint_id == "cp-code"
    assert delegate.layer_checkpoint_id == "cp-layer"

