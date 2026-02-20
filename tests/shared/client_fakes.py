from __future__ import annotations

from boj_api_client.timeseries.models import (
    DataCodeResponse,
    DataLayerResponse,
    MetadataResponse,
    make_success_envelope,
)
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from tests.shared.payloads import make_series_payload, make_success_payload


class DummyTransport:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True

    def request(self, endpoint: str, *, params: dict[str, str]):
        return make_success_payload()


class PagedDummyTransport(DummyTransport):
    def request(self, endpoint: str, *, params: dict[str, str]):
        start = str(params.get("startPosition", "1"))
        if start == "1":
            return make_success_payload(
                next_position=2,
                resultset=[make_series_payload("A", points=[(202401, 1)])],
            )
        return make_success_payload(resultset=[make_series_payload("A", points=[(202402, 2)])])


class DummyAsyncTransport:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True

    async def request(self, endpoint: str, *, params: dict[str, str]):
        return make_success_payload()


class PagedDummyAsyncTransport(DummyAsyncTransport):
    async def request(self, endpoint: str, *, params: dict[str, str]):
        start = str(params.get("startPosition", "1"))
        if start == "1":
            return make_success_payload(
                next_position=2,
                resultset=[make_series_payload("A", points=[(202401, 1)])],
            )
        return make_success_payload(resultset=[make_series_payload("A", points=[(202402, 2)])])


class CheckpointAwareTimeSeriesService:
    def __init__(self):
        self.code_checkpoint_id: str | None = None
        self.layer_checkpoint_id: str | None = None

    def get_data_code(self, query: DataCodeQuery, *, checkpoint_id: str | None = None):
        self.code_checkpoint_id = checkpoint_id
        return DataCodeResponse(envelope=make_success_envelope(), series=[])

    def get_data_layer(self, query: DataLayerQuery, *, checkpoint_id: str | None = None):
        self.layer_checkpoint_id = checkpoint_id
        return DataLayerResponse(envelope=make_success_envelope(), series=[], next_position=None)

    def get_metadata(self, query: MetadataQuery):
        return MetadataResponse(envelope=make_success_envelope(), entries=[])

    def iter_data_code(self, query: DataCodeQuery):
        if False:
            yield None

    def iter_data_layer(self, query: DataLayerQuery):
        if False:
            yield None


class CheckpointAwareAsyncTimeSeriesService:
    def __init__(self):
        self.code_checkpoint_id: str | None = None
        self.layer_checkpoint_id: str | None = None

    async def get_data_code(
        self,
        query: DataCodeQuery,
        *,
        checkpoint_id: str | None = None,
    ) -> DataCodeResponse:
        self.code_checkpoint_id = checkpoint_id
        return DataCodeResponse(envelope=make_success_envelope(), series=[])

    async def get_data_layer(
        self,
        query: DataLayerQuery,
        *,
        checkpoint_id: str | None = None,
    ) -> DataLayerResponse:
        self.layer_checkpoint_id = checkpoint_id
        return DataLayerResponse(envelope=make_success_envelope(), series=[], next_position=None)

    async def get_metadata(self, query: MetadataQuery) -> MetadataResponse:
        return MetadataResponse(envelope=make_success_envelope(), entries=[])

    async def iter_data_code(self, query: DataCodeQuery):
        if False:
            yield None

    async def iter_data_layer(self, query: DataLayerQuery):
        if False:
            yield None


