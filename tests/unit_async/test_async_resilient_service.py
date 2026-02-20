from __future__ import annotations

import asyncio

import pytest

from boj_api_client.core.checkpoint_store import MemoryCheckpointStore
from boj_api_client.core.errors import BojPartialResultError, BojServerError, BojValidationError
from boj_api_client.timeseries.async_orchestrator import AsyncTimeSeriesService
from boj_api_client.timeseries.models import DataCodeResponse, DataLayerResponse
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from tests.shared.payloads import make_metadata_item, make_series_payload, make_success_payload


class _FakeAsyncStrict:
    def __init__(self):
        self.calls: list[tuple[str, int, int]] = []
        self.fail_after_first_chunk = False

    async def execute_data_code(self, query, *, code_subset, start_position):
        self.calls.append(("code", len(code_subset), start_position))
        if self.fail_after_first_chunk and len(self.calls) > 1:
            raise BojServerError("boom", status=500, cause="server_transient")
        return make_success_payload(resultset=[make_series_payload(code) for code in code_subset])

    async def execute_data_layer(self, query, *, start_position):
        return make_success_payload(resultset=[make_series_payload("X1")])

    async def execute_metadata(self, query):
        return {
            "STATUS": 200,
            "MESSAGEID": "M181000I",
            "MESSAGE": "ok",
            "DATE": "2026-01-01T00:00:00+09:00",
            "DB": query.db,
            "RESULTSET": [
                {
                    "SERIES_CODE": "X",
                    "NAME_OF_TIME_SERIES_J": "j",
                    "NAME_OF_TIME_SERIES": "e",
                }
            ],
        }


@pytest.mark.asyncio
async def test_async_resilient_get_data_code_auto_split_and_order():
    strict = _FakeAsyncStrict()
    service = AsyncTimeSeriesService(strict)
    codes = [f"C{i:03d}" for i in range(251)]
    response = await service.get_data_code(DataCodeQuery(db="CO", code=codes))
    assert [s.series_code for s in response.series] == codes
    assert len(strict.calls) == 2


@pytest.mark.asyncio
async def test_async_resilient_returns_partial_result_on_mid_failure():
    strict = _FakeAsyncStrict()
    strict.fail_after_first_chunk = True
    service = AsyncTimeSeriesService(strict)
    codes = [f"C{i:03d}" for i in range(251)]
    with pytest.raises(BojPartialResultError) as exc:
        await service.get_data_code(DataCodeQuery(db="CO", code=codes))
    assert len(exc.value.partial_result.series) == 250
    assert exc.value.cause == "server_transient"


@pytest.mark.asyncio
async def test_async_resilient_iter_methods():
    service = AsyncTimeSeriesService(_FakeAsyncStrict())

    code_pages = []
    async for page in service.iter_data_code(DataCodeQuery(db="CO", code=["A"])):
        code_pages.append(page)
    assert len(code_pages) == 1
    assert isinstance(code_pages[0], DataCodeResponse)

    layer_pages = []
    async for page in service.iter_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="*")):
        layer_pages.append(page)
    assert len(layer_pages) == 1
    assert isinstance(layer_pages[0], DataLayerResponse)

    metadata = await service.get_metadata(MetadataQuery(db="FM08"))
    assert metadata.entries and metadata.entries[0].series_code == "X"


@pytest.mark.asyncio
async def test_async_resilient_iter_data_code_can_be_cancelled():
    class _SlowStrict(_FakeAsyncStrict):
        async def execute_data_code(self, query, *, code_subset, start_position):
            await asyncio.sleep(10.0)
            return await super().execute_data_code(
                query,
                code_subset=code_subset,
                start_position=start_position,
            )

    service = AsyncTimeSeriesService(_SlowStrict())
    iterator = service.iter_data_code(DataCodeQuery(db="CO", code=["A"]))
    task = asyncio.create_task(anext(iterator))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await iterator.aclose()


@pytest.mark.asyncio
async def test_async_resilient_validation_error_is_not_wrapped():
    class _ValidationFailStrict(_FakeAsyncStrict):
        async def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            if len(self.calls) > 1:
                raise BojValidationError("invalid")
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(code) for code in code_subset],
            }

    service = AsyncTimeSeriesService(_ValidationFailStrict())
    codes = [f"C{i:03d}" for i in range(251)]
    with pytest.raises(BojValidationError):
        await service.get_data_code(DataCodeQuery(db="CO", code=codes))


@pytest.mark.asyncio
async def test_async_resilient_data_code_resume_from_checkpoint_after_partial_failure():
    class _ResumeStrict(_FakeAsyncStrict):
        def __init__(self):
            super().__init__()
            self.failed_once = False

        async def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            if len(code_subset) == 1 and not self.failed_once:
                self.failed_once = True
                raise BojServerError("boom", status=500, cause="server_transient")
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(code) for code in code_subset],
            }

    strict = _ResumeStrict()
    service = AsyncTimeSeriesService(strict, checkpoint_store=MemoryCheckpointStore())
    codes = [f"C{i:03d}" for i in range(251)]
    query = DataCodeQuery(db="CO", code=codes)

    with pytest.raises(BojPartialResultError) as exc:
        await service.get_data_code(query)

    assert exc.value.checkpoint_id is not None
    resumed = await service.get_data_code(query, checkpoint_id=exc.value.checkpoint_id)
    assert [series.series_code for series in resumed.series] == codes


@pytest.mark.asyncio
async def test_async_resilient_data_layer_resume_from_checkpoint_after_partial_failure():
    class _LayerResumeStrict(_FakeAsyncStrict):
        def __init__(self):
            super().__init__()
            self.failed_once = False
            self.layer_calls: list[int] = []

        async def execute_data_layer(self, query, *, start_position):
            self.layer_calls.append(start_position)
            if start_position == 1:
                return {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": 2,
                    "RESULTSET": [make_series_payload("X1")],
                }
            if start_position == 2 and not self.failed_once:
                self.failed_once = True
                raise BojServerError("boom", status=500, cause="server_transient")
            if start_position == 2:
                return {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": "",
                    "RESULTSET": [make_series_payload("X2")],
                }
            raise AssertionError(f"unexpected start_position={start_position}")

    strict = _LayerResumeStrict()
    service = AsyncTimeSeriesService(strict, checkpoint_store=MemoryCheckpointStore())
    query = DataLayerQuery(db="MD10", frequency="Q", layer1="*")

    with pytest.raises(BojPartialResultError) as exc:
        await service.get_data_layer(query)

    assert exc.value.checkpoint_id is not None
    resumed = await service.get_data_layer(query, checkpoint_id=exc.value.checkpoint_id)
    assert [series.series_code for series in resumed.series] == ["X1", "X2"]


@pytest.mark.asyncio
async def test_async_resilient_data_layer_resume_rejects_when_store_disabled():
    service = AsyncTimeSeriesService(_FakeAsyncStrict())
    with pytest.raises(BojValidationError, match="checkpoint is disabled"):
        await service.get_data_layer(
            DataLayerQuery(db="MD10", frequency="Q", layer1="*"),
            checkpoint_id="any",
        )


@pytest.mark.asyncio
async def test_async_resilient_data_code_resume_rejects_mismatched_config_snapshot():
    class _ResumeStrict(_FakeAsyncStrict):
        def __init__(self):
            super().__init__()
            self.failed_once = False

        async def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            if len(code_subset) == 1 and not self.failed_once:
                self.failed_once = True
                raise BojServerError("boom", status=500, cause="server_transient")
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(code) for code in code_subset],
            }

    strict = _ResumeStrict()
    store = MemoryCheckpointStore()
    query = DataCodeQuery(db="CO", code=[f"C{i:03d}" for i in range(251)])
    service = AsyncTimeSeriesService(
        strict,
        checkpoint_store=store,
        config_snapshot={"max_attempts": 5},
    )

    with pytest.raises(BojPartialResultError) as exc:
        await service.get_data_code(query)
    assert exc.value.checkpoint_id is not None

    resumed = AsyncTimeSeriesService(
        strict,
        checkpoint_store=store,
        config_snapshot={"max_attempts": 3},
    )
    with pytest.raises(BojValidationError, match="checkpoint config mismatch"):
        await resumed.get_data_code(query, checkpoint_id=exc.value.checkpoint_id)


@pytest.mark.asyncio
async def test_async_resilient_layer_auto_partition_via_metadata_when_enabled():
    class _AutoPartitionStrict(_FakeAsyncStrict):
        def __init__(self):
            super().__init__()
            self.layer_calls = 0
            self.metadata_calls = 0

        async def execute_data_layer(self, query, *, start_position):
            self.layer_calls += 1
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(f"S{i}") for i in range(1251)],
            }

        async def execute_metadata(self, query):
            self.metadata_calls += 1
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "DB": query.db,
                "RESULTSET": [
                    make_metadata_item("S_A1", frequency="Q", layer1="A1"),
                    make_metadata_item("S_A2", frequency="Q", layer1="A2"),
                    make_metadata_item("S_B1", frequency="Q", layer1="B1"),
                    make_metadata_item("S_AM", frequency="M", layer1="A3"),
                ],
            }

        async def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(code) for code in code_subset],
            }

    strict = _AutoPartitionStrict()
    service = AsyncTimeSeriesService(strict, enable_layer_auto_partition=True)
    result = await service.get_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="A*"))

    assert [series.series_code for series in result.series] == ["S_A1", "S_A2"]
    assert strict.layer_calls == 1
    assert strict.metadata_calls == 1
    assert strict.calls == [("code", 2, 1)]


@pytest.mark.asyncio
async def test_async_resilient_layer_auto_partition_returns_empty_when_no_matching_series():
    class _NoMatchStrict(_FakeAsyncStrict):
        async def execute_data_layer(self, query, *, start_position):
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(f"S{i}") for i in range(1251)],
            }

        async def execute_metadata(self, query):
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "DB": query.db,
                "RESULTSET": [make_metadata_item("S_B1", frequency="Q", layer1="B1")],
            }

        async def execute_data_code(self, query, *, code_subset, start_position):
            raise AssertionError("execute_data_code must not be called when no series matched")

    service = AsyncTimeSeriesService(_NoMatchStrict(), enable_layer_auto_partition=True)
    result = await service.get_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="A*"))
    assert result.series == ()


@pytest.mark.asyncio
async def test_async_resilient_iter_data_code_closes_inner_page_iterator(monkeypatch):
    class _CloseAwarePageIter:
        def __init__(self):
            self._items = [
                {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": 2,
                    "RESULTSET": [make_series_payload("A")],
                },
                {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": "",
                    "RESULTSET": [make_series_payload("A")],
                },
            ]
            self.closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._items:
                raise StopAsyncIteration
            return self._items.pop(0)

        async def aclose(self):
            self.closed = True

    page_iter = _CloseAwarePageIter()

    def _fake_aiterate_pages(fetch_page, *, start_position=1, max_pages=10_000):
        return page_iter

    monkeypatch.setattr("boj_api_client.timeseries.async_orchestrator.aiterate_pages", _fake_aiterate_pages)

    service = AsyncTimeSeriesService(_FakeAsyncStrict())
    iterator = service.iter_data_code(DataCodeQuery(db="CO", code=["A"]))

    await anext(iterator)
    await iterator.aclose()

    assert page_iter.closed is True


@pytest.mark.asyncio
async def test_async_resilient_iter_data_layer_closes_inner_page_iterator(monkeypatch):
    class _CloseAwarePageIter:
        def __init__(self):
            self._items = [
                {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": 2,
                    "RESULTSET": [make_series_payload("X1")],
                },
                {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": "",
                    "RESULTSET": [make_series_payload("X1")],
                },
            ]
            self.closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._items:
                raise StopAsyncIteration
            return self._items.pop(0)

        async def aclose(self):
            self.closed = True

    page_iter = _CloseAwarePageIter()

    def _fake_aiterate_pages(fetch_page, *, start_position=1, max_pages=10_000):
        return page_iter

    monkeypatch.setattr("boj_api_client.timeseries.async_orchestrator.aiterate_pages", _fake_aiterate_pages)

    service = AsyncTimeSeriesService(_FakeAsyncStrict())
    iterator = service.iter_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="*"))

    await anext(iterator)
    await iterator.aclose()

    assert page_iter.closed is True


