from __future__ import annotations

import pytest

from boj_api_client.core.checkpoint_store import MemoryCheckpointStore
from boj_api_client.core.errors import BojPartialResultError, BojServerError, BojValidationError
from boj_api_client.timeseries.models import DataCodeResponse, DataLayerResponse
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from boj_api_client.timeseries.orchestrator import TimeSeriesService
from tests.shared.payloads import make_metadata_item, make_series_payload, make_success_payload


class _FakeStrict:
    def __init__(self):
        self.fail_after_first_chunk = False
        self.calls: list[tuple[str, int, int]] = []

    def execute_data_code(self, query, *, code_subset, start_position):
        self.calls.append(("code", len(code_subset), start_position))
        if self.fail_after_first_chunk and len(self.calls) > 1:
            raise BojServerError("boom", status=500, cause="server_transient")
        return make_success_payload(resultset=[make_series_payload(code) for code in code_subset])

    def execute_data_layer(self, query, *, start_position):
        if start_position > 1:
            raise BojValidationError("too many", status=400)
        return make_success_payload(resultset=[make_series_payload("X1")])

    def execute_metadata(self, query):
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


def test_resilient_auto_split_code_over_250_and_keep_input_order():
    strict = _FakeStrict()
    service = TimeSeriesService(strict)
    codes = [f"C{i:03d}" for i in range(251)]
    response = service.get_data_code(DataCodeQuery(db="CO", code=codes))
    assert [s.series_code for s in response.series] == codes
    # first chunk(250) + second chunk(1)
    assert len(strict.calls) == 2


def test_resilient_returns_partial_result_on_mid_failure():
    strict = _FakeStrict()
    strict.fail_after_first_chunk = True
    service = TimeSeriesService(strict)
    codes = [f"C{i:03d}" for i in range(251)]
    with pytest.raises(BojPartialResultError) as exc:
        service.get_data_code(DataCodeQuery(db="CO", code=codes))
    partial = exc.value.partial_result
    assert len(partial.series) == 250
    assert exc.value.cause == "server_transient"


def test_resilient_metadata_success():
    service = TimeSeriesService(_FakeStrict())
    result = service.get_metadata(MetadataQuery(db="FM08"))
    assert result.entries and result.entries[0].series_code == "X"


def test_resilient_layer_success_minimum():
    service = TimeSeriesService(_FakeStrict())
    result = service.get_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="*"))
    assert result.series and result.series[0].series_code == "X1"


def test_resilient_layer_raises_when_series_exceeds_1250():
    class _ManyStrict(_FakeStrict):
        def execute_data_layer(self, query, *, start_position):
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(f"S{i}") for i in range(1251)],
            }

    service = TimeSeriesService(_ManyStrict())
    with pytest.raises(BojValidationError):
        service.get_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="*"))


def test_resilient_layer_auto_partition_via_metadata_when_enabled():
    class _AutoPartitionStrict(_FakeStrict):
        def __init__(self):
            super().__init__()
            self.layer_calls = 0
            self.metadata_calls = 0

        def execute_data_layer(self, query, *, start_position):
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

        def execute_metadata(self, query):
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

        def execute_data_code(self, query, *, code_subset, start_position):
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
    service = TimeSeriesService(strict, enable_layer_auto_partition=True)
    result = service.get_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="A*"))

    assert [series.series_code for series in result.series] == ["S_A1", "S_A2"]
    assert strict.layer_calls == 1
    assert strict.metadata_calls == 1
    assert strict.calls == [("code", 2, 1)]


def test_resilient_layer_auto_partition_returns_empty_when_no_matching_series():
    class _NoMatchStrict(_FakeStrict):
        def execute_data_layer(self, query, *, start_position):
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(f"S{i}") for i in range(1251)],
            }

        def execute_metadata(self, query):
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "DB": query.db,
                "RESULTSET": [make_metadata_item("S_B1", frequency="Q", layer1="B1")],
            }

        def execute_data_code(self, query, *, code_subset, start_position):
            raise AssertionError("execute_data_code must not be called when no series matched")

    service = TimeSeriesService(_NoMatchStrict(), enable_layer_auto_partition=True)
    result = service.get_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="A*"))
    assert result.series == ()


def test_resilient_merges_same_series_across_pages():
    class _PagedStrict(_FakeStrict):
        def execute_data_code(self, query, *, code_subset, start_position):
            if start_position == 1:
                return {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": 2,
                    "RESULTSET": [make_series_payload(code_subset[0], points=[(202401, 1)])],
                }
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(code_subset[0], points=[(202402, 2)])],
            }

    service = TimeSeriesService(_PagedStrict())
    response = service.get_data_code(DataCodeQuery(db="CO", code=["C001"]))
    assert len(response.series) == 1
    assert [point.survey_date for point in response.series[0].points] == ["202401", "202402"]


def test_resilient_validation_error_is_not_wrapped():
    class _ValidationFailStrict(_FakeStrict):
        def execute_data_code(self, query, *, code_subset, start_position):
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

    service = TimeSeriesService(_ValidationFailStrict())
    codes = [f"C{i:03d}" for i in range(251)]
    with pytest.raises(BojValidationError):
        service.get_data_code(DataCodeQuery(db="CO", code=codes))


def test_resilient_partial_result_cause_network():
    class _NetworkFailStrict(_FakeStrict):
        def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            if len(self.calls) > 1:
                raise RuntimeError("network down")
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(code) for code in code_subset],
            }

    service = TimeSeriesService(_NetworkFailStrict())
    codes = [f"C{i:03d}" for i in range(251)]
    with pytest.raises(BojPartialResultError) as exc:
        service.get_data_code(DataCodeQuery(db="CO", code=codes))
    assert exc.value.cause == "network"


def test_resilient_data_code_resume_from_checkpoint_after_partial_failure():
    class _ResumeStrict(_FakeStrict):
        def __init__(self):
            super().__init__()
            self.failed_once = False

        def execute_data_code(self, query, *, code_subset, start_position):
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
    service = TimeSeriesService(strict, checkpoint_store=store)
    codes = [f"C{i:03d}" for i in range(251)]
    query = DataCodeQuery(db="CO", code=codes)

    with pytest.raises(BojPartialResultError) as exc:
        service.get_data_code(query)

    assert len(exc.value.partial_result.series) == 250
    assert exc.value.checkpoint_id is not None
    checkpoint_id = exc.value.checkpoint_id

    resumed = service.get_data_code(query, checkpoint_id=checkpoint_id)
    assert [s.series_code for s in resumed.series] == codes
    assert strict.calls[-1] == ("code", 1, 1)


def test_resilient_data_code_resume_rejects_mismatched_query():
    class _ResumeStrict(_FakeStrict):
        def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            if len(code_subset) == 1:
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
    service = TimeSeriesService(strict, checkpoint_store=MemoryCheckpointStore())
    good_query = DataCodeQuery(db="CO", code=[f"C{i:03d}" for i in range(251)])
    bad_query = DataCodeQuery(db="CO", code=[f"D{i:03d}" for i in range(251)])

    with pytest.raises(BojPartialResultError) as exc:
        service.get_data_code(good_query)

    with pytest.raises(BojValidationError, match="checkpoint query mismatch"):
        service.get_data_code(bad_query, checkpoint_id=exc.value.checkpoint_id)


def test_resilient_data_code_resume_rejects_mismatched_config_snapshot():
    class _ResumeStrict(_FakeStrict):
        def __init__(self):
            super().__init__()
            self.failed_once = False

        def execute_data_code(self, query, *, code_subset, start_position):
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
    service = TimeSeriesService(
        strict,
        checkpoint_store=store,
        config_snapshot={"max_attempts": 5},
    )

    with pytest.raises(BojPartialResultError) as exc:
        service.get_data_code(query)
    assert exc.value.checkpoint_id is not None

    resumed = TimeSeriesService(
        strict,
        checkpoint_store=store,
        config_snapshot={"max_attempts": 3},
    )
    with pytest.raises(BojValidationError, match="checkpoint config mismatch"):
        resumed.get_data_code(query, checkpoint_id=exc.value.checkpoint_id)


def test_resilient_data_code_resume_rejects_when_store_disabled():
    service = TimeSeriesService(_FakeStrict())
    with pytest.raises(BojValidationError, match="checkpoint is disabled"):
        service.get_data_code(
            DataCodeQuery(db="CO", code=["A"]),
            checkpoint_id="any",
        )


def test_resilient_data_layer_resume_from_checkpoint_after_partial_failure():
    class _LayerResumeStrict(_FakeStrict):
        def __init__(self):
            super().__init__()
            self.failed_once = False
            self.layer_calls: list[int] = []

        def execute_data_layer(self, query, *, start_position):
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
    service = TimeSeriesService(strict, checkpoint_store=MemoryCheckpointStore())
    query = DataLayerQuery(db="MD10", frequency="Q", layer1="*")

    with pytest.raises(BojPartialResultError) as exc:
        service.get_data_layer(query)

    assert exc.value.checkpoint_id is not None
    assert [s.series_code for s in exc.value.partial_result.series] == ["X1"]
    resumed = service.get_data_layer(query, checkpoint_id=exc.value.checkpoint_id)
    assert [s.series_code for s in resumed.series] == ["X1", "X2"]
    assert strict.layer_calls[-1] == 2


def test_resilient_data_layer_auto_partition_resume_from_checkpoint():
    class _LayerAutoPartitionResumeStrict(_FakeStrict):
        def __init__(self):
            super().__init__()
            self.failed_once = False

        def execute_data_layer(self, query, *, start_position):
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(f"S{i}") for i in range(1251)],
            }

        def execute_metadata(self, query):
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "DB": query.db,
                "RESULTSET": [
                    make_metadata_item("S_A1", frequency="Q", layer1="A1"),
                    make_metadata_item("S_A2", frequency="Q", layer1="A2"),
                ],
            }

        def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            if start_position == 1:
                return {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": 2,
                    "RESULTSET": [make_series_payload("S_A1")],
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
                    "RESULTSET": [make_series_payload("S_A2")],
                }
            raise AssertionError(f"unexpected start_position={start_position}")

    strict = _LayerAutoPartitionResumeStrict()
    service = TimeSeriesService(
        strict,
        enable_layer_auto_partition=True,
        checkpoint_store=MemoryCheckpointStore(),
    )
    query = DataLayerQuery(db="MD10", frequency="Q", layer1="A*")

    with pytest.raises(BojPartialResultError) as exc:
        service.get_data_layer(query)

    assert exc.value.checkpoint_id is not None
    assert [s.series_code for s in exc.value.partial_result.series] == ["S_A1"]

    resumed = service.get_data_layer(query, checkpoint_id=exc.value.checkpoint_id)
    assert [s.series_code for s in resumed.series] == ["S_A1", "S_A2"]


def test_resilient_data_layer_resume_rejects_when_store_disabled():
    service = TimeSeriesService(_FakeStrict())
    with pytest.raises(BojValidationError, match="checkpoint is disabled"):
        service.get_data_layer(
            DataLayerQuery(db="MD10", frequency="Q", layer1="*"),
            checkpoint_id="any",
        )


def test_resilient_data_code_resume_from_second_page_in_single_chunk():
    class _SingleChunkPagedStrict(_FakeStrict):
        def __init__(self):
            super().__init__()
            self.fail_once_on_second_page = True

        def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            if start_position == 1:
                return {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": 2,
                    "RESULTSET": [make_series_payload("C001", points=[(202401, 1)])],
                }
            if start_position == 2 and self.fail_once_on_second_page:
                self.fail_once_on_second_page = False
                raise BojServerError("boom", status=500, cause="server_transient")
            if start_position == 2:
                return {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": "",
                    "RESULTSET": [make_series_payload("C001", points=[(202402, 2)])],
                }
            raise AssertionError(f"unexpected start_position={start_position}")

    strict = _SingleChunkPagedStrict()
    service = TimeSeriesService(strict, checkpoint_store=MemoryCheckpointStore())
    query = DataCodeQuery(db="CO", code=["C001"])

    with pytest.raises(BojPartialResultError) as exc:
        service.get_data_code(query)
    assert exc.value.checkpoint_id is not None
    partial = exc.value.partial_result
    assert [point.survey_date for point in partial.series[0].points] == ["202401"]

    resumed = service.get_data_code(query, checkpoint_id=exc.value.checkpoint_id)
    assert len(resumed.series) == 1
    assert [point.survey_date for point in resumed.series[0].points] == ["202401", "202402"]
    assert strict.calls[-1] == ("code", 1, 2)


def test_resilient_does_not_persist_checkpoint_when_no_partial_series():
    class _FailFirstPageStrict(_FakeStrict):
        def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            raise BojServerError("boom", status=500, cause="server_transient")

    store = MemoryCheckpointStore()
    service = TimeSeriesService(_FailFirstPageStrict(), checkpoint_store=store)

    with pytest.raises(BojServerError):
        service.get_data_code(DataCodeQuery(db="CO", code=["A"]))

    # no partial progress => no checkpoint should be persisted
    assert store._items == {}


def test_resilient_iter_data_code_yields_page_responses():
    class _PagedStrict(_FakeStrict):
        def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            if start_position == 1:
                return {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": 2,
                    "RESULTSET": [make_series_payload(code_subset[0], points=[(202401, 1)])],
                }
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(code_subset[0], points=[(202402, 2)])],
            }

    strict = _PagedStrict()
    service = TimeSeriesService(strict)
    pages = list(service.iter_data_code(DataCodeQuery(db="CO", code=["C001"])))
    assert len(pages) == 2
    assert all(isinstance(page, DataCodeResponse) for page in pages)
    assert strict.calls == [("code", 1, 1), ("code", 1, 2)]


def test_resilient_iter_data_code_early_break_does_not_fetch_next_page():
    class _PagedStrict(_FakeStrict):
        def execute_data_code(self, query, *, code_subset, start_position):
            self.calls.append(("code", len(code_subset), start_position))
            if start_position == 1:
                return {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": 2,
                    "RESULTSET": [make_series_payload(code_subset[0], points=[(202401, 1)])],
                }
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload(code_subset[0], points=[(202402, 2)])],
            }

    strict = _PagedStrict()
    service = TimeSeriesService(strict)
    iterator = service.iter_data_code(DataCodeQuery(db="CO", code=["C001"]))
    first = next(iterator)
    assert isinstance(first, DataCodeResponse)
    assert strict.calls == [("code", 1, 1)]


def test_resilient_iter_data_layer_yields_page_responses():
    class _PagedLayerStrict(_FakeStrict):
        def execute_data_layer(self, query, *, start_position):
            if start_position == 1:
                return {
                    "STATUS": 200,
                    "MESSAGEID": "M181000I",
                    "MESSAGE": "ok",
                    "DATE": "2026-01-01T00:00:00+09:00",
                    "PARAMETER": {},
                    "NEXTPOSITION": 3,
                    "RESULTSET": [make_series_payload("X1")],
                }
            return {
                "STATUS": 200,
                "MESSAGEID": "M181000I",
                "MESSAGE": "ok",
                "DATE": "2026-01-01T00:00:00+09:00",
                "PARAMETER": {},
                "NEXTPOSITION": "",
                "RESULTSET": [make_series_payload("X2")],
            }

    service = TimeSeriesService(_PagedLayerStrict())
    pages = list(service.iter_data_layer(DataLayerQuery(db="MD10", frequency="Q", layer1="*")))
    assert len(pages) == 2
    assert all(isinstance(page, DataLayerResponse) for page in pages)
    assert [page.series[0].series_code for page in pages] == ["X1", "X2"]




