# AUTO-GENERATED FROM async_orchestrator.py. DO NOT EDIT.

"""Public sync orchestration for timeseries APIs."""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping

from ..core.pagination import iterate_pages
from ..core.checkpoint_store import CheckpointStore
from ..core.errors import BojPartialResultError, BojValidationError
from .aggregation import (
    build_data_code_response,
    build_data_layer_response_from_map,
    build_data_layer_response_from_series,
    cause_from_error,
    merge_series_map,
)
from .checkpoint_manager import CheckpointManager
from .checkpoint_models import (
    DataCodeCheckpointState,
    DataLayerAutoPartitionCheckpointState,
    DataLayerDirectCheckpointState,
)
from .strict import StrictTimeSeriesService
from .planner import chunk_codes, next_position_or_raise, plan_data_code_chunks, should_use_auto_partition
from .selectors import select_metadata_series_codes
from .models import DataCodeResponse, DataLayerResponse, MetadataResponse, TimeSeries, make_success_envelope
from .parser import parse_data_code_response, parse_data_layer_response, parse_metadata_response
from .queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from .validators import normalize_data_code_query, normalize_data_layer_query, normalize_metadata_query

logger = logging.getLogger("boj_api_client")


class TimeSeriesService:
    """Public resilient facade."""

    def __init__(
        self,
        strict_service: StrictTimeSeriesService,
        *,
        enable_layer_auto_partition: bool = False,
        checkpoint_store: CheckpointStore | None = None,
        config_snapshot: Mapping[str, int | float | bool] | None = None,
    ) -> None:
        self._strict = strict_service
        self._enable_layer_auto_partition = enable_layer_auto_partition
        self._checkpoint_manager = CheckpointManager(
            store=checkpoint_store,
            config_snapshot=config_snapshot,
        )

    def iter_data_code(self, query: DataCodeQuery) -> Iterator[DataCodeResponse]:
        normalized = normalize_data_code_query(query)
        for chunk_plan in plan_data_code_chunks(codes=normalized.code, chunk_size=250):
            page_iter = iterate_pages(
                lambda start_pos, _chunk=chunk_plan.codes: self._strict.execute_data_code(
                    normalized,
                    code_subset=_chunk,
                    start_position=start_pos,
                ),
                start_position=chunk_plan.start_position,
            )
            try:
                for payload in page_iter:
                    yield parse_data_code_response(payload)
            finally:
                page_iter.close()

    def iter_data_layer(self, query: DataLayerQuery) -> Iterator[DataLayerResponse]:
        normalized = normalize_data_layer_query(query)
        page_iter = iterate_pages(
            lambda start_pos, _normalized=normalized: self._strict.execute_data_layer(
                _normalized,
                start_position=start_pos,
            ),
            start_position=1,
        )
        try:
            for payload in page_iter:
                yield parse_data_layer_response(payload)
        finally:
            page_iter.close()

    def get_data_code(
        self,
        query: DataCodeQuery,
        *,
        checkpoint_id: str | None = None,
    ) -> DataCodeResponse:
        normalized = normalize_data_code_query(query)
        code_chunks = chunk_codes(normalized.code, chunk_size=250)
        logger.info(
            "data_code start db=%s total_codes=%s chunks=%s",
            normalized.db,
            len(normalized.code),
            len(code_chunks),
        )

        by_code: dict[str, TimeSeries] = {}
        last_envelope = make_success_envelope()
        resume_chunk_index = 0
        resume_start_position = 1

        if checkpoint_id is not None:
            state = self._checkpoint_manager.load_data_code(
                checkpoint_id=checkpoint_id,
                normalized=normalized,
            )
            by_code = dict(state.by_code)
            last_envelope = state.last_envelope
            resume_chunk_index = state.chunk_index
            resume_start_position = state.start_position

        for chunk_plan in plan_data_code_chunks(
            codes=normalized.code,
            chunk_size=250,
            resume_chunk_index=resume_chunk_index,
            resume_start_position=resume_start_position,
        ):
            logger.debug(
                "data_code chunk start chunk_index=%s chunk_size=%s start_position=%s",
                chunk_plan.chunk_index + 1,
                len(chunk_plan.codes),
                chunk_plan.start_position,
            )
            current_position = chunk_plan.start_position
            seen_positions: set[int] = set()

            try:
                while True:
                    payload = self._strict.execute_data_code(
                        normalized,
                        code_subset=chunk_plan.codes,
                        start_position=current_position,
                    )
                    parsed = parse_data_code_response(payload)
                    last_envelope = parsed.envelope
                    merge_series_map(by_code, parsed.series)
                    next_position = next_position_or_raise(
                        payload=payload,
                        seen_positions=seen_positions,
                        context_name="data_code",
                    )
                    if next_position is None:
                        break
                    current_position = next_position
                logger.debug(
                    "data_code chunk done chunk_index=%s accumulated_series=%s",
                    chunk_plan.chunk_index + 1,
                    len(by_code),
                )
            except Exception as exc:
                if isinstance(exc, BojValidationError):
                    raise
                emitted_checkpoint_id: str | None = None
                if by_code and self._checkpoint_manager.enabled:
                    emitted_checkpoint_id = self._checkpoint_manager.save_data_code(
                        DataCodeCheckpointState(
                            query=normalized,
                            config_snapshot=self._checkpoint_manager.config_snapshot,
                            by_code=dict(by_code),
                            last_envelope=last_envelope,
                            chunk_index=chunk_plan.chunk_index,
                            start_position=current_position,
                        )
                    )
                partial = build_data_code_response(
                    ordered_codes=normalized.code,
                    by_code=by_code,
                    envelope=last_envelope,
                )
                if partial.series:
                    logger.warning(
                        "data_code partial failure chunk_index=%s partial_series=%s cause=%s",
                        chunk_plan.chunk_index + 1,
                        len(partial.series),
                        cause_from_error(exc),
                    )
                    raise BojPartialResultError(
                        "data_code retrieval failed after partial progress",
                        partial_result=partial,
                        cause=cause_from_error(exc),
                        status=getattr(exc, "status", None),
                        message_id=getattr(exc, "message_id", None),
                        http_status=getattr(exc, "http_status", None),
                        checkpoint_id=emitted_checkpoint_id,
                    ) from exc
                logger.error(
                    "data_code failure without partial chunk_index=%s cause=%s",
                    chunk_plan.chunk_index + 1,
                    cause_from_error(exc),
                )
                raise
            resume_start_position = 1

        if checkpoint_id is not None:
            self._checkpoint_manager.cleanup(checkpoint_id)

        logger.info("data_code completed series=%s", len(by_code))
        return build_data_code_response(
            ordered_codes=normalized.code,
            by_code=by_code,
            envelope=last_envelope,
        )

    def get_data_layer(
        self,
        query: DataLayerQuery,
        *,
        checkpoint_id: str | None = None,
    ) -> DataLayerResponse:
        normalized = normalize_data_layer_query(query)

        if checkpoint_id is not None:
            state = self._checkpoint_manager.load_data_layer(
                checkpoint_id=checkpoint_id,
                normalized=normalized,
            )
            if isinstance(state, DataLayerDirectCheckpointState):
                response = self._get_data_layer_direct(normalized, checkpoint_state=state)
            elif isinstance(state, DataLayerAutoPartitionCheckpointState):
                response = self._get_data_layer_via_metadata(
                    normalized,
                    checkpoint_state=state,
                )
            else:
                raise BojValidationError("checkpoint path mismatch")
            self._checkpoint_manager.cleanup(checkpoint_id)
            return response

        if not self._enable_layer_auto_partition:
            return self._get_data_layer_direct(normalized)

        try:
            return self._get_data_layer_direct(normalized)
        except BojValidationError as exc:
            if not should_use_auto_partition(exc):
                raise
            logger.info(
                "data_layer auto_partition fallback activated db=%s frequency=%s",
                normalized.db,
                normalized.frequency,
            )
            return self._get_data_layer_via_metadata(normalized)

    def _get_data_layer_direct(
        self,
        normalized: DataLayerQuery,
        *,
        checkpoint_state: DataLayerDirectCheckpointState | None = None,
    ) -> DataLayerResponse:
        logger.info("data_layer start db=%s frequency=%s", normalized.db, normalized.frequency)
        by_code: dict[str, TimeSeries] = {}
        last_envelope = make_success_envelope()
        final_next_position = None
        current_position = 1

        if checkpoint_state is not None:
            by_code = dict(checkpoint_state.by_code)
            last_envelope = checkpoint_state.last_envelope
            current_position = checkpoint_state.start_position
            final_next_position = checkpoint_state.next_position
            logger.info(
                "data_layer resume path=direct start_position=%s partial_series=%s",
                current_position,
                len(by_code),
            )

        seen_positions: set[int] = set()

        try:
            while True:
                payload = self._strict.execute_data_layer(
                    normalized,
                    start_position=current_position,
                )
                parsed = parse_data_layer_response(payload)
                last_envelope = parsed.envelope
                merge_series_map(by_code, parsed.series)
                if len(by_code) > 1250:
                    logger.warning(
                        "data_layer exceeded series guardrail series=%s",
                        len(by_code),
                    )
                    raise BojValidationError(
                        "Layer query exceeds 1,250 series limit; narrow layer conditions"
                    )
                next_position = next_position_or_raise(
                    payload=payload,
                    seen_positions=seen_positions,
                    context_name="data_layer",
                )
                final_next_position = next_position
                if next_position is None:
                    break
                current_position = next_position
        except Exception as exc:
            if isinstance(exc, BojValidationError):
                raise
            partial_series = build_data_layer_response_from_map(
                envelope=last_envelope,
                by_code=by_code,
                next_position=final_next_position,
            ).series
            emitted_checkpoint_id: str | None = None
            if partial_series and self._checkpoint_manager.enabled:
                emitted_checkpoint_id = self._checkpoint_manager.save_data_layer_direct(
                    DataLayerDirectCheckpointState(
                        query=normalized,
                        config_snapshot=self._checkpoint_manager.config_snapshot,
                        by_code=dict(by_code),
                        last_envelope=last_envelope,
                        start_position=current_position,
                        next_position=final_next_position,
                    )
                )
            if partial_series:
                logger.warning(
                    "data_layer partial failure partial_series=%s cause=%s",
                    len(partial_series),
                    cause_from_error(exc),
                )
                raise BojPartialResultError(
                    "data_layer retrieval failed after partial progress",
                    partial_result=build_data_layer_response_from_series(
                        envelope=last_envelope,
                        series=partial_series,
                        next_position=final_next_position,
                    ),
                    cause=cause_from_error(exc),
                    status=getattr(exc, "status", None),
                    message_id=getattr(exc, "message_id", None),
                    http_status=getattr(exc, "http_status", None),
                    checkpoint_id=emitted_checkpoint_id,
                ) from exc
            raise

        logger.info("data_layer completed series=%s", len(by_code))
        return build_data_layer_response_from_map(
            envelope=last_envelope,
            by_code=by_code,
            next_position=final_next_position,
        )

    def _get_data_layer_via_metadata(
        self,
        normalized: DataLayerQuery,
        *,
        checkpoint_state: DataLayerAutoPartitionCheckpointState | None = None,
    ) -> DataLayerResponse:
        metadata_envelope = make_success_envelope()
        data_code_checkpoint_id: str | None = None

        if checkpoint_state is None:
            metadata = self.get_metadata(MetadataQuery(db=normalized.db, lang=normalized.lang))
            metadata_envelope = metadata.envelope
            codes = select_metadata_series_codes(metadata.entries, normalized)
            logger.info("data_layer auto_partition selected_codes=%s", len(codes))
        else:
            codes = checkpoint_state.selected_codes
            data_code_checkpoint_id = checkpoint_state.data_code_checkpoint_id
            logger.info(
                "data_layer resume path=auto_partition selected_codes=%s",
                len(codes),
            )

        if not codes:
            return build_data_layer_response_from_series(
                envelope=metadata_envelope,
                series=[],
                next_position=None,
            )

        code_query = DataCodeQuery(
            db=normalized.db,
            code=codes,
            lang=normalized.lang,
            start_date=normalized.start_date,
            end_date=normalized.end_date,
        )
        try:
            if data_code_checkpoint_id is None:
                code_result = self.get_data_code(code_query)
            else:
                code_result = self.get_data_code(
                    code_query,
                    checkpoint_id=data_code_checkpoint_id,
                )
        except BojPartialResultError as exc:
            partial = exc.partial_result
            if not isinstance(partial, DataCodeResponse):
                raise
            emitted_checkpoint_id: str | None = None
            if partial.series and self._checkpoint_manager.enabled:
                emitted_checkpoint_id = self._checkpoint_manager.save_data_layer_auto_partition(
                    DataLayerAutoPartitionCheckpointState(
                        query=normalized,
                        config_snapshot=self._checkpoint_manager.config_snapshot,
                        selected_codes=tuple(codes),
                        data_code_checkpoint_id=exc.checkpoint_id,
                    )
                )
            raise BojPartialResultError(
                "data_layer auto-partition retrieval failed after partial progress",
                partial_result=build_data_layer_response_from_series(
                    envelope=partial.envelope,
                    series=partial.series,
                    next_position=None,
                ),
                cause=exc.cause or "network",
                status=exc.status,
                message_id=exc.message_id,
                http_status=exc.http_status,
                checkpoint_id=emitted_checkpoint_id,
            ) from exc

        return build_data_layer_response_from_series(
            envelope=code_result.envelope,
            series=code_result.series,
            next_position=None,
        )

    def get_metadata(self, query: MetadataQuery) -> MetadataResponse:
        normalized = normalize_metadata_query(query)
        logger.info("metadata start db=%s", normalized.db)
        payload = self._strict.execute_metadata(normalized)
        parsed = parse_metadata_response(payload)
        logger.info("metadata completed entries=%s", len(parsed.entries))
        return parsed


__all__ = [
    "TimeSeriesService",
]
