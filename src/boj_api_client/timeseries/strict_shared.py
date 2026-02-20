"""Shared strict-query preparation utilities for sync/async services."""

from __future__ import annotations

from collections.abc import Sequence

from .params import build_data_code_params, build_data_layer_params, build_metadata_params
from .queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from .validators import (
    strict_validate_data_code_query,
    strict_validate_data_layer_query,
    strict_validate_metadata_query,
)


def _build_strict_data_code_query(
    query: DataCodeQuery,
    *,
    code_subset: Sequence[str],
    start_position: int,
) -> DataCodeQuery:
    return DataCodeQuery(
        db=query.db,
        code=tuple(code_subset),
        lang=query.lang,
        start_date=query.start_date,
        end_date=query.end_date,
        start_position=start_position,
    )


def _build_strict_data_layer_query(
    query: DataLayerQuery,
    *,
    start_position: int,
) -> DataLayerQuery:
    return DataLayerQuery(
        db=query.db,
        frequency=query.frequency,
        layer1=query.layer1,
        lang=query.lang,
        layer2=query.layer2,
        layer3=query.layer3,
        layer4=query.layer4,
        layer5=query.layer5,
        start_date=query.start_date,
        end_date=query.end_date,
        start_position=start_position,
    )


def build_strict_data_code_params(
    query: DataCodeQuery,
    *,
    code_subset: Sequence[str],
    start_position: int,
) -> dict[str, str]:
    strict_query = _build_strict_data_code_query(
        query,
        code_subset=code_subset,
        start_position=start_position,
    )
    strict_validate_data_code_query(strict_query)
    return build_data_code_params(strict_query, start_position=start_position)


def build_strict_data_layer_params(
    query: DataLayerQuery,
    *,
    start_position: int,
) -> dict[str, str]:
    strict_query = _build_strict_data_layer_query(
        query,
        start_position=start_position,
    )
    strict_validate_data_layer_query(strict_query)
    return build_data_layer_params(strict_query, start_position=start_position)


def build_strict_metadata_params(query: MetadataQuery) -> dict[str, str]:
    strict_validate_metadata_query(query)
    return build_metadata_params(query)


__all__ = [
    "build_strict_data_code_params",
    "build_strict_data_layer_params",
    "build_strict_metadata_params",
]
