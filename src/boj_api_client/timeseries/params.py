"""Request parameter builders for timeseries endpoints."""

from __future__ import annotations

from .queries import DataCodeQuery, DataLayerQuery, MetadataQuery


def build_layer_param(query: DataLayerQuery) -> str:
    values = [query.layer1]
    for layer in (query.layer2, query.layer3, query.layer4, query.layer5):
        if layer:
            values.append(layer)
            continue
        break
    return ",".join(values)


def build_data_code_params(
    query: DataCodeQuery,
    *,
    start_position: int,
) -> dict[str, str]:
    params: dict[str, str] = {
        "format": "json",
        "lang": query.lang,
        "db": query.db,
        "code": ",".join(query.code),
    }
    if start_position > 1:
        params["startPosition"] = str(start_position)
    if query.start_date:
        params["startDate"] = query.start_date
    if query.end_date:
        params["endDate"] = query.end_date
    return params


def build_data_layer_params(
    query: DataLayerQuery,
    *,
    start_position: int,
) -> dict[str, str]:
    params: dict[str, str] = {
        "format": "json",
        "lang": query.lang,
        "db": query.db,
        "frequency": query.frequency,
        "layer": build_layer_param(query),
    }
    if start_position > 1:
        params["startPosition"] = str(start_position)
    if query.start_date:
        params["startDate"] = query.start_date
    if query.end_date:
        params["endDate"] = query.end_date
    return params


def build_metadata_params(query: MetadataQuery) -> dict[str, str]:
    return {
        "format": "json",
        "lang": query.lang,
        "db": query.db,
    }


__all__ = [
    "build_layer_param",
    "build_data_code_params",
    "build_data_layer_params",
    "build_metadata_params",
]
