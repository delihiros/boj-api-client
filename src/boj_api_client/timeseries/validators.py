"""Input validation and resilient normalization."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace

from ..core.errors import BojValidationError
from .queries import DataCodeQuery, DataLayerQuery, MetadataQuery

_FORBIDDEN_CHARS = set('<>"!|\\;\'')


def _contains_forbidden(value: str) -> bool:
    return any(ch in _FORBIDDEN_CHARS for ch in value)


def _ensure_non_empty_str(value: str | None, *, name: str) -> str:
    if value is None:
        raise BojValidationError(f"{name} is required")
    text = value.strip()
    if text == "":
        raise BojValidationError(f"{name} is required")
    if _contains_forbidden(text):
        raise BojValidationError(f"{name} contains forbidden characters")
    return text


def _dedupe_keep_order(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return tuple(out)


def _validate_contiguous_layers(
    layer2: str | None,
    layer3: str | None,
    layer4: str | None,
    layer5: str | None,
) -> None:
    normalized: list[str | None] = []
    for index, layer in enumerate((layer2, layer3, layer4, layer5), start=2):
        if layer is None:
            normalized.append(None)
            continue
        normalized.append(_ensure_non_empty_str(layer, name=f"layer{index}"))

    seen_gap = False
    for index, layer in enumerate(normalized, start=2):
        if layer is None:
            seen_gap = True
            continue
        if seen_gap:
            raise BojValidationError("layer must be contiguous from layer1")


def normalize_data_code_query(query: DataCodeQuery) -> DataCodeQuery:
    db = _ensure_non_empty_str(query.db, name="db")
    if not query.code:
        raise BojValidationError("code must not be empty")
    deduped = _dedupe_keep_order([_ensure_non_empty_str(c, name="code") for c in query.code])
    return replace(query, db=db, code=deduped)


def strict_validate_data_code_query(query: DataCodeQuery) -> None:
    _ensure_non_empty_str(query.db, name="db")
    if not query.code:
        raise BojValidationError("code must not be empty")
    cleaned = [_ensure_non_empty_str(c, name="code") for c in query.code]
    if len(cleaned) != len(set(cleaned)):
        raise BojValidationError("code contains duplicates in strict mode")
    if len(cleaned) > 250:
        raise BojValidationError("code length must be <= 250 in strict mode")
    if query.start_position is not None and query.start_position < 1:
        raise BojValidationError("start_position must be >= 1")


def normalize_data_layer_query(query: DataLayerQuery) -> DataLayerQuery:
    db = _ensure_non_empty_str(query.db, name="db")
    frequency = _ensure_non_empty_str(query.frequency, name="frequency")
    layer1 = _ensure_non_empty_str(query.layer1, name="layer1")
    _validate_contiguous_layers(query.layer2, query.layer3, query.layer4, query.layer5)
    return replace(
        query,
        db=db,
        frequency=frequency,
        layer1=layer1,
        layer2=_ensure_non_empty_str(query.layer2, name="layer2") if query.layer2 is not None else None,
        layer3=_ensure_non_empty_str(query.layer3, name="layer3") if query.layer3 is not None else None,
        layer4=_ensure_non_empty_str(query.layer4, name="layer4") if query.layer4 is not None else None,
        layer5=_ensure_non_empty_str(query.layer5, name="layer5") if query.layer5 is not None else None,
    )


def strict_validate_data_layer_query(query: DataLayerQuery) -> None:
    _ensure_non_empty_str(query.db, name="db")
    _ensure_non_empty_str(query.frequency, name="frequency")
    _ensure_non_empty_str(query.layer1, name="layer1")
    _validate_contiguous_layers(query.layer2, query.layer3, query.layer4, query.layer5)
    if query.start_position is not None and query.start_position < 1:
        raise BojValidationError("start_position must be >= 1")


def normalize_metadata_query(query: MetadataQuery) -> MetadataQuery:
    db = _ensure_non_empty_str(query.db, name="db")
    return replace(query, db=db)


def strict_validate_metadata_query(query: MetadataQuery) -> None:
    _ensure_non_empty_str(query.db, name="db")


__all__ = [
    "normalize_data_code_query",
    "strict_validate_data_code_query",
    "normalize_data_layer_query",
    "strict_validate_data_layer_query",
    "normalize_metadata_query",
    "strict_validate_metadata_query",
]
