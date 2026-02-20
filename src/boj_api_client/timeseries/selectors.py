"""Selectors for metadata-driven layer auto-partition fallback."""

from __future__ import annotations

from collections.abc import Sequence
from fnmatch import fnmatchcase

from .models import MetadataEntry
from .queries import DataLayerQuery

_LAYER_FIELDS = ("layer1", "layer2", "layer3", "layer4", "layer5")


def _matches_pattern(pattern: str | None, value: str | None) -> bool:
    if pattern is None:
        return True
    if pattern == "*":
        return True
    candidate = value or ""
    if any(token in pattern for token in ("*", "?", "[")):
        return fnmatchcase(candidate, pattern)
    return candidate == pattern


def metadata_entry_matches_layer_query(entry: MetadataEntry, query: DataLayerQuery) -> bool:
    if (entry.frequency or "").casefold() != query.frequency.casefold():
        return False
    for field_name in _LAYER_FIELDS:
        pattern = getattr(query, field_name)
        value = getattr(entry, field_name)
        if not _matches_pattern(pattern, value):
            return False
    return True


def select_metadata_series_codes(
    entries: Sequence[MetadataEntry],
    query: DataLayerQuery,
) -> tuple[str, ...]:
    seen: set[str] = set()
    matched: list[str] = []
    for entry in entries:
        if not metadata_entry_matches_layer_query(entry, query):
            continue
        code = entry.series_code
        if code in seen:
            continue
        seen.add(code)
        matched.append(code)
    matched.sort()
    return tuple(matched)


__all__ = [
    "metadata_entry_matches_layer_query",
    "select_metadata_series_codes",
]
