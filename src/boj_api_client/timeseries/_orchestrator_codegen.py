"""Code generation helpers for sync orchestrator."""

from __future__ import annotations

import re

GENERATED_HEADER = "# AUTO-GENERATED FROM async_orchestrator.py. DO NOT EDIT."


def generate_sync_orchestrator_source(async_source: str) -> str:
    """Convert async orchestrator source into sync orchestrator source."""
    source = async_source

    source = source.replace(
        '"""Public async orchestration for timeseries APIs."""',
        '"""Public sync orchestration for timeseries APIs."""',
    )
    source = source.replace(
        '"""Public async resilient facade."""',
        '"""Public resilient facade."""',
    )
    source = source.replace(
        "from collections.abc import AsyncIterator, Mapping",
        "from collections.abc import Iterator, Mapping",
    )
    source = source.replace(
        "from ..core.async_pagination import aiterate_pages",
        "from ..core.pagination import iterate_pages",
    )
    source = source.replace(
        "from .async_checkpoint_manager import AsyncCheckpointManager",
        "from .checkpoint_manager import CheckpointManager",
    )
    source = source.replace(
        "from .async_strict import AsyncStrictTimeSeriesService",
        "from .strict import StrictTimeSeriesService",
    )

    source = _replace_word(source, "AsyncTimeSeriesService", "TimeSeriesService")
    source = _replace_word(source, "AsyncStrictTimeSeriesService", "StrictTimeSeriesService")
    source = _replace_word(source, "AsyncCheckpointManager", "CheckpointManager")
    source = _replace_word(source, "AsyncIterator", "Iterator")
    source = _replace_word(source, "aiterate_pages", "iterate_pages")

    source = re.sub(r"\basync def\b", "def", source)
    source = re.sub(r"\basync for\b", "for", source)
    source = re.sub(r"\bawait\s+", "", source)
    source = source.replace(".aclose()", ".close()")

    normalized = source.rstrip() + "\n"
    return f"{GENERATED_HEADER}\n\n{normalized}"


def _replace_word(source: str, old: str, new: str) -> str:
    return re.sub(rf"\b{re.escape(old)}\b", new, source)


__all__ = [
    "GENERATED_HEADER",
    "generate_sync_orchestrator_source",
]
