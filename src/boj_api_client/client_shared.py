"""Shared helpers for sync/async client bootstrap."""

from __future__ import annotations

from .config import BojClientConfig
from .core.checkpoint_store import CheckpointStore, MemoryCheckpointStore
from .core.errors import BojValidationError


def validate_client_config(config: BojClientConfig) -> None:
    try:
        config.validate()
    except ValueError as exc:
        raise BojValidationError(str(exc)) from exc


def resolve_checkpoint_store(
    *,
    config: BojClientConfig,
    checkpoint_store: CheckpointStore | None,
) -> CheckpointStore | None:
    if checkpoint_store is not None:
        return checkpoint_store
    if config.checkpoint.enabled:
        return MemoryCheckpointStore(ttl_seconds=config.checkpoint.ttl_seconds)
    return None


__all__ = [
    "validate_client_config",
    "resolve_checkpoint_store",
]
