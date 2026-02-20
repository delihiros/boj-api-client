"""Validation helpers for checkpoint serialization and decoding."""

from __future__ import annotations

from ..core.errors import BojValidationError


def as_str(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise BojValidationError(f"{field_name} is invalid")
    return value


def as_str_or_none(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def as_int(value: object, *, field_name: str) -> int:
    if not isinstance(value, int):
        raise BojValidationError(f"{field_name} is invalid")
    return value


def as_int_or_none(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    return as_int(value, field_name=field_name)


def as_config_snapshot(value: object) -> dict[str, int | float | bool]:
    if not isinstance(value, dict):
        raise BojValidationError("checkpoint config_snapshot is invalid")
    snapshot: dict[str, int | float | bool] = {}
    for key, raw in value.items():
        if not isinstance(key, str):
            raise BojValidationError("checkpoint config_snapshot is invalid")
        if not isinstance(raw, (bool, int, float)):
            raise BojValidationError("checkpoint config_snapshot is invalid")
        snapshot[key] = raw
    return snapshot


__all__ = [
    "as_str",
    "as_str_or_none",
    "as_int",
    "as_int_or_none",
    "as_config_snapshot",
]
