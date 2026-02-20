"""Core response models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .errors import extract_message, extract_message_id, extract_status


@dataclass(slots=True, frozen=True)
class ApiEnvelope:
    status: int | None
    message_id: str | None
    message: str | None
    date: str | None

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> "ApiEnvelope":
        return cls(
            status=extract_status(payload),
            message_id=extract_message_id(payload),
            message=extract_message(payload),
            date=str(payload.get("DATE")) if payload.get("DATE") is not None else None,
        )


__all__ = [
    "ApiEnvelope",
]
