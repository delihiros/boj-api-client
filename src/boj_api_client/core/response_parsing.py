"""Shared response parsing helpers for sync/async transports."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from .errors import (
    BojApiError,
    BojProtocolError,
    BojServerError,
    BojUnavailableError,
    BojValidationError,
    classify_api_error,
    extract_status,
)


class JsonPayloadResponse(Protocol):
    def json(self) -> object: ...


def parse_json_payload(
    response: JsonPayloadResponse,
    *,
    http_status: int | None,
) -> dict[str, object]:
    """Parse response JSON payload and map parse failures to domain errors."""

    try:
        payload = response.json()
    except Exception as exc:
        raise _json_parse_error(http_status=http_status) from exc

    if not isinstance(payload, dict):
        raise BojProtocolError(
            "response JSON root must be an object",
            http_status=http_status,
        )
    if any(not isinstance(key, str) for key in payload):
        raise BojProtocolError(
            "response JSON object keys must be strings",
            http_status=http_status,
        )
    return payload


def classify_payload_outcome(
    payload: Mapping[str, object],
    *,
    http_status: int | None,
) -> tuple[BojApiError | None, int | None]:
    """Classify payload according to BOJ status rules."""

    mapped_error = classify_api_error(payload, http_status=http_status)
    return mapped_error, extract_status(payload)


def _json_parse_error(*, http_status: int | None) -> BojApiError:
    message = "response body is not valid JSON"
    if http_status == 503:
        return BojUnavailableError(
            message,
            http_status=http_status,
            cause="server_transient",
        )
    if http_status is not None and http_status >= 500:
        return BojServerError(
            message,
            http_status=http_status,
            cause="server_transient",
        )
    if http_status is not None and http_status >= 400:
        return BojValidationError(
            message,
            http_status=http_status,
        )
    return BojProtocolError(
        message,
        http_status=http_status,
    )


__all__ = [
    "parse_json_payload",
    "classify_payload_outcome",
]
