"""Error types and status mapping."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..timeseries.models import DataCodeResponse, DataLayerResponse


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        if text.isdigit():
            return int(text)
    return None


def extract_status(payload: Mapping[str, object] | None) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    return _to_int(payload.get("STATUS"))


def extract_message_id(payload: Mapping[str, object] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get("MESSAGEID")
    return str(value) if value is not None else None


def extract_message(payload: Mapping[str, object] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get("MESSAGE")
    return str(value) if value is not None else None


class BojApiError(Exception):
    """Base exception for this package."""

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        message_id: str | None = None,
        http_status: int | None = None,
        cause: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.message_id = message_id
        self.http_status = http_status
        self.cause = cause


class BojTransportError(BojApiError):
    """Network/transport-level failure."""


class BojClientClosedError(BojApiError):
    """Raised when client is used after close."""


class BojValidationError(BojApiError):
    """Invalid input / request rejected."""


class BojServerError(BojApiError):
    """Server-side unexpected error."""


class BojUnavailableError(BojApiError):
    """Server unavailable / DB access error."""


class BojProtocolError(BojApiError):
    """Response shape or HTTP/body status inconsistency."""


class BojPartialResultError(BojApiError):
    """Raised when operation fails after collecting partial data."""

    def __init__(
        self,
        message: str,
        *,
        partial_result: "DataCodeResponse | DataLayerResponse",
        cause: str,
        status: int | None = None,
        message_id: str | None = None,
        http_status: int | None = None,
        checkpoint_id: str | None = None,
    ) -> None:
        super().__init__(
            message,
            status=status,
            message_id=message_id,
            http_status=http_status,
            cause=cause,
        )
        from ..timeseries.models import DataCodeResponse, DataLayerResponse

        if not isinstance(partial_result, (DataCodeResponse, DataLayerResponse)):
            raise TypeError("partial_result must be DataCodeResponse | DataLayerResponse")
        self.partial_result = partial_result
        self.checkpoint_id = checkpoint_id


def classify_api_error(
    payload: Mapping[str, object] | None,
    *,
    http_status: int | None,
) -> BojApiError | None:
    """Map HTTP/body status to domain exceptions."""

    status = extract_status(payload)
    message_id = extract_message_id(payload)
    message = extract_message(payload) or "BOJ API request failed"

    # Success only when both are successful and coherent.
    if status == 200 and http_status == 200:
        return None

    # Body status takes precedence when present.
    if status == 400:
        return BojValidationError(
            message,
            status=status,
            message_id=message_id,
            http_status=http_status,
        )
    if status == 500:
        return BojServerError(
            message,
            status=status,
            message_id=message_id,
            http_status=http_status,
            cause="server_transient",
        )
    if status == 503:
        return BojUnavailableError(
            message,
            status=status,
            message_id=message_id,
            http_status=http_status,
            cause="server_transient",
        )

    if status == 200 and http_status is not None and http_status >= 400:
        return BojProtocolError(
            "HTTP status and body STATUS are inconsistent",
            status=status,
            message_id=message_id,
            http_status=http_status,
        )

    if status is None:
        if http_status is None:
            return BojProtocolError("Missing both HTTP and body status")
        if http_status == 503:
            return BojUnavailableError(message, http_status=http_status, status=status)
        if http_status >= 500:
            return BojServerError(message, http_status=http_status, status=status)
        if http_status >= 400:
            return BojValidationError(message, http_status=http_status, status=status)
        return BojProtocolError(
            "body STATUS is missing in successful HTTP response",
            http_status=http_status,
            status=status,
        )

    return BojProtocolError(
        "Unknown STATUS in BOJ response",
        status=status,
        message_id=message_id,
        http_status=http_status,
    )


__all__ = [
    "BojApiError",
    "BojTransportError",
    "BojClientClosedError",
    "BojValidationError",
    "BojServerError",
    "BojUnavailableError",
    "BojProtocolError",
    "BojPartialResultError",
    "extract_status",
    "extract_message_id",
    "extract_message",
    "classify_api_error",
]
