from __future__ import annotations

import pytest

from boj_api_client.core.errors import (
    BojProtocolError,
    BojServerError,
    BojUnavailableError,
    BojValidationError,
)
from boj_api_client.core.transport import SyncTransport
from tests.shared.transport import Response, SyncSequencedClient, build_config


def test_http_200_body_400_is_validation_error():
    client = SyncSequencedClient([Response(200, {"STATUS": 400, "MESSAGEID": "X", "MESSAGE": "bad"})])
    transport = SyncTransport(build_config(), client=client, sleeper=lambda _: None, clock=lambda: 0.0)
    with pytest.raises(BojValidationError):
        transport.request("/getMetadata", params={"db": "FM08"})


def test_http_400_body_200_is_protocol_error():
    client = SyncSequencedClient([Response(400, {"STATUS": 200, "MESSAGEID": "X", "MESSAGE": "ok"})])
    transport = SyncTransport(build_config(), client=client, sleeper=lambda _: None, clock=lambda: 0.0)
    with pytest.raises(BojProtocolError):
        transport.request("/getMetadata", params={"db": "FM08"})


@pytest.mark.parametrize(
    ("http_status", "expected_error"),
    [
        (400, BojValidationError),
        (500, BojServerError),
        (503, BojUnavailableError),
    ],
)
def test_http_error_with_missing_body_status_is_mapped_by_http_status(http_status, expected_error):
    client = SyncSequencedClient([Response(http_status, {"MESSAGE": "missing status"})])
    transport = SyncTransport(build_config(), client=client, sleeper=lambda _: None, clock=lambda: 0.0)
    with pytest.raises(expected_error):
        transport.request("/getMetadata", params={"db": "FM08"})


@pytest.mark.parametrize(
    ("http_status", "expected_error"),
    [
        (400, BojValidationError),
        (500, BojServerError),
        (503, BojUnavailableError),
    ],
)
def test_http_error_with_non_json_body_is_mapped_by_http_status(http_status, expected_error):
    client = SyncSequencedClient([Response(http_status, ValueError("not json"))])
    transport = SyncTransport(build_config(), client=client, sleeper=lambda _: None, clock=lambda: 0.0)
    with pytest.raises(expected_error):
        transport.request("/getMetadata", params={"db": "FM08"})

