from __future__ import annotations

import pytest

from boj_api_client.core.errors import (
    BojProtocolError,
    BojServerError,
    BojUnavailableError,
    BojValidationError,
)
from boj_api_client.core.response_parsing import parse_json_payload


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def test_parse_json_payload_maps_status_503_error():
    with pytest.raises(BojUnavailableError):
        parse_json_payload(_Response(ValueError("bad json")), http_status=503)


def test_parse_json_payload_maps_status_500_error():
    with pytest.raises(BojServerError):
        parse_json_payload(_Response(ValueError("bad json")), http_status=500)


def test_parse_json_payload_maps_status_400_error():
    with pytest.raises(BojValidationError):
        parse_json_payload(_Response(ValueError("bad json")), http_status=400)


def test_parse_json_payload_raises_protocol_error_for_non_dict_payload():
    with pytest.raises(BojProtocolError, match="response JSON root must be an object"):
        parse_json_payload(_Response([1, 2, 3]), http_status=200)


def test_parse_json_payload_returns_dict_payload():
    payload = parse_json_payload(_Response({"STATUS": 200}), http_status=200)
    assert payload == {"STATUS": 200}
