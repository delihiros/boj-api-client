from __future__ import annotations

import pytest

from boj_api_client.core.errors import (
    BojPartialResultError,
    BojProtocolError,
    BojServerError,
    BojUnavailableError,
    BojValidationError,
    classify_api_error,
)
from boj_api_client.core.models import ApiEnvelope
from boj_api_client.timeseries.models import DataCodeResponse, DataLayerResponse


def test_classify_500_maps_to_server_error():
    payload = {"STATUS": 500, "MESSAGEID": "X", "MESSAGE": "err"}
    err = classify_api_error(payload, http_status=200)
    assert isinstance(err, BojServerError)


def test_classify_503_maps_to_unavailable_error():
    payload = {"STATUS": 503, "MESSAGEID": "X", "MESSAGE": "busy"}
    err = classify_api_error(payload, http_status=200)
    assert isinstance(err, BojUnavailableError)


def test_classify_400_maps_to_validation_error():
    payload = {"STATUS": 400, "MESSAGEID": "X", "MESSAGE": "bad"}
    err = classify_api_error(payload, http_status=400)
    assert isinstance(err, BojValidationError)


def test_inconsistent_http_400_with_body_200_is_protocol_error():
    payload = {"STATUS": 200, "MESSAGEID": "OK", "MESSAGE": "ok"}
    err = classify_api_error(payload, http_status=400)
    assert isinstance(err, BojProtocolError)


def test_http_200_with_body_400_is_validation_error():
    payload = {"STATUS": 400, "MESSAGEID": "E", "MESSAGE": "bad"}
    err = classify_api_error(payload, http_status=200)
    assert isinstance(err, BojValidationError)


def test_partial_result_error_can_hold_checkpoint_id():
    partial = DataCodeResponse(
        envelope=ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None),
        series=(),
    )
    err = BojPartialResultError(
        "partial",
        partial_result=partial,
        cause="network",
        checkpoint_id="abc123",
    )
    assert err.checkpoint_id == "abc123"
    assert err.partial_result is partial


def test_partial_result_error_defaults_checkpoint_id_to_none():
    partial = DataLayerResponse(
        envelope=ApiEnvelope(status=200, message_id="M181000I", message="ok", date=None),
        series=(),
        next_position=None,
    )
    err = BojPartialResultError(
        "partial",
        partial_result=partial,
        cause="network",
    )
    assert err.checkpoint_id is None


def test_partial_result_error_rejects_non_response_object():
    with pytest.raises(TypeError):
        BojPartialResultError(
            "partial",
            partial_result={"a": 1},  # type: ignore[arg-type]
            cause="network",
        )
