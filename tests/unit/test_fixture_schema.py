from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    ("filename", "expected_keys"),
    [
        (
            "get_data_code_success.json",
            {"STATUS", "MESSAGEID", "MESSAGE", "DATE", "PARAMETER", "NEXTPOSITION", "RESULTSET"},
        ),
        (
            "get_data_layer_page1.json",
            {"STATUS", "MESSAGEID", "MESSAGE", "DATE", "PARAMETER", "NEXTPOSITION", "RESULTSET"},
        ),
        (
            "get_metadata_success.json",
            {"STATUS", "MESSAGEID", "MESSAGE", "DATE", "DB", "RESULTSET"},
        ),
    ],
)
def test_success_fixture_has_expected_schema(fixture_loader, filename, expected_keys):
    payload = fixture_loader(filename)
    assert expected_keys.issubset(set(payload.keys()))
    assert isinstance(payload["RESULTSET"], list)


def test_error_fixture_is_json_and_has_message_id(fixture_loader):
    payload = fixture_loader("error_missing_db_http400.json")
    assert payload["STATUS"] == 400
    assert payload["MESSAGEID"] == "M181004E"
    assert isinstance(payload["MESSAGE"], str)

