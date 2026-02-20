from __future__ import annotations

from boj_api_client.timeseries.parser import (
    parse_data_code_response,
    parse_data_layer_response,
    parse_metadata_response,
)


def test_parse_data_code_success_fixture(fixture_loader):
    payload = fixture_loader("get_data_code_success.json")
    response = parse_data_code_response(payload)
    assert response.envelope.status == 200
    assert len(response.series) == 2
    assert isinstance(response.series, tuple)
    assert response.series[0].series_code
    assert response.series[0].points
    assert isinstance(response.series[0].points, tuple)


def test_parse_data_code_m181030i_returns_empty_series(fixture_loader):
    payload = fixture_loader("get_data_code_no_data_m181030i.json")
    response = parse_data_code_response(payload)
    assert response.envelope.message_id == "M181030I"
    assert response.series == ()


def test_parse_data_layer_fixture(fixture_loader):
    payload = fixture_loader("get_data_layer_page1.json")
    response = parse_data_layer_response(payload)
    assert response.envelope.status == 200
    assert response.next_position == 255
    assert len(response.series) > 0
    assert isinstance(response.series, tuple)


def test_parse_metadata_fixture(fixture_loader):
    payload = fixture_loader("get_metadata_success.json")
    response = parse_metadata_response(payload)
    assert response.envelope.status == 200
    assert len(response.entries) > 0
    assert isinstance(response.entries, tuple)


def test_parse_data_code_normalizes_bytes_to_utf8_str():
    payload = {
        "STATUS": 200,
        "MESSAGEID": "M181000I",
        "MESSAGE": "ok",
        "DATE": "2026-01-01T00:00:00+09:00",
        "PARAMETER": {},
        "NEXTPOSITION": "",
        "RESULTSET": [
            {
                "SERIES_CODE": b"ABC",
                "NAME_OF_TIME_SERIES_J": b"\xe6\x97\xa5\xe6\x9c\xac",
                "UNIT_J": b"\xe5\x86\x86",
                "FREQUENCY": "Q",
                "CATEGORY_J": "x",
                "LAST_UPDATE": "20260101",
                "VALUES": {
                    "SURVEY_DATES": [b"202401"],
                    "VALUES": [1],
                },
            }
        ],
    }
    response = parse_data_code_response(payload)
    assert response.series[0].series_code == "ABC"
    assert response.series[0].name == "日本"
    assert response.series[0].unit == "円"
    assert response.series[0].points[0].survey_date == "202401"
