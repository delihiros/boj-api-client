from __future__ import annotations

import pytest

from boj_api_client.core.errors import BojProtocolError
from boj_api_client.core.pagination import iterate_pages, parse_next_position


def test_parse_next_position_handles_blank_and_int():
    assert parse_next_position({"NEXTPOSITION": ""}) is None
    assert parse_next_position({"NEXTPOSITION": None}) is None
    assert parse_next_position({"NEXTPOSITION": 255}) == 255
    assert parse_next_position({"NEXTPOSITION": "255"}) == 255


def test_iterate_pages_until_no_next_position():
    pages = {
        1: {"NEXTPOSITION": 3, "x": 1},
        3: {"NEXTPOSITION": "", "x": 2},
    }
    visited = [p["x"] for p in iterate_pages(lambda pos: pages[pos], start_position=1)]
    assert visited == [1, 2]


def test_parse_next_position_raises_for_invalid_value():
    with pytest.raises(BojProtocolError):
        parse_next_position({"NEXTPOSITION": "ABC"})
    with pytest.raises(BojProtocolError):
        parse_next_position({"NEXTPOSITION": {"x": 1}})
