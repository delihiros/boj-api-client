from __future__ import annotations

import copy
from collections.abc import Callable


class SyncFixtureTransport:
    def __init__(
        self,
        fixture_loader: Callable[[str], dict[str, object]],
        *,
        single_page_data_layer: bool = True,
    ) -> None:
        self._load = fixture_loader
        self._single_page_data_layer = single_page_data_layer

    def request(self, endpoint: str, *, params: dict[str, str]) -> dict[str, object]:
        if endpoint == "/getDataCode":
            return copy.deepcopy(self._load("get_data_code_success.json"))
        if endpoint == "/getDataLayer":
            payload = copy.deepcopy(self._load("get_data_layer_page1.json"))
            if self._single_page_data_layer:
                payload["NEXTPOSITION"] = ""
            return payload
        if endpoint == "/getMetadata":
            return copy.deepcopy(self._load("get_metadata_success.json"))
        raise AssertionError(f"Unexpected endpoint: {endpoint}")


class AsyncFixtureTransport:
    def __init__(
        self,
        fixture_loader: Callable[[str], dict[str, object]],
        *,
        single_page_data_layer: bool = True,
    ) -> None:
        self._load = fixture_loader
        self._single_page_data_layer = single_page_data_layer

    async def request(self, endpoint: str, *, params: dict[str, str]) -> dict[str, object]:
        if endpoint == "/getDataCode":
            return copy.deepcopy(self._load("get_data_code_success.json"))
        if endpoint == "/getDataLayer":
            payload = copy.deepcopy(self._load("get_data_layer_page1.json"))
            if self._single_page_data_layer:
                payload["NEXTPOSITION"] = ""
            return payload
        if endpoint == "/getMetadata":
            return copy.deepcopy(self._load("get_metadata_success.json"))
        raise AssertionError(f"Unexpected endpoint: {endpoint}")


class SyncErrorFixtureTransport:
    def __init__(
        self,
        fixture_loader: Callable[[str], dict[str, object]],
        *,
        error_fixture: str = "error_missing_db_http400.json",
    ) -> None:
        self._load = fixture_loader
        self._error_fixture = error_fixture

    def request(self, endpoint: str, *, params: dict[str, str]) -> dict[str, object]:
        return copy.deepcopy(self._load(self._error_fixture))


class AsyncErrorFixtureTransport:
    def __init__(
        self,
        fixture_loader: Callable[[str], dict[str, object]],
        *,
        error_fixture: str = "error_missing_db_http400.json",
    ) -> None:
        self._load = fixture_loader
        self._error_fixture = error_fixture

    async def request(self, endpoint: str, *, params: dict[str, str]) -> dict[str, object]:
        return copy.deepcopy(self._load(self._error_fixture))

