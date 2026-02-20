from __future__ import annotations

from collections.abc import Sequence


def make_series_payload(
    series_code: str,
    *,
    points: Sequence[tuple[int, int | float | None]] | None = None,
) -> dict[str, object]:
    resolved_points = list(points) if points is not None else [(202401, 1)]
    return {
        "SERIES_CODE": series_code,
        "NAME_OF_TIME_SERIES_J": series_code,
        "UNIT_J": "u",
        "FREQUENCY": "QUARTERLY",
        "CATEGORY_J": "c",
        "LAST_UPDATE": 20250101,
        "VALUES": {
            "SURVEY_DATES": [date for date, _ in resolved_points],
            "VALUES": [value for _, value in resolved_points],
        },
    }


def make_metadata_item(
    series_code: str,
    *,
    frequency: str,
    layer1: str,
    layer2: str | None = None,
    layer3: str | None = None,
    layer4: str | None = None,
    layer5: str | None = None,
) -> dict[str, str]:
    item: dict[str, str] = {
        "SERIES_CODE": series_code,
        "FREQUENCY": frequency,
        "LAYER1": layer1,
    }
    if layer2 is not None:
        item["LAYER2"] = layer2
    if layer3 is not None:
        item["LAYER3"] = layer3
    if layer4 is not None:
        item["LAYER4"] = layer4
    if layer5 is not None:
        item["LAYER5"] = layer5
    return item


def make_success_payload(
    *,
    resultset: Sequence[dict[str, object]] | None = None,
    next_position: int | str = "",
) -> dict[str, object]:
    return {
        "STATUS": 200,
        "MESSAGEID": "M181000I",
        "MESSAGE": "ok",
        "DATE": "2026-01-01T00:00:00+09:00",
        "PARAMETER": {},
        "NEXTPOSITION": next_position,
        "RESULTSET": list(resultset or []),
    }

