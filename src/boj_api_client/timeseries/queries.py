"""Query models."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace


@dataclass(slots=True, frozen=True)
class DataCodeQuery:
    db: str
    code: Sequence[str]
    lang: str = "JP"
    start_date: str | None = None
    end_date: str | None = None
    start_position: int | None = field(default=None, kw_only=True, repr=False)

    def __post_init__(self) -> None:
        if isinstance(self.code, str):
            raise TypeError("code must be a sequence of str, not str")
        if not isinstance(self.code, Sequence):
            raise TypeError("code must be Sequence[str]")
        normalized: list[str] = []
        for code in self.code:
            if not isinstance(code, str):
                raise TypeError("code entries must be str")
            normalized.append(code)
        object.__setattr__(self, "code", tuple(normalized))

    def with_codes(self, codes: Sequence[str]) -> "DataCodeQuery":
        return replace(self, code=tuple(codes))


@dataclass(slots=True, frozen=True)
class DataLayerQuery:
    db: str
    frequency: str
    layer1: str
    lang: str = "JP"
    layer2: str | None = None
    layer3: str | None = None
    layer4: str | None = None
    layer5: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    start_position: int | None = field(default=None, kw_only=True, repr=False)


@dataclass(slots=True, frozen=True)
class MetadataQuery:
    db: str
    lang: str = "JP"


__all__ = [
    "DataCodeQuery",
    "DataLayerQuery",
    "MetadataQuery",
]
