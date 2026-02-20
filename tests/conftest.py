from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(scope="session")
def fixture_dir() -> Path:
    return ROOT / "tests" / "fixtures" / "live_api_2026-02-19"


@pytest.fixture(scope="session")
def fixture_loader(fixture_dir: Path):
    def _load(name: str) -> dict[str, Any]:
        path = fixture_dir / name
        return json.loads(path.read_text(encoding="utf-8"))

    return _load

