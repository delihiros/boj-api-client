from __future__ import annotations

from pathlib import Path

from boj_api_client.timeseries._orchestrator_codegen import (
    GENERATED_HEADER,
    generate_sync_orchestrator_source,
)


def test_sync_orchestrator_is_generated_from_async_source():
    repo_root = Path(__file__).resolve().parents[2]
    async_path = repo_root / "src" / "boj_api_client" / "timeseries" / "async_orchestrator.py"
    sync_path = repo_root / "src" / "boj_api_client" / "timeseries" / "orchestrator.py"

    async_source = async_path.read_text(encoding="utf-8")
    sync_source = sync_path.read_text(encoding="utf-8")

    generated = generate_sync_orchestrator_source(async_source)
    assert sync_source == generated
    assert sync_source.startswith(f"{GENERATED_HEADER}\n")
