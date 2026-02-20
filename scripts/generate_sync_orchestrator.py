"""Generate sync orchestrator source from async orchestrator source."""

from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from boj_api_client.timeseries._orchestrator_codegen import generate_sync_orchestrator_source


def main() -> None:
    async_path = SRC_ROOT / "boj_api_client" / "timeseries" / "async_orchestrator.py"
    sync_path = SRC_ROOT / "boj_api_client" / "timeseries" / "orchestrator.py"

    async_source = async_path.read_text(encoding="utf-8")
    sync_source = generate_sync_orchestrator_source(async_source)
    # Always emit LF to keep generated output stable across platforms.
    sync_path.write_text(sync_source, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
