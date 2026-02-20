from __future__ import annotations

import ast
from pathlib import Path


def _has_dunder_all(source: str) -> bool:
    module = ast.parse(source)
    for node in module.body:
        if not isinstance(node, ast.Assign | ast.AnnAssign):
            continue
        targets: list[ast.expr]
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
        else:
            targets = [node.target]
        for target in targets:
            if isinstance(target, ast.Name) and target.id == "__all__":
                return True
    return False


def test_all_source_modules_define_dunder_all() -> None:
    root = Path(__file__).resolve().parents[2] / "src" / "boj_api_client"
    missing: list[str] = []
    for path in sorted(root.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        if not _has_dunder_all(source):
            missing.append(path.as_posix())
    assert missing == []
