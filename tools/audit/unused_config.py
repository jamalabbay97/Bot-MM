#!/usr/bin/env python3
"""
tools/audit/unused_config.py
Audit script to detect pydantic Settings fields and StrategyParameters/extra_params keys
that no non-definition code reads.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def is_test_file(path: Path) -> bool:
    parts = path.parts
    return any(p in ("tests", "test", ".venv", ".git", "__pycache__", "build", "dist") for p in parts) or path.name.startswith("test_")


class ConfigDefinitionCollector(ast.NodeVisitor):
    def __init__(self, filepath: str):
        self.filepath = filepath
        # list of (field_name, lineno, class_name)
        self.config_fields: list[tuple[str, int, str]] = []
        self.extra_params_keys: list[tuple[str, int, str]] = []
        self.current_class: str | None = None
        self.is_settings_class = False

    def visit_ClassDef(self, node: ast.ClassDef):
        prev_cls = self.current_class
        prev_is_settings = self.is_settings_class

        self.current_class = node.name
        self.is_settings_class = any(
            (isinstance(b, ast.Name) and b.id in ("BaseSettings", "Settings"))
            or (isinstance(b, ast.Attribute) and b.attr in ("BaseSettings", "Settings"))
            for b in node.bases
        ) or node.name in ("EngineConfig", "StrategyParameters")

        if self.is_settings_class:
            for item in node.body:
                if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                    name = item.target.id
                    # Ignore private or internal fields
                    if not name.startswith("_") and name != "model_config":
                        self.config_fields.append((name, item.lineno, node.name))
                elif isinstance(item, ast.Assign):
                    for t in item.targets:
                        if isinstance(t, ast.Name) and not t.id.startswith("_") and t.id != "model_config":
                            self.config_fields.append((t.id, item.lineno, node.name))

        self.generic_visit(node)
        self.current_class = prev_cls
        self.is_settings_class = prev_is_settings

    def visit_Dict(self, node: ast.Dict):
        # Look for extra_params={...} definitions
        # in visit_Call or keyword
        self.generic_visit(node)

    def visit_keyword(self, node: ast.keyword):
        if node.arg == "extra_params" and isinstance(node.value, ast.Dict):
            for k in node.value.keys:
                if isinstance(k, ast.Constant) and isinstance(k.value, str):
                    self.extra_params_keys.append((k.value, node.lineno, "extra_params"))
        self.generic_visit(node)


class CodeUsageIndex:
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        # field_name -> set of (filepath, lineno)
        self.attribute_uses: dict[str, set[tuple[str, int]]] = {}
        self.string_uses: dict[str, set[tuple[str, int]]] = {}
        self._build_index()

    def _build_index(self):
        for path in self.repo_root.rglob("*.py"):
            rel = path.relative_to(self.repo_root)
            if is_test_file(rel):
                continue
            try:
                content = path.read_text(encoding="utf-8")
                tree = ast.parse(content, filename=str(path))
            except Exception:
                continue

            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute):
                    self.attribute_uses.setdefault(node.attr, set()).add((str(rel), node.lineno))
                elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                    self.string_uses.setdefault(node.value, set()).add((str(rel), node.lineno))


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent.parent
    usage_index = CodeUsageIndex(repo_root)

    findings: list[tuple[str, int, str]] = []

    for path in sorted(repo_root.rglob("*.py")):
        rel = path.relative_to(repo_root)
        if is_test_file(rel):
            continue

        try:
            content = path.read_text(encoding="utf-8")
            tree = ast.parse(content, filename=str(path))
        except Exception:
            continue

        collector = ConfigDefinitionCollector(str(rel))
        collector.visit(tree)

        # Check config fields
        for field_name, lineno, class_name in collector.config_fields:
            # Check if this field is used anywhere outside this definition line
            attr_uses = {
                (f, l) for (f, l) in usage_index.attribute_uses.get(field_name, set())
                if not (f == str(rel) and l == lineno)
            }
            str_uses = {
                (f, l) for (f, l) in usage_index.string_uses.get(field_name, set())
                if not (f == str(rel) and l == lineno)
            }

            if not attr_uses and not str_uses:
                findings.append((
                    str(rel),
                    lineno,
                    f"Unused pydantic Settings field {field_name!r} on {class_name} (never read in non-definition code)",
                ))

        # Check extra_params keys
        for key_name, lineno, container in collector.extra_params_keys:
            # Check if key is accessed as string lookup or attribute
            attr_uses = usage_index.attribute_uses.get(key_name, set())
            str_uses = {
                (f, l) for (f, l) in usage_index.string_uses.get(key_name, set())
                if not (f == str(rel) and l == lineno)
            }
            if not attr_uses and not str_uses:
                findings.append((
                    str(rel),
                    lineno,
                    f"Unused {container} key {key_name!r} (never read in non-definition code)",
                ))

    # Deduplicate findings
    seen = set()
    deduped: list[tuple[str, int, str]] = []
    for f, l, msg in sorted(findings, key=lambda x: (x[0], x[1])):
        key = (f, l, msg)
        if key not in seen:
            seen.add(key)
            deduped.append(key)
            print(f"{f}:{l}: {msg}")

    total = len(deduped)
    print(f"\n[unused_config] Total findings: {total}", file=sys.stderr)
    return 1 if total > 0 else 0


if __name__ == "__main__":
    sys.exit(main())

