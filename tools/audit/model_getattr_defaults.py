#!/usr/bin/env python3
"""
tools/audit/model_getattr_defaults.py
Audit script to detect getattr(obj, "field", <literal>) where "field" is
(or is not) a field of any pydantic model in the repo, marking whether
the field exists on a known model.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def is_test_file(path: Path) -> bool:
    parts = path.parts
    return any(p in ("tests", "test", ".venv", ".git", "__pycache__", "build", "dist") for p in parts) or path.name.startswith("test_")


class PydanticModelCollector(ast.NodeVisitor):
    def __init__(self, filepath: str):
        self.filepath = filepath
        # model_name -> set of field names
        self.models: dict[str, set[str]] = {}
        # field_name -> set of model_names
        self.field_to_models: dict[str, set[str]] = {}

    def _is_pydantic_base(self, base_node: ast.AST) -> bool:
        name = ""
        if isinstance(base_node, ast.Name):
            name = base_node.id
        elif isinstance(base_node, ast.Attribute):
            name = base_node.attr
        return name in (
            "BaseModel",
            "BaseSettings",
            "Settings",
            "EngineConfig",
            "StrategyParameters",
            "BaseAgentConfig",
        )

    def visit_ClassDef(self, node: ast.ClassDef):
        is_model = any(self._is_pydantic_base(b) for b in node.bases)
        # Also check if class has type-annotated fields and docstring or belongs to models/config
        if not is_model:
            for b in node.bases:
                base_repr = ast.unparse(b) if hasattr(ast, "unparse") else ""
                if any(x in base_repr for x in ("Model", "Settings", "Config")):
                    is_model = True
                    break

        if is_model:
            fields = set()
            for item in node.body:
                if isinstance(item, ast.AnnAssign):
                    if isinstance(item.target, ast.Name):
                        fields.add(item.target.id)
                elif isinstance(item, ast.Assign):
                    for t in item.targets:
                        if isinstance(t, ast.Name):
                            fields.add(t.id)
            if fields:
                self.models[node.name] = fields
                for f in fields:
                    self.field_to_models.setdefault(f, set()).add(node.name)

        self.generic_visit(node)


class GetattrVisitor(ast.NodeVisitor):
    def __init__(self, filepath: str, field_to_models: dict[str, set[str]]):
        self.filepath = filepath
        self.field_to_models = field_to_models
        self.findings: list[tuple[int, str]] = []

    def visit_Call(self, node: ast.Call):
        func_name = ""
        if isinstance(node.func, ast.Name):
            func_name = node.func.id

        if func_name == "getattr" and len(node.args) >= 3:
            attr_arg = node.args[1]
            default_arg = node.args[2]

            # We check when attr_arg is a string constant
            if isinstance(attr_arg, ast.Constant) and isinstance(attr_arg.value, str):
                field_name = attr_arg.value
                default_repr = ast.unparse(default_arg) if hasattr(ast, "unparse") else "default"

                if field_name in self.field_to_models:
                    models = sorted(self.field_to_models[field_name])
                    models_str = ", ".join(models[:4])
                    if len(models) > 4:
                        models_str += f" (+{len(models)-4} more)"
                    self.findings.append((
                        node.lineno,
                        f"getattr(..., {field_name!r}, {default_repr}) [EXISTS on model(s): {models_str}]",
                    ))
                else:
                    self.findings.append((
                        node.lineno,
                        f"getattr(..., {field_name!r}, {default_repr}) [NOT FOUND on any known pydantic model]",
                    ))

        self.generic_visit(node)


def build_pydantic_database(repo_root: Path) -> dict[str, set[str]]:
    field_to_models: dict[str, set[str]] = {}
    for path in repo_root.rglob("*.py"):
        rel = path.relative_to(repo_root)
        if is_test_file(rel):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            collector = PydanticModelCollector(str(path))
            collector.visit(tree)
            for f, models in collector.field_to_models.items():
                field_to_models.setdefault(f, set()).update(models)
        except Exception:
            continue
    return field_to_models


def audit_file(path: Path, field_to_models: dict[str, set[str]]) -> list[str]:
    try:
        content = path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(path))
    except Exception as e:
        return [f"{path}:1: Failed to parse: {e}"]

    visitor = GetattrVisitor(str(path), field_to_models)
    visitor.visit(tree)

    results: list[str] = []
    seen = set()
    for lineno, msg in sorted(visitor.findings, key=lambda x: x[0]):
        key = (lineno, msg)
        if key not in seen:
            seen.add(key)
            results.append(f"{path}:{lineno}: {msg}")
    return results


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent.parent
    field_to_models = build_pydantic_database(repo_root)
    findings: list[str] = []

    for path in sorted(repo_root.rglob("*.py")):
        rel = path.relative_to(repo_root)
        if is_test_file(rel):
            continue
        hits = audit_file(rel, field_to_models)
        findings.extend(hits)

    for hit in findings:
        print(hit)

    total = len(findings)
    print(f"\n[model_getattr_defaults] Total findings: {total}", file=sys.stderr)
    return 1 if total > 0 else 0


if __name__ == "__main__":
    sys.exit(main())

