#!/usr/bin/env python3
"""
tools/audit/swallowed_exceptions.py
Audit script to detect `except Exception` or bare `except` blocks whose body only:
- passes (pass)
- continues (continue)
- logs at debug level (logger.debug / logging.debug)
inside alpha_engine/engine, execution, ruflo, council, profiler, tracker, ingestion.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


TARGET_MODULES = ("engine", "execution", "ruflo", "council", "profiler", "tracker", "ingestion")


def is_target_file(path: Path, repo_root: Path) -> bool:
    rel_parts = path.relative_to(repo_root).parts
    if any(p in ("tests", "test", ".venv", ".git", "__pycache__", "build", "dist") for p in rel_parts):
        return False
    # Check if under alpha_engine/<target> or root/<target>
    for i, part in enumerate(rel_parts):
        if part in TARGET_MODULES:
            return True
        if part == "alpha_engine" and len(rel_parts) > i + 1 and rel_parts[i + 1] in TARGET_MODULES:
            return True
    return False


class SwallowedExceptionVisitor(ast.NodeVisitor):
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.findings: list[tuple[int, str]] = []

    def _is_debug_log_call(self, stmt: ast.AST) -> bool:
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
            call = stmt.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == "debug":
                return True
        return False

    def _is_empty_or_swallowed(self, body: list[ast.stmt]) -> tuple[bool, str]:
        # Filter out docstrings / string literals
        non_doc_stmts = [
            s for s in body
            if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant) and isinstance(s.value.value, str))
        ]
        if not non_doc_stmts:
            return True, "empty body (only docstring/comments)"

        # Check if all statements are pass, continue, or logger.debug
        actions = []
        for stmt in non_doc_stmts:
            if isinstance(stmt, ast.Pass):
                actions.append("pass")
            elif isinstance(stmt, ast.Continue):
                actions.append("continue")
            elif self._is_debug_log_call(stmt):
                actions.append("logger.debug")
            else:
                return False, ""

        return True, ", ".join(sorted(set(actions)))

    def visit_ExceptHandler(self, node: ast.ExceptHandler):
        # Check if bare except or except Exception / BaseException
        is_broad = False
        exc_type_name = "bare except"
        if node.type is None:
            is_broad = True
        else:
            type_repr = ast.unparse(node.type) if hasattr(ast, "unparse") else ""
            if type_repr in ("Exception", "BaseException"):
                is_broad = True
                exc_type_name = f"except {type_repr}"

        if is_broad:
            is_swallowed, reason = self._is_empty_or_swallowed(node.body)
            if is_swallowed:
                self.findings.append((
                    node.lineno,
                    f"Swallowed exception at {exc_type_name} with only: {reason}",
                ))

        self.generic_visit(node)


def audit_file(path: Path) -> list[str]:
    try:
        content = path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(path))
    except Exception as e:
        return [f"{path}:1: Failed to parse: {e}"]

    visitor = SwallowedExceptionVisitor(str(path))
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
    findings: list[str] = []

    for path in sorted(repo_root.rglob("*.py")):
        if not is_target_file(path, repo_root):
            continue
        rel = path.relative_to(repo_root)
        hits = audit_file(rel)
        findings.extend(hits)

    for hit in findings:
        print(hit)

    total = len(findings)
    print(f"\n[swallowed_exceptions] Total findings: {total}", file=sys.stderr)
    return 1 if total > 0 else 0


if __name__ == "__main__":
    sys.exit(main())

