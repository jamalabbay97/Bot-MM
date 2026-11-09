#!/usr/bin/env python3
"""
tools/audit/conditional_precedence.py
Audit script to detect conditional precedence issues:
- ast.IfExp used directly as the test of an if/while
- ast.IfExp as an operand of BoolOp or Compare
- ast.IfExp whose body (or orelse) is a Compare or BoolOp
(the exact pattern: `if a >= x if cond else y:`)
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def is_test_file(path: Path) -> bool:
    parts = path.parts
    return any(p in ("tests", "test", ".venv", ".git", "__pycache__", "build", "dist") for p in parts) or path.name.startswith("test_")


class ConditionalPrecedenceVisitor(ast.NodeVisitor):
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.findings: list[tuple[int, str]] = []

    def visit_If(self, node: ast.If):
        # ast.IfExp used directly as the test of an `if`
        if isinstance(node.test, ast.IfExp):
            code_snippet = ast.unparse(node.test) if hasattr(ast, "unparse") else "if_exp"
            self.findings.append((
                node.lineno,
                f"ast.IfExp used directly as test of 'if': if {code_snippet}:",
            ))
        self.generic_visit(node)

    def visit_While(self, node: ast.While):
        # ast.IfExp used directly as the test of a `while`
        if isinstance(node.test, ast.IfExp):
            code_snippet = ast.unparse(node.test) if hasattr(ast, "unparse") else "if_exp"
            self.findings.append((
                node.lineno,
                f"ast.IfExp used directly as test of 'while': while {code_snippet}:",
            ))
        self.generic_visit(node)

    def visit_BoolOp(self, node: ast.BoolOp):
        # ast.IfExp as an operand of BoolOp
        for val in node.values:
            if isinstance(val, ast.IfExp):
                code_snippet = ast.unparse(node) if hasattr(ast, "unparse") else "bool_op"
                self.findings.append((
                    node.lineno,
                    f"ast.IfExp used as operand of BoolOp ({type(node.op).__name__}): {code_snippet}",
                ))
        self.generic_visit(node)

    def visit_Compare(self, node: ast.Compare):
        # ast.IfExp as an operand of Compare
        if isinstance(node.left, ast.IfExp):
            code_snippet = ast.unparse(node) if hasattr(ast, "unparse") else "compare"
            self.findings.append((
                node.lineno,
                f"ast.IfExp used as left operand of Compare: {code_snippet}",
            ))
        for comp in node.comparators:
            if isinstance(comp, ast.IfExp):
                code_snippet = ast.unparse(node) if hasattr(ast, "unparse") else "compare"
                self.findings.append((
                    node.lineno,
                    f"ast.IfExp used as comparator operand of Compare: {code_snippet}",
                ))
        self.generic_visit(node)

    def visit_IfExp(self, node: ast.IfExp):
        # ast.IfExp whose body is a Compare or BoolOp
        if isinstance(node.body, (ast.Compare, ast.BoolOp)):
            code_snippet = ast.unparse(node) if hasattr(ast, "unparse") else "if_exp"
            self.findings.append((
                node.lineno,
                f"ast.IfExp whose body is a {type(node.body).__name__}: {code_snippet}",
            ))
        # Also flag if orelse is a Compare or BoolOp (e.g. x if cond else a >= y)
        if isinstance(node.orelse, (ast.Compare, ast.BoolOp)):
            code_snippet = ast.unparse(node) if hasattr(ast, "unparse") else "if_exp"
            self.findings.append((
                node.lineno,
                f"ast.IfExp whose orelse is a {type(node.orelse).__name__}: {code_snippet}",
            ))
        self.generic_visit(node)


def audit_file(path: Path) -> list[str]:
    try:
        content = path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(path))
    except Exception as e:
        return [f"{path}:1: Failed to parse: {e}"]

    visitor = ConditionalPrecedenceVisitor(str(path))
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
        rel = path.relative_to(repo_root)
        if is_test_file(rel):
            continue
        hits = audit_file(rel)
        findings.extend(hits)

    for hit in findings:
        print(hit)

    total = len(findings)
    print(f"\n[conditional_precedence] Total findings: {total}", file=sys.stderr)
    return 1 if total > 0 else 0


if __name__ == "__main__":
    sys.exit(main())

