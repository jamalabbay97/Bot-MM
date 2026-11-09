#!/usr/bin/env python3
"""
tools/audit/native_price_literals.py
Audit script to detect hardcoded native price literals and getattr defaults in non-test code.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def is_test_file(path: Path) -> bool:
    parts = path.parts
    return any(p in ("tests", "test", ".venv", ".git", "__pycache__", "build", "dist") for p in parts) or path.name.startswith("test_")


class NativePriceVisitor(ast.NodeVisitor):
    def __init__(self, filename: str):
        self.filename = filename
        self.findings: list[tuple[int, str]] = []

    def _is_price_literal(self, node: ast.AST) -> tuple[bool, str]:
        """Check if node is a known native price numeric literal or Decimal call."""
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)) and node.value in (150, 150.0, 3400, 3400.0, 2500, 2500.0):
                return True, f"literal number {node.value}"
            if isinstance(node.value, str) and node.value in ("150.0", "150", "3400.0", "3400", "2500.0", "2500"):
                return True, f"literal string {node.value!r}"
        elif isinstance(node, ast.Call):
            # Check for Decimal("150.0") or similar
            func_name = ""
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                func_name = node.func.attr
            if func_name == "Decimal" and node.args:
                arg0 = node.args[0]
                if isinstance(arg0, ast.Constant) and str(arg0.value) in ("150.0", "150", "3400.0", "3400", "2500.0", "2500", "200.0", "200"):
                    return True, f"Decimal({arg0.value!r})"
        return False, ""

    def visit_Call(self, node: ast.Call):
        # 1. Check for getattr(obj, "sol_price_usd" | "eth_price_usd", <literal>)
        func_name = ""
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        if func_name == "getattr" and len(node.args) >= 3:
            attr_arg = node.args[1]
            if isinstance(attr_arg, ast.Constant) and attr_arg.value in (
                "sol_price_usd",
                "eth_price_usd",
                "sol_price",
                "eth_price",
                "native_price_usd",
            ):
                default_arg = node.args[2]
                default_repr = ast.unparse(default_arg) if hasattr(ast, "unparse") else "default"
                self.findings.append((
                    node.lineno,
                    f"getattr with hardcoded native price default: getattr(..., {attr_arg.value!r}, {default_repr})",
                ))

        # 2. Check for dict.get("sol_price_usd" | "eth_price_usd", <literal>)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "get" and len(node.args) >= 2:
            key_arg = node.args[0]
            if isinstance(key_arg, ast.Constant) and key_arg.value in (
                "sol_price_usd",
                "eth_price_usd",
                "sol_price",
                "eth_price",
            ):
                default_arg = node.args[1]
                default_repr = ast.unparse(default_arg) if hasattr(ast, "unparse") else "default"
                self.findings.append((
                    node.lineno,
                    f"dict.get with hardcoded native price default: .get({key_arg.value!r}, {default_repr})",
                ))

        # 3. Direct Decimal("150.0") / Decimal("3400.0") calls anywhere
        is_lit, desc = self._is_price_literal(node)
        if is_lit:
            # Check context: if not already flagged in getattr
            self.findings.append((
                node.lineno,
                f"Hardcoded native price constant: {desc}",
            ))

        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign):
        # Check target names like sol_price, eth_price assigned to literals
        for target in node.targets:
            target_name = ""
            if isinstance(target, ast.Name):
                target_name = target.id
            elif isinstance(target, ast.Attribute):
                target_name = target.attr
            if any(k in target_name.lower() for k in ("sol_price", "eth_price", "sol_p", "eth_p")):
                is_lit, desc = self._is_price_literal(node.value)
                if is_lit and not isinstance(node.value, ast.Call):  # Call handled in visit_Call
                    self.findings.append((
                        node.lineno,
                        f"Assignment to {target_name} with hardcoded native price literal: {desc}",
                    ))
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign):
        target_name = ""
        if isinstance(node.target, ast.Name):
            target_name = node.target.id
        elif isinstance(node.target, ast.Attribute):
            target_name = node.target.attr
        if any(k in target_name.lower() for k in ("sol_price", "eth_price")):
            if node.value is not None:
                # Check for Field(default=...)
                if isinstance(node.value, ast.Call):
                    func_name = ast.unparse(node.value.func) if hasattr(ast, "unparse") else ""
                    if "Field" in func_name:
                        for kw in node.value.keywords:
                            if kw.arg == "default":
                                is_lit, desc = self._is_price_literal(kw.value)
                                if is_lit:
                                    self.findings.append((
                                        node.lineno,
                                        f"Pydantic Field default for {target_name} hardcoded: {desc}",
                                    ))
                else:
                    is_lit, desc = self._is_price_literal(node.value)
                    if is_lit:
                        self.findings.append((
                            node.lineno,
                            f"Annotated assignment to {target_name} with hardcoded price: {desc}",
                        ))
        self.generic_visit(node)


def audit_file(path: Path) -> list[str]:
    try:
        content = path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(path))
    except Exception as e:
        return [f"{path}:1: Failed to parse: {e}"]

    visitor = NativePriceVisitor(str(path))
    visitor.visit(tree)

    results: list[str] = []
    # Deduplicate findings on same line with same message
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
        if is_test_file(path.relative_to(repo_root)):
            continue
        rel_path = path.relative_to(repo_root)
        hits = audit_file(rel_path)
        findings.extend(hits)

    for hit in findings:
        print(hit)

    total = len(findings)
    print(f"\n[native_price_literals] Total findings: {total}", file=sys.stderr)
    return 1 if total > 0 else 0


if __name__ == "__main__":
    sys.exit(main())

