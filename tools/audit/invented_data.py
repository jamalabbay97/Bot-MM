#!/usr/bin/env python3
"""
tools/audit/invented_data.py
Audit script to detect hardcoded fallback reserves, supply, and price literals:
e.g. 30.0 SOL, 5.0 ETH, 1073000000, 1000000000 supply, 0.000000028
in non-test code.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def is_test_file(path: Path) -> bool:
    parts = path.parts
    return any(p in ("tests", "test", ".venv", ".git", "__pycache__", "build", "dist") for p in parts) or path.name.startswith("test_")


class InventedDataVisitor(ast.NodeVisitor):
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.findings: list[tuple[int, str]] = []

    def _is_ns_conversion(self, node: ast.AST) -> bool:
        """Check if node is part of a nanosecond calculation like seconds * 1_000_000_000."""
        # AST parent checks are easier via context, but we can check if inside a BinOp with 86400 or 1e9 or 3600
        return False

    def visit_Constant(self, node: ast.Constant):
        val = node.value
        # 1. 0.000000028 price literal
        if val == 0.000000028 or val == "0.000000028":
            self.findings.append((
                node.lineno,
                f"Hardcoded invented price fallback literal: {val!r}",
            ))
        # 2. 1073000000 pump.fun virtual token reserve
        elif val in (1073000000, 1073000000.0, "1073000000", "1073000000.0"):
            self.findings.append((
                node.lineno,
                f"Hardcoded invented virtual token reserve literal: {val!r}",
            ))
        # 3. 1000000000 supply literal (when string or Decimal, or in market/supply context)
        elif val in ("1000000000", "1000000000.0"):
            self.findings.append((
                node.lineno,
                f"Hardcoded invented token supply literal string: {val!r}",
            ))
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        func_name = ast.unparse(node.func) if hasattr(ast, "unparse") else ""
        if "Decimal" in func_name and node.args:
            arg = node.args[0]
            if isinstance(arg, ast.Constant):
                sval = str(arg.value)
                if sval in ("0.000000028", "1073000000", "1073000000.0", "1000000000", "1000000000.0"):
                    self.findings.append((
                        node.lineno,
                        f"Hardcoded invented market Decimal({arg.value!r})",
                    ))
                elif sval in ("30.0", "30", "5.0", "5"):
                    # Check if likely reserve
                    self.findings.append((
                        node.lineno,
                        f"Hardcoded fallback reserve Decimal({arg.value!r})",
                    ))

        # Check keyword arguments like token_reserve=1000000000 or virtual_sol_reserves=...
        for kw in node.keywords:
            if kw.arg in (
                "token_reserve",
                "native_reserve",
                "virtual_sol_reserves",
                "virtual_token_reserves",
                "circulating_supply",
                "total_supply",
                "initial_pool_reserve_native",
            ):
                kw_val = ast.unparse(kw.value) if hasattr(ast, "unparse") else ""
                if any(x in kw_val for x in ("30", "5", "1073000000", "1000000000")):
                    self.findings.append((
                        node.lineno,
                        f"Hardcoded market parameter in call kwarg {kw.arg}={kw_val}",
                    ))

        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign):
        for t in node.targets:
            name = ast.unparse(t).lower() if hasattr(ast, "unparse") else ""
            if any(k in name for k in ("reserve", "native_res", "token_res", "v_sol", "v_tok", "supply")):
                # Exclude if variable is ns / timestamp related
                if "_ns" in name or "time" in name:
                    continue
                val_str = ast.unparse(node.value) if hasattr(ast, "unparse") else ""
                if any(x in val_str for x in ("30.0", "30", "5.0", "1073000000", "1000000000", "0.000000028")):
                    self.findings.append((
                        node.lineno,
                        f"Hardcoded fallback market assignment {name} = {val_str}",
                    ))
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign):
        name = ast.unparse(node.target).lower() if hasattr(ast, "unparse") else ""
        if any(k in name for k in ("reserve", "reserves", "supply", "initial_price")):
            if node.value is not None:
                val_str = ast.unparse(node.value) if hasattr(ast, "unparse") else ""
                if any(x in val_str for x in ("30.0", "30", "5.0", "1073000000", "1000000000", "0.000000028")):
                    self.findings.append((
                        node.lineno,
                        f"Hardcoded fallback market default in annotation {name} = {val_str}",
                    ))
        self.generic_visit(node)


def audit_file(path: Path) -> list[str]:
    try:
        content = path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(path))
    except Exception as e:
        return [f"{path}:1: Failed to parse: {e}"]

    visitor = InventedDataVisitor(str(path))
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
    print(f"\n[invented_data] Total findings: {total}", file=sys.stderr)
    return 1 if total > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
