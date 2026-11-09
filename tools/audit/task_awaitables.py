#!/usr/bin/env python3
"""
tools/audit/task_awaitables.py
Audit script to detect synchronous functions passed to asyncio.create_task,
asyncio.ensure_future, asyncio.gather, or awaited directly.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def is_test_file(path: Path) -> bool:
    parts = path.parts
    return any(p in ("tests", "test", ".venv", ".git", "__pycache__", "build", "dist") for p in parts) or path.name.startswith("test_")


# Known stdlib or third-party coroutine methods that might share names with sync methods
STDLIB_ASYNC_METHODS = {
    "put", "get", "join", "task_done", "wait", "close", "read", "write",
    "send", "recv", "receive", "acquire", "release", "commit", "rollback",
    "execute", "fetchall", "fetchone", "fetchmany", "serve", "run", "start", "stop"
}


class FunctionDefCollector(ast.NodeVisitor):
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.current_class: str | None = None
        # (func_name, class_name) -> is_async
        self.definitions: dict[tuple[str, str | None], bool] = {}

    def visit_ClassDef(self, node: ast.ClassDef):
        prev = self.current_class
        self.current_class = node.name
        self.generic_visit(node)
        self.current_class = prev

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self.definitions[(node.name, self.current_class)] = False
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        self.definitions[(node.name, self.current_class)] = True
        self.generic_visit(node)


class TaskAwaitableVisitor(ast.NodeVisitor):
    def __init__(
        self,
        filepath: str,
        func_db: dict[str, set[bool]],
        qualified_db: dict[tuple[str, str], bool],
        top_level_funcs: dict[str, bool],
    ):
        self.filepath = filepath
        self.func_db = func_db
        self.qualified_db = qualified_db
        self.top_level_funcs = top_level_funcs
        self.findings: list[tuple[int, str]] = []

    def _resolve_call_target(self, node: ast.AST) -> tuple[str, str | None, bool]:
        """
        Returns (func_name, possible_class_or_obj_name, is_call).
        """
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                return func.id, None, True
            elif isinstance(func, ast.Attribute):
                obj_name = None
                if isinstance(func.value, ast.Name):
                    obj_name = func.value.id
                elif isinstance(func.value, ast.Attribute):
                    obj_name = func.value.attr
                return func.attr, obj_name, True
        elif isinstance(node, ast.Name):
            return node.id, None, False
        elif isinstance(node, ast.Attribute):
            return node.attr, None, False
        return "", None, False

    def _check_target(self, target_expr: ast.AST, context: str, lineno: int):
        func_name, obj_name, is_call = self._resolve_call_target(target_expr)
        if not func_name or func_name in STDLIB_ASYNC_METHODS:
            return

        if not is_call and context in ("create_task", "ensure_future"):
            # Check if this name is a known top-level function that was passed without calling it
            if func_name in self.top_level_funcs:
                self.findings.append((
                    lineno,
                    f"{context} called with uncalled function reference {func_name!r} (expected coroutine call)",
                ))
            return

        # Attempt resolution via obj_name -> class name matching
        matching_async_states: set[bool] = set()

        if obj_name:
            norm_obj = obj_name.replace("_", "").lower()
            for (fn, cls_name), is_async in self.qualified_db.items():
                if fn == func_name and cls_name:
                    norm_cls = cls_name.replace("_", "").lower()
                    if norm_obj in norm_cls or norm_cls in norm_obj:
                        matching_async_states.add(is_async)

        if not matching_async_states and func_name in self.top_level_funcs:
            matching_async_states.add(self.top_level_funcs[func_name])
        elif not matching_async_states and func_name in self.func_db:
            matching_async_states = self.func_db[func_name]

        # If all known definitions in the repo are synchronous (False)
        if matching_async_states == {False}:
            self.findings.append((
                lineno,
                f"{context} target resolves to synchronous def {func_name!r} in the codebase",
            ))

    def visit_Call(self, node: ast.Call):
        is_create_task = False
        is_ensure_future = False
        is_gather = False

        if isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            if attr == "create_task":
                is_create_task = True
            elif attr == "ensure_future":
                is_ensure_future = True
            elif attr == "gather":
                is_gather = True
        elif isinstance(node.func, ast.Name):
            if node.func.id == "create_task":
                is_create_task = True
            elif node.func.id == "ensure_future":
                is_ensure_future = True
            elif node.func.id == "gather":
                is_gather = True

        if (is_create_task or is_ensure_future) and node.args:
            ctx = "create_task" if is_create_task else "ensure_future"
            self._check_target(node.args[0], ctx, node.lineno)

        if is_gather and node.args:
            for arg in node.args:
                self._check_target(arg, "asyncio.gather", node.lineno)

        self.generic_visit(node)

    def visit_Await(self, node: ast.Await):
        self._check_target(node.value, "await", node.lineno)
        self.generic_visit(node)


def build_repo_function_database(
    repo_root: Path,
) -> tuple[dict[str, set[bool]], dict[tuple[str, str], bool], dict[str, bool]]:
    func_db: dict[str, set[bool]] = {}
    qualified_db: dict[tuple[str, str], bool] = {}
    top_level_funcs: dict[str, bool] = {}

    for path in repo_root.rglob("*.py"):
        rel = path.relative_to(repo_root)
        if is_test_file(rel):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            collector = FunctionDefCollector(str(path))
            collector.visit(tree)
            for (fn, cls_name), is_async in collector.definitions.items():
                func_db.setdefault(fn, set()).add(is_async)
                if cls_name:
                    qualified_db[(fn, cls_name)] = is_async
                else:
                    top_level_funcs[fn] = is_async
        except Exception:
            continue

    return func_db, qualified_db, top_level_funcs


def audit_file(
    path: Path,
    func_db: dict[str, set[bool]],
    qualified_db: dict[tuple[str, str], bool],
    top_level_funcs: dict[str, bool],
) -> list[str]:
    try:
        content = path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(path))
    except Exception as e:
        return [f"{path}:1: Failed to parse: {e}"]

    visitor = TaskAwaitableVisitor(str(path), func_db, qualified_db, top_level_funcs)
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
    func_db, qualified_db, top_level_funcs = build_repo_function_database(repo_root)
    findings: list[str] = []

    for path in sorted(repo_root.rglob("*.py")):
        rel = path.relative_to(repo_root)
        if is_test_file(rel):
            continue
        hits = audit_file(rel, func_db, qualified_db, top_level_funcs)
        findings.extend(hits)

    for hit in findings:
        print(hit)

    total = len(findings)
    print(f"\n[task_awaitables] Total findings: {total}", file=sys.stderr)
    return 1 if total > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
