#!/usr/bin/env python3
"""
tools/audit/run_all.py
Runs all static audit scripts, writes audit_baseline.txt, and prints a summary.
Pure stdlib. Exit code 1 if any findings exist.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


SCRIPTS = [
    ("native_price_literals", "native_price_literals.py"),
    ("task_awaitables", "task_awaitables.py"),
    ("model_getattr_defaults", "model_getattr_defaults.py"),
    ("conditional_precedence", "conditional_precedence.py"),
    ("swallowed_exceptions", "swallowed_exceptions.py"),
    ("invented_data", "invented_data.py"),
    ("unused_config", "unused_config.py"),
]


def main() -> int:
    audit_dir = Path(__file__).resolve().parent
    repo_root = audit_dir.parent.parent
    baseline_file = repo_root / "audit_baseline.txt"

    report_lines: list[str] = []
    summary_lines: list[str] = []
    total_findings_all = 0

    header = "================================================================================\n" \
             "BOT-MM STATIC CODE AUDIT BASELINE REPORT\n" \
             "================================================================================\n"
    report_lines.append(header)

    for name, filename in SCRIPTS:
        script_path = audit_dir / filename
        cmd = [sys.executable, str(script_path)]

        proc = subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True)
        stdout = proc.stdout.strip()
        stderr = proc.stderr.strip()
        if stderr and proc.returncode not in (0, 1):
            report_lines.append(f"Errors:\n{stderr}\n")

        # Count findings lines (exclude blank lines and stderr total line)
        lines = [line for line in stdout.splitlines() if line.strip()]
        count = len(lines)
        total_findings_all += count

        section_header = f"\n--- [{name}] (Findings: {count}) ---\n"
        report_lines.append(section_header)
        if stdout:
            report_lines.append(stdout + "\n")
        else:
            report_lines.append("No findings.\n")

        summary_lines.append(f"  - {name}: {count} findings")
        print(f"[{name}] Completed: {count} findings (exit code {proc.returncode})", file=sys.stderr)

    summary_block = (
        "\n================================================================================\n"
        "AUDIT SUMMARY BY SCRIPT\n"
        "================================================================================\n"
        + "\n".join(summary_lines)
        + f"\n\nTotal findings across all scripts: {total_findings_all}\n"
    )
    report_lines.append(summary_block)

    full_report = "".join(report_lines)
    baseline_file.write_text(full_report, encoding="utf-8")
    print(f"\nAudit baseline written to {baseline_file}", file=sys.stderr)
    print(summary_block, file=sys.stderr)

    return 1 if total_findings_all > 0 else 0


if __name__ == "__main__":
    sys.exit(main())

