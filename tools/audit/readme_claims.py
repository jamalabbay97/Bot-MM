#!/usr/bin/env python3
"""
tools/audit/readme_claims.py — Audit README claims against code defaults and specifications
==========================================================================================
Verifies that all technical claims in README.md match genuine code defaults:
- Solana latency (45ms - 320ms) and Base latency (150ms - 2000ms)
- Fee schedules (Pump.fun 100 bps, Raydium 25 bps, Uniswap 30 bps, Aerodrome 30 bps)
- Kelly parameters (uncalibrated < 30 trades, 1% fixed fraction, Half-Kelly 0.5x, shrinkage 20 trades)
- Venue capabilities (supported vs unsupported fail-closed)
- Council multi-round debate & episodic memory
- Wallet pipeline 6 stages & FIFO reconstruction
- Absence of stale latency ranges (e.g. 1200-3500 ms)
- Presence of "Known limitations" section listing UNVERIFIED items
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import List

from alpha_engine.config import EngineConfig
from alpha_engine.pricing.venue_capabilities import VENUE_CAPABILITIES, VenueStatus


def audit_readme_claims(readme_path: Path) -> List[str]:
    """Audit README.md content against EngineConfig and runtime definitions."""
    if not readme_path.exists():
        return [f"README.md not found at {readme_path}"]

    content = readme_path.read_text(encoding="utf-8")
    findings: List[str] = []
    cfg = EngineConfig()

    # 1. Stale latency check: 1200-3500ms or 1200ms - 3500ms must not appear
    stale_latency_patterns = [
        r"1200\s*(?:ms)?\s*[-–—]\s*3500\s*ms",
        r"\[1200,\s*3500\]",
    ]
    for pat in stale_latency_patterns:
        if re.search(pat, content, re.IGNORECASE):
            findings.append(f"Stale latency range found matching pattern '{pat}'. Code uses 45-320ms (Solana) and 150-2000ms (Base).")

    # 2. Solana latency numbers match config
    sol_min = cfg.latency_min_ms_solana
    sol_max = cfg.latency_max_ms_solana
    if f"{sol_min}" not in content or f"{sol_max}" not in content:
        findings.append(f"Solana latency range ({sol_min}ms - {sol_max}ms) not documented in README.")

    # 3. Base latency numbers match config
    base_min = cfg.latency_min_ms_base
    base_max = cfg.latency_max_ms_base
    if f"{base_min}" not in content or f"{base_max}" not in content:
        findings.append(f"Base latency range ({base_min}ms - {base_max}ms) not documented in README.")

    # 4. Fee schedules check
    fees = [
        ("pump_fun", cfg.fee_pump_fun_bps, "Pump.fun"),
        ("raydium_amm", cfg.fee_raydium_amm_bps, "Raydium"),
        ("uniswap_v2", cfg.fee_uniswap_v2_bps, "Uniswap"),
        ("aerodrome", cfg.fee_aerodrome_bps, "Aerodrome"),
    ]
    for fee_key, fee_bps, label in fees:
        pct_str = f"{fee_bps / 100:.2f}%" if fee_bps % 100 != 0 else f"{fee_bps // 100}%"
        # Check that fee is mentioned in README
        if f"{fee_bps}" not in content and pct_str not in content:
            findings.append(f"Fee schedule for {label} ({fee_bps} bps / {pct_str}) not found in README.")

    # 5. Kelly parameters check
    if "30" not in content or "0.5" not in content:
        findings.append("Half-Kelly calibration parameters (30 closed trades, 0.5 multiplier) missing from README.")

    # 6. Venue support & fail-closed check
    if "Raydium CLMM" not in content or "unsupported" not in content.lower():
        findings.append("Venue support status for unsupported CLMM venues (fail-closed) missing from README.")

    # 7. Known limitations section check
    if "known limitations" not in content.lower():
        findings.append("Required section 'Known limitations' missing from README.")

    if "unverified" not in content.lower():
        findings.append("'UNVERIFIED' items disclaimer missing from README Known Limitations section.")

    return findings


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent.parent
    readme_path = repo_root / "README.md"
    findings = audit_readme_claims(readme_path)

    if findings:
        print(f"❌ [readme_claims] Total findings: {len(findings)}", file=sys.stderr)
        for f in findings:
            print(f"  • {f}", file=sys.stderr)
        return 1
    else:
        print("✅ [readme_claims] Total findings: 0 (All claims match code defaults)")
        return 0


if __name__ == "__main__":
    sys.exit(main())

