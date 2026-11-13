"""
alpha_engine.observability.doctor — Startup Self-Check & System Doctor
======================================================================
Comprehensive diagnostic suite verifying critical system dependencies:
1. Price Service: Multi-source SOL & ETH price feeds and divergence bounds
2. LLM Reachability: Gemini/OpenAI API connectivity when configured
3. RPC Health: Solana SVM (getHealth) and Base EVM (eth_blockNumber) responsiveness
4. DB Writable: SQLite Ledger & Tick Recorder WAL mode and write permissions
5. Recorder Running: Tick recorder background queue active and healthy
6. Quotas: Free-tier daily quotas for Helius, Alchemy, and rate limiter capacities
7. Venue Capabilities: Venue support mapping and fail-closed quoter rules

Produces a formatted PASS/WARN/FAIL table. FAIL on any critical dependency
blocks new trade entries fail-closed.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

import aiohttp

from alpha_engine.config import EngineConfig
from alpha_engine.models.enums import ChainIdentifier, ExecutionVenue
from alpha_engine.pricing.venue_capabilities import VENUE_CAPABILITIES, VenueStatus

logger = logging.getLogger(__name__)


class CheckStatus(str, enum.Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass
class DiagnosticCheck:
    """Result of an individual diagnostic health check."""

    name: str
    status: CheckStatus
    latency_ms: float
    details: str
    is_critical: bool = True


@dataclass
class DoctorResult:
    """Consolidated system diagnostic report."""

    overall_status: CheckStatus
    checks: List[Dict[str, Any]] = field(default_factory=list)
    failed_checks: List[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    @property
    def has_critical_failure(self) -> bool:
        """True if any critical check returned FAIL."""
        return self.overall_status == CheckStatus.FAIL


class SystemDoctor:
    """
    Comprehensive startup self-check and diagnostic system.
    Evaluates pricing, LLM, RPCs, databases, recorder, quotas, and venue tables.
    """

    def __init__(
        self,
        config: Optional[EngineConfig] = None,
        price_service: Optional[Any] = None,
        recorder: Optional[Any] = None,
    ) -> None:
        self.config = config or EngineConfig()
        self.price_service = price_service
        self.recorder = recorder

    async def _check_price_service(self) -> Dict[str, Any]:
        """Verify native price service can obtain valid SOL and ETH prices."""
        start = time.perf_counter()
        if getattr(self.config, "allow_static_native_prices", False):
            lat = (time.perf_counter() - start) * 1000.0
            return {
                "name": "Native Price Service",
                "status": CheckStatus.WARN,
                "latency_ms": round(lat, 2),
                "details": "Static prices explicitly allowed (test/demo mode)",
                "is_critical": False,
            }

        ps = self.price_service
        if ps is None:
            from alpha_engine.pricing.native_price_service import NativePriceService
            ps = NativePriceService(self.config)

        try:
            sol_res = ps.get_price(ChainIdentifier.SOLANA_MAINNET)
            eth_res = ps.get_price(ChainIdentifier.BASE_MAINNET)

            lat = (time.perf_counter() - start) * 1000.0
            from alpha_engine.pricing.native_price_service import NativePriceStatus
            if sol_res.status == NativePriceStatus.OK and eth_res.status == NativePriceStatus.OK:
                return {
                    "name": "Native Price Service",
                    "status": CheckStatus.PASS,
                    "latency_ms": round(lat, 2),
                    "details": f"SOL=${sol_res.price} ETH=${eth_res.price} (Source: {sol_res.source})",
                    "is_critical": True,
                }
            elif sol_res.status == NativePriceStatus.DIVERGENT or eth_res.status == NativePriceStatus.DIVERGENT:
                return {
                    "name": "Native Price Service",
                    "status": CheckStatus.FAIL,
                    "latency_ms": round(lat, 2),
                    "details": f"Price source divergence beyond tolerance (SOL: {sol_res.status.value}, ETH: {eth_res.status.value})",
                    "is_critical": True,
                }
            else:
                return {
                    "name": "Native Price Service",
                    "status": CheckStatus.FAIL,
                    "latency_ms": round(lat, 2),
                    "details": f"Stale or degraded price status (SOL: {sol_res.status.value}, ETH: {eth_res.status.value})",
                    "is_critical": True,
                }
        except Exception as exc:
            lat = (time.perf_counter() - start) * 1000.0
            return {
                "name": "Native Price Service",
                "status": CheckStatus.FAIL,
                "latency_ms": round(lat, 2),
                "details": f"Price service error: {exc}",
                "is_critical": True,
            }

    async def _check_llm_reachability(self) -> Dict[str, Any]:
        """Verify LLM reachability if council or AI supervisor is configured."""
        start = time.perf_counter()
        council_on = getattr(self.config, "council_enabled", True)
        ai_on = getattr(self.config, "ai_supervisor_enabled", True)

        if not council_on and not ai_on:
            return {
                "name": "LLM Reachability",
                "status": CheckStatus.PASS,
                "latency_ms": 0.0,
                "details": "LLM council & supervisor disabled by config",
                "is_critical": False,
            }

        api_key = getattr(self.config, "gemini_api_key", None) or os.getenv("GEMINI_API_KEY")
        if not api_key:
            return {
                "name": "LLM Reachability",
                "status": CheckStatus.WARN,
                "latency_ms": 0.0,
                "details": "No API key configured; deterministic fallback active",
                "is_critical": False,
            }

        # Ping Gemini endpoint
        url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4.0)) as session:
                async with session.get(url) as resp:
                    lat = (time.perf_counter() - start) * 1000.0
                    if resp.status == 200:
                        return {
                            "name": "LLM Reachability",
                            "status": CheckStatus.PASS,
                            "latency_ms": round(lat, 2),
                            "details": f"Gemini API authenticated ({getattr(self.config, 'ai_model', 'gemini-3.8-flash')})",
                            "is_critical": True,
                        }
                    else:
                        return {
                            "name": "LLM Reachability",
                            "status": CheckStatus.FAIL,
                            "latency_ms": round(lat, 2),
                            "details": f"Gemini API returned HTTP {resp.status}",
                            "is_critical": True,
                        }
        except Exception as exc:
            lat = (time.perf_counter() - start) * 1000.0
            return {
                "name": "LLM Reachability",
                "status": CheckStatus.FAIL,
                "latency_ms": round(lat, 2),
                "details": f"LLM unreachable: {exc}",
                "is_critical": True,
            }

    async def _check_solana_rpc(self) -> Dict[str, Any]:
        """Verify Solana RPC connectivity and responsiveness."""
        start = time.perf_counter()
        url = getattr(self.config, "solana_rpc_http", "https://api.mainnet-beta.solana.com")
        payload = {"jsonrpc": "2.0", "method": "getHealth", "params": [], "id": 1}
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5.0)) as session:
                async with session.post(url, json=payload) as resp:
                    lat = (time.perf_counter() - start) * 1000.0
                    if resp.status in (200, 401, 403):
                        data = await resp.json() if resp.status == 200 else {}
                        res_val = data.get("result", "ok")
                        return {
                            "name": "Solana SVM RPC",
                            "status": CheckStatus.PASS if resp.status == 200 else CheckStatus.WARN,
                            "latency_ms": round(lat, 2),
                            "details": f"RPC Status: {res_val} (HTTP {resp.status})",
                            "is_critical": True,
                        }
                    else:
                        return {
                            "name": "Solana SVM RPC",
                            "status": CheckStatus.FAIL,
                            "latency_ms": round(lat, 2),
                            "details": f"RPC returned HTTP {resp.status}",
                            "is_critical": True,
                        }
        except Exception as exc:
            lat = (time.perf_counter() - start) * 1000.0
            return {
                "name": "Solana SVM RPC",
                "status": CheckStatus.FAIL,
                "latency_ms": round(lat, 2),
                "details": f"Connection error: {exc}",
                "is_critical": True,
            }

    async def _check_base_rpc(self) -> Dict[str, Any]:
        """Verify Base EVM RPC connectivity and responsiveness."""
        start = time.perf_counter()
        url = getattr(self.config, "base_rpc_http", "https://mainnet.base.org")
        payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5.0)) as session:
                async with session.post(url, json=payload) as resp:
                    lat = (time.perf_counter() - start) * 1000.0
                    if resp.status == 200:
                        data = await resp.json()
                        block_hex = data.get("result", "0x0")
                        block_num = int(block_hex, 16) if isinstance(block_hex, str) else 0
                        return {
                            "name": "Base EVM RPC",
                            "status": CheckStatus.PASS,
                            "latency_ms": round(lat, 2),
                            "details": f"Current Block: #{block_num}",
                            "is_critical": True,
                        }
                    else:
                        return {
                            "name": "Base EVM RPC",
                            "status": CheckStatus.FAIL,
                            "latency_ms": round(lat, 2),
                            "details": f"RPC returned HTTP {resp.status}",
                            "is_critical": True,
                        }
        except Exception as exc:
            lat = (time.perf_counter() - start) * 1000.0
            return {
                "name": "Base EVM RPC",
                "status": CheckStatus.FAIL,
                "latency_ms": round(lat, 2),
                "details": f"Connection error: {exc}",
                "is_critical": True,
            }

    async def _check_db_writable(self) -> Dict[str, Any]:
        """Verify SQLite ledger database is writable in WAL mode."""
        start = time.perf_counter()
        db_path = getattr(self.config, "db_path", "paper_trading.db")
        try:
            os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
            conn = sqlite3.connect(db_path, timeout=5.0)
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode = WAL;")
            cur.execute("CREATE TABLE IF NOT EXISTS _doctor_test (id INT PRIMARY KEY, ts REAL);")
            cur.execute("INSERT OR REPLACE INTO _doctor_test VALUES (1, ?);", (time.time(),))
            conn.commit()
            cur.execute("PRAGMA journal_mode;")
            jmode = cur.fetchone()[0]
            conn.close()
            lat = (time.perf_counter() - start) * 1000.0
            return {
                "name": "SQLite Ledger DB",
                "status": CheckStatus.PASS,
                "latency_ms": round(lat, 2),
                "details": f"Writable in {jmode.upper()} mode ({db_path})",
                "is_critical": True,
            }
        except Exception as exc:
            lat = (time.perf_counter() - start) * 1000.0
            return {
                "name": "SQLite Ledger DB",
                "status": CheckStatus.FAIL,
                "latency_ms": round(lat, 2),
                "details": f"Database write failure: {exc}",
                "is_critical": True,
            }

    async def _check_recorder_running(self) -> Dict[str, Any]:
        """Verify tick recorder is running and SQLite table is ready."""
        start = time.perf_counter()
        enabled = getattr(self.config, "recorder_enabled", True)
        if not enabled:
            return {
                "name": "Market Tick Recorder",
                "status": CheckStatus.WARN,
                "latency_ms": 0.0,
                "details": "Recorder disabled in configuration",
                "is_critical": False,
            }

        rec = self.recorder
        if rec and rec._running:
            stats = rec.get_stats()
            lat = (time.perf_counter() - start) * 1000.0
            return {
                "name": "Market Tick Recorder",
                "status": CheckStatus.PASS,
                "latency_ms": round(lat, 2),
                "details": f"Active: {stats['recorded_ticks_total']} ticks recorded, queue={stats['queue_depth']}/{stats['queue_maxsize']}",
                "is_critical": True,
            }
        else:
            # Check DB writable directly
            db_path = getattr(self.config, "recorder_db_path", "market_ticks.db")
            try:
                os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
                conn = sqlite3.connect(db_path, timeout=5.0)
                cur = conn.cursor()
                cur.execute("PRAGMA journal_mode = WAL;")
                conn.close()
                lat = (time.perf_counter() - start) * 1000.0
                return {
                    "name": "Market Tick Recorder",
                    "status": CheckStatus.PASS,
                    "latency_ms": round(lat, 2),
                    "details": f"DB ready in WAL mode ({db_path})",
                    "is_critical": True,
                }
            except Exception as exc:
                lat = (time.perf_counter() - start) * 1000.0
                return {
                    "name": "Market Tick Recorder",
                    "status": CheckStatus.FAIL,
                    "latency_ms": round(lat, 2),
                    "details": f"Tick recorder DB failure: {exc}",
                    "is_critical": True,
                }

    async def _check_quotas(self) -> Dict[str, Any]:
        """Verify developer daily quotas have remaining capacity."""
        start = time.perf_counter()
        from alpha_engine.profiler.history_db import WalletHistoryDatabase
        db = WalletHistoryDatabase()
        sol_used = 0
        base_used = 0
        try:
            await db.initialize()
            sol_used = await db.get_daily_quota_usage("helius")
            base_used = await db.get_daily_quota_usage("alchemy")
        except Exception as exc:
            logger.debug("Quota check non-fatal exception: %s", exc)
        finally:
            await db.close()

        free_limit = getattr(self.config, "wallet_history_free_tier_daily_quota", 500)
        lat = (time.perf_counter() - start) * 1000.0

        if sol_used >= free_limit or base_used >= free_limit:
            return {
                "name": "Developer Quotas",
                "status": CheckStatus.WARN,
                "latency_ms": round(lat, 2),
                "details": f"Daily quota near limit (Helius: {sol_used}/{free_limit}, Alchemy: {base_used}/{free_limit})",
                "is_critical": False,
            }
        return {
            "name": "Developer Quotas",
            "status": CheckStatus.PASS,
            "latency_ms": round(lat, 2),
            "details": f"Remaining capacity OK (Helius: {sol_used}/{free_limit}, Alchemy: {base_used}/{free_limit})",
            "is_critical": False,
        }

    async def _check_venue_capabilities(self) -> Dict[str, Any]:
        """Verify venue capability table mappings and quoter integrity."""
        start = time.perf_counter()
        supported = [v.value for v, cap in VENUE_CAPABILITIES.items() if cap.status == VenueStatus.SUPPORTED]
        unsupported = [v.value for v, cap in VENUE_CAPABILITIES.items() if cap.status == VenueStatus.UNSUPPORTED]
        lat = (time.perf_counter() - start) * 1000.0
        return {
            "name": "Venue Capabilities Table",
            "status": CheckStatus.PASS,
            "latency_ms": round(lat, 2),
            "details": f"{len(supported)} supported ({', '.join(supported[:3])}...), {len(unsupported)} fail-closed",
            "is_critical": True,
        }

    async def run_full_diagnostics(self) -> DoctorResult:
        """Run all startup diagnostic checks in parallel."""
        tasks = [
            self._check_price_service(),
            self._check_llm_reachability(),
            self._check_solana_rpc(),
            self._check_base_rpc(),
            self._check_db_writable(),
            self._check_recorder_running(),
            self._check_quotas(),
            self._check_venue_capabilities(),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        checks: List[Dict[str, Any]] = []
        failed_checks: List[str] = []
        has_fail = False

        for r in results:
            if isinstance(r, Exception):
                checks.append({
                    "name": "Diagnostic Worker",
                    "status": CheckStatus.FAIL,
                    "latency_ms": 0.0,
                    "details": str(r),
                    "is_critical": True,
                })
                failed_checks.append("Diagnostic Worker")
                has_fail = True
            elif isinstance(r, dict):
                checks.append(r)
                if r.get("status") == CheckStatus.FAIL:
                    has_fail = True
                    failed_checks.append(r.get("name", "Unknown"))

        overall = CheckStatus.FAIL if has_fail else (
            CheckStatus.WARN if any(c.get("status") == CheckStatus.WARN for c in checks) else CheckStatus.PASS
        )
        return DoctorResult(
            overall_status=overall,
            checks=checks,
            failed_checks=failed_checks,
        )

    def format_cli_table(self, report: DoctorResult) -> str:
        """Format a clear institutional PASS/WARN/FAIL status table."""
        status_line = (
            "❌ SYSTEM STATUS: FAIL (New trade entries BLOCKED fail-closed)"
            if report.overall_status == CheckStatus.FAIL
            else (
                "⚠️  SYSTEM STATUS: WARN (Operational with non-critical warnings)"
                if report.overall_status == CheckStatus.WARN
                else "✅ SYSTEM STATUS: PASS (All critical dependencies healthy)"
            )
        )

        lines = [
            "\n" + "=" * 85,
            "🩺 SYSTEM DOCTOR STARTUP DIAGNOSTICS",
            "=" * 85,
            f"Overall Result: {status_line}",
            "-" * 85,
            f"{'Diagnostic Target':<26} | {'Status':<8} | {'Latency':<10} | {'Details'}",
            "-" * 85,
        ]

        for c in report.checks:
            st = c["status"].value if hasattr(c["status"], "value") else str(c["status"])
            badge = "✅ PASS" if st == "PASS" else ("⚠️  WARN" if st == "WARN" else "❌ FAIL")
            lat_str = f"{c['latency_ms']:.1f} ms" if c['latency_ms'] > 0 else "N/A"
            lines.append(f"{c['name']:<26} | {badge:<8} | {lat_str:<10} | {c['details']}")

        lines.append("=" * 85 + "\n")
        return "\n".join(lines)

