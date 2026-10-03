"""
alpha_engine.cli — Command-Line Interface
=========================================
Multi-Chain Paper Trading & Alpha Analytics Engine
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine, _build_example_config


def main(args: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="alpha-engine",
        description="Multi-Chain Paper Trading & Alpha Analytics Engine",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run engine with example demo pool configurations",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print latest portfolio and engine telemetry",
    )
    parser.add_argument(
        "--trades",
        action="store_true",
        help="Display recent executed paper trades from ledger",
    )
    parser.add_argument(
        "--signals",
        action="store_true",
        help="Display recent token screening signals",
    )
    parser.add_argument(
        "--news",
        action="store_true",
        help="Display recent news ingestion and sentiment logs",
    )
    parser.add_argument(
        "--whales",
        action="store_true",
        help="Display recent on-chain whale alerts",
    )
    parser.add_argument(
        "--ai",
        action="store_true",
        help="Display AlphaSupervisor-AI risk posture and status",
    )
    parser.add_argument(
        "--audit",
        type=str,
        metavar="TOKEN_ADDRESS",
        help="Execute an on-demand AlphaSupervisor-AI forensics audit on a token address",
    )
    parsed = parser.parse_args(args)

    try:
        config = EngineConfig()
    except EnvironmentError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1


    if parsed.ai:
        from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
        supervisor = AlphaSupervisorAI(config=config)
        status = supervisor.get_status()
        print("=== AlphaSupervisor-AI Status ===")
        print(f"Status: Active ({status['provider']} - {status['model']})")
        print(f"Global Risk Mode: {status['global_risk_mode']}")
        print(f"Total Audits: {status['total_audits']} (Approved: {status['approved_buys']}, Vetoed: {status['vetoed_signals']})")
        print(f"Blacklisted Entities: {status['blacklisted_entities_count']}")
        return 0

    if parsed.audit:
        from decimal import Decimal
        from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
        from alpha_engine.models.enums import ChainIdentifier, OrderSide, SignalStrength
        from alpha_engine.models.events import SignalEvent
        from alpha_engine.models.state import PoolState
        token_ca = parsed.audit.strip()
        is_evm = token_ca.startswith("0x") and len(token_ca) == 42
        chain = ChainIdentifier.BASE_MAINNET if is_evm else ChainIdentifier.SOLANA_MAINNET
        pool = PoolState(
            pool_address="0x" + "0" * 40 if is_evm else "1" * 32,
            chain=chain,
            token_reserve=Decimal("1000000.0"),
            native_reserve=Decimal("50.0") if chain == ChainIdentifier.SOLANA_MAINNET else Decimal("2.0"),
            fee_numerator=3,
            fee_denominator=1000,
            last_updated_block=1,
        )
        import time
        from alpha_engine.models.enums import SecurityTier
        from alpha_engine.models.state import SecurityReport
        report = SecurityReport(
            token_address=token_ca,
            chain=chain,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
            lp_burned_ratio=1.0,
            top10_concentration=0.10,
            mint_authority_disabled=True,
            verified_source_code=True,
        )
        sig = SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=chain,
            pool_address=pool.pool_address,
            token_address=token_ca,
            suggested_side=OrderSide.BUY,
            pool_state=pool,
            security_report=report,
            strength=SignalStrength.STRONG,
            alpha_score=0.85,
        )
        supervisor = AlphaSupervisorAI(config=config)
        async def _run_audit():
            try:
                return await supervisor.audit_signal(signal=sig, pool_state=pool, security_report=report)
            finally:
                await supervisor.close()

        resp = asyncio.run(_run_audit())
        print(f"=== AlphaSupervisor-AI Audit: {token_ca} ({chain.value}) ===")
        print(resp.to_strict_json())
        return 0


    if parsed.status or parsed.trades or parsed.signals or parsed.news or parsed.whales:
        import sqlite3

        try:
            conn = sqlite3.connect(config.db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            if parsed.trades or parsed.status:
                cur.execute(
                    "SELECT chain, token_address, side, effective_price, realized_pnl_usd, created_at "
                    "FROM trades ORDER BY created_at DESC LIMIT 10"
                )
                rows = cur.fetchall()
                print(f"=== Last {len(rows)} Trades ({config.db_path}) ===")
                for r in rows:
                    pnl = f"+${r['realized_pnl_usd']:.2f}" if r['realized_pnl_usd'] else "OPEN"
                    print(f"{r['created_at']} | {r['chain']} | {r['side']} | {r['token_address'][:12]}.. | Fill: {r['effective_price']} | PnL: {pnl}")
            if parsed.signals or parsed.status:
                cur.execute(
                    "SELECT signal_id, chain, token_address, suggested_side, alpha_score, strength, created_at "
                    "FROM signals ORDER BY created_at DESC LIMIT 5"
                )
                rows = cur.fetchall()
                print(f"\n=== Last {len(rows)} Signals ===")
                for r in rows:
                    print(f"{r['created_at']} | {r['chain']} | {r['suggested_side']} | {r['token_address'][:12]}.. | Alpha: {r['alpha_score']:.2f} ({r['strength']})")
            if parsed.news or parsed.status:
                print("\n=== News Feed & Sentiment Buffer ===")
                print("Telemetry buffer accessible via active Telegram DM interface (/news)")
            if parsed.whales or parsed.status:
                print("\n=== Whale Signals Buffer ===")
                print("Telemetry buffer accessible via active Telegram DM interface (/whales)")
            conn.close()
        except Exception as exc:
            print(f"Database query error: {exc}", file=sys.stderr)
        return 0

    engine = PaperTradingEngine(config)
    watchlist, svm_reg, seed_states = _build_example_config()

    try:
        asyncio.run(
            engine.run(
                pool_watchlist=watchlist,
                svm_pool_registry=svm_reg,
                seed_pool_states=seed_states,
            )
        )
    except KeyboardInterrupt:
        print("\nShutdown by user.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
