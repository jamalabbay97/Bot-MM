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
    parsed = parser.parse_args(args)

    try:
        config = EngineConfig()
    except EnvironmentError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1

    if parsed.status or parsed.trades or parsed.signals or parsed.news:
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
