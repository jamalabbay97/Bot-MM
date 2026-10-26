"""
alpha_engine.cli — Command-Line Interface
=========================================
Multi-Chain Paper Trading & Alpha Analytics Engine
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine, _build_example_config
from alpha_engine.models.enums import resolve_trade_platform


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
    parser.add_argument(
        "--chat",
        action="store_true",
        help="Launch the interactive Conversational AI supervisor interface",
    )
    parser.add_argument(
        "--decisions",
        action="store_true",
        help="Display recent structured DecisionRecord audit logs",
    )
    parser.add_argument(
        "--reset-governor",
        action="store_true",
        help="Manually reset the trade frequency and pacing governor cooldown and trade counter",
    )
    parser.add_argument(
        "--doctor",
        action="store_true",
        help="Run system doctor diagnostics",
    )
    parser.add_argument(
        "--swarm",
        action="store_true",
        help="Display Ruflo multi-agent swarm status, topology, and agent telemetry",
    )
    parser.add_argument(
        "--swarm-audit",
        type=str,
        metavar="TOKEN_ADDRESS",
        help="Execute an autonomous Ruflo multi-agent consensus trade audit on a token address",
    )
    parser.add_argument(
        "--close-all",
        action="store_true",
        help="Force market exit and close all active open positions in the paper ledger",
    )
    parser.add_argument(
        "--reset-ledger",
        action="store_true",
        help="Reset paper trading ledger: clear all historical trades, lots, and snapshots",
    )
    parser.add_argument(
        "--experiments",
        action="store_true",
        help="Display live Strategy Tournament Leaderboard (Champion vs Challengers)",
    )
    parser.add_argument(
        "--backtest",
        action="store_true",
        help="Run walk-forward historical backtesting replay on SQLite ledger trades",
    )
    parsed = parser.parse_args(args)

    try:
        config = EngineConfig()
    except EnvironmentError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1

    if config.asyncio_debug:
        os.environ["PYTHONASYNCIODEBUG"] = "1"
        logging.getLogger("asyncio").setLevel(logging.DEBUG)

    if parsed.experiments:
        from alpha_engine.engine.experimentation import StrategyTournamentRegistry
        reg = StrategyTournamentRegistry()
        board = reg.get_leaderboard()
        print("\n=== 🏆 Quantitative Strategy Tournament Leaderboard ===")
        print(f"{'Status':<14} | {'Strategy Name':<34} | {'Regime':<18} | {'Trades':<8} | {'Win Rate':<10} | {'PF':<6} | {'Sortino':<8} | {'Max DD':<8} | {'Net PnL':<10} | {'Promotion'}")
        print("-" * 135)
        for b in board:
            print(f"{b['status']:<14} | {b['name']:<34} | {b['regime']:<18} | {b['trades']:<8} | {b['win_rate']:<10} | {b['profit_factor']:<6} | {b['sortino']:<8} | {b['max_dd']:<8} | {b['net_pnl']:<10} | {b['promotion_ready']}")
        print("-" * 135)
        return 0

    if parsed.backtest:
        from alpha_engine.engine.backtester import WalkForwardBacktester, StrategyParameters
        bt = WalkForwardBacktester(config.db_path)
        count = bt.load_historical_data()
        print(f"Loaded {count} historical trades from {config.db_path} for replay.")
        champ_params = StrategyParameters(strategy_name="champion_peak_trailing_50", hard_stop_loss_pct=0.15, take_profit_pct=0.50, trailing_stop_from_peak_pct=0.35)
        res = bt.evaluate_strategy(champ_params)
        print(res.print_summary())
        return 0

    if parsed.swarm:
        from alpha_engine.ruflo.swarm import RufloSwarmCoordinator
        swarm = RufloSwarmCoordinator(config=config)
        summary = swarm.get_swarm_status()
        print("=== 🐝 Ruflo Multi-Agent Trading Swarm Status ===")
        print(f"Topology: {summary['topology'].upper()} | Consensus: {summary['consensus'].upper()}")
        print(f"Total Specialized Agents: {summary['agents_count']}")
        print(f"Episodic Memory Episodes: {summary['episodic_memory_episodes']}")
        print(f"Active Social Weight: {summary['active_social_weight']:.3f}\n")
        print("Specialized Swarm Agents:")
        for role, a in summary["agents"].items():
            print(f"  • [{role}] {a['name']:<20} | Status: {a['status']:<10} | Tools: {a['tools_count']} | Tasks: {a['tasks_executed']}")
        return 0

    if parsed.swarm_audit:
        from alpha_engine.ruflo.swarm import RufloSwarmCoordinator
        token_ca = parsed.swarm_audit.strip()
        chain_str = "base_mainnet" if token_ca.startswith("0x") and len(token_ca) == 42 else "solana_mainnet"
        swarm = RufloSwarmCoordinator(config=config)

        async def _run_swarm_consensus():
            await swarm.start()
            try:
                return await swarm.execute_trade_consensus(
                    token_address=token_ca,
                    chain=chain_str,
                )
            finally:
                await swarm.stop()

        res = asyncio.run(_run_swarm_consensus())
        print(f"=== 🐝 Ruflo Multi-Agent Swarm Consensus Audit: {token_ca} ===")
        print(f"Approved: {'✅ YES' if res.get('consensus_approved') else '❌ NO'}")
        if not res.get("consensus_approved"):
            print(f"Rejection Agent: {res.get('rejection_agent')}")
            print(f"Reason: {res.get('reason')}")
        else:
            print(f"Security Score: {res.get('security_score')}/100")
            print(f"Protocol: {res.get('protocol')} | Topology: {res.get('topology')}")
            print(f"Position Sizing: {res.get('position_sizing')}")
        return 0

    if parsed.chat:
        from alpha_engine.chat_interface import cli_chat
        return asyncio.run(cli_chat(config.db_path))

    if parsed.doctor:
        from alpha_engine.reach.doctor import SystemDoctor
        doctor = SystemDoctor()
        report = asyncio.run(doctor.run_diagnostics())
        print(doctor.format_cli_summary(report))
        return 0

    if parsed.decisions:
        import sqlite3
        try:
            conn = sqlite3.connect(config.db_path, timeout=30.0)
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA busy_timeout = 30000;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA cache_size = -64000;")
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                "SELECT decision_id, decision_type, chain, token_symbol, token_address, confidence_score, pattern_match_score, reason, created_at "
                "FROM decision_audit_log ORDER BY created_at DESC LIMIT 15"
            )
            rows = cur.fetchall()
            print(f"=== Last {len(rows)} Structured Decision Records ({config.db_path}) ===")
            for r in rows:
                p_score = f"{r['pattern_match_score']:.2f}" if r['pattern_match_score'] is not None else "N/A"
                print(f"[{r['created_at']}] {r['decision_type']:<10} | {r['chain']} | {r['token_symbol'] or r['token_address'][:10]} | Conf: {r['confidence_score']:.2f} | Pattern: {p_score} | {r['reason']}")
            conn.close()
        except Exception as exc:
            print(f"Database query error: {exc}", file=sys.stderr)
        return 0


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

    if parsed.reset_governor:
        engine = PaperTradingEngine(config)
        engine.reset_trade_frequency_governor()
        print("✅ Trade Frequency & Pacing Governor successfully reset to 0 trades and 0 cooldown.")
        return 0

    if parsed.close_all:
        from alpha_engine.execution.ledger import SQLiteLedger
        engine = PaperTradingEngine(config)
        async def _close():
            ledger = SQLiteLedger(config.db_path)
            await ledger.initialize()
            engine._ledger = ledger
            open_lots = await ledger.restore_open_positions_from_ledger()
            engine._position_book.restore_open_lots(open_lots, max_age_s=0.0)
            res = await engine.force_close_all_open_lots()
            await ledger.close()
            return res
        closed_count = asyncio.run(_close())
        print(f"✅ Closed {closed_count} open positions across all chains and updated ledger.")
        return 0

    if parsed.reset_ledger:
        from alpha_engine.execution.ledger import SQLiteLedger
        engine = PaperTradingEngine(config)
        async def _reset():
            ledger = SQLiteLedger(config.db_path)
            await ledger.initialize()
            engine._ledger = ledger
            await engine.reset_paper_trading_ledger()
            await ledger.close()
        asyncio.run(_reset())
        print("✅ Paper trading ledger wiped clean (trades, lots, snapshots, signals reset to 0).")
        return 0

    if parsed.status or parsed.trades or parsed.signals or parsed.news or parsed.whales:
        import sqlite3

        try:
            conn = sqlite3.connect(config.db_path, timeout=30.0)
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA busy_timeout = 30000;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA cache_size = -64000;")
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            if parsed.trades or parsed.status:
                cur.execute("PRAGMA table_info(trades)")
                cols = [row["name"] for row in cur.fetchall()]
                has_platform = "platform" in cols

                if has_platform:
                    cur.execute(
                        "SELECT chain, token_address, side, effective_price, realized_pnl_usd, created_at, platform "
                        "FROM trades ORDER BY created_at DESC LIMIT 10"
                    )
                else:
                    cur.execute(
                        "SELECT chain, token_address, side, effective_price, realized_pnl_usd, created_at "
                        "FROM trades ORDER BY created_at DESC LIMIT 10"
                    )
                rows = cur.fetchall()
                print(f"=== Last {len(rows)} Trades ({config.db_path}) ===")
                for r in rows:
                    pnl_raw = r['realized_pnl_usd']
                    if pnl_raw is not None and str(pnl_raw).strip() != "":
                        try:
                            pnl_f = float(pnl_raw)
                            pnl = f"+${pnl_f:.2f}" if pnl_f >= 0 else f"-${abs(pnl_f):.2f}"
                        except Exception:
                            pnl = str(pnl_raw)
                    else:
                        pnl = "OPEN"
                    plat = r['platform'] if has_platform and r['platform'] else resolve_trade_platform(token_address=r['token_address'], chain=r['chain'])
                    print(f"{r['created_at']} | {plat} | {r['chain']} | {r['side']} | {r['token_address'][:12]}.. | Fill: {r['effective_price']} | PnL: {pnl}")
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
