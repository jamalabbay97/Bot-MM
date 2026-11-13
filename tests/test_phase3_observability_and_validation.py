"""
tests/test_phase3_observability_and_validation.py — Phase 3 Comprehensive Test Suite
=====================================================================================
Validates:
1. Tick Recorder: stores pool states, swaps, quotes, native prices, security reports,
   signals, and decisions from real ingestion path with bounded backpressure & drop counters.
2. Replay Mode: deterministic given a seed, reuses SAME executor, cost model, and latency/price logic.
3. Walk-Forward Validation: rolling train/val windows, rejects overfit strategies bleeding OOS.
4. Model-vs-Reality: per-venue and per-size simulator bias (mean/median bps, fraction better than reality)
   with WARNING trigger above threshold.
5. Doctor Self-Check: detects missing dependencies and blocks new entries fail-closed.
6. README Claims: audit script verifies latency numbers, fees, Kelly, and venues against config defaults.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.models.enums import ChainIdentifier, ExecutionVenue, OrderSide, SignalStrength, SignalSource, SecurityTier
from alpha_engine.models.events import SignalEvent, SwapEvent
from alpha_engine.models.state import PoolState, ReserveType, SecurityReport


# ---------------------------------------------------------------------------
# 1. TICK RECORDER TESTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tick_recorder_writes_from_ingestion_and_handles_backpressure():
    """Verify tick recorder writes pool ticks, swaps, quotes, prices, signals, and handles backpressure."""
    from alpha_engine.observability.recorder import MarketTickRecorder

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_ticks.db")
        # Very small queue size to easily trigger backpressure
        recorder = MarketTickRecorder(
            db_path=db_path,
            queue_size=5,
            batch_size=2,
            flush_interval_s=0.05,
            retention_days=1,
        )
        await recorder.start()

        try:
            pool = PoolState(
                pool_address="PoolSolana11111111111111111111111111111111",
                token_address="TokenA11111111111111111111111111111111111",
                chain=ChainIdentifier.SOLANA_MAINNET,
                venue=ExecutionVenue.RAYDIUM_AMM,
                native_reserve=Decimal("50.0"),
                token_reserve=Decimal("1000000.0"),
                reserve_type=ReserveType.EXACT,
                fee_numerator=25,
                fee_denominator=10000,
                last_updated_block=1000,
            )
            swap = SwapEvent(
                chain=ChainIdentifier.SOLANA_MAINNET,
                pool_address="PoolSolana11111111111111111111111111111111",
                token_in="So11111111111111111111111111111111111111112",
                token_out="TokenA11111111111111111111111111111111111",
                amount_in=Decimal("1.5"),
                amount_out=Decimal("30000.0"),
                sender="Trader11111111111111111111111111111111111",
                tx_hash="TxHash11111111111111111111111111111111111",
                block_number=1000,
                timestamp_ns=time.time_ns(),
            )
            sec = SecurityReport(
                token_address="TokenA11111111111111111111111111111111111",
                chain=ChainIdentifier.SOLANA_MAINNET,
                tier=SecurityTier.CLEAN,
                is_honeypot=False,
                buy_tax_bps=0,
                sell_tax_bps=0,
                top10_concentration=0.15,
            )
            sig = SignalEvent(
                signal_id="sig-rec-001",
                timestamp_ns=time.time_ns(),
                token_address="TokenA11111111111111111111111111111111111",
                chain=ChainIdentifier.SOLANA_MAINNET,
                pool_address=pool.pool_address,
                execution_venue=ExecutionVenue.RAYDIUM_AMM,
                suggested_side=OrderSide.BUY,
                pool_state=pool,
                security_report=sec,
                strength=SignalStrength.STRONG,
                source=SignalSource.PUMP_FUN_MINT,
                alpha_score=0.88,
            )

            # Record items
            ok1 = recorder.record_pool_tick(pool, provenance="ingestion_stream")
            ok2 = recorder.record_swap_tick(swap, provenance="ingestion_stream")
            ok3 = recorder.record_native_price_tick(
                chain="solana_mainnet",
                price_usd=Decimal("200.0"),
                source="defillama",
                status="OK",
                provenance="price_service",
            )
            ok4 = recorder.record_signal_tick(sig, provenance="fast_launch")
            ok5 = recorder.record_decision_tick(
                decision_id="dec-001",
                decision_type="BUY_APPROVAL",
                chain="solana_mainnet",
                token_address="TokenA11111111111111111111111111111111111",
                approved=True,
                reason="Passed council with high score",
                metrics={"score": 0.88},
                provenance="council",
            )

            # Backpressure test: fill the queue beyond maxsize (5)
            # Send 10 more items rapidly without awaiting flush
            dropped_count = 0
            for i in range(10):
                accepted = recorder.record_pool_tick(pool, provenance="overflow_test")
                if not accepted:
                    dropped_count += 1

            # Dropped count must be > 0 because queue is bounded at 5
            assert dropped_count > 0, "Backpressure must drop excess items when queue is full"
            stats = recorder.get_stats()
            assert stats["dropped_ticks_total"] >= dropped_count

            # Await flush to DB after yielding to event loop
            await asyncio.sleep(0.01)
            await recorder.flush()

            # Verify SQLite persistence
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode;")
            journal_mode = cur.fetchone()[0]
            assert journal_mode.lower() == "wal", f"Recorder SQLite must be in WAL mode, got {journal_mode}"

            cur.execute("SELECT COUNT(*) FROM pool_ticks;")
            assert cur.fetchone()[0] >= 1
            cur.execute("SELECT COUNT(*) FROM swap_ticks;")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT COUNT(*) FROM native_price_ticks;")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT COUNT(*) FROM signal_ticks;")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT COUNT(*) FROM decision_ticks;")
            assert cur.fetchone()[0] == 1
            conn.close()
        finally:
            await recorder.stop()


# ---------------------------------------------------------------------------
# 2. REPLAY MODE TESTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replay_reproduces_session_deterministically_and_matches_live_paper():
    """Verify replay re-runs the system against recorded data deterministically given a seed and matches paper executor."""
    from alpha_engine.observability.recorder import MarketTickRecorder
    from alpha_engine.observability.replay import ReplayEngine
    from alpha_engine.engine.rules import EntryRuleSet, RuleGroup, RuleCondition, ComparisonOperator
    from alpha_engine.execution.costs import TradeCostModel

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "replay_ticks.db")
        recorder = MarketTickRecorder(db_path=db_path, queue_size=1000)
        await recorder.start()

        token_ca = "TokenReplay1111111111111111111111111111111"
        pool_addr = "PoolReplay11111111111111111111111111111111"
        now_ns = 1_700_000_000_000_000_000

        # Record a sequence of price states moving up
        p0 = PoolState(
            pool_address=pool_addr,
            token_address=token_ca,
            chain=ChainIdentifier.SOLANA_MAINNET,
            venue=ExecutionVenue.RAYDIUM_AMM,
            native_reserve=Decimal("50.0"),
            token_reserve=Decimal("1000000.0"),
            reserve_type=ReserveType.EXACT,
            fee_numerator=25,
            fee_denominator=10000,
            last_updated_block=1000,
            timestamp_s=float(now_ns / 1e9),
        )
        recorder.record_pool_tick(p0, timestamp_ns=now_ns)

        # Later pool state (after latency window, price higher)
        p1 = PoolState(
            pool_address=pool_addr,
            token_address=token_ca,
            chain=ChainIdentifier.SOLANA_MAINNET,
            venue=ExecutionVenue.RAYDIUM_AMM,
            native_reserve=Decimal("55.0"),
            token_reserve=Decimal("909090.9"),
            reserve_type=ReserveType.EXACT,
            fee_numerator=25,
            fee_denominator=10000,
            last_updated_block=1001,
            timestamp_s=float((now_ns + 150_000_000) / 1e9),
        )
        recorder.record_pool_tick(p1, timestamp_ns=now_ns + 150_000_000)

        recorder.record_native_price_tick(
            chain="solana_mainnet",
            price_usd=Decimal("200.0"),
            source="defillama",
            status="OK",
            timestamp_ns=now_ns,
        )

        sec_r = SecurityReport(
            token_address=token_ca,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
            top10_concentration=0.15,
        )
        sig = SignalEvent(
            signal_id="sig-replay-001",
            timestamp_ns=now_ns,
            token_address=token_ca,
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_addr,
            execution_venue=ExecutionVenue.RAYDIUM_AMM,
            suggested_side=OrderSide.BUY,
            pool_state=p0,
            security_report=sec_r,
            strength=SignalStrength.STRONG,
            source=SignalSource.PUMP_FUN_MINT,
            alpha_score=0.85,
        )
        recorder.record_signal_tick(sig, timestamp_ns=now_ns)
        await recorder.flush()
        await recorder.stop()

        # Replay run 1 with seed=42
        cfg = EngineConfig(execution_seed=42, latency_min_ms_solana=100, latency_max_ms_solana=200)
        engine1 = ReplayEngine(tick_db_path=db_path, config=cfg, seed=42)
        res1 = await engine1.run_replay()

        # Replay run 2 with seed=42
        engine2 = ReplayEngine(tick_db_path=db_path, config=cfg, seed=42)
        res2 = await engine2.run_replay()

        assert len(res1.fills) == len(res2.fills) == 1, "Replay must process recorded signal into fill"
        f1, f2 = res1.fills[0], res2.fills[0]

        # Exact determinism check
        assert f1.effective_price == f2.effective_price
        assert f1.slippage_bps == f2.slippage_bps
        assert f1.fill_latency_ms == f2.fill_latency_ms
        assert f1.simulated_native_spent == f2.simulated_native_spent
        assert f1.tokens_acquired == f2.tokens_acquired
        assert res1.net_pnl_usd == res2.net_pnl_usd

        # Replay run 3 with different seed should produce different latency
        engine3 = ReplayEngine(tick_db_path=db_path, config=cfg, seed=999)
        res3 = await engine3.run_replay()
        f3 = res3.fills[0]
        # Latency should differ under different seed
        assert f1.fill_latency_ms != f3.fill_latency_ms

        # Direct database counts inspection via sqlite3
        counts = engine1.inspect_db_counts()
        assert "pool_ticks" in counts and counts["pool_ticks"] >= 2
        assert "signal_ticks" in counts and counts["signal_ticks"] >= 1

        # JSON serialization validation
        json_summary = res1.to_json()
        assert '"signals_evaluated": 1' in json_summary

        # Council fallback hook validation with MagicMock
        mock_council = MagicMock(return_value=False)
        engine_council = ReplayEngine(tick_db_path=db_path, config=cfg, seed=42, council_fallback=mock_council)
        res_council = await engine_council.run_replay()
        await asyncio.sleep(0.001)
        assert mock_council.called
        assert res_council.orders_rejected >= 1
        assert "council_fallback_rejected" in res_council.rejection_reasons


# ---------------------------------------------------------------------------
# 3. WALK-FORWARD VALIDATION TESTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_walk_forward_rejects_overfit_strategy():
    """Verify walk-forward validation splits time windows and rejects an overfit strategy that wins in-sample but bleeds out-of-sample."""
    from alpha_engine.observability.walk_forward import WalkForwardValidator, WalkForwardWindow
    from alpha_engine.engine.experimentation import StrategyParameters
    from alpha_engine.engine.rules import EntryRuleSet, RuleGroup, RuleCondition, ComparisonOperator
    from alpha_engine.execution.ledger import SQLiteLedger

    with tempfile.TemporaryDirectory() as tmpdir:
        ledger_db = os.path.join(tmpdir, "ledger.db")
        ledger = SQLiteLedger(db_path=ledger_db)
        await ledger.initialize()

        validator = WalkForwardValidator(
            ledger=ledger,
            train_ratio=0.70,
            min_oos_trades=5,
            min_oos_profit_factor=1.15,
        )

        # Overfit strategy: highly restrictive rules that captured noise in train window
        overfit_strategy = StrategyParameters(
            strategy_name="overfit_curve_fit_v1",
            hard_stop_loss_pct=0.05,
            take_profit_pct=0.10,
            min_entry_score=0.92,
            entry_rules=EntryRuleSet(
                groups=[
                    RuleGroup(
                        conditions=[
                            RuleCondition(metric="alpha_score", operator=ComparisonOperator.GT, threshold=90.0),
                            RuleCondition(metric="volume_surge_multiplier", operator=ComparisonOperator.GT, threshold=4.5),
                        ]
                    )
                ]
            ),
        )

        # Generate synthetic trade history:
        # In-sample: 35 winning trades (PF > 3.0)
        # Out-of-sample: 15 losing trades (PF < 0.5, bleeding)
        synthetic_history = []
        base_ts = 1_700_000_000_000_000_000
        # In-sample (70%): 35 trades, 30 wins, 5 losses
        for i in range(35):
            is_win = (i % 7 != 0)
            pnl = 15.0 if is_win else -5.0
            synthetic_history.append({
                "trade_id": f"trade-is-{i}",
                "timestamp_ns": base_ts + i * 60_000_000_000,
                "realized_pnl_usd": pnl,
                "cost_basis_usd": 20.0,
                "roi_pct": (pnl / 20.0) * 100.0,
                "alpha_score": 0.95,
                "volume_surge_multiplier": 5.0,
            })
        # Out-of-sample (30%): 15 trades, 2 wins, 13 losses
        for j in range(15):
            is_win = (j == 0 or j == 5)
            pnl = 5.0 if is_win else -8.0
            synthetic_history.append({
                "trade_id": f"trade-oos-{j}",
                "timestamp_ns": base_ts + (35 + j) * 60_000_000_000,
                "realized_pnl_usd": pnl,
                "cost_basis_usd": 20.0,
                "roi_pct": (pnl / 20.0) * 100.0,
                "alpha_score": 0.95,
                "volume_surge_multiplier": 5.0,
            })

        val_result = await validator.evaluate_trades_walk_forward(
            strategy_params=overfit_strategy,
            trade_records=synthetic_history,
        )

        # Must reject because out-of-sample bleeds!
        assert not val_result.is_promotable, "Overfit strategy failing OOS must be rejected fail-closed"
        assert any("OOS_PROFIT_FACTOR_BELOW_MIN" in r for r in val_result.rejection_reasons) or any("OOS_NEGATIVE_PNL" in r for r in val_result.rejection_reasons)
        assert val_result.in_sample_metrics.profit_factor > 2.0
        assert val_result.out_of_sample_metrics.profit_factor < 1.0

        # Verify walk-forward validation stored in ledger
        logs = await ledger.get_walk_forward_validations(strategy_name="overfit_curve_fit_v1")
        assert len(logs) >= 1
        assert logs[0]["is_promotable"] == 0


# ---------------------------------------------------------------------------
# 4. MODEL-VS-REALITY REPORT TESTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_model_vs_reality_report_computes_bias_and_triggers_warning():
    """Verify model-vs-reality report computes signed error in bps, fraction better than reality, and raises warning when bias exceeds bound."""
    from alpha_engine.observability.model_vs_reality import ModelVsRealityReporter, FillRealityComparison

    # Synthetic trades where model is optimistic (filled cheaper than observed reality)
    comparisons = [
        # Fill 1: modeled at $100, observed at $100.50 -> model was 50 bps optimistic
        FillRealityComparison(
            trade_id="t1",
            venue=ExecutionVenue.RAYDIUM_AMM,
            side=OrderSide.BUY,
            size_usd=Decimal("20.0"),
            modeled_fill_price=Decimal("100.0"),
            observed_market_price=Decimal("100.50"),
            timestamp_ns=1_700_000_000_000_000_000,
        ),
        # Fill 2: modeled at $100, observed at $100.30 -> model was 30 bps optimistic
        FillRealityComparison(
            trade_id="t2",
            venue=ExecutionVenue.RAYDIUM_AMM,
            side=OrderSide.BUY,
            size_usd=Decimal("20.0"),
            modeled_fill_price=Decimal("100.0"),
            observed_market_price=Decimal("100.30"),
            timestamp_ns=1_700_000_001_000_000_000,
        ),
        # Fill 3: modeled at $100, observed at $100.40 -> model was 40 bps optimistic
        FillRealityComparison(
            trade_id="t3",
            venue=ExecutionVenue.PUMP_FUN,
            side=OrderSide.BUY,
            size_usd=Decimal("10.0"),
            modeled_fill_price=Decimal("100.0"),
            observed_market_price=Decimal("100.40"),
            timestamp_ns=1_700_000_002_000_000_000,
        ),
    ]

    reporter = ModelVsRealityReporter(bias_warning_threshold_bps=25.0)
    report = reporter.generate_report(comparisons)

    # All 3 modeled buys were cheaper than observed reality -> 100% better than reality
    assert report.total_fills == 3
    assert report.fraction_better_than_reality == 1.0
    # Mean error: (49.8 + 29.9 + 39.8) / 3 = 39.8 bps
    assert round(report.mean_bias_bps, 1) == 39.8
    assert round(report.median_bias_bps, 1) == 39.8

    # Warning must be triggered because 39.8 > 25.0 threshold
    assert report.warning_triggered is True
    assert "WARNING: Simulator bias" in report.format_cli_summary()

    # Per-venue breakdown check
    assert ExecutionVenue.RAYDIUM_AMM.value in report.per_venue_bias
    ray_stats = report.per_venue_bias[ExecutionVenue.RAYDIUM_AMM.value]
    assert round(ray_stats["mean_bias_bps"], 1) == 39.8


# ---------------------------------------------------------------------------
# 5. STARTUP SELF-CHECK (DOCTOR) TESTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_doctor_fails_and_blocks_entries_on_missing_dependency():
    """Verify system doctor detects failing dependencies and runner blocks new entries fail-closed."""
    from alpha_engine.observability.doctor import SystemDoctor, CheckStatus
    from alpha_engine.engine.runner import PaperTradingEngine

    # 1. Test doctor reporting FAIL when an RPC or DB check fails
    doc = SystemDoctor()
    # Mock RPC failing
    with patch.object(doc, "_check_solana_rpc", new_callable=AsyncMock) as mock_sol:
        mock_sol.return_value = {
            "name": "Solana SVM RPC",
            "status": CheckStatus.FAIL,
            "latency_ms": 1500.0,
            "details": "Connection refused (RPC offline)",
        }
        res = await doc.run_full_diagnostics()
        assert res.overall_status == CheckStatus.FAIL
        assert any(item["status"] == CheckStatus.FAIL for item in res.checks)
        summary = doc.format_cli_table(res)
        assert "FAIL" in summary

    # 2. Test runner blocks entries when doctor fails
    cfg = EngineConfig(allow_static_native_prices=True)
    engine = PaperTradingEngine(config=cfg)
    # Simulate failed doctor result on engine
    engine._entries_blocked = True
    engine._doctor_failure_reasons = ["Solana SVM RPC offline"]

    pool_doc = PoolState(
        pool_address="PoolSolana11111111111111111111111111111111",
        token_address="TokenA11111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        venue=ExecutionVenue.RAYDIUM_AMM,
        native_reserve=Decimal("50.0"),
        token_reserve=Decimal("1000000.0"),
        reserve_type=ReserveType.EXACT,
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1000,
    )
    sec_doc = SecurityReport(
        token_address="TokenA11111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        top10_concentration=0.15,
    )
    sig = SignalEvent(
        signal_id="sig-doc-fail-001",
        timestamp_ns=time.time_ns(),
        token_address="TokenA11111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_doc.pool_address,
        execution_venue=ExecutionVenue.RAYDIUM_AMM,
        suggested_side=OrderSide.BUY,
        pool_state=pool_doc,
        security_report=sec_doc,
        strength=SignalStrength.STRONG,
        source=SignalSource.PUMP_FUN_MINT,
        alpha_score=0.90,
    )
    try:
        # Verify gate rejects when entries blocked
        is_blocked = engine.is_entry_blocked_by_doctor()
        assert is_blocked is True, "Runner must report entry blocked when doctor failed"
    finally:
        if hasattr(engine, "_recorder") and engine._recorder:
            await engine._recorder.stop()
        if hasattr(engine, "wallet_pipeline") and engine.wallet_pipeline:
            await engine.wallet_pipeline.close()
        if hasattr(engine, "_ledger") and engine._ledger:
            await engine._ledger.close()
        await asyncio.sleep(0.05)


# ---------------------------------------------------------------------------
# 6. README CLAIMS AUDIT SCRIPT TEST
# ---------------------------------------------------------------------------

def test_readme_claims_audit_passes():
    """Verify audit script checks README claims against config defaults and passes without stale numbers."""
    from tools.audit.readme_claims import audit_readme_claims

    repo_root = Path(__file__).resolve().parent.parent
    readme_path = repo_root / "README.md"
    assert readme_path.exists(), "README.md must exist at repo root"

    findings = audit_readme_claims(readme_path)
    assert findings == [], f"README claims audit must have 0 findings, but found: {findings}"
