"""
tests/test_issue7_slippage_and_tuner.py
Tests for Issue 7: Real realized slippage computation and tuner calibration.
"""

from __future__ import annotations

import ast
import random
import time
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.engine.self_learning.hyperparameter_tuner import (
    AutonomousHyperparameterTuner,
    TunedHyperparameters,
)
from alpha_engine.engine.self_learning.models import ClosedTradeOutcome
from alpha_engine.execution.book import OpenLot, PositionBook
from alpha_engine.execution.costs import TradeCostModel
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.models.decisions import DecisionSignal
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    LotStatus,
    OrderSide,
    SignalSource,
    SignalStrength,
    StrategyHorizon,
    TradeExitReason,
    SecurityTier,
)
from alpha_engine.models.events import SignalEvent, SwapEvent
from alpha_engine.pricing.native_price_service import NativePriceStatus, PriceResult
from alpha_engine.models.state import PaperFill, PoolState, SecurityReport, TradeRecord


class MockNativePriceService:
    def get_price(self, chain: ChainIdentifier) -> PriceResult:
        price = Decimal("200.0") if chain == ChainIdentifier.SOLANA_MAINNET else Decimal("3000.0")
        return PriceResult(
            price=price,
            age_s=0.1,
            status=NativePriceStatus.OK,
            source="mock",
        )


def _create_test_pool(
    spot_price: Decimal = Decimal("0.010"),
    token_reserve: Decimal = Decimal("100000.0"),
    native_reserve: Decimal = Decimal("1000.0"),
    token_address: str = "Token111111111111111111111111111111111111111",
    pool_address: str = "Pool1111111111111111111111111111111111111111",
) -> PoolState:
    return PoolState(
        pool_address=pool_address,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token_address,
        token_reserve=token_reserve,
        native_reserve=native_reserve,
        spot_price_native_per_token=spot_price,
        fee_numerator=3000,
        fee_denominator=1000000,
        last_updated_block=123456,
        token_decimals=6,
        native_decimals=9,
    )


def test_paper_fill_has_typed_slippage_bps_field():
    """Verify PaperFill has a real typed field slippage_bps (not just a property)."""
    fill = PaperFill(
        token_address="Token111111111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("1.0"),
        tokens_acquired=Decimal("100.0"),
        effective_price=Decimal("0.01"),
        price_impact_bps=25,
        slippage_bps=85,
    )
    assert fill.slippage_bps == 85
    dumped = fill.model_dump()
    assert "slippage_bps" in dumped
    assert dumped["slippage_bps"] == 85

    # Verify TradeRecord.from_fill copies slippage_bps and provides actual_slippage_bps alias
    trade_record = TradeRecord.from_fill(fill, exit_reason=TradeExitReason.TP_50)
    assert trade_record.slippage_bps == 85
    assert trade_record.actual_slippage_bps == 85


@pytest.mark.asyncio
async def test_paper_executor_computes_realized_slippage_buy():
    """Verify PaperExecutor computes realized slippage vs decision-time reference price on BUY."""
    cfg = EngineConfig(allow_static_native_prices=True)
    book = PositionBook()
    price_svc = MockNativePriceService()
    costs = TradeCostModel(config=cfg, price_service=price_svc)
    executor = PaperExecutor(
        position_book=book,
        price_service=price_svc,
        cost_model=costs,
        config=cfg,
    )

    pool = _create_test_pool(spot_price=Decimal("0.010"))
    ref_price = pool.spot_price_native_per_token

    sec_report = SecurityReport(
        token_address=pool.token_address,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
    )

    swap = SwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool.pool_address,
        token_in=pool.token_address,
        token_out="So11111111111111111111111111111111111111112",
        amount_in=Decimal("50.0"),
        amount_out=Decimal("0.5"),
        sender="SenderWallet1111111111111111111111111111111",
        timestamp_ns=time.time_ns(),
        block_number=123456,
        tx_hash="5h6xabcdef1234567890abcdef1234567890abcdef123456",
    )

    signal = SignalEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool.pool_address,
        token_address=pool.token_address,
        suggested_side=OrderSide.BUY,
        timestamp_ns=time.time_ns(),
        pool_state=pool,
        security_report=sec_report,
        strength=SignalStrength.STRONG,
        alpha_score=0.90,
        trigger_swap=swap,
        source=SignalSource.DEX_SWAP,
    )

    decision_signal = DecisionSignal(
        token_address=signal.token_address,
        pool_address=signal.pool_address,
        chain=signal.chain,
        suggested_side=OrderSide.BUY,
        strategy_horizon=StrategyHorizon.SHORT_TERM_SCALP,
        execution_venue=ExecutionVenue.RAYDIUM_CPMM,
        trigger_reason="Breakout momentum",
    )
    assert decision_signal.execution_venue == ExecutionVenue.RAYDIUM_CPMM
    assert decision_signal.strategy_horizon == StrategyHorizon.SHORT_TERM_SCALP

    fill = await executor.execute_signal(
        signal=signal,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=Decimal("5.0"),
    )

    assert fill is not None
    assert fill.effective_price > ref_price
    # Expected slippage = (effective_price - ref_price) / ref_price * 10000
    expected_slip = max(0, int(round((fill.effective_price - ref_price) / ref_price * Decimal(10000))))
    assert fill.slippage_bps == expected_slip
    assert fill.slippage_bps > 0


@pytest.mark.asyncio
async def test_paper_executor_computes_realized_slippage_sell():
    """Verify PaperExecutor computes realized slippage vs decision-time reference price on SELL."""
    cfg = EngineConfig(allow_static_native_prices=True)
    book = PositionBook()
    price_svc = MockNativePriceService()
    costs = TradeCostModel(config=cfg, price_service=price_svc)
    executor = PaperExecutor(
        position_book=book,
        price_service=price_svc,
        cost_model=costs,
        config=cfg,
    )

    pool = _create_test_pool(spot_price=Decimal("0.010"))
    ref_price = pool.spot_price_native_per_token

    fill = await executor.execute_exit(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=pool.token_address,
        pool=pool,
        tokens_to_sell=Decimal("500.0"),
        reason=TradeExitReason.TP_50,
    )

    assert fill is not None
    assert fill.effective_price < ref_price
    # Expected slippage for seller = (ref_price - effective_price) / ref_price * 10000
    expected_slip = max(0, int(round((ref_price - fill.effective_price) / ref_price * Decimal(10000))))
    assert fill.slippage_bps == expected_slip
    assert fill.slippage_bps > 0


@pytest.mark.asyncio
async def test_varying_trade_sizes_produce_varying_realized_slippage():
    """Verify that different market conditions / trade sizes produce distinct realized slippage (not constant 50)."""
    cfg = EngineConfig(allow_static_native_prices=True)
    book = PositionBook()
    price_svc = MockNativePriceService()
    costs = TradeCostModel(config=cfg, price_service=price_svc)
    executor = PaperExecutor(
        position_book=book,
        price_service=price_svc,
        cost_model=costs,
        config=cfg,
    )

    pool = _create_test_pool(spot_price=Decimal("0.010"), native_reserve=Decimal("100.0"), token_reserve=Decimal("10000.0"))

    # Small exit
    fill_small = await executor.execute_exit(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=pool.token_address,
        pool=pool,
        tokens_to_sell=Decimal("10.0"),
        reason=TradeExitReason.TP_50,
    )
    # Medium exit
    fill_med = await executor.execute_exit(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=pool.token_address,
        pool=pool,
        tokens_to_sell=Decimal("150.0"),
        reason=TradeExitReason.TP_50,
    )

    assert fill_small is not None and fill_med is not None
    assert fill_small.slippage_bps != fill_med.slippage_bps, (
        f"Small fill slippage ({fill_small.slippage_bps}) must differ from medium ({fill_med.slippage_bps})"
    )
    assert fill_small.slippage_bps != 50 or fill_med.slippage_bps != 50


def test_tuner_keeps_defaults_below_min_samples(tmp_path):
    """Verify tuner keeps default max_slippage_bps when observations are below min_slippage_samples."""
    settings_file = str(tmp_path / "settings_below.json")
    tuner = AutonomousHyperparameterTuner(
        settings_file_path=settings_file,
        min_trades_between_tuning=1,
        min_slippage_samples=10,
    )
    assert isinstance(tuner.current_params, TunedHyperparameters)
    default_max_slip = tuner.current_params.max_slippage_bps
    assert default_max_slip == 150

    # Feed 5 trade outcomes (less than required 10)
    for i in range(5):
        trade = ClosedTradeOutcome(
            trade_id=f"t_{i}",
            token_address="TokenA",
            chain="solana_mainnet",
            strategy_type="default",
            actual_slippage_bps=220,  # High slippage
            realized_pnl_usd=10.0,
            roi_pct=15.0,
        )
        tuner.record_trade_outcome(trade)

    # Must retain default because sample size (5) < min_slippage_samples (10)
    assert tuner.current_params.max_slippage_bps == default_max_slip, (
        f"Expected default {default_max_slip}, got {tuner.current_params.max_slippage_bps}"
    )


def test_tuner_calibrates_when_samples_reach_threshold(tmp_path):
    """Verify tuner calibrates max_slippage_bps once min_slippage_samples is reached, respecting [75, 300] bounds."""
    settings_file = str(tmp_path / "settings_reach.json")
    tuner = AutonomousHyperparameterTuner(
        settings_file_path=settings_file,
        min_trades_between_tuning=1,
        min_slippage_samples=10,
    )
    assert isinstance(tuner.current_params, TunedHyperparameters)

    # Feed 10 trades with moderate/high slippage around 150 bps
    # 75th percentile of [100, 110, 120, 130, 140, 150, 160, 170, 180, 190] is 170.
    # 170 * 1.20 = 204 bps.
    # Shuffling observations proves 75th percentile calibration is invariant to trade arrival order.
    slips = [100, 110, 120, 130, 140, 150, 160, 170, 180, 190]
    random.seed(42)
    shuffled_slips = list(slips)
    random.shuffle(shuffled_slips)
    for i, s in enumerate(shuffled_slips):
        trade = ClosedTradeOutcome(
            trade_id=f"t_{i}",
            token_address="TokenA",
            chain="solana_mainnet",
            strategy_type="default",
            actual_slippage_bps=s,
            realized_pnl_usd=10.0,
            roi_pct=15.0,
        )
        tuner.record_trade_outcome(trade)

    assert tuner.current_params.max_slippage_bps == 204, (
        f"Expected calibrated slippage 204, got {tuner.current_params.max_slippage_bps}"
    )

    # Test lower bound clamp (75 bps floor):
    settings_low = str(tmp_path / "settings_low.json")
    tuner_low = AutonomousHyperparameterTuner(
        settings_file_path=settings_low,
        min_trades_between_tuning=1,
        min_slippage_samples=10,
    )
    for i in range(10):
        trade = ClosedTradeOutcome(
            trade_id=f"t_low_{i}",
            token_address="TokenLow",
            chain="solana_mainnet",
            strategy_type="default",
            actual_slippage_bps=20,  # 20 * 1.2 = 24 -> clamped to 75
            realized_pnl_usd=10.0,
            roi_pct=15.0,
        )
        tuner_low.record_trade_outcome(trade)

    assert tuner_low.current_params.max_slippage_bps == 75, (
        f"Expected floor 75 bps, got {tuner_low.current_params.max_slippage_bps}"
    )

    # Test upper bound clamp (300 bps ceiling):
    settings_high = str(tmp_path / "settings_high.json")
    tuner_high = AutonomousHyperparameterTuner(
        settings_file_path=settings_high,
        min_trades_between_tuning=1,
        min_slippage_samples=10,
    )
    for i in range(10):
        trade = ClosedTradeOutcome(
            trade_id=f"t_high_{i}",
            token_address="TokenHigh",
            chain="solana_mainnet",
            strategy_type="default",
            actual_slippage_bps=350,  # 350 * 1.2 = 420 -> clamped to 300
            realized_pnl_usd=10.0,
            roi_pct=15.0,
        )
        tuner_high.record_trade_outcome(trade)

    assert tuner_high.current_params.max_slippage_bps == 300, (
        f"Expected ceiling 300 bps, got {tuner_high.current_params.max_slippage_bps}"
    )


@pytest.mark.asyncio
async def test_runner_feeds_real_slippage_to_tuner():
    """Verify that runner's closed-trade notification feeds the fill's real slippage to the tuner (not 50)."""
    cfg = EngineConfig(allow_static_native_prices=True)
    engine = PaperTradingEngine(config=cfg)

    # Track payloads passed to self_learning_engine
    captured_payloads = []
    original_hook = engine.self_learning_engine.on_trade_closed
    mock_tuner = MagicMock(wraps=engine.self_learning_engine.hyperparameter_tuner)
    engine.self_learning_engine.hyperparameter_tuner = mock_tuner

    async def spy_hook(payload):
        captured_payloads.append(payload)
        return await original_hook(payload)

    engine.self_learning_engine.on_trade_closed = AsyncMock(side_effect=spy_hook)

    lot = OpenLot(
        lot_id="test_lot_slip",
        token_address="Token111111111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("100.0"),
        tokens_held=Decimal("100.0"),
        cost_basis_native=Decimal("1.0"),
        entry_price=Decimal("0.01"),
        status=LotStatus.OPEN,
    )
    fill = PaperFill(
        token_address=lot.token_address,
        chain=lot.chain,
        side=OrderSide.SELL,
        simulated_native_spent=Decimal("1.2"),
        tokens_acquired=Decimal("100.0"),
        effective_price=Decimal("0.012"),
        price_impact_bps=45,
        slippage_bps=137,  # Real realized slippage
        fill_latency_ms=88,
    )

    engine._record_closed_trade_and_notify(
        lot=lot,
        exit_fill=fill,
        decision_reason="tp_50",
        exit_pnl_usd=Decimal("40.0"),
        pnl_native=Decimal("0.2"),
    )

    if hasattr(engine, "await_pending_hooks"):
        await engine.await_pending_hooks()

    assert engine.self_learning_engine.on_trade_closed.await_count == 1
    assert mock_tuner.record_trade_outcome.call_count == 1
    assert len(captured_payloads) == 1
    payload = captured_payloads[0]
    assert payload["actual_slippage_bps"] == 137, (
        f"Expected actual_slippage_bps=137 from fill, got {payload['actual_slippage_bps']}"
    )
    assert payload["fill_latency_ms"] == 88.0


def test_no_getattr_with_literal_default_targeting_fill_slippage():
    """Audit check: assert no getattr(..., 'slippage_bps', <literal>) exists in runner or execution."""
    target_files = [
        Path("alpha_engine/engine/runner.py"),
        Path("alpha_engine/execution/executor.py"),
        Path("alpha_engine/ruflo/memory.py"),
    ]

    findings = []
    for f in target_files:
        if not f.exists():
            continue
        content = f.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(f))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr":
                if len(node.args) >= 3 and isinstance(node.args[1], ast.Constant) and node.args[1].value in ("slippage_bps", "actual_slippage_bps"):
                    findings.append(f"{f}:{node.lineno}: {ast.unparse(node)}")

    assert not findings, (
        "Found forbidden getattr with default for slippage:\n" + "\n".join(findings)
    )
