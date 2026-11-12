"""
tests/test_issue6_post_trade_learning_hook.py
=============================================
Regression and integration tests for Issue 6:
Post-trade learning hook broken and failures hidden.

Covers:
1. All 3 consumers (self_learning_engine, swarm_coordinator, strategy_tournament)
   are invoked exactly once on closed trade.
2. Failure in one consumer does not prevent others from running, is logged at ERROR
   with traceback, and increments telemetry error counters.
3. record_live_trade_outcome records REAL trade data (token, chain, entry/exit prices,
   cost basis, fees, hold time) instead of a fabricated lot.
4. record_live_trade_outcome never rewrites the PnL sign; if is_win disagrees with PnL,
   it logs an ERROR and trusts the PnL.
5. Audit with tools/audit/task_awaitables.py produces 0 findings across the codebase.
6. End-to-end integration test through runner's real position close path.
"""

from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.experimentation import StrategyTournamentRegistry
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.engine.self_learning.self_learning_engine import SelfLearningEngine
from alpha_engine.execution.book import OpenLot
from alpha_engine.models.enums import ChainIdentifier, LotStatus, OrderSide, TradeExitReason
from alpha_engine.models.state import ExitDecision, PaperFill
from alpha_engine.pricing.native_price_service import NativePriceStatus, PriceResult
from alpha_engine.ruflo.swarm import RufloSwarmCoordinator


class MockPriceService:
    def __init__(self, sol_price: Decimal = Decimal("200.0"), eth_price: Decimal = Decimal("3000.0")):
        self.sol_price = sol_price
        self.eth_price = eth_price

    def get_price(self, chain: ChainIdentifier | str) -> PriceResult:
        c = chain.value if hasattr(chain, "value") else str(chain)
        if "base" in c.lower() or "eth" in c.lower():
            return PriceResult(Decimal(str(self.eth_price)), NativePriceStatus.OK, time.time())
        return PriceResult(Decimal(str(self.sol_price)), NativePriceStatus.OK, time.time())


def _create_test_price_service() -> MockPriceService:
    return MockPriceService()


def _create_sample_lot_and_fill(chain: ChainIdentifier = ChainIdentifier.BASE_MAINNET):
    lot = OpenLot(
        lot_id="test_lot_12345",
        token_address="0x1111222233334444555566667777888899990000",
        chain=chain,
        initial_tokens=Decimal("1000.0"),
        tokens_held=Decimal("1000.0"),
        cost_basis_native=Decimal("0.5"),
        entry_price=Decimal("0.0005"),
        signal_id="sig_test_123",
        open_timestamp_ns=time.time_ns() - 15_000_000_000,  # 15s ago
        status=LotStatus.OPEN,
        strategy_name="alpha_momentum_breakout",
        entry_native_price_usd=Decimal("3000.0"),
    )
    fill = PaperFill(
        order_id="exit_fill_999",
        token_address=lot.token_address,
        chain=lot.chain,
        side=OrderSide.SELL,
        simulated_native_spent=Decimal("0.6"),
        tokens_acquired=Decimal("1000.0"),
        effective_price=Decimal("0.0006"),
        price_impact_bps=45,
        simulated_gas_cost_usd=Decimal("0.05"),
        fill_latency_ms=62,
        signal_timestamp_ns=time.time_ns() - 100_000_000,
        fill_timestamp_ns=time.time_ns(),
        kelly_fraction=0.0,
        portfolio_equity_usd=Decimal("10000.0"),
        platform="Uniswap_v3",
    )
    decision = ExitDecision(
        lot_id=lot.lot_id,
        token_address=lot.token_address,
        chain=lot.chain,
        should_exit=True,
        exit_reason=TradeExitReason.TP_50,
        tokens_to_sell=Decimal("1000.0"),
    )
    assert decision.exit_reason == TradeExitReason.TP_50
    return lot, fill


@pytest.mark.asyncio
async def test_post_trade_hooks_invoked_exactly_once():
    """Verify that closing a trade invokes learning engine, swarm coordinator, and tournament exactly once each."""
    cfg = EngineConfig(allow_static_native_prices=True)
    engine = PaperTradingEngine(config=cfg)
    price_svc = _create_test_price_service()
    engine._native_price_service = price_svc

    mock_learning = MagicMock(spec=SelfLearningEngine)
    mock_learning.on_trade_closed = AsyncMock(return_value={"status": "ok"})

    mock_swarm = MagicMock(spec=RufloSwarmCoordinator)
    mock_swarm.on_trade_closed = AsyncMock(return_value={"status": "ok"})

    mock_tournament = MagicMock()
    mock_tournament.record_live_trade_outcome = MagicMock()

    lot, fill = _create_sample_lot_and_fill()

    with patch.object(engine, "self_learning_engine", mock_learning), \
         patch.object(engine, "_swarm_coordinator", mock_swarm), \
         patch.object(engine, "_strategy_tournament", mock_tournament):
        engine._record_closed_trade_and_notify(
            lot=lot,
            exit_fill=fill,
            decision_reason=TradeExitReason.TP_50.value,
            exit_pnl_usd=Decimal("300.0"),
            pnl_native=Decimal("0.1"),
        )

        if hasattr(engine, "await_pending_hooks"):
            await engine.await_pending_hooks()
        else:
            await asyncio.sleep(0.05)

    assert mock_learning.on_trade_closed.await_count == 1, (
        f"Expected self_learning_engine.on_trade_closed awaited exactly once, got {mock_learning.on_trade_closed.await_count}"
    )
    assert mock_swarm.on_trade_closed.await_count == 1, (
        f"Expected _swarm_coordinator.on_trade_closed awaited exactly once, got {mock_swarm.on_trade_closed.await_count}"
    )
    assert mock_tournament.record_live_trade_outcome.call_count == 1, (
        f"Expected record_live_trade_outcome called exactly once, got {mock_tournament.record_live_trade_outcome.call_count}"
    )


@pytest.mark.asyncio
async def test_failure_in_one_consumer_does_not_skip_others_and_logged_at_error(caplog):
    """Verify that an error in self-learning engine does not skip swarm coordinator or tournament, logs ERROR, and increments telemetry."""
    cfg = EngineConfig(allow_static_native_prices=True)
    engine = PaperTradingEngine(config=cfg)
    price_svc = _create_test_price_service()
    engine._native_price_service = price_svc

    mock_learning = MagicMock(spec=SelfLearningEngine)
    mock_learning.on_trade_closed = AsyncMock(side_effect=RuntimeError("Simulated SelfLearning failure!"))
    engine.self_learning_engine = mock_learning

    mock_swarm = MagicMock(spec=RufloSwarmCoordinator)
    mock_swarm.on_trade_closed = AsyncMock(return_value={"status": "ok"})
    engine._swarm_coordinator = mock_swarm

    mock_tournament = MagicMock()
    mock_tournament.record_live_trade_outcome = MagicMock()
    engine._strategy_tournament = mock_tournament

    lot, fill = _create_sample_lot_and_fill()

    with caplog.at_level(logging.ERROR):
        engine._record_closed_trade_and_notify(
            lot=lot,
            exit_fill=fill,
            decision_reason="take_profit_target_hit",
            exit_pnl_usd=Decimal("300.0"),
            pnl_native=Decimal("0.1"),
        )
        if hasattr(engine, "await_pending_hooks"):
            await engine.await_pending_hooks()
        else:
            await asyncio.sleep(0.05)

    # Swarm and tournament must have run despite learning engine failure
    assert mock_swarm.on_trade_closed.await_count == 1, "Swarm coordinator should still be invoked if learning engine fails"
    assert mock_tournament.record_live_trade_outcome.call_count == 1, "Tournament should still be invoked if learning engine fails"

    # Must log at ERROR with traceback
    error_logs = [rec for rec in caplog.records if rec.levelno >= logging.ERROR]
    assert any("self_learning" in rec.message.lower() or "post-trade hook" in rec.message.lower() for rec in error_logs), (
        f"Expected ERROR level log for hook failure, got: {[r.message for r in error_logs]}"
    )
    # Telemetry counter incremented
    assert engine.telemetry_counters.get("hook_self_learning_engine_failed", 0) > 0 or engine.telemetry_counters.get("post_trade_hook_failed", 0) > 0


def test_record_live_trade_outcome_records_real_trade_data():
    """Verify record_live_trade_outcome records REAL lot data, not fabricated constants."""
    registry = StrategyTournamentRegistry(config=EngineConfig())
    champion = registry.champion
    assert champion is not None

    real_token = "0xRealEVMTokenContractAddress000000000000"
    real_chain = ChainIdentifier.BASE_MAINNET
    entry_p = Decimal("0.00125")
    exit_p = Decimal("0.00185")
    cost_basis_usd = Decimal("245.50")
    realized_pnl_usd = Decimal("117.84")
    roi_pct = 48.0
    hold_time_s = 72.5
    gas_drag_usd = Decimal("0.12")
    cpmm_slippage_bps = 85
    latency_ms = 145.0
    exit_reason = TradeExitReason.TP_50.value

    registry.record_live_trade_outcome(
        strategy_name=champion.strategy_name,
        token_address=real_token,
        chain=real_chain,
        entry_price=entry_p,
        exit_price=exit_p,
        tokens_acquired=Decimal("196400.0"),
        cost_basis_usd=cost_basis_usd,
        realized_pnl_usd=realized_pnl_usd,
        roi_pct=roi_pct,
        hold_time_s=hold_time_s,
        gas_drag_usd=gas_drag_usd,
        cpmm_slippage_bps=cpmm_slippage_bps,
        latency_ms=latency_ms,
        exit_reason=exit_reason,
        is_win=True,
    )

    assert len(champion.closed_lots) == 1
    recorded_lot = champion.closed_lots[0]

    assert recorded_lot.token_address == real_token, f"Expected {real_token}, got {recorded_lot.token_address}"
    assert recorded_lot.chain == real_chain, f"Expected {real_chain}, got {recorded_lot.chain}"
    assert recorded_lot.entry_price == entry_p, f"Expected {entry_p}, got {recorded_lot.entry_price}"
    assert recorded_lot.exit_price == exit_p, f"Expected {exit_p}, got {recorded_lot.exit_price}"
    assert recorded_lot.cost_basis_usd == cost_basis_usd, f"Expected {cost_basis_usd}, got {recorded_lot.cost_basis_usd}"
    assert recorded_lot.gas_drag_usd == gas_drag_usd, f"Expected {gas_drag_usd}, got {recorded_lot.gas_drag_usd}"
    assert recorded_lot.cpmm_slippage_bps == cpmm_slippage_bps, f"Expected {cpmm_slippage_bps}, got {recorded_lot.cpmm_slippage_bps}"
    assert recorded_lot.latency_ms == latency_ms, f"Expected {latency_ms}, got {recorded_lot.latency_ms}"
    assert recorded_lot.realized_pnl_usd == realized_pnl_usd, f"Expected {realized_pnl_usd}, got {recorded_lot.realized_pnl_usd}"
    assert recorded_lot.roi_pct == roi_pct
    assert recorded_lot.exit_timestamp is not None
    assert recorded_lot.entry_timestamp < recorded_lot.exit_timestamp


def test_record_live_trade_outcome_never_rewrites_pnl_sign_and_trusts_pnl(caplog):
    """Verify record_live_trade_outcome never rewrites PnL sign, logs ERROR on disagreement, and trusts PnL."""
    registry = StrategyTournamentRegistry(config=EngineConfig())
    champion = registry.champion
    assert champion is not None

    # Case 1: is_win=True, but realized_pnl_usd is negative!
    with caplog.at_level(logging.ERROR):
        registry.record_live_trade_outcome(
            strategy_name=champion.strategy_name,
            token_address="0xTestTokenLossDisagreement",
            chain=ChainIdentifier.BASE_MAINNET,
            entry_price=Decimal("1.0"),
            exit_price=Decimal("0.8"),
            tokens_acquired=Decimal("100.0"),
            cost_basis_usd=Decimal("100.0"),
            realized_pnl_usd=Decimal("-20.0"),
            roi_pct=-20.0,
            hold_time_s=30.0,
            is_win=True,  # Disagrees with negative PnL!
        )

    recorded_lot = champion.closed_lots[-1]
    # PnL MUST stay -20.0, NOT converted to +20.0!
    assert recorded_lot.realized_pnl_usd == Decimal("-20.0"), (
        f"PnL sign was rewritten! Expected Decimal('-20.0'), got {recorded_lot.realized_pnl_usd}"
    )
    # Must log ERROR for disagreement
    error_logs = [rec for rec in caplog.records if rec.levelno >= logging.ERROR]
    assert any("disagree" in rec.message.lower() or "is_win" in rec.message.lower() for rec in error_logs), (
        "Expected ERROR log when is_win contradicts realized_pnl_usd"
    )

    caplog.clear()

    # Case 2: is_win=False, but realized_pnl_usd is positive!
    with caplog.at_level(logging.ERROR):
        registry.record_live_trade_outcome(
            strategy_name=champion.strategy_name,
            token_address="0xTestTokenWinDisagreement",
            chain=ChainIdentifier.BASE_MAINNET,
            entry_price=Decimal("1.0"),
            exit_price=Decimal("1.5"),
            tokens_acquired=Decimal("100.0"),
            cost_basis_usd=Decimal("100.0"),
            realized_pnl_usd=Decimal("50.0"),
            roi_pct=50.0,
            hold_time_s=30.0,
            is_win=False,  # Disagrees with positive PnL!
        )

    recorded_lot2 = champion.closed_lots[-1]
    assert recorded_lot2.realized_pnl_usd == Decimal("50.0"), (
        f"PnL sign was rewritten! Expected Decimal('50.0'), got {recorded_lot2.realized_pnl_usd}"
    )
    error_logs = [rec for rec in caplog.records if rec.levelno >= logging.ERROR]
    assert any("disagree" in rec.message.lower() or "is_win" in rec.message.lower() for rec in error_logs)


def test_task_awaitables_audit_zero_findings():
    """Run tools/audit/task_awaitables.py and verify 0 findings across repository."""
    cmd = [sys.executable, "tools/audit/task_awaitables.py"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"task_awaitables audit failed with findings:\n{res.stdout}\n{res.stderr}"
    assert "Total findings: 0" in res.stderr or "Total findings: 0" in res.stdout


@pytest.mark.asyncio
async def test_integration_runner_real_close_path():
    """Integration test through runner's real position closing path."""
    cfg = EngineConfig(allow_static_native_prices=True)
    engine = PaperTradingEngine(config=cfg)
    price_svc = _create_test_price_service()
    engine._native_price_service = price_svc

    # Open a real lot in position book
    lot = OpenLot(
        lot_id="integration_lot_123",
        token_address="Token111111111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("500.0"),
        tokens_held=Decimal("500.0"),
        cost_basis_native=Decimal("2.5"),
        entry_price=Decimal("0.005"),
        signal_id="sig_integration_1",
        open_timestamp_ns=time.time_ns() - 10_000_000_000,
        status=LotStatus.OPEN,
        strategy_name="peak_trailing_50",
        entry_native_price_usd=Decimal("200.0"),
    )
    lot.peak_price = Decimal("0.005")
    lot.trailing_stop_from_peak_pct = 0.50
    lot.partial_exit_enabled = False
    engine._position_book._lots[(lot.chain.value, lot.token_address)] = [lot]

    # Wire spy hooks to verify real execution
    learning_called = []
    original_learning_hook = engine.self_learning_engine.on_trade_closed
    async def spy_learning(payload):
        learning_called.append(payload)
        res = original_learning_hook(payload)
        if asyncio.iscoroutine(res):
            return await res
        return res
    engine.self_learning_engine.on_trade_closed = spy_learning

    swarm_called = []
    if engine._swarm_coordinator:
        original_swarm_hook = engine._swarm_coordinator.on_trade_closed
        async def spy_swarm(payload):
            swarm_called.append(payload)
            return await original_swarm_hook(payload)
        engine._swarm_coordinator.on_trade_closed = spy_swarm

    token = lot.token_address
    pool_addr = "Pool1111111111111111111111111111111111111111"
    lot.pool_address = pool_addr
    lot.peak_pool_reserve_native = Decimal("100.0")

    from alpha_engine.models.events import SwapEvent
    from alpha_engine.models.state import PoolState

    pool = PoolState(
        pool_address=pool_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        spot_price_native_per_token=Decimal("0.010"),  # +100% price -> triggers TP
        token_reserve=Decimal("10000.0"),
        native_reserve=Decimal("100.0"),
        fee_numerator=3000,
        fee_denominator=1000000,
        last_updated_block=123456,
        token_decimals=6,
        native_decimals=9,
        token_address=token,
    )
    engine._pool_registry._pools[pool.pool_address] = pool

    swap = SwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        token_in=token,
        token_out="So11111111111111111111111111111111111111112",
        amount_in=Decimal("100.0"),
        amount_out=Decimal("1.0"),
        sender="MakerWallet123MakerWallet123MakerWallet123",
        timestamp_ns=time.time_ns(),
        block_number=123456,
        tx_hash="txhash123txhash123txhash123txhash123",
    )

    # Close through runner's real closing path
    await engine._check_dynamic_exits_for_swap(swap, pool)

    if hasattr(engine, "await_pending_hooks"):
        await engine.await_pending_hooks()
    else:
        await asyncio.sleep(0.1)

    # 1. Position book reflects closure
    assert lot.status == LotStatus.CLOSED
    assert isinstance(engine.self_learning_engine, SelfLearningEngine)
    # 2. Learning hook called
    assert len(learning_called) == 1, "Self-learning hook was not called in real closing path"
    # 3. Swarm coordinator called if present
    if engine._swarm_coordinator:
        assert isinstance(engine._swarm_coordinator, RufloSwarmCoordinator)
        assert len(swarm_called) == 1, "Swarm coordinator hook was not called in real closing path"
    # 4. Tournament recorded real lot
    champion_lots = engine._strategy_tournament.champion.closed_lots
    assert len(champion_lots) >= 1
    latest = champion_lots[-1]
    assert latest.token_address == lot.token_address
    assert latest.chain == lot.chain
    assert latest.entry_price == lot.entry_price
    assert latest.cost_basis_usd > Decimal(0)
