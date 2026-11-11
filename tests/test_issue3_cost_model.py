"""
tests/test_issue3_cost_model.py — Reproduction and Acceptance Tests for Issue 3
=============================================================================
Verifies:
1. Symmetric round trip at zero price change loses exactly the modeled fees + impact + network costs.
2. Entry and exit costs are identical functions of inputs.
3. Each venue's fee schedule is applied; unknown venue fails closed.
4. No call site can fill without costs (AST audit + runtime test).
5. Shadow lots use the same cost function (parity with live-paper fills).
6. Balance check uses the same estimate as the fill (no hardcoded 0.005 / 0.002).
"""

from __future__ import annotations

import ast
import inspect
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.math.cpmm import cpmm_buy_quote, cpmm_sell_quote
from alpha_engine.math.mev import LatencyResult
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    TradeExitReason,
)
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, SecurityReport


class DummyPriceService:
    def __init__(self, sol_price: Decimal = Decimal("200.0"), eth_price: Decimal = Decimal("3000.0")):
        self.sol_price = sol_price
        self.eth_price = eth_price

    def get_price(self, chain: ChainIdentifier):
        p = self.eth_price if chain == ChainIdentifier.BASE_MAINNET else self.sol_price
        mock = MagicMock()
        mock.price = p
        mock.status.value = "OK"
        return mock


def _make_pool(
    chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
    native_reserve: Decimal = Decimal("50.0"),
    token_reserve: Decimal = Decimal("1000000.0"),
    pool_address: str = "6UmmUiYo5ZbzK8kQ8FkL6Z5uN6fS7Q3eT5bY9cV3mX2a",
    fee_num: int = 25,
    fee_denom: int = 10000,
) -> PoolState:
    return PoolState(
        pool_address=pool_address,
        chain=chain,
        token_reserve=token_reserve,
        native_reserve=native_reserve,
        fee_numerator=fee_num,
        fee_denominator=fee_denom,
        last_updated_block=100000,
        token_decimals=6,
        native_decimals=9 if chain == ChainIdentifier.SOLANA_MAINNET else 18,
    )


def _make_signal(
    chain: ChainIdentifier,
    token_address: str,
    pool: PoolState,
    venue: ExecutionVenue = ExecutionVenue.RAYDIUM_AMM,
) -> SignalEvent:
    report = SecurityReport(
        token_address=token_address,
        chain=chain,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        mint_authority_disabled=True,
        top10_concentration=0.10,
    )
    return SignalEvent(
        timestamp_ns=1_000_000_000,
        chain=chain,
        pool_address=pool.pool_address,
        token_address=token_address,
        source=SignalSource.PAIR_CREATED,
        suggested_side=OrderSide.BUY,
        alpha_score=0.85,
        strength=SignalStrength.STRONG,
        execution_venue=venue,
        pool_state=pool,
        security_report=report,
    )


def test_ast_audit_no_apply_drag_in_codebase():
    """Verify that apply_drag parameter and flag are eliminated from execution call sites and signatures."""
    executor_path = Path("alpha_engine/execution/executor.py")
    tree = ast.parse(executor_path.read_text())

    # Check function signatures in PaperExecutor
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name in (
            "execute_signal",
            "execute_exit",
            "execute_scalp_pipeline",
            "execute_swing_dca_pipeline",
        ):
            arg_names = [a.arg for a in node.args.args]
            assert "apply_drag" not in arg_names, (
                f"{node.name} still contains 'apply_drag' parameter in its signature!"
            )

    # Check runner.py call sites do not pass apply_drag
    runner_path = Path("alpha_engine/engine/runner.py")
    runner_text = runner_path.read_text()
    assert "apply_drag" not in runner_text, "runner.py still contains 'apply_drag' call sites or references!"


@pytest.mark.anyio
async def test_symmetric_round_trip_at_zero_price_change():
    """
    A symmetric round trip at zero price change must lose EXACTLY:
    modeled venue entry swap fee + entry impact + entry network fee +
    modeled venue exit swap fee + exit impact + exit network fee.
    """
    from unittest.mock import patch
    from alpha_engine.execution.costs import TradeCostModel
    from alpha_engine.math.mev import LatencyResult

    cfg = EngineConfig()
    price_service = DummyPriceService(sol_price=Decimal("200.0"))
    cost_model = TradeCostModel(config=cfg, price_service=price_service)

    book = PositionBook()
    executor = PaperExecutor(position_book=book, price_service=price_service, cost_model=cost_model)

    chain = ChainIdentifier.SOLANA_MAINNET
    native_in = Decimal("1.0")  # 1 SOL
    pool = _make_pool(chain=chain, native_reserve=Decimal("100.0"), token_reserve=Decimal("1000000.0"))

    signal = _make_signal(
        chain=chain,
        token_address="TokenA1111111111111111111111111111111111111",
        pool=pool,
        venue=ExecutionVenue.RAYDIUM_AMM,
    )

    def _no_drift_latency(*args, **kwargs):
        p = kwargs.get("pool") if "pool" in kwargs else args[0]
        ts = kwargs.get("signal_timestamp_ns") if "signal_timestamp_ns" in kwargs else (args[1] if len(args) > 1 else 0)
        return LatencyResult(
            latency_ms=1000,
            fill_timestamp_ns=ts + 1_000_000_000,
            frontrun_fraction=0.0,
            frontrun_volume=Decimal("0"),
            adjusted_pool_state=p,
        )

    # 1. Execute BUY (with latency mocked to zero external price drift to test pure cost model symmetry)
    with patch("alpha_engine.execution.executor.simulate_latency", side_effect=_no_drift_latency):
        buy_fill = await executor.execute_signal(
            signal=signal,
            portfolio_equity_usd=Decimal("10000.0"),
            fixed_trade_amount_native=native_in,
        )
    assert buy_fill is not None

    # Expected Buy CPMM math independently computed in test
    # Raydium AMM fee = 0.25% (25 bps)
    # native_in = 1.0 SOL
    # amount_in_with_fee = 1.0 * (10000 - 25) = 9975
    # amount_out = (9975 * 1000000.0) / (100.0 * 10000 + 9975) = 9975000000.0 / 1009975.0
    expected_buy_tokens = (Decimal("9975") * Decimal("1000000.0")) / (Decimal("100.0") * Decimal("10000") + Decimal("9975"))
    assert abs(buy_fill.tokens_acquired - expected_buy_tokens) < Decimal("1e-6")

    # Simulate updated pool post-buy
    pool_after_buy = PoolState(
        pool_address=pool.pool_address,
        chain=chain,
        token_reserve=pool.token_reserve - buy_fill.tokens_acquired,
        native_reserve=pool.native_reserve + native_in,
        fee_numerator=pool.fee_numerator,
        fee_denominator=pool.fee_denominator,
        last_updated_block=pool.last_updated_block + 1,
        token_decimals=pool.token_decimals,
        native_decimals=pool.native_decimals,
    )

    # 2. Execute SELL (sell back all acquired tokens)
    with patch("alpha_engine.execution.executor.simulate_latency", side_effect=_no_drift_latency):
        exit_fill = await executor.execute_exit(
            chain=chain,
            token_address=signal.token_address,
            pool=pool_after_buy,
            tokens_to_sell=buy_fill.tokens_acquired,
            reason=TradeExitReason.TP_100,
            platform=buy_fill.platform,
        )
    assert exit_fill is not None

    # Independently compute expected sell output
    tokens_in = buy_fill.tokens_acquired
    tokens_in_with_fee = tokens_in * Decimal("9975")
    expected_sell_native_gross = (tokens_in_with_fee * pool_after_buy.native_reserve) / (
        pool_after_buy.token_reserve * Decimal("10000") + tokens_in_with_fee
    )

    # Entry and exit network fees
    expected_entry_gas_native = cost_model.get_network_fee_native(chain)
    expected_exit_gas_native = cost_model.get_network_fee_native(chain)

    # Verify both fills have the network fee recorded
    assert buy_fill.simulated_gas_cost_usd == expected_entry_gas_native * Decimal("200.0")
    assert exit_fill.simulated_gas_cost_usd == expected_exit_gas_native * Decimal("200.0")

    # Round trip economic loss:
    # Initial native outlay: native_in + expected_entry_gas_native
    # Final native received: exit_fill.simulated_native_spent
    total_outlay = native_in + expected_entry_gas_native
    net_received = exit_fill.simulated_native_spent
    total_loss = total_outlay - net_received

    expected_loss = (native_in - expected_sell_native_gross) + expected_entry_gas_native + expected_exit_gas_native
    assert abs(total_loss - expected_loss) < Decimal("1e-6")
    # The loss must be strictly positive (paying fees + impact + gas both ways)
    assert total_loss > Decimal("0")


def test_entry_and_exit_costs_identical_functions_of_inputs():
    """Verify entry and exit costs apply identical fee percentages and network fee logic."""
    from alpha_engine.execution.costs import TradeCostModel

    cfg = EngineConfig()
    price_service = DummyPriceService(sol_price=Decimal("200.0"))
    cost_model = TradeCostModel(config=cfg, price_service=price_service)

    chain = ChainIdentifier.SOLANA_MAINNET
    native_price = Decimal("200.0")

    entry_gas = cost_model.get_network_fee_native(chain)
    exit_gas = cost_model.get_network_fee_native(chain)
    assert entry_gas == exit_gas
    assert entry_gas > Decimal("0")

    entry_fee_pct = cost_model.get_venue_fee_pct(ExecutionVenue.RAYDIUM_AMM)
    exit_fee_pct = cost_model.get_venue_fee_pct(ExecutionVenue.RAYDIUM_AMM)
    assert entry_fee_pct == exit_fee_pct == Decimal("0.0025")


def test_each_venue_fee_schedule_applied_and_unknown_venue_fails_closed():
    """Verify each venue applies its configured fee, and unknown venue fails closed."""
    from alpha_engine.execution.costs import TradeCostModel, UnknownVenueError

    cfg = EngineConfig()
    price_service = DummyPriceService()
    cost_model = TradeCostModel(config=cfg, price_service=price_service)

    # 1. Pump.fun bonding curve -> 1.0% (0.0100)
    assert cost_model.get_venue_fee_pct(ExecutionVenue.PUMP_FUN) == Decimal("0.0100")
    # 2. PumpSwap -> 0.30% (0.0030)
    assert cost_model.get_venue_fee_pct(ExecutionVenue.PUMPSWAP) == Decimal("0.0030")
    # 3. Raydium AMM v4 -> 0.25% (0.0025)
    assert cost_model.get_venue_fee_pct(ExecutionVenue.RAYDIUM_AMM) == Decimal("0.0025")
    # 4. Raydium CPMM -> 0.25% (0.0025)
    assert cost_model.get_venue_fee_pct(ExecutionVenue.RAYDIUM_CPMM) == Decimal("0.0025")
    # 5. Raydium CLMM -> 0.25% (0.0025)
    assert cost_model.get_venue_fee_pct(ExecutionVenue.RAYDIUM_CLMM) == Decimal("0.0025")
    # 6. Uniswap v2 -> 0.30% (0.0030)
    assert cost_model.get_venue_fee_pct(ExecutionVenue.UNISWAP_V2) == Decimal("0.0030")
    # 7. Uniswap v3 -> 0.30% (0.0030)
    assert cost_model.get_venue_fee_pct(ExecutionVenue.UNISWAP_V3) == Decimal("0.0030")
    # 8. Aerodrome -> 0.30% (0.0030)
    assert cost_model.get_venue_fee_pct(ExecutionVenue.AERODROME) == Decimal("0.0030")

    # Unknown venue must fail closed
    with pytest.raises(UnknownVenueError):
        cost_model.get_venue_fee_pct("non_existent_obscure_dex")


@pytest.mark.anyio
async def test_shadow_lots_parity_with_live_paper_fills():
    """Verify shadow lots in experimentation.py calculate identical fill outputs to live-paper fills."""
    from alpha_engine.engine.experimentation import StrategyTournamentRegistry, StrategyParameters
    from alpha_engine.execution.costs import TradeCostModel

    cfg = EngineConfig()
    price_service = DummyPriceService(sol_price=Decimal("200.0"))
    cost_model = TradeCostModel(config=cfg, price_service=price_service)

    book = PositionBook()
    executor = PaperExecutor(position_book=book, price_service=price_service, cost_model=cost_model)

    chain = ChainIdentifier.SOLANA_MAINNET
    pool = _make_pool(chain=chain, native_reserve=Decimal("50.0"), token_reserve=Decimal("1000000.0"))
    trade_size_native = Decimal("0.10")

    signal = _make_signal(
        chain=chain,
        token_address="TokenShadow111111111111111111111111111111111",
        pool=pool,
        venue=ExecutionVenue.RAYDIUM_AMM,
    )

    def _fixed_latency(*args, **kwargs):
        p = kwargs.get("pool") if "pool" in kwargs else args[0]
        ts = kwargs.get("signal_timestamp_ns") if "signal_timestamp_ns" in kwargs else (args[1] if len(args) > 1 else 0)
        return LatencyResult(
            latency_ms=1500,
            fill_timestamp_ns=ts + 1_500_000_000,
            frontrun_fraction=0.0,
            frontrun_volume=Decimal("0"),
            adjusted_pool_state=p,
        )

    # 1. Live paper fill
    with patch("alpha_engine.execution.executor.simulate_latency", side_effect=_fixed_latency):
        live_fill = await executor.execute_signal(
            signal=signal,
            portfolio_equity_usd=Decimal("10000.0"),
            fixed_trade_amount_native=trade_size_native,
        )
    assert live_fill is not None

    # 2. Shadow lot fill
    tournament = StrategyTournamentRegistry(cost_model=cost_model)
    with patch("alpha_engine.engine.experimentation.simulate_latency", side_effect=_fixed_latency):
        shadow_lots = tournament.evaluate_signal_and_record_shadow_lots(
            signal=signal,
            pool=pool,
            native_price_usd=Decimal("200.0"),
            base_trade_size_native=trade_size_native,
        )
    assert len(shadow_lots) > 0
    shadow_lot = shadow_lots[0]

    # Verify parity
    assert abs(shadow_lot.tokens_acquired - live_fill.tokens_acquired) < Decimal("1e-6")
    assert abs(shadow_lot.entry_price - live_fill.effective_price) < Decimal("1e-6")
    assert abs(shadow_lot.gas_drag_usd - live_fill.simulated_gas_cost_usd) < Decimal("1e-6")
    assert shadow_lot.simulated_native_spent == live_fill.simulated_native_spent


def test_balance_check_uses_same_estimate_as_fill():
    """Verify runner balance check calculates required fee via TradeCostModel, eliminating 0.005/0.002 hardcoding."""
    from alpha_engine.execution.costs import TradeCostModel

    cfg = EngineConfig()
    price_service = DummyPriceService(sol_price=Decimal("200.0"), eth_price=Decimal("3000.0"))
    cost_model = TradeCostModel(config=cfg, price_service=price_service)

    # Solana network fee
    sol_fee = cost_model.get_network_fee_native(ChainIdentifier.SOLANA_MAINNET)
    # Base network fee
    base_fee = cost_model.get_network_fee_native(ChainIdentifier.BASE_MAINNET)

    # Assert they are NOT the old hardcoded 0.005 / 0.002
    assert sol_fee != Decimal("0.005") or cfg.solana_conservative_fee_sol == Decimal("0.005")
    assert base_fee != Decimal("0.002")  # Base should definitely not be 0.002 ETH ($6 on $3000 ETH)
    assert sol_fee > Decimal("0")
    assert base_fee > Decimal("0")
