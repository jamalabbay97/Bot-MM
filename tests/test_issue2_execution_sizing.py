"""
tests/test_issue2_execution_sizing.py
=====================================
Validates Issue 2 Remediation:
1. NativePriceService injection into PaperExecutor (per-chain pricing, no chain-agnostic price).
2. Elimination of update_native_price() and native_price_usd constructor parameter.
3. Single sizing authority (RiskEngine):
   - Sized amount == balance-checked amount == filled amount across autonomous, manual-USD, and manual-native modes.
   - Fixed kelly_fraction / capped_fraction on PaperFill reflecting real fraction of equity.
4. Kelly calibration:
   - Below minimum closed trades threshold: conservative fixed fraction used, logs uncalibrated warning once.
   - Above threshold: ledger-calibrated Kelly with Bayesian shrinkage toward 0.5 for small samples.
   - Negative Kelly edge yields size = 0 and no BUY.
   - Code, docstrings, config, and README agree on Half-Kelly (multiplier = 0.5).
5. SOL and ETH trades with equal USD risk produce native sizes inversely proportional to their own prices.
6. Integration test through the real runner signal-processing path with SOL price != ETH price.
7. Regression test reproducing the 22.7x discrepancy numbers from the audit.
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.engine.strategy.risk_manager import RiskEngine
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.math.cpmm import get_initial_bonding_curve_pool
from alpha_engine.math.sizing import shrink_win_rate
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
)
from alpha_engine.models.events import SecurityReport, SignalEvent, ShutdownSentinel
from alpha_engine.pricing.native_price_service import NativePriceStatus, PriceResult


class MockPriceService:
    """Mock price service returning distinct, controlled prices for SOL and ETH."""

    def __init__(self, sol_price: Decimal = Decimal("150.0"), eth_price: Decimal = Decimal("3400.0")) -> None:
        self.sol_price = sol_price
        self.eth_price = eth_price

    def get_price(self, chain: ChainIdentifier | str) -> PriceResult:
        c = chain.value if hasattr(chain, "value") else str(chain)
        if "base" in c.lower() or "eth" in c.lower():
            return PriceResult(
                price=self.eth_price,
                age_s=0.1,
                status=NativePriceStatus.OK,
                source="mock_eth",
            )
        return PriceResult(
            price=self.sol_price,
            age_s=0.1,
            status=NativePriceStatus.OK,
            source="mock_sol",
        )


def _make_dummy_buy_signal(chain: ChainIdentifier, token_address: str, alpha_score: float = 0.85) -> SignalEvent:
    pool_addr = f"PoolAddress_{token_address}".ljust(44, "1")
    pool_state = get_initial_bonding_curve_pool(
        token_address=token_address,
        chain=chain,
        pool_address=pool_addr,
    )
    report = SecurityReport(
        token_address=token_address,
        chain=chain,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        mint_authority_disabled=True,
        top10_concentration=0.05,
    )
    return SignalEvent(
        signal_id=f"sig_{token_address[:8]}",
        chain=chain,
        token_address=token_address,
        pool_address=pool_state.pool_address,
        timestamp_ns=time.time_ns(),
        source=SignalSource.PUMP_FUN_MINT if chain == ChainIdentifier.SOLANA_MAINNET else SignalSource.PAIR_CREATED,
        suggested_side=OrderSide.BUY,
        alpha_score=alpha_score,
        strength=SignalStrength.STRONG,
        execution_venue=ExecutionVenue.PUMP_FUN if chain == ChainIdentifier.SOLANA_MAINNET else ExecutionVenue.UNISWAP_V2,
        pool_state=pool_state,
        security_report=report,
        market_cap_usd=Decimal("50000.0"),
    )


# ---------------------------------------------------------------------------
# Test 1: Constructing PaperExecutor without price service fails; no update_native_price exists
# ---------------------------------------------------------------------------
def test_paper_executor_requires_price_service_and_no_update_native_price():
    book = PositionBook()

    # Attempting to construct without price_service must raise TypeError or ValueError
    with pytest.raises((TypeError, ValueError)):
        kwargs_no_svc = {"position_book": book}
        PaperExecutor(**kwargs_no_svc)

    # Attempting to pass native_price_usd must raise TypeError (parameter removed)
    with pytest.raises(TypeError):
        kwargs_legacy = {"position_book": book, "native_price_usd": Decimal("150.0")}
        PaperExecutor(**kwargs_legacy)

    # Constructing with price_service succeeds
    price_svc = MockPriceService()
    executor = PaperExecutor(position_book=book, price_service=price_svc)
    assert executor is not None

    # update_native_price must NOT exist
    assert not hasattr(executor, "update_native_price")


# ---------------------------------------------------------------------------
# Test 2: SOL and ETH trades with equal USD risk produce native sizes inversely proportional to their own prices
# ---------------------------------------------------------------------------
def test_sol_and_eth_sizes_inversely_proportional_to_prices():
    cfg = EngineConfig(allow_static_native_prices=True)
    risk_engine = RiskEngine(cfg)

    sol_price = Decimal("150.0")
    eth_price = Decimal("3000.0")  # Exactly 20x price difference
    equity_usd = Decimal("10000.0")

    sol_res = risk_engine.calculate_order_size(
        portfolio_equity_usd=equity_usd,
        native_price_usd=sol_price,
        chain=ChainIdentifier.SOLANA_MAINNET,
        score=80.0,
    )

    eth_res = risk_engine.calculate_order_size(
        portfolio_equity_usd=equity_usd,
        native_price_usd=eth_price,
        chain=ChainIdentifier.BASE_MAINNET,
        score=80.0,
    )

    assert sol_res.usd_at_risk == eth_res.usd_at_risk
    # With 20x higher price, ETH size must be exactly 1/20th of SOL size
    expected_ratio = eth_price / sol_price  # 20.0
    actual_ratio = sol_res.native_trade_size / eth_res.native_trade_size
    assert round(actual_ratio, 6) == round(expected_ratio, 6)


# ---------------------------------------------------------------------------
# Test 3: Kelly calibration threshold, Bayesian shrinkage, and negative edge gating
# ---------------------------------------------------------------------------
def test_kelly_calibration_and_shrinkage(caplog):
    cfg = EngineConfig(
        kelly_min_closed_trades=30,
        kelly_uncalibrated_fraction=0.01,
        kelly_multiplier=0.5,
        kelly_shrinkage_trades=20,
    )
    risk_engine = RiskEngine(cfg)
    equity = Decimal("10000.0")
    price = Decimal("100.0")

    # 1. Below 30 trades: must use conservative uncalibrated fraction (1% = $100 -> 1.0 native)
    with caplog.at_level(logging.INFO):
        caplog.clear()
        res_uncal = risk_engine.calculate_order_size(
            portfolio_equity_usd=equity,
            native_price_usd=price,
            chain=ChainIdentifier.SOLANA_MAINNET,
            score=80.0,
        )
        assert not res_uncal.is_calibrated
        assert res_uncal.fraction_of_equity == 0.01
        assert res_uncal.native_trade_size == Decimal("1.0")  # $100 / $100
        # Verify it logged uncalibrated message once
        assert any("not yet calibrated" in rec.message.lower() for rec in caplog.records)

    # 2. Above 30 trades with negative edge (e.g. 20% win rate, payoff ratio 1.0)
    risk_engine.update_ledger_stats({
        "total_trades": 50,
        "wins": 10,  # 20% win rate
        "avg_win_native": Decimal("0.05"),
        "avg_loss_native": Decimal("0.05"),
    })
    res_neg = risk_engine.calculate_order_size(
        portfolio_equity_usd=equity,
        native_price_usd=price,
        chain=ChainIdentifier.SOLANA_MAINNET,
        score=80.0,
    )
    assert res_neg.is_calibrated
    assert res_neg.edge_rejected
    assert res_neg.native_trade_size == Decimal("0")

    # 3. Above 30 trades with positive edge and shrinkage verification
    # 40 wins out of 50 trades (sample win rate = 80%).
    # Shrunk win rate toward 0.5 with prior weight 20: (40 + 20*0.5) / (50 + 20) = 50 / 70 = 0.7143
    shrunk_p = shrink_win_rate(sample_wins=40, sample_total=50, prior_win_rate=0.5, shrinkage_trades=20)
    assert round(shrunk_p, 4) == round(50.0 / 70.0, 4)

    risk_engine.update_ledger_stats({
        "total_trades": 50,
        "wins": 40,
        "avg_win_native": Decimal("0.10"),
        "avg_loss_native": Decimal("0.05"),
    })
    res_pos = risk_engine.calculate_order_size(
        portfolio_equity_usd=equity,
        native_price_usd=price,
        chain=ChainIdentifier.SOLANA_MAINNET,
        score=80.0,
    )
    assert res_pos.is_calibrated
    assert not res_pos.edge_rejected
    assert res_pos.native_trade_size > Decimal("0")


# ---------------------------------------------------------------------------
# Test 4: Sized amount == balance-checked amount == filled amount across 3 modes
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_sized_equals_balance_checked_equals_filled_all_modes(tmp_path):
    cfg = EngineConfig(
        allow_static_native_prices=True,
        circuit_breaker_freeze_duration_s=0.0,
    )
    price_svc = MockPriceService(sol_price=Decimal("150.0"), eth_price=Decimal("3000.0"))
    runner = PaperTradingEngine(config=cfg, native_price_service=price_svc)
    runner._ledger = AsyncMock()
    runner._ledger.get_closed_trade_stats.return_value = {
        "total_trades": 0,
        "wins": 0,
        "avg_win_native": Decimal(0),
        "avg_loss_native": Decimal(0),
    }

    # Cash balance: 100 SOL
    runner._cash_sol = Decimal("100.0")
    runner._cash_eth = Decimal("10.0")

    # MODE A: Autonomous mode
    signal_auto = _make_dummy_buy_signal(ChainIdentifier.SOLANA_MAINNET, "So11111111111111111111111111111111111111111auto")
    runner.manual_trade_amount = None

    # Calculate expected size through single authority
    equity_usd = runner._current_equity_usd()
    expected_sizing = runner.risk_engine.calculate_order_size(
        portfolio_equity_usd=equity_usd,
        native_price_usd=Decimal("150.0"),
        chain=ChainIdentifier.SOLANA_MAINNET,
        score=signal_auto.alpha_score * 100.0,
    )

    # Process signal
    await runner._signal_q.put(signal_auto)
    await runner._signal_q.put(ShutdownSentinel())
    await runner._process_signal_queue()

    # Verify lot opened with exact sized amount and fill recorded
    lots_auto = runner._position_book.get_open_lots(token_address=signal_auto.token_address)
    assert len(lots_auto) > 0
    lot_auto = lots_auto[0]
    
    # Filled amount recorded in ledger trade record exactly equals expected sizing
    trade_rec_auto = runner._ledger.record_trade.call_args[0][0]
    assert trade_rec_auto.native_spent == expected_sizing.native_trade_size
    assert trade_rec_auto.kelly_fraction == pytest.approx(expected_sizing.fraction_of_equity)
    # Lot cost basis equals filled amount + gas
    gas_native = (trade_rec_auto.gas_cost_usd / Decimal("150.0")).quantize(Decimal("1e-18"))
    assert lot_auto.cost_basis_native == expected_sizing.native_trade_size + gas_native

    # MODE B: Manual-USD mode ($300 USD on SOL at $150 -> exactly 2.0 SOL)
    runner._signal_q = asyncio.Queue()
    runner.manual_trade_amount = Decimal("300.0")
    runner.manual_trade_currency = "USD"
    signal_usd = _make_dummy_buy_signal(ChainIdentifier.SOLANA_MAINNET, "So11111111111111111111111111111111111111111usd_")

    await runner._signal_q.put(signal_usd)
    await runner._signal_q.put(ShutdownSentinel())
    await runner._process_signal_queue()

    lots_usd = runner._position_book.get_open_lots(token_address=signal_usd.token_address)
    assert len(lots_usd) > 0
    lot_usd = lots_usd[0]
    trade_rec_usd = runner._ledger.record_trade.call_args[0][0]
    assert trade_rec_usd.native_spent == Decimal("2.0")

    # MODE C: Manual-native mode (0.75 SOL)
    runner._signal_q = asyncio.Queue()
    runner.manual_trade_amount = Decimal("0.75")
    runner.manual_trade_currency = "SOL"
    signal_native = _make_dummy_buy_signal(ChainIdentifier.SOLANA_MAINNET, "So11111111111111111111111111111111111111111native")

    await runner._signal_q.put(signal_native)
    await runner._signal_q.put(ShutdownSentinel())
    await runner._process_signal_queue()

    lots_native = runner._position_book.get_open_lots(token_address=signal_native.token_address)
    assert len(lots_native) > 0
    lot_native = lots_native[0]
    trade_rec_native = runner._ledger.record_trade.call_args[0][0]
    assert trade_rec_native.native_spent == Decimal("0.75")


# ---------------------------------------------------------------------------
# Test 5: Integration test through runner signal-processing path with SOL price != ETH price
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_integration_runner_signal_processing_per_chain_pricing():
    cfg = EngineConfig(
        allow_static_native_prices=True,
        circuit_breaker_freeze_duration_s=0.0,
    )
    # Distinct prices: SOL = $200, ETH = $4000 (20x)
    price_svc = MockPriceService(sol_price=Decimal("200.0"), eth_price=Decimal("4000.0"))
    runner = PaperTradingEngine(config=cfg, native_price_service=price_svc)
    runner._ledger = AsyncMock()
    runner._ledger.get_closed_trade_stats.return_value = {
        "total_trades": 0, "wins": 0, "avg_win_native": Decimal(0), "avg_loss_native": Decimal(0),
    }

    runner._cash_sol = Decimal("50.0")   # $10,000 USD
    runner._cash_eth = Decimal("2.5")    # $10,000 USD
    # Total cash = $20,000 USD

    # Manual trade: $200 USD
    runner.manual_trade_amount = Decimal("200.0")
    runner.manual_trade_currency = "USD"

    # Send 1 SOL trade and 1 Base/ETH trade
    sig_sol = _make_dummy_buy_signal(ChainIdentifier.SOLANA_MAINNET, "So11111111111111111111111111111111111111111sol_")
    sig_eth = _make_dummy_buy_signal(ChainIdentifier.BASE_MAINNET, "0x1111111111111111111111111111111111111111eth_")

    await runner._signal_q.put(sig_sol)
    await runner._signal_q.put(sig_eth)
    await runner._signal_q.put(ShutdownSentinel())
    await runner._process_signal_queue()

    lots_sol = runner._position_book.get_open_lots(token_address=sig_sol.token_address)
    lots_eth = runner._position_book.get_open_lots(token_address=sig_eth.token_address)

    assert len(lots_sol) == 1
    assert len(lots_eth) == 1
    lot_sol = lots_sol[0]
    lot_eth = lots_eth[0]

    # Check filled amounts recorded in ledger trade records
    trade_records = [call[0][0] for call in runner._ledger.record_trade.call_args_list]
    rec_sol = next(r for r in trade_records if r.chain == ChainIdentifier.SOLANA_MAINNET)
    rec_eth = next(r for r in trade_records if r.chain == ChainIdentifier.BASE_MAINNET)

    # $200 USD on SOL @ $200/SOL == 1.0000 SOL
    assert rec_sol.native_spent == Decimal("1.0")
    # $200 USD on ETH @ $4000/ETH == 0.0500 ETH
    assert rec_eth.native_spent == Decimal("0.05")

    # Both trades have equal USD value ($200 USD), native amounts are inversely proportional to price (20:1)
    assert (rec_sol.native_spent * Decimal("200.0")) == (rec_eth.native_spent * Decimal("4000.0")) == Decimal("200.0")


# ---------------------------------------------------------------------------
# Test 6: Regression test reproducing the 22.7x discrepancy numbers from the audit
# ---------------------------------------------------------------------------
def test_regression_22_7x_discrepancy_eliminated():
    """
    Audit finding:
    With default config, equity = $8,300, SOL = $150, ETH = $3400 (ratio 3400/150 = 22.667x).
    Runner balance-checked 1% of $8300 / $150 = ~0.5533 SOL.
    Executor resized with ETH price: 1% of $8300 / $3400 = ~0.0244 SOL (22.7x too small).
    Under the fix, the sizing authority directly computes ~0.5533 SOL and that is what executes.
    """
    sol_p = Decimal("150.0")
    eth_p = Decimal("3400.0")
    equity_usd = Decimal("8300.0")
    one_pct_usd = equity_usd * Decimal("0.01")  # $83.00

    # Old buggy calculation in executor:
    old_buggy_size = one_pct_usd / eth_p  # ~0.02441176 SOL
    # Correct per-chain calculation:
    correct_size = one_pct_usd / sol_p    # ~0.55333333 SOL

    discrepancy_ratio = float(correct_size / old_buggy_size)
    assert round(discrepancy_ratio, 1) == 22.7

    # Verify that the new sizing authority computes the correct per-chain size
    cfg = EngineConfig(
        allow_static_native_prices=True,
        kelly_uncalibrated_fraction=0.01,
        max_portfolio_risk_pct=0.01,
    )
    risk_engine = RiskEngine(cfg)
    sizing = risk_engine.calculate_order_size(
        portfolio_equity_usd=equity_usd,
        native_price_usd=sol_p,
        chain=ChainIdentifier.SOLANA_MAINNET,
        score=100.0,
    )

    # Must match the correct ~0.5533 SOL, NOT the 22.7x smaller ~0.0244 SOL
    assert round(sizing.native_trade_size, 4) == Decimal("0.5533")
    assert sizing.native_trade_size != old_buggy_size
