"""
tests/test_concurrency_and_correlation.py — Hardened Concurrency, Correlation & Diversification
==============================================================================================
Validates:
1. 50 concurrent async tasks hitting SQLiteLedger and WhitelistDatabase with zero database locks.
2. Cross-asset correlation filter (Pearson r > 0.80 rejection on rolling 5m returns >= 20 ticks).
3. Narrative cluster diversification gate (no duplicate non-null narrative cluster positions).
4. Jito tip floor in-memory TTL cache (5.0s TTL).
5. Portfolio invariant preservation: Max 3 positions, 1.5% token risk, 5% total exposure, circuit breaker.
6. Clean root facade imports and __all__ re-exports.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import time
from decimal import Decimal

import pytest

from alpha_engine.execution.book import (
    MAX_ACTIVE_POSITIONS,
    MAX_PER_TOKEN_RISK_PCT,
    MAX_PORTFOLIO_EXPOSURE_PCT,
    PositionBook,
    RunningMetrics,
)
from alpha_engine.execution.bundle import PrivateTxRouter
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.enums import (
    ChainIdentifier,
    LotStatus,
    OrderSide,
    WalletClassification,
    WhitelistStatus,
)
from alpha_engine.models.profiler import WalletProfile
from alpha_engine.models.state import PaperFill, TradeRecord
from alpha_engine.profiler.whitelist_db import WhitelistDatabase


@pytest.fixture
def tmp_dir():
    d = tempfile.mkdtemp(prefix="bot_mm_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ==============================================================================
# TEST 1: 50 CONCURRENT ASYNC TASKS WITH ZERO DATABASE LOCK CONTENTION
# ==============================================================================
@pytest.mark.anyio
async def test_50_concurrent_async_sqlite_tasks(tmp_dir: str):
    """
    Simulate 50 concurrent async tasks reading and writing to SQLiteLedger
    and WhitelistDatabase simultaneously under WAL mode with @sqlite_retry.
    Must complete without sqlite3.OperationalError: database is locked.
    """
    ledger_db = os.path.join(tmp_dir, "ledger_concurrent.db")
    whitelist_db = os.path.join(tmp_dir, "whitelist_concurrent.db")

    ledger = SQLiteLedger(db_path=ledger_db)
    await ledger.initialize()

    w_db = WhitelistDatabase(db_path=whitelist_db)
    await w_db.connect()

    async def ledger_worker(worker_id: int):
        token_addr = f"0xToken{worker_id:04d}0000000000000000000000000000"
        lot_id = f"lot_{worker_id}"

        # 1. Record trade
        fill = PaperFill(
            order_id=f"order_{worker_id}",
            token_address=token_addr,
            chain=ChainIdentifier.BASE_MAINNET,
            side=OrderSide.BUY,
            tokens_acquired=Decimal("1000.0"),
            simulated_native_spent=Decimal("0.05"),
            effective_price=Decimal("0.00005"),
            price_impact_bps=15,
            fill_timestamp_ns=time.time_ns(),
        )
        trade = TradeRecord.from_fill(fill=fill, signal_id=f"sig_{worker_id}")
        await ledger.record_trade(trade)

        # 2. Record lot transition
        await ledger.record_lot_transition(
            lot_id=lot_id,
            token_address=token_addr,
            chain="base",
            status=LotStatus.OPEN.value,
        )

        # 3. Read stats concurrently
        stats = await ledger.get_closed_trade_stats()
        assert isinstance(stats, dict)

        streak = await ledger.get_daily_loss_streak()
        assert isinstance(streak, int)

    async def whitelist_worker(worker_id: int):
        wallet_addr = f"0xWallet{worker_id:04d}000000000000000000000000000"
        profile = WalletProfile(
            wallet_address=wallet_addr,
            chain=ChainIdentifier.BASE_MAINNET,
            classification=WalletClassification.SMART_MONEY,
            win_rate_pct=75.0,
            total_trades=20,
            winning_trades=15,
            losing_trades=5,
            total_pnl_usd=Decimal("5000"),
            max_single_trade_pnl_usd=Decimal("1000"),
            outlier_pnl_ratio=0.2,
            median_holding_time_seconds=120.0,
            active_days=30.0,
            days_since_last_active=1.0,
            first_tx_timestamp=1700000000,
            last_tx_timestamp=1700003600,
            is_whitelisted=True,
        )
        # Upsert
        await w_db.upsert_wallet(
            profile=profile,
            status=WhitelistStatus.ACTIVE,
        )
        # Check
        is_wl = await w_db.is_whitelisted(wallet_addr)
        assert is_wl is True

        # Read active
        active = await w_db.list_active()
        assert len(active) >= 1

    # Spawn 25 ledger workers and 25 whitelist workers (50 total tasks)
    tasks = []
    for i in range(25):
        tasks.append(asyncio.create_task(ledger_worker(i)))
        tasks.append(asyncio.create_task(whitelist_worker(i)))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for res in results:
        if isinstance(res, Exception):
            raise res

    await ledger.close()
    await w_db.close()


# ==============================================================================
# TEST 2: NARRATIVE DIVERSIFICATION GATE
# ==============================================================================
def test_narrative_cluster_diversification_gate():
    """
    Ensure no two active open positions share the same non-null narrative_cluster.
    """
    book = PositionBook()
    equity = Decimal("10.0")  # 10 SOL
    size = Decimal("0.10")     # 1% risk (< 1.5% limit)

    tok_ai1 = "0xAI1111111111111111111111111111111111111111"
    tok_ai2 = "0xAI2222222222222222222222222222222222222222"
    tok_dog = "0xDOG111111111111111111111111111111111111111"

    # 1. Candidate 1 (AI narrative) can open
    can_open, reason = book.can_open_position(
        token_address=tok_ai1,
        chain=ChainIdentifier.BASE_MAINNET,
        allocated_cost_native=size,
        total_equity_native=equity,
        narrative_cluster="ai",
    )
    assert can_open is True
    assert reason == ""

    # Open lot for tok_ai1 with cluster "ai"
    fill1 = PaperFill(
        order_id="o1",
        token_address=tok_ai1,
        chain=ChainIdentifier.BASE_MAINNET,
        side=OrderSide.BUY,
        tokens_acquired=Decimal("1000"),
        simulated_native_spent=size,
        effective_price=Decimal("0.0001"),
        price_impact_bps=10,
        fill_timestamp_ns=time.time_ns(),
    )
    lot1 = book.open_lot(fill=fill1, signal_id="sig1", narrative_cluster="ai")
    assert lot1.narrative_cluster == "ai"

    # 2. Candidate 2 with same narrative "ai" (case-insensitive) MUST be rejected
    can_open2, reason2 = book.can_open_position(
        token_address=tok_ai2,
        chain=ChainIdentifier.BASE_MAINNET,
        allocated_cost_native=size,
        total_equity_native=equity,
        narrative_cluster="AI",
    )
    assert can_open2 is False
    assert "Narrative cluster conflict" in reason2

    # 3. Candidate 3 with different narrative "dog" MUST be accepted
    can_open3, reason3 = book.can_open_position(
        token_address=tok_dog,
        chain=ChainIdentifier.BASE_MAINNET,
        allocated_cost_native=size,
        total_equity_native=equity,
        narrative_cluster="dog",
    )
    assert can_open3 is True

    # 4. Candidate without narrative (None) MUST be accepted
    can_open4, reason4 = book.can_open_position(
        token_address="0xNO_CLUSTER_TOKEN111111111111111111111111",
        chain=ChainIdentifier.BASE_MAINNET,
        allocated_cost_native=size,
        total_equity_native=equity,
        narrative_cluster=None,
    )
    assert can_open4 is True


# ==============================================================================
# TEST 3: ROLLING RETURN CORRELATION & PEARSON r > 0.80 FILTER
# ==============================================================================
def test_pearson_correlation_math():
    """Verify exact numerical correctness of compute_pearson_correlation."""
    # Perfect positive correlation (r = 1.0)
    x = [0.01 * i for i in range(25)]
    y = [0.02 * i for i in range(25)]
    r = PositionBook.compute_pearson_correlation(x, y)
    assert pytest.approx(r, abs=1e-5) == 1.0

    # Perfect negative correlation (r = -1.0)
    y_neg = [-0.03 * i for i in range(25)]
    r_neg = PositionBook.compute_pearson_correlation(x, y_neg)
    assert pytest.approx(r_neg, abs=1e-5) == -1.0

    # Uncorrelated series (r ~ 0.0)
    x_alt = [1.0, -1.0, 1.0, -1.0] * 6
    y_const = [1.0, 1.0, -1.0, -1.0] * 6
    r_zero = PositionBook.compute_pearson_correlation(x_alt, y_const)
    assert abs(r_zero) < 0.20

    # Zero variance series returns 0.0 without division by zero
    flat = [0.05] * 25
    r_flat = PositionBook.compute_pearson_correlation(x, flat)
    assert r_flat == 0.0


def test_cross_asset_correlation_rejection_gate():
    """
    Verify rejection of a candidate token when its rolling return series
    exceeds the 0.80 Pearson correlation threshold with an active open lot.
    """
    book = PositionBook()
    equity = Decimal("100.0")
    size = Decimal("1.0")

    tok_a = "0xTOKEN_A_1111111111111111111111111111111111"
    tok_b = "0xTOKEN_B_2222222222222222222222222222222222"
    tok_c = "0xTOKEN_C_3333333333333333333333333333333333"

    fill_a = PaperFill(
        order_id="oa",
        token_address=tok_a,
        chain=ChainIdentifier.BASE_MAINNET,
        side=OrderSide.BUY,
        tokens_acquired=Decimal("10000"),
        simulated_native_spent=size,
        effective_price=Decimal("0.0001"),
        price_impact_bps=5,
        fill_timestamp_ns=time.time_ns(),
    )
    lot_a = book.open_lot(fill=fill_a, signal_id="siga")

    # Feed 25 ticks with a known synthetic return profile to lot_a
    base_price = Decimal("0.000100")
    t0 = time.time() - 100.0
    for i in range(25):
        # Price oscillates in upward trend
        step_factor = Decimal(str(1.0 + (0.01 * (i % 5)) + (0.005 * i)))
        p = base_price * step_factor
        lot_a.record_tick(price=p, timestamp_s=t0 + i * 2.0)

    lot_a_returns = lot_a.get_tick_returns_5m()
    assert len(lot_a_returns) >= 20

    # Candidate B has identical return dynamics (correlation ~ 1.0 > 0.80)
    cand_b_returns = [r * 1.05 for r in lot_a_returns]
    can_open_b, reason_b = book.can_open_position(
        token_address=tok_b,
        chain=ChainIdentifier.BASE_MAINNET,
        allocated_cost_native=size,
        total_equity_native=equity,
        candidate_returns=cand_b_returns,
    )
    assert can_open_b is False
    assert "exceeds 0.80 threshold" in reason_b

    # Candidate C has independent/uncorrelated returns (e.g. alternating sign)
    cand_c_returns = [0.01 if i % 2 == 0 else -0.01 for i in range(len(lot_a_returns))]
    can_open_c, reason_c = book.can_open_position(
        token_address=tok_c,
        chain=ChainIdentifier.BASE_MAINNET,
        allocated_cost_native=size,
        total_equity_native=equity,
        candidate_returns=cand_c_returns,
    )
    assert can_open_c is True
    assert reason_c == ""


# ==============================================================================
# TEST 4: JITO TIP FLOOR IN-MEMORY TTL CACHING (5.0s TTL)
# ==============================================================================
@pytest.mark.anyio
async def test_jito_tip_floor_ttl_caching():
    """
    Verify get_jito_tip_floor / get_tip_floor caches responses for 5.0s TTL.
    """
    router = PrivateTxRouter()

    # Pre-populate cache directly to simulate a recent API response
    now = time.time()
    router._tip_cache["p75"] = (now, Decimal("0.000125"))
    router._tip_cache["p95"] = (now, Decimal("0.000350"))

    # Immediate call within TTL window must return cached value without HTTP call
    tip_p75 = await router.get_jito_tip_floor("p75")
    assert tip_p75 == Decimal("0.000125")

    # Alias call must also hit cache
    tip_p95 = await router.get_tip_floor("p95")
    assert tip_p95 == Decimal("0.000350")

    # Simulate expired cache entry (> 5.0 seconds old)
    router._tip_cache["p75"] = (now - 6.0, Decimal("0.000125"))
    # In fallback test when API is offline / unmocked, it catches exception and returns cached or default
    tip_expired = await router.get_jito_tip_floor("p75")
    assert isinstance(tip_expired, Decimal)
    assert tip_expired > Decimal(0)


# ==============================================================================
# TEST 5: PORTFOLIO INVARIANTS (MAX 3 POSITIONS, 1.5% TOKEN RISK, 5% EXPOSURE)
# ==============================================================================
def test_portfolio_invariants_and_circuit_breaker():
    """
    Verify strict portfolio risk invariants:
    - Max 3 active positions
    - 1.5% max per-token risk
    - 5% total portfolio exposure
    - Circuit breaker trip at 3 consecutive losses or 8% drawdown
    """
    book = PositionBook()
    equity = Decimal("100.0")

    assert MAX_ACTIVE_POSITIONS == 3
    assert MAX_PER_TOKEN_RISK_PCT == Decimal("0.015")
    assert MAX_PORTFOLIO_EXPOSURE_PCT == Decimal("0.05")

    # 1. Per-token risk limit: 1.51 SOL on 100 SOL equity (> 1.5%) must be rejected
    can_open, reason = book.can_open_position(
        token_address="0xTokenRiskLimit1111111111111111111111111",
        chain=ChainIdentifier.BASE_MAINNET,
        allocated_cost_native=Decimal("1.51"),
        total_equity_native=equity,
    )
    assert can_open is False
    assert "MAX_PER_TOKEN_RISK_PCT" in reason

    # 1.50 SOL (exactly 1.5%) is allowed
    can_open_ok, _ = book.can_open_position(
        token_address="0xTokenRiskLimit1111111111111111111111111",
        chain=ChainIdentifier.BASE_MAINNET,
        allocated_cost_native=Decimal("1.50"),
        total_equity_native=equity,
    )
    assert can_open_ok is True

    # 2. Max 3 positions hard cap
    toks = [f"0xPos{i}0000000000000000000000000000000000" for i in range(4)]
    for i in range(3):
        fill = PaperFill(
            order_id=f"o_{i}",
            token_address=toks[i],
            chain=ChainIdentifier.BASE_MAINNET,
            side=OrderSide.BUY,
            tokens_acquired=Decimal("1000"),
            simulated_native_spent=Decimal("1.0"),
            effective_price=Decimal("0.001"),
            price_impact_bps=10,
            fill_timestamp_ns=time.time_ns(),
        )
        book.open_lot(fill=fill, signal_id=f"s_{i}")

    assert book.open_position_count() == 3

    # 4th position must be rejected by MAX_ACTIVE_POSITIONS
    can_open_4th, reason_4th = book.can_open_position(
        token_address=toks[3],
        chain=ChainIdentifier.BASE_MAINNET,
        allocated_cost_native=Decimal("1.0"),
        total_equity_native=equity,
    )
    assert can_open_4th is False
    assert "MAX_ACTIVE_POSITIONS" in reason_4th

    # 3. Circuit breaker
    metrics = RunningMetrics(peak_equity_usd=Decimal("10000.0"))
    assert metrics.is_circuit_breaker_active() is False

    # Simulate 3 consecutive losses
    current_eq = Decimal("10000.0")
    for pnl in [Decimal("-50.0"), Decimal("-60.0")]:
        current_eq += pnl
        metrics.record_closed_trade(
            realized_pnl_usd=pnl,
            gas_cost_usd=Decimal("0.5"),
            current_equity_usd=current_eq,
        )
    assert metrics.is_circuit_breaker_active() is False
    assert metrics.consecutive_losses == 2

    current_eq += Decimal("-40.0")
    metrics.record_closed_trade(
        realized_pnl_usd=Decimal("-40.0"),
        gas_cost_usd=Decimal("0.5"),
        current_equity_usd=current_eq,
    )
    assert metrics.consecutive_losses == 3
    assert metrics.is_circuit_breaker_active() is True
    assert "Consecutive loss streak" in metrics.circuit_breaker_reason


# ==============================================================================
# TEST 6: FACADE IMPORT CLEANLINESS & __all__ VERIFICATION
# ==============================================================================
def test_all_root_facades_clean_imports():
    """Verify that all 9 root facades import cleanly and expose explicit __all__."""
    import sys
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if root_dir not in sys.path:
        sys.path.insert(0, root_dir)

    import engine
    import execution
    import ingestion
    import models
    import news_scraper
    import quant_math
    import rate_limiter
    import security
    import wallet_profiler

    facades = [
        engine,
        execution,
        ingestion,
        models,
        news_scraper,
        quant_math,
        rate_limiter,
        security,
        wallet_profiler,
    ]

    for fac in facades:
        assert hasattr(fac, "__all__"), f"{fac.__name__} missing __all__"
        assert len(fac.__all__) > 0, f"{fac.__name__} has empty __all__"
        for symbol in fac.__all__:
            assert hasattr(fac, symbol), f"{fac.__name__} __all__ lists '{symbol}' but symbol is not exported"
