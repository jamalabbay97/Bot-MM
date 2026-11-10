"""
tests/test_strict_realtime_pricing.py
======================================
Comprehensive unit and integration test suite validating:
1. get_or_fetch_pool_state under strict_realtime=True and max_data_age_s=4.0
2. Parallel fetching from DexScreener + GeckoTerminal + On-Chain Pump.fun curve
3. Freshest source selection (lowest data_age_s)
4. Pump.fun on-chain bonding curve prioritization for ungraduated tokens
5. Rejection / trade abort on stale price (> 4.0s) for manual entries & automated signals
6. Suspension of dynamic exits (trailing stop, TP, SL) when fresh price unavailable
7. Telegram & logging messages reporting Price Source, Data Age, and Market Cap
"""

import time
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import AlphaEngine
from alpha_engine.execution.book import OpenLot
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.enums import ChainIdentifier, OrderSide, SecurityTier, SignalStrength, SignalSource
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, SecurityReport


@pytest.fixture
async def engine(tmp_path):
    db_file = tmp_path / "test_strict_pricing.db"
    cfg = EngineConfig(
        db_path=str(db_file),
        initial_sol=Decimal("100.0"),
        initial_eth=Decimal("10.0"),
        sol_price_usd=Decimal("100.0"),
        eth_price_usd=Decimal("2500.0"),
        max_active_positions=3,
        auto_trade_enabled=True,
    )
    eng = AlphaEngine(cfg)
    eng._ledger = SQLiteLedger(cfg.db_path)
    await eng._ledger.__aenter__()
    eng._executor = PaperExecutor(
        position_book=eng._position_book,
        price_service=eng._native_price_service,
    )
    yield eng
    await eng._ledger.__aexit__(None, None, None)


@pytest.mark.anyio
async def test_strict_realtime_rejection_when_data_age_exceeds_threshold(engine):
    """
    When strict_realtime=True, if sources return data older than 4.0s,
    get_or_fetch_pool_state MUST return None (no fallback to initial curve).
    """
    token = "StaleToken111111111111111111111111111111111"
    
    stale_pool = PoolState(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolStale11111111111111111111111111111111",
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("50.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=100,
        token_address=token,
        timestamp_s=time.time() - 10.0,
        price_source="DexScreener:Stale",
        data_age_s=8.5,  # Exceeds 4.0s
    )

    with patch.object(engine, "_fetch_dexscreener_state", new=AsyncMock(return_value=stale_pool)), \
         patch.object(engine, "_fetch_geckoterminal_state", new=AsyncMock(return_value=None)), \
         patch.object(engine, "_fetch_pump_curve_onchain", new=AsyncMock(return_value=None)):

        # Strict real-time MUST reject
        res_strict = await engine.get_or_fetch_pool_state(
            ChainIdentifier.SOLANA_MAINNET,
            token,
            strict_realtime=True,
            max_data_age_s=4.0,
        )
        assert res_strict is None, "Should reject stale price under strict_realtime=True"

        # Non-strict call may fallback
        res_non_strict = await engine.get_or_fetch_pool_state(
            ChainIdentifier.SOLANA_MAINNET,
            token,
            strict_realtime=False,
            max_data_age_s=4.0,
        )
        assert res_non_strict is not None, "Non-strict may return fallback pool"


@pytest.mark.anyio
async def test_strict_realtime_prefers_freshest_source(engine):
    """
    When DexScreener and GeckoTerminal return valid prices, engine prefers
    the source with the smallest data_age_s.
    """
    token = "MultiSourceToken11111111111111111111111111"

    dex_pool = PoolState(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolDex11111111111111111111111111111111",
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("50.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=100,
        token_address=token,
        timestamp_s=time.time() - 2.5,
        price_source="DexScreener",
        data_age_s=2.5,
    )

    gecko_pool = PoolState(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolGecko11111111111111111111111111111111",
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("52.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=100,
        token_address=token,
        timestamp_s=time.time() - 0.8,
        price_source="GeckoTerminal",
        data_age_s=0.8,  # Fresher!
    )

    with patch.object(engine, "_fetch_dexscreener_state", new=AsyncMock(return_value=dex_pool)), \
         patch.object(engine, "_fetch_geckoterminal_state", new=AsyncMock(return_value=gecko_pool)), \
         patch.object(engine, "_fetch_pump_curve_onchain", new=AsyncMock(return_value=None)):

        selected = await engine.get_or_fetch_pool_state(
            ChainIdentifier.SOLANA_MAINNET,
            token,
            strict_realtime=True,
            max_data_age_s=4.0,
        )
        assert selected is not None
        assert selected.price_source == "GeckoTerminal"
        assert selected.data_age_s == 0.8


@pytest.mark.anyio
async def test_pump_fun_onchain_bonding_curve_prioritization(engine):
    """
    When token is on Pump.fun and bonding curve is not complete,
    on-chain Pump.fun curve is prioritized over secondary aggregators.
    """
    token = "ActivePumpToken111111111111111111111111111pump"

    onchain_curve = PoolState(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PumpCurvePDA1111111111111111111111111111",
        token_reserve=Decimal("793100000.0"),
        native_reserve=Decimal("45.0"),
        fee_numerator=100,
        fee_denominator=10000,
        last_updated_block=1000,
        token_address=token,
        timestamp_s=time.time(),
        price_source="PumpFunOnChainCurve",
        data_age_s=0.15,
    )
    setattr(onchain_curve, "_is_pump_curve", True)
    setattr(onchain_curve, "_bonding_curve_complete", False)

    dex_pool = PoolState(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolDexLag1111111111111111111111111111111",
        token_reserve=Decimal("793100000.0"),
        native_reserve=Decimal("42.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=990,
        token_address=token,
        timestamp_s=time.time() - 1.2,
        price_source="DexScreener",
        data_age_s=1.2,
    )

    with patch.object(engine, "_fetch_pump_curve_onchain", new=AsyncMock(return_value=onchain_curve)), \
         patch.object(engine, "_fetch_dexscreener_state", new=AsyncMock(return_value=dex_pool)), \
         patch.object(engine, "_fetch_geckoterminal_state", new=AsyncMock(return_value=None)):

        selected = await engine.get_or_fetch_pool_state(
            ChainIdentifier.SOLANA_MAINNET,
            token,
            strict_realtime=True,
            max_data_age_s=4.0,
        )
        assert selected is not None
        assert selected.price_source == "PumpFunOnChainCurve"
        assert selected.data_age_s == 0.15


@pytest.mark.anyio
async def test_manual_enter_position_aborts_on_stale_price(engine):
    """
    manual_enter_position MUST fail and release reserved slot if real-time price
    is stale or unavailable.
    """
    token = "StaleManualToken1111111111111111111111111111"

    with patch.object(engine, "get_or_fetch_pool_state", new=AsyncMock(return_value=None)):
        ok, msg, sig = await engine.manual_enter_position(token, chain=ChainIdentifier.SOLANA_MAINNET)
        assert ok is False
        assert "تعذر تنفيذ الشراء الفوري" in msg
        assert "عمر البيانات > 4 ثوانٍ" in msg
        assert sig is None
        # Capacity slot must be released
        assert engine._position_book.effective_active_position_count() == 0


@pytest.mark.anyio
async def test_manual_enter_position_records_price_source_and_age(engine):
    """
    manual_enter_position records price source, data age, and market cap upon success.
    """
    token = "FreshManualToken1111111111111111111111111111"

    fresh_pool = PoolState(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolFresh1111111111111111111111111111111",
        token_reserve=Decimal("1000000000.0"),
        native_reserve=Decimal("50.0"),
        fee_numerator=100,
        fee_denominator=10000,
        last_updated_block=1234,
        token_address=token,
        timestamp_s=time.time(),
        price_source="DexScreener:Live",
        data_age_s=0.45,
    )

    with patch.object(engine, "get_or_fetch_pool_state", new=AsyncMock(return_value=fresh_pool)):
        ok, msg, sig = await engine.manual_enter_position(token, chain=ChainIdentifier.SOLANA_MAINNET)
        assert ok is True
        assert "تم إرسال أمر الشراء وتنفيذه بنجاح" in msg
        assert "Source: DexScreener:Live" in msg
        assert "Age: 0.45s" in msg
        assert sig is not None


@pytest.mark.anyio
async def test_position_price_poller_suspends_dynamic_exits_on_stale_data(engine):
    """
    When an open lot's price cannot be refreshed within 4.0s, dynamic exits
    (trailing stop, TP, SL) are suspended and only emergency exits are permitted.
    """
    token = "OpenPositionToken111111111111111111111111111"

    # Open a lot manually
    lot = OpenLot(
        lot_id="lot_test_stale_guard",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token,
        pool_address="PoolOpen11111111111111111111111111111111",
        cost_basis_native=Decimal("0.5"),
        tokens_held=Decimal("50000"),
        entry_price=Decimal("0.00001"),
        open_timestamp_ns=time.time_ns() - int(20 * 1e9),  # 20s ago
    )
    engine._position_book._lots[(ChainIdentifier.SOLANA_MAINNET, token)].append(lot)
    assert engine.is_position_open(token) is True

    # Mock get_or_fetch_pool_state to return None (stale/unavailable)
    with patch.object(engine, "get_or_fetch_pool_state", new=AsyncMock(return_value=None)), \
         patch.object(engine._position_book, "evaluate_lot_exit") as mock_eval:

        # Run poller pass
        await engine._poll_open_lots_prices([lot])

        # Verify strict real-time check returns None
        pool = await engine.get_or_fetch_pool_state(lot.chain, lot.token_address, strict_realtime=True, max_data_age_s=4.0)
        assert pool is None

        # Dynamic exit evaluation is suspended (not called) when real-time price is unavailable
        assert not mock_eval.called, "Dynamic exit evaluation should be suspended on stale price"


@pytest.mark.anyio
async def test_signal_queue_updates_frozen_signal_event_safely(engine):
    """
    Ensure SignalEvent (which has frozen=True) can be refreshed with a real-time
    PoolState in the signal consumer loop without triggering a frozen_instance ValidationError.
    """
    token = "FrozenSignalToken111111111111111111111111"
    pool_addr = "PoolFrozen1111111111111111111111111111111"

    stale_pool = PoolState(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        token_reserve=Decimal("1000000000.0"),
        native_reserve=Decimal("30.0"),
        fee_numerator=100,
        fee_denominator=10000,
        last_updated_block=100,
        token_address=token,
        timestamp_s=time.time() - 10.0,  # 10s old (stale)
        price_source="DexScreener",
        data_age_s=10.0,
    )

    fresh_pool = PoolState(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        token_reserve=Decimal("1000000000.0"),
        native_reserve=Decimal("45.0"),
        fee_numerator=100,
        fee_denominator=10000,
        last_updated_block=150,
        token_address=token,
        timestamp_s=time.time(),
        price_source="PumpFunOnChainCurve",
        data_age_s=0.15,
    )

    clean_sec = SecurityReport(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        is_rug=False,
        score=95.0,
        flags=[],
        buy_tax_bps=0,
        sell_tax_bps=0,
        checked_at_ts=int(time.time()),
        passes_hard_gates=True,
    )

    signal = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        token_address=token,
        suggested_side=OrderSide.BUY,
        pool_state=stale_pool,
        security_report=clean_sec,
        strength=SignalStrength.STRONG,
        alpha_score=0.85,
        source=SignalSource.DEX_SWAP,
    )

    with patch.object(engine, "get_or_fetch_pool_state", new=AsyncMock(return_value=fresh_pool)), \
         patch.object(engine._executor, "execute_signal", new=AsyncMock(return_value=None)):

        # Put signal in queue and process single item
        await engine._signal_q.put(signal)
        # Process single signal using consumer helper logic
        item = await engine._signal_q.get()

        # Simulate the exact refresh pattern in runner.py
        pool_age = (time.time() - getattr(item.pool_state, "timestamp_s", 0.0)) if item.pool_state else 999.0
        data_age = float(getattr(item.pool_state, "data_age_s", None) or pool_age)
        assert data_age > 4.0

        refreshed = await engine.get_or_fetch_pool_state(
            chain=item.chain,
            token_address=item.token_address,
            pool_address=getattr(item, "pool_address", "") or "",
            force_refresh=True,
            strict_realtime=True,
            max_data_age_s=4.0,
        )
        assert refreshed is not None
        # model_copy MUST succeed without ValidationError
        updated_item = item.model_copy(update={"pool_state": refreshed})
        assert updated_item.pool_state.price_source == "PumpFunOnChainCurve"
        assert updated_item.pool_state.data_age_s == 0.15

