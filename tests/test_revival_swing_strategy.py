"""
tests.test_revival_swing_strategy — Comprehensive Test Suite for Revival & CTO Breakout Swing Strategy.
====================================================================================================
Tests:
1. Token age filters (2h to 10 days).
2. Dormancy & tight consolidation metrics (1h volume, Bollinger squeeze).
3. Breakout volume inflection (V_5m >= 3.0x SMA_1h) and Net Buy Delta >= 0.65.
4. Anti-FOMO guards (current_price > 2.2 * base_price and RSI > 82).
5. CTO Dev balance (<= 0.1%) and Top-10 concentration (< 25%).
6. Dynamic Exits:
   - High Water Mark tracking.
   - 35% Trailing ATH Drawdown exit (CLOSE_ALL).
   - Laddered TP (50% at +100%, 50% at +200%).
   - Bypass of 120s velocity decay.
   - Slippage insolvency guard preserved.
7. Trade Frequency & Cooldown Governor (10 trades -> 4h cooldown -> Paced mode -> reset).
8. AI Supervisor deterministic green candle FOMO rejection (mcap / dormant_mcap > 2.5).
9. Closed-loop feature vector storage in PatternMemoryStore.
"""

from __future__ import annotations

import time
from decimal import Decimal
import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
from alpha_engine.engine.feedback import DynamicParameterTuner, PatternMemoryStore
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.engine.staging import RevivalBreakoutBuffer
from alpha_engine.execution.book import PositionBook
from alpha_engine.models.ai import AISupervisorDecisionEnum
from alpha_engine.models.decisions import RevivalPatternFeatureVector
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitProfile,
    LotStatus,
    OrderSide,
    SecurityTier,
    SignalSource,
    TradeExitReason,
)
from alpha_engine.models.events import SwapEvent
from alpha_engine.models.state import PaperFill, PoolState, SecurityReport


@pytest.fixture
def clean_security_report() -> SecurityReport:
    return SecurityReport(
        token_address="RevivalToken1111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.18,  # < 25% passes
        mint_authority_disabled=True,
        freeze_authority_disabled=True,
        verified_source_code=True,
    )


@pytest.fixture
def sample_pool() -> PoolState:
    return PoolState(
        pool_address="PoolRevival111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000000"),
        native_reserve=Decimal("100.0"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=1000,
    )


def test_token_age_filters(clean_security_report: SecurityReport):
    """Verify tokens must be between 2 hours and 10 days old."""
    buffer = RevivalBreakoutBuffer(config=EngineConfig())
    token = "AgeTestToken111111111111111111111111111111111"
    now = time.time()

    # Case 1: Too young (1 hour old = 3600s < 7200s)
    staged_young = buffer.stage_token(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_price=Decimal("0.0001"),
        token_creation_timestamp=now - 3600,
        report=clean_security_report,
    )
    assert staged_young is None or staged_young.dropped is True

    # Case 2: Too old (11 days old = 11 * 86400s > 10 days)
    staged_old = buffer.stage_token(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_price=Decimal("0.0001"),
        token_creation_timestamp=now - (11 * 86400),
        report=clean_security_report,
    )
    assert staged_old is None or staged_old.dropped is True

    # Case 3: Valid age (24 hours old = 86400s)
    staged_valid = buffer.stage_token(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_price=Decimal("0.0001"),
        token_creation_timestamp=now - 86400,
        report=clean_security_report,
    )
    assert staged_valid is not None
    assert staged_valid.dropped is False
    assert 2.0 <= staged_valid.token_age_hours <= 240.0


def test_dormancy_and_consolidation_metrics(clean_security_report: SecurityReport, sample_pool: PoolState):
    """Verify dormant volume verification and Bollinger Band squeeze calculation."""
    buffer = RevivalBreakoutBuffer(config=EngineConfig())
    token = "DormancyToken11111111111111111111111111111111"
    now = time.time()

    staged = buffer.stage_token(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_price=Decimal("0.0001"),
        token_creation_timestamp=now - (12 * 3600),  # 12 hours old
        report=clean_security_report,
        dormant_volume_1h_threshold=Decimal("5.0"),
    )
    assert staged is not None

    # Simulate quiet dormant trading (low volume < 5 SOL/h and tight prices)
    base_p = Decimal("0.0001")
    for i in range(25):
        staged.record_5m_price(base_p * Decimal(str(1.0 + (i % 3) * 0.005)))

    # Bollinger bandwidth should be tightly compressed
    bandwidth = staged.compute_bollinger_bandwidth()
    assert bandwidth < 0.15  # Tight squeeze

    # Add low volume trades
    for i in range(5):
        swap = SwapEvent(
            timestamp_ns=int((now + i * 60) * 1e9),
            block_number=1000 + i,
            tx_hash=f"tx_{i}".ljust(32, "0"),
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=sample_pool.pool_address,
            token_in="So11111111111111111111111111111111111111112",
            token_out=token,
            amount_in=Decimal("0.2"),  # 0.2 SOL
            amount_out=Decimal("2000"),
            sender=f"buyer_{i}".ljust(32, "1"),
        )
        buffer.record_trade(swap, pool=sample_pool)

    # 1-hour volume is 1.0 SOL < 5.0 SOL threshold -> dormant verified
    assert staged.one_hour_volume < Decimal("5.0")
    assert staged.dormant_detected is True


def test_breakout_volume_and_net_buy_delta(clean_security_report: SecurityReport, sample_pool: PoolState):
    """Verify breakout requires V_5m >= 3.0x SMA_1h and Net Buy Delta >= 0.65."""
    buffer = RevivalBreakoutBuffer(config=EngineConfig())
    token = "BreakoutToken1111111111111111111111111111111"
    now = time.time()

    staged = buffer.stage_token(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_price=Decimal("0.0001"),
        token_creation_timestamp=now - (24 * 3600),
        report=clean_security_report,
        dormant_volume_1h_threshold=Decimal("5.0"),
    )
    assert staged is not None
    staged.dormant_detected = True
    staged.consolidation_base_price = Decimal("0.0001")
    # Feed candles to populate historical SMA
    for _ in range(20):
        staged.record_5m_price(Decimal("0.0001"))

    # Case A: Heavy sell volume in breakout window -> Net Buy Delta < 0.65 -> No signal
    staged.five_min_volume = Decimal("15.0")
    staged.one_hour_volume = Decimal("3.0")
    staged.five_min_buy_volume = Decimal("6.0")
    staged.five_min_sell_volume = Decimal("9.0")  # Net buy = (6-9)/15 = -0.2 < 0.65
    sig = buffer.check_breakout(token)
    assert sig is None

    # Case B: Heavy organic buy volume -> Net Buy Delta >= 0.65 and Volume Surge >= 3.0x
    staged.five_min_buy_volume = Decimal("14.0")
    staged.five_min_sell_volume = Decimal("1.0")  # Net buy = (14-1)/15 = 0.866 >= 0.65
    staged.latest_price = Decimal("0.00012")       # +20% from base (not overextended)
    sig = buffer.check_breakout(token)
    assert sig is not None
    assert sig.exit_profile == ExitProfile.REVIVAL_SWING
    assert sig.signal_source == SignalSource.REVIVAL_BREAKOUT
    assert sig.suggested_side == OrderSide.BUY


def test_anti_fomo_overextended_guard(clean_security_report: SecurityReport):
    """Reject entry if price is > 2.2x base consolidation price or 5m RSI > 82."""
    buffer = RevivalBreakoutBuffer(config=EngineConfig())
    token = "FomoToken11111111111111111111111111111111111"
    now = time.time()

    staged = buffer.stage_token(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_price=Decimal("0.0001"),
        token_creation_timestamp=now - (48 * 3600),
        report=clean_security_report,
    )
    assert staged is not None
    staged.dormant_detected = True
    staged.consolidation_base_price = Decimal("0.0001")
    staged.five_min_volume = Decimal("20.0")
    staged.one_hour_volume = Decimal("4.0")
    staged.five_min_buy_volume = Decimal("18.0")
    staged.five_min_sell_volume = Decimal("2.0")

    # Price is 2.5x base price (> 2.2x threshold) -> OVEREXTENDED_CHASE
    staged.latest_price = Decimal("0.00025")
    sig = buffer.check_breakout(token)
    assert sig is None
    assert staged.dropped is True
    assert "OVEREXTENDED_CHASE" in staged.drop_reason

    # Test RSI > 82 rejection
    token2 = "RsiFomoToken11111111111111111111111111111111"
    staged2 = buffer.stage_token(
        token_address=token2,
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_price=Decimal("0.0001"),
        token_creation_timestamp=now - (48 * 3600),
        report=clean_security_report,
    )
    staged2.dormant_detected = True
    staged2.consolidation_base_price = Decimal("0.0001")
    staged2.latest_price = Decimal("0.00015")  # 1.5x base price (acceptable price ratio)
    staged2.five_min_volume = Decimal("20.0")
    staged2.one_hour_volume = Decimal("4.0")
    staged2.five_min_buy_volume = Decimal("18.0")
    staged2.five_min_sell_volume = Decimal("2.0")

    # Feed consecutively increasing candles so 14-period RSI approaches 100
    for p in range(1, 25):
        staged2.record_5m_price(Decimal(str(0.00010 + p * 0.000005)))

    rsi = staged2.compute_5m_rsi(14)
    assert rsi > 82.0
    sig2 = buffer.check_breakout(token2)
    assert sig2 is None
    assert staged2.dropped is True
    assert "RSI_OVERBOUGHT_FOMO" in staged2.drop_reason


def test_cto_safety_gates(clean_security_report: SecurityReport):
    """Dev balance must be <= 0.1% and top-10 concentration must be < 25%."""
    buffer = RevivalBreakoutBuffer(config=EngineConfig())
    token = "CtoGateToken11111111111111111111111111111111"
    now = time.time()

    # Case 1: Dev wallet holds 0.5% (> 0.1%) -> Dropped
    bad_dev_report = clean_security_report.model_copy(update={"dev_balance_ratio": 0.005})
    staged_bad_dev = buffer.stage_token(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_price=Decimal("0.0001"),
        token_creation_timestamp=now - (72 * 3600),
        report=bad_dev_report,
    )
    assert staged_bad_dev is None or staged_bad_dev.dropped is True

    # Case 2: Top 10 concentration 30% (>= 25%) -> Dropped
    bad_top10_report = clean_security_report.model_copy(update={"top10_concentration": 0.30})
    staged_bad_top10 = buffer.stage_token(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_price=Decimal("0.0001"),
        token_creation_timestamp=now - (72 * 3600),
        report=bad_top10_report,
    )
    assert staged_bad_top10 is None or staged_bad_top10.dropped is True


def test_revival_swing_dynamic_exits():
    """
    Test REVIVAL_SWING dynamic exits:
    1. High Water Mark (peak_price) tracking.
    2. 35% ATH Trailing Drawdown exit (CLOSE_ALL).
    3. Laddered TP (50% at +100%, 50% at +200%).
    4. Bypass 120s velocity decay.
    """
    book = PositionBook()
    fill = PaperFill(
        order_id="fill_revival_1",
        token_address="SwingToken111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        tokens_acquired=Decimal("10000.0"),
        simulated_native_spent=Decimal("1.0"),
        effective_price=Decimal("0.0001"),
        price_impact_bps=10,
        fill_timestamp_ns=int(time.time() * 1e9),
    )

    lot = book.open_lot(
        fill=fill,
        signal_id="sig_revival_1",
        initial_pool_reserve_native=Decimal("100.0"),
        exit_profile=ExitProfile.REVIVAL_SWING,
        base_market_cap=Decimal("50000"),
    )
    assert lot.exit_profile == ExitProfile.REVIVAL_SWING
    assert lot.peak_price == Decimal("0.0001")
    assert lot.tp_stage == 0

    # 1. Bypass 120s velocity decay:
    # Simulate 600s elapsed with stagnant price
    dec_stagnant = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.000105"),  # slight gain
        current_pool_reserve_native=Decimal("100.0"),
        tick_timestamp_s=time.time() + 600.0,
        current_timestamp_ns=int((time.time() + 600.0) * 1e9),
        bonding_curve_mode=True,
    )
    assert dec_stagnant is None  # Does NOT exit on velocity decay!

    # 2. Price surges to +100% (0.00020) -> TP Stage 1 triggers
    dec_tp1 = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.00020"),
        current_pool_reserve_native=Decimal("105.0"),
        tick_timestamp_s=time.time() + 1200.0,
        current_timestamp_ns=int((time.time() + 1200.0) * 1e9),
        bonding_curve_mode=True,
    )
    assert dec_tp1 is not None
    assert dec_tp1.should_exit is True
    assert dec_tp1.exit_reason == TradeExitReason.TP_2X
    assert dec_tp1.tokens_to_sell == Decimal("5000.0")  # 50% sold!
    book.apply_exit_decision(dec_tp1, sell_price=Decimal("0.00020"))
    assert lot.tp_stage == 1
    assert lot.tokens_held == Decimal("5000.0")
    assert lot.peak_price == Decimal("0.00020")

    # 3. Price surges to +200% (0.00030) -> TP Stage 2 triggers
    dec_tp2 = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.00030"),
        current_pool_reserve_native=Decimal("110.0"),
        tick_timestamp_s=time.time() + 2400.0,
        current_timestamp_ns=int((time.time() + 2400.0) * 1e9),
        bonding_curve_mode=True,
    )
    assert dec_tp2 is not None
    assert dec_tp2.should_exit is True
    assert dec_tp2.exit_reason == TradeExitReason.TP_3X
    assert dec_tp2.tokens_to_sell == Decimal("2500.0")  # 50% of remaining 5000 sold!
    book.apply_exit_decision(dec_tp2, sell_price=Decimal("0.00030"))
    assert lot.tp_stage == 2
    assert lot.tokens_held == Decimal("2500.0")
    assert lot.peak_price == Decimal("0.00030")

    # 4. Price continues rising to new ATH peak (0.00100 = 10x)
    book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.00100"),
        current_pool_reserve_native=Decimal("150.0"),
        tick_timestamp_s=time.time() + 3600.0,
        current_timestamp_ns=int((time.time() + 3600.0) * 1e9),
        bonding_curve_mode=True,
    )
    assert lot.peak_price == Decimal("0.00100")

    # 5. Price pulls back from 0.00100 to 0.00060 (40% drawdown from peak >= 35%)
    dec_drawdown = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.00060"),
        current_pool_reserve_native=Decimal("130.0"),
        tick_timestamp_s=time.time() + 4000.0,
        current_timestamp_ns=int((time.time() + 4000.0) * 1e9),
        bonding_curve_mode=True,
    )
    assert dec_drawdown is not None
    assert dec_drawdown.should_exit is True
    assert dec_drawdown.exit_reason == TradeExitReason.TRAILING_PEAK_DRAWDOWN_35PCT
    assert dec_drawdown.action == "CLOSE_ALL"
    assert dec_drawdown.tokens_to_sell == Decimal("2500.0")  # Liquidate 100% remaining!

    book.apply_exit_decision(dec_drawdown, sell_price=Decimal("0.00060"))
    assert lot.status == LotStatus.CLOSED
    assert lot.tokens_held == Decimal("0")


def test_trade_frequency_and_cooldown_governor():
    """
    Test dynamic trading cadence governor:
    10 initial trades -> 4-hour mandatory cooldown -> Paced Mode (1 trade, then 3h cooldown) -> reset.
    """
    cfg = EngineConfig(pacing_max_initial_trades=10, pacing_cooldown_hours=4.0, pacing_interval_hours=3.0)
    engine = PaperTradingEngine(cfg)

    now = 1000000.0

    # 1. Execute initial 9 trades -> No cooldown yet
    for _ in range(9):
        assert engine.is_trading_paused_by_governor(now=now) is False
        engine.record_governor_trade(now=now)

    assert engine._consecutive_trade_count == 9
    assert engine.is_trading_paused_by_governor(now=now) is False

    # 2. Execute 10th trade -> Mandatory 4h cooldown activates
    engine.record_governor_trade(now=now)
    assert engine._consecutive_trade_count == 10
    assert engine.is_trading_paused_by_governor(now=now) is True
    assert engine._cooldown_until == now + (4.0 * 3600.0)

    # During cooldown (e.g. 2 hours in) -> Still paused
    assert engine.is_trading_paused_by_governor(now=now + 7200.0) is True

    # 3. After 4h cooldown expires -> Switches to Paced Mode (allows 1 trade)
    after_cd = now + (4.0 * 3600.0) + 1.0
    assert engine.is_trading_paused_by_governor(now=after_cd) is False
    assert engine._in_paced_mode is True

    # Execute 1 paced trade -> Enters 3-hour interval cooldown
    engine.record_governor_trade(now=after_cd)
    assert engine.is_trading_paused_by_governor(now=after_cd) is True
    assert engine._cooldown_until == after_cd + (3.0 * 3600.0)

    # 4. Manual CLI / Telegram reset
    engine.reset_trade_frequency_governor()
    assert engine._consecutive_trade_count == 0
    assert engine._cooldown_until == 0.0
    assert engine._in_paced_mode is False
    assert engine.is_trading_paused_by_governor(now=after_cd) is False


def test_ai_supervisor_deterministic_fomo_veto():
    """
    AI Supervisor deterministically rejects entry if current_mcap / dormant_mcap > 2.5.
    """
    supervisor = AlphaSupervisorAI(config=EngineConfig())

    telemetry_fomo = {
        "target_token_address": "GreenCandleToken11111111111111111111111",
        "chain": "solana_mainnet",
        "revival_telemetry": {
            "is_revival_swing": True,
            "dormant_mcap": 50000.0,
            "current_mcap": 140000.0,  # 140k / 50k = 2.8x > 2.5x threshold!
        },
        "security_telemetry": {},
        "wallet_telemetry": {},
        "pool_telemetry": {},
        "sentiment_telemetry": {},
    }

    resp = supervisor.evaluate_deterministic(telemetry_fomo)
    assert resp.decision == AISupervisorDecisionEnum.PASS
    assert "OVEREXTENDED_GREEN_CANDLE_FOMO" in resp.security_assessment.flags


def test_pattern_memory_store_revival_storage():
    """Verify revival feature vectors are stored and used for hyperparameter tuning."""
    store = PatternMemoryStore()
    tuner = DynamicParameterTuner()

    vector = RevivalPatternFeatureVector(
        token_address="LearnedRevival11111111111111111111111111111",
        token_age_hours=72.0,
        consolidation_length_hours=18.0,
        base_mcap_usd=45000.0,
        volume_surge_multiplier=3.8,
        net_buy_ratio=0.78,
        peak_roi_pct=280.0,
        realized_pnl_pct=210.0,
    )
    store.add_revival_pattern(vector)
    patterns = store.get_revival_patterns()
    assert len(patterns) == 1
    assert patterns[0].peak_roi_pct == 280.0

    # Hyperparameter tuning based on high-gain revival patterns
    params = tuner.tune_revival_parameters(patterns)
    assert params["recommended_surge_k"] == 3.8
    assert params["min_consolidation_hours"] == 18.0
