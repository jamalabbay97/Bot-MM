import time
from decimal import Decimal
import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.engine.staging import Wave2StagingBuffer
from alpha_engine.execution.book import OpenLot, PositionBook
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitProfile,
    LotStatus,
    OrderSide,
    SecurityTier,
    SignalSource,
    StrategyType,
    TradeExitReason,
)
from alpha_engine.models.events import SignalEvent, SwapEvent
from alpha_engine.models.state import PaperFill, PoolState, SecurityReport


@pytest.fixture
def clean_security_report():
    return SecurityReport(
        token_address="EarlyRallyToken11111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.12,
        mint_authority_disabled=True,
        freeze_authority_disabled=True,
        verified_source_code=True,
    )


@pytest.fixture
def sample_pool():
    return PoolState(
        pool_address="RallyPool11111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000000"),
        native_reserve=Decimal("30"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=1000,
        token_decimals=6,
        native_decimals=9,
        token_address="EarlyRallyToken11111111111111111111111111111",
    )


def test_early_rally_breakout_detection_at_bottom(clean_security_report, sample_pool):
    """
    Test that a token bottoming and consolidating (e.g. at $3.8K mcap like VB)
    detects volume surge and emits an early rally breakout signal.
    """
    buffer = Wave2StagingBuffer(
        ttl_seconds=7200.0,
        volume_surge_multiplier_k=2.5,
        min_net_buy_delta_pct=65.0,
        min_consolidation_duration_s=60.0,
    )
    token_addr = clean_security_report.token_address
    t_stage = time.time() - 700.0

    buffer.stage_token(
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=sample_pool.pool_address,
        initial_price=Decimal("0.0000100"),
        report=clean_security_report,
        t_staged=t_stage,
    )

    # 1. Simulate dump to bottom & quiet consolidation 10 minutes ago
    base_time_ns = int((time.time() - 600.0) * 1e9)
    for i in range(8):
        ts = base_time_ns + i * 5_000_000_000
        is_buy = i % 2 == 0
        swap = SwapEvent(
            block_number=100 + i,
            timestamp_ns=ts,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tx_hash=f"TxConsolidate_{i:02d}".ljust(32, "1"),
            pool_address=sample_pool.pool_address,
            sender=f"ConsolidateWallet_{i:02d}".ljust(32, "1"),
            token_in="So11111111111111111111111111111111111111112" if is_buy else token_addr,
            token_out=token_addr if is_buy else "So11111111111111111111111111111111111111112",
            amount_in=Decimal("0.08") if is_buy else Decimal("3000.0"),
            amount_out=Decimal("3000.0") if is_buy else Decimal("0.08"),
        )
        sig = buffer.record_trade(swap, pool=sample_pool)
        assert sig is None  # Still in base consolidation

    # 2. Early rally starts: strong volume surge and heavy buy delta
    breakout_time_ns = int(time.time() * 1e9)
    breakout_sig = None
    for j in range(5):
        swap_surge = SwapEvent(
            block_number=200 + j,
            timestamp_ns=breakout_time_ns + j * 1_000_000_000,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tx_hash=f"TxEarlyRallySurge_{j:02d}".ljust(32, "1"),
            pool_address=sample_pool.pool_address,
            sender=f"SmartBuyer_{j:02d}".ljust(32, "1"),
            token_in="So11111111111111111111111111111111111111112",
            token_out=token_addr,
            amount_in=Decimal("1.2"),
            amount_out=Decimal("40000.0"),
        )
        sig = buffer.record_trade(swap_surge, pool=sample_pool)
        if sig is not None:
            breakout_sig = sig
            break

    assert breakout_sig is not None
    assert breakout_sig.suggested_side == OrderSide.BUY
    assert breakout_sig.exit_profile == ExitProfile.EARLY_RALLY_SWING
    assert breakout_sig.strategy_type == StrategyType.EARLY_RALLY_BREAKOUT
    assert breakout_sig.strategy_pattern == "early_rally_breakout"
    assert breakout_sig.volume_surge_multiplier is not None
    assert breakout_sig.volume_surge_multiplier >= 2.5
    assert breakout_sig.net_buy_delta is not None
    assert breakout_sig.net_buy_delta >= 0.65


def test_early_rally_anti_fomo_guard(clean_security_report, sample_pool):
    """
    Test that if a token has already pumped > 120% from its base floor (e.g. buying near top of pump),
    breakout generation is suppressed to protect against FOMO traps.
    """
    buffer = Wave2StagingBuffer(
        ttl_seconds=7200.0,
        volume_surge_multiplier_k=2.5,
        min_net_buy_delta_pct=65.0,
        min_consolidation_duration_s=60.0,
    )
    token_addr = clean_security_report.token_address
    t_stage = time.time() - 700.0

    buffer.stage_token(
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=sample_pool.pool_address,
        initial_price=Decimal("0.0000100"),
        report=clean_security_report,
        t_staged=t_stage,
    )

    tok = buffer._staged[token_addr]
    tok.trough_price = Decimal("0.0000100")
    # Simulate current price already blown past +150% from floor (FOMO top)
    tok.latest_price = Decimal("0.0000260")

    now = time.time()
    sig = buffer._evaluate_breakout(tok, pool=sample_pool, current_time_s=now)
    assert sig is None
    assert tok.active_rules_status.get("AntiFOMOPassed") is False


def test_early_rally_hard_stop_loss_15_pct(sample_pool):
    """
    Verify that an early rally trade triggers the strict -15% hard stop-loss immediately
    if entry fails, preventing holding all the way down into catastrophic -91% losses.
    """
    book = PositionBook()
    token_addr = sample_pool.token_address

    fill = PaperFill(
        order_id="order_early_01",
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        effective_price=Decimal("1.000"),
        tokens_acquired=Decimal("1000.0"),
        simulated_native_spent=Decimal("1.0"),
    )
    lot: OpenLot = book.open_lot(
        fill=fill,
        signal_id="sig-early-1",
        strategy_name="early_rally",
        exit_profile=ExitProfile.EARLY_RALLY_SWING,
        trailing_stop_from_peak_pct=0.28,
        custom_stop_loss_pct=0.15,
        custom_take_profit_pct=0.50,
        partial_exit_enabled=True,
    )
    assert lot is not None

    # Price drops to 0.88 (-12%): Stop loss not yet reached (-15% is threshold)
    exit_decision = book.evaluate_lot_exit(lot, current_price=Decimal("0.880"))
    assert exit_decision is None

    # Price drops to 0.849 (-15.1%): Strict -15% Stop-loss MUST trigger immediately!
    exit_decision = book.evaluate_lot_exit(lot, current_price=Decimal("0.849"))
    assert exit_decision is not None
    assert exit_decision.should_exit is True
    assert exit_decision.exit_reason == TradeExitReason.EARLY_RALLY_SL_15
    assert exit_decision.action == "CLOSE_ALL"


def test_early_rally_scale_out_and_peak_trailing_stop(sample_pool):
    """
    Test two-stage exit:
      1. +50% rally triggers 50% partial exit, locking profit and securing breakeven.
      2. Moonbag runner rides up to ATH peak (4.00, +300%) and trails with 28% offset (stops at 2.88).
    """
    book = PositionBook()
    token_addr = sample_pool.token_address

    fill = PaperFill(
        order_id="order_early_02",
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        effective_price=Decimal("1.000"),
        tokens_acquired=Decimal("1000.0"),
        simulated_native_spent=Decimal("1.0"),
    )
    lot: OpenLot = book.open_lot(
        fill=fill,
        signal_id="sig-early-2",
        strategy_name="early_rally",
        exit_profile=ExitProfile.EARLY_RALLY_SWING,
        trailing_stop_from_peak_pct=0.28,
        custom_stop_loss_pct=0.15,
        custom_take_profit_pct=0.50,
        partial_exit_enabled=True,
    )
    assert lot is not None

    # 1. Price runs up to 1.52 (+52%): Triggers 50% Take Profit Scale-Out
    tp1_decision = book.evaluate_lot_exit(lot, current_price=Decimal("1.520"))
    assert tp1_decision is not None
    assert tp1_decision.should_exit is True
    assert tp1_decision.action == "PARTIAL"
    assert tp1_decision.exit_reason in (TradeExitReason.TP_50, TradeExitReason.TP_CUSTOM)
    assert tp1_decision.tokens_to_sell == Decimal("500.0")

    # Apply partial exit fill
    lot.scaled_out_50_pct = True
    lot.tokens_held = Decimal("500.0")

    # 2. Moonbag runner explodes to 4.00 (+300% from entry, ATH peak)
    book.evaluate_lot_exit(lot, current_price=Decimal("4.000"))
    assert lot.peak_price == Decimal("4.000")

    # Trailing stop threshold = 4.00 * (1 - 0.28) = 2.88
    # Price pulls back to 3.20 (within 28% trail from peak): Runner remains open
    decision_at_320 = book.evaluate_lot_exit(lot, current_price=Decimal("3.200"))
    assert decision_at_320 is None

    # Price pulls back to 2.85 (< 2.88 trailing stop): Moonbag exits with runner profit!
    runner_decision = book.evaluate_lot_exit(lot, current_price=Decimal("2.850"))
    assert runner_decision is not None
    assert runner_decision.should_exit is True
    assert runner_decision.exit_reason == TradeExitReason.TRAILING_PEAK_STOP
    assert runner_decision.action == "CLOSE_ALL"


def test_early_rally_runner_strategy_preset():
    """Verify that PaperTradingEngine set_trading_strategy('early_rally') configures correct parameters."""
    config = EngineConfig()
    runner = PaperTradingEngine(config=config)

    res = runner.set_trading_strategy(strategy_name="early_rally")
    assert res["active_strategy"] == "early_rally"
    assert res["trailing_stop_from_peak_pct"] == 0.28
    assert res["custom_take_profit_pct"] == 0.50
    assert res["custom_stop_loss_pct"] == 0.15
    assert res["early_rally_min_market_cap_usd"] == 3500.0
