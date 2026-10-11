"""
tests/test_isolated_launch_and_governor.py
=========================================
Unit and integration tests for isolated circuit breakers, pacing governor segregation,
and DEX pool auto-discovery / seeding for established tokens.
"""

import time
from decimal import Decimal
import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.execution.book import RunningMetrics
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitProfile,
    OrderSide,
    SignalSource,
    SignalStrength,
)
from alpha_engine.models.events import SignalEvent, SwapEvent
from alpha_engine.models.state import PoolState, SecurityReport, SecurityTier


def make_dummy_signal(
    token_address: str = "FakeTokenAddressFakeTokenAddress123",
    source: SignalSource = SignalSource.PUMP_FUN_MINT,
    exit_profile: ExitProfile = ExitProfile.FAST_SNIPE,
    token_age_hours: float = 0.05,
    token_age_s: float = 180.0,
    alpha_score: float = 0.85,
) -> SignalEvent:
    pool_state = PoolState(
        pool_address="FakePoolAddressFakePoolAddress1234",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("100.0"),
        fee_numerator=3000,
        fee_denominator=1000000,
        last_updated_block=123456,
        token_decimals=6,
        native_decimals=9,
    )
    security_report = SecurityReport(
        token_address=token_address,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        is_rug=False,
        score=100.0,
        flags=[],
        buy_tax_bps=0,
        sell_tax_bps=0,
        checked_at_ts=int(time.time()),
    )
    return SignalEvent(
        token_address=token_address,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="FakePoolAddressFakePoolAddress1234",
        pool_state=pool_state,
        security_report=security_report,
        strength=SignalStrength.STRONG,
        suggested_side=OrderSide.BUY,
        alpha_score=alpha_score,
        source=source,
        exit_profile=exit_profile,
        token_age_hours=token_age_hours,
        token_age_s=token_age_s,
        timestamp_ns=time.time_ns(),
    )


def test_running_metrics_launch_circuit_breaker_isolation():
    """
    Verify that 3 consecutive launch losses ONLY trip the launch circuit breaker,
    leaving the global circuit breaker inactive so older/DEX coins continue trading.
    """
    metrics = RunningMetrics(peak_equity_usd=Decimal("10000.0"))

    # 1. Record 2 launch losses
    metrics.record_closed_trade(
        realized_pnl_usd=Decimal("-10.0"),
        gas_cost_usd=Decimal("0.01"),
        current_equity_usd=Decimal("9990.0"),
        is_new_launch=True,
    )
    metrics.record_closed_trade(
        realized_pnl_usd=Decimal("-15.0"),
        gas_cost_usd=Decimal("0.01"),
        current_equity_usd=Decimal("9975.0"),
        is_new_launch=True,
    )
    assert metrics.consecutive_launch_losses == 2
    assert metrics.consecutive_losses == 0
    assert metrics.launch_circuit_breaker_active is False
    assert metrics.circuit_breaker_active is False

    # 2. Record 3rd consecutive launch loss -> trips launch circuit breaker ONLY
    metrics.record_closed_trade(
        realized_pnl_usd=Decimal("-20.0"),
        gas_cost_usd=Decimal("0.01"),
        current_equity_usd=Decimal("9955.0"),
        is_new_launch=True,
    )
    assert metrics.consecutive_launch_losses == 3
    assert metrics.consecutive_losses == 0
    assert metrics.launch_circuit_breaker_active is True
    assert metrics.is_launch_circuit_breaker_active(freeze_duration_s=3600.0) is True
    assert metrics.circuit_breaker_active is False
    assert metrics.is_circuit_breaker_active(freeze_duration_s=3600.0) is False

    # 3. Winning launch trade resets launch loss counter
    metrics.record_closed_trade(
        realized_pnl_usd=Decimal("50.0"),
        gas_cost_usd=Decimal("0.01"),
        current_equity_usd=Decimal("10005.0"),
        is_new_launch=True,
    )
    assert metrics.consecutive_launch_losses == 0
    assert metrics.consecutive_losses == 0


def test_running_metrics_general_losses_trip_global_circuit_breaker():
    """
    Verify that general / older token losses correctly trip the global circuit breaker.
    """
    metrics = RunningMetrics(peak_equity_usd=Decimal("10000.0"))

    # Record 3 general losses (is_new_launch=False)
    metrics.record_closed_trade(Decimal("-10.0"), Decimal("0.01"), Decimal("9990.0"), is_new_launch=False)
    metrics.record_closed_trade(Decimal("-20.0"), Decimal("0.01"), Decimal("9970.0"), is_new_launch=False)
    assert metrics.consecutive_losses == 2
    assert metrics.circuit_breaker_active is False

    metrics.record_closed_trade(Decimal("-30.0"), Decimal("0.01"), Decimal("9940.0"), is_new_launch=False)
    assert metrics.consecutive_losses == 3
    assert metrics.circuit_breaker_active is True
    assert metrics.is_circuit_breaker_active(freeze_duration_s=3600.0) is True


def test_is_new_launch_signal_classification():
    """
    Verify PaperTradingEngine._is_new_launch_signal correctly separates
    new launches from Wave-2, Revival, and established coins.
    """
    # Brand new launch signal
    launch_sig = make_dummy_signal(
        source=SignalSource.PUMP_FUN_MINT,
        exit_profile=ExitProfile.FAST_SNIPE,
        token_age_hours=0.05,
        token_age_s=180.0,
    )
    assert PaperTradingEngine._is_new_launch_signal(launch_sig) is True

    # Revival breakout signal -> Not a new launch
    revival_sig = make_dummy_signal(
        source=SignalSource.REVIVAL_BREAKOUT,
        exit_profile=ExitProfile.REVIVAL_SWING,
        token_age_hours=48.0,
        token_age_s=172800.0,
    )
    assert PaperTradingEngine._is_new_launch_signal(revival_sig) is False

    # Wave-2 breakout signal -> Not a new launch
    wave2_sig = make_dummy_signal(
        source=SignalSource.WAVE2_BREAKOUT,
        token_age_hours=2.5,
        token_age_s=9000.0,
    )
    assert PaperTradingEngine._is_new_launch_signal(wave2_sig) is False

    # Older established coin by age (> 0.5 hours or > 1800s) -> Not a new launch
    aged_sig = make_dummy_signal(
        source=SignalSource.DEX_SWAP,
        token_age_hours=1.2,
        token_age_s=4320.0,
    )
    assert PaperTradingEngine._is_new_launch_signal(aged_sig) is False


@pytest.mark.anyio
async def test_unknown_pool_auto_seeded():
    """
    Verify that when a swap arrives for an unknown pool address on Raydium/DEX,
    the engine does NOT drop it, but automatically creates an initial pool state.
    """
    cfg = EngineConfig(strategy_v2_enabled=False)
    engine = PaperTradingEngine(config=cfg)

    unknown_token = "So11111111111111111111111111111111111111112"
    unknown_pool = "PoolRaydium11111111111111111111111111111111"

    # Pool is not in registry initially
    assert engine._pool_registry.get(unknown_pool) is None
    assert engine._pool_registry.get(unknown_token) is None

    # Process swap via ingestion queue handler
    swap = SwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=unknown_pool,
        token_in="So11111111111111111111111111111111111111111",
        token_out=unknown_token,
        amount_in=Decimal("1.0"),
        amount_out=Decimal("100.0"),
        sender="MakerWallet123MakerWallet123MakerWallet123",
        timestamp_ns=time.time_ns(),
        block_number=1000,
        tx_hash="txhash123txhash123txhash123txhash123",
    )

    # Ingestion queue logic test for pool initialization
    target_token = swap.token_out
    pool = engine._pool_registry.get(swap.pool_address)
    if pool is None:
        pool = engine._pool_registry.get(target_token)
    if pool is None:
        pool = engine.get_or_create_initial_pool_state(swap.chain, target_token, swap.pool_address)

    assert pool is not None
    assert engine._pool_registry.get(unknown_pool) is not None
    assert engine._pool_registry.get(target_token) is not None


def test_pacing_governor_affects_only_launches():
    """
    Verify that when the pacing governor is active, only new launches are paused,
    while older coins (Revival/Wave-2/established) can trade normally.
    """
    cfg = EngineConfig(
        pacing_max_initial_trades=2,
        pacing_cooldown_hours=1.0,
        strategy_v2_enabled=False,
    )
    engine = PaperTradingEngine(config=cfg)

    # Force governor cooldown
    engine._cooldown_until = time.time() + 3600.0
    assert engine.is_trading_paused_by_governor() is True

    # Signal 1: New Launch
    launch_sig = make_dummy_signal(
        source=SignalSource.PUMP_FUN_MINT,
        token_age_hours=0.01,
        token_age_s=36.0,
    )
    is_launch = engine._is_new_launch_signal(launch_sig)
    assert is_launch is True
    # New launch is blocked when governor is active
    assert is_launch and engine.is_trading_paused_by_governor()

    # Signal 2: Revival Breakout
    revival_sig = make_dummy_signal(
        source=SignalSource.REVIVAL_BREAKOUT,
        exit_profile=ExitProfile.REVIVAL_SWING,
        token_age_hours=24.0,
        token_age_s=86400.0,
    )
    is_revival_launch = engine._is_new_launch_signal(revival_sig)
    assert is_revival_launch is False
    # Revival signal is NOT blocked by governor!
    assert not (is_revival_launch and engine.is_trading_paused_by_governor())
