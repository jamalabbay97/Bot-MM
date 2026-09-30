"""
Unit and integration tests for PendingLaunchBuffer parameter calibration,
countdown/progress telemetry, detailed expiration drop reason logging,
and execution pipeline dispatch for autonomous paper trading.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
import logging
from pathlib import Path
import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.signals import PendingLaunchBuffer, SignalGenerator
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.book import PositionBook
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    SecurityTier,
    SignalSource,
)
from alpha_engine.models.events import RawSignalEvent, SwapEvent
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.execution.ledger import SQLiteLedger


def test_pending_launch_buffer_default_parameters():
    """Verify PendingLaunchBuffer default thresholds are calibrated for paper trading."""
    buffer = PendingLaunchBuffer()
    assert buffer.min_age_s == 30.0
    assert buffer.max_age_s == 120.0
    assert buffer.min_volume_native == Decimal("3.0")
    assert buffer.min_buys == 5
    assert buffer.min_unique_signers == 3


def test_engine_config_buffer_parameters():
    """Verify EngineConfig loads and exposes relaxed buffer parameters with backward compatibility."""
    cfg = EngineConfig()
    assert cfg.observation_window_min_sec == 30.0
    assert cfg.observation_window_max_sec == 120.0
    assert cfg.buffer_target_volume_sol == Decimal("3.0")
    assert cfg.buffer_min_buys == 5
    assert cfg.buffer_min_unique_buyers == 3
    assert cfg.min_unique_buyers == 3

    # Backward compatibility: setting min_unique_buyers works
    cfg_legacy = EngineConfig(min_unique_buyers=10)
    assert cfg_legacy.buffer_min_unique_buyers == 10
    assert cfg_legacy.min_unique_buyers == 10


def test_buffer_telemetry_countdown_and_progress_logging(caplog):
    """Verify explicit countdown and progress logging every 15 seconds for staged mints."""
    buffer = PendingLaunchBuffer()
    token = "PumpMintTelemetry111111111111111111111111111"
    pool_addr = "CurveAddrTelemetry11111111111111111111111111"
    report = SecurityReport(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    raw_sig = RawSignalEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token,
        pool_address=pool_addr,
        source=SignalSource.PUMP_FUN_MINT,
    )
    t_0 = 1000.0
    init_price = Decimal("0.000000028")

    buffer.add_launch(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        initial_price=init_price,
        report=report,
        raw_signal=raw_sig,
        t_0=t_0,
    )

    pool = PoolState(
        pool_address=pool_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token,
        native_reserve=Decimal("30.0"),
        token_reserve=Decimal("1073000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1,
    )
    sig_gen = SignalGenerator()

    with caplog.at_level(logging.INFO):
        # 1. At t_0 + 15s: periodic check_telemetry logs progress
        buffer.check_telemetry(current_time_s=t_0 + 15.0, interval_s=15.0)
        assert f"Token [{token[:10]}]: 15s elapsed" in caplog.text
        assert "Vol: 0.0/3.0 SOL" in caplog.text
        assert "Buys: 0/5" in caplog.text
        assert "Buyers: 0/3" in caplog.text

        caplog.clear()

        # 2. Add buy transactions to simulate progress (3 unique buyers, 4 buys totaling 2.1 SOL)
        for i in range(4):
            swap = SwapEvent(
                timestamp_ns=int((t_0 + 20.0 + i * 5.0) * 1e9),
                block_number=10 + i,
                chain=ChainIdentifier.SOLANA_MAINNET,
                pool_address=pool_addr,
                token_in="So11111111111111111111111111111111111111112",
                token_out=token,
                amount_in=Decimal("0.525"),  # 4 * 0.525 = 2.1 SOL
                amount_out=Decimal("10000000"),
                sender=f"Buyer{i % 3:028d}",  # 3 unique buyers
                tx_hash=f"tx_{i:030d}",
            )
            buffer.record_swap(swap, pool, sig_gen, current_time_s=t_0 + 20.0 + i * 5.0)

        caplog.clear()

        # Check telemetry output at 45s
        buffer.check_telemetry(current_time_s=t_0 + 45.0, interval_s=15.0)
        assert f"Token [{token[:10]}]: 45s elapsed" in caplog.text
        assert "Vol: 2.1/3.0 SOL" in caplog.text
        assert "Buys: 4/5" in caplog.text
        assert "Buyers: 3/3" in caplog.text


def test_buffer_exact_condition_logged_on_expiration(caplog):
    """Verify that when a token expires without execution, the exact unmet conditions are logged."""
    buffer = PendingLaunchBuffer(
        min_age_s=30.0,
        max_age_s=120.0,
        min_buys=5,
        min_volume_native=Decimal("3.0"),
        min_unique_signers=3,
    )
    token = "PumpMintExpire11111111111111111111111111111"
    pool_addr = "CurveAddrExpire1111111111111111111111111111"
    report = SecurityReport(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    raw_sig = RawSignalEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token,
        pool_address=pool_addr,
        source=SignalSource.PUMP_FUN_MINT,
    )
    t_0 = 1000.0
    buffer.add_launch(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        initial_price=Decimal("0.000000028"),
        report=report,
        raw_signal=raw_sig,
        t_0=t_0,
    )

    pool = PoolState(
        pool_address=pool_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token,
        native_reserve=Decimal("30.0"),
        token_reserve=Decimal("1073000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1,
    )
    sig_gen = SignalGenerator()

    # Only 2 buys of 0.6 SOL from 2 buyers (insufficient volume 1.2 < 3.0, insufficient buys 2 < 5, buyers 2 < 3)
    for i in range(2):
        sw = SwapEvent(
            timestamp_ns=int((t_0 + 40.0) * 1e9),
            block_number=i + 1,
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_addr,
            token_in="So11111111111111111111111111111111111111112",
            token_out=token,
            amount_in=Decimal("0.6"),
            amount_out=Decimal("10000000"),
            sender=f"BuyerExpired{i:020d}",
            tx_hash=f"tx_exp_{i:025d}",
        )
        buffer.record_swap(sw, pool, sig_gen, current_time_s=t_0 + 40.0)

    caplog.clear()

    # Time reaches 125s (> 120s max_age_s), sweep_expired triggers eviction
    with caplog.at_level(logging.INFO):
        expired = buffer.sweep_expired(current_time_s=t_0 + 125.0)
        assert token in expired
        staged = buffer.get_staged(token)
        assert staged is not None
        assert staged.dropped is True
        assert "Observation window expired (125.0s > 120.0s)" in staged.drop_reason
        # Verify exact conditions are documented
        assert "Vol: 1.2/3.0 SOL" in staged.drop_reason
        assert "Buys: 2/5" in staged.drop_reason
        assert "Buyers: 2/3" in staged.drop_reason
        assert f"DROPPING launch {token[:10]}" in caplog.text


@pytest.mark.anyio
async def test_buffer_graduation_and_paper_execution_pipeline(tmp_path: Path):
    """Verify staged token graduates under relaxed parameters and cleanly executes a paper trade."""
    buffer = PendingLaunchBuffer(
        min_age_s=30.0,
        max_age_s=120.0,
        min_buys=5,
        min_volume_native=Decimal("3.0"),
        min_unique_signers=3,
        min_blocks_span=3,
    )
    sig_gen = SignalGenerator()
    token = "PumpMintGrad11111111111111111111111111111111"
    pool_addr = "CurveAddrGrad111111111111111111111111111111"
    report = SecurityReport(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    raw_sig = RawSignalEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token,
        pool_address=pool_addr,
        source=SignalSource.PUMP_FUN_MINT,
    )
    t_0 = 1000.0

    buffer.add_launch(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        initial_price=Decimal("0.000000028"),
        report=report,
        raw_signal=raw_sig,
        t_0=t_0,
    )

    pool = PoolState(
        pool_address=pool_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token,
        native_reserve=Decimal("30.0"),
        token_reserve=Decimal("1073000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=10,
    )

    # 4 buys of 0.6 SOL in different blocks and different buyers
    for i in range(4):
        sw = SwapEvent(
            timestamp_ns=int((t_0 + 35.0) * 1e9),
            block_number=10 + i,
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_addr,
            token_in="So11111111111111111111111111111111111111112",
            token_out=token,
            amount_in=Decimal("0.6"),
            amount_out=Decimal("10000000"),
            sender=f"GradBuyer_{i:025d}",
            tx_hash=f"tx_grad_{i:028d}",
        )
        sig = buffer.record_swap(sw, pool, sig_gen, current_time_s=t_0 + 35.0)
        assert sig is None

    # 5th buy: brings volume to 3.1 SOL (>= 3.0), buys=5 (>= 5), unique buyers=5 (>= 3), age=35s (>= 30s)
    sw_5 = SwapEvent(
        timestamp_ns=int((t_0 + 35.0) * 1e9),
        block_number=15,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        token_in="So11111111111111111111111111111111111111112",
        token_out=token,
        amount_in=Decimal("0.7"),
        amount_out=Decimal("12000000"),
        sender="GradBuyer_0000000000000000000000004",
        tx_hash="tx_grad_0000000000000000000000000004",
    )
    grad_signal = buffer.record_swap(sw_5, pool, sig_gen, current_time_s=t_0 + 35.0)

    # Verify graduation
    assert grad_signal is not None
    assert grad_signal.suggested_side == OrderSide.BUY
    assert grad_signal.token_address == token
    assert grad_signal.source == SignalSource.PUMP_FUN_MINT

    # Now verify paper execution via PaperExecutor and SQLiteLedger
    db_file = tmp_path / "test_exec.db"
    book = PositionBook()
    executor = PaperExecutor(
        position_book=book,
        native_price_usd=Decimal("150.0"),  # SOL = $150
    )

    async with SQLiteLedger(str(db_file)) as ledger:
        await ledger.record_signal(grad_signal)

        # Execute paper trade
        fill = await executor.execute_signal(grad_signal, portfolio_equity_usd=Decimal("1500.0"))
        assert fill is not None
        assert fill.side == OrderSide.BUY
        assert fill.tokens_acquired > Decimal(0)
        assert fill.effective_price > Decimal(0)

        # Open lot in PositionBook & record in ledger
        lot = book.open_lot(
            fill=fill,
            signal_id=grad_signal.signal_id,
            initial_pool_reserve_native=pool.native_reserve,
            pool_address=pool.pool_address,
            bonding_curve_mode=True,
        )
        assert lot is not None
        assert book.is_position_open(token, ChainIdentifier.SOLANA_MAINNET) is True

        from alpha_engine.models.state import TradeRecord
        trade_rec = TradeRecord.from_fill(fill=fill, signal_id=grad_signal.signal_id)
        await ledger.record_trade(trade_rec)
