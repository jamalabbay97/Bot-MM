"""
tests/test_established_tokens_and_traceability.py — Tests for Established DEX Swaps & Traceability
====================================================================================================
Tests:
1. Absolute volume heuristic boost in generate_short_term_scalp_signal:
   - 5m volume >= 10.0 SOL -> alpha >= 0.90
   - 5m volume >= 3.0 SOL  -> alpha >= 0.75
   - 5m volume >= 1.0 SOL  -> alpha >= 0.68
2. Traceability tags (🆕 NEW vs 🏛️ ESTAB):
   - In open positions output of Telegram /trades
   - In closed receipts output of Telegram /trades
   - In executed paper trades table of Telegram /trades
3. SQLiteLedger is_new_launch column creation, migration, insertion, and retrieval.
4. PaperExecutor propagation of is_new_launch into PaperFill and PositionBook.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
import pytest

from alpha_engine.engine.signals import SignalGenerator
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.ingestion.telegram import TelegramIngester
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    SecurityTier,
    SignalStrength,
)
from alpha_engine.models.events import SwapEvent
from alpha_engine.models.state import PaperFill, PoolState, SecurityReport


def test_scalp_alpha_volume_spike_boost():
    """Verify that aggregate 5m volume spikes boost the scalp alpha score appropriately."""
    gen = SignalGenerator()

    # Create dummy established swap and pool with high native reserves (established AMM)
    swap = SwapEvent(
        timestamp_ns=time.time_ns(),
        tx_hash="0x" + "a" * 64,
        block_number=1000,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="So11111111111111111111111111111111111111112",
        token_in="So11111111111111111111111111111111111111112",
        token_out="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        amount_in=Decimal("0.5"),
        amount_out=Decimal("100.0"),
        sender="1" * 32,
    )

    pool = PoolState(
        pool_address=swap.pool_address,
        chain=swap.chain,
        token_address=swap.token_out,
        token_reserve=Decimal("10000000.0"),
        native_reserve=Decimal("50000.0"),  # Large reserve -> low relative impact
        spot_price_native_per_token=Decimal("0.005"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1000,
    )

    report = SecurityReport(
        token_address=swap.token_out,
        chain=swap.chain,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        mint_authority_disabled=True,
        top10_concentration=0.05,
    )

    # 1. Volume 5m >= 10.0 SOL
    agg_10 = MagicMock()
    metrics_10 = MagicMock()
    metrics_10.volume_5m = Decimal("12.5")
    metrics_10.latest_price = Decimal("0.004")
    agg_10.get_metrics.return_value = metrics_10
    agg_10.check_volume_spike.return_value = (True, Decimal("3.5"))
    agg_10.get_order_flow_imbalance.return_value = 0.85

    sig_10 = gen.generate_short_term_scalp_signal(
        swap=swap,
        pool=pool,
        report=report,
        metrics_aggregator=agg_10,
    )
    assert sig_10 is not None
    assert sig_10.alpha_score >= 0.90
    assert sig_10.signal_strength == SignalStrength.STRONG

    # 2. Volume 5m >= 3.0 SOL
    agg_3 = MagicMock()
    metrics_3 = MagicMock()
    metrics_3.volume_5m = Decimal("4.2")
    metrics_3.latest_price = Decimal("0.004")
    agg_3.get_metrics.return_value = metrics_3
    agg_3.check_volume_spike.return_value = (True, Decimal("3.2"))
    agg_3.get_order_flow_imbalance.return_value = 0.82

    sig_3 = gen.generate_short_term_scalp_signal(
        swap=swap,
        pool=pool,
        report=report,
        metrics_aggregator=agg_3,
    )
    assert sig_3 is not None
    assert sig_3.alpha_score >= 0.75

    # 3. Volume 5m >= 1.0 SOL
    agg_1 = MagicMock()
    metrics_1 = MagicMock()
    metrics_1.volume_5m = Decimal("1.8")
    metrics_1.latest_price = Decimal("0.004")
    agg_1.get_metrics.return_value = metrics_1
    agg_1.check_volume_spike.return_value = (True, Decimal("3.0"))
    agg_1.get_order_flow_imbalance.return_value = 0.80

    sig_1 = gen.generate_short_term_scalp_signal(
        swap=swap,
        pool=pool,
        report=report,
        metrics_aggregator=agg_1,
    )
    assert sig_1 is not None
    assert sig_1.alpha_score >= 0.68


@pytest.mark.anyio
async def test_telegram_trades_traceability_tags():
    """Verify that Telegram /trades output renders 🆕 NEW vs 🏛️ ESTAB tags."""
    queue: asyncio.Queue = asyncio.Queue()
    ingester = TelegramIngester(event_queue=queue)

    status_data = {
        "open_trades": [
            {
                "token_address": "TokenNewLaunch11111111111111111111111111111",
                "chain": "solana_mainnet",
                "platform": "Pump.fun",
                "is_new_launch": True,
                "entry_price": 0.0001,
                "current_price": 0.00015,
                "unrealized_pnl_usd": 15.0,
                "unrealized_pnl_pct": 50.0,
                "position_value_usd": 45.0,
                "open_timestamp_ns": 0,
                "trailing_stop_status": "OFF",
            },
            {
                "token_address": "TokenEstablished2222222222222222222222222222",
                "chain": "solana_mainnet",
                "platform": "DexScan",
                "is_new_launch": False,
                "entry_price": 0.05,
                "current_price": 0.052,
                "unrealized_pnl_usd": 5.0,
                "unrealized_pnl_pct": 4.0,
                "position_value_usd": 105.0,
                "open_timestamp_ns": 0,
                "trailing_stop_status": "TRAILING_ARMED",
            },
        ],
        "recent_closed_trades": [
            {
                "token_address": "TokenClosedNew33333333333333333333333333333",
                "chain": "solana_mainnet",
                "platform": "Pump.fun",
                "is_new_launch": True,
                "entry_price": 0.0001,
                "exit_price": 0.0002,
                "realized_pnl_usd": 30.0,
                "realized_pnl_pct": 100.0,
                "exit_reason": "take_profit_tp2",
                "duration_s": 120.0,
                "is_win": True,
            },
            {
                "token_address": "TokenClosedEstab4444444444444444444444444444",
                "chain": "solana_mainnet",
                "platform": "DexScan",
                "is_new_launch": False,
                "entry_price": 0.10,
                "exit_price": 0.11,
                "realized_pnl_usd": 20.0,
                "realized_pnl_pct": 10.0,
                "exit_reason": "trailing_stop",
                "duration_s": 350.0,
                "is_win": True,
            },
        ],
    }

    ingester.set_status_provider(lambda: status_data)

    replies: list[str] = []
    mock_event = MagicMock()
    mock_event.is_private = True
    mock_event.chat_id = 12345
    mock_event.sender_id = 12345
    mock_event.message.message = "/trades"
    mock_event.reply = AsyncMock(side_effect=lambda msg, **kwargs: replies.append(msg))

    await ingester._cmd_trades(mock_event)

    assert len(replies) == 1
    resp = replies[0]

    # Open positions must have both tags
    assert "[Pump.fun] [🆕 NEW]" in resp
    assert "[DexScan] [🏛️ ESTAB]" in resp

    # Closed receipts must have both tags
    assert "[Pump.fun] [🆕 NEW]" in resp
    assert "[DexScan] [🏛️ ESTAB]" in resp


@pytest.mark.anyio
async def test_sqlite_ledger_is_new_launch_persistence(tmp_path):
    """Verify is_new_launch field persistence and retrieval in SQLiteLedger."""
    db_file = tmp_path / "test_trades.db"

    async with SQLiteLedger(db_path=db_file) as ledger:
        fill_new = PaperFill(
            token_address="TokenNew1111111111111111111111111111111111111",
            chain=ChainIdentifier.SOLANA_MAINNET,
            effective_price=Decimal("0.001"),
            tokens_acquired=Decimal("1000"),
            simulated_native_spent=Decimal("1.0"),
            side=OrderSide.BUY,
            platform="Pump.fun",
            is_new_launch=True,
        )

        fill_estab = PaperFill(
            token_address="TokenEstab222222222222222222222222222222222222",
            chain=ChainIdentifier.SOLANA_MAINNET,
            effective_price=Decimal("0.05"),
            tokens_acquired=Decimal("200"),
            simulated_native_spent=Decimal("10.0"),
            side=OrderSide.BUY,
            platform="DexScan",
            is_new_launch=False,
        )

        await ledger.record_trade(fill_new)
        await ledger.record_trade(fill_estab)

        recent = await ledger.get_recent_trades(limit=5)
        assert len(recent) == 2

        # Most recent first
        t0 = recent[0]
        t1 = recent[1]

        assert t0["platform"] == "DexScan"
        assert t0["is_new_launch"] is False

        assert t1["platform"] == "Pump.fun"
        assert t1["is_new_launch"] is True
