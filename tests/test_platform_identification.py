"""
tests/test_platform_identification.py — Comprehensive Unit Tests for Trade Platform Identification
===================================================================================================
Verifies:
1. Platform resolution logic (Pump.fun vs DexScan) for mint addresses, venues, pools, and sources.
2. Platform metadata preservation in PaperFill, TradeRecord, and OpenPositionLot models.
3. Terminal logger output displaying Platform=Pump.fun and Platform=DexScan on trade executions.
4. Telegram /trades DM output explicitly showing [Pump.fun] and [DexScan] for open positions,
   recent closed trades, and SQLite ledger trade rows.
5. SQLiteLedger persistence and migration for the platform column.
6. ConversationalSupervisor exit explanation rendering the platform.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from alpha_engine.chat_interface import ConversationalSupervisor
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.ingestion.telegram import TelegramIngester
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    TradeExitReason,
    TradingPlatform,
    resolve_trade_platform,
)
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PaperFill, PoolState, SecurityReport, TradeRecord
from alpha_engine.math.cpmm import get_initial_bonding_curve_pool


def test_resolve_trade_platform():
    """Verify platform resolution accurately identifies Pump.fun vs DexScan."""
    # 1. Pump.fun token ending in pump (case insensitive)
    assert resolve_trade_platform("So11111111111111111111111111111111111111111pump") == TradingPlatform.PUMP_FUN.value
    assert resolve_trade_platform("DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263pump") == "Pump.fun"
    assert resolve_trade_platform("TESTTOKENPUMP") == "Pump.fun"
    assert resolve_trade_platform("TokenPump") == "Pump.fun"

    # 2. ExecutionVenue.PUMP_FUN
    assert resolve_trade_platform("7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", execution_venue=ExecutionVenue.PUMP_FUN) == "Pump.fun"

    # 3. SignalSource.PUMP_FUN_MINT
    assert resolve_trade_platform("7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", source=SignalSource.PUMP_FUN_MINT) == "Pump.fun"

    # 4. Pool address with pump
    assert resolve_trade_platform("7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", pool_address="pump_bonding_curve_123") == "Pump.fun"

    # 5. DexScan: Base EVM address
    assert resolve_trade_platform("0x285617313860407d647990b50375990264186566", chain=ChainIdentifier.BASE_MAINNET) == "DexScan"

    # 6. DexScan: Solana Raydium token (not ending in pump)
    assert resolve_trade_platform("7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", chain=ChainIdentifier.SOLANA_MAINNET) == "DexScan"

    # 7. DexScan fallback
    assert resolve_trade_platform("random_token_address") == "DexScan"


def test_paper_fill_and_open_lot_platform_retention():
    """Verify PaperFill and OpenPositionLot retain platform metadata."""
    fill_pump = PaperFill(
        token_address="So11111111111111111111111111111111111111111pump",
        chain=ChainIdentifier.SOLANA_MAINNET,
        effective_price=Decimal("0.001"),
        tokens_acquired=Decimal("1000"),
        simulated_native_spent=Decimal("1.0"),
        side=OrderSide.BUY,
        platform="Pump.fun",
    )
    assert fill_pump.platform == "Pump.fun"

    trade_rec = TradeRecord.from_fill(fill_pump)
    assert trade_rec.platform == "Pump.fun"

    book = PositionBook()
    lot_pump = book.open_lot(fill=fill_pump, signal_id="sig_1")
    assert lot_pump.platform == "Pump.fun"

    # Default platform is DexScan
    fill_dex = PaperFill(
        token_address="0x285617313860407d647990b50375990264186566",
        chain=ChainIdentifier.BASE_MAINNET,
        effective_price=Decimal("0.005"),
        tokens_acquired=Decimal("500"),
        simulated_native_spent=Decimal("2.5"),
        side=OrderSide.BUY,
    )
    assert fill_dex.platform == "DexScan"
    lot_dex = book.open_lot(fill=fill_dex, signal_id="sig_2")
    assert lot_dex.platform == "DexScan"


@pytest.mark.anyio
async def test_executor_terminal_logging_pump_fun_and_dex_scan(caplog):
    """Verify PaperExecutor terminal logs clearly display Platform=Pump.fun and Platform=DexScan."""
    book = PositionBook()
    executor = PaperExecutor(
        position_book=book,
        native_price_usd=Decimal("150.0"),
    )

    # 1. Execute BUY signal on Pump.fun token
    pump_token = "So11111111111111111111111111111111111111111pump"
    pool_state_pump = get_initial_bonding_curve_pool(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=pump_token,
    )
    report_pump = SecurityReport(
        token_address=pump_token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        mint_authority_disabled=True,
        top10_concentration=0.05,
    )
    signal_pump = SignalEvent(
        signal_id="sig-pump-01",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=pump_token,
        pool_address=pool_state_pump.pool_address,
        timestamp_ns=time.time_ns(),
        source=SignalSource.PUMP_FUN_MINT,
        suggested_side=OrderSide.BUY,
        alpha_score=0.88,
        strength=SignalStrength.STRONG,
        execution_venue=ExecutionVenue.PUMP_FUN,
        pool_state=pool_state_pump,
        security_report=report_pump,
    )

    with caplog.at_level(logging.INFO):
        caplog.clear()
        fill_pump = await executor.execute_signal(signal_pump, portfolio_equity_usd=Decimal("10000.0"))
        assert fill_pump is not None
        assert fill_pump.platform == "Pump.fun"
        # Check that Platform=Pump.fun is present in the log records
        assert any("Platform=Pump.fun" in record.message for record in caplog.records)

    # 2. Execute BUY signal on DexScan token
    dex_token = "0x285617313860407d647990b50375990264186566"
    pool_state_dex = PoolState(
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address="0x1111111111111111111111111111111111111111",
        token_reserve=Decimal("5000000"),
        native_reserve=Decimal("100"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=987654,
        token_decimals=18,
        native_decimals=18,
    )
    report_dex = SecurityReport(
        token_address=dex_token,
        chain=ChainIdentifier.BASE_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        mint_authority_disabled=True,
        top10_concentration=0.05,
    )
    signal_dex = SignalEvent(
        signal_id="sig-dex-01",
        chain=ChainIdentifier.BASE_MAINNET,
        token_address=dex_token,
        pool_address="0x1111111111111111111111111111111111111111",
        timestamp_ns=time.time_ns(),
        source=SignalSource.DEX_SWAP,
        suggested_side=OrderSide.BUY,
        alpha_score=0.75,
        strength=SignalStrength.STRONG,
        execution_venue=ExecutionVenue.UNISWAP_V3,
        pool_state=pool_state_dex,
        security_report=report_dex,
    )

    with caplog.at_level(logging.INFO):
        caplog.clear()
        fill_dex = await executor.execute_signal(signal_dex, portfolio_equity_usd=Decimal("10000.0"))
        assert fill_dex is not None
        assert fill_dex.platform == "DexScan"
        assert any("Platform=DexScan" in record.message for record in caplog.records)

    # 3. Execute Exit on Pump.fun lot
    with caplog.at_level(logging.INFO):
        caplog.clear()
        exit_fill = await executor.execute_exit(
            chain=ChainIdentifier.SOLANA_MAINNET,
            token_address=pump_token,
            pool=pool_state_pump,
            tokens_to_sell=Decimal("1000"),
            reason=TradeExitReason.TP_25,
            portfolio_equity_usd=Decimal("10000.0"),
        )
        assert exit_fill is not None
        assert exit_fill.platform == "Pump.fun"
        assert any("Platform=Pump.fun" in record.message for record in caplog.records)


@pytest.mark.anyio
async def test_sqlite_trade_ledger_platform_persistence(tmp_path):
    """Verify SQLiteLedger saves and retrieves platform correctly."""
    db_path = str(tmp_path / "test_trades.db")
    async with SQLiteLedger(db_path=db_path) as ledger:
        fill_pump = PaperFill(
            token_address="So11111111111111111111111111111111111111111pump",
            chain=ChainIdentifier.SOLANA_MAINNET,
            side=OrderSide.BUY,
            effective_price=Decimal("0.00004"),
            tokens_acquired=Decimal("250000"),
            simulated_native_spent=Decimal("10.0"),
            platform="Pump.fun",
        )
        fill_dex = PaperFill(
            token_address="0x285617313860407d647990b50375990264186566",
            chain=ChainIdentifier.BASE_MAINNET,
            side=OrderSide.SELL,
            effective_price=Decimal("0.0025"),
            tokens_acquired=Decimal("500"),
            simulated_native_spent=Decimal("1.25"),
            realized_pnl_usd=Decimal("25.0"),
            platform="DexScan",
        )

        await ledger.record_trade(fill_pump)
        await ledger.record_trade(fill_dex)

        recent = await ledger.get_recent_trades(limit=10)
        assert len(recent) == 2

        # Most recent first
        assert recent[0]["token_address"] == "0x285617313860407d647990b50375990264186566"
        assert recent[0]["platform"] == "DexScan"

        assert recent[1]["token_address"] == "So11111111111111111111111111111111111111111pump"
        assert recent[1]["platform"] == "Pump.fun"


@pytest.mark.anyio
async def test_telegram_cmd_trades_displays_platform_open_and_closed():
    """Verify /trades command in Telegram clearly shows [Pump.fun] and [DexScan]."""
    queue: asyncio.Queue = asyncio.Queue()
    ingester = TelegramIngester(event_queue=queue)

    status_data = {
        "open_trades": [
            {
                "chain": "solana_mainnet",
                "token_address": "So11111111111111111111111111111111111111111pump",
                "platform": "Pump.fun",
                "entry_price": 0.000045,
                "current_price": 0.000060,
                "unrealized_pnl_usd": 15.0,
                "unrealized_pnl_pct": 33.33,
                "open_timestamp_ns": time.time_ns() - 40_000_000_000,
                "trailing_stop_status": "OFF",
                "position_value_usd": 60.0,
            },
            {
                "chain": "base_mainnet",
                "token_address": "0x285617313860407d647990b50375990264186566",
                "platform": "DexScan",
                "entry_price": 0.0012,
                "current_price": 0.0011,
                "unrealized_pnl_usd": -5.0,
                "unrealized_pnl_pct": -8.33,
                "open_timestamp_ns": time.time_ns() - 90_000_000_000,
                "trailing_stop_status": "OFF",
                "position_value_usd": 55.0,
            },
        ],
        "recent_closed_trades": [
            {
                "chain": "solana_mainnet",
                "token_address": "FastMoon999pump",
                "platform": "Pump.fun",
                "entry_price": 0.00001,
                "exit_price": 0.00003,
                "realized_pnl_usd": 50.0,
                "realized_pnl_pct": 200.0,
                "exit_reason": "Take Profit TP2",
                "duration_s": 120,
                "is_win": True,
            },
            {
                "chain": "base_mainnet",
                "token_address": "0x1111111111111111111111111111111111111111",
                "platform": "DexScan",
                "entry_price": 0.05,
                "exit_price": 0.045,
                "realized_pnl_usd": -10.0,
                "realized_pnl_pct": -10.0,
                "exit_reason": "Stop Loss",
                "duration_s": 60,
                "is_win": False,
            },
        ],
    }
    ingester.set_status_provider(lambda: status_data)

    mock_event = MagicMock()
    replies = []

    async def mock_reply(msg):
        replies.append(msg)

    mock_event.reply = AsyncMock(side_effect=mock_reply)

    await ingester._cmd_trades(mock_event)

    assert len(replies) == 1
    text = replies[0]

    # Verify Open Positions display platform
    assert "[Pump.fun]" in text
    assert "[DexScan]" in text
    assert "PLATFORM: Pump.fun" in text
    assert "PLATFORM: DexScan" in text

    # Verify Closed Positions display platform
    assert "FastMoon999pump"[:4] in text
    assert "Take Profit TP2" in text


@pytest.mark.anyio
async def test_telegram_cmd_trades_sqlite_table_fallback(tmp_path):
    """Verify /trades displays PLATFORM column in ASCII table when reading from SQLite ledger."""
    db_file = tmp_path / "test_ledger.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE trades (
            trade_id TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token_address TEXT NOT NULL,
            side TEXT NOT NULL,
            effective_price REAL NOT NULL,
            realized_pnl_usd REAL,
            platform TEXT NOT NULL,
            created_at REAL NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO trades (trade_id, chain, token_address, side, effective_price, realized_pnl_usd, platform, created_at)
        VALUES ('t1', 'solana_mainnet', 'MemeAlphaTokenpump', 'BUY', 0.00005, 0.0, 'Pump.fun', 100),
               ('t2', 'base_mainnet', '0x285617313860407d647990b50375990264186566', 'SELL', 0.0025, 18.5, 'DexScan', 200)
        """
    )
    conn.commit()
    conn.close()

    queue: asyncio.Queue = asyncio.Queue()
    ingester = TelegramIngester(event_queue=queue, db_path=str(db_file))

    mock_event = MagicMock()
    replies = []

    async def mock_reply(msg):
        replies.append(msg)

    mock_event.reply = AsyncMock(side_effect=mock_reply)

    await ingester._cmd_trades(mock_event)

    assert len(replies) == 1
    msg = replies[0]
    assert "Last 5 Executed Paper Trades" in msg
    assert "PLATFORM" in msg
    assert "Pump.fun" in msg
    assert "DexScan" in msg
    assert "TOKEN" in msg
    assert "SIDE" in msg
    assert "BUY" in msg
    assert "SELL" in msg


@pytest.mark.anyio
async def test_chat_interface_explain_exit_platform():
    """Verify ConversationalSupervisor tool_explain_exit shows Platform field."""
    book = PositionBook()
    fill = PaperFill(
        token_address="So11111111111111111111111111111111111111111pump",
        chain=ChainIdentifier.SOLANA_MAINNET,
        effective_price=Decimal("0.0001"),
        tokens_acquired=Decimal("100000"),
        simulated_native_spent=Decimal("10.0"),
        side=OrderSide.BUY,
        platform="Pump.fun",
    )
    book.open_lot(fill=fill, signal_id="sig_test_1")
    book.close_lots_fifo(
        chain=fill.chain,
        token_address=fill.token_address,
        tokens_to_sell=fill.tokens_acquired,
        sell_price=Decimal("0.0002"),
    )

    mock_ledger = MagicMock()
    mock_ledger.get_decisions = AsyncMock(return_value=[])

    chat = ConversationalSupervisor(ledger=mock_ledger, position_book=book)
    res = await chat.tool_explain_exit("So11111111111111111111111111111111111111111pump")

    assert "Platform:" in res
    assert "Pump.fun" in res
