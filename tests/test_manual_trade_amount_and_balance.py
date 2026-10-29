"""
Tests for Manual Trade Amount Configuration, Telegram Interaction,
Persistence across Restarts, and Balance/Risk Protection.
"""

import asyncio
from decimal import Decimal
import json
import os
import time
import pytest
from unittest.mock import AsyncMock

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.ingestion.telegram import TelegramIngester, parse_trade_amount_input
from alpha_engine.models.enums import ChainIdentifier, OrderSide, SignalStrength, SignalSource, SecurityTier
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PaperFill, PoolState, SecurityReport


class DummyEvent:
    def __init__(self, text: str, sender_id: int = 12345, is_private: bool = True):
        self.raw_text = text
        self.text = text
        self.sender_id = sender_id
        self.is_private = is_private
        self.out = False
        self.replies: list[dict] = []
        self.answered = False
        self.alert_text = ""

    async def reply(self, msg: str, **kwargs):
        self.replies.append({"text": msg, "kwargs": kwargs})

    async def respond(self, msg: str, **kwargs):
        self.replies.append({"text": msg, "kwargs": kwargs})

    async def answer(self, text: str = "", alert: bool = False):
        self.answered = True
        self.alert_text = text


class DummyCallbackEvent(DummyEvent):
    def __init__(self, data: bytes, sender_id: int = 12345):
        super().__init__("", sender_id=sender_id, is_private=True)
        self.data = data


def test_parse_trade_amount_input():
    """Verify input validation and parsing for various currencies and formats."""
    # SOL formats
    amt, curr, err = parse_trade_amount_input("0.1")
    assert err is None
    assert amt == Decimal("0.1")
    assert curr == "SOL"

    amt, curr, err = parse_trade_amount_input("0.25 SOL")
    assert err is None
    assert amt == Decimal("0.25")
    assert curr == "SOL"

    amt, curr, err = parse_trade_amount_input("/setamount 0.5 solana")
    assert err is None
    assert amt == Decimal("0.5")
    assert curr == "SOL"

    # USD formats
    amt, curr, err = parse_trade_amount_input("$50")
    assert err is None
    assert amt == Decimal("50")
    assert curr == "USD"

    amt, curr, err = parse_trade_amount_input("100 USD")
    assert err is None
    assert amt == Decimal("100")
    assert curr == "USD"

    amt, curr, err = parse_trade_amount_input("25 دولار")
    assert err is None
    assert amt == Decimal("25")
    assert curr == "USD"

    # ETH formats
    amt, curr, err = parse_trade_amount_input("0.05 ETH")
    assert err is None
    assert amt == Decimal("0.05")
    assert curr == "ETH"

    # Rejection of zero or negative
    amt, curr, err = parse_trade_amount_input("0")
    assert amt is None
    assert err is not None
    assert "صفراً" in err or "zero" in err.lower()

    amt, curr, err = parse_trade_amount_input("-0.5 SOL")
    assert amt is None
    assert err is not None

    # Rejection of out-of-bounds values
    amt, curr, err = parse_trade_amount_input("0.0001 SOL")
    assert amt is None
    assert "الحد الأدنى" in err

    amt, curr, err = parse_trade_amount_input("1000 SOL")
    assert amt is None
    assert "الحد الأقصى" in err

    amt, curr, err = parse_trade_amount_input("$0.10 USD")
    assert amt is None
    assert "الحد الأدنى" in err

    # Non-numeric
    amt, curr, err = parse_trade_amount_input("invalid text")
    assert amt is None
    assert err is not None


@pytest.mark.anyio
async def test_telegram_setamount_interactive_prompt_and_cancel(tmp_path):
    """Verify /setamount prompts user with preset buttons, handles user follow-up input, and /cancel."""
    q = asyncio.Queue()
    ingester = TelegramIngester(event_queue=q, admin_ids=[12345], db_path=str(tmp_path / "test.db"))

    # 1. User sends /setamount without args -> triggers prompt and waiting state
    evt1 = DummyEvent("/setamount", sender_id=12345)
    await ingester._handle_dm_message(evt1)
    assert len(evt1.replies) == 1
    assert "تحديد مبلغ التداول" in evt1.replies[0]["text"]
    assert ingester._pending_user_states.get(12345) == "waiting_for_trade_amount"

    # 2. User cancels via /cancel
    evt_cancel = DummyEvent("/cancel", sender_id=12345)
    await ingester._handle_dm_message(evt_cancel)
    assert 12345 not in ingester._pending_user_states
    assert "إلغاء" in evt_cancel.replies[0]["text"]

    # 3. User sends /setamount again, then enters "0.15 SOL"
    evt2 = DummyEvent("/setamount", sender_id=12345)
    await ingester._handle_dm_message(evt2)
    assert ingester._pending_user_states.get(12345) == "waiting_for_trade_amount"

    evt_input = DummyEvent("0.15 SOL", sender_id=12345)
    await ingester._handle_dm_message(evt_input)
    assert 12345 not in ingester._pending_user_states
    assert ingester._manual_trade_amount == Decimal("0.15")
    assert ingester._manual_trade_currency == "SOL"
    assert "تم اعتماد وتثبيت مبلغ التداول بنجاح" in evt_input.replies[0]["text"]


@pytest.mark.anyio
async def test_telegram_direct_commands_and_callback_presets(tmp_path):
    """Verify direct /setamount with args, /getamount, /balance, and inline button presets."""
    q = asyncio.Queue()
    ingester = TelegramIngester(event_queue=q, admin_ids=[12345], db_path=str(tmp_path / "test.db"))

    # 1. Direct /setamount 0.2 SOL
    evt_set = DummyEvent("/setamount 0.2 SOL", sender_id=12345)
    await ingester._handle_dm_message(evt_set)
    assert ingester._manual_trade_amount == Decimal("0.2")
    assert ingester._manual_trade_currency == "SOL"
    assert "0.2 SOL" in evt_set.replies[0]["text"]

    # 2. Check /getamount
    evt_get = DummyEvent("/getamount", sender_id=12345)
    await ingester._handle_dm_message(evt_get)
    assert "0.2 SOL" in evt_get.replies[0]["text"]
    assert "🟢 محدد يدويًا" in evt_get.replies[0]["text"]

    # 3. Check /balance
    evt_bal = DummyEvent("/balance", sender_id=12345)
    await ingester._handle_dm_message(evt_bal)
    assert "تقرير الرصيد والمحفظة" in evt_bal.replies[0]["text"]

    # 4. Inline callback preset: amt:set_0.5_SOL
    evt_cb = DummyCallbackEvent(b"amt:set_0.5_SOL", sender_id=12345)
    await ingester._handle_callback_query(evt_cb)
    assert ingester._manual_trade_amount == Decimal("0.5")
    assert ingester._manual_trade_currency == "SOL"

    # 5. Inline callback preset: amt:set_50_USD
    evt_cb2 = DummyCallbackEvent(b"amt:set_50_USD", sender_id=12345)
    await ingester._handle_callback_query(evt_cb2)
    assert ingester._manual_trade_amount == Decimal("50")
    assert ingester._manual_trade_currency == "USD"


@pytest.mark.anyio
async def test_trade_amount_persistence_across_restarts(tmp_path):
    """Verify that manual trade amount is saved to JSON file & SQLite, and reloaded on restart."""
    db_file = tmp_path / "paper_trading.db"
    settings_file = tmp_path / "bot_runtime_settings.json"

    # Pass db_file and initialize runner
    cfg = EngineConfig(db_path=str(db_file))
    runner = PaperTradingEngine(config=cfg)
    runner._ledger = SQLiteLedger(db_path=str(db_file))
    await runner._ledger.initialize()

    # Set manual trade amount
    res = runner.set_manual_trade_amount(Decimal("0.35"), "SOL")
    assert res["manual_trade_amount"] == "0.35"
    assert res["manual_trade_currency"] == "SOL"

    # Wait briefly for SQLite task if any
    await asyncio.sleep(0.05)

    # Check that settings file was written
    assert os.path.exists(settings_file)
    with open(settings_file, "r") as f:
        data = json.load(f)
    assert data["manual_trade_amount"] == "0.35"
    assert data["manual_trade_currency"] == "SOL"

    # Check ledger SQLite table
    db_amt = await runner._ledger.get_setting("manual_trade_amount")
    assert db_amt == "0.35"
    db_curr = await runner._ledger.get_setting("manual_trade_currency")
    assert db_curr == "SOL"

    # Simulate restart: create a new runner instance on the same path
    runner2 = PaperTradingEngine(config=cfg)
    assert runner2.manual_trade_amount == Decimal("0.35")
    assert runner2.manual_trade_currency == "SOL"

    # Check get_manual_trade_amount returns restored values
    status = runner2.get_manual_trade_amount()
    assert status["manual_trade_amount"] == "0.35"
    assert status["manual_trade_currency"] == "SOL"


@pytest.mark.anyio
async def test_manual_trade_amount_sizing_in_pipeline(tmp_path):
    """Verify signal processing uses the exact manual trade amount instead of Kelly sizing."""
    db_file = tmp_path / "paper_trading.db"
    cfg = EngineConfig(
        db_path=str(db_file),
        auto_trade_enabled=True,
        ai_supervisor_enabled=False,
        manual_trade_amount=Decimal("0.25"),
        manual_trade_currency="SOL",
    )
    runner = PaperTradingEngine(config=cfg)
    runner._ledger = SQLiteLedger(db_path=str(db_file))
    await runner._ledger.initialize()
    runner._position_book = PositionBook()
    runner._position_book.cash_balance = Decimal("100.0")
    runner._executor = PaperExecutor(
        position_book=runner._position_book,
        native_price_usd=Decimal("150.0"),
    )

    # Mock execute_signal to observe fixed_trade_amount_native
    mock_fill = PaperFill(
        order_id="ord_1",
        token_address="TokenABC123456789012345678901234567890123",
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("0.25"),
        tokens_acquired=Decimal("2500"),
        effective_price=Decimal("0.0001"),
    )
    runner._executor.execute_signal = AsyncMock(return_value=mock_fill)

    pool_state = PoolState(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolABC111111111111111111111111111111111111",
        token_reserve=Decimal("1000000000"),
        native_reserve=Decimal("100"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=100000,
        token_address="TokenABC123456789012345678901234567890123",
    )
    sec_rep = SecurityReport(
        token_address="TokenABC123456789012345678901234567890123",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    sig = SignalEvent(
        signal_id="sig_test_1",
        timestamp_ns=int(time.time() * 1e9),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_state.pool_address,
        token_address="TokenABC123456789012345678901234567890123",
        suggested_side=OrderSide.BUY,
        pool_state=pool_state,
        security_report=sec_rep,
        strength=SignalStrength.STRONG,
        alpha_score=0.90,
        source=SignalSource.RAYDIUM_NEW_POOL,
        market_cap_usd=Decimal("25000.0"),
    )

    await runner._signal_q.put(sig)

    # Run queue processing for one tick
    task = asyncio.create_task(runner._process_signal_queue())
    for _ in range(120):
        if runner._executor.execute_signal.called:
            break
        await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify executor was called with fixed_trade_amount_native=0.25
    runner._executor.execute_signal.assert_called_once()
    call_kwargs = runner._executor.execute_signal.call_args.kwargs
    assert call_kwargs.get("fixed_trade_amount_native") == Decimal("0.25")


@pytest.mark.anyio
async def test_insufficient_balance_prevents_execution_and_alerts(tmp_path):
    """Verify that when available balance is less than trade amount + fees, the trade is rejected and alerted."""
    db_file = tmp_path / "paper_trading.db"
    cfg = EngineConfig(
        db_path=str(db_file),
        auto_trade_enabled=True,
        manual_trade_amount=Decimal("10.0"),  # User requested 10 SOL
        manual_trade_currency="SOL",
    )
    runner = PaperTradingEngine(config=cfg)
    runner._ledger = SQLiteLedger(db_path=str(db_file))
    await runner._ledger.initialize()
    runner._position_book = PositionBook()
    # Mock book to have only 1.0 SOL cash (insufficient for 10.0 SOL trade)
    runner._position_book.cash_balance = Decimal("1.0")
    runner._executor = PaperExecutor(
        position_book=runner._position_book,
        native_price_usd=Decimal("150.0"),
    )
    runner._executor.execute_signal = AsyncMock()

    # Setup telegram ingester spy
    ingester = TelegramIngester(event_queue=asyncio.Queue(), admin_ids=[12345], db_path=str(db_file))
    ingester.broadcast_trade_alert = AsyncMock()
    runner._telegram = ingester

    pool_state = PoolState(
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolDEF111111111111111111111111111111111111",
        token_reserve=Decimal("1000000000"),
        native_reserve=Decimal("100"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=100000,
        token_address="TokenDEF123456789012345678901234567890123",
    )
    sec_rep = SecurityReport(
        token_address="TokenDEF123456789012345678901234567890123",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    sig = SignalEvent(
        signal_id="sig_insufficient",
        timestamp_ns=int(time.time() * 1e9),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_state.pool_address,
        token_address="TokenDEF123456789012345678901234567890123",
        suggested_side=OrderSide.BUY,
        pool_state=pool_state,
        security_report=sec_rep,
        strength=SignalStrength.STRONG,
        alpha_score=0.90,
        source=SignalSource.RAYDIUM_NEW_POOL,
        market_cap_usd=Decimal("30000.0"),
    )

    await runner._signal_q.put(sig)

    task = asyncio.create_task(runner._process_signal_queue())
    await asyncio.sleep(0.08)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify execution was NOT called (no auto-scaling or forced trade)
    runner._executor.execute_signal.assert_not_called()

    # Verify broadcast alert was sent to Telegram
    ingester.broadcast_trade_alert.assert_called_once()
    alert_msg = ingester.broadcast_trade_alert.call_args[0][0]
    assert "رصيد غير كافٍ" in alert_msg or "INSUFFICIENT BALANCE" in alert_msg
    assert "10.0" in alert_msg
