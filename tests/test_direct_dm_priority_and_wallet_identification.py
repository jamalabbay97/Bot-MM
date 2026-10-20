"""
tests.test_direct_dm_priority_and_wallet_identification
========================================================
Comprehensive test suite verifying:
1. Disambiguation between Token Smart Contracts and Wallet Addresses.
2. Direct Telegram DM Priority: Bypassing the minimum market cap threshold.
3. Live wallet tracking on-chain activity and terminal formatting.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.models.enums import ChainIdentifier, OrderSide, SecurityTier, SignalSource, SignalStrength
from alpha_engine.models.events import RawSignalEvent, SecurityReport, SignalEvent
from alpha_engine.models.state import PoolState
from alpha_engine.tracker.wallet_tracker import WalletActivityData, WalletTracker, WalletTradePoint
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.config import EngineConfig


@pytest.mark.anyio
async def test_detect_address_type_disambiguation(tmp_path):
    """
    Verify that detect_address_type accurately distinguishes Token Mints from Wallets.
    """
    db_file = tmp_path / "test_tracker.db"
    tracker = WalletTracker(db_path=str(db_file))

    # 1. Pump.fun token ending in 'pump'
    pump_token = "5ATvVADHKmxcouozCsgugCU8qvDyXkkjRVSQ3sTypump"
    res1 = await tracker.detect_address_type(pump_token)
    assert res1 == "TOKEN"

    # 2. Known Solana wallet address (System Program account)
    sol_wallet = "Dde9n8s2NPiL9Wd99NftqDtbwRrsPkyxxKDAJeRhCXoJ"
    res2 = await tracker.detect_address_type(sol_wallet)
    assert res2 == "WALLET"

    # 3. Base EOA wallet (returns 0x bytecode)
    base_wallet = "0x71C7656EC7ab88b098defB751B7401B5f6d8976F"
    res3 = await tracker.detect_address_type(base_wallet)
    assert res3 == "WALLET"


@pytest.mark.anyio
async def test_wallet_tracker_live_formatting(tmp_path):
    """
    Verify format_wallet_activity_message correctly displays on-chain balance and explorer links.
    """
    db_file = tmp_path / "test_tracker.db"
    tracker = WalletTracker(db_path=str(db_file))

    sol_wallet = "Dde9n8s2NPiL9Wd99NftqDtbwRrsPkyxxKDAJeRhCXoJ"
    activity = await tracker.track_wallet(sol_wallet)

    assert isinstance(activity, WalletActivityData)
    assert activity.wallet_address == sol_wallet
    assert "solscan.io/account" in activity.explorer_url or "basescan.org/address" in activity.explorer_url

    msg = tracker.format_wallet_activity_message(activity)
    assert sol_wallet in msg
    assert "الرصيد الحي على السلسلة" in msg
    assert "تقرير نشاط المحفظة" in msg


@pytest.mark.anyio
async def test_direct_dm_bypasses_market_cap_threshold(tmp_path):
    """
    Verify that an incoming Direct DM signal with market cap < $10,000
    is NOT dropped by the market cap check, while a non-DM signal with the same low mcap is dropped.
    """
    cfg = EngineConfig()
    cfg.min_market_cap_usd = Decimal("10000.0")

    cfg.ai_supervisor_enabled = False

    engine = PaperTradingEngine(config=cfg)
    engine._eth_price = Decimal("2600.0")
    engine._sol_price = Decimal("150.0")
    engine._cash_sol = Decimal("10.0")
    engine._position_book.cash_balance = Decimal("1000.0")

    token_addr = "5ATvVADHKmxcouozCsgugCU8qvDyXkkjRVSQ3sTypump"
    pool_state = PoolState(
        pool_address="BondingCurveMockAddress111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1073000000.0"),
        native_reserve=Decimal("28.0"),
        fee_numerator=3000,
        fee_denominator=1000000,
        last_updated_block=123456,
        token_decimals=6,
        native_decimals=9,
        token_address=token_addr,
    )
    # Calculated market cap = 28.0 SOL * $150 = $4,200 (well below $10,000 threshold)
    calculated_mcap = pool_state.calculate_market_cap_usd(native_price_usd=Decimal("150.0"))
    assert calculated_mcap < Decimal("10000.0")

    sec_report = SecurityReport(
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        is_rug=False,
        score=100.0,
        flags=[],
        buy_tax_bps=0,
        sell_tax_bps=0,
        checked_at_ts=int(time.time()),
        passes_hard_gates=True,
    )

    # 1. Non-DM signal: Should be dropped due to Market Cap < $10,000
    non_dm_signal = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_state.pool_address,
        token_address=token_addr,
        suggested_side=OrderSide.BUY,
        pool_state=pool_state,
        security_report=sec_report,
        strength=SignalStrength.STRONG,
        alpha_score=0.85,
        source=SignalSource.PUMP_FUN_MINT,
        is_direct_dm=False,
    )

    # 2. Direct DM signal: Should BYPASS the market cap check
    dm_signal = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_state.pool_address,
        token_address=token_addr,
        suggested_side=OrderSide.BUY,
        pool_state=pool_state,
        security_report=sec_report,
        strength=SignalStrength.STRONG,
        alpha_score=0.85,
        source=SignalSource.TELEGRAM_SCRAPER,
        originating_channel="DM:7977541477",
        is_direct_dm=True,
    )

    # Mock dependencies
    from alpha_engine.models.state import PaperFill
    mock_fill = PaperFill(
        order_id="ord_dm_1",
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("0.1"),
        tokens_acquired=Decimal("1000"),
        effective_price=Decimal("0.0001"),
    )
    engine._executor = AsyncMock()
    engine._executor.execute_signal = AsyncMock(return_value=mock_fill)
    engine._ledger = AsyncMock()
    engine._ledger.record_trade = AsyncMock()

    processor_task = asyncio.create_task(engine._process_signal_queue())

    # 1. Feed non_dm_signal to runner's queue: Should be dropped due to MCAP < $10,000
    await engine._signal_q.put(non_dm_signal)
    await asyncio.sleep(0.05)

    assert engine._executor.execute_signal.call_count == 0

    # 2. Feed Direct DM signal: Should bypass MCAP threshold and execute
    await engine._signal_q.put(dm_signal)
    await asyncio.sleep(0.05)

    assert engine._executor.execute_signal.call_count == 1

    # Clean up processor
    processor_task.cancel()
    try:
        await processor_task
    except asyncio.CancelledError:
        pass


class DummyEvent:
    def __init__(self, text: str, sender_id: int = 12345):
        self.text = text
        self.raw_text = text
        self.sender_id = sender_id
        self.is_private = True
        self.out = False
        self.replies: list[dict[str, Any]] = []

    async def reply(self, message: str, **kwargs: Any) -> Any:
        self.replies.append({"text": message, "kwargs": kwargs})
        return self


class DummyCallbackEvent:
    def __init__(self, data: bytes, sender_id: int = 12345):
        self.data = data
        self.sender_id = sender_id
        self.is_private = True
        self.out = False
        self.edits: list[dict[str, Any]] = []
        self.replies: list[dict[str, Any]] = []

    async def edit(self, message: str, **kwargs: Any) -> Any:
        self.edits.append({"text": message, "kwargs": kwargs})
        return self

    async def reply(self, message: str, **kwargs: Any) -> Any:
        self.replies.append({"text": message, "kwargs": kwargs})
        return self

    async def answer(self, *args: Any, **kwargs: Any) -> Any:
        pass


@pytest.mark.anyio
async def test_manual_enter_and_exit_position_integration(tmp_path):
    """
    Verify manual entry (/buy, tok:buy) and manual exit (/sell, tok:sell, trade:close)
    allow users to directly enter trades and exit them from Telegram.
    """
    from alpha_engine.ingestion.telegram import TelegramIngester

    db_file = tmp_path / "test_engine.db"
    cfg = EngineConfig(db_path=str(db_file), auto_trade_enabled=True)
    engine = PaperTradingEngine(config=cfg)
    engine._eth_price = Decimal("2600.0")
    engine._sol_price = Decimal("150.0")
    engine._cash_sol = Decimal("10.0")
    engine._position_book.cash_balance = Decimal("1000.0")

    token_addr = "7F4DGZr5cygRf7tDtXyqA6QeFn4KpzWXsRQWzEojpump"

    # Set up Telegram Ingester connected to engine
    ingester = TelegramIngester(event_queue=asyncio.Queue(), admin_ids=[12345], db_path=str(db_file))
    ingester.set_execution_target(engine)

    # 1. Test manual enter position on engine
    ok, msg, sig = await engine.manual_enter_position(token_addr, originating_channel="DM:12345")
    assert ok is True
    assert "تم إرسال أمر الشراء" in msg
    assert sig is not None
    assert sig.is_direct_dm is True

    # 2. Simulate opening a lot in position book
    from alpha_engine.models.state import PaperFill
    fill = PaperFill(
        order_id="ord_test_open",
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("0.5"),
        tokens_acquired=Decimal("5000"),
        effective_price=Decimal("0.0001"),
    )
    lot = engine._position_book.open_lot(fill, "sig_manual_1")
    assert engine.is_position_open(token_addr) is True

    # 3. Test token page detects open position and shows Sell/Exit button
    tok_event = DummyEvent(f"/token {token_addr}", sender_id=12345)
    await ingester._handle_dm_message(tok_event)
    assert len(tok_event.replies) >= 1
    buttons = tok_event.replies[0]["kwargs"].get("buttons")
    assert buttons is not None
    # Row 1[0] should be Sell/Exit button
    sell_btn = buttons[1][0]
    assert b"tok:sell:" in sell_btn.data
    assert "خروج" in sell_btn.text or "بيع" in sell_btn.text

    # 4. Mock executor exit
    exit_fill = PaperFill(
        order_id="ord_test_exit",
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.SELL,
        simulated_native_spent=Decimal("0.75"), # +0.25 SOL profit
        tokens_acquired=Decimal("5000"),
        effective_price=Decimal("0.00015"),
    )
    engine._executor = AsyncMock()
    engine._executor.execute_exit = AsyncMock(return_value=exit_fill)

    # 5. Trigger manual sell via callback (tok:sell)
    cb_sell = DummyCallbackEvent(data=f"tok:sell:{token_addr}".encode("utf-8"), sender_id=12345)
    await ingester._handle_callback_query(cb_sell)
    assert len(cb_sell.replies) >= 1
    assert "تم بيع وإغلاق الصفقة بنجاح" in cb_sell.replies[0]["text"]
    assert engine.is_position_open(token_addr) is False

    # 6. Verify /sell command directly
    # Re-open lot for test
    engine._position_book.open_lot(fill, "sig_manual_2")
    assert engine.is_position_open(token_addr) is True
    engine._ledger = AsyncMock()
    sell_event = DummyEvent(f"/sell {token_addr}", sender_id=12345)
    await ingester._handle_dm_message(sell_event)
    assert len(sell_event.replies) >= 1
    assert "تم بيع وإغلاق الصفقة بنجاح" in sell_event.replies[0]["text"]
    assert engine.is_position_open(token_addr) is False

    # Verify ledger recorded trade using a TradeRecord
    assert engine._ledger.record_trade.called
    recorded_arg = engine._ledger.record_trade.call_args[0][0]
    from alpha_engine.models.state import TradeRecord
    assert isinstance(recorded_arg, TradeRecord)
    assert recorded_arg.realized_pnl_usd is not None

    # 7. Test custom amount_native sizing in manual_enter_position and Telegram /buy command
    custom_tok = "9ZNTfG4Nyftzy2gcvZwoNoNoW54gWkWv3r7nm2cqpump"
    ok_amt, msg_amt, sig_amt = await engine.manual_enter_position(
        custom_tok,
        amount_native=Decimal("0.25"),
        originating_channel="DM:12345",
    )
    assert ok_amt is True
    assert sig_amt is not None
    assert sig_amt.custom_amount_native == Decimal("0.25")

    # Test insufficient balance rejection in manual_enter_position
    ok_excess, msg_excess, _ = await engine.manual_enter_position(
        "8ZNTfG4Nyftzy2gcvZwoNoNoW54gWkWv3r7nm2cqpump",
        amount_native=Decimal("99999.0"),
        originating_channel="DM:12345",
    )
    assert ok_excess is False
    assert "رصيد غير كافٍ" in msg_excess

    # Test Telegram /buy with custom amount
    buy_event = DummyEvent(f"/buy {custom_tok}2 0.35", sender_id=12345)
    await ingester._handle_dm_message(buy_event)
    assert len(buy_event.replies) >= 2
    reply_text = "\n".join(r["text"] for r in buy_event.replies)
    assert "0.35" in reply_text


