"""
Tests for Token Live Page Capture, Token Notifications & Privacy,
Wallet Activity Tracking (Entries, Exits, Exit Strategy), and Copy-Trading Management.
"""

import asyncio
from decimal import Decimal
import time
import pytest
from unittest.mock import AsyncMock, patch

from alpha_engine.ingestion.telegram import TelegramIngester
from alpha_engine.tracker.wallet_tracker import (
    WalletTracker,
    get_wallet_tracker,
    TokenPageData,
    WalletActivityData,
    WalletTradePoint,
)


class DummyEvent:
    def __init__(self, text: str, sender_id: int = 12345, is_private: bool = True):
        self.raw_text = text
        self.text = text
        self.sender_id = sender_id
        self.is_private = is_private
        self.out = False
        self.replies: list[dict] = []
        self.edits: list[dict] = []
        self.deleted = False
        self.answered = False
        self.alert_text = ""

    async def reply(self, msg: str, **kwargs):
        self.replies.append({"text": msg, "kwargs": kwargs})

    async def respond(self, msg: str, **kwargs):
        self.replies.append({"text": msg, "kwargs": kwargs})

    async def edit(self, msg: str, **kwargs):
        self.edits.append({"text": msg, "kwargs": kwargs})

    async def delete(self):
        self.deleted = True

    async def answer(self, text: str = "", alert: bool = False):
        self.answered = True
        self.alert_text = text


class DummyCallbackEvent(DummyEvent):
    def __init__(self, data: bytes, sender_id: int = 12345):
        super().__init__("", sender_id=sender_id, is_private=True)
        self.data = data


@pytest.fixture
def test_tracker(tmp_path):
    db_file = tmp_path / "test_tracker.db"
    return WalletTracker(db_path=str(db_file))


@pytest.mark.anyio
async def test_token_page_fetch_and_format(test_tracker):
    """
    Test 1: Verify token page data fetching, formatting, and live metrics.
    Live price in USD & native, changes, liquidity, market cap, security audit.
    """
    token_ca = "7F4DGZr5cygRf7tDtXyqA6QeFn4KpzWXsRQWzEojpump"
    page = await test_tracker.fetch_token_page(token_ca, user_id=12345)

    assert isinstance(page, TokenPageData)
    assert page.token_address == token_ca
    assert page.price_usd > Decimal(0)
    assert page.market_cap_usd > Decimal(0)
    assert page.liquidity_usd > Decimal(0)
    assert page.volume_24h_usd > Decimal(0)
    assert page.is_honeypot is False
    assert token_ca in page.to_json()

    # Format public view
    msg_public = test_tracker.format_token_page_message(page, is_private=False, notif_enabled=True)
    assert "صفحة العملة اللحظية" in msg_public or "TOKEN TERMINAL" in msg_public
    assert "🔔 التنبيهات: مفعّلة" in msg_public
    assert "🌐 الوضع: عام" in msg_public
    assert "القيمة السوقية (Market Cap):" in msg_public
    assert "السيولة (Liquidity):" in msg_public
    assert "الفحص الأمني (Security Audit):" in msg_public
    assert "Honeypot" in msg_public

    # Format private view
    msg_private = test_tracker.format_token_page_message(page, is_private=True, notif_enabled=True)
    assert "🔒 الوضع: خاص (Private - Sensitive Info Hidden)" in msg_private
    assert "محجوب" in msg_private or "خاص" in msg_private


@pytest.mark.anyio
async def test_token_auto_notifications_and_privacy_toggle(test_tracker):
    """
    Test 2: Verify automatic notification subscription upon token entry,
    and toggling notifications & privacy settings.
    """
    token_ca = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    user_id = 998877

    # Fetching page auto-subscribes with notifications_enabled=True and is_private=False
    await test_tracker.fetch_token_page(token_ca, user_id=user_id)
    is_sub, notif_on, is_priv = await test_tracker.get_token_subscription(token_ca, user_id=user_id)
    assert is_sub is True
    assert notif_on is True
    assert is_priv is False

    # Toggle privacy to True
    new_priv = await test_tracker.toggle_token_privacy(token_ca, user_id=user_id)
    assert new_priv is True
    _, _, is_priv2 = await test_tracker.get_token_subscription(token_ca, user_id=user_id)
    assert is_priv2 is True

    # Toggle privacy back to False
    new_priv2 = await test_tracker.toggle_token_privacy(token_ca, user_id=user_id)
    assert new_priv2 is False

    # Toggle notifications to False
    new_notif = await test_tracker.toggle_token_notifications(token_ca, user_id=user_id)
    assert new_notif is False
    _, notif_on2, _ = await test_tracker.get_token_subscription(token_ca, user_id=user_id)
    assert notif_on2 is False

    # Toggle notifications back to True
    new_notif2 = await test_tracker.toggle_token_notifications(token_ca, user_id=user_id)
    assert new_notif2 is True


@pytest.mark.anyio
async def test_wallet_detection_and_activity_tracking(test_tracker):
    """
    Test 3: Verify wallet detection, activity tracking with entry/exit points,
    and exit strategy classification.
    """
    wallet_addr = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"
    user_id = 554433

    # Detect address type
    addr_type = await test_tracker.detect_address_type(wallet_addr)
    # Since DexScreener has no pair for this wallet and it doesn't end in 'pump', it resolves as WALLET
    assert addr_type in ("WALLET", "TOKEN")

    # Track wallet activity
    activity = await test_tracker.track_wallet(wallet_addr, user_id=user_id)
    assert isinstance(activity, WalletActivityData)
    assert activity.address == wallet_addr
    assert activity.total_trades > 0
    assert activity.win_rate_pct >= 0.0
    assert len(activity.entry_points) > 0
    assert len(activity.exit_points) > 0
    assert activity.detected_strategy != ""
    assert wallet_addr in activity.to_json()

    # Format wallet terminal message
    msg = test_tracker.format_wallet_activity_message(activity)
    assert "تقرير نشاط المحفظة" in msg or "WALLET TRACKER" in msg
    assert "نسبة الفوز (Win Rate):" in msg
    assert "استراتيجية الخروج المصنفة:" in msg
    assert "نقاط الدخول الأخيرة (Entry Points):" in msg
    assert "نقاط الخروج وجني الأرباح (Exit Points):" in msg


@pytest.mark.anyio
async def test_exit_strategy_classifier(test_tracker):
    """
    Test 4: Verify exit strategy classification heuristics.
    """
    # 1. Scalper: holds < 300s
    t_now = time.time()
    scalp_entries = [WalletTradePoint("buy", "Tok1", Decimal("0.001"), Decimal("1000"), t_now - 120)]
    scalp_exits = [WalletTradePoint("sell", "Tok1", Decimal("0.0012"), Decimal("1000"), t_now, pnl_pct=20.0)]
    strat_scalp = test_tracker._classify_exit_strategy(scalp_entries, scalp_exits)
    assert "Scalp" in strat_scalp

    # 2. Moonbag: holds a runner (bought 10,000, sold 8,000)
    moon_entries = [WalletTradePoint("buy", "Tok2", Decimal("0.001"), Decimal("10000"), t_now - 7200)]
    moon_exits = [WalletTradePoint("sell", "Tok2", Decimal("0.005"), Decimal("8000"), t_now, pnl_pct=400.0)]
    strat_moon = test_tracker._classify_exit_strategy(moon_entries, moon_exits)
    assert "Moonbag" in strat_moon

    # 3. Trailing Stop: exit on negative ROI
    trail_entries = [WalletTradePoint("buy", "Tok3", Decimal("0.001"), Decimal("1000"), t_now - 1800)]
    trail_exits = [WalletTradePoint("sell", "Tok3", Decimal("0.00085"), Decimal("1000"), t_now, pnl_pct=-15.0)]
    strat_trail = test_tracker._classify_exit_strategy(trail_entries, trail_exits)
    assert "Trailing Stop" in strat_trail or "Retrace" in strat_trail

    # 4. Diamond Hold: entries without exits
    hold_entries = [WalletTradePoint("buy", "Tok4", Decimal("0.001"), Decimal("5000"), t_now - 3600)]
    strat_hold = test_tracker._classify_exit_strategy(hold_entries, [])
    assert "Diamond Hold" in strat_hold


@pytest.mark.anyio
async def test_copy_trading_management_and_untrack(test_tracker):
    """
    Test 5: Verify enabling/disabling copy trading for specific wallets,
    untracking/removing wallets, and global copy-trading switch.
    """
    wallet1 = "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"
    wallet2 = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
    user_id = 112233

    # Add wallets
    await test_tracker.track_wallet(wallet1, user_id=user_id)
    await test_tracker.track_wallet(wallet2, user_id=user_id)

    wallets = await test_tracker.list_tracked_wallets(user_id=user_id)
    assert len(wallets) >= 2

    # Enable copy trading on wallet1
    ok = await test_tracker.set_wallet_copy_trading(wallet1, enabled=True, user_id=user_id)
    assert ok is True
    wallets_after = await test_tracker.list_tracked_wallets(user_id=user_id)
    w1_data = next(w for w in wallets_after if w["address"] == wallet1)
    assert w1_data["copy_trading_enabled"] == 1

    # Stop copy trading on wallet1
    ok_stop = await test_tracker.stop_wallet_copy_trading(wallet1, user_id=user_id)
    assert ok_stop is True
    wallets_after_stop = await test_tracker.list_tracked_wallets(user_id=user_id)
    w1_stop_data = next(w for w in wallets_after_stop if w["address"] == wallet1)
    assert w1_stop_data["copy_trading_enabled"] == 0

    # Remove/untrack wallet2
    ok_untrack = await test_tracker.untrack_wallet(wallet2, user_id=user_id)
    assert ok_untrack is True
    wallets_after_untrack = await test_tracker.list_tracked_wallets(user_id=user_id)
    assert not any(w["address"] == wallet2 for w in wallets_after_untrack)

    # Global copy trading toggle
    current_global = await test_tracker.is_global_copy_trading_enabled()
    new_global = await test_tracker.toggle_global_copy_trading()
    assert new_global != current_global
    is_glob = await test_tracker.is_global_copy_trading_enabled()
    assert is_glob == new_global


@pytest.mark.anyio
async def test_telegram_token_page_direct_entry_and_callbacks(tmp_path):
    """
    Test 6: Verify Telegram ingester direct CA entry captures token page,
    enables notifications, and responds to inline buttons (priv, notif, close).
    """
    db_file = tmp_path / "tg_tracker.db"
    ingester = TelegramIngester(event_queue=asyncio.Queue(), admin_ids=[12345], db_path=str(db_file))
    token_ca = "7F4DGZr5cygRf7tDtXyqA6QeFn4KpzWXsRQWzEojpump"

    # User enters token smart contract address directly
    event = DummyEvent(token_ca, sender_id=12345)
    await ingester._handle_dm_message(event)

    # Verify bot captured and displayed the token page immediately
    assert len(event.replies) >= 1
    reply = event.replies[0]
    reply_text = reply["text"]
    assert "صفحة العملة اللحظية" in reply_text or "TOKEN TERMINAL" in reply_text
    assert "القيمة السوقية" in reply_text
    assert "Honeypot" in reply_text

    # Verify buttons are present: Private toggle, Notifications toggle, Refresh, Close
    buttons = reply["kwargs"].get("buttons")
    assert buttons is not None
    # Check button data
    priv_btn = buttons[0][0]
    notif_btn = buttons[0][1]
    close_btn = buttons[2][0]
    assert b"tok:priv:" in priv_btn.data
    assert b"tok:notif:" in notif_btn.data
    assert b"tok:close:" in close_btn.data

    # Callback: Toggle privacy
    cb_priv = DummyCallbackEvent(data=f"tok:priv:{token_ca}".encode("utf-8"), sender_id=12345)
    await ingester._handle_callback_query(cb_priv)
    assert len(cb_priv.edits) >= 1
    assert "🔒 الوضع: خاص" in cb_priv.edits[0]["text"]

    # Callback: Toggle notification
    cb_notif = DummyCallbackEvent(data=f"tok:notif:{token_ca}".encode("utf-8"), sender_id=12345)
    await ingester._handle_callback_query(cb_notif)
    assert len(cb_notif.edits) >= 1
    assert "🔕 التنبيهات: معطّلة" in cb_notif.edits[0]["text"]

    # Callback: Close page
    cb_close = DummyCallbackEvent(data=f"tok:close:{token_ca}".encode("utf-8"), sender_id=12345)
    await ingester._handle_callback_query(cb_close)
    assert cb_close.deleted or len(cb_close.edits) >= 1


@pytest.mark.anyio
async def test_telegram_wallet_tracking_and_copytrade_commands(tmp_path):
    """
    Test 7: Verify /wallet, /track, /copytrade, /stopcopy, /untrack, /alerts commands in Telegram.
    """
    db_file = tmp_path / "tg_tracker.db"
    ingester = TelegramIngester(event_queue=asyncio.Queue(), admin_ids=[12345], db_path=str(db_file))
    wallet_addr = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"

    # 1. /track <WALLET>
    ev_track = DummyEvent(f"/track {wallet_addr}", sender_id=12345)
    await ingester._handle_dm_message(ev_track)
    assert len(ev_track.replies) >= 1
    assert "تقرير نشاط المحفظة" in ev_track.replies[0]["text"] or "WALLET TRACKER" in ev_track.replies[0]["text"]
    assert "استراتيجية الخروج المصنفة:" in ev_track.replies[0]["text"]

    # 2. Callback: Start copy trading
    cb_copy = DummyCallbackEvent(data=f"wal:copy:{wallet_addr}".encode("utf-8"), sender_id=12345)
    await ingester._handle_callback_query(cb_copy)
    assert len(cb_copy.edits) >= 1
    assert "🟢 نعم (مفعّل)" in cb_copy.edits[0]["text"]

    # 3. /copytrade dashboard
    ev_cpt = DummyEvent("/copytrade", sender_id=12345)
    await ingester._handle_dm_message(ev_cpt)
    assert len(ev_cpt.replies) >= 1
    assert "لوحة التحكم بنسخ الصفقات" in ev_cpt.replies[0]["text"]
    assert "المحافظ المفعل عليها النسخ:" in ev_cpt.replies[0]["text"]

    # 4. /stopcopy <WALLET>
    ev_stop = DummyEvent(f"/stopcopy {wallet_addr}", sender_id=12345)
    await ingester._handle_dm_message(ev_stop)
    assert len(ev_stop.replies) >= 1
    assert "تم إيقاف نسخ الصفقات بنجاح" in ev_stop.replies[0]["text"]

    # 5. /untrack <WALLET>
    ev_untrack = DummyEvent(f"/untrack {wallet_addr}", sender_id=12345)
    await ingester._handle_dm_message(ev_untrack)
    assert len(ev_untrack.replies) >= 1
    assert "تمت إزالة المحفظة بنجاح" in ev_untrack.replies[0]["text"]

    # 6. /alerts & /mute & /unmute
    token_ca = "7F4DGZr5cygRf7tDtXyqA6QeFn4KpzWXsRQWzEojpump"
    ev_tok = DummyEvent(f"/token {token_ca}", sender_id=12345)
    await ingester._handle_dm_message(ev_tok)

    ev_alerts = DummyEvent("/alerts", sender_id=12345)
    await ingester._handle_dm_message(ev_alerts)
    assert len(ev_alerts.replies) >= 1
    assert "إدارة إشعارات وتنبيهات العملات" in ev_alerts.replies[0]["text"]

    ev_mute = DummyEvent(f"/mute {token_ca}", sender_id=12345)
    await ingester._handle_dm_message(ev_mute)
    assert len(ev_mute.replies) >= 1
    assert "تم كتم إشعارات العملة بنجاح" in ev_mute.replies[0]["text"]

    ev_unmute = DummyEvent(f"/unmute {token_ca}", sender_id=12345)
    await ingester._handle_dm_message(ev_unmute)
    assert len(ev_unmute.replies) >= 1
    assert "تم تفعيل جميع إشعارات العملة بنجاح" in ev_unmute.replies[0]["text"]


def test_get_wallet_tracker_singleton():
    """
    Test get_wallet_tracker: verifies that calling get_wallet_tracker()
    returns the singleton instance of WalletTracker.
    """
    tracker1 = get_wallet_tracker()
    tracker2 = get_wallet_tracker()
    assert tracker1 is tracker2
    assert isinstance(tracker1, WalletTracker)


@pytest.mark.anyio
async def test_wallet_tracker_mocked_network_resilience_and_user_isolation(test_tracker):
    """
    Test using patch and AsyncMock to simulate DexScreener upstream failure,
    verifying fallback TokenPageData generation and multi-user settings isolation.
    """
    token_ca = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    user_a = 771122
    user_b = 883344

    # Simulate DexScreener HTTP error using patch and AsyncMock
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.side_effect = Exception("Simulated network timeout connecting to DexScreener")
    mock_ctx.__aexit__.return_value = None

    with patch("aiohttp.ClientSession.get", return_value=mock_ctx) as mock_get:
        page = await test_tracker.fetch_token_page(token_ca, user_id=user_a)
        assert isinstance(page, TokenPageData)
        assert page.token_address == token_ca
        assert page.price_usd > Decimal(0)
        assert mock_get.called

    # Test user_id-isolated privacy and alerts settings
    await test_tracker.toggle_token_privacy(token_ca, enabled=True, user_id=user_a)
    sub_a, notif_a, priv_a = await test_tracker.get_token_subscription(token_ca, user_id=user_a)
    assert priv_a is True

    # User B has their own independent subscription state
    await test_tracker.fetch_token_page(token_ca, user_id=user_b)
    sub_b, notif_b, priv_b = await test_tracker.get_token_subscription(token_ca, user_id=user_b)
    assert priv_b is False

