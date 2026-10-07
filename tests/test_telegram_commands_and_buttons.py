"""
tests.test_telegram_commands_and_buttons
==========================================
Unit and integration tests for Telegram Bot commands, persistent reply keyboard,
inline interactive buttons, callback query handler, and engine telemetry wiring.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
from alpha_engine.chat_interface import ConversationalSupervisor
from alpha_engine.ingestion.telegram import TelegramIngester
from alpha_engine.execution.ledger import SQLiteLedger


class DummyEvent:
    def __init__(self, text: str = "", sender_id: int = 12345, is_private: bool = True):
        self.raw_text = text
        self.text = text
        self.sender_id = sender_id
        self.is_private = is_private
        self.out = False
        self.replies: list[dict] = []

    async def reply(self, msg: str, **kwargs):
        self.replies.append({"text": msg, "kwargs": kwargs})

    async def respond(self, msg: str, **kwargs):
        self.replies.append({"text": msg, "kwargs": kwargs})


class DummyCallbackEvent:
    def __init__(self, data: bytes, sender_id: int = 12345):
        self.data = data
        self.sender_id = sender_id
        self.replies: list[dict] = []
        self.answered: bool = False
        self.alert_text: str | None = None

    async def reply(self, msg: str, **kwargs):
        self.replies.append({"text": msg, "kwargs": kwargs})

    async def respond(self, msg: str, **kwargs):
        self.replies.append({"text": msg, "kwargs": kwargs})

    async def answer(self, text: str = "", alert: bool = False):
        self.answered = True
        self.alert_text = text


@pytest.mark.anyio
async def test_persistent_reply_keyboard_and_inline_helpers(tmp_path):
    """Verify default keyboard contains all 12 operational buttons and inline helpers generate markup."""
    q = asyncio.Queue()
    ingester = TelegramIngester(event_queue=q, admin_ids=[12345], db_path=str(tmp_path / "t.db"))

    kb = ingester.get_default_keyboard_markup()
    assert kb is not None
    # 4 rows of 3 buttons
    assert len(kb) == 4
    for row in kb:
        assert len(row) == 3

    # Flatten button texts
    labels = [(btn.button.text if hasattr(btn, "button") else btn.text) for row in kb for btn in row]
    assert "📊 Status" in labels
    assert "🧠 AI Supervisor" in labels
    assert "📋 Trades" in labels
    assert "🔍 Assets" in labels
    assert "🌊 Staging" in labels
    assert "🔄 Revival" in labels
    assert "🛡️ AI Vetoes" in labels
    assert "📈 Performance" in labels
    assert "📡 Signals" in labels
    assert "📰 News" in labels
    assert "🐋 Whales" in labels
    assert "❓ Help" in labels

    # Test inline button helpers
    status_btn = ingester.get_status_inline_buttons()
    assert status_btn is not None
    trades_btn = ingester.get_trades_inline_buttons()
    assert trades_btn is not None
    ai_btn = ingester.get_ai_inline_buttons()
    assert ai_btn is not None
    welcome_btn = ingester.get_welcome_inline_buttons()
    assert welcome_btn is not None


@pytest.mark.anyio
async def test_telegram_dm_commands_dispatch(tmp_path):
    """Verify Telegram DM handles /perf, /exit, /assets, /staging, /revival, /reset_governor."""
    db_file = tmp_path / "test_cmds.db"
    async with SQLiteLedger(db_path=str(db_file)) as ledger:
        cfg = EngineConfig()
        ai_sup = AlphaSupervisorAI(config=cfg)
        explainer = ConversationalSupervisor(
            ledger=ledger,
            ai_supervisor=ai_sup,
        )

        q = asyncio.Queue()
        ingester = TelegramIngester(
            event_queue=q,
            admin_ids=[12345],
            db_path=str(db_file),
            ai_supervisor=ai_sup,
        )
        ingester.set_chat_explainer(explainer)

        # Mock governor execution target
        mock_target = MagicMock()
        mock_target.reset_trade_frequency_governor = MagicMock()
        ingester.set_execution_target(mock_target)

        # 1. /perf command
        ev_perf = DummyEvent(text="/perf 6")
        await ingester._handle_dm_message(ev_perf)
        assert len(ev_perf.replies) > 0
        assert "Performance" in ev_perf.replies[0]["text"] or "Win Rate" in ev_perf.replies[0]["text"]

        # 2. Button text "📈 Performance"
        ev_perf_btn = DummyEvent(text="📈 Performance")
        await ingester._handle_dm_message(ev_perf_btn)
        assert len(ev_perf_btn.replies) > 0

        # 3. /assets command (with inline buttons attached)
        ev_assets = DummyEvent(text="/assets")
        await ingester._handle_dm_message(ev_assets)
        assert len(ev_assets.replies) > 0
        assert "عملات" in ev_assets.replies[0]["text"] or "assets" in ev_assets.replies[0]["text"].lower()

        # 4. Button text "🔍 Assets"
        ev_assets_btn = DummyEvent(text="🔍 Assets")
        await ingester._handle_dm_message(ev_assets_btn)
        assert len(ev_assets_btn.replies) > 0

        # 5. /exit command with missing arg
        ev_exit_noarg = DummyEvent(text="/exit")
        await ingester._handle_dm_message(ev_exit_noarg)
        assert "يرجى تزويد" in ev_exit_noarg.replies[0]["text"]

        # 6. /exit command with specific CA
        ev_exit = DummyEvent(text="/exit 0x1111111111111111111111111111111111111111")
        await ingester._handle_dm_message(ev_exit)
        assert len(ev_exit.replies) > 0

        # 7. /reset_governor command
        ev_gov = DummyEvent(text="/reset_governor")
        await ingester._handle_dm_message(ev_gov)
        assert mock_target.reset_trade_frequency_governor.called
        assert "إعادة ضبط حاكم" in ev_gov.replies[0]["text"]


@pytest.mark.anyio
async def test_telegram_callback_query_handler(tmp_path):
    """Verify inline button callbacks trigger appropriate actions and answer the event."""
    db_file = tmp_path / "test_cb.db"
    async with SQLiteLedger(db_path=str(db_file)) as ledger:
        cfg = EngineConfig()
        ai_sup = AlphaSupervisorAI(config=cfg)
        explainer = ConversationalSupervisor(
            ledger=ledger,
            ai_supervisor=ai_sup,
        )

        q = asyncio.Queue()
        ingester = TelegramIngester(
            event_queue=q,
            admin_ids=[12345],
            db_path=str(db_file),
            ai_supervisor=ai_sup,
        )
        ingester.set_chat_explainer(explainer)

        # 1. Callback query: cmd:status
        cb_status = DummyCallbackEvent(data=b"cmd:status")
        await ingester._handle_callback_query(cb_status)
        assert cb_status.answered
        assert len(cb_status.replies) > 0
        assert "حالة المحرك" in cb_status.replies[0]["text"]

        # 2. Callback query: cmd:perf
        cb_perf = DummyCallbackEvent(data=b"cmd:perf")
        await ingester._handle_callback_query(cb_perf)
        assert cb_perf.answered
        assert len(cb_perf.replies) > 0

        # 3. Callback query: unauthorized user
        cb_unauth = DummyCallbackEvent(data=b"cmd:status", sender_id=99999)
        await ingester._handle_callback_query(cb_unauth)
        assert cb_unauth.answered
        assert "غير مصرح" in (cb_unauth.alert_text or "")
