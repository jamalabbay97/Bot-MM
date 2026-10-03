"""
tests/test_telegram_news_whale_ingestion.py — Verification of Telegram Real-Time News & Whale Pipeline
======================================================================================================
Tests:
  1. NewsEvent model initialization, mint normalization, and is_actionable predicate.
  2. Contract address extraction (Solana base58 + EVM hex) with system address filtering.
  3. High-impact keyword extraction and narrative tagging.
  4. Sentiment scoring calculation (positive/negative triggers and bounds [-1.0, +1.0]).
  5. TelegramIngester channel message handling pushing NewsEvent to signal_queue.
  6. Interactive DM commands: /news and /whales.
  7. Engine runner processing of actionable NewsEvents with Gatekeeper preflight.
  8. EngineConfig and .env defaults for Telegram news channels and session name.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.ingestion.telegram import TelegramIngester
from alpha_engine.models.enums import ChainIdentifier, SecurityTier
from alpha_engine.models.events import ShutdownSentinel
from alpha_engine.models.news import NewsEvent
from alpha_engine.models.state import SecurityReport


def test_news_event_model_and_actionable_property() -> None:
    """Validate NewsEvent model fields, mint normalization, and actionable predicate."""
    # 1. Actionable: contains detected mint and sentiment > 0.4
    ev1 = NewsEvent(
        source_channel="TreeNewsFeed",
        raw_text="Binance will list $XYZ token 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        detected_mints=["7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"],
        sentiment_score=0.75,
        urgency=0.9,
    )
    assert ev1.is_actionable is True
    assert len(ev1.detected_mints) == 1

    # 2. Normalization: tuple of (chain, mint) passed to detected_mints
    ev2 = NewsEvent(
        source_channel="lookonchain",
        raw_text="Whale bought $ABC",
        detected_mints=[(ChainIdentifier.SOLANA_MAINNET, "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU")],
        sentiment_score=0.8,
    )
    assert ev2.detected_mints == ["7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"]
    assert ev2.is_actionable is True

    # 3. Non-actionable: positive sentiment without detected mint
    ev3 = NewsEvent(
        source_channel="insiderpaper",
        raw_text="SEC approved spot crypto index ETF",
        detected_mints=[],
        sentiment_score=0.85,
    )
    assert ev3.is_actionable is False

    # 4. Non-actionable: detected mint but neutral or negative sentiment (<= 0.4)
    ev4 = NewsEvent(
        source_channel="WatcherGuru",
        raw_text="Token 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU had exploit hack",
        detected_mints=["7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"],
        sentiment_score=-0.8,
    )
    assert ev4.is_actionable is False


def test_contract_address_extraction_and_system_filtering() -> None:
    """Test extraction of Solana and EVM contract addresses with system program exclusion."""
    queue = asyncio.Queue()
    ingester = TelegramIngester(event_queue=queue)

    # Solana valid token vs Wrapped SOL system address
    solana_text = (
        "Breaking: Whale accumulated 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU "
        "using Wrapped SOL So11111111111111111111111111111111111111112"
    )
    sol_extracted = ingester.extract_contract_addresses(solana_text)
    assert len(sol_extracted) == 1
    assert sol_extracted[0][0] == ChainIdentifier.SOLANA_MAINNET
    assert sol_extracted[0][1] == "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"

    # EVM valid address vs Base WETH and USDC system addresses
    evm_text = (
        "New pool deployed at 0x1234567890abcdef1234567890abcdef12345678 "
        "paired against WETH 0x4200000000000000000000000000000000000006 and USDC 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    )
    evm_extracted = ingester.extract_contract_addresses(evm_text)
    assert len(evm_extracted) == 1
    assert evm_extracted[0][0] == ChainIdentifier.BASE_MAINNET
    assert evm_extracted[0][1] == "0x1234567890abcdef1234567890abcdef12345678"


def test_sentiment_scoring_and_keyword_tagging() -> None:
    """Test sentiment polarity score calculation and high impact keyword extraction."""
    # Positive triggers
    text_pos = "Urgent: Whale bought and is accumulating $ALPHA after SEC approved listing!"
    score_pos, urgency_pos, keywords_pos = TelegramIngester.parse_news_content(text_pos)
    assert score_pos >= 0.4
    assert urgency_pos >= 0.8
    assert "urgent" in keywords_pos
    assert "whale" in keywords_pos
    assert "bought" in keywords_pos
    assert "listing" in keywords_pos
    assert "sec" in keywords_pos

    # Negative triggers
    text_neg = "Alert: Protocol suffered exploit and rug, team dumped all tokens under investigation"
    score_neg, urgency_neg, keywords_neg = TelegramIngester.parse_news_content(text_neg)
    assert score_neg <= -0.5
    assert -1.0 <= score_neg <= 1.0


def test_telegram_channel_message_pushes_news_event_to_signal_queue() -> None:
    """Ensure channel messages are packaged as NewsEvent and pushed to signal_queue."""
    async def run() -> None:
        event_q: asyncio.Queue = asyncio.Queue()
        signal_q: asyncio.Queue = asyncio.Queue()

        ingester = TelegramIngester(
            event_queue=event_q,
            signal_queue=signal_q,
            target_channels=["TreeNewsFeed", "lookonchain"],
        )

        class MockChannelMsg:
            def __init__(self, text: str, title: str = "TreeNewsFeed", chat_id: int = 1001) -> None:
                self.raw_text = text
                self.chat_id = chat_id
                self.id = 555
                self.chat = MagicMock(title=title, id=chat_id)

        mock_msg = MockChannelMsg(
            "BREAKING: Whale bought $PEPE on Solana: 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
        )

        await ingester._handle_channel_message(mock_msg)

        # 1. Event should be in signal_queue
        assert not signal_q.empty()
        queued_item = signal_q.get_nowait()
        assert isinstance(queued_item, NewsEvent)
        assert queued_item.source_channel == "TreeNewsFeed"
        assert "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU" in queued_item.detected_mints
        assert queued_item.sentiment_score > 0.4
        assert queued_item.is_actionable is True

        # 2. Ingester history deques should have recorded the news and whale alert
        assert len(ingester._recent_news) == 1
        assert len(ingester._recent_whales) == 1

    asyncio.run(run())


def test_interactive_dm_news_and_whales_commands() -> None:
    """Test /news and /whales commands reply with formatted telemetry."""
    async def run() -> None:
        event_q: asyncio.Queue = asyncio.Queue()
        signal_q: asyncio.Queue = asyncio.Queue()

        ingester = TelegramIngester(event_queue=event_q, signal_queue=signal_q)

        # Populate status provider
        status_mock = {
            "recent_news": [
                {
                    "source": "TreeNewsFeed",
                    "headline": "Binance lists new token",
                    "sentiment": 0.85,
                    "timestamp": time.time() - 30,
                }
            ],
            "recent_whales": [
                {
                    "source": "@lookonchain",
                    "token": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
                    "action": "BOUGHT",
                    "summary": "Whale bought 500 SOL worth of tokens",
                    "timestamp": time.time() - 120,
                }
            ],
        }
        ingester.set_status_provider(lambda: status_mock)

        class MockDMEvent:
            def __init__(self, text: str) -> None:
                self.raw_text = text
                self.sender_id = 99999
                self.is_private = True
                self.id = 1
                self.reply_text = ""

            async def reply(self, msg: str) -> None:
                self.reply_text = msg

        # 1. Test /news command
        news_event = MockDMEvent("/news")
        await ingester._handle_dm_message(news_event)
        assert "Last 5 Ingested News Headlines" in news_event.reply_text
        assert "TreeNewsFeed" in news_event.reply_text
        assert "+0.85" in news_event.reply_text

        # 2. Test /whales command
        whales_event = MockDMEvent("/whales")
        await ingester._handle_dm_message(whales_event)
        assert "Last 3 On-Chain Whale & Smart Money Alerts" in whales_event.reply_text
        assert "@lookonchain" in whales_event.reply_text
        assert "BOUGHT" in whales_event.reply_text

    asyncio.run(run())


def test_runner_processes_actionable_news_event() -> None:
    """Test engine runner handles actionable NewsEvent, validates with gatekeeper, and packages SignalEvent."""
    async def run() -> None:
        cfg = EngineConfig(
            base_rpc_http="https://base-mainnet.g.alchemy.com/v2/test",
            base_rpc_ws="wss://base-mainnet.g.alchemy.com/v2/test",
            solana_rpc_http="https://mainnet.helius-rpc.com/?api-key=test",
            solana_rpc_ws="wss://mainnet.helius-rpc.com/?api-key=test",
        )
        engine = PaperTradingEngine(cfg)

        # Mock gatekeeper to approve token
        mock_gatekeeper = MagicMock()
        mock_report = SecurityReport(
            token_address="7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
            chain=ChainIdentifier.SOLANA_MAINNET,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
            lp_burned_ratio=1.0,
            top10_concentration=0.1,
            mint_authority_disabled=True,
            verified_source_code=True,
        )
        mock_gatekeeper.screen_token = AsyncMock(return_value=mock_report)
        engine._gatekeeper = mock_gatekeeper

        # Mock executor and ledger
        mock_executor = MagicMock()
        mock_executor.execute_signal = AsyncMock(return_value=None)
        engine._executor = mock_executor
        engine._ledger = MagicMock()
        engine._ledger.record_signal = AsyncMock()

        # Enqueue an actionable NewsEvent
        actionable_news = NewsEvent(
            source_channel="lookonchain",
            raw_text="Whale bought 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU after listing",
            detected_mints=["7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"],
            sentiment_score=0.8,
        )
        await engine._signal_q.put(actionable_news)
        await engine._signal_q.put(ShutdownSentinel())

        # Run signal queue processor
        await engine._process_signal_queue()

        # Verify gatekeeper screened token
        mock_gatekeeper.screen_token.assert_awaited_once_with(
            token_address="7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address="",
        )

        # Verify news and whale telemetry recorded in engine
        status = engine._get_engine_status()
        assert len(status["recent_news"]) >= 1
        assert len(status["recent_whales"]) >= 1
        assert status["recent_whales"][0]["token"] == "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"

    asyncio.run(run())


def test_config_telegram_defaults() -> None:
    """Verify EngineConfig and defaults match specified settings."""
    cfg = EngineConfig(
        base_rpc_http="https://base-mainnet.g.alchemy.com/v2/test",
        base_rpc_ws="wss://base-mainnet.g.alchemy.com/v2/test",
        solana_rpc_http="https://mainnet.helius-rpc.com/?api-key=test",
        solana_rpc_ws="wss://mainnet.helius-rpc.com/?api-key=test",
    )
    assert cfg.telegram_session_name == "alpha_engine_listener"
    assert "insiderpaper" in cfg.telegram_channels
    assert "TreeNewsFeed" in cfg.telegram_channels
    assert "lookonchain" in cfg.telegram_channels
    assert "WatcherGuru" in cfg.telegram_channels


def test_telegram_broadcast_and_rich_closed_trades_command() -> None:
    """Verify TelegramIngester broadcasts real-time trade alerts and displays rich closed trades telemetry."""
    async def run() -> None:
        from decimal import Decimal
        queue: asyncio.Queue = asyncio.Queue()
        mock_client = MagicMock()
        mock_client.send_message = AsyncMock()

        ingester = TelegramIngester(
            event_queue=queue,
            admin_ids=[12345],
            client=mock_client,
        )

        # 1. Test broadcast_trade_alert
        await ingester.broadcast_trade_alert("🟢 **BUY EXECUTED**\n• Token: `TestToken`")
        mock_client.send_message.assert_called_once_with(12345, "🟢 **BUY EXECUTED**\n• Token: `TestToken`")

        # 2. Test /trades with rich closed trades
        status_mock = {
            "open_trades": [],
            "recent_closed_trades": [
                {
                    "token_address": "CcnCKDE6Zz11111111111111111111111111111111",
                    "chain": ChainIdentifier.SOLANA_MAINNET,
                    "side": "SELL",
                    "entry_price": Decimal("0.00000005422"),
                    "exit_price": Decimal("0.00000003424"),
                    "realized_pnl_usd": Decimal("-15.25"),
                    "realized_pnl_pct": -36.85,
                    "exit_reason": "sl_hard",
                    "duration_s": 42.5,
                    "is_win": False,
                    "timestamp": time.time(),
                },
                {
                    "token_address": "WinToken1111111111111111111111111111111111",
                    "chain": ChainIdentifier.SOLANA_MAINNET,
                    "side": "SELL",
                    "entry_price": Decimal("0.00000001000"),
                    "exit_price": Decimal("0.00000001500"),
                    "realized_pnl_usd": Decimal("25.00"),
                    "realized_pnl_pct": 50.00,
                    "exit_reason": "tp_50",
                    "duration_s": 85.0,
                    "is_win": True,
                    "timestamp": time.time(),
                },
            ],
        }
        ingester.set_status_provider(lambda: status_mock)

        class MockEvent:
            def __init__(self):
                self.is_private = True
                self.sender_id = 12345
                self.raw_text = "/trades"
                self.reply_text = ""

            async def reply(self, text):
                self.reply_text = text

        event = MockEvent()
        await ingester._handle_dm_message(event)

        assert "Recent Closed Trades" in event.reply_text
        assert "🔴 LOSS" in event.reply_text
        assert "sl_hard" in event.reply_text
        assert "-36.85%" in event.reply_text
        assert "-$15.25" in event.reply_text
        assert "🟢 WIN" in event.reply_text
        assert "+50.00%" in event.reply_text
        assert "+$25.00" in event.reply_text
        assert "tp_50" in event.reply_text

    asyncio.run(run())

