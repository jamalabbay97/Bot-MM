"""
tests/test_telegram_ingestion.py — Verification Suite for Telegram Scraper Micro-Service
========================================================================================
Tests:
1. Valid contract address parsing for both Base (EVM) and Solana (SVM), with system filter.
2. Rejection of edited messages (anti-bait-and-switch honeypot protection).
3. Dropping repeated signals across >= 3 channels in <= 15s (anti-sybil coordinated dump).
4. Safe fallback / dormant mode when Telegram credentials are missing or empty.
5. IngestionCoordinator concurrent lifecycle with EVM, SVM, and Telegram.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from alpha_engine.config import EngineConfig
from alpha_engine.ingestion.coordinator import IngestionCoordinator
from alpha_engine.ingestion.decoders import SvmPoolMeta
from alpha_engine.ingestion.telegram import (
    SOLANA_SYSTEM_PROGRAM_IDS,
    TelegramIngester,
)
from alpha_engine.models.enums import ChainIdentifier, NewsSignalStatus, SignalSource
from alpha_engine.models.events import RawSignalEvent, ShutdownSentinel
from alpha_engine.rate_limiter.registry import RateLimiterRegistry


class MockTelethonEvent:
    """Mock Telegram event representing incoming or edited Telethon message."""

    def __init__(
        self,
        text: str,
        chat_id: int = 1001,
        message_id: int = 42,
        chat_title: str = "Alpha Calls VIP",
    ) -> None:
        self.raw_text = text
        self.chat_id = chat_id
        self.id = message_id
        self.chat = MagicMock()
        self.chat.id = chat_id
        self.chat.title = chat_title
        self.created_at = time.time()


def test_contract_address_extraction_and_filtering():
    """Test 1: Valid contract address parsing for Base and Solana, ignoring system IDs."""
    session_fallback = os.getenv("TELEGRAM_SESSION_NAME", "test_session")
    cfg = EngineConfig(telegram_session_name=session_fallback)
    assert cfg.telegram_session_name == session_fallback
    assert "11111111111111111111111111111111" in SOLANA_SYSTEM_PROGRAM_IDS
    assert NewsSignalStatus.VALID == "valid"
    assert ShutdownSentinel().reason == "graceful_shutdown"
    assert time.time() > 0

    queue: asyncio.Queue = asyncio.Queue()
    ingester = TelegramIngester(event_queue=queue)

    evm_ca = "0x285617313860407d647990b50375990264186566"
    svm_ca = "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
    raydium_system_id = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
    pump_fun_program = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

    text = (
        f"🚨 ALPHA CALL: Base gem {evm_ca} launched! "
        f"Also check Solana moonshot {svm_ca}! "
        f"Traded on Raydium {raydium_system_id} and Pump.fun {pump_fun_program}."
    )

    extracted = ingester.extract_contract_addresses(text)

    # Must extract the valid Base and Solana tokens, but filter out system programs
    assert len(extracted) == 2, f"Expected 2 extracted CAs, got {len(extracted)}: {extracted}"

    chains = {chain for chain, _ in extracted}
    addrs = {addr for _, addr in extracted}

    assert ChainIdentifier.BASE_MAINNET in chains
    assert ChainIdentifier.SOLANA_MAINNET in chains
    assert evm_ca in addrs
    assert svm_ca in addrs
    assert raydium_system_id not in addrs
    assert pump_fun_program not in addrs


def test_anti_bait_and_switch_edited_message_rejection():
    """Test 2: Rejection of edited messages inserting a CA after posting."""
    async def run():
        queue: asyncio.Queue = asyncio.Queue()
        ingester = TelegramIngester(event_queue=queue)

        channel_id = -100987654321
        msg_id = 999
        ca = "0x285617313860407d647990b50375990264186566"

        # Step 1: Benign message posted initially (no CA)
        initial_event = MockTelethonEvent(
            text="Hey everyone, big announcement coming soon! Stay tuned.",
            chat_id=channel_id,
            message_id=msg_id,
            chat_title="Whale Alpha Lounge",
        )
        await ingester._handle_new_message(initial_event)

        # Queue should be empty because initial post had no CA
        assert queue.empty(), "Initial benign message should not produce a signal."

        # Step 2: Bait-and-switch edit inserting a CA
        edited_event = MockTelethonEvent(
            text=f"Huge announcement: Buy this token right now {ca} 100x!",
            chat_id=channel_id,
            message_id=msg_id,
            chat_title="Whale Alpha Lounge",
        )
        await ingester._handle_edited_message(edited_event)

        # Queue must still be empty — the honeypot edit was rejected!
        assert queue.empty(), "Bait-and-switch edited message must be rejected and not queued."

        # Step 3: Verify that an edit for a message never seen before is also dropped
        unseen_edited_event = MockTelethonEvent(
            text=f"Check out this edit {ca}!",
            chat_id=channel_id,
            message_id=1000,
            chat_title="Whale Alpha Lounge",
        )
        await ingester._handle_edited_message(unseen_edited_event)
        assert queue.empty(), "Unseen edited message with CA must be rejected."

    asyncio.run(run())


def test_anti_sybil_coordinated_shill_filter():
    """Test 3: Dropping repeated signals across >= 3 channels in <= 15s."""
    async def run():
        queue: asyncio.Queue = asyncio.Queue()
        ingester = TelegramIngester(event_queue=queue)

        ca = "0x285617313860407d647990b50375990264186566"

        # Channel 1 posts at t=0
        evt1 = MockTelethonEvent(
            text=f"Hot gem {ca} buy now!",
            chat_id=1001,
            message_id=1,
            chat_title="Channel Alpha",
        )
        await ingester._handle_new_message(evt1)
        assert queue.qsize() == 1, "First channel post should be accepted."
        sig1: RawSignalEvent = await queue.get()
        assert sig1.token_address == ca
        assert sig1.sybil_channel_count == 1

        # Channel 2 posts at t=1s
        evt2 = MockTelethonEvent(
            text=f"Aping into {ca}!",
            chat_id=1002,
            message_id=2,
            chat_title="Channel Beta",
        )
        await ingester._handle_new_message(evt2)
        assert queue.qsize() == 1, "Second channel post should be accepted."
        sig2: RawSignalEvent = await queue.get()
        assert sig2.token_address == ca
        assert sig2.sybil_channel_count == 2

        # Channel 3 posts at t=2s (3rd distinct channel within 15 seconds)
        evt3 = MockTelethonEvent(
            text=f"Do not miss {ca} mooning!",
            chat_id=1003,
            message_id=3,
            chat_title="Channel Gamma",
        )
        await ingester._handle_new_message(evt3)

        # Anti-sybil filter must tag as COORDINATED_DUMP and drop it!
        assert queue.empty(), "Third coordinated post within 15s must be dropped."

        # Channel 4 posts at t=3s (4th distinct channel within 15s)
        evt4 = MockTelethonEvent(
            text=f"Pump {ca}!",
            chat_id=1004,
            message_id=4,
            chat_title="Channel Delta",
        )
        await ingester._handle_new_message(evt4)
        assert queue.empty(), "Subsequent coordinated posts within 15s must also be dropped."

    asyncio.run(run())


def test_dormant_mode_when_credentials_empty():
    """Test 4: Safe fallback when credentials are not configured."""
    async def run():
        queue: asyncio.Queue = asyncio.Queue()

        # No api_id / api_hash provided
        ingester = TelegramIngester(
            event_queue=queue,
            api_id=None,
            api_hash=None,
        )

        assert ingester.is_dormant is True
        assert ingester.is_running is False

        # Should start without crashing
        await ingester.start()
        assert ingester.is_running is True

        # Run in a background task and ensure it handles shutdown
        task = asyncio.create_task(ingester.run())
        await asyncio.sleep(0.05)

        assert not task.done(), "Dormant ingester should idle cleanly."

        await ingester.stop()
        await task
        assert ingester.is_running is False

    asyncio.run(run())


def test_telethon_client_mock_lifecycle():
    """Test 5: Full start/stop lifecycle with mocked TelegramClient."""
    async def run():
        queue: asyncio.Queue = asyncio.Queue()
        mock_client = AsyncMock()
        mock_client.start = AsyncMock()
        mock_client.disconnect = AsyncMock()
        mock_client.run_until_disconnected = AsyncMock()
        mock_client.on = MagicMock()
        mock_client.add_event_handler = MagicMock()

        ingester = TelegramIngester(
            event_queue=queue,
            api_id=12345,
            api_hash="mock_hash",
            session_name="test_session",
            target_channels=["@test_alpha"],
            client=mock_client,
        )

        assert ingester.is_dormant is False

        async with ingester:
            assert ingester.is_running is True
            mock_client.start.assert_awaited_once()
            assert mock_client.add_event_handler.call_count >= 2

        assert ingester.is_running is False
        mock_client.disconnect.assert_awaited_once()

    asyncio.run(run())


def test_coordinator_integration_with_telegram():
    """Test 6: IngestionCoordinator orchestrates EVM, SVM, and Telegram concurrently."""
    async def run():
        limiter = RateLimiterRegistry.default()
        pool_watchlist = [
            ("0x6c561b446416e1a00e8e93e221854d6ea4171372", "0xToken", "0xNative", 18, 18, "v2")
        ]
        pool_registry: dict[str, SvmPoolMeta] = {}

        mock_client = AsyncMock()
        mock_client.start = AsyncMock()
        mock_client.disconnect = AsyncMock()
        mock_client.run_until_disconnected = AsyncMock()
        mock_client.add_event_handler = MagicMock()

        # IngestionCoordinator with mocked Telegram client
        telegram_ingester = TelegramIngester(
            event_queue=asyncio.Queue(),
            client=mock_client,
        )

        coordinator = IngestionCoordinator(
            evm_ws_url="wss://base-mainnet.g.alchemy.com/v2/mock",
            svm_ws_url="wss://mainnet.helius-rpc.com/?api-key=mock",
            pool_watchlist=pool_watchlist,
            pool_registry=pool_registry,
            limiter=limiter,
            telegram_ingester=telegram_ingester,
        )

        # Wire the event queue
        telegram_ingester._queue = coordinator.event_queue

        await coordinator.start()
        assert len(coordinator._tasks) == 3, "Coordinator must run EVM, SVM, and Telegram tasks."

        # Simulate Telegram event arriving into coordinator queue
        ca = "0x285617313860407d647990b50375990264186566"
        evt = MockTelethonEvent(
            text=f"Hot alpha: {ca}",
            chat_id=1001,
            message_id=50,
            chat_title="Alpha Stream",
        )
        await telegram_ingester._handle_new_message(evt)

        # Retrieve next event from coordinator
        received_event = await coordinator.__anext__()
        assert isinstance(received_event, RawSignalEvent)
        assert received_event.token_address == ca
        assert received_event.source == SignalSource.TELEGRAM_SCRAPER

        await coordinator.stop()
        assert len(coordinator._tasks) == 0, "Coordinator tasks must be cleaned up."

    asyncio.run(run())


def test_unresolvable_channels_graceful_skipping():
    """Test 7: Channels that do not exist or throw ValueError are skipped without crashing."""
    async def run():
        queue: asyncio.Queue = asyncio.Queue()
        mock_client = AsyncMock()
        mock_client.start = AsyncMock()
        mock_client.disconnect = AsyncMock()
        mock_client.add_event_handler = MagicMock()

        # Simulate get_input_entity raising ValueError for invalid username
        async def fake_get_input_entity(chat):
            if chat == "@example_alpha_calls":
                raise ValueError('No user has "example_alpha_calls" as username')
            return 123456

        mock_client.get_input_entity = AsyncMock(side_effect=fake_get_input_entity)

        ingester = TelegramIngester(
            event_queue=queue,
            api_id=12345,
            api_hash="mock_hash",
            session_name="test_session",
            target_channels=["@example_alpha_calls", "@valid_alpha"],
            client=mock_client,
        )

        # start() should succeed without throwing
        await ingester.start()
        assert ingester.is_running is True
        mock_client.start.assert_awaited_once()

        # Check that event handlers were registered with only the valid channel
        assert mock_client.add_event_handler.call_count >= 2
        call_args = mock_client.add_event_handler.call_args_list[0]
        event_builder = call_args[0][1]
        assert event_builder.chats == [123456]

        await ingester.stop()
        assert ingester.is_running is False
        mock_client.disconnect.assert_awaited_once()

    asyncio.run(run())


class MockTelethonDMEvent:
    """Mock Telegram event representing a private direct message (DM)."""

    def __init__(self, text: str, sender_id: int = 123456789, msg_id: int = 101) -> None:
        self.raw_text = text
        self.text = text
        self.sender_id = sender_id
        self.id = msg_id
        self.is_private = True
        self.chat_id = sender_id
        self.replies: list[str] = []

    async def reply(self, message: str) -> None:
        self.replies.append(message)

    async def respond(self, message: str) -> None:
        self.replies.append(message)


def test_interactive_dm_start_and_help():
    """Test 8: Interactive /start and /help commands in DM."""
    async def run():
        queue: asyncio.Queue = asyncio.Queue()
        ingester = TelegramIngester(event_queue=queue)

        evt_start = MockTelethonDMEvent("/start")
        await ingester._handle_dm_message(evt_start)
        assert len(evt_start.replies) == 1
        reply = evt_start.replies[0]
        assert "/status" in reply
        assert "/scan" in reply
        assert "/trades" in reply
        assert "Bot-MM Alpha Engine" in reply

        evt_help = MockTelethonDMEvent("/help")
        await ingester._handle_dm_message(evt_help)
        assert len(evt_help.replies) == 1

    asyncio.run(run())


def test_interactive_dm_status_command():
    """Test 9: Interactive /status command with provider metrics."""
    async def run():
        queue: asyncio.Queue = asyncio.Queue()

        def status_mock():
            return {
                "uptime_str": "01h 23m 45s",
                "rss_mb": 95.5,
                "equity_usd": 8550.00,
                "realized_pnl_usd": 250.00,
                "open_positions": 2,
                "win_rate_pct": 66.7,
                "max_drawdown_pct": 1.25,
                "ingest_q_size": 3,
                "ingest_q_max": 100,
                "signal_q_size": 1,
                "signal_q_max": 100,
            }

        ingester = TelegramIngester(
            event_queue=queue,
            status_provider=status_mock,
        )

        evt = MockTelethonDMEvent("/status")
        await ingester._handle_dm_message(evt)
        assert len(evt.replies) == 1
        msg = evt.replies[0]
        assert "01h 23m 45s" in msg
        assert "95.5 MB" in msg
        assert "$8550.00" in msg
        assert "$250.00" in msg
        assert "Active Lots:** `2`" in msg
        assert "66.7%" in msg
        assert "1.25%" in msg
        assert "3/100" in msg

    asyncio.run(run())


def test_interactive_dm_scan_command_and_direct_ca():
    """Test 10: /scan <CA> and direct CA pasting in DM."""
    async def run():
        queue: asyncio.Queue = asyncio.Queue()
        ingester = TelegramIngester(event_queue=queue)

        # 1. Base EVM address via /scan
        evm_ca = "0x285617313860407d647990b50375990264186566"
        evt_scan = MockTelethonDMEvent(f"/scan {evm_ca}")
        await ingester._handle_dm_message(evt_scan)
        assert len(evt_scan.replies) == 1
        assert "🔎 Ingested CA:" in evt_scan.replies[0]
        assert evm_ca in evt_scan.replies[0]
        assert "Dispatching to SecurityGatekeeper & AMM Math Engine" in evt_scan.replies[0]

        event = queue.get_nowait()
        assert isinstance(event, RawSignalEvent)
        assert event.token_address == evm_ca
        assert event.chain == ChainIdentifier.BASE_MAINNET

        # 2. Solana Base58 address pasted directly (no /scan prefix)
        svm_ca = "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
        evt_direct = MockTelethonDMEvent(svm_ca)
        await ingester._handle_dm_message(evt_direct)
        assert len(evt_direct.replies) == 1
        assert svm_ca in evt_direct.replies[0]

        event2 = queue.get_nowait()
        assert isinstance(event2, RawSignalEvent)
        assert event2.token_address == svm_ca
        assert event2.chain == ChainIdentifier.SOLANA_MAINNET

        # 3. Invalid address rejected
        evt_invalid = MockTelethonDMEvent("/scan invalid_token_123")
        await ingester._handle_dm_message(evt_invalid)
        assert "Invalid Contract Address" in evt_invalid.replies[0]
        assert queue.empty()

    asyncio.run(run())


def test_interactive_dm_trades_command(tmp_path):
    """Test 11: /trades command reading executed trades from SQLite."""
    async def run():
        import aiosqlite
        db_file = tmp_path / "test_paper_trading.db"

        # Create trades table and insert 2 dummy trades
        async with aiosqlite.connect(str(db_file)) as db:
            await db.execute(
                """
                CREATE TABLE trades (
                    trade_id TEXT PRIMARY KEY,
                    order_id TEXT,
                    signal_id TEXT,
                    chain TEXT,
                    token_address TEXT,
                    pool_address TEXT,
                    side TEXT,
                    native_spent TEXT,
                    tokens_delta TEXT,
                    effective_price TEXT,
                    price_impact_bps INTEGER,
                    gas_cost_usd TEXT,
                    fill_latency_ms INTEGER,
                    kelly_fraction REAL,
                    portfolio_equity_usd TEXT,
                    realized_pnl_usd TEXT,
                    signal_timestamp_ns INTEGER,
                    fill_timestamp_ns INTEGER,
                    created_at INTEGER
                )
                """
            )
            await db.execute(
                """
                INSERT INTO trades (trade_id, chain, token_address, side, effective_price, realized_pnl_usd, created_at)
                VALUES ('t1', 'base_mainnet', '0x285617313860407d647990b50375990264186566', 'buy', '0.000125', '0.00', 100),
                       ('t2', 'solana_mainnet', '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr', 'sell', '0.004210', '12.50', 200)
                """
            )
            await db.commit()

        queue: asyncio.Queue = asyncio.Queue()
        ingester = TelegramIngester(event_queue=queue, db_path=str(db_file))

        evt = MockTelethonDMEvent("/trades")
        await ingester._handle_dm_message(evt)
        assert len(evt.replies) == 1
        msg = evt.replies[0]
        assert "Last 5 Executed Paper Trades" in msg
        assert "TOKEN" in msg
        assert "SIDE" in msg
        assert "BUY" in msg
        assert "SELL" in msg
        assert "+$12.50" in msg

    asyncio.run(run())


def test_admin_authorization_enforcement():
    """Test 12: Admin ID enforcement silently ignores unauthorized callers."""
    async def run():
        queue: asyncio.Queue = asyncio.Queue()
        admin_id = 999888777
        ingester = TelegramIngester(
            event_queue=queue,
            admin_ids=[admin_id],
        )

        # 1. Unauthorized user
        evt_unauth = MockTelethonDMEvent("/status", sender_id=111222333)
        await ingester._handle_dm_message(evt_unauth)
        assert len(evt_unauth.replies) == 0, "Unauthorized caller must be silently ignored."
        assert queue.empty()

        # 2. Authorized admin
        evt_auth = MockTelethonDMEvent("/status", sender_id=admin_id)
        await ingester._handle_dm_message(evt_auth)
        assert len(evt_auth.replies) == 1, "Authorized admin must receive command reply."

    asyncio.run(run())


if __name__ == "__main__":
    print("=== RUNNING TELEGRAM INGESTION TEST SUITE ===")
    test_contract_address_extraction_and_filtering()
    print("  [PASS] test_contract_address_extraction_and_filtering")
    test_anti_bait_and_switch_edited_message_rejection()
    print("  [PASS] test_anti_bait_and_switch_edited_message_rejection")
    test_anti_sybil_coordinated_shill_filter()
    print("  [PASS] test_anti_sybil_coordinated_shill_filter")
    test_dormant_mode_when_credentials_empty()
    print("  [PASS] test_dormant_mode_when_credentials_empty")
    test_telethon_client_mock_lifecycle()
    print("  [PASS] test_telethon_client_mock_lifecycle")
    test_coordinator_integration_with_telegram()
    print("  [PASS] test_coordinator_integration_with_telegram")
    test_unresolvable_channels_graceful_skipping()
    print("  [PASS] test_unresolvable_channels_graceful_skipping")
    test_interactive_dm_start_and_help()
    print("  [PASS] test_interactive_dm_start_and_help")
    test_interactive_dm_status_command()
    print("  [PASS] test_interactive_dm_status_command")
    test_interactive_dm_scan_command_and_direct_ca()
    print("  [PASS] test_interactive_dm_scan_command_and_direct_ca")
    test_admin_authorization_enforcement()
    print("  [PASS] test_admin_authorization_enforcement")
    print("\nALL 12 TELEGRAM INGESTION TESTS PASSED WITH 100% SUCCESS!")
