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


def test_contract_address_extraction_and_filtering():
    """Test 1: Valid contract address parsing for Base and Solana, ignoring system IDs."""
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

        # Check that event handlers were registered with only the valid channel
        assert mock_client.add_event_handler.call_count >= 2
        call_args = mock_client.add_event_handler.call_args_list[0]
        event_builder = call_args[0][1]
        assert event_builder.chats == [123456]

        await ingester.stop()
        assert ingester.is_running is False

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
    print("\nALL 7 TELEGRAM INGESTION TESTS PASSED WITH 100% SUCCESS!")
