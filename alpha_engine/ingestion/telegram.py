"""
alpha_engine.ingestion.telegram — Telegram & Social Media Sentiment Scraper
===========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | telethon | asyncio
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import defaultdict
from typing import Any, Optional, Sequence, Set

try:
    from telethon import TelegramClient, events
    from telethon.errors import FloodWaitError
    TELETHON_AVAILABLE = True
except ImportError:  # pragma: no cover
    TELETHON_AVAILABLE = False

    class TelegramClient:  # type: ignore
        """Fallback class when telethon is not installed."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    events = None  # type: ignore

    class FloodWaitError(Exception):  # type: ignore
        seconds: int = 0

from alpha_engine.models.enums import ChainIdentifier, NewsSignalStatus, SignalSource
from alpha_engine.models.events import RawSignalEvent, ShutdownSentinel
from alpha_engine.rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)

# Base (EVM) 40-hex-char address regex
EVM_CA_REGEX = re.compile(r"\b(0x[a-fA-F0-9]{40})\b")

# Solana (SVM) base58 32-44 character address regex
SVM_CA_REGEX = re.compile(r"\b([1-9A-HJ-NP-za-km-z]{32,44})\b")

# Well-known system program IDs and base infrastructure to exclude from Solana token extraction
SOLANA_SYSTEM_PROGRAM_IDS: Set[str] = {
    "11111111111111111111111111111111",                     # System Program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",          # Token Program
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",          # Token-2022
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",          # Associated Token Account
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",          # Raydium Liquidity Pool V4
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",          # Raydium CLMM
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",          # Raydium CPMM
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",          # Raydium Authority
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",          # Pump.fun Program
    "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM",          # Pump.fun Fee Account
    "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1",          # Pump.fun Global
    "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",          # Serum v3
    "opnb2TXrmDdHGauUQavWet8xTzJpdodAx7LvnR38sNY",          # OpenBook
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",          # Meteora DLMM
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",          # Meteora Pools
    "ComputeBudget111111111111111111111111111111",          # Compute Budget
    "SysvarRent111111111111111111111111111111111",          # Sysvar Rent
    "SysvarC1ock11111111111111111111111111111111",          # Sysvar Clock
    "SysvarRecentB1ockHashes11111111111111111111",          # Sysvar Blockhashes
    "So11111111111111111111111111111111111111112",          # Wrapped SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",          # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",          # USDT
}

# EVM well-known system / router / zero addresses to exclude
EVM_SYSTEM_ADDRESSES: Set[str] = {
    "0x0000000000000000000000000000000000000000",
    "0x4200000000000000000000000000000000000006",          # Base WETH
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",          # Base USDC
    "0xcf77a3ba9a5ca399b7c97c749566343833341fdc",          # Aerodrome Router
}


class TelegramIngester:
    """
    Micro-service for scraping Telegram alpha signals and news channels.
    Features:
      - Resilient MTProto client lifecycle via Telethon
      - Dual-chain CA extraction (EVM Base & SVM Solana) with system router filtering
      - Anti-sybil coordinated dump detection (sliding window: 60s, burst: >=3 channels in <=15s)
      - Anti bait-and-switch protection (events.MessageEdited rejection)
      - FloodWaitError backoff and rate limiter integration
      - Non-crashing dormant fallback when credentials are not configured
    """

    SLIDING_WINDOW_SEC: float = 60.0
    SYBIL_BURST_WINDOW_SEC: float = 15.0
    SYBIL_CHANNEL_THRESHOLD: int = 3

    def __init__(
        self,
        event_queue: asyncio.Queue[Any],
        api_id: Optional[int] = None,
        api_hash: Optional[str] = None,
        session_name: str = "bot_mm_session",
        bot_token: Optional[str] = None,
        target_channels: Optional[Sequence[str | int]] = None,
        limiter: Optional[RateLimiterRegistry] = None,
        client: Optional[TelegramClient] = None,
    ) -> None:
        self._queue = event_queue
        self._api_id = api_id
        self._api_hash = api_hash
        self._bot_token = bot_token
        # If session_name was mistakenly given as a bot token, extract it
        if session_name and ":" in session_name:
            if not self._bot_token:
                self._bot_token = session_name
            session_name = "bot_mm_session"
        self._session_name = session_name
        self._target_channels = list(target_channels) if target_channels else []
        self._limiter = limiter

        self._shutdown_event = asyncio.Event()
        self._running = False
        self._client: Optional[TelegramClient] = client

        # Anti-sybil tracking: token_address.lower() -> list of (monotonic_timestamp, channel_id_str)
        self._ca_post_history: dict[str, list[tuple[float, str]]] = defaultdict(list)

        # Anti bait-and-switch tracking: (channel_id, message_id) -> initial extracted CAs (lower)
        self._seen_messages: dict[tuple[int, int], Set[str]] = {}

        # Determine if running in dormant/mock mode
        self._is_dormant = False
        if self._client is None:
            if not TELETHON_AVAILABLE or self._api_id is None or not self._api_hash:
                self._is_dormant = True
                logger.info(
                    "TelegramIngester operating in dormant mode (missing TELEGRAM_API_ID or TELEGRAM_API_HASH)."
                )

    @property
    def is_dormant(self) -> bool:
        """Returns True if the ingester is in dormant fallback mode."""
        return self._is_dormant

    @property
    def is_running(self) -> bool:
        return self._running

    def extract_contract_addresses(self, text: str) -> list[tuple[ChainIdentifier, str]]:
        """
        Extract valid contract addresses from raw message text.
        Returns a list of tuples: (ChainIdentifier, address_string).
        """
        if not text:
            return []

        results: list[tuple[ChainIdentifier, str]] = []

        # 1. Base (EVM) extraction
        for match in EVM_CA_REGEX.finditer(text):
            ca = match.group(1)
            if ca.lower() not in EVM_SYSTEM_ADDRESSES:
                results.append((ChainIdentifier.BASE_MAINNET, ca))

        # 2. Solana (SVM) extraction
        for match in SVM_CA_REGEX.finditer(text):
            ca = match.group(1)
            if ca not in SOLANA_SYSTEM_PROGRAM_IDS:
                # Discard pure lowercase hex strings of length 40 (already captured by EVM)
                if len(ca) == 40 and all(c in "0123456789abcdefABCDEF" for c in ca):
                    continue
                results.append((ChainIdentifier.SOLANA_MAINNET, ca))

        return results

    def check_anti_sybil(self, token_address: str, channel_id_str: str) -> tuple[bool, int]:
        """
        Sliding time window (60s) tracking CA occurrence frequency across channels.
        If the identical CA appears across >= 3 separate channels within 15 seconds,
        tags as COORDINATED_DUMP and returns (True, unique_channel_count).
        """
        now = time.monotonic()
        key = token_address.lower()

        history = self._ca_post_history[key]
        # Purge entries older than sliding window (60s)
        recent_history = [
            (ts, ch) for ts, ch in history if (now - ts) <= self.SLIDING_WINDOW_SEC
        ]
        recent_history.append((now, channel_id_str))
        self._ca_post_history[key] = recent_history

        # Count unique channels within burst window (15s)
        burst_channels = {
            ch for ts, ch in recent_history if (now - ts) <= self.SYBIL_BURST_WINDOW_SEC
        }
        unique_count = len(burst_channels)
        is_coordinated = unique_count >= self.SYBIL_CHANNEL_THRESHOLD

        return is_coordinated, unique_count

    def _extract_event_metadata(self, event: Any) -> tuple[int, int, str, str]:
        """
        Safely extracts (channel_id, message_id, channel_title, text) from a Telethon event
        or a mock event object.
        """
        text = getattr(event, "raw_text", None)
        if text is None:
            message_obj = getattr(event, "message", None)
            if message_obj is not None:
                text = getattr(message_obj, "message", getattr(message_obj, "text", ""))
            else:
                text = getattr(event, "text", "")

        chat_id = getattr(event, "chat_id", None)
        if chat_id is None:
            chat_obj = getattr(event, "chat", None)
            chat_id = getattr(chat_obj, "id", 0) if chat_obj else 0

        msg_id = getattr(event, "id", None)
        if msg_id is None:
            message_obj = getattr(event, "message", None)
            msg_id = getattr(message_obj, "id", 0) if message_obj else 0

        chat_title = ""
        chat_obj = getattr(event, "chat", None)
        if chat_obj is not None:
            chat_title = getattr(chat_obj, "title", str(chat_id))
        else:
            chat_title = str(chat_id)

        return int(chat_id), int(msg_id), str(chat_title), str(text or "")

    async def _enqueue_event(self, event: Any) -> None:
        """
        Push event to queue with drop-oldest policy on backpressure.
        """
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            try:
                _ = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("Event queue full; dropped event %s", event)

    async def _handle_new_message(self, event: Any) -> None:
        """
        Handle events.NewMessage from Telegram client.
        """
        channel_id, message_id, channel_title, text = self._extract_event_metadata(event)
        cas = self.extract_contract_addresses(text)

        # Store message history for bait-and-switch detection
        msg_key = (channel_id, message_id)
        found_cas = {ca.lower() for _, ca in cas}
        self._seen_messages[msg_key] = found_cas

        # Bound seen messages cache to 10,000 entries
        if len(self._seen_messages) > 10_000:
            keys_to_purge = list(self._seen_messages.keys())[:5000]
            for k in keys_to_purge:
                del self._seen_messages[k]

        if not cas:
            return

        for chain, ca in cas:
            is_sybil, channel_count = self.check_anti_sybil(ca, str(channel_id))
            if is_sybil:
                logger.warning(
                    "[Anti-Sybil] Dropped COORDINATED_DUMP for CA %s (seen across %d channels in <=15s)",
                    ca,
                    channel_count,
                )
                continue

            raw_signal = RawSignalEvent(
                timestamp_ns=time.time_ns(),
                chain=chain,
                token_address=ca,
                source=SignalSource.TELEGRAM_SCRAPER,
                originating_channel=channel_title or str(channel_id),
                channel_id=channel_id,
                message_id=message_id,
                raw_text=text,
                status=NewsSignalStatus.VALID,
                sybil_channel_count=channel_count,
                is_edit=False,
            )
            logger.info(
                "[TelegramIngester] Valid signal detected: %s on %s from channel '%s'",
                ca,
                chain.value,
                channel_title,
            )
            await self._enqueue_event(raw_signal)

    async def _handle_edited_message(self, event: Any) -> None:
        """
        Handle events.MessageEdited.
        If a message was edited to insert a CA after posting, reject the signal
        immediately (classic bait-and-switch honeypot trap).
        """
        channel_id, message_id, channel_title, text = self._extract_event_metadata(event)
        cas = self.extract_contract_addresses(text)
        if not cas:
            return

        msg_key = (channel_id, message_id)
        original_cas = self._seen_messages.get(msg_key)

        for chain, ca in cas:
            # If the CA was not present in the original message, it is a bait-and-switch insertion
            is_injected = (original_cas is None) or (ca.lower() not in original_cas)
            if is_injected:
                logger.warning(
                    "[Bait-and-Switch] Rejected edited post inserting CA %s on %s in '%s' (msg_id=%d)",
                    ca,
                    chain.value,
                    channel_title,
                    message_id,
                )
                # Discard signal immediately
                continue

    async def start(self) -> None:
        """
        Initialize and connect the Telethon TelegramClient.
        Registers event listeners for new and edited messages.
        """
        if self._is_dormant:
            logger.info("TelegramIngester starting in dormant mode (no credentials provided).")
            self._running = True
            return

        if self._client is None and TELETHON_AVAILABLE:
            assert self._api_id is not None and self._api_hash is not None
            self._client = TelegramClient(self._session_name, self._api_id, self._api_hash)

        if self._client is not None:
            # 1. Start and authenticate client first
            if hasattr(self._client, "start"):
                start_kwargs = {}
                if self._bot_token:
                    start_kwargs["bot_token"] = self._bot_token
                start_res = self._client.start(**start_kwargs)
                if asyncio.iscoroutine(start_res):
                    await start_res

            # 2. Resolve target channels safely
            chats_arg: Optional[list[Any]] = None
            if self._target_channels:
                resolved_chats: list[Any] = []
                for ch in self._target_channels:
                    # Check if already an integer ID or digit string
                    if isinstance(ch, int) or (isinstance(ch, str) and ch.lstrip("-").isdigit()):
                        resolved_chats.append(int(ch))
                        continue

                    # String username or channel link: resolve safely via Telethon
                    if hasattr(self._client, "get_input_entity"):
                        try:
                            entity_res = self._client.get_input_entity(ch)
                            if asyncio.iscoroutine(entity_res):
                                entity = await entity_res
                            else:
                                entity = entity_res
                            resolved_chats.append(entity)
                            logger.info("[TelegramIngester] Target channel '%s' resolved successfully.", ch)
                        except Exception as exc:
                            logger.warning(
                                "[TelegramIngester] Target channel '%s' could not be resolved (%s). Skipping.",
                                ch,
                                exc,
                            )
                    else:
                        resolved_chats.append(ch)

                chats_arg = resolved_chats
                logger.info(
                    "[TelegramIngester] Configured %d/%d target channels.",
                    len(resolved_chats),
                    len(self._target_channels),
                )

            # 3. Register event handlers
            if hasattr(self._client, "add_event_handler") and TELETHON_AVAILABLE:
                self._client.add_event_handler(
                    self._handle_new_message,
                    events.NewMessage(chats=chats_arg),
                )
                self._client.add_event_handler(
                    self._handle_edited_message,
                    events.MessageEdited(chats=chats_arg),
                )

            self._running = True
            logger.info("TelegramIngester connected and listening to channels.")

    async def stop(self) -> None:
        """Gracefully disconnect and tear down the client."""
        self._shutdown_event.set()
        self._running = False

        if self._client is not None:
            if hasattr(self._client, "disconnect"):
                disconnect_res = self._client.disconnect()
                if asyncio.iscoroutine(disconnect_res):
                    await disconnect_res

        logger.info("TelegramIngester stopped.")

    async def run(self) -> None:
        """
        Continuous ingestion loop with auto-reconnect, exponential backoff,
        and FloodWaitError sleep penalties.
        """
        await self.start()

        if self._is_dormant:
            # In dormant mode, simply wait for the shutdown event
            await self._shutdown_event.wait()
            return

        backoff = 1.0
        while not self._shutdown_event.is_set():
            try:
                if self._client is not None and hasattr(self._client, "run_until_disconnected"):
                    res = self._client.run_until_disconnected()
                    if asyncio.iscoroutine(res):
                        await res
                    else:
                        await self._shutdown_event.wait()
                else:
                    await self._shutdown_event.wait()
                break
            except FloodWaitError as exc:
                wait_s = getattr(exc, "seconds", 60) + 1
                logger.warning(
                    "Telegram FloodWaitError encountered: sleeping for %d seconds",
                    wait_s,
                )
                if self._limiter is not None:
                    # Penalize rate limiter if supported
                    try:
                        self._limiter.penalize("telegram", wait_s)
                    except Exception:
                        pass
                await asyncio.sleep(wait_s)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("TelegramIngester error: %s. Reconnecting in %.1fs...", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 60.0)

        await self.stop()

    async def __aenter__(self) -> "TelegramIngester":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()
