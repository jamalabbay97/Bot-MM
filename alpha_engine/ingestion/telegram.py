"""
alpha_engine.ingestion.telegram — Telegram & Social Media Sentiment Scraper + Interactive Bot
============================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | telethon | asyncio | aiosqlite

Dual-Mode Architecture:
  1. Passive Channel Scraping:
     Monitors target alpha call channels, detects contract addresses (EVM & SVM),
     filters sybil coordinated dumps and bait-and-switch post edits, and routes
     RawSignalEvent objects to the ingestion queue without generating chat replies.
  2. Interactive Bot Control & DM Interface:
     Listens to direct messages (DMs), enforces admin authorization (TELEGRAM_ADMIN_IDS),
     and executes command handlers:
       - /start & /help : Help overview and usage instructions.
       - /status        : Real-time engine health, uptime, memory, equity, PnL, queues.
       - /scan <CA>     : Direct token submission -> immediate validation and pipeline injection.
       - /trades        : ASCII summary of the last 5 executed paper trades.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import defaultdict, deque
from decimal import Decimal
from typing import Any, Awaitable, Callable, Optional, Sequence, Set

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False
    aiosqlite = None  # type: ignore

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None  # type: ignore

try:
    from telethon import TelegramClient, events, utils
    from telethon.errors import FloodWaitError, RPCError
    from telethon.sessions import StringSession
    TELETHON_AVAILABLE = True
except ImportError:  # pragma: no cover
    TELETHON_AVAILABLE = False

    class TelegramClient:  # type: ignore
        """Fallback class when telethon is not installed."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    events = None  # type: ignore
    utils = None   # type: ignore
    StringSession = None  # type: ignore

    class FloodWaitError(Exception):  # type: ignore
        seconds: int = 0

    class RPCError(Exception):  # type: ignore
        pass

from alpha_engine.models.enums import ChainIdentifier, NewsSignalStatus, SignalSource
from alpha_engine.models.events import RawSignalEvent, ShutdownSentinel
from alpha_engine.models.news import NewsEvent
from alpha_engine.rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)

# Base (EVM) 40-hex-char address regex (42 characters with 0x prefix)
EVM_CA_REGEX = re.compile(r"\b(0x[a-fA-F0-9]{40})\b")

# Solana (SVM) base58 32-44 character address regex
SVM_CA_REGEX = re.compile(r"\b([1-9A-HJ-NP-za-km-z]{32,44})\b")

# Default public channels for macro news & whale intel
DEFAULT_TELEGRAM_CHANNELS: list[str] = [
    "insiderpaper",
    "disclosetv",
    "financialjuice",
    "TreeNewsFeed",
    "lookonchain",
    "WatcherGuru",
    "bubblemaps",
]

# High-impact news keywords & narrative triggers
HIGH_IMPACT_KEYWORDS: list[str] = [
    "break",
    "urgent",
    "launch",
    "pump",
    "listing",
    "sec",
    "trump",
    "elon",
    "hack",
    "whale",
    "bought",
]

# Polarity scoring weights (+0.4 to +0.9)
POSITIVE_TRIGGERS: dict[str, float] = {
    "bought": 0.6,
    "accumulating": 0.7,
    "accumulated": 0.7,
    "approved": 0.8,
    "approval": 0.8,
    "listing": 0.6,
    "listed": 0.6,
    "pump": 0.5,
    "launch": 0.5,
    "partnership": 0.6,
    "breakout": 0.5,
}

# Polarity scoring weights (-0.5 to -1.0)
NEGATIVE_TRIGGERS: dict[str, float] = {
    "dumped": -0.6,
    "dump": -0.6,
    "rug": -0.9,
    "rugged": -0.9,
    "hack": -0.8,
    "hacked": -0.8,
    "exploit": -0.8,
    "exploited": -0.8,
    "investigation": -0.5,
    "scam": -0.8,
    "drained": -0.8,
}

# Urgency keywords
URGENCY_KEYWORDS: list[str] = [
    "urgent",
    "break",
    "breaking",
    "alert",
    "emergency",
    "just in",
]

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
    Micro-service for scraping Telegram alpha signals and interactive bot management.
    Features:
      - Resilient MTProto client lifecycle via Telethon
      - Dual-mode operation: Passive Channel Scraping + Interactive Admin DM Control
      - Dual-chain CA extraction (EVM Base & SVM Solana) with system router filtering
      - Anti-sybil coordinated dump detection (sliding window: 60s, burst: >=3 channels in <=15s)
      - Anti bait-and-switch protection (events.MessageEdited rejection)
      - Interactive slash commands (/start, /help, /status, /scan, /trades, /news, /whales)
      - Direct CA pasting in DM for instant screening
      - Non-blocking SQLite queries (aiosqlite)
      - FloodWaitError backoff and rate limiter integration
      - Non-crashing dormant fallback when credentials are not configured
    """

    SLIDING_WINDOW_SEC: float = 60.0
    SYBIL_BURST_WINDOW_SEC: float = 15.0
    SYBIL_CHANNEL_THRESHOLD: int = 3

    def __init__(
        self,
        event_queue: asyncio.Queue[Any],
        signal_queue: Optional[asyncio.Queue[Any]] = None,
        api_id: Optional[int] = None,
        api_hash: Optional[str] = None,
        session_name: str = "bot_mm_session",
        session_string: Optional[str] = None,
        bot_token: Optional[str] = None,
        target_channels: Optional[Sequence[str | int]] = None,
        admin_ids: Optional[Sequence[int]] = None,
        db_path: str = "paper_trading.db",
        status_provider: Optional[Callable[[], dict[str, Any] | Awaitable[dict[str, Any]]]] = None,
        limiter: Optional[RateLimiterRegistry] = None,
        client: Optional[TelegramClient] = None,
    ) -> None:
        self._queue = event_queue
        self._signal_queue = signal_queue
        self._api_id = api_id
        self._api_hash = api_hash
        self._bot_token = bot_token

        # Normalize session name if inadvertently passed as a bot token
        if session_name and ":" in session_name:
            if not self._bot_token:
                self._bot_token = session_name
            session_name = "bot_mm_session"

        self._session_name = session_name
        self._session_string = session_string or os.getenv("TELEGRAM_SESSION_STRING")

        if target_channels:
            self._target_channels = list(target_channels)
        else:
            self._target_channels = list(DEFAULT_TELEGRAM_CHANNELS)

        self._admin_ids: Set[int] = {int(x) for x in admin_ids} if admin_ids else set()
        self._db_path = db_path
        self._status_provider = status_provider
        self._limiter = limiter

        self._shutdown_event = asyncio.Event()
        self._running = False
        self._start_time = time.time()
        self._client: Optional[TelegramClient] = client

        # Anti-sybil tracking: token_address.lower() -> list of (monotonic_timestamp, channel_id_str)
        self._ca_post_history: dict[str, list[tuple[float, str]]] = defaultdict(list)

        # Anti bait-and-switch tracking: (channel_id, message_id) -> initial extracted CAs (lower)
        self._seen_messages: dict[tuple[int, int], Set[str]] = {}

        # Real-time telemetry deques for /news and /whales
        self._recent_news: deque[dict[str, Any]] = deque(maxlen=50)
        self._recent_whales: deque[dict[str, Any]] = deque(maxlen=50)

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

    @property
    def admin_ids(self) -> Set[int]:
        return self._admin_ids

    def set_status_provider(
        self,
        provider: Callable[[], dict[str, Any] | Awaitable[dict[str, Any]]],
    ) -> None:
        """Assign or update dynamic status provider callback."""
        self._status_provider = provider

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

    @classmethod
    def parse_news_content(cls, text: str) -> tuple[float, float, list[str]]:
        """
        Analyze raw message text for sentiment score (-1.0 to +1.0), urgency (0.0 to 1.0),
        and matched high-impact keywords.
        """
        if not text:
            return 0.0, 0.5, []

        lower = text.lower()
        matched_kw: list[str] = [kw for kw in HIGH_IMPACT_KEYWORDS if kw in lower]

        score = 0.0
        pos_hits = 0
        neg_hits = 0

        for word, val in POSITIVE_TRIGGERS.items():
            if re.search(r"\b" + re.escape(word) + r"\b", lower):
                score += val
                pos_hits += 1

        for word, val in NEGATIVE_TRIGGERS.items():
            if re.search(r"\b" + re.escape(word) + r"\b", lower):
                score += val
                neg_hits += 1

        total_hits = pos_hits + neg_hits
        if total_hits > 0:
            score = score / total_hits
        score = max(-1.0, min(1.0, score))

        urgency = 0.5
        for u in URGENCY_KEYWORDS:
            if re.search(r"\b" + re.escape(u) + r"\b", lower):
                urgency = 0.9
                break

        return score, urgency, matched_kw

    @staticmethod
    def _format_time_elapsed(ts: float) -> str:
        """Format a timestamp into a human-readable elapsed time string (e.g. 15s ago, 2m ago)."""
        elapsed = max(0.0, time.time() - ts)
        if elapsed < 60:
            return f"{int(elapsed)}s ago"
        elif elapsed < 3600:
            return f"{int(elapsed // 60)}m ago"
        elif elapsed < 86400:
            return f"{int(elapsed // 3600)}h ago"
        return f"{int(elapsed // 86400)}d ago"

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

    async def _safe_reply(self, event: Any, text: str) -> None:
        """
        Safely reply to a message in DM, catching RPCError or network failures.
        """
        try:
            if hasattr(event, "reply"):
                res = event.reply(text)
                if asyncio.iscoroutine(res):
                    await res
            elif hasattr(event, "respond"):
                res = event.respond(text)
                if asyncio.iscoroutine(res):
                    await res
            elif self._client is not None and hasattr(self._client, "send_message"):
                sender_id = getattr(event, "sender_id", None)
                if sender_id is not None:
                    res = self._client.send_message(sender_id, text)
                    if asyncio.iscoroutine(res):
                        await res
        except RPCError as exc:
            logger.warning("[TelegramIngester] Telethon RPC error during reply: %s", exc)
        except Exception as exc:
            logger.warning("[TelegramIngester] Failed to send reply: %s", exc)

    def _is_authorized(self, sender_id: Optional[int]) -> bool:
        """
        Check if sender is an authorized admin.
        If TELEGRAM_ADMIN_IDS is configured, sender must be in the whitelist.
        If TELEGRAM_ADMIN_IDS is empty, allows all direct message callers (development mode).
        """
        if not self._admin_ids:
            return True
        return sender_id in self._admin_ids

    # -------------------------------------------------------------------------
    # 1. Passive Channel Scraping Handlers (No Chat Replies)
    # -------------------------------------------------------------------------

    async def _handle_channel_message(self, event: Any) -> None:
        """
        Passive scraping handler for monitored public/private channels.
        Extracts CAs, verifies anti-sybil and anti-bait-and-switch, and routes to queue.
        Ignores private DMs (which are handled by _handle_dm_message).
        """
        if getattr(event, "is_private", False):
            return

        channel_id, message_id, channel_title, text = self._extract_event_metadata(event)
        cas = self.extract_contract_addresses(text)
        sentiment, urgency, matched_kw = self.parse_news_content(text)
        detected_mints = [ca for _, ca in cas]

        news_event = NewsEvent(
            timestamp=time.time(),
            source_channel=channel_title or str(channel_id),
            raw_text=text,
            detected_mints=detected_mints,
            sentiment_score=sentiment,
            urgency=urgency,
            keywords=matched_kw,
            chain=cas[0][0] if cas else None,
        )

        # Store in recent news for telemetry & inspection commands
        self._recent_news.append({
            "timestamp": news_event.timestamp,
            "source": news_event.source_channel,
            "headline": text.replace("\n", " ").strip()[:100],
            "sentiment": sentiment,
            "urgency": urgency,
            "detected_mints": detected_mints,
            "keywords": matched_kw,
        })

        # Whale tracking for @lookonchain, @bubblemaps, or whale narrative posts
        src_lower = (channel_title or "").lower()
        if "lookonchain" in src_lower or "bubblemaps" in src_lower or "whale" in matched_kw:
            action = "WHALE ALERT"
            for kw in ("bought", "accumulating", "accumulated", "dumped", "sold", "transferred", "exploit"):
                if kw in text.lower():
                    action = kw.upper()
                    break
            self._recent_whales.append({
                "timestamp": news_event.timestamp,
                "source": news_event.source_channel,
                "token": detected_mints[0] if detected_mints else "N/A",
                "action": action,
                "summary": text.replace("\n", " ").strip()[:120],
            })

        # Push every valid NewsEvent onto coordinator.signal_queue
        if self._signal_queue is not None:
            try:
                self._signal_queue.put_nowait(news_event)
            except asyncio.QueueFull:
                pass

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

    async def _handle_channel_edited_message(self, event: Any) -> None:
        """
        Handle events.MessageEdited in monitored channels.
        If a message was edited to insert a CA after posting, reject the signal
        immediately (classic bait-and-switch honeypot trap).
        """
        if getattr(event, "is_private", False):
            return

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
                continue

    # Backward compatibility aliases for existing tests
    _handle_new_message = _handle_channel_message
    _handle_edited_message = _handle_channel_edited_message

    # -------------------------------------------------------------------------
    # 2. Interactive Bot Control & DM Interface Handlers
    # -------------------------------------------------------------------------

    async def _handle_dm_message(self, event: Any) -> None:
        """
        Interactive command & control handler for Direct Messages (DMs).
        Enforces admin authorization and processes commands:
          /start, /help, /status, /scan <CA>, /trades, or raw CA pasting.
        """
        if not getattr(event, "is_private", False):
            return

        sender_id = getattr(event, "sender_id", None)
        if sender_id is None:
            sender = getattr(event, "sender", None)
            sender_id = getattr(sender, "id", None)

        if not self._is_authorized(sender_id):
            logger.warning(
                "[TelegramIngester] Unauthorized DM access attempt from user %s. Ignored.",
                sender_id,
            )
            return

        raw_text = getattr(event, "raw_text", getattr(event, "text", "")) or ""
        text = raw_text.strip()
        if not text:
            return

        lower_text = text.lower()
        first_token = lower_text.split()[0].split("@")[0]

        if first_token in ("/start", "/help"):
            await self._cmd_start_help(event)
        elif first_token == "/status":
            await self._cmd_status(event)
        elif first_token == "/trades":
            await self._cmd_trades(event)
        elif first_token == "/news":
            await self._cmd_news(event)
        elif first_token == "/whales":
            await self._cmd_whales(event)
        elif first_token == "/signals":
            await self._cmd_signals(event)
        elif first_token == "/scan":
            args = text[len(text.split()[0]):].strip()
            await self._cmd_scan(event, args, sender_id)
        else:
            await self._cmd_direct_ca_or_help(event, text, sender_id)

    async def _cmd_start_help(self, event: Any) -> None:
        """Handle /start and /help command in DMs."""
        msg = (
            "🤖 **Bot-MM Alpha Engine & Trading Terminal**\n\n"
            "**Status:** Online 🟢\n"
            "**Engine Mode:** Paper Trading (Zero-Capital Simulation)\n\n"
            "**Available Commands:**\n"
            "• `/status` — View real-time system uptime, memory RSS, portfolio equity, PnL, and queue telemetry.\n"
            "• `/scan` — Submit a Solana or Base token CA for immediate security audit & AMM execution.\n"
            "• `/trades` — View active OPEN positions and executed CLOSED paper trades with entry/current prices and PnL.\n"
            "• `/news` — View the last 5 ingested news headlines, sources, time elapsed, and sentiment scores.\n"
            "• `/whales` — View the last 3 on-chain whale alerts from @lookonchain or @bubblemaps.\n"
            "• `/signals` — View the last 5 tokens evaluated by the gatekeeper and exact pass/reject reasons.\n"
            "• `/help` — Display this command reference.\n\n"
            "💡 *Tip:* You can also directly paste a contract address (EVM `0x...` or Solana Base58) in this chat to trigger an immediate scan."
        )
        await self._safe_reply(event, msg)

    async def _cmd_status(self, event: Any) -> None:
        """Handle /status command in DMs."""
        metrics: dict[str, Any] = {}
        if self._status_provider is not None:
            try:
                res = self._status_provider()
                if asyncio.iscoroutine(res):
                    res = await res
                if isinstance(res, dict):
                    metrics = res
            except Exception as exc:
                logger.warning("[TelegramIngester] status_provider error: %s", exc)

        # 1. System Uptime
        uptime_str = metrics.get("uptime_str")
        if not uptime_str:
            uptime_s = time.time() - self._start_time
            hrs, rem = divmod(int(uptime_s), 3600)
            mins, secs = divmod(rem, 60)
            uptime_str = f"{hrs:02d}h {mins:02d}m {secs:02d}s"

        # 2. Memory RSS
        rss_mb = metrics.get("rss_mb")
        if rss_mb is None:
            rss_mb = 0.0
            if PSUTIL_AVAILABLE and psutil is not None:
                try:
                    rss_mb = psutil.Process().memory_info().rss / (1024 * 1024)
                except Exception:
                    pass
            else:
                try:
                    import resource
                    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
                except Exception:
                    pass

        # 3. Portfolio & Ledger stats
        equity_usd = metrics.get("equity_usd")
        realized_pnl = metrics.get("realized_pnl_usd")
        open_positions = metrics.get("open_positions")
        wr = metrics.get("win_rate_pct")
        mdd = metrics.get("max_drawdown_pct")

        if equity_usd is None and AIOSQLITE_AVAILABLE and aiosqlite is not None:
            try:
                async with aiosqlite.connect(self._db_path) as db:
                    async with db.execute(
                        "SELECT total_equity_usd, realized_pnl_usd, open_positions, win_rate_pct, max_drawdown_pct "
                        "FROM portfolio_snapshots ORDER BY created_at DESC LIMIT 1"
                    ) as cur:
                        row = await cur.fetchone()
                        if row:
                            equity_usd = Decimal(str(row[0]))
                            realized_pnl = Decimal(str(row[1]))
                            open_positions = int(row[2])
                            wr = float(row[3])
                            mdd = float(row[4])
            except Exception as exc:
                logger.debug("[TelegramIngester] DB status query fallback error: %s", exc)

        equity_str = f"${float(equity_usd):.2f}" if equity_usd is not None else "$8300.00"
        pnl_str = f"${float(realized_pnl):.2f}" if realized_pnl is not None else "$0.00"
        open_lots_val = open_positions if open_positions is not None else 0
        wr_str = f"{float(wr):.1f}%" if wr is not None else "0.0%"
        mdd_str = f"{float(mdd):.2f}%" if mdd is not None else "0.00%"

        # 4. Queue backlogs
        ingest_q_size = metrics.get("ingest_q_size", self._queue.qsize())
        ingest_q_max = metrics.get("ingest_q_max", getattr(self._queue, "maxsize", 100))
        signal_q_size = metrics.get("signal_q_size", 0)
        signal_q_max = metrics.get("signal_q_max", 100)

        msg = (
            "📊 **Bot-MM Engine Status Report**\n\n"
            f"⏱ **Uptime:** `{uptime_str}`\n"
            f"🧠 **Memory RSS:** `{float(rss_mb):.1f} MB`\n"
            f"💰 **Portfolio Equity:** `{equity_str}`\n"
            f"📈 **Realized PnL:** `{pnl_str}`\n"
            f"📦 **Active Lots:** `{open_lots_val}`\n"
            f"🎯 **Win Rate (WR):** `{wr_str}`\n"
            f"📉 **Max Drawdown (MDD):** `{mdd_str}`\n\n"
            "🚦 **Queue Backlog:**\n"
            f"   • Ingest: `{ingest_q_size}/{ingest_q_max}`\n"
            f"   • Signal: `{signal_q_size}/{signal_q_max}`\n\n"
            "⚡ **Telemetry:** Normal 🟢"
        )
        await self._safe_reply(event, msg)

    async def _cmd_scan(self, event: Any, args: str, sender_id: Optional[int]) -> None:
        """Handle /scan <CONTRACT_ADDRESS> command."""
        if not args:
            await self._safe_reply(
                event,
                "ℹ️ **Usage:** `/scan <CONTRACT_ADDRESS>`\n\n"
                "• Base (EVM): 42-char hex string starting with `0x`\n"
                "• Solana (SVM): 32-44 char Base58 address\n\n"
                "Example:\n`/scan 0x285617313860407d647990b50375990264186566`",
            )
            return

        cas = self.extract_contract_addresses(args)
        if not cas:
            is_evm_syntax = bool(EVM_CA_REGEX.search(args))
            is_svm_syntax = bool(SVM_CA_REGEX.search(args))
            if is_evm_syntax or is_svm_syntax:
                await self._safe_reply(
                    event,
                    "⚠️ Address is a recognized system program or DEX infrastructure router. Trade scanning rejected.",
                )
            else:
                await self._safe_reply(
                    event,
                    "❌ Invalid Contract Address. Please provide a valid Base EVM (42 hex chars starting with 0x) or Solana (32-44 base58 chars) address.",
                )
            return

        for chain, ca in cas:
            raw_signal = RawSignalEvent(
                timestamp_ns=time.time_ns(),
                chain=chain,
                token_address=ca,
                source=SignalSource.TELEGRAM_SCRAPER,
                originating_channel=f"DM:{sender_id or 'admin'}",
                channel_id=int(sender_id or 0),
                message_id=int(getattr(event, "id", 0) or 0),
                raw_text=args,
                status=NewsSignalStatus.VALID,
                sybil_channel_count=1,
                is_edit=False,
            )
            logger.info(
                "[TelegramIngester] Admin DM scan dispatched: %s on %s by user %s",
                ca,
                chain.value,
                sender_id,
            )
            await self._enqueue_event(raw_signal)
            await self._safe_reply(
                event,
                f"🔎 Ingested CA: `{ca}` | Dispatching to SecurityGatekeeper & AMM Math Engine...",
            )

    async def _cmd_direct_ca_or_help(self, event: Any, text: str, sender_id: Optional[int]) -> None:
        """Handle raw messages containing contract addresses in DM."""
        cas = self.extract_contract_addresses(text)
        if cas:
            for chain, ca in cas:
                raw_signal = RawSignalEvent(
                    timestamp_ns=time.time_ns(),
                    chain=chain,
                    token_address=ca,
                    source=SignalSource.TELEGRAM_SCRAPER,
                    originating_channel=f"DM:{sender_id or 'admin'}",
                    channel_id=int(sender_id or 0),
                    message_id=int(getattr(event, "id", 0) or 0),
                    raw_text=text,
                    status=NewsSignalStatus.VALID,
                    sybil_channel_count=1,
                    is_edit=False,
                )
                logger.info(
                    "[TelegramIngester] Direct DM CA dispatched: %s on %s by user %s",
                    ca,
                    chain.value,
                    sender_id,
                )
                await self._enqueue_event(raw_signal)
                await self._safe_reply(
                    event,
                    f"🔎 Ingested CA: `{ca}` | Dispatching to SecurityGatekeeper & AMM Math Engine...",
                )
        else:
            if text.startswith("/"):
                await self._safe_reply(
                    event,
                    f"❓ Unknown command: `{text.split()[0]}`. Use `/help` to see available commands.",
                )

    @staticmethod
    def _format_price(val: Any) -> str:
        try:
            f = float(val)
            if f <= 0.0:
                return "0.0"
            if f >= 1.0:
                return f"{f:.4f}"
            if f >= 0.0001:
                return f"{f:.6f}"
            return f"{f:.3e}"
        except Exception:
            return str(val)[:10]

    async def _cmd_news(self, event: Any) -> None:
        """Handle /news command in DMs: returns last 5 ingested news headlines, channel name, elapsed time, and sentiment score."""
        metrics: dict[str, Any] = {}
        if self._status_provider is not None:
            try:
                res = self._status_provider()
                if asyncio.iscoroutine(res):
                    res = await res
                if isinstance(res, dict):
                    metrics = res
            except Exception as exc:
                logger.warning("[TelegramIngester] status_provider error in /news: %s", exc)

        recent_news = metrics.get("recent_news") or list(self._recent_news)
        if not recent_news:
            await self._safe_reply(event, "📰 No news items ingested yet. Awaiting live RSS / X / Telegram news feeds.")
            return

        items = list(reversed(recent_news))[:5]
        lines = [
            "📰 **Last 5 Ingested News Headlines & Sentiment**\n",
            "```",
            f"{'TIME':<8} | {'SRC':<14} | {'SENT':<6} | {'HEADLINE'}",
            "-" * 65,
        ]
        for item in items:
            t_s = self._format_time_elapsed(item.get("timestamp", time.time()))
            src = str(item.get("source", "NEWS"))[:14]
            score = float(item.get("sentiment", 0.0))
            sent_str = f"{score:+.2f}"
            headline = str(item.get("headline", "")).replace("\n", " ")[:32]
            lines.append(f"{t_s:<8} | {src:<14} | {sent_str:<6} | {headline}")
        lines.append("```")
        await self._safe_reply(event, "\n".join(lines))

    async def _cmd_whales(self, event: Any) -> None:
        """Handle /whales command in DMs: displays last 3 on-chain alerts parsed from @lookonchain or @bubblemaps."""
        metrics: dict[str, Any] = {}
        if self._status_provider is not None:
            try:
                res = self._status_provider()
                if asyncio.iscoroutine(res):
                    res = await res
                if isinstance(res, dict):
                    metrics = res
            except Exception as exc:
                logger.warning("[TelegramIngester] status_provider error in /whales: %s", exc)

        recent_whales = metrics.get("recent_whales") or list(self._recent_whales)
        if not recent_whales:
            await self._safe_reply(event, "🐋 No on-chain whale alerts recorded yet from @lookonchain or @bubblemaps.")
            return

        items = list(reversed(recent_whales))[:3]
        lines = [
            "🐋 **Last 3 On-Chain Whale & Smart Money Alerts**\n",
            "```",
            f"{'TIME':<8} | {'SOURCE':<14} | {'TOKEN':<12} | {'ACTION'}",
            "-" * 55,
        ]
        for item in items:
            t_s = self._format_time_elapsed(item.get("timestamp", time.time()))
            src = str(item.get("source", "@lookonchain"))[:14]
            tok = str(item.get("token", "N/A"))
            short_tok = f"{tok[:4]}..{tok[-4:]}" if len(tok) > 10 else tok
            action = str(item.get("action", "ALERT"))[:15]
            lines.append(f"{t_s:<8} | {src:<14} | {short_tok:<12} | {action}")
        lines.append("```")

        for idx, item in enumerate(items, 1):
            summary = item.get("summary") or item.get("headline", "")
            if summary:
                lines.append(f"\n*{idx}.* `{item.get('source', '')}`: {summary}")

        await self._safe_reply(event, "\n".join(lines))

    async def _cmd_signals(self, event: Any) -> None:
        """Handle /signals command in DMs: shows last 5 tokens evaluated by gatekeeper and exact reason."""
        metrics: dict[str, Any] = {}
        if self._status_provider is not None:
            try:
                res = self._status_provider()
                if asyncio.iscoroutine(res):
                    res = await res
                if isinstance(res, dict):
                    metrics = res
            except Exception as exc:
                logger.warning("[TelegramIngester] status_provider error in /signals: %s", exc)

        recent_signals = metrics.get("recent_signals", [])
        if not recent_signals:
            await self._safe_reply(event, "🎯 No security evaluations recorded yet. Awaiting incoming token signals.")
            return

        items = list(reversed(recent_signals))[:5]
        lines = [
            "🎯 **Last 5 Token Security Screenings**\n",
            "```",
            f"{'TOKEN':<12} | {'CHAIN':<6} | {'STATUS':<6} | {'REASON'}",
            "-" * 65,
        ]
        for item in items:
            token_str = str(item.get("token_address", ""))
            short_token = f"{token_str[:4]}..{token_str[-4:]}" if len(token_str) > 10 else token_str
            chain = str(item.get("chain", "")).replace("ChainIdentifier.", "").replace("_mainnet", "")[:6]
            passed = item.get("passed", False)
            status_str = "PASS 🟢" if passed else "REJ 🔴"
            reason = str(item.get("reason", "N/A"))[:32]
            lines.append(f"{short_token:<12} | {chain:<6} | {status_str:<6} | {reason}")
        lines.append("```")
        await self._safe_reply(event, "\n".join(lines))

    async def _cmd_trades(self, event: Any) -> None:
        """Handle /trades command in DMs: displays entry price, current price, unrealized PnL for OPEN trades, and realized PnL for CLOSED trades."""
        metrics: dict[str, Any] = {}
        if self._status_provider is not None:
            try:
                res = self._status_provider()
                if asyncio.iscoroutine(res):
                    res = await res
                if isinstance(res, dict):
                    metrics = res
            except Exception as exc:
                logger.warning("[TelegramIngester] status_provider error in /trades: %s", exc)

        open_trades = metrics.get("open_trades", [])

        # Fetch executed trades from DB
        trade_rows: list[Any] = []
        if AIOSQLITE_AVAILABLE and aiosqlite is not None:
            try:
                async with aiosqlite.connect(self._db_path) as db:
                    cursor = await db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('trades', 'paper_trades')"
                    )
                    table_row = await cursor.fetchone()
                    if table_row:
                        table_name = table_row[0]
                        async with db.execute(
                            f"SELECT chain, token_address, side, effective_price, realized_pnl_usd "
                            f"FROM {table_name} "
                            f"ORDER BY created_at DESC LIMIT 5"
                        ) as cur:
                            trade_rows = await cur.fetchall()
            except Exception as exc:
                logger.debug("[TelegramIngester] DB trades query error: %s", exc)

        if not open_trades and not trade_rows:
            await self._safe_reply(event, "📋 No executed paper trades recorded in ledger yet.")
            return

        response_sections = []

        if open_trades:
            open_lines = [
                f"🟢 **Active Open Positions ({len(open_trades)})**\n",
                "```",
                f"{'TOKEN':<12} | {'CHAIN':<6} | {'ENTRY':<10} | {'CURRENT':<10} | {'UNREALIZED'}",
                "-" * 60,
            ]
            for t in open_trades[:5]:
                token_str = str(t.get("token_address", ""))
                short_token = f"{token_str[:4]}..{token_str[-4:]}" if len(token_str) > 10 else token_str
                chain_str = str(t.get("chain", "")).replace("ChainIdentifier.", "").replace("_mainnet", "")[:6]
                entry_p = self._format_price(t.get("entry_price", 0))
                curr_p = self._format_price(t.get("current_price", 0))
                pnl_f = float(t.get("unrealized_pnl_usd", 0.0))
                pnl_str = f"+${pnl_f:.2f}" if pnl_f > 0 else f"-${abs(pnl_f):.2f}" if pnl_f < 0 else "$0.00"
                open_lines.append(
                    f"{short_token:<12} | {chain_str:<6} | {entry_p:<10} | {curr_p:<10} | {pnl_str}"
                )
            open_lines.append("```")
            response_sections.append("\n".join(open_lines))

        if trade_rows:
            lines = [
                "📋 **Last 5 Executed Paper Trades**\n",
                "```",
                f"{'TOKEN':<12} | {'CHAIN':<6} | {'SIDE':<4} | {'FILL PRICE':<12} | {'PNL':<9}",
                "-" * 53,
            ]
            for r in trade_rows:
                chain_str = str(r[0]).replace("_mainnet", "")[:6]
                token_str = str(r[1])
                short_token = f"{token_str[:4]}..{token_str[-4:]}" if len(token_str) > 10 else token_str
                side_str = str(r[2]).upper()
                fill_p = self._format_price(r[3])
                pnl_raw = r[4]
                if pnl_raw is not None and str(pnl_raw) != "":
                    try:
                        pnl_f = float(pnl_raw)
                        pnl_str = f"+${pnl_f:.2f}" if pnl_f > 0 else f"-${abs(pnl_f):.2f}" if pnl_f < 0 else "$0.00"
                    except Exception:
                        pnl_str = str(pnl_raw)
                else:
                    pnl_str = "OPEN"

                lines.append(
                    f"{short_token:<12} | {chain_str:<6} | {side_str:<4} | {fill_p:<12} | {pnl_str:<9}"
                )
            lines.append("```")
            response_sections.append("\n".join(lines))

        await self._safe_reply(event, "\n\n".join(response_sections))

    # -------------------------------------------------------------------------
    # 3. Lifecycle & Connection Management
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """
        Initialize and connect the Telethon TelegramClient.
        Registers event listeners for channel messages and admin DMs.
        """
        if self._is_dormant:
            logger.info("TelegramIngester starting in dormant mode (no credentials provided).")
            self._running = True
            return

        if self._client is None and TELETHON_AVAILABLE:
            assert self._api_id is not None and self._api_hash is not None
            session: Any = self._session_name
            if self._session_string:
                session = StringSession(self._session_string)
            self._client = TelegramClient(session, self._api_id, self._api_hash)

        if self._client is not None:
            # 1. Start and authenticate client with exponential backoff on FloodWaitError / disconnect
            max_retries = 5
            backoff = 2.0
            for attempt in range(max_retries):
                try:
                    if hasattr(self._client, "start"):
                        start_kwargs = {}
                        if self._bot_token:
                            start_kwargs["bot_token"] = self._bot_token
                        start_res = self._client.start(**start_kwargs)
                        if asyncio.iscoroutine(start_res):
                            await start_res
                    break
                except FloodWaitError as exc:
                    wait_sec = getattr(exc, "seconds", backoff)
                    logger.warning(
                        "[TelegramIngester] FloodWaitError during start. Waiting %s seconds (attempt %d/%d).",
                        wait_sec, attempt + 1, max_retries
                    )
                    await asyncio.sleep(wait_sec)
                    backoff = min(backoff * 2, 60.0)
                except (ConnectionError, OSError, RPCError) as exc:
                    logger.warning(
                        "[TelegramIngester] Connection error during start: %s. Retrying in %.1fs (attempt %d/%d).",
                        exc, backoff, attempt + 1, max_retries
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)
                except Exception as exc:
                    logger.error("[TelegramIngester] Unexpected error starting TelegramClient: %s", exc)
                    raise

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
                    clean_ch = ch
                    if hasattr(self._client, "get_input_entity"):
                        try:
                            entity_res = self._client.get_input_entity(ch)
                            if asyncio.iscoroutine(entity_res):
                                entity = await entity_res
                            else:
                                entity = entity_res
                            resolved_chats.append(entity)
                            logger.info("[TelegramIngester] Target channel '%s' resolved successfully.", ch)
                        except FloodWaitError as exc:
                            logger.warning(
                                "[TelegramIngester] FloodWaitError resolving channel '%s' (%s s). Skipping.",
                                ch, getattr(exc, "seconds", "?")
                            )
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

            # 3. Register dual-mode event handlers
            if hasattr(self._client, "add_event_handler") and TELETHON_AVAILABLE:
                # Passive Channel Scraping
                self._client.add_event_handler(
                    self._handle_channel_message,
                    events.NewMessage(chats=chats_arg),
                )
                self._client.add_event_handler(
                    self._handle_channel_edited_message,
                    events.MessageEdited(chats=chats_arg),
                )
                # Interactive DM Interface
                self._client.add_event_handler(
                    self._handle_dm_message,
                    events.NewMessage(func=lambda e: bool(getattr(e, "is_private", False))),
                )

            self._running = True
            logger.info("TelegramIngester connected: dual-mode active (passive channel scraping + interactive DM control).")

    async def stop(self, send_sentinel: bool = False) -> None:
        """Gracefully disconnect and tear down the client."""
        self._shutdown_event.set()
        self._running = False

        if send_sentinel and self._queue is not None:
            await self._queue.put(ShutdownSentinel())

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
