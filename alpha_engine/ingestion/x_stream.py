"""
alpha_engine.ingestion.x_stream — Autonomous X (Twitter) Sentiment & Discovery Stream
=====================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio + aiohttp | Anti-Sybil & Engagement Velocity Algorithm
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional, Sequence, Set

import random
import aiohttp

from alpha_engine.models.enums import ChainIdentifier, NewsSignalStatus, SignalSource
from alpha_engine.models.events import RawSignalEvent, ShutdownSentinel
from alpha_engine.models.news import NewsSignalEvent
from alpha_engine.rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)


class ExponentialBackoff:
    """Stateful exponential backoff with ±50% uniform jitter."""

    def __init__(
        self,
        initial: float = 1.0,
        multiplier: float = 2.0,
        max_delay: float = 60.0,
        jitter: float = 0.5,
    ) -> None:
        self._initial = initial
        self._multiplier = multiplier
        self._max_delay = max_delay
        self._jitter = jitter
        self._attempt = 0

    def next_delay(self) -> float:
        base = min(self._initial * (self._multiplier ** self._attempt), self._max_delay)
        jitter_delta = base * self._jitter * (2 * random.random() - 1)
        delay = max(0.0, base + jitter_delta)
        self._attempt += 1
        return delay

# Target Extraction Patterns
SVM_CA_REGEX = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
EVM_CA_REGEX = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
TICKER_REGEX = re.compile(r"\$([A-Za-z0-9]{2,10})\b")

# Anti-Sybil & Botnet Filters
MIN_ACCOUNT_AGE_DAYS: float = 90.0
MIN_FOLLOWERS_COUNT: int = 1_000
MAX_DUPLICATE_RETWEET_RATIO: float = 15.0  # Retweets vs organic comments/quotes

BOT_FARM_KEYWORDS: frozenset[str] = frozenset({
    "airdrop bot",
    "send dm to promote",
    "fast pump telegram",
    "1000x call join",
    "free solana giveaway",
    "guaranteed 100x",
    "pump and dump",
    "t.me/pump",
    "t.me/crypto",
    "dm for promo",
    "automated raid",
})

SOLANA_SYSTEM_PROGRAMS: frozenset[str] = frozenset({
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
    "ComputeBudget111111111111111111111111111111",
    "So11111111111111111111111111111111111111112",
})

EVM_SYSTEM_ADDRESSES: frozenset[str] = frozenset({
    "0x0000000000000000000000000000000000000000",
    "0x000000000000000000000000000000000000dead",
    "0x4200000000000000000000000000000000000006",  # WETH on Base
})


@dataclass(frozen=True, init=False)
class TweetPayload:
    """Normalized payload representing a tweet scanned from the X stream."""

    tweet_id: str
    author_id: str
    author_username: str
    account_age_days: float
    followers_count: int
    text: str
    created_at_timestamp: float = 0.0
    impressions_count: int = 0
    retweet_count: int = 0
    reply_count: int = 0
    quote_count: int = 0

    def __init__(
        self,
        tweet_id: str,
        author_id: str,
        author_username: str,
        account_age_days: float,
        followers_count: int,
        text: str,
        created_at_timestamp: float = 0.0,
        impressions_count: int = 0,
        retweet_count: int = 0,
        reply_count: int = 0,
        quote_count: int = 0,
        retweets_count: Optional[int] = None,
        replies_count: Optional[int] = None,
        quotes_count: Optional[int] = None,
    ) -> None:
        object.__setattr__(self, "tweet_id", tweet_id)
        object.__setattr__(self, "author_id", author_id)
        object.__setattr__(self, "author_username", author_username)
        object.__setattr__(self, "account_age_days", account_age_days)
        object.__setattr__(self, "followers_count", followers_count)
        object.__setattr__(self, "text", text)
        object.__setattr__(self, "created_at_timestamp", created_at_timestamp)
        object.__setattr__(self, "impressions_count", impressions_count)
        object.__setattr__(
            self,
            "retweet_count",
            retweets_count if retweets_count is not None else retweet_count,
        )
        object.__setattr__(
            self,
            "reply_count",
            replies_count if replies_count is not None else reply_count,
        )
        object.__setattr__(
            self,
            "quote_count",
            quotes_count if quotes_count is not None else quote_count,
        )


@dataclass(frozen=True)
class EngagementEvaluation:
    """Result of the engagement velocity algorithm and botnet checks."""

    is_organic: bool
    organic_score: float
    rejection_reason: Optional[str] = None
    bot_farm_flagged: bool = False
    extracted_solana_cas: list[str] = None  # type: ignore
    extracted_evm_cas: list[str] = None     # type: ignore
    extracted_tickers: list[str] = None    # type: ignore

    @property
    def passes_sybil_filter(self) -> bool:
        return self.is_organic

    @property
    def solana_ca(self) -> Optional[str]:
        return self.extracted_solana_cas[0] if self.extracted_solana_cas else None

    @property
    def ticker(self) -> Optional[str]:
        return self.extracted_tickers[0] if self.extracted_tickers else None

    @property
    def velocity_score(self) -> float:
        return self.organic_score

    @property
    def score_decimal(self) -> Decimal:
        return Decimal(str(round(self.organic_score, 4)))


class XStreamIngester:
    """
    Autonomous X (Twitter) Sentiment & Discovery Stream.
    Monitors high-velocity alpha mentions, applies Anti-Sybil & Botnet
    Engagement Velocity filtering, and pushes clean RawSignalEvents into the pipeline.
    """

    def __init__(
        self,
        event_queue: asyncio.Queue[RawSignalEvent | ShutdownSentinel | Any],
        bearer_token: Optional[str] = None,
        proxy_pool: Optional[Sequence[str]] = None,
        limiter: Optional[RateLimiterRegistry] = None,
        poll_interval_s: float = 5.0,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self._queue = event_queue
        self._bearer_token = bearer_token
        self._proxy_pool = list(proxy_pool) if proxy_pool else []
        self._proxy_index = 0
        self._limiter = limiter
        self._poll_interval = poll_interval_s
        self._session = session
        self._running = False
        self._blacklisted_symbols: Set[str] = set()
        self._seen_tweet_ids: Set[str] = set()
        self._backoff = ExponentialBackoff()

    def to_news_signal_event(
        self,
        tweet: TweetPayload,
        token_address: str,
        chain: ChainIdentifier,
    ) -> NewsSignalEvent:
        """Convert an organic tweet into a structured NewsSignalEvent."""
        return NewsSignalEvent(
            timestamp_ns=int(getattr(tweet, "created_at_timestamp", time.time()) * 1e9),
            token_address=token_address,
            chain=chain,
            originating_channel=f"@{tweet.author_username}",
            channel_id=int(tweet.author_id) if tweet.author_id.isdigit() else 0,
            message_id=int(tweet.tweet_id) if tweet.tweet_id.isdigit() else 0,
            status=NewsSignalStatus.VALID,
            raw_text=tweet.text,
        )

    def is_symbol_blacklisted(self, symbol: str) -> bool:
        """Check if a token symbol/ticker is currently blacklisted."""
        return symbol.upper() in self._blacklisted_symbols

    def get_next_proxy(self) -> Optional[str]:
        """Rotate proxies round-robin."""
        if not self._proxy_pool:
            return None
        proxy = self._proxy_pool[self._proxy_index % len(self._proxy_pool)]
        self._proxy_index += 1
        return proxy

    def evaluate_tweet(self, tweet: TweetPayload) -> EngagementEvaluation:
        """
        Evaluate tweet authenticity using anti-sybil and engagement velocity rules:
        1. Account age >= 90 days
        2. Followers >= 1,000
        3. Bot-farm keyword detection (triggers blacklisting of symbol)
        4. Engagement Velocity Algorithm (quotes/replies vs duplicate retweets)
        """
        # 1. Account Age Filter (< 90 days discarded)
        if tweet.account_age_days < MIN_ACCOUNT_AGE_DAYS:
            return EngagementEvaluation(
                is_organic=False,
                organic_score=0.0,
                rejection_reason=f"Young account: age {tweet.account_age_days:.1f}d < {MIN_ACCOUNT_AGE_DAYS}d",
                bot_farm_flagged=False,
                extracted_solana_cas=[],
                extracted_evm_cas=[],
                extracted_tickers=[],
            )

        # 2. Organic Follower Filter (< 1,000 discarded)
        if tweet.followers_count < MIN_FOLLOWERS_COUNT:
            return EngagementEvaluation(
                is_organic=False,
                organic_score=0.0,
                rejection_reason=f"Insufficient followers: count {tweet.followers_count} < {MIN_FOLLOWERS_COUNT}",
                bot_farm_flagged=False,
                extracted_solana_cas=[],
                extracted_evm_cas=[],
                extracted_tickers=[],
            )

        text_lower = tweet.text.lower()
        extracted_tickers = [t.upper() for t in TICKER_REGEX.findall(tweet.text)]

        # 3. Bot Farm Keyword Detection
        for kw in BOT_FARM_KEYWORDS:
            if kw in text_lower:
                for ticker in extracted_tickers:
                    self._blacklisted_symbols.add(ticker)
                    logger.warning("Blacklisted token symbol due to bot-farm keyword '%s': %s", kw, ticker)
                return EngagementEvaluation(
                    is_organic=False,
                    organic_score=0.0,
                    rejection_reason=f"Bot farm keywords detected: '{kw}'",
                    bot_farm_flagged=True,
                    extracted_solana_cas=[],
                    extracted_evm_cas=[],
                    extracted_tickers=extracted_tickers,
                )

        # Reject if symbol previously blacklisted
        for ticker in extracted_tickers:
            if ticker in self._blacklisted_symbols:
                return EngagementEvaluation(
                    is_organic=False,
                    organic_score=0.0,
                    rejection_reason=f"Token symbol {ticker} is blacklisted",
                    bot_farm_flagged=True,
                    extracted_solana_cas=[],
                    extracted_evm_cas=[],
                    extracted_tickers=extracted_tickers,
                )

        # 4. Engagement Velocity Algorithm
        organic_engagement = (tweet.quote_count * 2.0) + (tweet.reply_count * 1.5)
        total_eng = tweet.retweet_count + tweet.reply_count + tweet.quote_count

        # Check for paid bot manipulation: huge impressions but near-zero genuine comments/quotes
        if tweet.impressions_count > 10_000 and organic_engagement < 2.0:
            return EngagementEvaluation(
                is_organic=False,
                organic_score=0.0,
                rejection_reason=f"Paid bot manipulation: {tweet.impressions_count} impressions with {organic_engagement:.1f} organic velocity",
                bot_farm_flagged=True,
                extracted_solana_cas=[],
                extracted_evm_cas=[],
                extracted_tickers=extracted_tickers,
            )

        # Duplicate retweet farm check
        if tweet.retweet_count > 30 and (tweet.retweet_count / max(1.0, organic_engagement)) > MAX_DUPLICATE_RETWEET_RATIO:
            return EngagementEvaluation(
                is_organic=False,
                organic_score=0.0,
                rejection_reason=f"Retweet farm detected: {tweet.retweet_count} RTs vs {organic_engagement:.1f} organic quotes/replies",
                bot_farm_flagged=True,
                extracted_solana_cas=[],
                extracted_evm_cas=[],
                extracted_tickers=extracted_tickers,
            )

        # 5. Extract CAs
        sol_cas = [
            ca for ca in SVM_CA_REGEX.findall(tweet.text)
            if ca not in SOLANA_SYSTEM_PROGRAMS and len(ca) >= 32
        ]
        evm_cas = [
            ca.lower() for ca in EVM_CA_REGEX.findall(tweet.text)
            if ca.lower() not in EVM_SYSTEM_ADDRESSES
        ]

        if not sol_cas and not evm_cas and not extracted_tickers:
            return EngagementEvaluation(
                is_organic=False,
                organic_score=0.0,
                rejection_reason="No valid CA or $TICKER found",
                bot_farm_flagged=False,
                extracted_solana_cas=[],
                extracted_evm_cas=[],
                extracted_tickers=[],
            )

        organic_score = min(1.0, (organic_engagement + 1.0) / (total_eng + 1.0))

        return EngagementEvaluation(
            is_organic=True,
            organic_score=organic_score,
            rejection_reason=None,
            bot_farm_flagged=False,
            extracted_solana_cas=sol_cas,
            extracted_evm_cas=evm_cas,
            extracted_tickers=extracted_tickers,
        )

    async def ingest_tweet(self, tweet: TweetPayload) -> list[RawSignalEvent]:
        """Evaluate a tweet and dispatch clean signals to the ingestion queue."""
        if tweet.tweet_id in self._seen_tweet_ids:
            return []
        self._seen_tweet_ids.add(tweet.tweet_id)
        if len(self._seen_tweet_ids) > 10_000:
            self._seen_tweet_ids.clear()

        evaluation = self.evaluate_tweet(tweet)
        if not evaluation.is_organic:
            logger.debug(
                "Tweet %s rejected: %s", tweet.tweet_id, evaluation.rejection_reason
            )
            return []

        generated_signals: list[RawSignalEvent] = []

        # Process Solana CAs
        for ca in evaluation.extracted_solana_cas:
            event = RawSignalEvent(
                chain=ChainIdentifier.SOLANA_MAINNET,
                token_address=ca,
                source=SignalSource.X_SENTIMENT,
                originating_channel=f"@{tweet.author_username}",
                raw_text=tweet.text,
                status=NewsSignalStatus.VALID,
            )
            await self._queue.put(event)
            generated_signals.append(event)
            logger.info("X-Stream Ingested Solana CA: %s from @%s", ca[:10], tweet.author_username)

        # Process EVM CAs
        for ca in evaluation.extracted_evm_cas:
            event = RawSignalEvent(
                chain=ChainIdentifier.BASE_MAINNET,
                token_address=ca,
                source=SignalSource.X_SENTIMENT,
                originating_channel=f"@{tweet.author_username}",
                raw_text=tweet.text,
                status=NewsSignalStatus.VALID,
            )
            await self._queue.put(event)
            generated_signals.append(event)
            logger.info("X-Stream Ingested EVM CA: %s from @%s", ca[:10], tweet.author_username)

        return generated_signals

    async def run(self) -> None:
        """Continuous execution loop with exponential backoff and proxy failover."""
        self._running = True
        logger.info("XStreamIngester started with Anti-Sybil and Engagement Velocity filters.")

        while self._running:
            try:
                # Polling / streaming stub: in live mode queries X v2 filtered stream or rotating scrapers
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                logger.info("XStreamIngester cancelled.")
                break
            except Exception as exc:
                delay = self._backoff.next_delay()
                logger.warning("XStreamIngester error: %s — retrying in %.1fs", exc, delay)
                await asyncio.sleep(delay)

    async def stop(self) -> None:
        """Gracefully terminate stream."""
        self._running = False
