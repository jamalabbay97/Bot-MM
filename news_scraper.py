"""
news_scraper.py — Telegram & Social Media News Scraper (Facade)
==============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | telethon | asyncio

Module B:
- Dual-chain Contract Address (CA) extraction (Base EVM & Solana SVM).
- Anti-Sybil Coordinated Shill Filter (sliding window 60s, burst >= 3 channels in <= 15s).
- Anti Bait-and-Switch Protection (events.MessageEdited rejection).
- FloodWaitError backoff and rate limiter integration.
- Non-crashing dormant fallback mode.
"""

from alpha_engine.ingestion.telegram import (
    EVM_CA_REGEX,
    EVM_SYSTEM_ADDRESSES,
    SOLANA_SYSTEM_PROGRAM_IDS,
    SVM_CA_REGEX,
    TelegramIngester,
)
from alpha_engine.models import (
    ChainIdentifier,
    NewsSignalEvent,
    NewsSignalStatus,
    RawSignalEvent,
    SignalSource,
    TelegramMessage,
)

__all__ = [
    "TelegramIngester",
    "RawSignalEvent",
    "NewsSignalEvent",
    "NewsSignalStatus",
    "SignalSource",
    "TelegramMessage",
    "ChainIdentifier",
    "EVM_CA_REGEX",
    "SVM_CA_REGEX",
    "SOLANA_SYSTEM_PROGRAM_IDS",
    "EVM_SYSTEM_ADDRESSES",
]
