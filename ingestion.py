"""
ingestion.py — Resilient Dual-Chain Event Streaming (Compatibility Facade)
==========================================================================
Re-exports all ingestion streaming and decoding definitions from alpha_engine.ingestion.
"""

from alpha_engine.ingestion import (
    _SWAP_TOPIC,
    _SYNC_TOPIC,
    DEXMetricsAggregator,
    EVMIngester,
    EvmPoolMeta,
    ExponentialBackoff,
    IngestionCoordinator,
    SVMIngester,
    SvmPoolMeta,
    TelegramIngester,
    XStreamIngester,
    _decode_evm_swap_log,
    _decode_evm_sync_log,
    _normalise,
    _parse_raydium_log_line,
)

__all__ = [
    "EvmPoolMeta",
    "SvmPoolMeta",
    "_normalise",
    "ExponentialBackoff",
    "_decode_evm_swap_log",
    "_decode_evm_sync_log",
    "_parse_raydium_log_line",
    "EVMIngester",
    "SVMIngester",
    "TelegramIngester",
    "XStreamIngester",
    "IngestionCoordinator",
    "DEXMetricsAggregator",
    "_SWAP_TOPIC",
    "_SYNC_TOPIC",
]
