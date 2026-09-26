"""
alpha_engine.ingestion — Resilient Dual-Chain Event Streaming
=============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
"""

from alpha_engine.ingestion.coordinator import (
    ExponentialBackoff,
    IngestionCoordinator,
)
from alpha_engine.ingestion.decoders import (
    _SWAP_TOPIC,
    _SYNC_TOPIC,
    EvmPoolMeta,
    SvmPoolMeta,
    _decode_evm_swap_log,
    _decode_evm_sync_log,
    _normalise,
    _parse_raydium_log_line,
)
from alpha_engine.ingestion.evm import EVMIngester
from alpha_engine.ingestion.svm import SVMIngester
from alpha_engine.ingestion.telegram import TelegramIngester

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
    "IngestionCoordinator",
    "_SWAP_TOPIC",
    "_SYNC_TOPIC",
]
