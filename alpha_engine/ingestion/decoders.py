"""
alpha_engine.ingestion.decoders — On-Chain Log Decoders & Normalization
=======================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import base64
import logging
import struct
import time
from decimal import Decimal
from typing import Any

from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.models.events import PoolStateUpdateEvent, SwapEvent
from alpha_engine.models.state import PoolState

logger = logging.getLogger(__name__)

# Uniswap v2 / Aerodrome Swap event topic
_SWAP_TOPIC: str = (
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
)

# Uniswap v2 / Aerodrome Sync event topic
_SYNC_TOPIC: str = (
    "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
)

# EVM: pool_address → (token0, token1, decimals0, decimals1, native_token)
EvmPoolMeta = tuple[str, str, int, int, str]

# SVM: pool_pubkey → (coin_mint, pc_mint, coin_decimals, pc_decimals)
SvmPoolMeta = tuple[str, str, int, int]


def _normalise(raw_int: int, decimals: int) -> Decimal:
    """Normalise a raw integer amount (Wei / Lamport) to human-scale Decimal."""
    if decimals < 0:
        raise ValueError(f"decimals must be >= 0, got {decimals}")
    divisor = Decimal(10) ** decimals
    return Decimal(raw_int) / divisor


def _decode_evm_swap_log(
    log: dict[str, Any],
    token0: str,
    token1: str,
    decimals0: int,
    decimals1: int,
    native_token: str,
) -> SwapEvent | None:
    """Decode a raw eth_subscribe Swap log into a SwapEvent."""
    try:
        topics: list[str] = log.get("topics", [])
        if len(topics) < 3:
            return None
        if topics[0].lower() != _SWAP_TOPIC:
            return None

        sender = "0x" + topics[1][-40:]

        data_hex = log.get("data", "0x")[2:]
        if len(data_hex) < 256:
            logger.debug("Swap log data too short: %d chars", len(data_hex))
            return None

        def _u256(chunk: str) -> int:
            return int(chunk, 16)

        amount0_in_raw = _u256(data_hex[0:64])
        amount1_in_raw = _u256(data_hex[64:128])
        amount0_out_raw = _u256(data_hex[128:192])
        amount1_out_raw = _u256(data_hex[192:256])

        if amount0_in_raw > 0 and amount1_out_raw > 0:
            token_in = token0
            token_out = token1
            amount_in = _normalise(amount0_in_raw, decimals0)
            amount_out = _normalise(amount1_out_raw, decimals1)
        elif amount1_in_raw > 0 and amount0_out_raw > 0:
            token_in = token1
            token_out = token0
            amount_in = _normalise(amount1_in_raw, decimals1)
            amount_out = _normalise(amount0_out_raw, decimals0)
        else:
            logger.debug("Ambiguous Swap direction: tx=%s", log.get("transactionHash"))
            return None

        pool_address = log.get("address", "").lower()
        tx_hash = log.get("transactionHash", "")
        block_hex = log.get("blockNumber", "0x0")
        block_number = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
        log_index = int(log.get("logIndex", "0x0"), 16)

        return SwapEvent(
            timestamp_ns=time.time_ns(),
            block_number=block_number,
            chain=ChainIdentifier.BASE_MAINNET,
            pool_address=pool_address,
            token_in=token_in.lower(),
            token_out=token_out.lower(),
            amount_in=amount_in,
            amount_out=amount_out,
            sender=sender.lower(),
            tx_hash=tx_hash.lower(),
            log_index=log_index,
        )
    except (KeyError, ValueError, IndexError, struct.error) as exc:
        logger.debug("Failed to decode EVM Swap log: %s", exc)
        return None


def _decode_evm_sync_log(
    log: dict[str, Any],
    pool_address: str,
    token0: str,
    token1: str,
    decimals0: int,
    decimals1: int,
    native_token: str,
    existing_pool: PoolState | None,
) -> PoolStateUpdateEvent | None:
    """Decode a Uniswap v2 / Aerodrome Sync log into a PoolStateUpdateEvent."""
    try:
        topics: list[str] = log.get("topics", [])
        if not topics or topics[0].lower() != _SYNC_TOPIC:
            return None

        data_hex = log.get("data", "0x")[2:]
        if len(data_hex) < 128:
            logger.debug("Sync log data too short: %d chars", len(data_hex))
            return None

        reserve0_raw = int(data_hex[0:64], 16)
        reserve1_raw = int(data_hex[64:128], 16)

        reserve0 = _normalise(reserve0_raw, decimals0)
        reserve1 = _normalise(reserve1_raw, decimals1)

        if token0.lower() == native_token.lower():
            native_reserve = reserve0
            token_reserve = reserve1
            token_decimals = decimals1
        else:
            native_reserve = reserve1
            token_reserve = reserve0
            token_decimals = decimals0

        block_hex = log.get("blockNumber", "0x0")
        block_number = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)

        fee_num = existing_pool.fee_numerator if existing_pool else 3
        fee_denom = existing_pool.fee_denominator if existing_pool else 1000

        new_pool = PoolState(
            pool_address=pool_address,
            chain=ChainIdentifier.BASE_MAINNET,
            native_reserve=native_reserve,
            token_reserve=token_reserve,
            fee_numerator=fee_num,
            fee_denominator=fee_denom,
            last_updated_block=block_number,
            token_decimals=token_decimals,
            native_decimals=18,
        )

        return PoolStateUpdateEvent(
            timestamp_ns=time.time_ns(),
            chain=ChainIdentifier.BASE_MAINNET,
            pool_address=pool_address,
            new_pool_state=new_pool,
        )
    except (KeyError, ValueError, IndexError) as exc:
        logger.debug("Failed to decode EVM Sync log: %s", exc)
        return None


def _parse_raydium_log_line(line: str) -> dict[str, Any] | None:
    """Parse a Raydium V4 'ray_log:' instruction log line."""
    prefix = "Program log: ray_log: "
    if not line.startswith(prefix):
        return None

    b64_data = line[len(prefix):].strip()
    try:
        raw_bytes = base64.b64decode(b64_data)
    except Exception:
        return None

    if len(raw_bytes) < 57:
        return None

    log_type = raw_bytes[0]
    if log_type != 3:
        return None

    try:
        amount_in = struct.unpack_from("<Q", raw_bytes, 1)[0]
        direction = struct.unpack_from("<Q", raw_bytes, 17)[0]
        pool_coin = struct.unpack_from("<Q", raw_bytes, 33)[0]
        pool_pc = struct.unpack_from("<Q", raw_bytes, 41)[0]
        amount_out = struct.unpack_from("<Q", raw_bytes, 49)[0]
    except struct.error:
        return None

    return {
        "amount_in": amount_in,
        "amount_out": amount_out,
        "direction": direction,
        "pool_coin_reserve": pool_coin,
        "pool_pc_reserve": pool_pc,
    }
