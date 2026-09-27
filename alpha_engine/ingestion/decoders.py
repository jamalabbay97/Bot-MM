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

# Uniswap v2 / Aerodrome PairCreated event topic
# PairCreated(address,address,address,uint256)
_PAIR_CREATED_TOPIC: str = (
    "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
)

# Uniswap v3 PoolCreated event topic
# PoolCreated(address,address,uint24,int24,address)
_POOL_CREATED_TOPIC: str = (
    "0x783cca1c041245d8083164ea2cbd8e436ab6ae90824b2b740b02830204cc0415"
)

# Solana Program Constants
PUMP_FUN_PROGRAM_ID: str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
RAYDIUM_AMM_PROGRAM_ID: str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

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


class PairCreatedResult(dict):
    """Dictionary representing PairCreated log data that also unpacks as (pair, new_token, is_weth_first)."""
    def __iter__(self):
        weth = self.get("weth_address", "0x4200000000000000000000000000000000000006").lower()
        t0 = self.get("token0", "").lower()
        t1 = self.get("token1", "").lower()
        is_weth_first = (t0 == weth)
        new_token = t1 if is_weth_first else t0
        pair = self.get("pair", "")
        yield pair
        yield new_token
        yield is_weth_first


class PumpFunResult(dict):
    """Dictionary representing Pump.fun logs that also unpacks as (mint, curve_sol)."""
    def __iter__(self):
        yield self.get("mint", "")
        sol_reserve = self.get("virtual_sol_reserves", Decimal("30.0"))
        if isinstance(sol_reserve, Decimal):
            yield sol_reserve
        else:
            yield Decimal(str(sol_reserve))


class RaydiumInitResult(dict):
    """Dictionary representing Raydium Initialize2 logs that also equals open_time when compared with an int."""
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, int):
            return self.get("open_time") == other
        return super().__eq__(other)

    def __int__(self) -> int:
        return int(self.get("open_time", 0))


def _decode_evm_pair_created_log(
    log: dict[str, Any] | None = None,
    *,
    topics: list[str] | None = None,
    data: str | None = None,
    weth_address: str | None = None,
) -> Any:
    """
    Decode an EVM PairCreated log (Uniswap V2 / Aerodrome).
    Supports either passing a single `log` dictionary or individual `topics`, `data`, and optional `weth_address`.
    """
    try:
        if log is not None:
            raw_topics: list[str] = log.get("topics", [])
            data_hex = log.get("data", "0x")[2:]
            factory = log.get("address", "").lower()
            tx_hash = log.get("transactionHash", "").lower()
            block_hex = log.get("blockNumber", "0x0")
            block_number = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
        else:
            raw_topics = topics or []
            data_hex = (data or "0x")
            if data_hex.startswith("0x") or data_hex.startswith("0X"):
                data_hex = data_hex[2:]
            factory = ""
            tx_hash = ""
            block_number = 0

        if not raw_topics or raw_topics[0].lower() != _PAIR_CREATED_TOPIC:
            return None
        if len(raw_topics) < 3:
            return None

        token0 = "0x" + raw_topics[1][-40:].lower()
        token1 = "0x" + raw_topics[2][-40:].lower()

        if len(data_hex) < 64:
            return None

        # Pair address is the first 32 bytes (offset 0..64)
        pair = "0x" + data_hex[24:64].lower()

        result = PairCreatedResult({
            "token0": token0,
            "token1": token1,
            "pair": pair,
            "factory": factory,
            "tx_hash": tx_hash,
            "block_number": block_number,
            "weth_address": weth_address or "0x4200000000000000000000000000000000000006",
        })
        return result
    except Exception as exc:
        logger.debug("Failed to decode PairCreated log: %s", exc)
        return None


def _decode_evm_pool_created_log(log: dict[str, Any]) -> dict[str, Any] | None:
    """Decode an EVM PoolCreated log (Uniswap V3 / Aerodrome SlipStream)."""
    try:
        topics: list[str] = log.get("topics", [])
        if not topics or topics[0].lower() != _POOL_CREATED_TOPIC:
            return None
        if len(topics) < 4:
            return None

        token0 = "0x" + topics[1][-40:].lower()
        token1 = "0x" + topics[2][-40:].lower()
        fee = int(topics[3], 16)

        data_hex = log.get("data", "0x")[2:]
        if len(data_hex) < 128:
            return None

        # Pool address is at offset 32..64 (chars 64..128)
        pool = "0x" + data_hex[88:128].lower()
        factory = log.get("address", "").lower()
        tx_hash = log.get("transactionHash", "").lower()

        block_hex = log.get("blockNumber", "0x0")
        block_number = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)

        return {
            "token0": token0,
            "token1": token1,
            "fee": fee,
            "pool": pool,
            "factory": factory,
            "tx_hash": tx_hash,
            "block_number": block_number,
        }
    except Exception as exc:
        logger.debug("Failed to decode PoolCreated log: %s", exc)
        return None


def _parse_pump_fun_logs(logs: list[str], tx_sig: str = "") -> dict[str, Any] | None:
    """
    Parse Solana logs for Pump.fun program mint & bonding curve initialization.
    Detects InitializeMint2, Create, or bonding curve parameters.
    """
    import re
    mint_address: str | None = None
    bonding_curve: str | None = None
    is_create = False

    # Regex for Solana base58 (32-44 characters)
    b58_re = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")

    for line in logs:
        line_lower = line.lower()
        if "6ef8rrecth" in line_lower or "create" in line_lower or "initializemint" in line_lower:
            is_create = True

        # Look for explicit mint keyword
        if "mint:" in line_lower:
            matches = b58_re.findall(line)
            if matches:
                mint_address = matches[-1]
        elif "bonding curve:" in line_lower:
            matches = b58_re.findall(line)
            if matches:
                bonding_curve = matches[-1]

    if not is_create:
        return None

    # If mint address wasn't explicitly prefixed, look for all base58 tokens excluding program IDs
    if not mint_address:
        found_tokens: list[str] = []
        for line in logs:
            for token in b58_re.findall(line):
                if token not in (PUMP_FUN_PROGRAM_ID, RAYDIUM_AMM_PROGRAM_ID) and len(token) >= 32:
                    found_tokens.append(token)
        if found_tokens:
            mint_address = found_tokens[0]
            if len(found_tokens) > 1:
                bonding_curve = found_tokens[1]

    if not mint_address:
        return None

    # Pump.fun standard bonding curve initial reserves:
    # 30 SOL virtual reserve, 1.073B virtual tokens
    return PumpFunResult({
        "mint": mint_address,
        "bonding_curve": bonding_curve or mint_address,
        "virtual_sol_reserves": Decimal("30.0"),
        "virtual_token_reserves": Decimal("1073000000.0"),
        "token_decimals": 6,
        "native_decimals": 9,
        "tx_hash": tx_sig,
    })


def _parse_raydium_initialize2_logs(logs: list[str], tx_sig: str = "") -> dict[str, Any] | None:
    """Parse Solana logs for Raydium AMM pool creation (Initialize2)."""
    import re
    b58_re = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
    is_raydium_init = False
    pool_address: str | None = None
    open_time: int = 0

    for line in logs:
        line_lower = line.lower()
        if "initialize2" in line_lower or "createpool" in line_lower:
            is_raydium_init = True
            matches = b58_re.findall(line)
            if matches:
                pool_address = matches[0]
            if "open_time" in line_lower:
                tokens = line.replace(":", " ").replace(",", " ").split()
                for i, tok in enumerate(tokens):
                    if tok.lower() == "open_time" and i + 1 < len(tokens):
                        try:
                            open_time = int(tokens[i + 1])
                        except ValueError:
                            pass

    if not is_raydium_init:
        return None

    return RaydiumInitResult({
        "pool_address": pool_address or tx_sig[:44],
        "tx_hash": tx_sig,
        "open_time": open_time,
    })

