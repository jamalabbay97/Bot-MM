"""
alpha_engine.security.preflight — Tier 2 EVM Honeypot Simulation via eth_call
=============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | web3.py
"""

from __future__ import annotations

import logging
import time
from decimal import Decimal

from typing import Any, Optional

from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import (
    _ERC20_BALANCE_ABI,
    _MIN_SELL_RETURN_RATIO,
    _ROUTER_ABI_SWAP_EXACT_ETH,
    TAX_MUTATION_MAP,
    TAX_MUTATION_SELECTORS,
)

logger = logging.getLogger(__name__)


class BytecodeInspectionResult(tuple):
    """Tuple subclass that is also awaitable to allow dual sync/async calls."""
    def __await__(self):
        async def _coro():
            return self
        return _coro().__await__()


def parse_payload_or_abi(
    payload: Optional[Any] = None,
    abi_params: Optional[dict[str, Any]] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Parse arbitrary transaction payload or ABI parameters with optional metadata.
    """
    result: dict[str, Any] = {}
    if payload is not None:
        result["payload"] = payload
    if abi_params is not None:
        result["abi"] = abi_params
    if metadata is not None:
        result["metadata"] = metadata
    return result


def inspect_bytecode_for_delayed_taxes(
    bytecode_or_w3: Any,
    token_address: Optional[str] = None,
    metadata: Optional[Any] = None,
) -> Any:
    """
    Inspect contract bytecode for delayed fee modifications, dynamic tax setters,
    or trading trap functions (setTax, updateFees, enableTrading, setMaxTxPercent).
    Supports direct bytecode string/bytes (sync/awaitable) or AsyncWeb3 instance (coroutine).
    """
    if isinstance(bytecode_or_w3, (str, bytes)):
        code_hex = bytecode_or_w3.hex().lower() if isinstance(bytecode_or_w3, bytes) else str(bytecode_or_w3).lower()
        if code_hex.startswith("0x"):
            code_hex = code_hex[2:]
        reasons: list[str] = []
        if any(sel in code_hex for sel in TAX_MUTATION_SELECTORS):
            for selector, name in TAX_MUTATION_MAP.items():
                if selector in code_hex:
                    reasons.append(name)
        return BytecodeInspectionResult((len(reasons) == 0, reasons))

    async def _async_inspect() -> tuple[bool, list[str]]:
        reasons: list[str] = []
        try:
            code = await bytecode_or_w3.eth.get_code(bytecode_or_w3.to_checksum_address(token_address))
            if not code or code == b"" or code == "0x":
                return False, ["No bytecode deployed at token address"]

            code_hex = code.hex().lower() if isinstance(code, bytes) else str(code).lower()
            if code_hex.startswith("0x"):
                code_hex = code_hex[2:]

            if any(sel in code_hex for sel in TAX_MUTATION_SELECTORS):
                for selector, name in TAX_MUTATION_MAP.items():
                    if selector in code_hex:
                        reasons.append(name)

            if reasons:
                return False, reasons
            return True, []
        except Exception as exc:
            logger.debug("Bytecode inspection failed for %s: %s", token_address, exc)
            return True, []

    return _async_inspect()



async def _tier2_evm_preflight(
    token_address: str,
    pool_address: str = "",
    weth_address: str = "",
    router_address: str = "",
    rpc_url: str = "",
    limiter: Optional[RateLimiterRegistry] = None,
) -> bool:
    """
    Execute a sequential BUY + SELL simulation via eth_call to detect
    honeypots and hidden transfer taxes on EVM (Base) tokens.
    """
    try:
        from web3 import AsyncWeb3
        from web3.middleware import ExtraDataToPOAMiddleware
    except ImportError:
        logger.error(
            "web3 package not installed. Tier 2 pre-flight disabled. "
            "Install with: pip install web3"
        )
        return True  # Fail-open: do not block on missing dependency

    await limiter.alchemy.acquire(cost=2.0)

    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    # ── Step 0: Bytecode delayed fee / dynamic tax inspection ────────────────
    is_clean_bytecode, bytecode_reasons = await inspect_bytecode_for_delayed_taxes(w3, token_address)
    if not is_clean_bytecode:
        logger.warning(
            "Tier 2 Bytecode inspection REJECTED %s: %s. Flagging as honeypot/trap.",
            token_address,
            "; ".join(bytecode_reasons),
        )
        return False

    sim_wallet = "0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF"
    buy_amount_wei = w3.to_wei(Decimal("0.001"), "ether")
    deadline = int(time.time()) + 300


    token = w3.eth.contract(
        address=w3.to_checksum_address(token_address),
        abi=_ERC20_BALANCE_ABI,
    )
    router = w3.eth.contract(
        address=w3.to_checksum_address(router_address),
        abi=_ROUTER_ABI_SWAP_EXACT_ETH,
    )

    # ── Step 1: Simulate BUY ────────────────────────────────────────────────
    buy_path = [
        w3.to_checksum_address(weth_address),
        w3.to_checksum_address(token_address),
    ]

    try:
        balance_before: int = await token.functions.balanceOf(sim_wallet).call()

        await router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
            0,
            buy_path,
            sim_wallet,
            deadline,
        ).call({"from": sim_wallet, "value": buy_amount_wei})

        balance_after_buy: int = await token.functions.balanceOf(sim_wallet).call()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Tier 2 BUY simulation reverted for %s: %s. Flagging as honeypot.",
            token_address,
            exc,
        )
        return False

    tokens_received = balance_after_buy - balance_before
    if tokens_received <= 0:
        logger.warning(
            "Tier 2: BUY simulation returned 0 tokens for %s. Flagging as honeypot.",
            token_address,
        )
        return False

    # ── Step 2: Simulate SELL ───────────────────────────────────────────────
    sell_path = [
        w3.to_checksum_address(token_address),
        w3.to_checksum_address(weth_address),
    ]

    await limiter.alchemy.acquire(cost=2.0)

    try:
        eth_before: int = await w3.eth.get_balance(sim_wallet)

        await router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
            tokens_received,
            0,
            sell_path,
            sim_wallet,
            deadline,
        ).call({"from": sim_wallet})

        eth_after: int = await w3.eth.get_balance(sim_wallet)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Tier 2 SELL simulation reverted for %s: %s. Flagging as honeypot.",
            token_address,
            exc,
        )
        return False

    eth_returned = eth_after - eth_before
    if eth_returned <= 0:
        logger.warning(
            "Tier 2: SELL produced 0 ETH for %s. Likely honeypot.", token_address
        )
        return False

    # ── Step 3: Assess hidden tax ────────────────────────────────────────────
    return_ratio = Decimal(str(eth_returned)) / Decimal(str(buy_amount_wei))
    if return_ratio < _MIN_SELL_RETURN_RATIO:
        logger.warning(
            "Tier 2: Hidden tax detected for %s — return_ratio=%.4f < %.2f threshold.",
            token_address,
            float(return_ratio),
            float(_MIN_SELL_RETURN_RATIO),
        )
        return False

    logger.debug(
        "Tier 2 pre-flight PASSED for %s | return_ratio=%.4f",
        token_address,
        float(return_ratio),
    )
    return True
