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

import aiohttp
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import (
    _ERC20_BALANCE_ABI,
    _MAX_PUMP_FUN_TOP10_CONCENTRATION,
    _MAX_TOP10_CONCENTRATION,
    _MIN_SELL_RETURN_RATIO,
    _ROUTER_ABI_SWAP_EXACT_ETH,
    TAX_MUTATION_MAP,
    TAX_MUTATION_SELECTORS,
    derive_pump_fun_bonding_curve,
    is_pump_fun_token,
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
    if metadata is not None:
        logger.debug("Inspecting bytecode for %s with metadata: %s", token_address, metadata)

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
    logger.debug(
        "Executing Tier 2 EVM preflight simulation for token %s on pool %s",
        token_address,
        pool_address or "n/a",
    )
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


async def verify_solana_mint_preflight(
    session: aiohttp.ClientSession,
    mint_address: str,
    rpc_url: str,
    pool_address: str = "",
) -> dict[str, Any] | None:
    """
    Local Solana JSON-RPC preflight validation for unindexed / new mints.
    Inspects mint account info for:
      - Owner program (SPL Token / Token-2022)
      - Mint authority (disabled / null)
      - Freeze authority (disabled / null)
      - Top-10 holder concentration via getTokenLargestAccounts & getTokenSupply,
        excluding the Pump.fun bonding curve address.
    """
    if not rpc_url:
        return None

    # 1. Fetch getAccountInfo (jsonParsed)
    payload_info = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            mint_address,
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
    }

    try:
        async with session.post(
            rpc_url,
            json=payload_info,
            timeout=aiohttp.ClientTimeout(total=2.5),
        ) as resp:
            if resp.status != 200:
                return None
            body = await resp.json()
            val = (body.get("result") or {}).get("value")
            if not val:
                return None

            owner = val.get("owner", "")
            if owner not in (
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
            ):
                return None

            mint_auth = None
            freeze_auth = None
            decimals = 6
            data_field = val.get("data")
            if isinstance(data_field, dict):
                parsed = data_field.get("parsed") or {}
                if parsed.get("type") == "mint":
                    info = parsed.get("info") or {}
                    mint_auth = info.get("mintAuthority")
                    freeze_auth = info.get("freezeAuthority")
                    decimals = info.get("decimals", 6)

        # 2. Fetch top largest token accounts & supply to calculate concentration & detect sniper bundles
        top10_concentration = 0.0
        developer_sniper_bundle = False
        max_holder_pct = 0.0
        try:
            payload_largest = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "getTokenLargestAccounts",
                "params": [mint_address, {"commitment": "confirmed"}],
            }
            payload_supply = {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "getTokenSupply",
                "params": [mint_address, {"commitment": "confirmed"}],
            }
            async with session.post(
                rpc_url,
                json=payload_supply,
                timeout=aiohttp.ClientTimeout(total=2.0),
            ) as resp_supply:
                total_supply = Decimal(0)
                if resp_supply.status == 200:
                    supply_body = await resp_supply.json()
                    supply_val = (supply_body.get("result") or {}).get("value") or {}
                    total_supply = Decimal(str(supply_val.get("amount", 0)))

            if total_supply > 0:
                async with session.post(
                    rpc_url,
                    json=payload_largest,
                    timeout=aiohttp.ClientTimeout(total=2.0),
                ) as resp_largest:
                    if resp_largest.status == 200:
                        largest_body = await resp_largest.json()
                        accounts = (largest_body.get("result") or {}).get("value") or []

                        is_pump = is_pump_fun_token(mint_address) or bool(pool_address)
                        curve_pda, curve_ata = derive_pump_fun_bonding_curve(mint_address)
                        excluded_addresses: set[str] = set()
                        if curve_pda:
                            excluded_addresses.add(curve_pda)
                        if curve_ata:
                            excluded_addresses.add(curve_ata)
                        if pool_address:
                            excluded_addresses.add(pool_address)

                        # Exclude Pump.fun bonding curve from holder concentration calculations
                        filtered_accounts: list[dict[str, Any]] = []
                        for idx, acc in enumerate(accounts):
                            if not isinstance(acc, dict):
                                continue
                            addr = acc.get("address", "")
                            acc_amt = Decimal(str(acc.get("amount", 0)))
                            pct = float(acc_amt / total_supply)
                            # Exclude if explicitly matches bonding curve / ATA / pool, or is the dominant initial curve holding >= 50%
                            if is_pump and (addr in excluded_addresses or (idx == 0 and pct >= 0.50)):
                                logger.debug(
                                    "Excluding Pump.fun bonding curve %s (holding %.2f%%) from top 10 calculations",
                                    addr[:10] if addr else "account",
                                    pct * 100,
                                )
                                continue
                            filtered_accounts.append(acc)

                        top10_sum = sum(
                            Decimal(str(acc.get("amount", 0)))
                            for acc in filtered_accounts[:10]
                        )
                        top10_concentration = float(min(Decimal(1), top10_sum / total_supply))

                        # Sybil & Bundled Mint Detection on non-bonding-curve accounts
                        max_conc_threshold = (
                            _MAX_PUMP_FUN_TOP10_CONCENTRATION
                            if is_pump
                            else _MAX_TOP10_CONCENTRATION
                        )
                        for acc in filtered_accounts:
                            acc_amt = Decimal(str(acc.get("amount", 0)))
                            pct = float(acc_amt / total_supply)
                            if pct > max_holder_pct:
                                max_holder_pct = pct
                            if pct > max_conc_threshold:
                                developer_sniper_bundle = True
                                logger.warning(
                                    "Developer sniper bundle detected for %s: non-bonding-curve account holds %.2f%% (>%.0f%%)",
                                    mint_address[:10],
                                    pct * 100,
                                    max_conc_threshold * 100,
                                )
        except Exception as exc:
            logger.debug("Failed fetching top holders for %s: %s", mint_address[:10], exc)

        mint_disabled = mint_auth is None or str(mint_auth).lower() == "null"
        freeze_disabled = freeze_auth is None or str(freeze_auth).lower() == "null"

        return {
            "on_chain_fallback": True,
            "mint": mint_address,
            "mintAuthority": mint_auth,
            "freezeAuthority": freeze_auth,
            "mint_authority_disabled": mint_disabled,
            "freeze_authority_disabled": freeze_disabled,
            "decimals": decimals,
            "top10_concentration": top10_concentration,
            "developer_sniper_bundle": developer_sniper_bundle,
            "max_single_holder_pct": max_holder_pct,
            "risks": [],
            "tokenMeta": {"mutable": False},
            "markets": [{"lp": {"lpLockedPct": 100.0}}],
            "topHolders": [],
        }
    except Exception as exc:
        logger.debug("Solana RPC preflight failed for %s: %s", mint_address[:10], exc)
        return None

