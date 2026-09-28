"""
alpha_engine.security.rugcheck — RugCheck Token Security API Client (Solana/SVM)
================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiohttp
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

import aiohttp

from alpha_engine.models.enums import ChainIdentifier, SecurityTier
from alpha_engine.models.state import SecurityReport
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import (
    _API_TIMEOUT_S,
    _MAX_BUY_TAX_BPS,
    _MAX_SELL_TAX_BPS,
    _MAX_TOP10_CONCENTRATION,
    _MIN_LP_BURNED_RATIO,
    _RUGCHECK_URL,
    is_blacklisted_token,
)
from alpha_engine.security.preflight import verify_solana_mint_preflight

logger = logging.getLogger(__name__)

# Retry backoff delays for server errors (400ms, 800ms, 1200ms)
_RUGCHECK_RETRY_DELAYS_S: tuple[float, ...] = (0.4, 0.8, 1.2)


async def _verify_solana_mint_on_chain(
    session: aiohttp.ClientSession,
    mint_address: str,
    rpc_url: str,
) -> dict[str, Any] | None:
    """
    Fallback local Solana JSON-RPC check (getAccountInfo) to verify if an unindexed
    token mint account physically exists on-chain as a valid SPL / Token-2022 mint.
    """
    return await verify_solana_mint_preflight(session, mint_address, rpc_url)


async def _fetch_rugcheck_report(
    session: aiohttp.ClientSession,
    mint_address: str,
    limiter: RateLimiterRegistry,
    rpc_url: str = "",
) -> dict[str, Any] | None:
    """
    Fetch token report from RugCheck API for a Solana token mint.
    If RugCheck returns HTTP 400 or 404 (unindexed mint / not found), immediately falls back
    to local RPC preflight validation without blocking sleep retries.
    """
    if is_blacklisted_token(mint_address, ChainIdentifier.SOLANA_MAINNET):
        logger.debug("Skipping RugCheck for blacklisted/system Solana address %s", mint_address)
        return {"invalid_mint": True}

    url = _RUGCHECK_URL.format(mint=mint_address)
    max_attempts = len(_RUGCHECK_RETRY_DELAYS_S) + 1

    for attempt in range(max_attempts):
        await limiter.helius.acquire(cost=1.0)
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=_API_TIMEOUT_S),
            ) as resp:
                if resp.status == 200:
                    return await resp.json()

                if resp.status in (400, 404):
                    logger.info(
                        "RugCheck returned HTTP %d for unindexed mint %s. Bypassing sleep retries; falling back immediately to local RPC preflight.",
                        resp.status,
                        mint_address[:10],
                    )
                    break

                if resp.status != 200:
                    logger.warning(
                        "RugCheck returned HTTP %d for %s (attempt %d/%d)",
                        resp.status,
                        mint_address[:10],
                        attempt + 1,
                        max_attempts,
                    )
                    if attempt < len(_RUGCHECK_RETRY_DELAYS_S):
                        await asyncio.sleep(_RUGCHECK_RETRY_DELAYS_S[attempt])
                        continue
                    return None

        except (aiohttp.ClientError, TimeoutError) as exc:
            logger.warning(
                "RugCheck request failed for %s (attempt %d/%d): %s",
                mint_address[:10],
                attempt + 1,
                max_attempts,
                exc,
            )
            if attempt < len(_RUGCHECK_RETRY_DELAYS_S):
                await asyncio.sleep(_RUGCHECK_RETRY_DELAYS_S[attempt])
                continue
            return None

    # Fallback to local RPC account validation before declaring token invalid
    fallback_rpc = rpc_url or os.getenv("SOLANA_RPC_HTTP", "")
    if fallback_rpc:
        on_chain_info = await verify_solana_mint_preflight(session, mint_address, fallback_rpc)
        if on_chain_info is not None:
            logger.info(
                "Local RPC preflight validation verified unindexed mint %s on-chain.",
                mint_address[:10],
            )
            return on_chain_info

    logger.info("RugCheck indicated invalid/non-existent token mint %s", mint_address[:10])
    return {"invalid_mint": True}


def _parse_rugcheck_report(
    mint_address: str,
    raw: dict[str, Any],
) -> SecurityReport:
    """Parse a RugCheck API response or on-chain fallback into a SecurityReport."""
    if raw.get("invalid_mint"):
        return SecurityReport(
            token_address=mint_address,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tier=SecurityTier.TIER1_REJECTED,
            is_honeypot=False,
            buy_tax_bps=10_000,
            sell_tax_bps=10_000,
            lp_burned_ratio=0.0,
            top10_concentration=1.0,
            mint_authority_disabled=False,
            verified_source_code=False,
            external_api_raw=json.dumps(raw),
        )

    if raw.get("on_chain_fallback"):
        mint_authority = raw.get("mintAuthority")
        mint_disabled = raw.get(
            "mint_authority_disabled",
            mint_authority is None or str(mint_authority).lower() == "null",
        )
        freeze_authority = raw.get("freezeAuthority")
        freeze_disabled = raw.get(
            "freeze_authority_disabled",
            freeze_authority is None or str(freeze_authority).lower() == "null",
        )
        top10_conc = float(raw.get("top10_concentration", 0.0) or 0.0)
        tier = (
            SecurityTier.CLEAN
            if (mint_disabled and freeze_disabled and top10_conc <= _MAX_TOP10_CONCENTRATION)
            else SecurityTier.TIER1_REJECTED
        )

        return SecurityReport(
            token_address=mint_address,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tier=tier,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
            lp_burned_ratio=1.0,
            top10_concentration=top10_conc,
            mint_authority_disabled=mint_disabled,
            verified_source_code=True,
            external_api_raw=json.dumps(raw, default=str),
        )

    risks: list[dict[str, Any]] = raw.get("risks") or []
    token_meta: dict[str, Any] = raw.get("tokenMeta") or {}

    mint_authority = raw.get("mintAuthority")
    mint_disabled = mint_authority is None or str(mint_authority).lower() == "null"

    freeze_authority = raw.get("freezeAuthority")
    has_freeze = freeze_authority is not None and str(freeze_authority).lower() != "null"
    if has_freeze:
        mint_disabled = False

    markets: list[dict[str, Any]] = raw.get("markets") or []
    lp_burned_ratio = 0.0
    if markets:
        first_m = markets[0]
        lp_info: dict[str, Any] = (first_m.get("lp") or {}) if isinstance(first_m, dict) else {}
        lp_locked_pct = float(lp_info.get("lpLockedPct", 0) or 0)
        lp_burned_ratio = min(1.0, lp_locked_pct / 100.0)

    top_holders: list[dict[str, Any]] = raw.get("topHolders") or []
    non_insider = [h for h in top_holders if isinstance(h, dict) and not h.get("insider", False)]
    top10_pcts = [float(h.get("pct", 0) or 0) for h in non_insider[:10]]
    top10_concentration = min(1.0, sum(top10_pcts) / 100.0)

    buy_tax_bps = 0
    sell_tax_bps = 0
    for risk in risks:
        name = risk.get("name", "").lower()
        if "transfer fee" in name or "tax" in name:
            score_proxy = int(risk.get("score", 0))
            inferred_bps = min(10_000, score_proxy)
            sell_tax_bps = max(sell_tax_bps, inferred_bps)
            buy_tax_bps = max(buy_tax_bps, inferred_bps)

    raw_json = json.dumps(raw, default=str)
    freeze_disabled = freeze_authority is None or str(freeze_authority).lower() == "null"

    tier = SecurityTier.CLEAN
    if (
        sell_tax_bps > _MAX_SELL_TAX_BPS
        or buy_tax_bps > _MAX_BUY_TAX_BPS
        or lp_burned_ratio < _MIN_LP_BURNED_RATIO
        or not mint_disabled
        or not freeze_disabled
        or top10_concentration > _MAX_TOP10_CONCENTRATION
    ):
        tier = SecurityTier.TIER1_REJECTED

    return SecurityReport(
        token_address=mint_address,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=tier,
        is_honeypot=False,
        buy_tax_bps=buy_tax_bps,
        sell_tax_bps=sell_tax_bps,
        lp_burned_ratio=lp_burned_ratio,
        top10_concentration=top10_concentration,
        mint_authority_disabled=mint_disabled,
        verified_source_code=not token_meta.get("mutable", True),
        external_api_raw=raw_json,
    )

