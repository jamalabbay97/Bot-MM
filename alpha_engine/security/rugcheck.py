"""
alpha_engine.security.rugcheck — RugCheck Token Security API Client (Solana/SVM)
================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiohttp
"""

from __future__ import annotations

import json
import logging
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
    SOLANA_SYSTEM_PROGRAM_IDS,
)


logger = logging.getLogger(__name__)


async def _fetch_rugcheck_report(
    session: aiohttp.ClientSession,
    mint_address: str,
    limiter: RateLimiterRegistry,
) -> dict[str, Any] | None:
    """Fetch token report from RugCheck API for a Solana token mint."""
    if mint_address in SOLANA_SYSTEM_PROGRAM_IDS or mint_address.startswith("11111111"):
        logger.debug("Skipping RugCheck for known Solana system/infrastructure address %s", mint_address)
        return {"invalid_mint": True}

    url = _RUGCHECK_URL.format(mint=mint_address)
    await limiter.helius.acquire(cost=1.0)

    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=_API_TIMEOUT_S),
        ) as resp:
            if resp.status in (400, 404):
                logger.info("RugCheck indicated invalid/non-existent token mint %s (HTTP %d)", mint_address, resp.status)
                return {"invalid_mint": True}
            if resp.status != 200:
                logger.warning(
                    "RugCheck returned HTTP %d for %s", resp.status, mint_address
                )
                return None
            return await resp.json()
    except (aiohttp.ClientError, TimeoutError) as exc:
        logger.warning("RugCheck request failed for %s: %s", mint_address, exc)
        return None


def _parse_rugcheck_report(
    mint_address: str,
    raw: dict[str, Any],
) -> SecurityReport:
    """Parse a RugCheck API response into a SecurityReport."""
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
