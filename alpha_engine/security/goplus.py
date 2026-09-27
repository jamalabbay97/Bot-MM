"""
alpha_engine.security.goplus — GoPlus Token Security API Client (Base/EVM)
==========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiohttp
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import aiohttp


from alpha_engine.models.enums import ChainIdentifier, SecurityTier
from alpha_engine.models.state import SecurityReport
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import (
    _API_TIMEOUT_S,
    _BURN_ADDRESSES,
    _GOPLUS_CHAIN_IDS,
    _GOPLUS_URL,
    _MAX_BUY_TAX_BPS,
    _MAX_SELL_TAX_BPS,
    _MAX_TOP10_CONCENTRATION,
    _MIN_LP_BURNED_RATIO,
    MIN_LOCK_DURATION_SECONDS,
    MIXER_AND_RUG_FUNDING_ADDRESSES,
    VERIFIED_LP_LOCKERS,
)

logger = logging.getLogger(__name__)


def _calculate_lp_burned_ratio(lp_holders: list[dict[str, Any]]) -> float:
    """
    Compute the fraction of LP tokens held by known burn addresses
    or verified lockers locked for at least 6 months.
    """
    if not lp_holders:
        return 0.0
    total_pct = 0.0
    burned_or_locked_pct = 0.0
    now = time.time()
    for holder in lp_holders:
        pct = float(holder.get("percent", 0) or 0)
        total_pct += pct
        addr = holder.get("address", "").lower()
        if addr in _BURN_ADDRESSES:
            burned_or_locked_pct += pct
        elif addr in VERIFIED_LP_LOCKERS or holder.get("is_locked"):
            # Check lock duration (at least 6 months = 180 days)
            end_time = float(holder.get("end_time", 0) or 0)
            if end_time > 0 and (end_time - now) >= MIN_LOCK_DURATION_SECONDS:
                burned_or_locked_pct += pct
            elif holder.get("is_locked") and not end_time:
                # Treated as permanently locked
                burned_or_locked_pct += pct
    if total_pct == 0.0:
        return 0.0
    return min(1.0, burned_or_locked_pct / total_pct)



def _calculate_top10_concentration(holders: list[dict[str, Any]]) -> float:
    """Compute top-10 holder concentration, excluding LP holder entries."""
    non_lp = [h for h in holders if not h.get("is_lp")]
    top10 = non_lp[:10]
    concentration = sum(float(h.get("percent", 0) or 0) for h in top10)
    return min(1.0, concentration / 100.0)


async def _fetch_goplus_report(
    session: aiohttp.ClientSession,
    token_address: str,
    chain: ChainIdentifier,
    limiter: RateLimiterRegistry,
) -> dict[str, Any] | None:
    """Fetch token security report from GoPlus API for an EVM token."""
    chain_id = _GOPLUS_CHAIN_IDS.get(chain)
    if chain_id is None:
        logger.warning("GoPlus does not support chain %s", chain)
        return None

    url = _GOPLUS_URL.format(chain_id=chain_id)
    params = {"contract_addresses": token_address.lower()}

    await limiter.alchemy.acquire(cost=1.0)

    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=_API_TIMEOUT_S),
        ) as resp:
            if resp.status != 200:
                logger.warning(
                    "GoPlus returned HTTP %d for %s", resp.status, token_address
                )
                return None
            payload: dict[str, Any] = await resp.json()
            result: dict[str, Any] = payload.get("result", {})
            return result.get(token_address.lower())
    except (aiohttp.ClientError, TimeoutError) as exc:
        logger.warning("GoPlus request failed for %s: %s", token_address, exc)
        return None


def _parse_goplus_report(
    token_address: str,
    chain: ChainIdentifier,
    raw: dict[str, Any],
) -> SecurityReport:
    """Parse a GoPlus API response into a SecurityReport."""

    def to_bool(val: Any, default: bool = False) -> bool:
        if val is None:
            return default
        return str(val).strip() == "1"

    def to_bps(val: Any) -> int:
        try:
            pct = float(str(val).strip())
            return min(10_000, max(0, int(pct * 100)))
        except (ValueError, TypeError):
            return 0

    buy_tax_bps = to_bps(raw.get("buy_tax", 0))
    sell_tax_bps = to_bps(raw.get("sell_tax", 0))
    lp_burned_ratio = _calculate_lp_burned_ratio(raw.get("lp_holders", []))
    top10_concentration = _calculate_top10_concentration(raw.get("holders", []))

    mint_disabled = not to_bool(raw.get("can_take_back_ownership"))
    if to_bool(raw.get("mintable")):
        mint_disabled = False

    raw_json = json.dumps(raw, default=str)
    creator_address = str(raw.get("creator_address", "")).lower()

    tier = SecurityTier.CLEAN
    if (
        sell_tax_bps > _MAX_SELL_TAX_BPS
        or buy_tax_bps > _MAX_BUY_TAX_BPS
        or lp_burned_ratio < _MIN_LP_BURNED_RATIO
        or not mint_disabled
        or top10_concentration > _MAX_TOP10_CONCENTRATION
        or creator_address in MIXER_AND_RUG_FUNDING_ADDRESSES
    ):
        tier = SecurityTier.TIER1_REJECTED


    return SecurityReport(
        token_address=token_address,
        chain=chain,
        tier=tier,
        is_honeypot=to_bool(raw.get("is_honeypot")),
        buy_tax_bps=buy_tax_bps,
        sell_tax_bps=sell_tax_bps,
        lp_burned_ratio=lp_burned_ratio,
        top10_concentration=top10_concentration,
        mint_authority_disabled=mint_disabled,
        verified_source_code=to_bool(raw.get("is_open_source")),
        external_api_raw=raw_json,
    )
