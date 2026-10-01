"""
alpha_engine.security.gatekeeper — Multi-Tier Security Gatekeeper Orchestrator
==============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiohttp + web3.py
"""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from alpha_engine.models.enums import ChainIdentifier, SecurityTier
from alpha_engine.models.state import SecurityReport
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import (
    _MAX_PUMP_FUN_TOP10_CONCENTRATION,
    _MAX_TOP10_CONCENTRATION,
    derive_pump_fun_bonding_curve,
    is_blacklisted_token,
)
from alpha_engine.security.goplus import _fetch_goplus_report, _parse_goplus_report
from alpha_engine.security.preflight import _tier2_evm_preflight
from alpha_engine.security.rugcheck import _fetch_rugcheck_report, _parse_rugcheck_report

import time

logger = logging.getLogger(__name__)

MAX_TOP10_CONCENTRATION_PUMP_FUN: float = _MAX_PUMP_FUN_TOP10_CONCENTRATION  # 0.65
MAX_TOP10_CONCENTRATION: float = _MAX_TOP10_CONCENTRATION  # 0.20


def calculate_top10_concentration(
    holders: list[dict[str, Any]],
    is_pump: bool = False,
    mint_address: str = "",
    pool_address: str = "",
) -> float:
    """
    Calculate Top 10 holder concentration, excluding Pump.fun bonding curve / ATA.
    """
    excluded: set[str] = set()
    if is_pump and mint_address:
        pda, ata = derive_pump_fun_bonding_curve(mint_address)
        if pda:
            excluded.add(pda)
        if ata:
            excluded.add(ata)
    if pool_address:
        excluded.add(pool_address)

    filtered: list[dict[str, Any]] = []
    for idx, h in enumerate(holders):
        if not isinstance(h, dict):
            continue
        addr = h.get("address", "") or h.get("owner", "")
        pct = float(h.get("pct", 0) or h.get("percent", 0) or 0)
        if is_pump and (addr in excluded or (idx == 0 and pct >= 50.0)):
            continue
        filtered.append(h)

    top10_pcts = [float(h.get("pct", 0) or h.get("percent", 0) or 0) for h in filtered[:10]]
    raw_sum = sum(top10_pcts)
    if raw_sum > 1.0:
        raw_sum /= 100.0
    return min(1.0, max(0.0, raw_sum))


def _build_fallback_report(
    token_address: str,
    chain: ChainIdentifier,
    tier: SecurityTier,
    reason: str | None = None,
) -> SecurityReport:
    """Construct a conservative fallback SecurityReport when external API is unavailable or token is blacklisted."""
    return SecurityReport(
        token_address=token_address,
        chain=chain,
        tier=tier,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=0.0,
        top10_concentration=1.0,
        mint_authority_disabled=False,
        verified_source_code=False,
        external_api_raw=reason,
    )


class NegativeRejectionCache:
    """
    In-memory sliding-window TTL/LRU Negative Cache for rejected tokens.
    Prevents repeated expensive external API and RPC security audits on incoming
    swaps or duplicate discovery signals for already-rejected tokens.
    """

    def __init__(self, maxsize: int = 5000, ttl_seconds: float = 300.0) -> None:
        self._maxsize = maxsize
        self._ttl = ttl_seconds
        # maps (token_address.lower(), chain_str) -> (expiry_monotonic, SecurityReport)
        self._cache: dict[tuple[str, str], tuple[float, SecurityReport]] = {}

    def _make_key(self, token_address: str, chain: ChainIdentifier | str | None) -> tuple[str, str]:
        c_str = chain.value if isinstance(chain, ChainIdentifier) else (str(chain) if chain else "")
        return (token_address.lower().strip(), c_str)

    def is_rejected(self, token_address: str, chain: ChainIdentifier | str | None = None) -> bool:
        """Return True if token is cached as rejected and within TTL."""
        if not token_address:
            return False
        now = time.monotonic()
        k = self._make_key(token_address, chain)
        item = self._cache.get(k)
        if item is not None:
            exp, _ = item
            if now < exp:
                return True
            del self._cache[k]
        return False

    def get_rejection(
        self, token_address: str, chain: ChainIdentifier | str | None = None
    ) -> SecurityReport | None:
        """Return cached rejected SecurityReport if valid, or None."""
        if not token_address:
            return None
        now = time.monotonic()
        k = self._make_key(token_address, chain)
        item = self._cache.get(k)
        if item is not None:
            exp, report = item
            if now < exp:
                return report
            del self._cache[k]
        return None

    def record_rejection(
        self,
        token_address: str,
        chain: ChainIdentifier | str | None,
        report: SecurityReport | None = None,
        reason: str = "",
    ) -> None:
        """Record token rejection status with TTL expiration."""
        if not token_address:
            return
        now = time.monotonic()
        k = self._make_key(token_address, chain)

        # LRU eviction if full
        if len(self._cache) >= self._maxsize:
            expired = [key for key, (exp, _) in self._cache.items() if now >= exp]
            for exp_k in expired:
                del self._cache[exp_k]
            if len(self._cache) >= self._maxsize:
                sorted_keys = sorted(self._cache, key=lambda key: self._cache[key][0])
                for old_key in sorted_keys[: max(1, self._maxsize // 5)]:
                    self._cache.pop(old_key, None)

        actual_chain = chain if isinstance(chain, ChainIdentifier) else ChainIdentifier.SOLANA_MAINNET
        rep = report or _build_fallback_report(
            token_address=token_address,
            chain=actual_chain,
            tier=SecurityTier.TIER1_REJECTED,
            reason=reason or "Negative cache gatekeeper rejection",
        )
        self._cache[k] = (now + self._ttl, rep)

    def clear(self) -> None:
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)


class SecurityGatekeeper:
    """
    Orchestrates the two-tier security screening pipeline with in-memory negative caching.
    """

    is_blacklisted = staticmethod(is_blacklisted_token)
    MAX_TOP10_CONCENTRATION_PUMP_FUN: float = MAX_TOP10_CONCENTRATION_PUMP_FUN
    MAX_TOP10_CONCENTRATION: float = MAX_TOP10_CONCENTRATION

    async def evaluate_token(
        self,
        token_address: str,
        chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
        pool_address: str = "",
        token_age_s: float | None = None,
    ) -> SecurityReport:
        """
        Evaluate token security posture across all screening tiers.
        Alias / wrapper for screen_token for pipeline interoperability.
        """
        return await self.screen_token(
            token_address=token_address,
            chain=chain,
            pool_address=pool_address,
            token_age_s=token_age_s,
        )

    def __init__(
        self,
        session: aiohttp.ClientSession,
        limiter: RateLimiterRegistry,
        evm_rpc_url: str,
        evm_router_address: str,
        weth_address: str,
        enable_tier2: bool = True,
        solana_rpc_url: str = "",
        negative_cache_ttl_s: float = 300.0,
        negative_cache_maxsize: int = 5000,
    ) -> None:
        self._session = session
        self._limiter = limiter
        self._evm_rpc_url = evm_rpc_url
        self._evm_router = evm_router_address
        self._weth = weth_address
        self._enable_tier2 = enable_tier2
        self._solana_rpc_url = solana_rpc_url
        self._negative_cache = NegativeRejectionCache(
            maxsize=negative_cache_maxsize,
            ttl_seconds=negative_cache_ttl_s,
        )

    @property
    def negative_cache(self) -> NegativeRejectionCache:
        return self._negative_cache

    def is_rejected(self, token_address: str, chain: ChainIdentifier | str | None = None) -> bool:
        """Fast-path check whether a token is already recorded in the negative rejection cache."""
        return self._negative_cache.is_rejected(token_address, chain)

    def record_rejection(
        self,
        token_address: str,
        chain: ChainIdentifier | str | None,
        report: SecurityReport | None = None,
        reason: str = "",
    ) -> None:
        """Explicitly record a rejected token in the negative cache."""
        self._negative_cache.record_rejection(token_address, chain, report=report, reason=reason)

    @staticmethod
    def evaluate_bundle_heuristics(
        slot_buys: dict[int, int] | None = None,
        unique_buyers: set[str] | None = None,
        threshold: float = 0.60,
        min_unique: int = 8,
    ) -> tuple[bool, str]:
        """
        Evaluate Sybil and Jito slot bundle footprints:
        1. Unique Buyer Entropy: Ensure buys originate from distinct, non-bundled wallets (>= min_unique).
        2. Slot Clustering / Bundle Detection: Reject if >= threshold buys cluster in the exact same slot.
        """
        if slot_buys:
            total = sum(slot_buys.values())
            if total >= 5:
                max_slot = max(slot_buys.values())
                if (max_slot / total) >= threshold:
                    return False, f"DEV_BUNDLED: {max_slot}/{total} ({max_slot/total:.1%}) in single slot"
        if unique_buyers is not None and len(unique_buyers) < min_unique:
            return False, f"SYBIL_SUSPECT: unique signers {len(unique_buyers)} < {min_unique}"
        return True, ""

    async def screen_token(
        self,
        token_address: str,
        chain: ChainIdentifier,
        pool_address: str = "",
        token_age_s: float | None = None,
    ) -> SecurityReport:
        """Run the full dual-tier screening pipeline for a token with negative cache lookup."""
        # Static fast-path blacklist lookup: ignore WSOL, native base/quote and system tokens
        if self.is_blacklisted(token_address, chain):
            logger.debug(
                "Fast-path filter: token %s is a blacklisted/quote/system token on %s; skipping evaluation.",
                token_address,
                chain.value,
            )
            return _build_fallback_report(
                token_address,
                chain,
                SecurityTier.TIER1_REJECTED,
                reason="Blacklisted native/wrapped token",
            )

        # Fast-path negative cache lookup: immediately drop already rejected tokens with zero latency
        cached_rejection = self._negative_cache.get_rejection(token_address, chain)
        if cached_rejection is not None:
            logger.debug(
                "Fast-path negative cache hit: token %s already rejected; returning cached rejection with zero latency.",
                token_address[:10],
            )
            return cached_rejection

        tier1_report = await self._run_tier1(
            token_address, chain, pool_address, token_age_s=token_age_s
        )

        if tier1_report.tier == SecurityTier.TIER1_REJECTED or not tier1_report.passes_hard_gates:
            self._negative_cache.record_rejection(token_address, chain, tier1_report)
            logger.info(
                "Token %s REJECTED at Tier 1 (sell_tax=%d bps, lp_burned=%.2f, "
                "mint_disabled=%s, top10=%.2f)",
                token_address,
                tier1_report.sell_tax_bps,
                tier1_report.lp_burned_ratio,
                tier1_report.mint_authority_disabled,
                tier1_report.top10_concentration,
            )
            return tier1_report

        if self._enable_tier2 and chain == ChainIdentifier.BASE_MAINNET and pool_address:
            is_clean = await _tier2_evm_preflight(
                token_address=token_address,
                pool_address=pool_address,
                weth_address=self._weth,
                router_address=self._evm_router,
                rpc_url=self._evm_rpc_url,
                limiter=self._limiter,
            )

            if not is_clean:
                honeypot_report = tier1_report.model_copy(
                    update={
                        "is_honeypot": True,
                        "tier": SecurityTier.TIER2_HONEYPOT,
                    }
                )
                self._negative_cache.record_rejection(token_address, chain, honeypot_report)
                return honeypot_report

        logger.info("Token %s PASSED all security tiers.", token_address)
        return tier1_report

    async def _run_tier1(
        self,
        token_address: str,
        chain: ChainIdentifier,
        pool_address: str = "",
        token_age_s: float | None = None,
    ) -> SecurityReport:
        logger.debug(
            "Running Tier 1 security check for %s (pool: %s) on %s (age=%s)",
            token_address,
            pool_address or "n/a",
            chain.value,
            f"{token_age_s:.1f}s" if token_age_s is not None else "unknown",
        )
        if chain == ChainIdentifier.BASE_MAINNET:
            raw = await _fetch_goplus_report(
                self._session, token_address, chain, self._limiter
            )
            if raw is None:
                logger.warning(
                    "GoPlus unavailable for %s — issuing WARN-level report.", token_address
                )
                return _build_fallback_report(token_address, chain, SecurityTier.TIER1_WARN)
            return _parse_goplus_report(token_address, chain, raw)

        elif chain == ChainIdentifier.SOLANA_MAINNET:
            raw = await _fetch_rugcheck_report(
                self._session,
                token_address,
                self._limiter,
                rpc_url=self._solana_rpc_url,
                token_age_s=token_age_s,
                pool_address=pool_address,
            )
            if raw is None:
                logger.warning(
                    "RugCheck unavailable for %s — issuing WARN-level report.", token_address
                )
                return _build_fallback_report(
                    token_address, chain, SecurityTier.TIER1_WARN
                )
            return _parse_rugcheck_report(token_address, raw, pool_address=pool_address)

        else:
            raise ValueError(f"Unsupported chain for security screening: {chain}")

