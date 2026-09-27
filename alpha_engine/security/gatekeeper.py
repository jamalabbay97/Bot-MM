"""
alpha_engine.security.gatekeeper — Multi-Tier Security Gatekeeper Orchestrator
==============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiohttp + web3.py
"""

from __future__ import annotations

import logging

import aiohttp

from alpha_engine.models.enums import ChainIdentifier, SecurityTier
from alpha_engine.models.state import SecurityReport
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.goplus import _fetch_goplus_report, _parse_goplus_report
from alpha_engine.security.preflight import _tier2_evm_preflight
from alpha_engine.security.rugcheck import _fetch_rugcheck_report, _parse_rugcheck_report

logger = logging.getLogger(__name__)


def _build_fallback_report(
    token_address: str,
    chain: ChainIdentifier,
    tier: SecurityTier,
) -> SecurityReport:
    """Construct a conservative fallback SecurityReport when external API is unavailable."""
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
        external_api_raw=None,
    )


class SecurityGatekeeper:
    """
    Orchestrates the two-tier security screening pipeline.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        limiter: RateLimiterRegistry,
        evm_rpc_url: str,
        evm_router_address: str,
        weth_address: str,
        enable_tier2: bool = True,
    ) -> None:
        self._session = session
        self._limiter = limiter
        self._evm_rpc_url = evm_rpc_url
        self._evm_router = evm_router_address
        self._weth = weth_address
        self._enable_tier2 = enable_tier2

    async def screen_token(
        self,
        token_address: str,
        chain: ChainIdentifier,
        pool_address: str = "",
    ) -> SecurityReport:
        """Run the full dual-tier screening pipeline for a token."""
        tier1_report = await self._run_tier1(token_address, chain, pool_address)

        if tier1_report.tier == SecurityTier.TIER1_REJECTED:
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
                return tier1_report.model_copy(
                    update={
                        "is_honeypot": True,
                        "tier": SecurityTier.TIER2_HONEYPOT,
                    }
                )

        logger.info("Token %s PASSED all security tiers.", token_address)
        return tier1_report

    async def _run_tier1(
        self,
        token_address: str,
        chain: ChainIdentifier,
        pool_address: str = "",
    ) -> SecurityReport:
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
                self._session, token_address, self._limiter
            )
            if raw is None:
                logger.warning(
                    "RugCheck unavailable for %s — issuing WARN-level report.", token_address
                )
                return _build_fallback_report(
                    token_address, chain, SecurityTier.TIER1_WARN
                )
            return _parse_rugcheck_report(token_address, raw)

        else:
            raise ValueError(f"Unsupported chain for security screening: {chain}")
