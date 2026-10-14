"""
tests.test_dns_resilience_and_gatekeeper_network_recovery
==========================================================
Tests verifying:
1. Anti-sinkhole DNS resolver recovers from system socket.gaierror (-3 EAI_AGAIN) via upstream DNS.
2. verify_solana_mint_preflight returns None on transport/timeout errors rather than false invalid_mint.
3. _fetch_rugcheck_report returns None on network failures, preventing synthetic 10,000 bps tax.
4. Gatekeeper issues TIER1_WARN (not fatal rejection) when external screening APIs are unavailable.
5. Pump.fun tokens never receive synthetic 10,000 bps tax on unindexed or preflight checks.
6. NegativeRejectionCache does not mark transient network or unindexed mints as fatal scams.
"""

from __future__ import annotations

import asyncio
import socket
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.dns_resolver import patch_dns_resolvers
from alpha_engine.models.enums import ChainIdentifier, SecurityTier
from alpha_engine.models.state import SecurityReport
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.gatekeeper import NegativeRejectionCache, SecurityGatekeeper
from alpha_engine.security.preflight import verify_solana_mint_preflight
from alpha_engine.security.rugcheck import _fetch_rugcheck_report, _parse_rugcheck_report


def test_safe_getaddrinfo_gaierror_upstream_recovery():
    """Verify socket.getaddrinfo recovers from temporary gaierror by querying upstream DNS."""
    patch_dns_resolvers(servers=["1.1.1.1", "8.8.8.8"])

    with patch("alpha_engine.dns_resolver._ORIG_GETADDRINFO") as mock_orig, \
         patch("alpha_engine.dns_resolver.query_upstream_dns") as mock_upstream:
        # First call to _ORIG_GETADDRINFO raises gaierror(-3)
        mock_orig.side_effect = [
            socket.gaierror(-3, "Temporary failure in name resolution"),
            [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("174.138.15.144", 443))],
        ]
        mock_upstream.return_value = ["174.138.15.144"]

        res = socket.getaddrinfo("unresolved.example.com", 443)
        assert res is not None
        assert res[0][4][0] == "174.138.15.144"
        mock_upstream.assert_called_once_with("unresolved.example.com", servers=("1.1.1.1", "8.8.8.8"))


@pytest.mark.anyio
async def test_verify_solana_mint_preflight_transport_error_returns_none():
    """Verify RPC network / DNS transport failure returns None and NOT invalid_mint."""
    session = MagicMock()
    # Simulate aiohttp connection or gaierror transport failure
    session.post.side_effect = aiohttp_err = OSError("Temporary failure in name resolution")

    res = await verify_solana_mint_preflight(
        session=session,
        mint_address="7cEMq8uRd43dFnaj6xf73QLxKMfLf5Es8FjiszMqpump",
        rpc_url="https://api.mainnet-beta.solana.com",
    )
    # Must return None indicating RPC transport error, NOT {"invalid_mint": True}
    assert res is None


@pytest.mark.anyio
async def test_fetch_rugcheck_report_network_error_returns_none():
    """Verify _fetch_rugcheck_report returns None when both RugCheck and fallback RPC fail with network error."""
    session = MagicMock()
    # RugCheck connection error
    session.get.side_effect = OSError("Connection refused")
    # RPC preflight failure
    session.post.side_effect = OSError("Temporary failure in name resolution")

    limiter = RateLimiterRegistry.default()
    res = await _fetch_rugcheck_report(
        session=session,
        mint_address="7cEMq8uRd43dFnaj6xf73QLxKMfLf5Es8FjiszMqpump",
        limiter=limiter,
        rpc_url="https://api.mainnet-beta.solana.com",
        token_age_s=15.0,  # fresh token
    )
    # Must return None so Gatekeeper marks as TIER1_WARN, NOT {"invalid_mint": True}
    assert res is None


@pytest.mark.anyio
async def test_gatekeeper_network_unavailable_issues_warn_report():
    """Verify SecurityGatekeeper handles unavailable external screening by issuing TIER1_WARN."""
    session = AsyncMock()
    limiter = RateLimiterRegistry.default()
    gk = SecurityGatekeeper(
        session=session,
        limiter=limiter,
        evm_rpc_url="",
        evm_router_address="",
        weth_address="",
        solana_rpc_url="https://api.mainnet-beta.solana.com",
    )

    with patch("alpha_engine.security.gatekeeper._fetch_rugcheck_report", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = None  # Service temporarily unavailable

        report = await gk.screen_token(
            "7cEMq8uRd43dFnaj6xf73QLxKMfLf5Es8FjiszMqpump",
            ChainIdentifier.SOLANA_MAINNET,
            token_age_s=20.0,
        )
        assert report.tier == SecurityTier.TIER1_WARN
        assert report.sell_tax_bps == 0
        assert report.buy_tax_bps == 0
        # Must not be cached as a fatal rejection
        assert gk.is_rejected("7cEMq8uRd43dFnaj6xf73QLxKMfLf5Es8FjiszMqpump", ChainIdentifier.SOLANA_MAINNET) is False


def test_pump_fun_token_never_receives_10000_bps_sell_tax():
    """Verify Pump.fun token in _parse_rugcheck_report never gets stamped with 10,000 bps tax."""
    pump_mint = "7cEMq8uRd43dFnaj6xf73QLxKMfLf5Es8FjiszMqpump"
    raw_invalid = {"invalid_mint": True}

    report = _parse_rugcheck_report(pump_mint, raw_invalid)
    assert report.is_pump_fun is True
    assert report.tier == SecurityTier.TIER1_REJECTED
    assert report.sell_tax_bps == 0
    assert report.buy_tax_bps == 0


def test_negative_rejection_cache_transient_and_pump_not_fatal():
    """Verify NegativeRejectionCache treats transient unindexed and pump.fun rejections as soft probation."""
    cache = NegativeRejectionCache(maxsize=10, ttl_seconds=300.0)

    # 1. Unindexed / network fallback report
    rep_unindexed = SecurityReport(
        token_address="UnindexedMint1111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.TIER1_REJECTED,
        is_honeypot=False,
        buy_tax_bps=10_000,
        sell_tax_bps=10_000,
        external_api_raw='{"invalid_mint": true, "state": "UNINDEXED_NEW_BONDING_CURVE"}',
    )
    cache.record_rejection(
        "UnindexedMint1111111111111111111111111111111",
        ChainIdentifier.SOLANA_MAINNET,
        rep_unindexed,
    )
    k = cache._make_key("UnindexedMint1111111111111111111111111111111", ChainIdentifier.SOLANA_MAINNET)
    assert cache._cache[k].is_fatal is False

    # Soft rejected token CAN be reassessed when significant momentum arrives (>= 0.25 SOL)
    assert cache.can_reassess(
        "UnindexedMint1111111111111111111111111111111",
        ChainIdentifier.SOLANA_MAINNET,
        sol_amount=Decimal("1.5"),
    ) is True

    # 2. Confirmed honeypot scam IS fatal
    rep_honeypot = SecurityReport(
        token_address="ScamToken111111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.TIER1_REJECTED,
        is_honeypot=True,
        buy_tax_bps=10_000,
        sell_tax_bps=10_000,
        external_api_raw='{"is_honeypot": true}',
    )
    cache.record_rejection(
        "ScamToken111111111111111111111111111111111111",
        ChainIdentifier.SOLANA_MAINNET,
        rep_honeypot,
        reason="Confirmed Honeypot",
    )
    k_scam = cache._make_key("ScamToken111111111111111111111111111111111111", ChainIdentifier.SOLANA_MAINNET)
    assert cache._cache[k_scam].is_fatal is True
    assert cache.can_reassess(
        "ScamToken111111111111111111111111111111111111",
        ChainIdentifier.SOLANA_MAINNET,
        sol_amount=Decimal("5.0"),
    ) is False
