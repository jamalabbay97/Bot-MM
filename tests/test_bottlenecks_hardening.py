"""
tests/test_bottlenecks_hardening.py — Tests for production bottleneck resolutions
================================================================================
Validates:
1. Fast-path blacklist & quote/native token filter in SecurityGatekeeper.
2. RugCheck retry loop with exponential backoff on HTTP 400 & local RPC fallback.
3. TokenTTLCache and coordinator deduplication within 60s sliding window.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure project root and virtual environment site-packages are always in sys.path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

for _p in (_ROOT / ".venv" / "lib").glob("python*/site-packages"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from alpha_engine.ingestion.coordinator import IngestionCoordinator, TokenTTLCache
from alpha_engine.models.enums import ChainIdentifier, SecurityTier, SignalSource
from alpha_engine.models.events import RawSignalEvent, ShutdownSentinel
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import is_blacklisted_token
from alpha_engine.security.gatekeeper import SecurityGatekeeper
from alpha_engine.security.rugcheck import (
    _fetch_rugcheck_report,
    _parse_rugcheck_report,
)


def test_fast_path_token_blacklist_filtering():
    """Verify that WSOL and standard quote/base tokens skip external network calls."""
    async def _run():
        session = AsyncMock()
        limiter = RateLimiterRegistry.default()

        gk = SecurityGatekeeper(
            session=session,
            limiter=limiter,
            evm_rpc_url="https://base-mainnet.g.alchemy.com/v2/demo",
            evm_router_address="0x" + "1" * 40,
            weth_address="0x4200000000000000000000000000000000000006",
            enable_tier2=False,
        )

        gk._run_tier1 = AsyncMock()

        # 1. Solana Wrapped SOL
        wsol = "So11111111111111111111111111111111111111112"
        assert SecurityGatekeeper.is_blacklisted(wsol, ChainIdentifier.SOLANA_MAINNET) is True
        assert is_blacklisted_token(wsol, ChainIdentifier.SOLANA_MAINNET) is True

        rep_wsol = await gk.screen_token(wsol, ChainIdentifier.SOLANA_MAINNET)
        assert rep_wsol.tier == SecurityTier.TIER1_REJECTED
        assert rep_wsol.passes_hard_gates is False
        gk._run_tier1.assert_not_awaited()

        # 2. Solana USDC
        usdc_sol = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        rep_usdc = await gk.screen_token(usdc_sol, ChainIdentifier.SOLANA_MAINNET)
        assert rep_usdc.tier == SecurityTier.TIER1_REJECTED
        gk._run_tier1.assert_not_awaited()

        # 3. EVM Base WETH
        weth_base = "0x4200000000000000000000000000000000000006"
        assert SecurityGatekeeper.is_blacklisted(weth_base, ChainIdentifier.BASE_MAINNET) is True
        rep_weth = await gk.screen_token(weth_base, ChainIdentifier.BASE_MAINNET)
        assert rep_weth.tier == SecurityTier.TIER1_REJECTED
        gk._run_tier1.assert_not_awaited()

        # 4. Legitimate meme coin address should NOT be blacklisted
        clean_mint = "TokenMint1111111111111111111111111111111111"
        assert SecurityGatekeeper.is_blacklisted(clean_mint, ChainIdentifier.SOLANA_MAINNET) is False

    asyncio.run(_run())


def test_rugcheck_retry_loop_on_http_400():
    """Verify that unindexed mints returning HTTP 400 do not perform progressive sleep retries, falling back immediately."""
    async def _run():
        limiter = RateLimiterRegistry.default()
        mint = "PumpFunNewMint1111111111111111111111111111"

        resp_400 = AsyncMock()
        resp_400.status = 400

        class MockContextManager:
            async def __aenter__(self):
                return resp_400

            async def __aexit__(self, *args):
                pass

        session = MagicMock()
        session.get = MagicMock(side_effect=lambda *args, **kwargs: MockContextManager())

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep, \
             patch("alpha_engine.security.rugcheck.verify_solana_mint_preflight", new_callable=AsyncMock) as mock_preflight:
            mock_preflight.return_value = {
                "mint_authority": None,
                "freeze_authority": None,
                "top10_concentration": 0.15,
                "rpc_fallback": True,
            }
            report = await _fetch_rugcheck_report(session, mint, limiter, rpc_url="https://api.mainnet-beta.solana.com")
            assert report is not None
            assert report.get("mintAuthority") is None
            assert report.get("rpc_fallback") is True
            # Zero progressive sleep retries on HTTP 400
            assert mock_sleep.await_count == 0
            mock_preflight.assert_awaited_once()

    asyncio.run(_run())


def test_rugcheck_unindexed_mint_rpc_fallback():
    """Verify that persistent HTTP 400 falls back to local RPC and creates a valid report."""
    async def _run():
        limiter = RateLimiterRegistry.default()
        mint = "BrandNewPumpFunMint1111111111111111111111111"

        resp_400 = AsyncMock()
        resp_400.status = 400

        class MockRugcheckContext:
            async def __aenter__(self):
                return resp_400

            async def __aexit__(self, *args):
                pass

        resp_rpc = AsyncMock()
        resp_rpc.status = 200
        resp_rpc.json = AsyncMock(
            return_value={
                "jsonrpc": "2.0",
                "result": {
                    "value": {
                        "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                        "data": {
                            "parsed": {
                                "type": "mint",
                                "info": {
                                    "decimals": 6,
                                    "mintAuthority": None,
                                    "freezeAuthority": None,
                                },
                            }
                        },
                    }
                },
            }
        )

        class MockRpcContext:
            async def __aenter__(self):
                return resp_rpc

            async def __aexit__(self, *args):
                pass

        session = MagicMock()
        session.get = MagicMock(side_effect=lambda *args, **kwargs: MockRugcheckContext())
        session.post = MagicMock(side_effect=lambda *args, **kwargs: MockRpcContext())

        with patch("asyncio.sleep", new_callable=AsyncMock):
            raw_report = await _fetch_rugcheck_report(
                session, mint, limiter, rpc_url="https://api.mainnet-beta.solana.com"
            )
            assert raw_report is not None
            assert raw_report.get("on_chain_fallback") is True

            parsed_rep = _parse_rugcheck_report(mint, raw_report)
            assert parsed_rep.tier == SecurityTier.CLEAN
            assert parsed_rep.sell_tax_bps == 0
            assert parsed_rep.buy_tax_bps == 0
            assert parsed_rep.mint_authority_disabled is True
            assert parsed_rep.lp_burned_ratio >= 0.90
            assert parsed_rep.passes_hard_gates is True

    asyncio.run(_run())


def test_token_ttl_cache_deduplication():
    """Verify TokenTTLCache drops duplicate tokens within 60s window and accepts them after TTL."""
    async def _run():
        cache = TokenTTLCache(ttl_seconds=60.0)
        token = "MemeCoin11111111111111111111111111111111111"

        # First access -> not duplicate
        is_dup1 = await cache.is_duplicate_or_add(token)
        assert is_dup1 is False

        # Second immediate access -> duplicate
        is_dup2 = await cache.is_duplicate_or_add(token)
        assert is_dup2 is True

        # Different token -> not duplicate
        token2 = "OtherCoin22222222222222222222222222222222222"
        assert await cache.is_duplicate_or_add(token2) is False

        # Simulate 65 seconds passing
        with patch("time.monotonic", return_value=time.monotonic() + 65.0):
            assert await cache.is_duplicate_or_add(token) is False

    asyncio.run(_run())


def test_coordinator_drops_duplicate_and_blacklisted_signals():
    """Verify IngestionCoordinator drops duplicate RawSignalEvents within TTL and blacklisted tokens."""
    async def _run():
        limiter = RateLimiterRegistry.default()

        coord = IngestionCoordinator(
            evm_ws_url="wss://base-mainnet.g.alchemy.com/v2/demo",
            svm_ws_url="wss://mainnet.helius-rpc.com/?api-key=demo",
            pool_watchlist=[],
            pool_registry={},
            limiter=limiter,
            telegram_ingester=MagicMock(),
        )

        q = coord.event_queue
        wsol = "So11111111111111111111111111111111111111112"
        meme = "ValidMemeCoin111111111111111111111111111111"

        # 1. WSOL signal (should be dropped because blacklisted)
        # 2. Valid meme coin signal 1 (should be yielded)
        # 3. Duplicate valid meme coin signal 2 (should be dropped by TTL dedup)
        # 4. ShutdownSentinel (should terminate iteration)
        await q.put(
            RawSignalEvent(
                chain=ChainIdentifier.SOLANA_MAINNET,
                token_address=wsol,
                source=SignalSource.TELEGRAM_SCRAPER,
                originating_channel="alpha_call",
                raw_text="buy wsol",
            )
        )
        await q.put(
            RawSignalEvent(
                chain=ChainIdentifier.SOLANA_MAINNET,
                token_address=meme,
                source=SignalSource.TELEGRAM_SCRAPER,
                originating_channel="alpha_call",
                raw_text="buy meme",
            )
        )
        await q.put(
            RawSignalEvent(
                chain=ChainIdentifier.SOLANA_MAINNET,
                token_address=meme,
                source=SignalSource.PUMP_FUN_MINT,
                originating_channel="pump_fun",
                raw_text="pump meme",
            )
        )
        await q.put(ShutdownSentinel())

        yielded_items = []
        async for item in coord:
            yielded_items.append(item)

        assert len(yielded_items) == 1
        assert isinstance(yielded_items[0], RawSignalEvent)
        assert yielded_items[0].token_address == meme
        assert yielded_items[0].source == SignalSource.TELEGRAM_SCRAPER

    asyncio.run(_run())


if __name__ == "__main__":
    print("Testing fast-path token blacklist filtering...")
    test_fast_path_token_blacklist_filtering()
    print("Testing RugCheck retry loop on HTTP 400...")
    test_rugcheck_retry_loop_on_http_400()
    print("Testing RugCheck unindexed mint RPC fallback...")
    test_rugcheck_unindexed_mint_rpc_fallback()
    print("Testing TokenTTLCache deduplication...")
    test_token_ttl_cache_deduplication()
    print("Testing coordinator drops duplicate & blacklisted signals...")
    test_coordinator_drops_duplicate_and_blacklisted_signals()
    print("\n>>> ALL 5 TESTS PASSED SUCCESSFULLY! <<<")
