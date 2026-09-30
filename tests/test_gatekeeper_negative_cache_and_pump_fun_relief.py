"""
tests.test_gatekeeper_negative_cache_and_pump_fun_relief
=========================================================
Tests verifying:
1. Gatekeeper in-memory TTL/LRU Negative Cache (zero-latency drop of rejected mints).
2. Separation of token ingestion from swap ingestion (swaps bypass gatekeeper security screening).
3. Pump.fun bonding curve exclusion from Top 10 holder concentration.
4. Pump.fun relaxed concentration threshold (65% limit instead of 20%/50%).
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.models.enums import ChainIdentifier, SecurityTier, SignalSource
from alpha_engine.models.events import RawSignalEvent, SwapEvent
from alpha_engine.models.state import SecurityReport
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import (
    _MAX_PUMP_FUN_TOP10_CONCENTRATION,
    _MAX_TOP10_CONCENTRATION,
    derive_pump_fun_bonding_curve,
    is_pump_fun_token,
)
from alpha_engine.security.gatekeeper import NegativeRejectionCache, SecurityGatekeeper
from alpha_engine.security.preflight import verify_solana_mint_preflight
from alpha_engine.security.rugcheck import _parse_rugcheck_report


def test_negative_rejection_cache_ttl_and_lru():
    """Verify in-memory NegativeRejectionCache basic storage, TTL expiry, and capacity LRU eviction."""
    cache = NegativeRejectionCache(maxsize=3, ttl_seconds=0.2)
    assert len(cache) == 0

    mint1 = "QmUdgPAvJz111111111111111111111111111111111"
    mint2 = "ConGoUKgS611111111111111111111111111111111"
    mint3 = "TokenThree111111111111111111111111111111111"
    mint4 = "TokenFour1111111111111111111111111111111111"

    # 1. Record rejection
    cache.record_rejection(mint1, ChainIdentifier.SOLANA_MAINNET, reason="Honeypot")
    assert cache.is_rejected(mint1, ChainIdentifier.SOLANA_MAINNET) is True
    assert cache.is_rejected("UnrelatedMint1111111111111111111111111111111", ChainIdentifier.SOLANA_MAINNET) is False
    rep = cache.get_rejection(mint1, ChainIdentifier.SOLANA_MAINNET)
    assert rep is not None
    assert rep.tier == SecurityTier.TIER1_REJECTED

    # 2. Add up to maxsize and trigger eviction
    cache.record_rejection(mint2, ChainIdentifier.SOLANA_MAINNET)
    cache.record_rejection(mint3, ChainIdentifier.SOLANA_MAINNET)
    assert len(cache) == 3

    cache.record_rejection(mint4, ChainIdentifier.SOLANA_MAINNET)
    assert len(cache) <= 3
    assert cache.is_rejected(mint4, ChainIdentifier.SOLANA_MAINNET) is True

    # 3. Test TTL expiration
    time.sleep(0.25)
    assert cache.is_rejected(mint4, ChainIdentifier.SOLANA_MAINNET) is False
    assert cache.get_rejection(mint4, ChainIdentifier.SOLANA_MAINNET) is None


@pytest.mark.anyio
async def test_gatekeeper_screen_token_negative_cache_fast_path():
    """Verify SecurityGatekeeper screen_token populates negative cache on rejection and drops future calls with 0 latency."""
    session = AsyncMock()
    limiter = RateLimiterRegistry.default()
    gk = SecurityGatekeeper(
        session=session,
        limiter=limiter,
        evm_rpc_url="",
        evm_router_address="",
        weth_address="",
        solana_rpc_url="",
        negative_cache_ttl_s=10.0,
    )

    mint = "QmUdgPAvJz111111111111111111111111111111111"

    # First call: simulate RugCheck returning rejected report (sell tax 100%)
    rejected_raw = {
        "mintAuthority": None,
        "freezeAuthority": None,
        "risks": [{"name": "Transfer fee", "score": 10000}],  # 100% tax
        "topHolders": [],
    }

    with patch("alpha_engine.security.gatekeeper._fetch_rugcheck_report", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = rejected_raw

        report1 = await gk.screen_token(mint, ChainIdentifier.SOLANA_MAINNET)
        assert report1.tier == SecurityTier.TIER1_REJECTED
        assert report1.passes_hard_gates is False
        assert mock_fetch.call_count == 1
        assert gk.is_rejected(mint, ChainIdentifier.SOLANA_MAINNET) is True

        # Second call: must hit negative cache and NOT call _fetch_rugcheck_report
        report2 = await gk.screen_token(mint, ChainIdentifier.SOLANA_MAINNET)
        assert report2.tier == SecurityTier.TIER1_REJECTED
        assert mock_fetch.call_count == 1  # No additional network call!


def test_pump_fun_bonding_curve_pda_derivation():
    """Verify derive_pump_fun_bonding_curve derives matching PDA and ATA for pump tokens."""
    mint = "8xBGzQDTfJm8xZfxbHqZ8VvL3WqCj4D7Jb1a7d6upump"
    assert is_pump_fun_token(mint) is True
    assert is_pump_fun_token("NonPumpMint11111111111111111111111111111111") is False

    curve_pda, curve_ata = derive_pump_fun_bonding_curve(mint)
    assert curve_pda is not None
    assert curve_ata is not None
    assert len(curve_pda) >= 32
    assert len(curve_ata) >= 32


def test_pump_fun_relaxed_concentration_and_bonding_curve_exclusion():
    """Verify that Pump.fun tokens exclude bonding curve and accept up to 65% top-10 concentration."""
    mint = "8xBGzQDTfJm8xZfxbHqZ8VvL3WqCj4D7Jb1a7d6upump"
    curve_pda, curve_ata = derive_pump_fun_bonding_curve(mint)

    # Simulated RugCheck raw payload where bonding curve holds 80%, top 10 retail hold 40%
    raw_rugcheck = {
        "mintAuthority": None,
        "freezeAuthority": None,
        "markets": [{"lp": {"lpLockedPct": 100.0}}],
        "topHolders": [
            {"address": curve_pda, "pct": 80.0, "insider": False},  # Bonding curve (must be excluded)
            {"address": "Holder1", "pct": 15.0, "insider": False},
            {"address": "Holder2", "pct": 10.0, "insider": False},
            {"address": "Holder3", "pct": 10.0, "insider": False},
            {"address": "Holder4", "pct": 5.0, "insider": False},
        ],
        "risks": [],
    }

    report = _parse_rugcheck_report(mint, raw_rugcheck, pool_address=curve_pda or "")
    # Top 10 sum without the 80% bonding curve = 15 + 10 + 10 + 5 = 40% (0.40)
    assert report.top10_concentration == pytest.approx(0.40, rel=1e-2)
    assert report.is_pump_fun is True
    assert report.passes_hard_gates is True
    assert report.tier == SecurityTier.CLEAN

    # Non-pump token with 40% concentration MUST fail hard gates
    non_pump_mint = "StandardMint1111111111111111111111111111111"
    raw_non_pump = {
        "mintAuthority": None,
        "freezeAuthority": None,
        "markets": [{"lp": {"lpLockedPct": 100.0}}],
        "topHolders": [
            {"address": "Holder1", "pct": 20.0, "insider": False},
            {"address": "Holder2", "pct": 20.0, "insider": False},
        ],
        "risks": [],
    }
    non_pump_rep = _parse_rugcheck_report(non_pump_mint, raw_non_pump)
    assert non_pump_rep.top10_concentration == pytest.approx(0.40, rel=1e-2)
    assert non_pump_rep.is_pump_fun is False
    assert non_pump_rep.passes_hard_gates is False
    assert non_pump_rep.tier == SecurityTier.TIER1_REJECTED


@pytest.mark.anyio
async def test_verify_solana_mint_preflight_pump_fun_bonding_curve_exclusion():
    """Verify verify_solana_mint_preflight excludes Pump.fun bonding curve and enforces concentration limit."""
    mint = "8xBGzQDTfJm8xZfxbHqZ8VvL3WqCj4D7Jb1a7d6upump"
    curve_pda, _ = derive_pump_fun_bonding_curve(mint)
    total_supply = 1_000_000_000

    class MockRpcResponse:
        def __init__(self, data: dict):
            self.data = data
            self.status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

        async def json(self):
            return self.data

    def post_mock(url, json=None, timeout=None):
        method = json.get("method") if json else ""
        if method == "getAccountInfo":
            return MockRpcResponse({
                "result": {
                    "value": {
                        "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                        "data": {
                            "parsed": {
                                "type": "mint",
                                "info": {
                                    "mintAuthority": None,
                                    "freezeAuthority": None,
                                    "decimals": 6,
                                },
                            }
                        },
                    }
                }
            })
        elif method == "getTokenSupply":
            return MockRpcResponse({"result": {"value": {"amount": total_supply}}})
        elif method == "getTokenLargestAccounts":
            return MockRpcResponse({
                "result": {
                    "value": [
                        {"address": curve_pda, "amount": 800_000_000},  # 80% bonding curve
                        {"address": "RetailHolder1", "amount": 200_000_000},  # 20%
                    ]
                }
            })
        return MockRpcResponse({})

    session = MagicMock()
    session.post = MagicMock(side_effect=post_mock)

    result = await verify_solana_mint_preflight(
        session=session,
        mint_address=mint,
        rpc_url="https://api.mainnet-beta.solana.com",
    )
    assert result is not None
    # 80% bonding curve is excluded, leaving 200M / 1,000,000,000 = 20%
    assert result["top10_concentration"] == pytest.approx(0.20, rel=1e-2)
    assert result["top10_concentration"] <= _MAX_PUMP_FUN_TOP10_CONCENTRATION
    assert result["developer_sniper_bundle"] is False

    # Heavy holder with 70% (> _MAX_PUMP_FUN_TOP10_CONCENTRATION = 0.65)
    def post_mock_heavy(url, json=None, timeout=None):
        method = json.get("method") if json else ""
        if method == "getAccountInfo":
            return MockRpcResponse({
                "result": {
                    "value": {
                        "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                        "data": {
                            "parsed": {
                                "type": "mint",
                                "info": {"mintAuthority": None, "freezeAuthority": None, "decimals": 6},
                            }
                        },
                    }
                }
            })
        elif method == "getTokenSupply":
            return MockRpcResponse({"result": {"value": {"amount": total_supply}}})
        elif method == "getTokenLargestAccounts":
            return MockRpcResponse({
                "result": {
                    "value": [
                        {"address": curve_pda, "amount": 250_000_000},
                        {"address": "WhaleHolder", "amount": 700_000_000},  # 70%
                    ]
                }
            })
        return MockRpcResponse({})

    session.post = MagicMock(side_effect=post_mock_heavy)
    heavy_result = await verify_solana_mint_preflight(
        session=session,
        mint_address=mint,
        rpc_url="https://api.mainnet-beta.solana.com",
    )
    assert heavy_result is not None
    assert heavy_result["developer_sniper_bundle"] is True
    assert heavy_result["top10_concentration"] > _MAX_TOP10_CONCENTRATION
    assert heavy_result["top10_concentration"] > _MAX_PUMP_FUN_TOP10_CONCENTRATION


@pytest.mark.anyio
async def test_swap_ingestion_bypasses_security_screening_and_drains_queue():
    """
    Verify that incoming swap events:
    1. Check negative cache and drop trades for rejected tokens immediately.
    2. Completely bypass SecurityGatekeeper.screen_token for unstaged/staged tokens.
    3. Route exclusively to PendingLaunchBuffer.record_trade().
    4. Drain ingestion queue without backlog deadlock.
    """
    config = EngineConfig(
        observation_window_min_sec=5.0,
        observation_window_max_sec=60.0,
        buffer_min_buys=2,
        buffer_target_volume_sol=Decimal("0.5"),
        buffer_min_unique_buyers=2,
    )
    engine = PaperTradingEngine(config)

    # Mock gatekeeper to verify screen_token is NEVER invoked for swaps
    mock_gk = MagicMock(spec=SecurityGatekeeper)
    mock_gk.is_rejected = MagicMock(return_value=False)
    mock_gk.screen_token = AsyncMock()
    engine._gatekeeper = mock_gk

    staged_token = "SafePumpToken11111111111111111111111111pump"
    rejected_token = "QmUdgPAvJz111111111111111111111111111111111"
    pool_addr = "PumpCurve11111111111111111111111111111111111"

    # Configure mock_gk to report rejected_token as rejected in negative cache
    def mock_is_rejected_impl(token, chain=None):
        return token == rejected_token
    mock_gk.is_rejected.side_effect = mock_is_rejected_impl

    # Stage the safe token in PendingLaunchBuffer
    clean_report = SecurityReport(
        token_address=staged_token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        top10_concentration=0.35,  # 35% < 65% limit
    )
    raw_sig = RawSignalEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=staged_token,
        pool_address=pool_addr,
        source=SignalSource.PUMP_FUN_MINT,
    )
    engine._pending_launch_buffer.add_launch(
        token_address=staged_token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        initial_price=Decimal("0.000000030"),
        report=clean_report,
        raw_signal=raw_sig,
        t_0=time.time() - 10.0,
    )

    engine.get_or_create_initial_pool_state(
        ChainIdentifier.SOLANA_MAINNET,
        staged_token,
        pool_addr,
    )

    # Enqueue swaps for the rejected token (should be dropped instantly)
    now_ns = time.time_ns()
    rejected_swap = SwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        block_number=100,
        tx_hash="rej1" * 16,
        sender="BuyerRej11111111111111111111111111111111",
        pool_address=pool_addr,
        token_in="So11111111111111111111111111111111111111112",
        token_out=rejected_token,
        amount_in=Decimal("0.5"),
        amount_out=Decimal("1000000"),
        timestamp_ns=now_ns,
    )
    await engine._ingestion_q.put(rejected_swap)

    # Enqueue 3 swaps across 3 blocks for the staged token (should graduate without screening)
    swap1 = SwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        block_number=101,
        tx_hash="safe1" * 16,
        sender="BuyerA1111111111111111111111111111111111",
        pool_address=pool_addr,
        token_in="So11111111111111111111111111111111111111112",
        token_out=staged_token,
        amount_in=Decimal("0.3"),
        amount_out=Decimal("10000000"),
        timestamp_ns=now_ns + 1_000_000,
    )
    swap2 = SwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        block_number=102,
        tx_hash="safe2" * 16,
        sender="BuyerB1111111111111111111111111111111111",
        pool_address=pool_addr,
        token_in="So11111111111111111111111111111111111111112",
        token_out=staged_token,
        amount_in=Decimal("0.3"),
        amount_out=Decimal("10000000"),
        timestamp_ns=now_ns + 2_000_000,
    )
    swap3 = SwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        block_number=103,
        tx_hash="safe3" * 16,
        sender="BuyerA1111111111111111111111111111111111",
        pool_address=pool_addr,
        token_in="So11111111111111111111111111111111111111112",
        token_out=staged_token,
        amount_in=Decimal("0.3"),
        amount_out=Decimal("10000000"),
        timestamp_ns=now_ns + 3_000_000,
    )
    await engine._ingestion_q.put(swap1)
    await engine._ingestion_q.put(swap2)
    await engine._ingestion_q.put(swap3)

    # Process all items through _process_ingestion_queue
    task = asyncio.create_task(engine._process_ingestion_queue())
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass

    # Ingestion queue must be drained to 0!
    assert engine._ingestion_q.qsize() == 0

    # Gatekeeper screen_token MUST NEVER have been called for swaps!
    assert mock_gk.screen_token.call_count == 0

    # Staged token graduated and placed in signal queue!
    assert not engine._signal_q.empty()
    signal = await engine._signal_q.get()
    assert signal.token_address == staged_token
