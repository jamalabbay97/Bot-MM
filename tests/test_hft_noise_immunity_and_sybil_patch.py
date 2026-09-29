"""
tests/test_hft_noise_immunity_and_sybil_patch.py — Verification of HFT Noise Immunity,
RugCheck DNS / HTTP 400 Fallback, and PendingLaunchBuffer Sybil & Bundle Protections.
"""

from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.dns_resolver import (
    _RESOLVED_CACHE,
    patch_dns_resolvers,
    preresolve_trusted_endpoints,
)
from alpha_engine.engine.signals import PendingLaunchBuffer, SignalGenerator
from alpha_engine.execution.book import OpenLot, PositionBook
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitStage,
    SecurityTier,
    SignalSource,
    TradeExitReason,
)
from alpha_engine.models.events import RawSignalEvent, SwapEvent
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.security.gatekeeper import SecurityGatekeeper
from alpha_engine.security.rugcheck import (
    UNINDEXED_MINT_CACHE,
    _fetch_rugcheck_report,
    _parse_rugcheck_report,
)


# ==============================================================================
# 1. PREVENT PREMATURE STOP-OUTS & TIGHT EXIT CHURN (GRACE PERIOD & VWAP)
# ==============================================================================

def test_exit_grace_period_suppresses_micro_tick_noise():
    """Verify that trailing stops and tight hard-stops are suppressed during the 15s grace period."""
    book = PositionBook()
    now_s = 1000.0
    now_ns = int(now_s * 1e9)

    lot = OpenLot(
        token_address="GrfrToken111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("1000000"),
        tokens_held=Decimal("1000000"),
        entry_price=Decimal("0.0000100"),
        peak_price=Decimal("0.0000100"),
        cost_basis_native=Decimal("10.0"),
        open_timestamp_ns=now_ns,  # opened right at now
        bonding_curve_mode=True,
        trailing_stop_active=True,
        trailing_stop_price=Decimal("0.0000095"),  # tight stop
    )

    # 1. Micro-tick noise at t + 3s (< 15s grace period): price dips to 0.0000092 (-8%)
    decision_noise = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.0000092"),
        tick_timestamp_s=now_s + 3.0,
        current_timestamp_ns=now_ns + int(3.0 * 1e9),
        bonding_curve_mode=True,
        exit_grace_period_sec=15.0,
    )
    # Must NOT trigger exit during grace period
    assert decision_noise is None

    # 2. Emergency Hard Stop at t + 5s: price drops -30% (below -25% emergency threshold)
    decision_emergency = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.0000070"),  # -30% drop <= -25%
        tick_timestamp_s=now_s + 5.0,
        current_timestamp_ns=now_ns + int(5.0 * 1e9),
        bonding_curve_mode=True,
        exit_grace_period_sec=15.0,
    )
    assert decision_emergency is not None
    assert decision_emergency.should_exit is True
    assert decision_emergency.exit_reason == TradeExitReason.SL_HARD
    assert decision_emergency.prioritized is True

    # 3. Normal stop after grace period expires (t + 18s > 15s)
    lot_post_grace = OpenLot(
        token_address="GrfrToken111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("1000000"),
        tokens_held=Decimal("1000000"),
        entry_price=Decimal("0.0000100"),
        peak_price=Decimal("0.0000120"),
        cost_basis_native=Decimal("10.0"),
        open_timestamp_ns=now_ns,
        bonding_curve_mode=True,
        trailing_stop_active=True,
        trailing_stop_price=Decimal("0.0000108"),
    )
    # Add prior ticks so VWAP is established
    lot_post_grace.record_tick(Decimal("0.0000120"), Decimal("1.0"), now_s + 16.0)
    lot_post_grace.record_tick(Decimal("0.0000105"), Decimal("2.0"), now_s + 18.0, is_sell=True)

    decision_post_grace = book.evaluate_lot_exit(
        lot=lot_post_grace,
        current_price=Decimal("0.0000105"),
        tick_timestamp_s=now_s + 18.0,
        current_timestamp_ns=now_ns + int(18.0 * 1e9),
        bonding_curve_mode=True,
        exit_grace_period_sec=15.0,
    )
    assert decision_post_grace is not None
    assert decision_post_grace.should_exit is True
    assert decision_post_grace.exit_reason == TradeExitReason.SL_TRAILING


def test_vwap_and_virtual_reserve_dump_distinction():
    """Verify that a single micro-tick dip does not trigger trailing stop if VWAP holds and reserve is intact."""
    book = PositionBook()
    now = time.time()

    lot = OpenLot(
        token_address="Hk45Token111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("1.0"),
        tokens_held=Decimal("1.0"),
        entry_price=Decimal("1.0"),
        peak_price=Decimal("1.50"),
        cost_basis_native=Decimal("1.0"),
        bonding_curve_mode=True,
        exit_stage=ExitStage.TP1,
        trailing_stop_active=True,
        trailing_stop_price=Decimal("1.30"),
        initial_pool_reserve_native=Decimal("30.0"),
        peak_pool_reserve_native=Decimal("35.0"),
    )

    # Populate moving window with high volume purchases at 1.45
    lot.record_tick(Decimal("1.45"), Decimal("5.0"), now - 4.0)
    lot.record_tick(Decimal("1.45"), Decimal("5.0"), now - 2.0)

    # 1. Single micro-tick drop: price = 1.25 (< 1.30 stop), but trade volume is small (0.05 SOL)
    # and reserve remains stable at 34.9 SOL (normal curve slippage, not a dump)
    decision_single_tick = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.25"),
        current_pool_reserve_native=Decimal("34.9"),
        tick_timestamp_s=now,
        bonding_curve_mode=True,
        trade_volume_native=Decimal("0.05"),
        is_sell=True,
    )
    # VWAP is ~1.39, reserve is intact -> exit must be suppressed as noise
    assert decision_single_tick is None

    # 2. Cumulative sell-volume spike / reserve drainage: pool reserve drops from 35.0 to 32.5 (loss of 2.5 SOL >= 1.5 SOL)
    decision_real_dump = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.25"),
        current_pool_reserve_native=Decimal("32.5"),  # 2.5 SOL dump confirmed
        tick_timestamp_s=now + 1.0,
        bonding_curve_mode=True,
        trade_volume_native=Decimal("2.5"),
        is_sell=True,
    )
    assert decision_real_dump is not None
    assert decision_real_dump.should_exit is True
    assert decision_real_dump.exit_reason == TradeExitReason.SL_TRAILING


# ==============================================================================
# 2. RUGCHECK DNS PRE-RESOLUTION & HTTP 400 UNINDEXED FALLBACK
# ==============================================================================

def test_dns_resolver_preresolve_trusted_endpoints():
    """Verify that preresolve_trusted_endpoints seeds cache and bypasses sinkhole intercepts."""
    import socket

    patch_dns_resolvers(servers=["1.1.1.1", "8.8.8.8"])
    preresolve_trusted_endpoints(servers=["1.1.1.1", "8.8.8.8"])

    assert "api.rugcheck.xyz" in _RESOLVED_CACHE
    assert "mainnet.helius-rpc.com" in _RESOLVED_CACHE

    # Check that socket.getaddrinfo returns the verified IP without hitting system resolver
    addrs = socket.getaddrinfo("api.rugcheck.xyz", 443)
    assert len(addrs) > 0
    ip = addrs[0][4][0]
    assert ip == "174.138.15.144"
    assert not ip.startswith("146.112.")


@pytest.mark.anyio
async def test_rugcheck_http_400_reclassified_as_unindexed_new_bonding_curve():
    """Verify HTTP 400 is reclassified as expected UNINDEXED_NEW_BONDING_CURVE and silently routes to preflight."""
    mint = "UnindexedMint1111111111111111111111111111111"
    UNINDEXED_MINT_CACHE.clear()

    mock_resp = AsyncMock()
    mock_resp.status = 400
    mock_resp.__aenter__.return_value = mock_resp

    mock_session = MagicMock()
    mock_session.get.return_value = mock_resp

    mock_limiter = MagicMock()
    mock_limiter.helius.acquire = AsyncMock()

    with patch("alpha_engine.security.rugcheck.verify_solana_mint_preflight", new_callable=AsyncMock) as mock_preflight:
        mock_preflight.return_value = {
            "on_chain_fallback": True,
            "mint": mint,
            "mintAuthority": None,
            "freezeAuthority": None,
            "top10_concentration": 0.12,
            "developer_sniper_bundle": False,
        }

        res = await _fetch_rugcheck_report(
            session=mock_session,
            mint_address=mint,
            limiter=mock_limiter,
            rpc_url="https://solana.rpc.endpoint",
            token_age_s=120.0,
        )

        assert res is not None
        assert res.get("on_chain_fallback") is True
        assert res.get("state") == "UNINDEXED_NEW_BONDING_CURVE"
        assert UNINDEXED_MINT_CACHE.is_cached(mint) is True

        # Parse report
        parsed = _parse_rugcheck_report(mint, res)
        assert parsed.tier == SecurityTier.CLEAN
        assert parsed.passes_hard_gates is True


# ==============================================================================
# 3. PENDING LAUNCH BUFFER SYBIL & DEV-BUNDLE FILTERING
# ==============================================================================

def test_pending_launch_buffer_sybil_and_bundle_rejection():
    """Verify PendingLaunchBuffer rejects tokens with low buyer entropy (<8) or slot clustering (>=60%)."""
    buffer = PendingLaunchBuffer(
        min_age_s=90.0,
        max_age_s=300.0,
        min_buys=15,
        min_volume_native=Decimal("15.0"),
        min_unique_signers=8,
        slot_bundle_threshold=0.60,
        min_blocks_span=3,
    )
    sig_gen = SignalGenerator()
    pool_addr = "CurveAddr33333333333333333333333333333"
    t_0 = 1000.0

    pool = PoolState(
        pool_address=pool_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="TokenBundled",
        native_reserve=Decimal("30.0"),
        token_reserve=Decimal("1073000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=100,
    )

    # --- Scenario A: Slot Clustering / Jito Bundle Detection (>= 60% in exact same slot) ---
    token_bundle = "BundleMint11111111111111111111111111111111"
    report = SecurityReport(
        token_address=token_bundle,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    raw_sig = RawSignalEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token_bundle,
        pool_address=pool_addr,
        source=SignalSource.PUMP_FUN_MINT,
    )
    staged_b = buffer.add_launch(
        token_address=token_bundle,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        initial_price=Decimal("0.000000028"),
        report=report,
        raw_signal=raw_sig,
        t_0=t_0,
    )

    # 4 buys in slot 100, 1 buy in slot 101 -> 4/5 = 80% >= 60% slot cluster
    for i in range(4):
        sw = SwapEvent(
            timestamp_ns=int((t_0 + 95.0) * 1e9),
            block_number=100,  # SAME SLOT
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_addr,
            token_in="So11111111111111111111111111111111111111112",
            token_out=token_bundle,
            amount_in=Decimal("3.0"),
            amount_out=Decimal("100000"),
            sender=f"Buyer{i:028d}",
            tx_hash=f"tx_cluster_{i:024d}",
        )
        buffer.record_swap(sw, pool, sig_gen, current_time_s=t_0 + 95.0)

    # 5th buy in slot 100 triggers evaluation (5/5 = 100% in slot 100)
    sw_cluster_trigger = SwapEvent(
        timestamp_ns=int((t_0 + 95.0) * 1e9),
        block_number=100,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        token_in="So11111111111111111111111111111111111111112",
        token_out=token_bundle,
        amount_in=Decimal("3.0"),
        amount_out=Decimal("100000"),
        sender="BuyerCluster11111111111111111111",
        tx_hash="tx_cluster_trigger_11111111111111111111",
    )
    buffer.record_swap(sw_cluster_trigger, pool, sig_gen, current_time_s=t_0 + 95.0)

    assert staged_b.is_dev_bundled is True
    assert staged_b.dropped is True
    assert "DEV_BUNDLED" in staged_b.drop_reason
    assert buffer.is_staged(token_bundle) is False

    # --- Scenario B: Sybil Wash Trading (15 buys, but only 3 distinct signers < 8) ---
    token_sybil = "SybilMint111111111111111111111111111111111"
    staged_s = buffer.add_launch(
        token_address=token_sybil,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        initial_price=Decimal("0.000000028"),
        report=report,
        raw_signal=raw_sig,
        t_0=t_0,
    )

    # 15 buys distributed over 15 blocks, but repeating 3 senders
    for i in range(15):
        sender_id = f"SybilWash{i % 3:024d}"  # only 3 unique senders!
        sw = SwapEvent(
            timestamp_ns=int((t_0 + 95.0 + i) * 1e9),
            block_number=200 + i,
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_addr,
            token_in="So11111111111111111111111111111111111111112",
            token_out=token_sybil,
            amount_in=Decimal("1.5"),
            amount_out=Decimal("100000"),
            sender=sender_id,
            tx_hash=f"tx_sybil_{i:025d}",
        )
        sig = buffer.record_swap(sw, pool, sig_gen, current_time_s=t_0 + 95.0 + i)

    # Even though volume >= 15 SOL and buys >= 15, unique buyers = 3 (< 8)
    assert staged_s.buy_count == 15
    assert staged_s.total_volume_native >= Decimal("15.0")
    assert len(staged_s.unique_buyers) == 3
    assert staged_s.graduated is False

    # Verify SecurityGatekeeper.evaluate_bundle_heuristics directly on both staged sets
    bundle_ok, bundle_reason = SecurityGatekeeper.evaluate_bundle_heuristics(
        slot_buys=staged_b.slot_buy_counts,
        unique_buyers=staged_b.unique_buyers,
        threshold=0.60,
        min_unique=8,
    )
    assert bundle_ok is False
    assert "DEV_BUNDLED" in bundle_reason

    sybil_ok, sybil_reason = SecurityGatekeeper.evaluate_bundle_heuristics(
        slot_buys=staged_s.slot_buy_counts,
        unique_buyers=staged_s.unique_buyers,
        threshold=0.60,
        min_unique=8,
    )
    assert sybil_ok is False
    assert "SYBIL_SUSPECT" in sybil_reason


# ==============================================================================
# 4. CONFIGURATION PARAMETERS VERIFICATION
# ==============================================================================

def test_engine_config_new_parameters():
    """Verify that EngineConfig loads and parses the new parameters correctly."""
    cfg = EngineConfig(
        exit_grace_period_sec=20.0,
        dns_doh_servers="1.1.1.1,8.8.8.8,9.9.9.9",
        min_unique_buyers=10,
        slot_bundle_threshold=0.50,
        emergency_stop_pct=-0.30,
    )
    assert cfg.exit_grace_period_sec == 20.0
    assert cfg.dns_doh_servers == ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
    assert cfg.min_unique_buyers == 10
    assert cfg.slot_bundle_threshold == 0.50
    assert cfg.emergency_stop_pct == -0.30
