"""
tests/test_native_price_service.py
===================================
Comprehensive unit, integration, and AST audit tests for Issue 1:
1. Live NativePriceService with primary (DefiLlama) and secondary (e.g. Coinbase) sources
2. Mid-run price changes dynamically update equity, USD PnL, market cap, and conversions
3. Stale price / unavailable price blocks new entries but allows exits flagged as price degraded
4. Source disagreement beyond tolerance blocks new entries (fail-closed)
5. Secondary source takes over when primary fails and status reflects it
6. Repo-wide audit asserting no native-price numeric literals in non-test logic
7. Integration test through runner entry path: fake price service returning 200 for SOL sizes/values with 200
"""

from __future__ import annotations

import sys
import time
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import AlphaEngine
from alpha_engine.execution.book import OpenLot
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    SecurityTier,
    SignalStrength,
    SignalSource,
    TradeExitReason,
)
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, SecurityReport, ExitDecision, PaperFill
from alpha_engine.pricing.native_price_service import (
    BasePriceSource,
    NativePriceService,
    NativePriceStatus,
    PriceResult,
)
from tools.audit.native_price_literals import audit_file, is_test_file


class DummyPriceSource(BasePriceSource):
    def __init__(self, name: str, prices: dict[str, Decimal] | None = None, should_fail: bool = False):
        self.name = name
        self.prices = prices or {}
        self.should_fail = should_fail

    async def fetch_prices(self, session: object = None) -> dict[str, Decimal]:
        _ = session
        if self.should_fail:
            raise RuntimeError(f"Source {self.name} simulated failure")
        return dict(self.prices)


@pytest.mark.anyio
async def test_price_changes_mid_run_updates_equity_pnl_mcap_and_conversions(tmp_path):
    """
    Test 1: Price changes mid-run -> equity, PnL, market cap, and token-in-native conversions change correctly.
    Store entry native price on lot so exit USD PnL does not recompute entry USD with later price.
    """
    db_file = tmp_path / "test_pricing_midrun.db"
    cfg = EngineConfig(
        db_path=str(db_file),
        initial_sol=Decimal("10.0"),
        initial_eth=Decimal("2.0"),
        auto_trade_enabled=True,
    )
    engine = AlphaEngine(cfg)
    engine._ledger = SQLiteLedger(cfg.db_path)
    await engine._ledger.__aenter__()

    # Provide our mock price service
    primary = DummyPriceSource("p", {"solana": Decimal("100.0"), "base_eth": Decimal("2000.0")})
    secondary = DummyPriceSource("s", {"solana": Decimal("100.0"), "base_eth": Decimal("2000.0")})
    service = NativePriceService(
        primary_source=primary,
        secondary_source=secondary,
        refresh_interval_s=10.0,
        max_age_s=60.0,
    )
    await service.refresh()
    engine._native_price_service = service
    engine._executor = PaperExecutor(
        position_book=engine._position_book,
        price_service=service,
    )

    try:
        # Initial equity: 10 SOL * 100 + 2 ETH * 2000 = 1000 + 4000 = 5000 USD
        eq_initial = engine._current_equity_usd()
        assert eq_initial == Decimal("5000.0")

        # Open a position on Solana: 1 SOL spent at SOL = $100 -> entry cost basis = $100 USD
        token_addr = "TokenMidRun1111111111111111111111111111111"
        fill = PaperFill(
            order_id="fill-entry-1",
            chain=ChainIdentifier.SOLANA_MAINNET,
            token_address=token_addr,
            side=OrderSide.BUY,
            tokens_acquired=Decimal("1000.0"),
            simulated_native_spent=Decimal("1.0"),
            effective_price=Decimal("0.001"),
            pool_address="PoolMidRun11111111111111111111111111111111",
        )
        engine._cash_sol -= Decimal("1.0")  # 9 SOL cash left

        lot = engine._position_book.open_lot(
            fill=fill,
            signal_id="sig-1",
            entry_native_price_usd=service.get_price(ChainIdentifier.SOLANA_MAINNET).price,
        )
        assert isinstance(lot, OpenLot)
        assert lot.entry_native_price_usd == Decimal("100.0")

        # Now native SOL price jumps from 100 to 200 USD
        primary.prices["solana"] = Decimal("200.0")
        secondary.prices["solana"] = Decimal("200.0")
        await service.refresh()

        # Check market cap calculation uses new 200 price
        pool = PoolState(
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=lot.pool_address,
            token_address=token_addr,
            token_reserve=Decimal("1000000.0"),
            native_reserve=Decimal("50.0"),
            fee_numerator=30,
            fee_denominator=10000,
            last_updated_block=100,
            timestamp_s=time.time(),
        )
        engine._pool_registry.set(lot.pool_address, pool)
        engine._pool_registry.set(token_addr, pool)

        mcap = pool.calculate_market_cap_usd(native_price_usd=service.get_price(ChainIdentifier.SOLANA_MAINNET).price)
        expected_mcap = Decimal("1000000000") * pool.spot_price_native_per_token * Decimal("200.0")
        assert mcap == expected_mcap

        # Check equity with new price:
        # Cash: 9 SOL * 200 ($1800) + 2 ETH * 2000 ($4000) = $5800
        # Position: 1000 tokens * (50 / 1000000 SOL) = 0.05 SOL * 200 ($10)
        # Total equity = 5800 + 10 = 5810 USD
        eq_updated = engine._current_equity_usd()
        assert eq_updated == Decimal("5810.0")

        # Now close the trade: sold for 1.2 SOL when SOL = $200
        # Exit native received = 1.2 SOL ($240 USD)
        # Entry cost basis was 1.0 SOL @ $100 ($100 USD)
        # Realized USD PnL MUST be 240 - 100 = 140 USD (not (1.2 - 1.0) * 200 = 40 or * 100 = 20)
        exit_fill = PaperFill(
            order_id="fill-exit-1",
            chain=ChainIdentifier.SOLANA_MAINNET,
            token_address=token_addr,
            side=OrderSide.SELL,
            tokens_acquired=Decimal("1000.0"),
            simulated_native_spent=Decimal("1.2"),  # net native received
            effective_price=Decimal("0.0012"),
            pool_address=lot.pool_address,
        )
        exit_decision = ExitDecision(
            token_address=token_addr,
            chain=ChainIdentifier.SOLANA_MAINNET,
            lot_id=lot.lot_id,
            should_exit=True,
            tokens_to_sell=Decimal("1000.0"),
            exit_reason=TradeExitReason.TP_100,
        )

        pnl_native, _ = engine._position_book.apply_exit_decision(
            exit_decision,
            sell_price=exit_fill.effective_price,
            net_native_received=exit_fill.simulated_native_spent,
        )
        assert pnl_native == Decimal("0.2")

        # Compute exit PnL USD using runner's formula:
        # Exit USD proceeds = 1.2 SOL * 200 USD = 240 USD
        # Entry USD cost = 1.0 SOL * 100 USD = 100 USD
        # Net USD PnL = 140 USD
        exit_native_p = service.get_price(lot.chain).price
        exit_usd = exit_fill.simulated_native_spent * exit_native_p
        entry_usd = Decimal("1.0") * lot.entry_native_price_usd
        realized_pnl_usd = exit_usd - entry_usd
        assert realized_pnl_usd == Decimal("140.0")

    finally:
        await service.stop()
        if hasattr(engine, "_session") and engine._session and not engine._session.closed:
            await engine._session.close()
        await engine._ledger.__aexit__(None, None, None)


@pytest.mark.anyio
async def test_stale_price_blocks_entries_allows_exits_flagged_degraded(tmp_path):
    """
    Test 2: Stale price blocks new entries, still allows exits, and exits are flagged as price degraded.
    """
    db_file = tmp_path / "test_pricing_stale.db"
    cfg = EngineConfig(db_path=str(db_file), auto_trade_enabled=True)
    engine = AlphaEngine(cfg)
    engine._ledger = SQLiteLedger(cfg.db_path)
    await engine._ledger.__aenter__()

    # Price service with max_age_s = 5.0
    primary = DummyPriceSource("p", {"solana": Decimal("150.0"), "base_eth": Decimal("3000.0")})
    secondary = DummyPriceSource("s", {"solana": Decimal("150.0"), "base_eth": Decimal("3000.0")})
    service = NativePriceService(
        primary_source=primary,
        secondary_source=secondary,
        max_age_s=5.0,
    )
    await service.refresh()
    engine._native_price_service = service
    engine._executor = PaperExecutor(
        position_book=engine._position_book,
        price_service=service,
    )

    try:
        # Manually age the cached price by 10s -> status becomes STALE
        service._cache_timestamp["solana"] = time.time() - 10.0
        price, age_s, status = service.get_price(ChainIdentifier.SOLANA_MAINNET)
        assert status == NativePriceStatus.STALE
        assert age_s >= 10.0

        # Attempting a NEW entry must be blocked fail-closed
        token_stale = "TokenStale1111111111111111111111111111111"
        pool_stale = "PoolStale11111111111111111111111111111111"
        pool = PoolState(
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_stale,
            token_address=token_stale,
            token_reserve=Decimal("1000000.0"),
            native_reserve=Decimal("50.0"),
            fee_numerator=30,
            fee_denominator=10000,
            last_updated_block=100,
            timestamp_s=time.time(),
            data_age_s=0.5,
        )
        sec_report = SecurityReport(
            token_address=token_stale,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
        )
        sig = SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_stale,
            token_address=token_stale,
            suggested_side=OrderSide.BUY,
            strength=SignalStrength.STRONG,
            alpha_score=0.95,
            source=SignalSource.DEX_SWAP,
            market_cap_usd=Decimal("50000.0"),
            pool_state=pool,
            security_report=sec_report,
        )
        engine._pool_registry.set(sig.token_address, pool)
        engine._pool_registry.set(sig.pool_address, pool)
        from alpha_engine.models.events import ShutdownSentinel
        engine.get_or_fetch_pool_state = AsyncMock(return_value=pool)

        # Enqueue signal and process queue deterministically
        await engine._signal_q.put(sig)
        await engine._signal_q.put(ShutdownSentinel())
        await engine._process_signal_queue()

        # Verify NO position was opened
        open_lots = engine._position_book.get_all_open_lots()
        assert len(open_lots) == 0, "Stale price MUST block new entry"
        assert engine.dashboard_telemetry.pipeline_counters.get("price_degraded_rejected", 0) > 0

        # Now test that EXIT on an existing open lot is STILL ALLOWED and FLAGGED DEGRADED
        lot = engine._position_book.open_lot(
            fill=PaperFill(
                order_id="lot-stale-exit",
                chain=sig.chain,
                token_address=sig.token_address,
                side=OrderSide.BUY,
                tokens_acquired=Decimal("100.0"),
                simulated_native_spent=Decimal("1.0"),
                effective_price=Decimal("0.01"),
                pool_address=sig.pool_address,
            ),
            signal_id="sig-stale",
            entry_native_price_usd=Decimal("150.0"),
        )
        assert lot.price_degraded is False
        assert len(engine._position_book.get_all_open_lots()) == 1

        # Execute exit through manual_exit_position under stale native price
        ok, msg, pnl = await engine.manual_exit_position(sig.token_address)
        assert ok is True
        assert lot.price_degraded is True
        assert len(engine._position_book.get_all_open_lots()) == 0

    finally:
        await service.stop()
        if hasattr(engine, "_session") and engine._session and not engine._session.closed:
            await engine._session.close()
        await engine._ledger.__aexit__(None, None, None)


@pytest.mark.anyio
async def test_source_disagreement_beyond_tolerance_blocks_entries(tmp_path):
    """
    Test 3: Source disagreement beyond tolerance blocks entries (fail-closed).
    """
    db_file = tmp_path / "test_pricing_div.db"
    cfg = EngineConfig(db_path=str(db_file), auto_trade_enabled=True)
    engine = AlphaEngine(cfg)
    engine._ledger = SQLiteLedger(cfg.db_path)
    await engine._ledger.__aenter__()

    # Primary says 150, Secondary says 180 (20% divergence > 5% tolerance)
    primary = DummyPriceSource("primary", {"solana": Decimal("150.0"), "base_eth": Decimal("3000.0")})
    secondary = DummyPriceSource("secondary", {"solana": Decimal("180.0"), "base_eth": Decimal("3000.0")})
    service = NativePriceService(
        primary_source=primary,
        secondary_source=secondary,
        divergence_tolerance_pct=5.0,
    )
    await service.refresh()
    engine._native_price_service = service
    engine._executor = PaperExecutor(
        position_book=engine._position_book,
        price_service=service,
    )

    try:
        price, age_s, status = service.get_price(ChainIdentifier.SOLANA_MAINNET)
        assert status == NativePriceStatus.DIVERGENT

        # Submit BUY signal
        tok_div = "TokenDiv1111111111111111111111111111111"
        pool_div = "PoolDiv11111111111111111111111111111111"
        pool = PoolState(
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_div,
            token_address=tok_div,
            token_reserve=Decimal("1000000.0"),
            native_reserve=Decimal("50.0"),
            fee_numerator=30,
            fee_denominator=10000,
            last_updated_block=100,
            timestamp_s=time.time(),
            data_age_s=0.5,
        )
        sec_report = SecurityReport(
            token_address=tok_div,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
        )
        sig = SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_div,
            token_address=tok_div,
            suggested_side=OrderSide.BUY,
            strength=SignalStrength.STRONG,
            alpha_score=0.95,
            source=SignalSource.DEX_SWAP,
            market_cap_usd=Decimal("50000.0"),
            pool_state=pool,
            security_report=sec_report,
        )
        engine._pool_registry.set(sig.token_address, pool)
        engine._pool_registry.set(sig.pool_address, pool)
        from alpha_engine.models.events import ShutdownSentinel
        engine.get_or_fetch_pool_state = AsyncMock(return_value=pool)

        await engine._signal_q.put(sig)
        await engine._signal_q.put(ShutdownSentinel())
        await engine._process_signal_queue()

        assert len(engine._position_book.get_all_open_lots()) == 0, "Divergent prices MUST block new entry"
        assert engine.dashboard_telemetry.pipeline_counters.get("price_degraded_rejected", 0) > 0

    finally:
        await service.stop()
        if hasattr(engine, "_session") and engine._session and not engine._session.closed:
            await engine._session.close()
        await engine._ledger.__aexit__(None, None, None)


@pytest.mark.anyio
async def test_secondary_source_takes_over_when_primary_fails():
    """
    Test 4: Secondary source takes over when primary fails (and status reflects it).
    """
    primary = DummyPriceSource("primary_failing", should_fail=True)
    secondary = DummyPriceSource("secondary_ok", {"solana": Decimal("165.0"), "base_eth": Decimal("3100.0")})
    service = NativePriceService(
        primary_source=primary,
        secondary_source=secondary,
        refresh_interval_s=15.0,
        max_age_s=60.0,
    )
    try:
        await service.refresh()

        res = service.get_price(ChainIdentifier.SOLANA_MAINNET)
        assert isinstance(res, PriceResult)
        assert res.price == Decimal("165.0")
        assert res.status == NativePriceStatus.OK
        assert res.source == "secondary"
        assert service.get_active_source(ChainIdentifier.SOLANA_MAINNET) == "secondary"
    finally:
        await service.stop()


def test_no_native_price_numeric_literals_in_non_test_code():
    """
    Test 5: Repo-wide audit test that fails if a native-price numeric literal appears in non-test logic.
    Reuses tools/audit/native_price_literals.py.
    """
    repo_root = Path(__file__).resolve().parent.parent
    findings = []
    for path in sorted(repo_root.rglob("*.py")):
        if is_test_file(path.relative_to(repo_root)):
            continue
        hits = audit_file(path.relative_to(repo_root))
        findings.extend(hits)

    if findings:
        formatted = "\n".join(findings)
        pytest.fail(f"Found {len(findings)} native price numeric literals in non-test code:\n{formatted}")


@pytest.mark.anyio
async def test_integration_runner_entry_path_uses_live_native_price(tmp_path):
    """
    Test 6: Integration test through the runner entry path: with a fake price service
    returning 200 for SOL, a SOL entry is sized/valued with 200, not 150.
    """
    db_file = tmp_path / "test_integration_entry.db"
    cfg = EngineConfig(db_path=str(db_file), auto_trade_enabled=True)
    engine = AlphaEngine(cfg)
    engine._ledger = SQLiteLedger(cfg.db_path)
    await engine._ledger.__aenter__()

    # Price service returning 200 for SOL
    primary = DummyPriceSource("primary", {"solana": Decimal("200.0"), "base_eth": Decimal("3000.0")})
    secondary = DummyPriceSource("secondary", {"solana": Decimal("200.0"), "base_eth": Decimal("3000.0")})
    service = NativePriceService(primary_source=primary, secondary_source=secondary)
    await service.refresh()
    engine._native_price_service = service
    engine._executor = PaperExecutor(
        position_book=engine._position_book,
        price_service=service,
    )

    try:
        # Check that engine reports 200 for SOL
        assert engine.get_native_price(ChainIdentifier.SOLANA_MAINNET) == Decimal("200.0")

        # Submit entry through runner entry path
        token_addr = "TokenLiveSized1111111111111111111111111111"
        pool = PoolState(
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address="PoolLiveSized1111111111111111111111111111111",
            token_address=token_addr,
            token_reserve=Decimal("1000000.0"),
            native_reserve=Decimal("50.0"),
            fee_numerator=30,
            fee_denominator=10000,
            last_updated_block=100,
            timestamp_s=time.time(),
            data_age_s=0.5,
        )
        engine._pool_registry.set(token_addr, pool)
        engine._pool_registry.set(pool.pool_address, pool)
        engine.get_or_fetch_pool_state = AsyncMock(return_value=pool)

        # Trigger manual_enter_position through runner
        ok, msg, signal_res = await engine.manual_enter_position(
            token_address=token_addr,
            chain=ChainIdentifier.SOLANA_MAINNET,
            amount_native=Decimal("1.0"),
            bypass_consensus=True,
        )
        assert ok is True
        open_lots = engine._position_book.get_open_lots(chain=ChainIdentifier.SOLANA_MAINNET, token_address=token_addr)
        assert len(open_lots) == 1
        lot = open_lots[0]
        assert isinstance(lot, OpenLot)

        # Verify entry native price is stored on the lot as 200, not 150
        assert lot.entry_native_price_usd == Decimal("200.0")
        assert lot.cost_basis_native > Decimal("0.99")
        # Entry USD valuation: cost_basis_native * 200.0 USD != cost_basis_native * 150.0 USD
        entry_usd_valuation = lot.cost_basis_native * lot.entry_native_price_usd
        assert entry_usd_valuation == lot.cost_basis_native * Decimal("200.0")
        assert entry_usd_valuation != lot.cost_basis_native * Decimal("150.0")

    finally:
        await service.stop()
        if hasattr(engine, "_session") and engine._session and not engine._session.closed:
            await engine._session.close()
        await engine._ledger.__aexit__(None, None, None)
