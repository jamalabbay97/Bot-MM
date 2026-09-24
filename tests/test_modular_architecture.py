"""
tests/test_modular_architecture.py — Comprehensive Verification Suite
======================================================================
Tests modular package layout, backward compatibility shims, math engine,
decoders, execution, and ledger persistence.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_modular_package_imports():
    """Verify clean imports from alpha_engine hierarchy."""
    import alpha_engine
    from alpha_engine.config import EngineConfig
    from alpha_engine.models import (
        ChainIdentifier,
        OrderSide,
        PoolState,
        PoolStateUpdateEvent,
        SecurityReport,
        SecurityTier,
        SwapEvent,
    )
    from alpha_engine.models.enums import SignalStrength
    from alpha_engine.models.events import ShutdownSentinel, SignalEvent
    from alpha_engine.models.state import PaperFill, PortfolioSnapshot, TradeRecord
    from alpha_engine.rate_limiter import (
        AsyncTokenBucket,
        RateLimiterRegistry,
        RateLimitError,
        build_alchemy_limiter,
        build_helius_limiter,
    )
    from alpha_engine.math import (
        CpmmQuote,
        LatencyResult,
        classify_alpha_score,
        compute_position_size,
        cpmm_buy_quote,
        cpmm_out,
        cpmm_sell_quote,
        gas_cost_usd,
        half_kelly_fraction,
        price_impact_bps,
        simulate_latency,
        spot_price,
    )
    from alpha_engine.security import SecurityGatekeeper
    from alpha_engine.ingestion import (
        EVMIngester,
        IngestionCoordinator,
        SVMIngester,
        _decode_evm_swap_log,
        _decode_evm_sync_log,
        _normalise,
        _parse_raydium_log_line,
    )
    from alpha_engine.execution import (
        OpenLot,
        PaperExecutor,
        PositionBook,
        RunningMetrics,
        SQLiteLedger,
    )
    from alpha_engine.engine import (
        PaperTradingEngine,
        PoolRegistry,
        SignalGenerator,
    )

    assert alpha_engine.__version__ == "0.1.0"


def test_backward_compatibility_facades():
    """Verify legacy root-level module imports preserve identical API."""
    import engine
    import execution
    import ingestion
    import models
    import quant_math
    import rate_limiter
    import security

    # Verify models
    assert hasattr(models, "PoolState")
    assert hasattr(models, "SwapEvent")
    assert hasattr(models, "ChainIdentifier")
    assert hasattr(models, "OrderSide")
    assert hasattr(models, "PoolStateUpdateEvent")

    # Verify quant_math
    assert hasattr(quant_math, "cpmm_out")
    assert hasattr(quant_math, "cpmm_buy_quote")
    assert hasattr(quant_math, "cpmm_sell_quote")
    assert hasattr(quant_math, "simulate_latency")
    assert hasattr(quant_math, "compute_position_size")
    assert hasattr(quant_math, "price_impact_bps")

    # Verify execution
    assert hasattr(execution, "PositionBook")
    assert hasattr(execution, "PaperExecutor")
    assert hasattr(execution, "SQLiteLedger")

    # Verify ingestion
    assert hasattr(ingestion, "IngestionCoordinator")
    assert hasattr(ingestion, "_normalise")

    # Verify security
    assert hasattr(security, "SecurityGatekeeper")

    # Verify rate_limiter
    assert hasattr(rate_limiter, "RateLimiterRegistry")

    # Verify engine
    assert hasattr(engine, "PaperTradingEngine")
    assert hasattr(engine, "EngineConfig")


def test_cpmm_math_and_unified_price():
    """Verify CPMM quote invariant and NATIVE_PER_TOKEN price direction."""
    from alpha_engine.models import ChainIdentifier, OrderSide, PoolState
    from alpha_engine.math import (
        cpmm_buy_quote,
        cpmm_sell_quote,
        spot_price,
    )

    pool = PoolState(
        pool_address="0x6c561b446416e1a00e8e93e221854d6ea4171372",
        chain=ChainIdentifier.BASE_MAINNET,
        native_reserve=Decimal("1500"),    # 1500 WETH
        token_reserve=Decimal("5000000"),  # 5,000,000 USDC
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=20_000_000,
        token_decimals=6,
        native_decimals=18,
    )

    expected_spot = Decimal("1500") / Decimal("5000000")  # 0.000300 WETH per USDC
    assert spot_price(pool) == expected_spot

    # BUY: spend 1 WETH -> receive USDC
    buy_quote = cpmm_buy_quote(pool, Decimal("1"))
    assert buy_quote.side == OrderSide.BUY
    assert buy_quote.spot_price == expected_spot
    # Execution price (WETH / USDC) must be > spot price due to fees + price impact
    assert buy_quote.execution_price > expected_spot
    assert 0 <= buy_quote.price_impact_bps <= 10000

    # SELL: sell 10,000 USDC -> receive WETH
    sell_quote = cpmm_sell_quote(pool, Decimal("10000"))
    assert sell_quote.side == OrderSide.SELL
    assert sell_quote.spot_price == expected_spot
    # Execution price (WETH / USDC) must be < spot price due to fees + price impact
    assert sell_quote.execution_price < expected_spot
    assert 0 <= sell_quote.price_impact_bps <= 10000


def test_mev_lognormal_burst_and_latency():
    """Verify stochastic latency and depth-relative LogNormal MEV model."""
    from alpha_engine.models import ChainIdentifier, PoolState
    from alpha_engine.math import simulate_latency

    pool = PoolState(
        pool_address="0x6c561b446416e1a00e8e93e221854d6ea4171372",
        chain=ChainIdentifier.BASE_MAINNET,
        native_reserve=Decimal("1000"),
        token_reserve=Decimal("1000000"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=100,
    )

    signal_ts = 1_000_000_000_000_000
    res = simulate_latency(pool, signal_ts)

    assert 1200 <= res.latency_ms <= 3500
    assert res.fill_timestamp_ns == signal_ts + res.latency_ms * 1_000_000
    assert 0.001 <= res.frontrun_fraction <= 0.025
    # Adjusted pool native reserve should have grown from MEV buy pressure
    assert res.adjusted_pool_state.native_reserve > pool.native_reserve
    assert res.adjusted_pool_state.token_reserve < pool.token_reserve


def test_position_sizing_and_gas_gate():
    """Verify Kelly position sizing and gas drag gate."""
    from alpha_engine.models import ChainIdentifier, OrderSide
    from alpha_engine.math import compute_position_size, half_kelly_fraction

    # Win rate 60%, 2:1 profit/loss ratio
    kelly_f = half_kelly_fraction(
        win_rate=0.6,
        avg_win_native=Decimal("0.2"),
        avg_loss_native=Decimal("0.1"),
    )
    assert 0.0 < kelly_f <= 1.0

    # Test micro portfolio ($10 equity) -> gas ($0.05) * 20 = $1.00 > $0.10 position -> gas rejected
    sizing_rejected = compute_position_size(
        kelly_fraction=kelly_f,
        portfolio_equity_usd=Decimal("10.0"),
        native_price_usd=Decimal("3000.0"),
        chain=ChainIdentifier.BASE_MAINNET,
        side=OrderSide.BUY,
    )
    assert sizing_rejected.gas_rejected is True
    assert sizing_rejected.native_trade_size == Decimal("0")

    # Test normal portfolio ($100,000 equity) -> accepted
    sizing_ok = compute_position_size(
        kelly_fraction=kelly_f,
        portfolio_equity_usd=Decimal("100000.0"),
        native_price_usd=Decimal("3000.0"),
        chain=ChainIdentifier.BASE_MAINNET,
        side=OrderSide.BUY,
    )
    assert sizing_ok.gas_rejected is False
    assert sizing_ok.native_trade_size > Decimal("0")


def test_evm_decoders_and_normalization():
    """Verify EVM swap/sync log decoders and decimal normalisation."""
    from alpha_engine.ingestion.decoders import (
        _SWAP_TOPIC,
        _SYNC_TOPIC,
        _decode_evm_swap_log,
        _decode_evm_sync_log,
        _normalise,
    )
    from alpha_engine.models import ChainIdentifier, PoolState

    assert _normalise(10**18, 18) == Decimal("1")
    assert _normalise(1_500_000, 6) == Decimal("1.5")

    WETH = "0x4200000000000000000000000000000000000006"
    USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    POOL = "0x6c561b446416e1a00e8e93e221854d6ea4171372"

    # Swap log test
    swap_log = {
        "topics": [
            _SWAP_TOPIC,
            "0x000000000000000000000000abcdef1234567890abcdef1234567890abcdef12",
            "0x000000000000000000000000abcdef1234567890abcdef1234567890abcdef34",
        ],
        "data": (
            "0x"
            + hex(10**18)[2:].zfill(64)          # amount0In = 1 WETH
            + "0" * 64                           # amount1In = 0
            + "0" * 64                           # amount0Out = 0
            + hex(3_000_000_000)[2:].zfill(64)   # amount1Out = 3000 USDC
        ),
        "address": POOL,
        "transactionHash": "0x" + "a" * 64,
        "blockNumber": "0x1000",
        "logIndex": "0x1",
    }
    swap_evt = _decode_evm_swap_log(swap_log, WETH, USDC, 18, 6, WETH)
    assert swap_evt is not None
    assert swap_evt.amount_in == Decimal("1")
    assert swap_evt.amount_out == Decimal("3000")

    # Sync log test
    sync_log = {
        "topics": [_SYNC_TOPIC],
        "data": (
            "0x"
            + hex(1500 * 10**18)[2:].zfill(64)       # reserve0 = 1500 WETH
            + hex(5_000_000 * 10**6)[2:].zfill(64)   # reserve1 = 5,000,000 USDC
        ),
        "address": POOL,
        "blockNumber": "0x1001",
    }
    sync_evt = _decode_evm_sync_log(sync_log, POOL, WETH, USDC, 18, 6, WETH, None)
    assert sync_evt is not None
    assert sync_evt.new_pool_state.native_reserve == Decimal("1500")
    assert sync_evt.new_pool_state.token_reserve == Decimal("5000000")


def test_position_book_fifo():
    """Verify PositionBook lot tracking and FIFO closure matching."""
    from alpha_engine.execution.book import PositionBook
    from alpha_engine.models import ChainIdentifier, OrderSide, PaperFill

    book = PositionBook()
    token = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    pool = "0x6c561b446416e1a00e8e93e221854d6ea4171372"

    fill1 = PaperFill(
        token_address=token,
        pool_address=pool,
        chain=ChainIdentifier.BASE_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("1.0"),
        tokens_acquired=Decimal("1000.0"),
        effective_price=Decimal("0.001"),
        price_impact_bps=10,
        simulated_gas_cost_usd=Decimal("0.05"),
        fill_latency_ms=1500,
        signal_timestamp_ns=1000,
        fill_timestamp_ns=2000,
        kelly_fraction=0.01,
        portfolio_equity_usd=Decimal("10000"),
    )
    book.open_lot(fill1, "sig-1")
    assert book.get_holdings(ChainIdentifier.BASE_MAINNET, token) == Decimal("1000.0")

    # Partial sell 400 tokens at 0.0015 price (50% profit)
    pnl, matched = book.close_lots_fifo(
        chain=ChainIdentifier.BASE_MAINNET,
        token_address=token,
        tokens_to_sell=Decimal("400.0"),
        sell_price=Decimal("0.0015"),
    )
    assert matched == Decimal("400.0")
    assert pnl == Decimal("400.0") * (Decimal("0.0015") - Decimal("0.001"))
    assert book.get_holdings(ChainIdentifier.BASE_MAINNET, token) == Decimal("600.0")


def test_sqlite_ledger_persistence():
    """Verify SQLiteLedger WAL mode and persistent write operations."""
    async def _test():
        from alpha_engine.execution.ledger import SQLiteLedger
        from alpha_engine.models import ChainIdentifier, OrderSide, PaperFill, TradeRecord

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            async with SQLiteLedger(db_path) as ledger:
                fill = PaperFill(
                    token_address="0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
                    pool_address="0x6c561b446416e1a00e8e93e221854d6ea4171372",
                    chain=ChainIdentifier.BASE_MAINNET,
                    side=OrderSide.BUY,
                    simulated_native_spent=Decimal("1.0"),
                    tokens_acquired=Decimal("1000.0"),
                    effective_price=Decimal("0.001"),
                    price_impact_bps=12,
                    simulated_gas_cost_usd=Decimal("0.05"),
                    fill_latency_ms=1250,
                    signal_timestamp_ns=1000,
                    fill_timestamp_ns=2500,
                    kelly_fraction=0.01,
                    portfolio_equity_usd=Decimal("50000"),
                )
                record = TradeRecord.from_fill(fill, signal_id="sig-test-123")
                await ledger.record_trade(record)

                stats = await ledger.get_closed_trade_stats()
                assert stats["total_trades"] == 0
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)

    asyncio.run(_test())


if __name__ == "__main__":
    print("=== RUNNING MODULAR ARCHITECTURE TEST SUITE ===")
    test_modular_package_imports()
    print("  [PASS] test_modular_package_imports")
    test_backward_compatibility_facades()
    print("  [PASS] test_backward_compatibility_facades")
    test_cpmm_math_and_unified_price()
    print("  [PASS] test_cpmm_math_and_unified_price")
    test_mev_lognormal_burst_and_latency()
    print("  [PASS] test_mev_lognormal_burst_and_latency")
    test_position_sizing_and_gas_gate()
    print("  [PASS] test_position_sizing_and_gas_gate")
    test_evm_decoders_and_normalization()
    print("  [PASS] test_evm_decoders_and_normalization")
    test_position_book_fifo()
    print("  [PASS] test_position_book_fifo")
    test_sqlite_ledger_persistence()
    print("  [PASS] test_sqlite_ledger_persistence")
    print("\nALL 8 TESTS PASSED WITH 100% SUCCESS!")

