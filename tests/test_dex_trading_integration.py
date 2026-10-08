"""
tests/test_dex_trading_integration.py
======================================
Integration tests verifying DEX trading functionality across Solana (Raydium)
and Base (Uniswap/Aerodrome):
1. Raydium pool creation and swap transaction decoding.
2. Short-term scalp & buy signal generation with DEX execution venues.
3. Order execution and lot management ensuring platform="DexScan" and bonding_curve_mode=False.
4. Dynamic exit execution and TradeRecord ledger persistence preserving platform="DexScan".
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.engine.signals import SignalGenerator
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.ingestion.decoders import (
    RAYDIUM_AMM_PROGRAM_ID,
    _parse_raydium_initialize2_logs,
)
from alpha_engine.ingestion.dex_metrics import DEXMetricsAggregator
from alpha_engine.ingestion.svm import SVMIngester
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    ExitProfile,
    OrderSide,
    SignalSource,
    SignalStrength,
    TradeExitReason,
    resolve_trade_platform,
)
from alpha_engine.models.events import (
    SignalEvent,
    SwapEvent,
)
from alpha_engine.models.state import PoolState, SecurityReport, SecurityTier, TradeRecord


@pytest.mark.anyio
async def test_raydium_pool_creation_and_swap_ingestion():
    """Verify Raydium initialize2 pool creation decodes both pool and token, and swap parsing succeeds."""
    mock_pool = "6UmmUiYo5ZbzK8kQ8FkL6Z5uN6fS7Q3eT5bY9cV3mX2a"
    mock_token = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
    wsol = "So11111111111111111111111111111111111111112"
    init_logs = [
        f"Program {RAYDIUM_AMM_PROGRAM_ID} invoke [1]",
        "Program log: initialize2: Initialize instruction",
        "Program log: init_pc_amount: 1000000000, init_coin_amount: 500000000000",
        f"Program log: pool: {mock_pool}, token: {mock_token}",
        f"Program {RAYDIUM_AMM_PROGRAM_ID} success",
    ]

    result = _parse_raydium_initialize2_logs(init_logs, tx_sig="sig_raydium_init_12345")
    assert result is not None
    assert result.pool_address == mock_pool
    assert result.token_address == mock_token

    # 2. SVMIngester registers Raydium pool and parses swaps
    queue = asyncio.Queue()
    ingester = SVMIngester(
        ws_url="wss://mock.helius.xyz",
        pool_registry={},
        event_queue=queue,
        limiter=RateLimiterRegistry.default(),
    )

    # Register mock pool in pool_registry
    ingester._pool_registry[mock_pool] = (mock_token, wsol, 9, 9)

    # 3. Subsequent Raydium Swap parsing with valid packed binary log
    import base64
    import struct

    raw = bytearray(64)
    raw[0] = 3  # log_type = 3 (Swap)
    struct.pack_into("<Q", raw, 1, 1_000_000_000)  # 1.0 WSOL in (amount_in)
    struct.pack_into("<Q", raw, 17, 0)             # direction = 0 (buy token)
    struct.pack_into("<Q", raw, 33, 500_000_000_000) # pool coin reserve
    struct.pack_into("<Q", raw, 41, 1_000_000_000_000) # pool pc reserve
    struct.pack_into("<Q", raw, 49, 200_000_000)   # 0.2 token out (amount_out)

    b64_line = f"Program log: ray_log: {base64.b64encode(raw).decode()}"
    swap_logs = [
        f"Program {RAYDIUM_AMM_PROGRAM_ID} invoke [1]",
        b64_line,
        f"Program {RAYDIUM_AMM_PROGRAM_ID} success",
    ]

    swap_event = ingester._parse_raydium_transaction(
        tx_signature="sig_raydium_swap_67890",
        logs=swap_logs,
        pool_pubkey=mock_pool,
    )
    assert swap_event is not None
    assert swap_event.pool_address == mock_pool
    assert swap_event.token_in == wsol
    assert swap_event.token_out == mock_token
    assert swap_event.amount_in == Decimal("1.0")
    assert swap_event.amount_out == Decimal("0.2")


@pytest.mark.anyio
async def test_dex_scalp_signal_and_conversion():
    """Verify SignalGenerator generates short-term scalp DecisionSignal on DEX and converts to SignalEvent."""
    sig_gen = SignalGenerator()
    metrics_agg = DEXMetricsAggregator()

    pool_addr = "6UmmUiYo5ZbzK8kQ8FkL6Z5uN6fS7Q3eT5bY9cV3mX2a"
    token_addr = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
    wsol = "So11111111111111111111111111111111111111112"

    pool = PoolState(
        pool_address=pool_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("100000"),
        native_reserve=Decimal("500"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=250000000,
        token_decimals=5,
        native_decimals=9,
    )
    metrics_agg.record_pool_state(pool)

    # Simulate organic buy volume spike
    now_ns = time.time_ns()
    for i in range(5):
        sw = SwapEvent(
            timestamp_ns=now_ns + i * 1_000_000,
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_addr,
            token_in=wsol,
            token_out=token_addr,
            amount_in=Decimal("5.0"),
            amount_out=Decimal("1000"),
            sender=f"BuyerWallet{i}1111111111111111111111111111111",
            tx_hash=f"tx_spike_{i}111111111111111111111111111111111",
            block_number=250000000 + i,
        )
        metrics_agg.record_swap(sw, pool_state=pool)

    trigger_swap = SwapEvent(
        timestamp_ns=now_ns + 10_000_000,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        token_in=wsol,
        token_out=token_addr,
        amount_in=Decimal("6.0"),
        amount_out=Decimal("1200"),
        sender="TriggerBuyer1111111111111111111111111111111",
        tx_hash="tx_trigger_scalp_11111111111111111111111111",
        block_number=250000005,
    )

    report = SecurityReport(
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        mint_authority_disabled=True,
        top10_concentration=0.15,
    )

    # Generate scalp decision signal (baseline 1h volume set low to trigger spike)
    decision = sig_gen.generate_short_term_scalp_signal(
        swap=trigger_swap,
        pool=pool,
        report=report,
        metrics_aggregator=metrics_agg,
        baseline_1h_volume=Decimal("2.0"),
    )
    assert decision is not None
    assert decision.execution_venue == ExecutionVenue.RAYDIUM_AMM
    assert decision.token_address == token_addr
    assert decision.suggested_side == OrderSide.BUY

    # Convert to SignalEvent
    sig_event = decision.to_signal_event(pool_state=pool, security_report=report)
    assert isinstance(sig_event, SignalEvent)
    assert sig_event.execution_venue == ExecutionVenue.RAYDIUM_AMM
    assert sig_event.source == SignalSource.DEX_SWAP
    assert sig_event.exit_profile == ExitProfile.FAST_SNIPE


@pytest.mark.anyio
async def test_dex_lot_opening_and_exit_platform_preservation():
    """Verify PaperExecutor and PositionBook preserve platform='DexScan' and bonding_curve_mode=False for DEX trades."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_dex_platform.db"
        async with SQLiteLedger(db_path=db_path) as ledger:
            book = PositionBook()
            executor = PaperExecutor(
                position_book=book,
                native_price_usd=Decimal("150.0"),
            )

            pool_addr = "6UmmUiYo5ZbzK8kQ8FkL6Z5uN6fS7Q3eT5bY9cV3mX2a"
            token_addr = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"

            pool = PoolState(
                pool_address=pool_addr,
                chain=ChainIdentifier.SOLANA_MAINNET,
                token_reserve=Decimal("1000000000"),
                native_reserve=Decimal("500"),
                fee_numerator=25,
                fee_denominator=10000,
                last_updated_block=250000000,
                token_decimals=5,
                native_decimals=9,
            )
            report = SecurityReport(
                token_address=token_addr,
                chain=ChainIdentifier.SOLANA_MAINNET,
                tier=SecurityTier.CLEAN,
                is_honeypot=False,
                buy_tax_bps=0,
                sell_tax_bps=0,
                lp_burned_ratio=1.0,
                mint_authority_disabled=True,
                top10_concentration=0.15,
            )

            # Signal on Raydium AMM
            signal = SignalEvent(
                timestamp_ns=time.time_ns(),
                chain=ChainIdentifier.SOLANA_MAINNET,
                token_address=token_addr,
                pool_address=pool_addr,
                suggested_side=OrderSide.BUY,
                strength=SignalStrength.STRONG,
                alpha_score=0.85,
                source=SignalSource.DEX_SWAP,
                execution_venue=ExecutionVenue.RAYDIUM_AMM,
                pool_state=pool,
                security_report=report,
            )

            fill = await executor.execute_signal(signal, portfolio_equity_usd=Decimal("10000"))
            assert fill is not None
            assert fill.platform == "DexScan"

            platform_name = getattr(fill, "platform", None) or resolve_trade_platform(
                token_address=fill.token_address,
                chain=fill.chain,
                source=signal.source,
                execution_venue=signal.execution_venue,
                pool_address=signal.pool_address,
            )
            assert platform_name == "DexScan"

            # Open lot with dynamic bonding_curve_mode
            is_bonding_curve = (platform_name == "Pump.fun")
            assert is_bonding_curve is False

            lot = book.open_lot(
                fill=fill,
                signal_id=signal.signal_id,
                initial_pool_reserve_native=pool.native_reserve,
                pool_address=pool_addr,
                bonding_curve_mode=is_bonding_curve,
                platform=platform_name,
            )
            assert lot.platform == "DexScan"
            assert lot.bonding_curve_mode is False

            # Verify resolve_trade_platform on this lot resolves to DexScan
            res_plat = resolve_trade_platform(
                token_address=lot.token_address,
                chain=lot.chain,
                bonding_curve_mode=lot.bonding_curve_mode,
                platform=lot.platform,
            )
            assert res_plat == "DexScan"

            # Execute exit passing platform
            exit_fill = await executor.execute_exit(
                chain=lot.chain,
                token_address=lot.token_address,
                pool=pool,
                tokens_to_sell=lot.tokens_held,
                reason=TradeExitReason.TP_25,
                apply_drag=True,
                portfolio_equity_usd=Decimal("10000"),
                platform=getattr(lot, "platform", None),
            )
            assert exit_fill is not None
            assert exit_fill.platform == "DexScan"

            # Record TradeRecord in ledger
            rec = TradeRecord.from_fill(
                fill=exit_fill,
                signal_id=lot.signal_id,
                realized_pnl_usd=Decimal("15.50"),
                platform=getattr(lot, "platform", None) or exit_fill.platform,
            )
            assert rec.platform == "DexScan"
            await ledger.record_trade(rec)

            stored_trades = await ledger.get_recent_trades(limit=5)
            assert len(stored_trades) == 1
            assert stored_trades[0]["platform"] == "DexScan"


@pytest.mark.anyio
async def test_evm_base_dex_trading_flow():
    """Verify Base EVM DEX swaps on unstaged tokens generate Uniswap V2 DEX signals and execute with platform='DexScan'."""
    cfg = EngineConfig()
    engine = PaperTradingEngine(cfg)

    brett_token = "0x532f27101965dd16442e59d40670faf5ebb142e4"
    weth_token = "0x4200000000000000000000000000000000000006"
    aero_pool = "0x94cc044155b9e1d88258525b6a37887e2261543b"

    pool_state = PoolState(
        pool_address=aero_pool,
        chain=ChainIdentifier.BASE_MAINNET,
        token_reserve=Decimal("100_000"),
        native_reserve=Decimal("5"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=20000000,
        token_decimals=18,
        native_decimals=18,
    )
    engine._pool_registry.set(aero_pool, pool_state)

    # Mock gatekeeper to approve BRETT
    mock_gk = MagicMock()
    mock_gk.is_rejected.return_value = False
    clean_rep = SecurityReport(
        token_address=brett_token,
        chain=ChainIdentifier.BASE_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        mint_authority_disabled=True,
        top10_concentration=0.10,
    )
    mock_gk.evaluate_token = AsyncMock(return_value=clean_rep)
    mock_gk.screen_token = AsyncMock(return_value=clean_rep)
    engine._gatekeeper = mock_gk

    # Ingest a buy swap: 2.0 ETH in for BRETT out on a 5.0 ETH pool (40% volume ratio -> strong alpha score)
    swap = SwapEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address=aero_pool,
        token_in=weth_token,
        token_out=brett_token,
        amount_in=Decimal("2.0"),
        amount_out=Decimal("40000"),
        sender="0xBuyerAddress11111111111111111111111111111111",
        tx_hash="0xTxHashBaseDexSwap11111111111111111111111111111111",
        block_number=20000005,
    )

    await engine._ingestion_q.put(swap)

    # Run ingestion queue briefly to process the swap
    ingest_task = asyncio.create_task(engine._process_ingestion_queue())
    await asyncio.sleep(0.1)
    ingest_task.cancel()
    try:
        await ingest_task
    except asyncio.CancelledError:
        pass

    # Signal queue should have received a DEX buy or scalp signal
    assert not engine._signal_q.empty()
    sig = engine._signal_q.get_nowait()
    assert isinstance(sig, SignalEvent)
    assert sig.token_address == brett_token
    assert sig.chain == ChainIdentifier.BASE_MAINNET
    assert sig.suggested_side == OrderSide.BUY
    assert sig.execution_venue in (ExecutionVenue.UNISWAP_V2, ExecutionVenue.UNISWAP_V3)

    # Platform resolution for this signal must be DexScan
    plat = resolve_trade_platform(
        token_address=sig.token_address,
        chain=sig.chain,
        source=sig.source,
        execution_venue=sig.execution_venue,
        pool_address=sig.pool_address,
    )
    assert plat == "DexScan"


@pytest.mark.anyio
async def test_solana_raydium_unstaged_swap_generates_dex_signal_safely():
    """Verify Solana Raydium swaps on unstaged tokens (including pump-minted tokens) do not crash on frozen SignalEvent."""
    cfg = EngineConfig()
    engine = PaperTradingEngine(cfg)

    sol_mint = "So11111111111111111111111111111111111111112"
    pump_token = "GNdwu4s8QYkf8ysSqp3UZeUfB1RtMtvPrnFrsVG4pump"
    raydium_pool = "6UmmUiYo5ZbzK8kQ8FkL6Z5uN6fS7Q3eT5bY9cV3mX2a"

    pool_state = PoolState(
        pool_address=raydium_pool,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("50_000_000_000_00000"),
        native_reserve=Decimal("50_000_000_000"),  # 50 SOL
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=280_000_000,
        token_decimals=6,
        native_decimals=9,
    )
    engine._pool_registry.set(raydium_pool, pool_state)

    # Mock gatekeeper to approve GNdwu4s8QY
    mock_gk = MagicMock()
    mock_gk.is_rejected.return_value = False
    clean_rep = SecurityReport(
        token_address=pump_token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        mint_authority_disabled=True,
        top10_concentration=0.10,
    )
    mock_gk.evaluate_token = AsyncMock(return_value=clean_rep)
    mock_gk.screen_token = AsyncMock(return_value=clean_rep)
    engine._gatekeeper = mock_gk

    # Ingest a buy swap: 10 SOL in for token out on 50 SOL pool (20% volume ratio -> strong alpha, but no 3x scalp spike)
    swap = SwapEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=raydium_pool,
        token_in=sol_mint,
        token_out=pump_token,
        amount_in=Decimal("10.0"),
        amount_out=Decimal("1000000"),
        sender="BuyerWallet111111111111111111111111111111111",
        tx_hash="tx_sol_swap_111111111111111111111111111111111",
        block_number=280_000_005,
    )

    await engine._ingestion_q.put(swap)

    ingest_task = asyncio.create_task(engine._process_ingestion_queue())
    await asyncio.sleep(0.1)
    ingest_task.cancel()
    try:
        await ingest_task
    except asyncio.CancelledError:
        pass

    assert not engine._signal_q.empty()
    sig = engine._signal_q.get_nowait()
    assert isinstance(sig, SignalEvent)
    assert sig.token_address == pump_token
    assert sig.chain == ChainIdentifier.SOLANA_MAINNET
    assert sig.execution_venue == ExecutionVenue.RAYDIUM_AMM
    assert sig.suggested_side == OrderSide.BUY

    # Crucial: platform must resolve to DexScan because venue is Raydium AMM
    plat = resolve_trade_platform(
        token_address=sig.token_address,
        chain=sig.chain,
        source=sig.source,
        execution_venue=sig.execution_venue,
        pool_address=sig.pool_address,
    )
    assert plat == "DexScan"

