"""
alpha_engine.engine.runner — Async Main Loop & System Orchestrator
==================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from decimal import Decimal

import aiohttp

from alpha_engine.config import EngineConfig
from alpha_engine.engine.registry import PoolRegistry
from alpha_engine.engine.signals import SignalGenerator
from alpha_engine.execution.book import PositionBook, RunningMetrics
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.ingestion.coordinator import IngestionCoordinator
from alpha_engine.models.enums import ChainIdentifier, OrderSide
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    ShutdownSentinel,
    SignalEvent,
    SwapEvent,
)
from alpha_engine.models.state import PoolState, PortfolioSnapshot, TradeRecord
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.gatekeeper import SecurityGatekeeper

logger = logging.getLogger(__name__)


class PaperTradingEngine:
    """
    Top-level engine that wires all subsystems together and runs the
    async event loop.
    """

    def __init__(self, config: EngineConfig) -> None:
        self._cfg = config
        self._shutdown_event = asyncio.Event()

        self._limiter = RateLimiterRegistry.default()
        self._pool_registry = PoolRegistry()
        self._position_book = PositionBook()
        self._metrics = RunningMetrics(peak_equity_usd=config.initial_equity_usd)

        self._cash_sol = config.initial_sol
        self._cash_eth = config.initial_eth
        self._sol_price = config.sol_price_usd
        self._eth_price = config.eth_price_usd

        self._ingestion_q: asyncio.Queue[
            SwapEvent | PoolStateUpdateEvent | ShutdownSentinel
        ] = asyncio.Queue(maxsize=512)
        self._signal_q: asyncio.Queue[SignalEvent | ShutdownSentinel] = (
            asyncio.Queue(maxsize=256)
        )

        self._signal_gen = SignalGenerator(hold_seconds=300.0)
        self._executor: PaperExecutor | None = None
        self._ledger: SQLiteLedger | None = None
        self._gatekeeper: SecurityGatekeeper | None = None
        self._session: aiohttp.ClientSession | None = None

        self._trades_since_kelly_refresh = 0
        self._kelly_refresh_interval = 10

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        def _on_signal(sig: signal.Signals) -> None:
            logger.info("Received %s — initiating graceful shutdown.", sig.name)
            self._shutdown_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _on_signal, sig)
            except (NotImplementedError, RuntimeError):
                pass  # May fail on non-main thread or unsupported platforms

    def _current_equity_usd(self) -> Decimal:
        cash_usd = (
            self._cash_sol * self._sol_price
            + self._cash_eth * self._eth_price
        )
        unrealized = self._metrics.cumulative_realized_usd
        return cash_usd + unrealized

    async def _process_ingestion_queue(self) -> None:
        gk = self._gatekeeper
        assert gk is not None

        while True:
            item = await self._ingestion_q.get()

            if isinstance(item, ShutdownSentinel):
                logger.info("Ingestion processor received shutdown sentinel.")
                await self._signal_q.put(ShutdownSentinel())
                return

            if isinstance(item, PoolStateUpdateEvent):
                self._pool_registry.set(item.pool_address, item.new_pool_state)
                logger.debug(
                    "Pool registry refreshed from Sync: pool=%s native=%s",
                    item.pool_address[:10],
                    item.new_pool_state.native_reserve,
                )
                continue

            swap: SwapEvent = item

            pool = self._pool_registry.get(swap.pool_address)
            if pool is None:
                logger.debug(
                    "Unknown pool %s — skipping until state is seeded.",
                    swap.pool_address[:10],
                )
                continue

            try:
                report = await gk.screen_token(
                    token_address=swap.token_out,
                    chain=swap.chain,
                    pool_address=swap.pool_address,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Security gatekeeper raised for %s: %s",
                    swap.token_out[:10],
                    exc,
                )
                continue

            if not report.passes_hard_gates:
                logger.info(
                    "Token %s rejected by gatekeeper (tier=%s).",
                    swap.token_out[:10],
                    report.tier.value,
                )
                continue

            updated_pool = self._pool_registry.update_from_swap(swap, pool)

            signal_event = self._signal_gen.generate_buy_signal(swap, updated_pool, report)
            if signal_event is None:
                continue

            if self._ledger:
                await self._ledger.record_signal(signal_event)

            await self._signal_q.put(signal_event)
            logger.info(
                "Signal [%s] generated: %s %s alpha=%.3f strength=%s",
                signal_event.signal_id[:8],
                signal_event.suggested_side.value.upper(),
                signal_event.token_address[:10],
                signal_event.alpha_score,
                signal_event.strength.value,
            )

    async def _process_signal_queue(self) -> None:
        executor = self._executor
        ledger = self._ledger
        assert executor is not None and ledger is not None

        while True:
            item = await self._signal_q.get()

            if isinstance(item, ShutdownSentinel):
                logger.info("Signal processor received shutdown sentinel.")
                return

            signal_item: SignalEvent = item

            equity = self._current_equity_usd()
            fill = await executor.execute_signal(signal_item, portfolio_equity_usd=equity)
            if fill is None:
                continue

            realized_pnl_usd: Decimal | None = None

            if fill.side == OrderSide.BUY:
                self._position_book.open_lot(fill, signal_item.signal_id)
                if signal_item.chain == ChainIdentifier.BASE_MAINNET:
                    self._cash_eth -= fill.simulated_native_spent
                else:
                    self._cash_sol -= fill.simulated_native_spent

            elif fill.side == OrderSide.SELL:
                native_price = (
                    self._eth_price
                    if signal_item.chain == ChainIdentifier.BASE_MAINNET
                    else self._sol_price
                )
                pnl_native, _ = self._position_book.close_lots_fifo(
                    chain=signal_item.chain,
                    token_address=signal_item.token_address,
                    tokens_to_sell=fill.tokens_acquired,
                    sell_price=fill.effective_price,
                )
                realized_pnl_usd = pnl_native * native_price
                if signal_item.chain == ChainIdentifier.BASE_MAINNET:
                    self._cash_eth += fill.simulated_native_spent
                else:
                    self._cash_sol += fill.simulated_native_spent

                new_equity = self._current_equity_usd()
                self._metrics.record_closed_trade(
                    realized_pnl_usd=realized_pnl_usd,
                    gas_cost_usd=fill.simulated_gas_cost_usd,
                    current_equity_usd=new_equity,
                )

                self._trades_since_kelly_refresh += 1
                if self._trades_since_kelly_refresh >= self._kelly_refresh_interval:
                    await self._refresh_kelly(executor, ledger)

                logger.info(
                    "SELL fill | PnL=%.4f USD | MDD=%.2f%% | WinRate=%.1f%%",
                    float(realized_pnl_usd),
                    self._metrics.max_drawdown_pct,
                    self._metrics.win_rate_pct,
                )

            trade_record = TradeRecord.from_fill(
                fill=fill,
                signal_id=signal_item.signal_id,
                realized_pnl_usd=realized_pnl_usd,
            )
            await ledger.record_trade(trade_record)

    async def _snapshot_scheduler(self) -> None:
        ledger = self._ledger
        assert ledger is not None

        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._cfg.snapshot_interval_s,
                )
            except asyncio.TimeoutError:
                pass

            equity = self._current_equity_usd()
            snapshot = PortfolioSnapshot(
                timestamp_ns=time.time_ns(),
                total_equity_usd=equity,
                cash_native_sol=self._cash_sol,
                cash_native_eth=self._cash_eth,
                open_positions=self._position_book.open_position_count(),
                realized_pnl_usd=self._metrics.cumulative_realized_usd,
                unrealized_pnl_usd=Decimal(0),
                max_drawdown_pct=self._metrics.max_drawdown_pct,
                win_rate_pct=self._metrics.win_rate_pct,
                profit_factor=self._metrics.profit_factor,
                total_gas_spent_usd=self._metrics.total_gas_usd,
            )
            await ledger.record_snapshot(snapshot)
            logger.info(
                "Snapshot | equity=$%.2f | MDD=%.2f%% | PF=%.2f | WR=%.1f%%",
                float(equity),
                self._metrics.max_drawdown_pct,
                self._metrics.profit_factor,
                self._metrics.win_rate_pct,
            )

    async def _refresh_kelly(
        self,
        executor: PaperExecutor,
        ledger: SQLiteLedger,
    ) -> None:
        stats = await ledger.get_closed_trade_stats()
        executor.update_historical_stats(
            win_rate=stats["win_rate"],
            avg_win=stats["avg_win_native"],
            avg_loss=stats["avg_loss_native"],
        )
        self._trades_since_kelly_refresh = 0
        logger.info(
            "Kelly refreshed: win_rate=%.2f avg_win=%.4f avg_loss=%.4f total=%d",
            stats["win_rate"],
            float(stats["avg_win_native"]),
            float(stats["avg_loss_native"]),
            stats["total_trades"],
        )

    async def run(
        self,
        pool_watchlist: list[tuple[str, str, str, int, int, str]],
        svm_pool_registry: dict[str, tuple[str, str, int, int]],
        seed_pool_states: dict[str, PoolState] | None = None,
    ) -> None:
        cfg = self._cfg
        self._install_signal_handlers()

        logging.basicConfig(
            level=getattr(logging, cfg.log_level.upper(), logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )

        logger.info("PaperTradingEngine starting | equity=$%.2f", float(cfg.initial_equity_usd))

        if seed_pool_states:
            for addr, state in seed_pool_states.items():
                self._pool_registry.set(addr, state)
            logger.info("Pool registry seeded with %d pools.", len(seed_pool_states))

        async with (
            aiohttp.ClientSession(
                headers={"User-Agent": "PaperTradingEngine/1.0"}
            ) as session,
            SQLiteLedger(cfg.db_path) as ledger,
        ):
            self._session = session
            self._ledger = ledger

            self._gatekeeper = SecurityGatekeeper(
                session=session,
                limiter=self._limiter,
                evm_rpc_url=cfg.alchemy_http_url,
                evm_router_address=cfg.aerodrome_router,
                weth_address=cfg.weth_address,
                enable_tier2=True,
            )

            self._executor = PaperExecutor(
                position_book=self._position_book,
                native_price_usd=cfg.eth_price_usd,
            )

            coordinator = IngestionCoordinator(
                evm_ws_url=cfg.alchemy_ws_url,
                svm_ws_url=cfg.helius_ws_url,
                pool_watchlist=pool_watchlist,
                pool_registry=svm_pool_registry,
                limiter=self._limiter,
            )

            async with coordinator:
                async def _bridge_queues() -> None:
                    async for event in coordinator:
                        if self._shutdown_event.is_set():
                            break
                        await self._ingestion_q.put(event)
                    await self._ingestion_q.put(ShutdownSentinel())

                tasks = [
                    asyncio.create_task(_bridge_queues(), name="queue_bridge"),
                    asyncio.create_task(
                        self._process_ingestion_queue(), name="ingestion_processor"
                    ),
                    asyncio.create_task(
                        self._process_signal_queue(), name="signal_processor"
                    ),
                    asyncio.create_task(
                        self._snapshot_scheduler(), name="snapshot_scheduler"
                    ),
                ]

                logger.info("All pipelines active. Waiting for market events...")

                await self._shutdown_event.wait()
                logger.info("Shutdown event received — draining pipelines.")

                tasks[0].cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(
            "Engine stopped cleanly. Final equity: $%.2f | Trades: %d",
            float(self._current_equity_usd()),
            self._metrics.total_trades,
        )


def _build_example_config() -> tuple[
    list[tuple[str, str, str, int, int, str]],
    dict[str, tuple[str, str, int, int]],
    dict[str, PoolState],
]:
    WETH = "0x4200000000000000000000000000000000000006"
    USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    AERO_WETH_USDC = "0x6c561b446416e1a00e8e93e221854d6eA4171372"

    evm_watchlist: list[tuple[str, str, str, int, int, str]] = [
        (AERO_WETH_USDC, WETH, USDC, 18, 6, WETH),
    ]

    SOL_USDC_POOL = "8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj"
    SOL_MINT = "So11111111111111111111111111111111111111112"
    USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    svm_registry: dict[str, tuple[str, str, int, int]] = {
        SOL_USDC_POOL: (SOL_MINT, USDC_MINT, 9, 6),
    }

    seed_states: dict[str, PoolState] = {
        AERO_WETH_USDC: PoolState(
            pool_address=AERO_WETH_USDC,
            chain=ChainIdentifier.BASE_MAINNET,
            token_reserve=Decimal("5_000_000_000_000"),
            native_reserve=Decimal("1_500_000_000_000_000_000_000"),
            fee_numerator=3,
            fee_denominator=1000,
            last_updated_block=20_000_000,
            token_decimals=6,
            native_decimals=18,
        ),
    }

    return evm_watchlist, svm_registry, seed_states


async def _async_main() -> None:
    cfg = EngineConfig()
    engine = PaperTradingEngine(cfg)
    watchlist, svm_reg, seed_states = _build_example_config()
    await engine.run(
        pool_watchlist=watchlist,
        svm_pool_registry=svm_reg,
        seed_pool_states=seed_states,
    )
