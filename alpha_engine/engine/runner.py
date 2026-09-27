"""
alpha_engine.engine.runner — Async Main Loop, 24/7 Supervisor & System Orchestrator
===================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | asyncio | 24/7 Self-Healing Supervisor | Zero-Leak Production Grade
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time
from decimal import Decimal
from typing import Any, Optional

import aiohttp

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:  # pragma: no cover
    psutil = None
    PSUTIL_AVAILABLE = False

from alpha_engine.config import EngineConfig
from alpha_engine.engine.feedback import AdaptiveFeedbackEngine, TradeReflection
from alpha_engine.engine.registry import PoolRegistry
from alpha_engine.engine.rpc_health import RPCHealthMonitor
from alpha_engine.engine.signals import SignalGenerator
from alpha_engine.execution.book import PositionBook, RunningMetrics
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.ingestion.coordinator import IngestionCoordinator
from alpha_engine.logging_config import setup_production_logging
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    SignalSource,
    SignalStrength,
)
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    RawSignalEvent,
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
    async event loop under an autonomous self-healing supervisor.
    """

    def __init__(self, config: EngineConfig) -> None:
        self._cfg = config
        self._shutdown_event = asyncio.Event()
        self._start_time = time.time()

        self._limiter = RateLimiterRegistry.default()
        self._pool_registry = PoolRegistry()
        self._position_book = PositionBook()
        self._metrics = RunningMetrics(peak_equity_usd=config.initial_equity_usd)

        self._cash_sol = config.initial_sol
        self._cash_eth = config.initial_eth
        self._sol_price = config.sol_price_usd
        self._eth_price = config.eth_price_usd

        max_q = getattr(config, "max_queue_size", 100)
        self._ingestion_q: asyncio.Queue[
            SwapEvent | PoolStateUpdateEvent | RawSignalEvent | ShutdownSentinel
        ] = asyncio.Queue(maxsize=max_q)
        self._signal_q: asyncio.Queue[SignalEvent | ShutdownSentinel] = (
            asyncio.Queue(maxsize=max_q)
        )

        self._signal_gen = SignalGenerator(hold_seconds=300.0)
        self._executor: PaperExecutor | None = None
        self._ledger: SQLiteLedger | None = None
        self._gatekeeper: SecurityGatekeeper | None = None
        self._session: aiohttp.ClientSession | None = None

        self._trades_since_kelly_refresh = 0
        self._kelly_refresh_interval = 10
        self._signal_handlers_installed = False

        self._feedback = AdaptiveFeedbackEngine()
        base_urls = [config.base_rpc_http] + list(getattr(config, "base_fallback_rpcs", []))
        solana_urls = [config.solana_rpc_http] + list(getattr(config, "solana_fallback_rpcs", []))
        self._rpc_monitor = RPCHealthMonitor(
            endpoints={
                ChainIdentifier.BASE_MAINNET: [u for u in base_urls if u],
                ChainIdentifier.SOLANA_MAINNET: [u for u in solana_urls if u],
            }
        )

    @property
    def feedback_engine(self) -> AdaptiveFeedbackEngine:
        return self._feedback

    @property
    def rpc_monitor(self) -> RPCHealthMonitor:
        return self._rpc_monitor

    def _install_signal_handlers(self) -> None:
        """
        Registers asynchronous handlers for SIGINT and SIGTERM to initiate
        a clean, graceful shutdown sequence.
        """
        if self._signal_handlers_installed:
            return

        def _on_signal(sig_name: str) -> None:
            logger.info("Received %s — initiating graceful system teardown.", sig_name)
            self._shutdown_event.set()

        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, _on_signal, sig.name)
                except (NotImplementedError, RuntimeError):
                    # Fallback for Windows or non-main thread environments
                    signal.signal(
                        sig,
                        lambda s, _: _on_signal(signal.Signals(s).name),
                    )
            self._signal_handlers_installed = True
            logger.debug("Signal handlers registered for SIGINT and SIGTERM.")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not register async signal handlers: %s", exc)

    def _reset_queues(self) -> None:
        """
        Safely clears and reinitializes runtime queues between restart cycles.
        """
        max_q = getattr(self._cfg, "max_queue_size", 100)
        self._ingestion_q = asyncio.Queue(maxsize=max_q)
        self._signal_q = asyncio.Queue(maxsize=max_q)
        logger.debug("Runtime queues reinitialized (capacity=%d).", max_q)

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
                logger.info("Ingestion processor received shutdown sentinel. Forwarding to signal queue.")
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

            if isinstance(item, RawSignalEvent):
                raw_sig: RawSignalEvent = item
                try:
                    report = await gk.screen_token(
                        token_address=raw_sig.token_address,
                        chain=raw_sig.chain,
                        pool_address=raw_sig.pool_address or "",
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Security gatekeeper raised for raw signal %s: %s",
                        raw_sig.token_address[:10],
                        exc,
                    )
                    continue

                if not report.passes_hard_gates:
                    logger.info(
                        "Raw signal token %s rejected by gatekeeper (tier=%s).",
                        raw_sig.token_address[:10],
                        report.tier.value,
                    )
                    continue

                pool = self._pool_registry.get(raw_sig.token_address)
                if pool is None and raw_sig.pool_address:
                    pool = self._pool_registry.get(raw_sig.pool_address)
                if pool is None:
                    logger.debug(
                        "Token %s from Telegram feed passed security; awaiting liquidity pool seeding.",
                        raw_sig.token_address[:10],
                    )
                    continue

                social_weight = self._feedback.get_social_weight()
                signal_event = self._signal_gen.generate_social_signal(
                    raw_signal=raw_sig,
                    pool=pool,
                    report=report,
                    social_weight=social_weight,
                )
                if signal_event is None:
                    continue

                if self._ledger:
                    await self._ledger.record_signal(signal_event)

                await self._signal_q.put(signal_event)
                logger.info(
                    "Social Signal [%s] queued: %s (source=%s, sybil_count=%d, weight=%.2f)",
                    signal_event.signal_id[:8],
                    raw_sig.token_address[:10],
                    signal_event.source.value,
                    raw_sig.sybil_channel_count,
                    social_weight,
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

            # Check dynamic exits (TP ladder, trailing stop-loss, emergency liquidity drain)
            await self._check_dynamic_exits_for_swap(swap, updated_pool)

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

    async def _check_dynamic_exits_for_swap(self, swap: SwapEvent, pool: PoolState) -> None:
        executor = self._executor
        ledger = self._ledger
        if executor is None:
            return

        for token in (swap.token_out, swap.token_in):
            open_lots = self._position_book.get_open_lots(chain=swap.chain, token_address=token)
            for lot in open_lots:
                decision = self._position_book.evaluate_lot_exit(
                    lot=lot,
                    current_price=pool.spot_price_native_per_token,
                    current_pool_reserve_native=pool.native_reserve,
                )
                if decision is not None and decision.should_exit:
                    exit_fill = await executor.execute_exit(
                        chain=decision.chain,
                        token_address=decision.token_address,
                        pool=pool,
                        tokens_to_sell=decision.tokens_to_sell,
                        reason=decision.exit_reason,
                        apply_drag=True,
                    )
                    if exit_fill is not None:
                        native_price = (
                            self._eth_price
                            if decision.chain == ChainIdentifier.BASE_MAINNET
                            else self._sol_price
                        )
                        pnl_native, _ = self._position_book.apply_exit_decision(
                            decision, sell_price=exit_fill.effective_price
                        )
                        exit_pnl_usd = pnl_native * native_price
                        if decision.chain == ChainIdentifier.BASE_MAINNET:
                            self._cash_eth += exit_fill.simulated_native_spent
                        else:
                            self._cash_sol += exit_fill.simulated_native_spent

                        new_equity = self._current_equity_usd()
                        self._metrics.record_closed_trade(
                            realized_pnl_usd=exit_pnl_usd,
                            gas_cost_usd=exit_fill.simulated_gas_cost_usd,
                            current_equity_usd=new_equity,
                        )
                        rec = TradeRecord.from_fill(
                            fill=exit_fill,
                            signal_id=lot.signal_id,
                            realized_pnl_usd=exit_pnl_usd,
                        )
                        if ledger:
                            await ledger.record_trade(rec)

                        reflection = TradeReflection.from_trade(
                            trade_id=exit_fill.order_id,
                            token_address=exit_fill.token_address,
                            chain=exit_fill.chain,
                            signal_source=SignalSource.DEX_SWAP,
                            entry_price=lot.entry_price,
                            exit_price=exit_fill.effective_price,
                            realized_pnl_usd=exit_pnl_usd,
                            realized_pnl_native=pnl_native,
                            time_to_fill_ms=exit_fill.fill_latency_ms,
                        )
                        await self._feedback.record_closed_trade(reflection)

    async def _process_signal_queue(self) -> None:
        executor = self._executor
        ledger = self._ledger
        assert executor is not None and ledger is not None

        while True:
            item = await self._signal_q.get()

            if isinstance(item, ShutdownSentinel):
                logger.info("Signal processor received shutdown sentinel. Terminating queue.")
                return

            signal_item: SignalEvent = item

            equity = self._current_equity_usd()
            fill = await executor.execute_signal(signal_item, portfolio_equity_usd=equity)
            if fill is None:
                continue

            realized_pnl_usd: Decimal | None = None

            if fill.side == OrderSide.BUY:
                initial_reserve = (
                    signal_item.pool_state.native_reserve
                    if signal_item.pool_state
                    else Decimal(0)
                )
                self._position_book.open_lot(
                    fill,
                    signal_item.signal_id,
                    initial_pool_reserve_native=initial_reserve,
                )
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

                reflection = TradeReflection.from_trade(
                    trade_id=fill.order_id,
                    token_address=fill.token_address,
                    chain=fill.chain,
                    signal_source=signal_item.source,
                    entry_price=fill.effective_price,
                    exit_price=fill.effective_price,
                    realized_pnl_usd=realized_pnl_usd,
                    realized_pnl_native=pnl_native,
                    time_to_fill_ms=fill.fill_latency_ms,
                )
                await self._feedback.record_closed_trade(reflection)

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
                break
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

    async def _heartbeat_task(self) -> None:
        """
        Telemetry Watchdog: Emits periodic system health metrics every 60 seconds
        (CPU %, memory RSS MB, queue depths, open positions, uptime).
        """
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._cfg.snapshot_interval_s,
                )
                break
            except asyncio.TimeoutError:
                pass

            uptime_s = time.time() - self._start_time
            hrs, rem = divmod(int(uptime_s), 3600)
            mins, secs = divmod(rem, 60)
            uptime_str = f"{hrs:02d}h {mins:02d}m {secs:02d}s"

            rss_mb = 0.0
            cpu_pct = 0.0
            if PSUTIL_AVAILABLE and psutil is not None:
                try:
                    proc = psutil.Process()
                    rss_mb = proc.memory_info().rss / (1024 * 1024)
                    cpu_pct = proc.cpu_percent(interval=None)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Telemetry read error (psutil): %s", exc)
            else:
                try:
                    import resource
                    # ru_maxrss is KiB on Linux
                    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
                except Exception:
                    pass

            open_positions = self._position_book.open_position_count()
            equity = self._current_equity_usd()
            realized_pnl = self._metrics.cumulative_realized_usd

            logger.info(
                "HEARTBEAT | Uptime: %s | CPU: %.1f%% | RSS: %.1fMB | "
                "Queues: [Ingest: %d/%d, Signal: %d/%d] | Open Lots: %d | "
                "Trades: %d | Equity: $%.2f | Realized PnL: $%.2f | MDD: %.2f%%",
                uptime_str,
                cpu_pct,
                rss_mb,
                self._ingestion_q.qsize(),
                self._ingestion_q.maxsize,
                self._signal_q.qsize(),
                self._signal_q.maxsize,
                open_positions,
                self._metrics.total_trades,
                float(equity),
                float(realized_pnl),
                self._metrics.max_drawdown_pct,
            )

    def _get_engine_status(self) -> dict[str, Any]:
        """Collects real-time engine telemetry for the interactive Telegram bot."""
        uptime_s = time.time() - self._start_time if self._start_time else 0.0
        hrs, rem = divmod(int(uptime_s), 3600)
        mins, secs = divmod(rem, 60)
        uptime_str = f"{hrs:02d}h {mins:02d}m {secs:02d}s"

        rss_mb = 0.0
        if PSUTIL_AVAILABLE and psutil is not None:
            try:
                proc = psutil.Process()
                rss_mb = proc.memory_info().rss / (1024 * 1024)
            except Exception:
                pass
        else:
            try:
                import resource
                rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
            except Exception:
                pass

        return {
            "uptime_str": uptime_str,
            "rss_mb": rss_mb,
            "equity_usd": self._current_equity_usd(),
            "realized_pnl_usd": self._metrics.cumulative_realized_usd,
            "open_positions": self._position_book.open_position_count(),
            "win_rate_pct": self._metrics.win_rate_pct,
            "max_drawdown_pct": self._metrics.max_drawdown_pct,
            "ingest_q_size": self._ingestion_q.qsize(),
            "ingest_q_max": self._ingestion_q.maxsize,
            "signal_q_size": self._signal_q.qsize(),
            "signal_q_max": self._signal_q.maxsize,
        }

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

    async def _run_single_cycle(
        self,
        pool_watchlist: list[tuple[str, str, str, int, int, str]],
        svm_pool_registry: dict[str, tuple[str, str, int, int]],
        seed_pool_states: dict[str, PoolState] | None = None,
    ) -> None:
        """
        Executes a single cycle of the trading engine pipelines.
        Raises any unhandled critical exception so the parent supervisor can catch and restart.
        """
        cfg = self._cfg

        if seed_pool_states:
            for addr, state in seed_pool_states.items():
                self._pool_registry.set(addr, state)
            logger.info("Pool registry seeded with %d pools.", len(seed_pool_states))

        async with (
            aiohttp.ClientSession(
                headers={"User-Agent": "Bot-MM-Engine/1.0"}
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
                telegram_api_id=cfg.telegram_api_id,
                telegram_api_hash=cfg.telegram_api_hash,
                telegram_session_name=cfg.telegram_session_name,
                telegram_bot_token=cfg.telegram_bot_token,
                telegram_channels=cfg.telegram_channels,
                telegram_admin_ids=cfg.telegram_admin_ids,
                db_path=cfg.db_path,
                status_provider=self._get_engine_status,
            )

            async with coordinator:
                async def _bridge_queues() -> None:
                    try:
                        async for event in coordinator:
                            if self._shutdown_event.is_set():
                                break
                            await self._ingestion_q.put(event)
                    finally:
                        await self._ingestion_q.put(ShutdownSentinel())

                bridge_task = asyncio.create_task(_bridge_queues(), name="queue_bridge")
                ingest_task = asyncio.create_task(
                    self._process_ingestion_queue(), name="ingestion_processor"
                )
                signal_task = asyncio.create_task(
                    self._process_signal_queue(), name="signal_processor"
                )
                snapshot_task = asyncio.create_task(
                    self._snapshot_scheduler(), name="snapshot_scheduler"
                )
                heartbeat_task = asyncio.create_task(
                    self._heartbeat_task(), name="heartbeat_watchdog"
                )

                worker_tasks = [bridge_task, ingest_task, signal_task, snapshot_task, heartbeat_task]
                logger.info("All engine pipelines active. Awaiting market and social signals...")

                # Monitor tasks: wait for either graceful shutdown or unexpected worker crash
                shutdown_waiter = asyncio.create_task(
                    self._shutdown_event.wait(), name="shutdown_waiter"
                )
                done, _ = await asyncio.wait(
                    [shutdown_waiter, *worker_tasks],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Check if a worker task crashed unexpectedly
                for t in done:
                    if t is not shutdown_waiter and not t.cancelled():
                        exc = t.exception()
                        if exc is not None:
                            logger.error("Pipeline task %s failed with exception: %s", t.get_name(), exc)
                            # Re-raise so supervisor can contain and backoff restart
                            raise exc

                # If shutdown was signaled, execute graceful teardown
                if self._shutdown_event.is_set():
                    logger.info("Graceful shutdown in progress: draining remaining events...")
                    # 1. Stop ingestion loops (WebSocket & Telegram)
                    await coordinator.stop()
                    bridge_task.cancel()

                    # 2. Drain queues with a safety timeout
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(ingest_task, signal_task, return_exceptions=True),
                            timeout=15.0,
                        )
                        logger.info("All queues successfully drained and processed.")
                    except asyncio.TimeoutError:
                        logger.warning("Queue drain timed out after 15s; proceeding with teardown.")
                        ingest_task.cancel()
                        signal_task.cancel()

                    # 3. Cancel background monitoring tasks
                    snapshot_task.cancel()
                    heartbeat_task.cancel()
                    shutdown_waiter.cancel()
                    await asyncio.gather(snapshot_task, heartbeat_task, return_exceptions=True)

                    # 4. Flush and checkpoint SQLite database
                    await ledger.checkpoint()

        logger.info(
            "Engine cycle completed. Current equity: $%.2f | Closed Trades: %d",
            float(self._current_equity_usd()),
            self._metrics.total_trades,
        )

    async def run_supervised(
        self,
        pool_watchlist: list[tuple[str, str, str, int, int, str]],
        svm_pool_registry: dict[str, tuple[str, str, int, int]],
        seed_pool_states: dict[str, PoolState] | None = None,
        max_restarts: int | None = None,
        initial_backoff_s: float = 5.0,
        max_backoff_s: float = 60.0,
        backoff_factor: float = 2.0,
    ) -> None:
        """
        24/7 Self-Healing Supervisor Loop.
        Catches unhandled runtime exceptions, applies exponential backoff,
        and automatically restarts the trading engine without killing the parent process.
        """
        self._install_signal_handlers()
        setup_production_logging(
            log_level=self._cfg.log_level,
            max_bytes=20 * 1024 * 1024,
            backup_count=5,
        )

        restart_count = 0
        backoff = initial_backoff_s

        logger.info(
            "24/7 Self-Healing Supervisor initialized | initial_backoff=%.1fs | max_backoff=%.1fs",
            initial_backoff_s,
            max_backoff_s,
        )

        while not self._shutdown_event.is_set():
            cycle_start = time.time()
            try:
                logger.info(
                    "Supervisor: starting engine cycle (restart_count=%d)...",
                    restart_count,
                )
                await self._run_single_cycle(
                    pool_watchlist=pool_watchlist,
                    svm_pool_registry=svm_pool_registry,
                    seed_pool_states=seed_pool_states,
                )
                if self._shutdown_event.is_set():
                    logger.info("Supervisor: shutdown event flagged. Exiting supervisor loop.")
                    break

            except (asyncio.CancelledError, KeyboardInterrupt):
                logger.info("Supervisor: received cancellation/interrupt. Halting gracefully.")
                self._shutdown_event.set()
                break

            except Exception as exc:
                if self._shutdown_event.is_set():
                    logger.info("Supervisor: exception occurred during active shutdown: %s", exc)
                    break

                restart_count += 1
                run_duration = time.time() - cycle_start

                # Reset backoff if engine ran stably for at least 2 minutes
                if run_duration >= 120.0:
                    backoff = initial_backoff_s

                logger.critical(
                    "SUPERVISOR RECOVERY: Engine cycle crashed (#%d) after %.1fs: %s. "
                    "Applying exponential backoff: restarting in %.1fs...",
                    restart_count,
                    run_duration,
                    exc,
                    backoff,
                    exc_info=True,
                )

                if max_restarts is not None and restart_count >= max_restarts:
                    logger.error(
                        "Supervisor reached maximum allowed restarts (%d). Halting.",
                        max_restarts,
                    )
                    break

                # Sleep with interruptible wait on shutdown event
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=backoff)
                    if self._shutdown_event.is_set():
                        break
                except asyncio.TimeoutError:
                    pass

                backoff = min(max_backoff_s, backoff * backoff_factor)
                self._reset_queues()

        logger.info(
            "24/7 Autonomous Supervisor terminated. Total restarts: %d | Final Equity: $%.2f",
            restart_count,
            float(self._current_equity_usd()),
        )

    async def run(
        self,
        pool_watchlist: list[tuple[str, str, str, int, int, str]],
        svm_pool_registry: dict[str, tuple[str, str, int, int]],
        seed_pool_states: dict[str, PoolState] | None = None,
        supervised: bool = True,
        max_restarts: int | None = None,
    ) -> None:
        """
        Engine entry point. By default, executes inside the 24/7 self-healing supervisor loop.
        """
        if supervised:
            await self.run_supervised(
                pool_watchlist=pool_watchlist,
                svm_pool_registry=svm_pool_registry,
                seed_pool_states=seed_pool_states,
                max_restarts=max_restarts,
            )
        else:
            self._install_signal_handlers()
            setup_production_logging(log_level=self._cfg.log_level)
            await self._run_single_cycle(
                pool_watchlist=pool_watchlist,
                svm_pool_registry=svm_pool_registry,
                seed_pool_states=seed_pool_states,
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


if __name__ == "__main__":
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\n[Bot-MM] Shutdown initiated by operator.")
        sys.exit(0)
