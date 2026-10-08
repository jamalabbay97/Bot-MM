"""
alpha_engine.engine.runner — Async Main Loop, 24/7 Supervisor & System Orchestrator
===================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | asyncio | 24/7 Self-Healing Supervisor | Zero-Leak Production Grade
"""

from __future__ import annotations

import asyncio
import logging
import os
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

from collections import deque

from alpha_engine.chat_interface import ConversationalSupervisor
from alpha_engine.config import EngineConfig
from alpha_engine.dns_resolver import patch_dns_resolvers
from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
from alpha_engine.engine.feedback import AdaptiveFeedbackEngine, TradeReflection
from alpha_engine.engine.registry import PoolRegistry
from alpha_engine.engine.rpc_health import RPCHealthMonitor
from alpha_engine.engine.signals import PendingLaunchBuffer, SignalGenerator
from alpha_engine.engine.staging import RevivalBreakoutBuffer, Wave2StagingBuffer
from alpha_engine.execution.book import PositionBook, RunningMetrics
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.tracer import tracer
from alpha_engine.ingestion.coordinator import IngestionCoordinator
from alpha_engine.logging_config import setup_production_logging
from alpha_engine.math.cpmm import get_initial_bonding_curve_pool
from alpha_engine.models.ai import ActionParameters, AISupervisorDecisionEnum
from alpha_engine.models.decisions import DecisionRecord, DecisionType, PatternFeatureVector, RevivalPatternFeatureVector
from alpha_engine.ingestion.dex_metrics import DEXMetricsAggregator
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    ExitProfile,
    LotStatus,
    OrderSide,
    SignalSource,
    SignalStrength,
    resolve_trade_platform,
)
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    PumpMintEvent,
    PumpSwapEvent,
    RawSignalEvent,
    ShutdownSentinel,
    SignalEvent,
    SwapEvent,
)
from alpha_engine.models.news import NewsEvent, NewsSignalEvent
from alpha_engine.models.state import PoolState, PortfolioSnapshot, SecurityReport, TradeRecord
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import is_blacklisted_token
from alpha_engine.security.gatekeeper import SecurityGatekeeper

logger = logging.getLogger(__name__)


class PaperTradingEngine:
    """
    Top-level engine that wires all subsystems together and runs the
    async event loop under an autonomous self-healing supervisor.
    """

    def __init__(self, config: EngineConfig) -> None:
        self._cfg = config
        self.config = config
        self._shutdown_event = asyncio.Event()
        self._start_time = time.time()

        if getattr(config, "asyncio_debug", False):
            os.environ["PYTHONASYNCIODEBUG"] = "1"
            logging.getLogger("asyncio").setLevel(logging.DEBUG)
            try:
                loop = asyncio.get_running_loop()
                loop.set_debug(True)
            except RuntimeError:
                pass

        self._limiter = RateLimiterRegistry.default()
        self._pool_registry = PoolRegistry()
        self._position_book = PositionBook()
        self._metrics = RunningMetrics(peak_equity_usd=config.initial_equity_usd)
        self._metrics_aggregator = DEXMetricsAggregator()

        self._cash_sol = config.initial_sol
        self._cash_eth = config.initial_eth
        self._sol_price = config.sol_price_usd
        self._eth_price = config.eth_price_usd

        max_q = getattr(config, "max_queue_size", 100)
        self._ingestion_q: asyncio.Queue[
            SwapEvent | PoolStateUpdateEvent | RawSignalEvent | NewsSignalEvent | ShutdownSentinel
        ] = asyncio.Queue(maxsize=max_q)
        self._signal_q: asyncio.Queue[SignalEvent | NewsEvent | ShutdownSentinel] = (
            asyncio.Queue(maxsize=max_q)
        )

        self._recent_gatekeeper_evaluations: deque[dict[str, Any]] = deque(maxlen=20)
        self._recent_news_events: deque[dict[str, Any]] = deque(maxlen=20)
        self._recent_whale_alerts: deque[dict[str, Any]] = deque(maxlen=20)

        self._signal_gen = SignalGenerator(hold_seconds=300.0)
        doh_servers = getattr(config, "dns_doh_servers", None)
        patch_dns_resolvers(servers=doh_servers)
        min_buyers = getattr(config, "buffer_min_unique_buyers", getattr(config, "min_unique_buyers", 2))
        bundle_thresh = getattr(config, "slot_bundle_threshold", 0.60)
        self._pending_launch_buffer = PendingLaunchBuffer(
            min_age_s=getattr(config, "observation_window_min_sec", 30.0),
            max_age_s=getattr(config, "observation_window_max_sec", 90.0),
            min_buys=getattr(config, "buffer_min_buys", 3),
            min_volume_native=getattr(config, "buffer_target_volume_sol", Decimal("0.8")),
            min_unique_signers=min_buyers,
            slot_bundle_threshold=bundle_thresh,
        )
        self._wave2_buffer = Wave2StagingBuffer(config=config)
        self._revival_buffer = RevivalBreakoutBuffer(config=config)
        self._pending_launch_buffer.set_wave2_staging_buffer(self._wave2_buffer)

        # Trade Frequency & Cooldown Governor State
        self._consecutive_trade_count: int = 0
        self._cooldown_until: float = 0.0
        self._in_paced_mode: bool = False
        self._last_pnl_cycle_ts: float = time.time()

        self._explainer: Optional[ConversationalSupervisor] = None
        self._executor: PaperExecutor | None = None
        self._ledger: SQLiteLedger | None = None
        self.tracer = tracer
        self._gatekeeper: SecurityGatekeeper | None = None
        self._session: aiohttp.ClientSession | None = None
        self._telegram: Any = None
        self._coordinator: Any | None = None
        self._recent_closed_trades: deque[dict[str, Any]] = deque(maxlen=50)

        self._trades_since_kelly_refresh = 0
        self._kelly_refresh_interval = 10
        self._signal_handlers_installed = False
        self._screening_semaphore = asyncio.Semaphore(10)
        self._screening_tasks: set[asyncio.Task[Any]] = set()

        self._feedback = AdaptiveFeedbackEngine()
        self._ai_supervisor = AlphaSupervisorAI(config=config, session=self._session)
        base_urls = [config.base_rpc_http] + list(getattr(config, "base_fallback_rpcs", []))
        solana_urls = [config.solana_rpc_http] + list(getattr(config, "solana_fallback_rpcs", []))
        self._rpc_monitor = RPCHealthMonitor(
            endpoints={
                ChainIdentifier.BASE_MAINNET: [u for u in base_urls if u],
                ChainIdentifier.SOLANA_MAINNET: [u for u in solana_urls if u],
            }
        )

    @property
    def config(self) -> EngineConfig:
        return self._cfg

    @config.setter
    def config(self, val: EngineConfig) -> None:
        self._cfg = val

    @property
    def feedback_engine(self) -> AdaptiveFeedbackEngine:
        return self._feedback

    @property
    def ai_supervisor(self) -> AlphaSupervisorAI:
        return self._ai_supervisor

    @property
    def wave2_buffer(self) -> Wave2StagingBuffer:
        return self._wave2_buffer

    @property
    def revival_buffer(self) -> RevivalBreakoutBuffer:
        return self._revival_buffer

    def is_trading_paused_by_governor(self, now: Optional[float] = None) -> bool:
        """
        Check if trading frequency governor is currently enforcing cooldown.
        10 initial trades -> 4-6h mandatory cooldown -> Paced Mode (1 trade, then 3h cooldown, etc.).
        Resets after 24h positive PnL cycle or manual CLI reset.
        """
        t = now if now is not None else time.time()
        # 24-hour positive PnL cycle check
        if t - self._last_pnl_cycle_ts >= 86400.0:
            self._last_pnl_cycle_ts = t
            if hasattr(self, "_metrics") and self._metrics.realized_pnl_usd > Decimal(0):
                self.reset_trade_frequency_governor()
                logger.info(
                    "Governor: 24h cycle of positive PnL ($%s) completed. Reset trade counter.",
                    self._metrics.realized_pnl_usd,
                )

        if t < self._cooldown_until:
            return True

        if self._cooldown_until > 0 and t >= self._cooldown_until:
            if not self._in_paced_mode:
                self._in_paced_mode = True
        return False

    def record_governor_trade(self, now: Optional[float] = None) -> None:
        """
        Record a newly executed BUY trade with the governor and trigger pacing cooldowns when limits hit.
        """
        t = now if now is not None else time.time()
        self._consecutive_trade_count += 1
        max_initial = getattr(self._cfg, "pacing_max_initial_trades", 10)
        cooldown_hours = getattr(self._cfg, "pacing_cooldown_hours", 4.0)
        interval_hours = getattr(self._cfg, "pacing_interval_hours", 3.0)

        if not self._in_paced_mode:
            if self._consecutive_trade_count >= max_initial:
                self._cooldown_until = t + (cooldown_hours * 3600.0)
                self._in_paced_mode = True
                logger.warning(
                    "Governor: Reached %d initial trades. Enforcing %.1f hour mandatory cooldown until %s.",
                    self._consecutive_trade_count,
                    cooldown_hours,
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._cooldown_until)),
                )
        else:
            self._cooldown_until = t + (interval_hours * 3600.0)
            logger.info(
                "Governor: Paced trade executed. Entering %.1f hour interval cooldown until %s.",
                interval_hours,
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._cooldown_until)),
            )

    def reset_trade_frequency_governor(self) -> None:
        """Manually or cyclically reset trade frequency governor state."""
        self._consecutive_trade_count = 0
        self._cooldown_until = 0.0
        self._in_paced_mode = False
        logger.info("Governor: Trading frequency and pacing governor reset.")

    @property
    def explainer(self) -> Optional[ConversationalSupervisor]:
        return self._explainer

    @property
    def rpc_monitor(self) -> RPCHealthMonitor:
        return self._rpc_monitor

    @property
    def launch_buffer(self) -> PendingLaunchBuffer:
        return self._pending_launch_buffer

    @staticmethod
    def _format_number_subscript(price: Any) -> str:
        try:
            val = float(price)
            if val <= 0:
                return "0.00"
            if val >= 1.0:
                return f"{val:.4f}"
            s = f"{val:.12f}"
            dec_part = s.split(".")[1]
            zero_count = 0
            for ch in dec_part:
                if ch == "0":
                    zero_count += 1
                else:
                    break
            subscripts = "₀₁₂₃₄₅₆₇₈₉"
            if zero_count >= 3:
                sub_str = "".join(subscripts[int(d)] for d in str(zero_count))
                sig_digits = dec_part[zero_count : zero_count + 4]
                return f"0.0{sub_str}{sig_digits}"
            return f"{val:.8f}"
        except Exception:
            return f"{price}"

    @classmethod
    def _format_price_clean(cls, price: Any, native_price_usd: Any = None, unit: str = "") -> str:
        try:
            val = float(price)
            if native_price_usd is not None and float(native_price_usd) > 0 and unit:
                usd_val = val * float(native_price_usd)
                return f"${cls._format_number_subscript(usd_val)} ({cls._format_number_subscript(val)} {unit})"
            return f"${cls._format_number_subscript(val)}"
        except Exception:
            return f"${price}"

    async def _broadcast_trade_alert(self, message: str) -> None:
        """Broadcast real-time trade alert to Telegram admins."""
        if self._telegram is not None and hasattr(self._telegram, "broadcast_trade_alert"):
            try:
                res = self._telegram.broadcast_trade_alert(message)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as exc:
                logger.warning("Failed to broadcast trade alert to Telegram: %s", exc)

    def _record_closed_trade_and_notify(
        self,
        lot: Any,
        exit_fill: Any,
        decision_reason: str,
        exit_pnl_usd: Decimal,
        pnl_native: Decimal,
    ) -> None:
        """Records closed trade in memory and dispatches rich Telegram alert."""
        ep = getattr(lot, "entry_price", Decimal(0))
        xp = getattr(exit_fill, "effective_price", Decimal(0))
        pct = float((xp - ep) / ep * 100) if ep > 0 else 0.0
        is_win = exit_pnl_usd > 0
        dur_s = (time.time_ns() - lot.open_timestamp_ns) / 1e9 if getattr(lot, "open_timestamp_ns", 0) > 0 else 0.0

        platform_name = getattr(lot, "platform", None) or getattr(exit_fill, "platform", None) or resolve_trade_platform(
            token_address=lot.token_address,
            chain=lot.chain,
        )

        self._recent_closed_trades.append({
            "token_address": lot.token_address,
            "chain": lot.chain,
            "platform": platform_name,
            "side": "SELL",
            "entry_price": ep,
            "exit_price": xp,
            "realized_pnl_usd": exit_pnl_usd,
            "realized_pnl_pct": pct,
            "exit_reason": decision_reason,
            "duration_s": dur_s,
            "is_win": is_win,
            "timestamp": time.time(),
        })

        status_icon = "🟢" if is_win else "🔴"
        title = f"TAKE-PROFIT (+{pct:.2f}%)" if is_win else f"STOP-LOSS ({pct:.2f}%)"
        unit = "ETH" if lot.chain == ChainIdentifier.BASE_MAINNET else "SOL"
        native_price = self._eth_price if lot.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
        ep_str = self._format_price_clean(ep, native_price, unit)
        xp_str = self._format_price_clean(xp, native_price, unit)
        t_str = f"{lot.token_address[:4]}..{lot.token_address[-4:]}"
        usd_sign = "+" if exit_pnl_usd > 0 else ""
        chain_val = lot.chain.value if hasattr(lot.chain, "value") else str(lot.chain)
        is_swing = getattr(lot, "exit_profile", None) == ExitProfile.REVIVAL_SWING

        logger.info(
            "CLOSED TRADE | Platform=%s | Token=%s (%s) | Entry=%s | Exit=%s | PnL=%+.2f%% (%s$%.2f) | Reason=%s | Duration=%.1fs",
            platform_name,
            lot.token_address[:10],
            chain_val,
            ep_str,
            xp_str,
            pct,
            usd_sign,
            float(exit_pnl_usd),
            decision_reason,
            dur_s,
        )

        if is_swing:
            peak_p = getattr(lot, "peak_price", ep)
            peak_roi = float((peak_p - ep) / ep * 100) if ep > 0 else 0.0
            revival_vec = RevivalPatternFeatureVector(
                token_address=lot.token_address,
                token_age_hours=float(getattr(lot, "token_age_hours", 0.0)),
                consolidation_length_hours=float(getattr(lot, "consolidation_length_hours", 0.0)),
                base_mcap_usd=float(getattr(lot, "base_market_cap", 0.0) or 0.0),
                volume_surge_multiplier=float(getattr(lot, "volume_surge_multiplier", 3.0)),
                net_buy_ratio=float(getattr(lot, "net_buy_delta", 0.70)),
                peak_roi_pct=peak_roi,
                realized_pnl_pct=pct,
            )
            if hasattr(self, "_feedback") and self._feedback.pattern_store:
                self._feedback.pattern_store.add_revival_pattern(revival_vec)

            exit_msg = (
                f"{status_icon} **REVIVAL SWING EXIT ({title})**\n\n"
                f"• **Platform:** `{platform_name}`\n"
                f"• **Token:** `{t_str}` (`{lot.token_address}`)\n"
                f"• **Chain:** {chain_val}\n"
                f"• **Strategy Profile:** `REVIVAL_SWING`\n"
                f"• **Entry Price:** `{ep_str}`\n"
                f"• **Exit Price:** `{xp_str}`\n"
                f"• **Peak Price:** `{self._format_price_clean(peak_p, native_price, unit)}` (Peak: `{peak_roi:+.1f}%`)\n"
                f"• **Return %:** `{pct:+.2f}%` ({usd_sign}${float(exit_pnl_usd):.2f})\n"
                f"• **Exit Reason:** `{decision_reason}`\n"
                f"• **Holding Time:** `{dur_s / 3600.0:.2f}h`"
            )
        else:
            exit_msg = (
                f"{status_icon} **{title} (Paper Trade)**\n\n"
                f"• **Platform:** `{platform_name}`\n"
                f"• **Token:** `{t_str}` (`{lot.token_address}`)\n"
                f"• **Chain:** {chain_val}\n"
                f"• **Entry Price:** `{ep_str}`\n"
                f"• **Exit Price:** `{xp_str}`\n"
                f"• **Return %:** `{pct:+.2f}%` ({usd_sign}${float(exit_pnl_usd):.2f})\n"
                f"• **Exit Reason:** `{decision_reason}`\n"
                f"• **Holding Time:** `{dur_s:.1f}s`"
            )
        asyncio.create_task(self._broadcast_trade_alert(exit_msg))
        if hasattr(self, "_ai_supervisor") and self._ai_supervisor:
            reflection = TradeReflection.from_trade(
                trade_id=str(getattr(exit_fill, "order_id", "")),
                token_address=lot.token_address,
                chain=lot.chain,
                signal_source=SignalSource.DEX_SWAP,
                entry_price=ep,
                exit_price=xp,
                realized_pnl_usd=exit_pnl_usd,
                realized_pnl_native=pnl_native,
                time_to_fill_ms=getattr(exit_fill, "fill_latency_ms", 50.0),
            )
            asyncio.create_task(self._ai_supervisor.reflect_on_trade(reflection))


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

    def _total_equity_usd(self) -> Decimal:
        return self._current_equity_usd()

    def is_position_open(
        self,
        token_address: str,
        chain: ChainIdentifier | None = None,
    ) -> bool:
        return self._position_book.is_position_open(token_address, chain=chain)

    def _extract_gatekeeper_rejection_reason(self, report: SecurityReport) -> str:
        if getattr(report, "passes_hard_gates", False):
            return "Passed all security filters"
        reasons = []
        if getattr(report, "is_honeypot", False):
            reasons.append("Honeypot detected")
        buy_tax = (
            float(report.buy_tax_bps) / 100.0
            if hasattr(report, "buy_tax_bps")
            else float(getattr(report, "buy_tax_pct", 0.0))
        )
        sell_tax = (
            float(report.sell_tax_bps) / 100.0
            if hasattr(report, "sell_tax_bps")
            else float(getattr(report, "sell_tax_pct", 0.0))
        )
        if buy_tax > 5.0 or sell_tax > 5.0:
            reasons.append(f"Excessive tax (B:{buy_tax:.1f}%/S:{sell_tax:.1f}%)")
        mint_ok = getattr(report, "mint_authority_disabled", None)
        if mint_ok is None:
            mint_ok = getattr(report, "mint_disabled", True)
        if not mint_ok:
            reasons.append("Mint authority active")
        top10 = float(getattr(report, "top10_concentration", 0.0) or 0.0)
        is_pump = getattr(report, "is_pump_fun", False) or (
            hasattr(report, "token_address") and str(report.token_address).lower().endswith("pump")
        )
        if is_pump:
            if top10 > 0.65:
                reasons.append(f"Top 10 concentration > 65% ({top10 * 100:.1f}%)")
        else:
            if top10 > 0.50:
                reasons.append(f"Top 10 concentration > 50% ({top10 * 100:.1f}%)")
            elif top10 > 0.20:
                reasons.append(f"Top 10 concentration > 20% ({top10 * 100:.1f}%)")
        lp_burned = float(getattr(report, "lp_burned_ratio", 1.0) or 0.0)
        if lp_burned < 0.90:
            reasons.append(f"LP not burned or locked ({lp_burned * 100:.1f}% < 90%)")
        return "; ".join(reasons) if reasons else "Failed hard security gates"

    def _record_gatekeeper_eval(
        self,
        token_address: str,
        chain: ChainIdentifier,
        reason: str,
        passed: bool,
    ) -> None:
        self._recent_gatekeeper_evaluations.append({
            "token_address": token_address,
            "chain": chain,
            "reason": reason,
            "passed": passed,
            "timestamp": time.time(),
        })

    def _calculate_news_sentiment(self, text: str) -> float:
        bullish_words = {"bull", "bullish", "moon", "pump", "surge", "breakout", "listing", "ath", "launch", "rally", "partnership", "buy", "up"}
        bearish_words = {"bear", "bearish", "dump", "rug", "scam", "hack", "exploit", "crash", "drain", "down", "sell", "liquidation", "fud"}
        tokens = text.lower().split()
        bull_count = sum(1 for w in tokens if any(b in w for b in bullish_words))
        bear_count = sum(1 for w in tokens if any(b in w for b in bearish_words))
        total = bull_count + bear_count
        if total == 0:
            return 0.0
        return (bull_count - bear_count) / total

    def _record_news_event(
        self,
        token_address: str,
        chain: ChainIdentifier,
        source: str,
        headline: str,
        sentiment: float,
    ) -> None:
        self._recent_news_events.append({
            "token_address": token_address,
            "chain": chain,
            "source": source,
            "headline": headline,
            "sentiment": sentiment,
            "timestamp": time.time(),
        })

    def get_or_create_initial_pool_state(
        self,
        chain: ChainIdentifier,
        token_address: str,
        pool_address: str = "",
    ) -> PoolState:
        pool = self._pool_registry.get(token_address)
        if pool is None and pool_address:
            pool = self._pool_registry.get(pool_address)
        if pool is None:
            pool = get_initial_bonding_curve_pool(
                chain=chain,
                token_address=token_address,
                pool_address=pool_address,
            )
            self._pool_registry.set(token_address, pool)
            if pool_address:
                self._pool_registry.set(pool_address, pool)
        return pool

    async def _process_ingestion_queue(self) -> None:
        gk = self._gatekeeper
        assert gk is not None

        while True:
            item = await self._ingestion_q.get()

            if isinstance(item, ShutdownSentinel):
                logger.info("Ingestion processor received shutdown sentinel. Forwarding to signal queue.")
                if self._screening_tasks:
                    await asyncio.gather(*self._screening_tasks, return_exceptions=True)
                await self._signal_q.put(ShutdownSentinel())
                return

            if isinstance(item, PoolStateUpdateEvent):
                self._pool_registry.set(item.pool_address, item.new_pool_state)
                self._metrics_aggregator.record_pool_state(item.new_pool_state)
                logger.debug(
                    "Pool registry refreshed from Sync: pool=%s native=%s",
                    item.pool_address[:10],
                    item.new_pool_state.native_reserve,
                )
                if item.new_pool_state.token_address:
                    self._ai_supervisor.record_price_update(
                        item.new_pool_state.token_address,
                        item.new_pool_state.spot_price_native_per_token,
                    )
                continue

            if isinstance(item, NewsSignalEvent):
                news_event: NewsSignalEvent = item
                token_addr = news_event.token_address
                chain = news_event.chain

                # Fast-path blacklist filter
                if is_blacklisted_token(token_addr, chain):
                    self._record_gatekeeper_eval(token_addr, chain, "Blacklisted native/wrapped token", passed=False)
                    logger.debug("Skipping blacklisted token from news: %s", token_addr)
                    continue

                # Deduplication check
                if self.is_position_open(token_addr, chain):
                    logger.debug("Position already open for news token %s; skipping signal generation", token_addr[:10])
                    continue

                task = asyncio.create_task(
                    self._async_screen_and_process_news(news_event, gk),
                    name=f"screen_news_{token_addr[:8]}",
                )
                self._screening_tasks.add(task)
                task.add_done_callback(self._screening_tasks.discard)
                continue

            if isinstance(item, PumpMintEvent):
                mint_ev: PumpMintEvent = item
                mint = mint_ev.mint
                chain = mint_ev.chain

                if is_blacklisted_token(mint, chain):
                    self._record_gatekeeper_eval(mint, chain, "Blacklisted native/wrapped token", passed=False)
                    logger.debug("Skipping blacklisted mint: %s", mint)
                    continue

                if gk.is_rejected(mint, chain):
                    logger.debug("Skipping rejected mint from negative cache: %s", mint[:10])
                    continue

                task = asyncio.create_task(
                    self._async_screen_and_process_pump_mint(mint_ev, gk),
                    name=f"screen_mint_{mint[:8]}",
                )
                self._screening_tasks.add(task)
                task.add_done_callback(self._screening_tasks.discard)
                continue

            if isinstance(item, RawSignalEvent):
                raw_sig: RawSignalEvent = item
                if is_blacklisted_token(raw_sig.token_address, raw_sig.chain):
                    self._record_gatekeeper_eval(raw_sig.token_address, raw_sig.chain, "Blacklisted native/wrapped token", passed=False)
                    logger.debug("Skipping blacklisted token: %s", raw_sig.token_address)
                    continue

                if self.is_position_open(raw_sig.token_address, raw_sig.chain):
                    logger.debug("Position already open for %s; skipping raw signal", raw_sig.token_address[:10])
                    continue

                task = asyncio.create_task(
                    self._async_screen_and_process_raw_signal(raw_sig, gk),
                    name=f"screen_raw_{raw_sig.token_address[:8]}",
                )
                self._screening_tasks.add(task)
                task.add_done_callback(self._screening_tasks.discard)
                continue

            swap: SwapEvent = item

            out_is_native = is_blacklisted_token(swap.token_out, swap.chain)
            in_is_native = is_blacklisted_token(swap.token_in, swap.chain)

            # Drop only if both sides are blacklisted/native
            if out_is_native and in_is_native:
                self._record_gatekeeper_eval(swap.token_out, swap.chain, "Blacklisted native/wrapped token", passed=False)
                continue

            target_token = swap.token_in if out_is_native else swap.token_out
            is_buy = not out_is_native
            sol_amount = swap.amount_in if is_buy else swap.amount_out

            # Fast-path check: immediately drop swap if token is in negative rejection cache
            if gk.is_rejected(target_token, swap.chain) is True:
                continue

            is_pump = (
                isinstance(swap, PumpSwapEvent)
                or (
                    swap.chain == ChainIdentifier.SOLANA_MAINNET
                    and "pump" in swap.pool_address.lower()
                )
            )
            auto_min_vol = Decimal("0.02") if swap.chain == ChainIdentifier.BASE_MAINNET else Decimal("0.5")
            if (
                is_pump
                and is_buy
                and sol_amount >= auto_min_vol
                and not self._pending_launch_buffer.is_staged(target_token)
                and not self.is_position_open(target_token, swap.chain)
                and not is_blacklisted_token(target_token, swap.chain)
                and not gk.is_rejected(target_token, swap.chain)
            ):
                try:
                    disc_report = await gk.evaluate_token(
                        token_address=target_token,
                        chain=swap.chain,
                        pool_address=swap.pool_address,
                    )
                    disc_reason = self._extract_gatekeeper_rejection_reason(disc_report)
                    self._record_gatekeeper_eval(target_token, swap.chain, disc_reason, passed=disc_report.passes_hard_gates)
                    if disc_report.passes_hard_gates:
                        initial_pool = self.get_or_create_initial_pool_state(swap.chain, target_token, swap.pool_address)
                        init_price = initial_pool.spot_price_native_per_token if initial_pool else Decimal("0.000000028")
                        self._pending_launch_buffer.stage_token(
                            token_address=target_token,
                            chain=swap.chain,
                            pool_address=swap.pool_address,
                            initial_price=init_price,
                            report=disc_report,
                            t_0=time.time(),
                        )
                        logger.info(
                            "Auto-discovery fallback: staged %s in PendingLaunchBuffer on buy of %s SOL",
                            target_token[:10],
                            sol_amount,
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Auto-discovery security evaluation failed for %s: %s",
                        target_token[:10],
                        exc,
                    )

            pool = self._pool_registry.get(swap.pool_address)
            if pool is None:
                pool = self._pool_registry.get(target_token)
            if pool is None and (self._pending_launch_buffer.is_staged(target_token) or self.is_position_open(target_token, swap.chain)):
                pool = self.get_or_create_initial_pool_state(swap.chain, target_token, swap.pool_address)

            if pool is None:
                logger.debug(
                    "Unknown pool %s — skipping until state is seeded.",
                    swap.pool_address[:10],
                )
                continue

            updated_pool = self._pool_registry.update_from_swap(swap, pool)
            self._metrics_aggregator.record_swap(swap, pool_state=updated_pool)
            self._ai_supervisor.record_price_update(
                target_token,
                updated_pool.spot_price_native_per_token,
            )

            # Check dynamic exits (TP ladder, trailing stop-loss, emergency liquidity drain)
            await self._check_dynamic_exits_for_swap(swap, updated_pool)

            # Route trade directly to launch buffer; incoming swap events bypass gatekeeper screening
            launch_signal = self._pending_launch_buffer.record_trade(
                swap,
                pool=updated_pool,
                signal_generator=self._signal_gen,
            )
            if launch_signal is not None:
                if not self.is_position_open(launch_signal.token_address, launch_signal.chain):
                    if self._ledger:
                        await self._ledger.record_signal(launch_signal)
                    await self._signal_q.put(launch_signal)
                    logger.info(
                        "Smart Launch Signal [%s] graduated & queued: %s (source=%s)",
                        launch_signal.signal_id[:8],
                        launch_signal.token_address[:10],
                        launch_signal.source.value,
                    )
                continue

            # Route trade to Wave-2 Staging Buffer to track consolidation and detect accumulation breakout
            wave2_signal = self._wave2_buffer.record_trade(
                swap,
                pool=updated_pool,
            )
            if wave2_signal is not None:
                if not self.is_position_open(wave2_signal.token_address, wave2_signal.chain):
                    if self._ledger:
                        await self._ledger.record_signal(wave2_signal)
                    await self._signal_q.put(wave2_signal)
                    logger.info(
                        "Wave-2 Breakout Signal [%s] generated & queued: %s (alpha=%.3f, strength=%s)",
                        wave2_signal.signal_id[:8],
                        wave2_signal.token_address[:10],
                        wave2_signal.alpha_score,
                        wave2_signal.strength.value,
                    )
                continue

            # Route trade to Revival Breakout Buffer to track dormant aged tokens and detect CTO breakout
            revival_signal = self._revival_buffer.record_trade(
                swap,
                pool=updated_pool,
            )
            if revival_signal is not None:
                if not self.is_position_open(revival_signal.token_address, revival_signal.chain):
                    if self._ledger:
                        await self._ledger.record_signal(revival_signal)
                    await self._signal_q.put(revival_signal)
                    logger.info(
                        "Revival Breakout Signal [%s] generated & queued: %s (alpha=%.3f, strength=%s, age=%.1fh)",
                        revival_signal.signal_id[:8],
                        revival_signal.token_address[:10],
                        revival_signal.alpha_score,
                        revival_signal.strength.value,
                        getattr(revival_signal, "token_age_hours", 0.0),
                    )
                continue

            # Sells do not trigger buy signals
            if out_is_native:
                continue

            # Deduplication check for BUY
            if self.is_position_open(swap.token_out, swap.chain):
                logger.debug("Position already open for token %s; skipping buy signal from swap", swap.token_out[:10])
                continue

            # If token is staged in pending launch buffer, Wave-2, or Revival buffer, do not generate premature swap buy signal
            if self._pending_launch_buffer.is_staged(swap.token_out):
                logger.debug("Token %s staged in PendingLaunchBuffer; awaiting graduation before BUY signal", swap.token_out[:10])
                continue

            if self._wave2_buffer.is_staged(swap.token_out):
                logger.debug("Token %s staged in Wave2StagingBuffer; awaiting Wave-2 breakout before BUY signal", swap.token_out[:10])
                continue

            if self._revival_buffer.is_staged(swap.token_out):
                logger.debug("Token %s staged in RevivalBreakoutBuffer; awaiting revival breakout before BUY signal", swap.token_out[:10])
                continue

            staged = self._pending_launch_buffer.get_staged(target_token)
            report = staged.report if staged is not None else None
            if report is not None and report.passes_hard_gates:
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
            elif not gk.is_rejected(target_token, swap.chain):
                # Unstaged DEX swap path: evaluate security and check for DEX scalp or buy signal
                try:
                    dex_report = await gk.evaluate_token(
                        token_address=target_token,
                        chain=swap.chain,
                        pool_address=swap.pool_address,
                    )
                    reason = self._extract_gatekeeper_rejection_reason(dex_report)
                    self._record_gatekeeper_eval(target_token, swap.chain, reason, passed=dex_report.passes_hard_gates)
                except Exception as exc:  # noqa: BLE001
                    logger.error("DEX swap security screening failed for %s: %s", target_token[:10], exc)
                    dex_report = None

                if dex_report is not None and dex_report.passes_hard_gates:
                    scalp_decision = self._signal_gen.generate_short_term_scalp_signal(
                        swap=swap,
                        pool=updated_pool,
                        report=dex_report,
                        metrics_aggregator=self._metrics_aggregator,
                    )
                    signal_event = None
                    if scalp_decision is not None:
                        signal_event = scalp_decision.to_signal_event(updated_pool, dex_report)
                    else:
                        venue = (
                            ExecutionVenue.RAYDIUM_AMM
                            if swap.chain == ChainIdentifier.SOLANA_MAINNET
                            else ExecutionVenue.UNISWAP_V2
                        )
                        signal_event = self._signal_gen.generate_buy_signal(
                            swap, updated_pool, dex_report, execution_venue=venue
                        )
                        if signal_event is not None and signal_event.execution_venue != venue:
                            signal_event = signal_event.model_copy(update={"execution_venue": venue})

                    if signal_event is not None:
                        if not self.is_position_open(signal_event.token_address, signal_event.chain):
                            if self._ledger:
                                await self._ledger.record_signal(signal_event)
                            await self._signal_q.put(signal_event)
                            logger.info(
                                "DEX Signal [%s] generated: %s %s alpha=%.3f strength=%s (venue=%s)",
                                signal_event.signal_id[:8],
                                signal_event.suggested_side.value.upper(),
                                signal_event.token_address[:10],
                                signal_event.alpha_score,
                                signal_event.strength.value,
                                getattr(signal_event.execution_venue, "value", signal_event.execution_venue),
                            )

    async def _async_screen_and_process_news(self, news_event: NewsSignalEvent, gk: Any) -> None:
        async with self._screening_semaphore:
            token_addr = news_event.token_address
            chain = news_event.chain
            try:
                report = await gk.screen_token(
                    token_address=token_addr,
                    chain=chain,
                    pool_address="",
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("Security gatekeeper raised for news signal %s: %s", token_addr[:10], exc)
                return

            reason = self._extract_gatekeeper_rejection_reason(report)
            self._record_gatekeeper_eval(token_addr, chain, reason, passed=report.passes_hard_gates)

            headline = news_event.headline or news_event.content[:80]
            sentiment = (
                news_event.sentiment_score
                if news_event.sentiment_score != 0.0
                else self._calculate_news_sentiment(news_event.content)
            )
            self._record_news_event(token_addr, chain, news_event.source.value, headline, sentiment)

            if not report.passes_hard_gates:
                logger.info("News token %s rejected by gatekeeper: %s", token_addr[:10], reason)
                return

            pool = self.get_or_create_initial_pool_state(chain, token_addr)
            social_weight = self._feedback.get_social_weight()
            raw_sig = RawSignalEvent(
                timestamp_ns=time.time_ns(),
                chain=chain,
                token_address=token_addr,
                source=news_event.source,
                raw_text=news_event.content,
                sybil_channel_count=1,
                narrative_cluster=getattr(news_event, "narrative_cluster", None),
            )
            signal_event = self._signal_gen.generate_social_signal(
                raw_signal=raw_sig,
                pool=pool,
                report=report,
                social_weight=social_weight,
            )
            if signal_event is not None:
                if getattr(news_event, "narrative_cluster", None):
                    signal_event = signal_event.model_copy(update={"narrative_cluster": news_event.narrative_cluster})
                if self._ledger:
                    await self._ledger.record_signal(signal_event)
                await self._signal_q.put(signal_event)
                logger.info("News Social Signal [%s] queued: %s", signal_event.signal_id[:8], token_addr[:10])

    async def _async_screen_and_process_pump_mint(self, mint_ev: PumpMintEvent, gk: Any) -> None:
        async with self._screening_semaphore:
            mint = mint_ev.mint
            chain = mint_ev.chain
            curve_addr = mint_ev.bonding_curve or mint
            try:
                report = await gk.evaluate_token(
                    token_address=mint,
                    chain=chain,
                    pool_address=curve_addr,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("Security gatekeeper raised for PumpMintEvent %s: %s", mint[:10], exc)
                return

            reason = self._extract_gatekeeper_rejection_reason(report)
            self._record_gatekeeper_eval(mint, chain, reason, passed=report.passes_hard_gates)

            if not report.passes_hard_gates:
                logger.info("Pump.fun mint %s rejected by gatekeeper (reason=%s).", mint[:10], reason)
                return

            pool = self.get_or_create_initial_pool_state(chain, mint, curve_addr)
            init_price = pool.spot_price_native_per_token if pool else Decimal("0.000000028")

            self._pending_launch_buffer.stage_token(
                token_address=mint,
                chain=chain,
                pool_address=curve_addr,
                initial_price=init_price,
                report=report,
                t_0=time.time(),
            )
            logger.info(
                "Pump.fun mint [%s] staged in PendingLaunchBuffer (observation window %.0fs-%.0fs, target vol >= %s SOL, >= %d buys, >= %d unique buyers)",
                mint[:10],
                self._pending_launch_buffer.min_age_s,
                self._pending_launch_buffer.max_age_s,
                self._pending_launch_buffer.min_volume_native,
                self._pending_launch_buffer.min_buys,
                self._pending_launch_buffer.min_unique_signers,
            )

    async def _async_screen_and_process_raw_signal(self, raw_sig: RawSignalEvent, gk: Any) -> None:
        async with self._screening_semaphore:
            try:
                report = await gk.evaluate_token(
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
                return

            reason = self._extract_gatekeeper_rejection_reason(report)
            self._record_gatekeeper_eval(raw_sig.token_address, raw_sig.chain, reason, passed=report.passes_hard_gates)

            if not report.passes_hard_gates:
                logger.info(
                    "Raw signal token %s rejected by gatekeeper (reason=%s).",
                    raw_sig.token_address[:10],
                    reason,
                )
                return

            pool = self._pool_registry.get(raw_sig.token_address)
            if pool is None and raw_sig.pool_address:
                pool = self._pool_registry.get(raw_sig.pool_address)
            if pool is None:
                pool = self.get_or_create_initial_pool_state(
                    raw_sig.chain, raw_sig.token_address, raw_sig.pool_address or ""
                )

            if raw_sig.source == SignalSource.PUMP_FUN_MINT:
                init_price = pool.spot_price_native_per_token if pool else Decimal("0.000000028")
                self._pending_launch_buffer.stage_token(
                    token_address=raw_sig.token_address,
                    chain=raw_sig.chain,
                    pool_address=raw_sig.pool_address or (pool.pool_address if pool else ""),
                    initial_price=init_price,
                    report=report,
                    raw_signal=raw_sig,
                    t_0=time.time(),
                )
                logger.info(
                    "Pump.fun mint [%s] staged in PendingLaunchBuffer (observation window %.0fs-%.0fs, target vol >= %s SOL, >= %d buys, >= %d unique buyers)",
                    raw_sig.token_address[:10],
                    self._pending_launch_buffer.min_age_s,
                    self._pending_launch_buffer.max_age_s,
                    self._pending_launch_buffer.min_volume_native,
                    self._pending_launch_buffer.min_buys,
                    self._pending_launch_buffer.min_unique_signers,
                )
                return

            social_weight = self._feedback.get_social_weight()
            signal_event = self._signal_gen.generate_social_signal(
                raw_signal=raw_sig,
                pool=pool,
                report=report,
                social_weight=social_weight,
            )
            if signal_event is None:
                return

            updates: dict[str, Any] = {}
            if raw_sig.source == SignalSource.PAIR_CREATED:
                updates["source"] = SignalSource.PAIR_CREATED
                updates["execution_venue"] = (
                    ExecutionVenue.RAYDIUM_AMM
                    if raw_sig.chain == ChainIdentifier.SOLANA_MAINNET
                    else ExecutionVenue.UNISWAP_V2
                )

            if getattr(raw_sig, "narrative_cluster", None):
                updates["narrative_cluster"] = raw_sig.narrative_cluster

            if updates:
                signal_event = signal_event.model_copy(update=updates)

            if self._ledger:
                await self._ledger.record_signal(signal_event)

            await self._signal_q.put(signal_event)
            logger.info(
                "Signal [%s] queued: %s (source=%s, sybil_count=%d, weight=%.2f)",
                signal_event.signal_id[:8],
                raw_sig.token_address[:10],
                signal_event.source.value,
                raw_sig.sybil_channel_count,
                social_weight,
            )

    async def _check_dynamic_exits_for_swap(self, swap: SwapEvent, pool: PoolState) -> None:
        executor = self._executor
        ledger = self._ledger
        if executor is None:
            return

        grace_sec = getattr(self.config, "exit_grace_period_sec", 15.0)
        for token in (swap.token_out, swap.token_in):
            open_lots = self._position_book.get_open_lots(chain=swap.chain, token_address=token)
            is_sell = (swap.token_in == token)
            vol = swap.amount_out if is_sell else swap.amount_in
            for lot in open_lots:
                decision = self._position_book.evaluate_lot_exit(
                    lot=lot,
                    current_price=pool.spot_price_native_per_token,
                    current_pool_reserve_native=pool.native_reserve,
                    tick_timestamp_s=time.time(),
                    current_timestamp_ns=swap.timestamp_ns,
                    bonding_curve_mode=lot.bonding_curve_mode,
                    exit_grace_period_sec=grace_sec,
                    trade_volume_native=vol,
                    is_sell=is_sell,
                )
                if decision is not None and decision.should_exit:
                    exit_fill = await executor.execute_exit(
                        chain=decision.chain,
                        token_address=decision.token_address,
                        pool=pool,
                        tokens_to_sell=decision.tokens_to_sell,
                        reason=decision.exit_reason,
                        apply_drag=True,
                        portfolio_equity_usd=max(Decimal("1.0"), self._current_equity_usd()),
                        platform=getattr(lot, "platform", None),
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
                        platform_name = getattr(lot, "platform", None) or getattr(exit_fill, "platform", None) or resolve_trade_platform(
                            token_address=decision.token_address,
                            chain=decision.chain,
                            pool_address=getattr(pool, "pool_address", ""),
                        )
                        logger.info(
                            "SWAP EXIT executed | Platform=%s | %s %s | reason=%s | exit_price=%s | PnL=%.4f USD | WR=%.1f%%",
                            platform_name,
                            decision.chain.value,
                            decision.token_address[:10],
                            decision.exit_reason.value if decision.exit_reason else "unknown",
                            exit_fill.effective_price,
                            float(exit_pnl_usd),
                            self._metrics.win_rate_pct,
                        )
                        rec = TradeRecord.from_fill(
                            fill=exit_fill,
                            signal_id=lot.signal_id,
                            realized_pnl_usd=exit_pnl_usd,
                            platform=platform_name,
                        )
                        if ledger:
                            await ledger.record_trade(rec)
                            exit_type = DecisionType.TRAIL_STOP if "trail" in str(decision.exit_reason).lower() else DecisionType.EXIT
                            exit_decision_rec = DecisionRecord(
                                decision_type=exit_type,
                                token_address=decision.token_address,
                                chain=decision.chain,
                                signal_id=lot.signal_id,
                                token_symbol=getattr(lot, "token_symbol", "UNKNOWN"),
                                market_cap_usd=float(pool.spot_price_native_per_token * native_price * Decimal("1000000000")) if pool else 0.0,
                                liquidity_usd=float(pool.native_reserve * native_price * 2) if pool else 0.0,
                                volume_5m_usd=float(vol * native_price) if vol else 0.0,
                                volume_1h_usd=0.0,
                                rule_triggers={
                                    "ExitReason": str(decision.exit_reason.value if decision.exit_reason else "swap_exit"),
                                    "TokensSold": float(decision.tokens_to_sell),
                                    "EntryPrice": float(lot.entry_price),
                                    "ExitPrice": float(exit_fill.effective_price),
                                    "RealizedPnLUSD": float(exit_pnl_usd),
                                },
                                confidence_score=1.0,
                                reason=f"Dynamic exit on swap: {decision.exit_reason}. Entry={lot.entry_price:.8f}, Exit={exit_fill.effective_price:.8f}, PnL=${float(exit_pnl_usd):+.2f}",
                            )
                            await ledger.record_decision(exit_decision_rec)

                        realized_pnl = float(
                            getattr(self._metrics, "cumulative_realized_usd",
                            getattr(self._metrics, "total_realized_pnl_usd", 0.0))
                        )
                        self._feedback.parameter_tuner.update_from_performance(
                            win_rate_24h=self._metrics.win_rate_pct,
                            realized_pnl_usd=realized_pnl,
                            total_trades_24h=self._metrics.total_trades,
                        )

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
                        self._record_closed_trade_and_notify(
                            lot=lot,
                            exit_fill=exit_fill,
                            decision_reason=decision.exit_reason.value if decision.exit_reason else "swap_exit",
                            exit_pnl_usd=exit_pnl_usd,
                            pnl_native=pnl_native,
                        )

    def validate_signal_strength(
        self,
        signal_item: SignalEvent,
        min_strength: Optional[SignalStrength] = None,
    ) -> bool:
        """
        Validate whether the signal meets the minimum strength threshold before placing an order.
        SELL signals always pass for risk management / staged exits.
        """
        if signal_item.suggested_side == OrderSide.SELL:
            return True

        threshold = min_strength or getattr(self._cfg, "min_signal_strength", None)
        if threshold is None:
            return True

        rank = {
            SignalStrength.WEAK: 1,
            SignalStrength.MODERATE: 2,
            SignalStrength.STRONG: 3,
        }
        item_rank = rank.get(signal_item.strength, 0)
        req_rank = rank.get(threshold, 0)
        return item_rank >= req_rank

    async def _process_signal_queue(self) -> None:
        executor = self._executor
        ledger = self._ledger
        assert executor is not None and ledger is not None

        while True:
            item = await self._signal_q.get()

            if isinstance(item, ShutdownSentinel):
                logger.info("Signal processor received shutdown sentinel. Terminating queue.")
                return

            if isinstance(item, NewsEvent):
                # 1. Record recent news headline
                news_headline = item.raw_text.replace("\n", " ").strip()
                if len(news_headline) > 80:
                    news_headline = news_headline[:77] + "..."
                self._recent_news_events.append({
                    "source": item.source_channel,
                    "headline": news_headline,
                    "sentiment": item.sentiment_score,
                    "timestamp": item.timestamp,
                    "detected_mints": list(item.detected_mints),
                })

                # 2. Track whale alert if lookonchain, bubblemaps, or whale keyword
                lower_src = item.source_channel.lower()
                lower_text = item.raw_text.lower()
                if "lookonchain" in lower_src or "bubblemaps" in lower_src or "whale" in lower_text:
                    action_match = "BUY/ACCUMULATE" if item.sentiment_score > 0 else "SELL/DUMP" if item.sentiment_score < 0 else "ALERT"
                    for kw in ["bought", "accumulating", "sold", "dumped", "transferred", "swap"]:
                        if kw in lower_text:
                            action_match = kw.upper()
                            break
                    primary_tok = item.detected_mints[0] if item.detected_mints else "N/A"
                    self._recent_whale_alerts.append({
                        "source": item.source_channel,
                        "token": primary_tok,
                        "action": action_match,
                        "summary": news_headline[:80],
                        "timestamp": item.timestamp,
                    })

                # 3. Actionable check: contains detected mint and sentiment_score > 0.4
                if not item.is_actionable or not item.detected_mints:
                    continue

                gk = self._gatekeeper
                for mint in item.detected_mints:
                    chain = (
                        ChainIdentifier.SOLANA_MAINNET
                        if len(mint) > 42 or not mint.startswith("0x")
                        else ChainIdentifier.BASE_MAINNET
                    )

                    # Blacklist filter
                    if is_blacklisted_token(mint, chain):
                        self._record_gatekeeper_eval(mint, chain, "Blacklisted native/wrapped token", passed=False)
                        logger.debug("Skipping blacklisted token from news: %s", mint)
                        continue

                    # Deduplication check
                    if self.is_position_open(mint, chain):
                        logger.debug("Position already open for news token %s; skipping", mint[:10])
                        continue

                    # Preflight validation by Gatekeeper
                    report = None
                    if gk is not None:
                        try:
                            report = await gk.screen_token(
                                token_address=mint,
                                chain=chain,
                                pool_address="",
                            )
                        except Exception as exc:  # noqa: BLE001
                            logger.error("Security gatekeeper raised for news token %s: %s", mint[:10], exc)
                            continue

                        reason = self._extract_gatekeeper_rejection_reason(report)
                        self._record_gatekeeper_eval(mint, chain, reason, passed=report.passes_hard_gates)

                        if not report.passes_hard_gates:
                            logger.info("News token %s rejected by gatekeeper: %s", mint[:10], reason)
                            continue

                    if report is None:
                        from alpha_engine.models.enums import SecurityTier
                        report = SecurityReport(
                            token_address=mint,
                            chain=chain,
                            tier=SecurityTier.CLEAN,
                            is_honeypot=False,
                            buy_tax_bps=0,
                            sell_tax_bps=0,
                            lp_burned_ratio=1.0,
                            top10_concentration=0.1,
                            mint_authority_disabled=True,
                            verified_source_code=True,
                        )

                    # News Catalyst & Social Momentum Half-Life Decay:
                    # If incoming catalyst is older than 5 minutes (300s), classify as local liquidity top and forbid entry.
                    catalyst_age_s = (time.time() - item.timestamp) if item.timestamp else 0.0
                    if catalyst_age_s > 300.0:
                        logger.info(
                            "News catalyst %s is expired (age=%.1fs > 300s). Classifying as local liquidity top; suppressing entry.",
                            mint[:10],
                            catalyst_age_s,
                        )
                        continue

                    pool = self.get_or_create_initial_pool_state(chain, mint)
                    social_weight = self._feedback.get_social_weight() if hasattr(self, "_feedback") else 1.0
                    raw_sig = RawSignalEvent(
                        timestamp_ns=int(item.timestamp * 1e9) if item.timestamp else time.time_ns(),
                        chain=chain,
                        token_address=mint,
                        source=SignalSource.TELEGRAM_SCRAPER,
                        raw_text=item.raw_text,
                        sybil_channel_count=1,
                    )
                    packaged_signal = self._signal_gen.generate_social_signal(
                        raw_signal=raw_sig,
                        pool=pool,
                        report=report,
                        social_weight=social_weight,
                    )
                    if packaged_signal is not None:
                        if ledger is not None:
                            await ledger.record_signal(packaged_signal)
                        logger.info(
                            "Actionable News Signal generated [%s]: %s (sentiment: %.2f)",
                            packaged_signal.signal_id[:8],
                            mint[:10],
                            item.sentiment_score,
                        )
                        await self._signal_q.put(packaged_signal)
                continue

            signal_item: SignalEvent = item

            if signal_item.suggested_side == OrderSide.BUY:
                # 0. Trade Frequency & Cooldown Governor
                if self.is_trading_paused_by_governor():
                    remaining_cd = max(0.0, self._cooldown_until - time.time())
                    logger.warning(
                        "Trading Frequency Governor ACTIVE: Pausing BUY signal for %s (cooldown remaining: %.1f minutes).",
                        signal_item.token_address[:10],
                        remaining_cd / 60.0,
                    )
                    continue

                # 1. Daily Drawdown & Streak Circuit Breakers (Directive 1)
                freeze_dur = getattr(self._cfg, "circuit_breaker_freeze_duration_s", 3600.0)
                if self._metrics.is_circuit_breaker_active(freeze_duration_s=freeze_dur):
                    logger.warning(
                        "CIRCUIT BREAKER ACTIVE (%s) - Freezing BUY signal for %s",
                        self._metrics.circuit_breaker_reason,
                        signal_item.token_address[:10],
                    )
                    continue

                # 2. Concurrency & Exposure Locks (Directive 1)
                native_p = (
                    self._eth_price
                    if signal_item.chain == ChainIdentifier.BASE_MAINNET
                    else self._sol_price
                )
                equity_usd = self._current_equity_usd()
                equity_native = equity_usd / native_p if native_p > Decimal(0) else Decimal(0)
                preliminary_cost = equity_native * Decimal("0.015")

                can_open, reject_reason = self._position_book.can_open_position(
                    token_address=signal_item.token_address,
                    chain=signal_item.chain,
                    allocated_cost_native=preliminary_cost,
                    total_equity_native=equity_native,
                    pool_address=getattr(signal_item, "pool_address", "") or "",
                    narrative_cluster=getattr(signal_item, "narrative_cluster", None),
                )
                if not can_open:
                    logger.info(
                        "Dropping BUY signal for %s: %s",
                        signal_item.token_address[:10],
                        reject_reason,
                    )
                    continue

            if not self.validate_signal_strength(signal_item):
                logger.info(
                    "Signal for %s dropped: strength %s does not meet required threshold",
                    getattr(signal_item, "token_address", "")[:10],
                    getattr(signal_item, "strength", None),
                )
                continue

            if signal_item.pool_state is None or signal_item.pool_state.spot_price_native_per_token <= Decimal(0):
                signal_item.pool_state = self.get_or_create_initial_pool_state(
                    chain=signal_item.chain,
                    token_address=signal_item.token_address,
                    pool_address=getattr(signal_item, "pool_address", "") or "",
                )

            # AlphaSupervisor-AI Pre-Flight Audit & Dynamic Tuning
            ai_action_params: Optional[ActionParameters] = None
            if signal_item.suggested_side == OrderSide.BUY and getattr(self._cfg, "ai_supervisor_enabled", True):
                try:
                    ai_audit = await self._ai_supervisor.audit_signal(
                        signal=signal_item,
                        pool_state=signal_item.pool_state,
                        security_report=getattr(signal_item, "security_report", None),
                        recent_win_rate=self._metrics.win_rate_pct,
                        recent_profit_factor=self._metrics.profit_factor,
                    )
                    decision_str = str(ai_audit.decision.value if hasattr(ai_audit.decision, "value") else ai_audit.decision)
                    logger.info(
                        "AlphaSupervisor-AI Audit for %s: %s (confidence=%.2f) | %s",
                        signal_item.token_address[:10],
                        decision_str,
                        ai_audit.confidence_score,
                        ai_audit.wallet_audit.rationale,
                    )
                    if decision_str == AISupervisorDecisionEnum.PASS.value and getattr(self._cfg, "ai_strict_veto", True):
                        logger.info(
                            "AlphaSupervisor-AI VETO enforced for %s: %s",
                            signal_item.token_address[:10],
                            ai_audit.security_assessment.flags or ai_audit.wallet_audit.rationale,
                        )
                        self._record_gatekeeper_eval(
                            signal_item.token_address,
                            signal_item.chain,
                            f"AlphaSupervisor-AI Veto: {', '.join(ai_audit.security_assessment.flags) if ai_audit.security_assessment.flags else ai_audit.wallet_audit.rationale}",
                            passed=False,
                        )
                        continue

                    ai_action_params = ai_audit.action_parameters
                except Exception as exc:  # noqa: BLE001
                    logger.error("AlphaSupervisor-AI audit raised for %s: %s", signal_item.token_address[:10], exc)

            equity = self._current_equity_usd()
            fill = await executor.execute_signal(signal_item, portfolio_equity_usd=equity)
            if fill is None or fill.effective_price <= Decimal(0):
                if fill is not None and fill.effective_price <= Decimal(0):
                    logger.warning(
                        "Fill rejected for %s: zero or negative effective price: %s",
                        signal_item.token_address[:10],
                        fill.effective_price,
                    )
                continue

            realized_pnl_usd: Decimal | None = None

            if fill.side == OrderSide.BUY:
                self.record_governor_trade()
                initial_reserve = (
                    signal_item.pool_state.native_reserve
                    if signal_item.pool_state
                    else Decimal(0)
                )
                platform_name = getattr(fill, "platform", None) or resolve_trade_platform(
                    token_address=fill.token_address,
                    chain=fill.chain,
                    source=getattr(signal_item, "source", None),
                    execution_venue=getattr(signal_item, "execution_venue", None),
                    pool_address=getattr(signal_item, "pool_address", ""),
                )
                lot = self._position_book.open_lot(
                    fill,
                    signal_item.signal_id,
                    initial_pool_reserve_native=initial_reserve,
                    pool_address=getattr(signal_item, "pool_address", "") or "",
                    bonding_curve_mode=(platform_name == "Pump.fun"),
                    ai_hard_stop_loss_pct=ai_action_params.hard_stop_loss_pct if ai_action_params else None,
                    ai_trailing_stop_activation_pct=ai_action_params.trailing_stop_activation_pct if ai_action_params else None,
                    ai_time_exit_minutes=ai_action_params.time_exit_minutes if ai_action_params else None,
                    narrative_cluster=getattr(signal_item, "narrative_cluster", None),
                    exit_profile=getattr(signal_item, "exit_profile", ExitProfile.FAST_SNIPE),
                    base_market_cap=Decimal(str(getattr(signal_item, "base_market_cap", 0.0) or 0.0)),
                    token_age_hours=float(getattr(signal_item, "token_age_hours", 0.0) or 0.0),
                    consolidation_length_hours=float(getattr(signal_item, "consolidation_length_hours", 0.0) or 0.0),
                    volume_surge_multiplier=float(getattr(signal_item, "volume_surge_multiplier", 0.0) or 0.0),
                    net_buy_delta=float(getattr(signal_item, "net_buy_delta", 0.0) or 0.0),
                    platform=platform_name,
                )

                if self._ledger:
                    await self._ledger.record_lot_transition(
                        lot_id=lot.lot_id,
                        token_address=lot.token_address,
                        chain=lot.chain.value,
                        status=LotStatus.OPEN.value,
                    )
                if signal_item.chain == ChainIdentifier.BASE_MAINNET:
                    self._cash_eth -= fill.simulated_native_spent
                else:
                    self._cash_sol -= fill.simulated_native_spent

                unit = "ETH" if signal_item.chain == ChainIdentifier.BASE_MAINNET else "SOL"
                native_price = self._eth_price if signal_item.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
                p_str = self._format_price_clean(fill.effective_price, native_price, unit)
                t_str = f"{fill.token_address[:4]}..{fill.token_address[-4:]}"
                spent_val = float(fill.simulated_native_spent)
                tok_val = float(fill.tokens_acquired)

                logger.info(
                    "BUY fill [%s] | Platform=%s | Token=%s (%s) | price=%s | spent=%.4f %s | tokens=%.2f",
                    fill.order_id[:8],
                    platform_name,
                    fill.token_address[:10],
                    signal_item.chain.value,
                    p_str,
                    spent_val,
                    unit,
                    tok_val,
                )

                if getattr(signal_item, "exit_profile", None) == ExitProfile.REVIVAL_SWING:
                    buy_alert = (
                        f"🚀 **REVIVAL & CTO BREAKOUT DETECTED**\n\n"
                        f"• **Platform:** `{platform_name}`\n"
                        f"• **Token:** `{t_str}` (`{fill.token_address}`)\n"
                        f"• **Chain:** {signal_item.chain.value}\n"
                        f"• **Strategy Profile:** `REVIVAL_SWING`\n"
                        f"• **Buy Price:** `{p_str}`\n"
                        f"• **Spent:** `{spent_val:.4f} {unit}`\n"
                        f"• **Tokens Acquired:** `{tok_val:,.2f}`\n"
                        f"• **Token Age:** `{float(getattr(signal_item, 'token_age_hours', 0.0)):.1f}h`\n"
                        f"• **Consolidation Base:** `${float(getattr(signal_item, 'base_market_cap', 0.0)):,.0f}`\n"
                        f"• **Volume Surge:** `{float(getattr(signal_item, 'volume_surge_multiplier', 0.0)):.1f}x` | **Net Buy Delta:** `{float(getattr(signal_item, 'net_buy_delta', 0.0))*100:.1f}%`\n"
                        f"• **Order ID:** `{fill.order_id[:8]}`"
                    )
                else:
                    buy_alert = (
                        f"🟢 **BUY EXECUTED (Paper Trade)**\n\n"
                        f"• **Platform:** `{platform_name}`\n"
                        f"• **Token:** `{t_str}` (`{fill.token_address}`)\n"
                        f"• **Chain:** {signal_item.chain.value}\n"
                        f"• **Buy Price:** `{p_str}`\n"
                        f"• **Spent:** `{spent_val:.4f} {unit}`\n"
                        f"• **Tokens Acquired:** `{tok_val:,.2f}`\n"
                        f"• **Order ID:** `{fill.order_id[:8]}`"
                    )
                asyncio.create_task(self._broadcast_trade_alert(buy_alert))

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

                platform_name = getattr(fill, "platform", None) or resolve_trade_platform(
                    token_address=signal_item.token_address,
                    chain=signal_item.chain,
                    source=getattr(signal_item, "source", None),
                )

                logger.info(
                    "SELL fill [%s] | Platform=%s | Token=%s (%s) | PnL=%.4f USD | MDD=%.2f%% | WinRate=%.1f%%",
                    fill.order_id[:8],
                    platform_name,
                    fill.token_address[:10],
                    signal_item.chain.value,
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

                is_win = bool(realized_pnl_usd and realized_pnl_usd > 0)
                xp = fill.effective_price
                self._recent_closed_trades.append({
                    "token_address": fill.token_address,
                    "chain": fill.chain,
                    "platform": platform_name,
                    "side": "SELL",
                    "entry_price": xp,
                    "exit_price": xp,
                    "realized_pnl_usd": realized_pnl_usd or Decimal(0),
                    "realized_pnl_pct": 0.0,
                    "exit_reason": "signal_sell",
                    "duration_s": 0.0,
                    "is_win": is_win,
                    "timestamp": time.time(),
                })
                usd_sign = "+" if (realized_pnl_usd or 0) > 0 else ""
                icon = "🟢" if is_win else "🔴"
                unit = "ETH" if fill.chain == ChainIdentifier.BASE_MAINNET else "SOL"
                native_price = self._eth_price if fill.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
                xp_str = self._format_price_clean(xp, native_price, unit)
                sell_alert = (
                    f"{icon} **SELL EXECUTED (Paper Trade)**\n\n"
                    f"• **Platform:** `{platform_name}`\n"
                    f"• **Token:** `{fill.token_address[:4]}..{fill.token_address[-4:]}` (`{fill.token_address}`)\n"
                    f"• **Chain:** {fill.chain.value}\n"
                    f"• **Exit Price:** `{xp_str}`\n"
                    f"• **Realized PnL:** `{usd_sign}${float(realized_pnl_usd or 0):.2f}`\n"
                    f"• **Reason:** `signal_sell`"
                )
                asyncio.create_task(self._broadcast_trade_alert(sell_alert))

            trade_record = TradeRecord.from_fill(
                fill=fill,
                signal_id=signal_item.signal_id,
                realized_pnl_usd=realized_pnl_usd,
                platform=platform_name,
            )
            await ledger.record_trade(trade_record)

    async def _snapshot_scheduler(self) -> None:
        ledger = self._ledger
        if ledger is None:
            return

        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._cfg.snapshot_interval_s,
                )
                break
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

            if self._shutdown_event.is_set():
                break

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
            try:
                await ledger.record_snapshot(snapshot)
                logger.info(
                    "Snapshot | equity=$%.2f | MDD=%.2f%% | PF=%.2f | WR=%.1f%%",
                    float(equity),
                    self._metrics.max_drawdown_pct,
                    self._metrics.profit_factor,
                    self._metrics.win_rate_pct,
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("Snapshot recording skipped during shutdown: %s", exc)
                break

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
            except asyncio.CancelledError:
                break

            if self._shutdown_event.is_set():
                break

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

            # Calculate queue saturation percentage
            ingest_max = self._ingestion_q.maxsize if self._ingestion_q.maxsize > 0 else 1
            signal_max = self._signal_q.maxsize if self._signal_q.maxsize > 0 else 1
            ingest_saturation = (self._ingestion_q.qsize() / ingest_max) * 100
            signal_saturation = (self._signal_q.qsize() / signal_max) * 100

            if ingest_saturation > 80.0 or signal_saturation > 80.0:
                logger.warning(
                    "⚠️ BACKPRESSURE DETECTED: Ingestion Queue: %d%% | Signal Queue: %d%%. "
                    "Workers may be blocked by slow I/O or AI API rate limits.",
                    int(ingest_saturation), int(signal_saturation)
                )

    async def _pending_buffer_watchdog(self) -> None:
        """
        Monitors PendingLaunchBuffer: logs progress telemetry every 15s
        and sweeps expired tokens.
        """
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=5.0,
                )
                break
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

            if self._shutdown_event.is_set():
                break

            try:
                self._pending_launch_buffer.check_telemetry(interval_s=15.0)
                self._pending_launch_buffer.sweep_expired()
                self._wave2_buffer.sweep_expired()
                self._revival_buffer.sweep_expired()
            except Exception as exc:  # noqa: BLE001
                logger.error("Error in pending buffer watchdog: %s", exc)

    async def _missed_opportunity_watchdog(self) -> None:
        """
        Autonomous Learning Loop Watchdog:
        Periodically cross-references market gainers and staged tokens
        to record breakout feature vectors and analyze false negatives.
        """
        logger.info("Autonomous learning & missed opportunity watchdog started.")
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=60.0,
                )
                break
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

            if self._shutdown_event.is_set():
                break

            try:
                # 1. Learn from closed winning trades: record breakout feature vectors
                reflections = self._feedback.get_reflections(limit=20)
                for ref in reflections:
                    if ref.is_win and ref.roi_pct >= 50.0:
                        vec = PatternFeatureVector(
                            token_address=ref.token_address,
                            consolidation_duration_s=1800.0,
                            dip_depth_pct=40.0,
                            volume_surge_multiplier=3.0,
                            net_buy_delta=0.75,
                            top10_concentration=0.18,
                            liquidity_to_mc_ratio=0.25,
                            smart_wallet_inflows=15.0,
                            peak_gain_multiplier=1.0 + ref.roi_pct / 100.0,
                        )
                        self._feedback.pattern_store.add_pattern(vec)

                # 2. Check staged tokens against current prices to identify breakout patterns
                staged_tokens = self._wave2_buffer.get_all_staged()
                staged_list = list(staged_tokens.values()) if isinstance(staged_tokens, dict) else list(staged_tokens)
                for staged in staged_list:
                    init_p = getattr(staged, "initial_price", Decimal("0"))
                    highest_p = getattr(staged, "highest_price", getattr(staged, "peak_price", getattr(staged, "latest_price", Decimal("0"))))
                    if init_p > Decimal("0") and highest_p > init_p * Decimal("2.0"):
                        # Staged token reached >= 2x; capture market vector
                        gain_mult = float(highest_p / init_p)
                        if hasattr(staged, "extract_feature_vector"):
                            vec = staged.extract_feature_vector(peak_gain_multiplier=gain_mult)
                        else:
                            vec = PatternFeatureVector(
                                token_address=getattr(staged, "token_address", ""),
                                consolidation_duration_s=1800.0,
                                dip_depth_pct=40.0,
                                volume_surge_multiplier=float(getattr(staged, "volume_surge_multiplier", 2.0)),
                                net_buy_delta=float(getattr(staged, "net_buy_delta", 0.6)),
                                top10_concentration=float(getattr(staged, "top10_concentration", 0.2)),
                                liquidity_to_mc_ratio=0.20,
                                smart_wallet_inflows=5.0,
                                peak_gain_multiplier=gain_mult,
                            )
                        self._feedback.pattern_store.add_pattern(vec)

            except Exception as exc:  # noqa: BLE001
                logger.error("Error in missed opportunity watchdog: %s", exc)

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

        open_trades_info = []
        now_ns = time.time_ns()
        for lot in self._position_book.get_open_lots():
            pool = self._pool_registry.get(lot.token_address)
            if pool is None and lot.pool_address:
                pool = self._pool_registry.get(lot.pool_address)
            curr_p = pool.spot_price_native_per_token if pool else lot.entry_price
            native_p = self._eth_price if lot.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
            unrealized_usd = (lot.tokens_held * (curr_p - lot.entry_price)) * native_p
            pos_val_usd = float(lot.tokens_held * curr_p * native_p)
            dur_s = (now_ns - lot.open_timestamp_ns) / 1e9 if lot.open_timestamp_ns > 0 else 0.0
            pnl_pct = float((curr_p - lot.entry_price) / lot.entry_price * 100) if lot.entry_price > 0 else 0.0
            if lot.trailing_stop_active:
                if lot.trailing_stop_price >= lot.entry_price * Decimal("1.30"):
                    trailing_status = "LOCKED (+30%)"
                elif lot.trailing_stop_price >= lot.entry_price * Decimal("1.08"):
                    trailing_status = "LOCKED (+8%)"
                else:
                    trailing_status = "ACTIVE"
            else:
                trailing_status = "OFF"
            open_trades_info.append({
                "token_address": lot.token_address,
                "chain": lot.chain,
                "platform": getattr(lot, "platform", None) or resolve_trade_platform(token_address=lot.token_address, chain=lot.chain),
                "entry_price": lot.entry_price,
                "current_price": curr_p,
                "unrealized_pnl_usd": unrealized_usd,
                "unrealized_pnl_pct": pnl_pct,
                "position_value_usd": pos_val_usd,
                "duration_s": dur_s,
                "open_timestamp_ns": lot.open_timestamp_ns,
                "trailing_stop_status": trailing_status,
                "tokens_held": lot.tokens_held,
                "status": lot.status.value,
            })

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
            "recent_signals": list(self._recent_gatekeeper_evaluations),
            "recent_news": list(self._recent_news_events),
            "recent_whales": list(self._recent_whale_alerts),
            "open_trades": open_trades_info,
            "recent_closed_trades": list(self._recent_closed_trades),
            "ai_supervisor": self._ai_supervisor.get_status() if hasattr(self, "_ai_supervisor") else {},
        }


    async def _position_price_poller(self) -> None:
        """
        Adaptive High-Speed Exit Monitor Loop (Directive 2 & 4).
        Runs every 500ms. Evaluates dynamic bonding-curve exits:
        - Stale tick guard: ticks > 3.0s old halt exit triggers.
        - Slippage insolvency guard: curves unable to absorb lot without >15% collapse trigger emergency dump.
        - Velocity/time decay: duration > 120s and PnL < +3% triggers liquidation.
        - Hard SL (-8%): prioritized if price impact > 300 bps.
        - Trailing Stop: activates at +15%, locks at +8%, trails 5% from peak.
        - TP: +25% (scale out 50%), +50% (sell remaining 50%).
        - Monotonic state recovery: PENDING_BUY -> OPEN -> PENDING_SELL -> CLOSED -> SETTLED.
        """
        logger.info("Adaptive high-speed position price poller started (500ms tick interval).")
        while not self._shutdown_event.is_set():
            try:
                # Active high-speed tick evaluation running every 500ms
                await asyncio.sleep(0.5)
                if self._executor is None:
                    continue

                open_lots = self._position_book.get_open_lots()
                if not open_lots:
                    continue

                now_s = time.time()
                now_ns = time.time_ns()

                for lot in open_lots:
                    # Idempotent state recovery: check lots in PENDING_SELL
                    if lot.status == LotStatus.PENDING_SELL:
                        if (now_s - lot.pending_exit_timestamp) > 15.0:
                            logger.warning(
                                "Lot %s unconfirmed in PENDING_SELL for %.1fs (>15s). Re-polling on-chain status before retrying.",
                                lot.lot_id[:8],
                                now_s - lot.pending_exit_timestamp,
                            )
                            # Re-poll status and reset lot so it can retry
                            lot.status = LotStatus.OPEN
                            lot.pending_exit_timestamp = 0.0
                        else:
                            continue

                    pool = self._pool_registry.get(lot.token_address)
                    if pool is None:
                        pool = self.get_or_create_initial_pool_state(lot.chain, lot.token_address)

                    current_price = pool.spot_price_native_per_token
                    self._ai_supervisor.record_price_update(
                        lot.token_address,
                        current_price,
                        now_s,
                    )
                    # Pull tick timestamp; if not stamped on pool, default to now
                    tick_timestamp_s = getattr(pool, "timestamp_s", None) or now_s

                    grace_sec = getattr(self.config, "exit_grace_period_sec", 15.0)
                    decision = self._position_book.evaluate_lot_exit(
                        lot=lot,
                        current_price=current_price,
                        current_pool_reserve_native=pool.native_reserve,
                        tick_timestamp_s=tick_timestamp_s,
                        current_timestamp_ns=now_ns,
                        bonding_curve_mode=True,
                        exit_grace_period_sec=grace_sec,
                    )
                    if decision is not None and decision.should_exit:
                        # Idempotent state transition: OPEN -> PENDING_SELL
                        lot.status = LotStatus.PENDING_SELL
                        lot.pending_exit_timestamp = now_s
                        if self._ledger:
                            await self._ledger.record_lot_transition(
                                lot_id=lot.lot_id,
                                token_address=lot.token_address,
                                chain=lot.chain.value,
                                status=LotStatus.PENDING_SELL.value,
                                exit_reason=decision.exit_reason.value if decision.exit_reason else None,
                            )

                        exit_fill = await self._executor.execute_exit(
                            chain=decision.chain,
                            token_address=decision.token_address,
                            pool=pool,
                            tokens_to_sell=decision.tokens_to_sell,
                            reason=decision.exit_reason,
                            apply_drag=True,
                            portfolio_equity_usd=max(Decimal("1.0"), self._current_equity_usd()),
                            platform=getattr(lot, "platform", None),
                        )
                        if exit_fill is not None and exit_fill.effective_price > Decimal(0):
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

                            if self._ledger:
                                if lot.status == LotStatus.CLOSED:
                                    # Monotonic state transition: CLOSED -> SETTLED
                                    await self._ledger.record_lot_transition(
                                        lot_id=lot.lot_id,
                                        token_address=lot.token_address,
                                        chain=lot.chain.value,
                                        status=LotStatus.CLOSED.value,
                                        exit_reason=decision.exit_reason.value if decision.exit_reason else None,
                                        pnl_usd=exit_pnl_usd,
                                    )
                                    await self._ledger.record_lot_transition(
                                        lot_id=lot.lot_id,
                                        token_address=lot.token_address,
                                        chain=lot.chain.value,
                                        status=LotStatus.SETTLED.value,
                                        exit_reason=decision.exit_reason.value if decision.exit_reason else None,
                                        pnl_usd=exit_pnl_usd,
                                    )
                                else:
                                    # Partial exit: reset lot back to OPEN for subsequent ticks
                                    lot.status = LotStatus.OPEN
                                    lot.pending_exit_timestamp = 0.0
                                    await self._ledger.record_lot_transition(
                                        lot_id=lot.lot_id,
                                        token_address=lot.token_address,
                                        chain=lot.chain.value,
                                        status=LotStatus.OPEN.value,
                                        exit_reason=decision.exit_reason.value if decision.exit_reason else None,
                                        pnl_usd=exit_pnl_usd,
                                    )
                                rec = TradeRecord.from_fill(
                                    fill=exit_fill,
                                    signal_id=lot.signal_id,
                                    realized_pnl_usd=exit_pnl_usd,
                                    platform=getattr(lot, "platform", None) or getattr(exit_fill, "platform", None),
                                )
                                await self._ledger.record_trade(rec)
                                poller_exit_type = DecisionType.TRAIL_STOP if "trail" in str(decision.exit_reason).lower() else DecisionType.EXIT
                                poller_decision_rec = DecisionRecord(
                                    decision_type=poller_exit_type,
                                    token_address=decision.token_address,
                                    chain=decision.chain,
                                    signal_id=lot.signal_id,
                                    token_symbol=getattr(lot, "token_symbol", "UNKNOWN"),
                                    market_cap_usd=float(pool.spot_price_native_per_token * native_price * Decimal("1000000000")) if pool else 0.0,
                                    liquidity_usd=float(pool.native_reserve * native_price * 2) if pool else 0.0,
                                    volume_5m_usd=0.0,
                                    volume_1h_usd=0.0,
                                    rule_triggers={
                                        "ExitReason": str(decision.exit_reason.value if decision.exit_reason else "poller_exit"),
                                        "TokensSold": float(decision.tokens_to_sell),
                                        "EntryPrice": float(lot.entry_price),
                                        "ExitPrice": float(exit_fill.effective_price),
                                        "RealizedPnLUSD": float(exit_pnl_usd),
                                    },
                                    confidence_score=1.0,
                                    reason=f"High-speed poller exit: {decision.exit_reason}. Entry={lot.entry_price:.8f}, Exit={exit_fill.effective_price:.8f}, PnL=${float(exit_pnl_usd):+.2f}",
                                )
                                await self._ledger.record_decision(poller_decision_rec)

                            realized_pnl = float(
                                getattr(self._metrics, "cumulative_realized_usd",
                                getattr(self._metrics, "total_realized_pnl_usd", 0.0))
                            )
                            self._feedback.parameter_tuner.update_from_performance(
                                win_rate_24h=self._metrics.win_rate_pct,
                                realized_pnl_usd=realized_pnl,
                                total_trades_24h=self._metrics.total_trades,
                            )

                            reflection = TradeReflection.from_trade(
                                trade_id=exit_fill.order_id,
                                token_address=exit_fill.token_address,
                                chain=exit_fill.chain,
                                signal_source=SignalSource.DYNAMIC_EXIT,
                                entry_price=lot.entry_price,
                                exit_price=exit_fill.effective_price,
                                realized_pnl_usd=exit_pnl_usd,
                                realized_pnl_native=pnl_native,
                                time_to_fill_ms=exit_fill.fill_latency_ms,
                            )
                            await self._feedback.record_closed_trade(reflection)
                            self._record_closed_trade_and_notify(
                                lot=lot,
                                exit_fill=exit_fill,
                                decision_reason=decision.exit_reason.value if decision.exit_reason else "dynamic_exit",
                                exit_pnl_usd=exit_pnl_usd,
                                pnl_native=pnl_native,
                            )
                            platform_name = getattr(lot, "platform", None) or getattr(exit_fill, "platform", None) or resolve_trade_platform(
                                token_address=decision.token_address,
                                chain=decision.chain,
                            )
                            logger.info(
                                "DYNAMIC EXIT executed | Platform=%s | %s %s | reason=%s | exit_price=%s | PnL=%.4f USD | WR=%.1f%% | impact=%d bps",
                                platform_name,
                                decision.chain.value,
                                decision.token_address[:10],
                                decision.exit_reason.value if decision.exit_reason else "unknown",
                                exit_fill.effective_price,
                                float(exit_pnl_usd),
                                self._metrics.win_rate_pct,
                                exit_fill.price_impact_bps,
                            )
                        else:
                            # Exit failed or rejected by CPMM
                            logger.warning(
                                "Exit execution failed or returned zero price for lot %s (%s). Resetting status to OPEN.",
                                lot.lot_id[:8],
                                lot.token_address[:10],
                            )
                            lot.status = LotStatus.OPEN
                            lot.pending_exit_timestamp = 0.0
                            if self._ledger:
                                await self._ledger.record_lot_transition(
                                    lot_id=lot.lot_id,
                                    token_address=lot.token_address,
                                    chain=lot.chain.value,
                                    status=LotStatus.OPEN.value,
                                    exit_reason=None,
                                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in position price poller: %s", exc, exc_info=True)
                await asyncio.sleep(1.0)

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
            self.tracer.ledger = self._ledger
            self.tracer.start_worker()

            self._gatekeeper = SecurityGatekeeper(
                session=session,
                limiter=self._limiter,
                evm_rpc_url=cfg.alchemy_http_url,
                evm_router_address=cfg.aerodrome_router,
                weth_address=cfg.weth_address,
                enable_tier2=True,
                solana_rpc_url=cfg.solana_rpc_http or cfg.helius_http_url,
                negative_cache_ttl_s=getattr(cfg, "gatekeeper_negative_cache_ttl_s", 300.0),
                negative_cache_maxsize=getattr(cfg, "gatekeeper_negative_cache_maxsize", 5000),
            )

            self._executor = PaperExecutor(
                position_book=self._position_book,
                native_price_usd=cfg.eth_price_usd,
            )

            svm_failovers = []
            for rpc in getattr(cfg, "solana_fallback_rpcs", []):
                if rpc.startswith("https://"):
                    svm_failovers.append(rpc.replace("https://", "wss://"))
                elif rpc.startswith("http://"):
                    svm_failovers.append(rpc.replace("http://", "ws://"))
                elif rpc.startswith("wss://"):
                    svm_failovers.append(rpc)
            if "wss://api.mainnet-beta.solana.com" not in svm_failovers:
                svm_failovers.append("wss://api.mainnet-beta.solana.com")

            coordinator = IngestionCoordinator(
                evm_ws_url=cfg.alchemy_ws_url,
                svm_ws_url=cfg.helius_ws_url,
                pool_watchlist=pool_watchlist,
                pool_registry=svm_pool_registry,
                limiter=self._limiter,
                signal_queue=self._signal_q,
                telegram_api_id=cfg.telegram_api_id,
                telegram_api_hash=cfg.telegram_api_hash,
                telegram_session_name=cfg.telegram_session_name,
                telegram_session_string=getattr(cfg, "telegram_session_string", None),
                telegram_bot_token=cfg.telegram_bot_token,
                telegram_channels=cfg.telegram_channels,
                telegram_admin_ids=cfg.telegram_admin_ids,
                db_path=cfg.db_path,
                status_provider=self._get_engine_status,
                gatekeeper=self._gatekeeper,
                ai_supervisor=self._ai_supervisor,
                svm_failover_urls=svm_failovers,
                metrics_aggregator=self._metrics_aggregator,
            )

            self._ai_supervisor.set_ledger(self._ledger)
            self._ai_supervisor.set_pattern_store(self._feedback.pattern_store)
            self._ai_supervisor.set_parameter_tuner(self._feedback.parameter_tuner)

            async with coordinator:
                self._coordinator = coordinator
                self._telegram = coordinator.telegram_ingester

                async def _on_wave2_signal(sig: SignalEvent) -> None:
                    if not self.is_position_open(sig.token_address, sig.chain):
                        if self._ledger:
                            await self._ledger.record_signal(sig)
                        await self._signal_q.put(sig)

                self._wave2_buffer.register_signal_callback(_on_wave2_signal)

                async def _on_revival_signal(sig: SignalEvent) -> None:
                    if not self.is_position_open(sig.token_address, sig.chain):
                        if self._ledger:
                            await self._ledger.record_signal(sig)
                        await self._signal_q.put(sig)

                self._revival_buffer.register_signal_callback(_on_revival_signal)

                self._explainer = ConversationalSupervisor(
                    ledger=self._ledger,
                    position_book=self._position_book,
                    staging_buffer=self._wave2_buffer,
                    revival_buffer=self._revival_buffer,
                    parameter_tuner=self._feedback.parameter_tuner,
                    ai_supervisor=self._ai_supervisor,
                    metrics=self._metrics,
                )
                if self._telegram is not None:
                    if hasattr(self._telegram, "set_chat_explainer"):
                        self._telegram.set_chat_explainer(self._explainer)
                    if hasattr(self._telegram, "set_status_provider"):
                        self._telegram.set_status_provider(self._get_engine_status)
                    if hasattr(self._telegram, "set_execution_target"):
                        self._telegram.set_execution_target(self)
                    else:
                        self._telegram.execution_target = self
                    if hasattr(self._telegram, "set_ai_supervisor"):
                        self._telegram.set_ai_supervisor(self._ai_supervisor)
                    else:
                        self._telegram._ai_supervisor = self._ai_supervisor

                async def _on_launch_buffer_removal(token_addr: str) -> None:
                    if coordinator.svm_ingester:
                        await coordinator.svm_ingester.unsubscribe(token_addr)

                self._pending_launch_buffer.register_on_removal_callback(_on_launch_buffer_removal)

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
                poller_task = asyncio.create_task(
                    self._position_price_poller(), name="price_poller"
                )
                snapshot_task = asyncio.create_task(
                    self._snapshot_scheduler(), name="snapshot_scheduler"
                )
                heartbeat_task = asyncio.create_task(
                    self._heartbeat_task(), name="heartbeat_watchdog"
                )
                buffer_task = asyncio.create_task(
                    self._pending_buffer_watchdog(), name="pending_buffer_watchdog"
                )
                missed_opp_task = asyncio.create_task(
                    self._missed_opportunity_watchdog(), name="missed_opportunity_watchdog"
                )

                worker_tasks = [bridge_task, ingest_task, signal_task, poller_task, snapshot_task, heartbeat_task, buffer_task, missed_opp_task]
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
                    poller_task.cancel()
                    snapshot_task.cancel()
                    heartbeat_task.cancel()
                    buffer_task.cancel()
                    missed_opp_task.cancel()
                    shutdown_waiter.cancel()
                    await asyncio.gather(poller_task, snapshot_task, heartbeat_task, buffer_task, missed_opp_task, return_exceptions=True)

                    # 4. Flush traces and checkpoint SQLite database
                    try:
                        await self.tracer.stop_worker()
                    except Exception:
                        pass
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

    # Tradeable tokens on Base: BRETT
    BRETT = "0x532f27101965dd16442e59d40670faf5ebb142e4"
    AERO_BRETT_WETH = "0x94cc044155b9e1d88258525b6a37887e2261543b"

    evm_watchlist: list[tuple[str, str, str, int, int, str]] = [
        (AERO_WETH_USDC, WETH, USDC, 18, 6, WETH),
        (AERO_BRETT_WETH, BRETT, WETH, 18, 18, WETH),
    ]

    SOL_USDC_POOL = "8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj"
    SOL_MINT = "So11111111111111111111111111111111111111112"
    USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    # Tradeable token on Solana: BONK
    BONK_MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
    RAYDIUM_BONK_SOL_POOL = "HVbpJAhmZj5ffHGsqP71BBvsnoQTmnTabmMoDs29imEt"

    svm_registry: dict[str, tuple[str, str, int, int]] = {
        SOL_USDC_POOL: (SOL_MINT, USDC_MINT, 9, 6),
        RAYDIUM_BONK_SOL_POOL: (BONK_MINT, SOL_MINT, 5, 9),
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
        AERO_BRETT_WETH: PoolState(
            pool_address=AERO_BRETT_WETH,
            chain=ChainIdentifier.BASE_MAINNET,
            token_reserve=Decimal("100_000_000_000_000_000_000_000_000"),
            native_reserve=Decimal("50_000_000_000_000_000_000"),
            fee_numerator=3,
            fee_denominator=1000,
            last_updated_block=20_000_000,
            token_decimals=18,
            native_decimals=18,
        ),
        RAYDIUM_BONK_SOL_POOL: PoolState(
            pool_address=RAYDIUM_BONK_SOL_POOL,
            chain=ChainIdentifier.SOLANA_MAINNET,
            token_reserve=Decimal("50_000_000_000_00000"),
            native_reserve=Decimal("5_000_000_000_000"),
            fee_numerator=25,
            fee_denominator=10000,
            last_updated_block=280_000_000,
            token_decimals=5,
            native_decimals=9,
        ),
    }

    return evm_watchlist, svm_registry, seed_states


async def _async_main() -> None:
    cfg = EngineConfig()
    if cfg.asyncio_debug:
        os.environ["PYTHONASYNCIODEBUG"] = "1"
        logging.getLogger("asyncio").setLevel(logging.DEBUG)
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
