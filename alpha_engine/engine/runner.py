"""
alpha_engine.engine.runner — Async Main Loop, 24/7 Supervisor & System Orchestrator
===================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | asyncio | 24/7 Self-Healing Supervisor | Zero-Leak Production Grade
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path
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
from alpha_engine.ruflo.protocol import ConsensusProtocol, SwarmTopology
from alpha_engine.ruflo.swarm import RufloSwarmCoordinator
from alpha_engine.engine.experimentation import StrategyTournamentRegistry
from alpha_engine.execution.book import OpenLot, PositionBook, RunningMetrics
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.tracer import tracer
from alpha_engine.ingestion.coordinator import IngestionCoordinator
from alpha_engine.ingestion.market_oracle import MarketOracle
from alpha_engine.logging_config import setup_production_logging
from alpha_engine.math.cpmm import get_initial_bonding_curve_pool
from alpha_engine.models.ai import ActionParameters, AISupervisorDecisionEnum
from alpha_engine.models.decisions import DecisionRecord, DecisionType, PatternFeatureVector, RevivalPatternFeatureVector
from alpha_engine.ingestion.dex_metrics import DEXMetricsAggregator
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    ExitProfile,
    ExitStage,
    LotStatus,
    OrderSide,
    ScreenerSection,
    SecurityTier,
    SignalSource,
    SignalStrength,
    TokenRequestState,
    TradeExitReason,
    resolve_trade_platform,
)
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    PumpMintEvent,
    PumpSwapEvent,
    RawSignalEvent,
    ScreenerDiscoveryEvent,
    ShutdownSentinel,
    SignalEvent,
    SwapEvent,
)
from alpha_engine.models.news import NewsEvent, NewsSignalEvent
from alpha_engine.models.state import (
    ExitDecision,
    PoolState,
    PortfolioSnapshot,
    SecurityReport,
    TokenRequestRecord,
    TradeRecord,
)
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import is_blacklisted_token, derive_pump_fun_bonding_curve
from alpha_engine.ingestion.svm import is_valid_solana_pubkey
from alpha_engine.security.gatekeeper import SecurityGatekeeper
from alpha_engine.engine.strategy.screener_priority import (
    GraduatedProfileConfig,
    MoversProfileConfig,
    ScreenerDecision,
    ScreenerEvaluationResult,
    ScreenerPriorityEvaluator,
)

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
        self._signal_q: asyncio.Queue[
            SignalEvent | NewsEvent | ScreenerDiscoveryEvent | ShutdownSentinel
        ] = asyncio.Queue(maxsize=max_q)

        # Dedicated User Request Queue & Opportunity Processing Pipeline
        user_q_max = getattr(config, "max_user_request_queue_size", 200)
        self._user_request_queue: asyncio.Queue[TokenRequestRecord] = asyncio.Queue(maxsize=user_q_max)
        concurrent_analyses = getattr(config, "max_concurrent_analyses", 5)
        self._analysis_semaphore = asyncio.Semaphore(concurrent_analyses)
        self._waiting_for_capacity: list[TokenRequestRecord] = []
        self._token_request_tasks: set[asyncio.Task[Any]] = set()
        self._waiting_capacity_lock = asyncio.Lock()
        self._position_book.register_capacity_freed_callback(self._on_position_capacity_freed)

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
        
        # Strategy V2 Engines
        from alpha_engine.engine.strategy.risk_manager import RiskEngine
        from alpha_engine.engine.strategy.exit_engine import ExitEngine
        self.risk_engine = RiskEngine(config)
        self.exit_engine = ExitEngine(config)
        grad_cfg = GraduatedProfileConfig(
            min_liquidity_usd=Decimal(str(config.graduated_min_liquidity_usd)),
            max_price_impact_pct=config.graduated_max_price_impact_pct,
            min_volume_usd=Decimal(str(config.screener_min_volume_usd)),
        )
        movers_cfg = MoversProfileConfig(
            min_buy_pressure_ratio=0.55,
            max_extension_pct=config.movers_max_extension_pct,
            min_volume_usd=Decimal(str(config.screener_min_volume_usd)),
        )
        self.screener_evaluator = ScreenerPriorityEvaluator(
            config=config,
            gatekeeper=None,
            metrics_aggregator=self._metrics_aggregator,
            ledger=None,
            graduated_config=grad_cfg,
            movers_config=movers_cfg,
        )
        self._last_pnl_cycle_ts: float = time.time()

        # Strategy dynamic controls & Telegram interface state
        self.active_strategy: str = getattr(config, "active_strategy", "peak_trailing_50")
        self.trailing_stop_from_peak_pct: float = getattr(config, "trailing_stop_from_peak_pct", 0.50)
        self.custom_take_profit_pct: Optional[float] = getattr(config, "custom_take_profit_pct", 0.50)
        self.custom_stop_loss_pct: Optional[float] = getattr(config, "custom_stop_loss_pct", 0.50)
        self.hard_stop_loss_pct: float = getattr(config, "hard_stop_loss_pct", 0.20)
        self.early_rally_min_market_cap_usd: Decimal = Decimal(
            str(getattr(config, "early_rally_min_market_cap_usd", 3500.0))
        )
        self.auto_trade_enabled: bool = getattr(config, "auto_trade_enabled", True)
        self.screener_exclusive_focus: bool = getattr(config, "screener_exclusive_focus", False)
        self.screener_poll_interval_s: float = getattr(config, "screener_poll_interval_s", 25.0)
        self.manual_trade_amount: Optional[Decimal] = (
            Decimal(str(config.manual_trade_amount)) if getattr(config, "manual_trade_amount", None) is not None else None
        )
        self.manual_trade_currency: str = getattr(config, "manual_trade_currency", "SOL")
        self._load_persisted_trade_amount()

        self._explainer: Optional[ConversationalSupervisor] = None
        self._executor: PaperExecutor | None = None
        self._ledger: SQLiteLedger | None = None
        self.tracer = tracer
        self._gatekeeper: SecurityGatekeeper | None = None
        self._session: aiohttp.ClientSession | None = None
        self._market_oracle: Optional[MarketOracle] = None
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

        # Ruflo Multi-Agent Swarm Orchestrator
        self._swarm_coordinator: Optional[RufloSwarmCoordinator] = None
        if getattr(config, "ruflo_enabled", True):
            top_str = getattr(config, "ruflo_topology", "hierarchical")
            cons_str = getattr(config, "ruflo_consensus", "bft")
            topo = SwarmTopology.HIERARCHICAL
            if top_str.lower() == "mesh":
                topo = SwarmTopology.MESH
            elif top_str.lower() == "adaptive":
                topo = SwarmTopology.ADAPTIVE

            cons = ConsensusProtocol.BFT
            if cons_str.lower() == "raft":
                cons = ConsensusProtocol.RAFT
            elif cons_str.lower() == "gossip":
                cons = ConsensusProtocol.GOSSIP

            self._swarm_coordinator = RufloSwarmCoordinator(
                config=config,
                coordinator=None,
                gatekeeper=None,
                executor=None,
                position_book=self._position_book,
                ledger=None,
                ai_supervisor=self._ai_supervisor,
                feedback_engine=self._feedback,
                rpc_monitor=self._rpc_monitor,
                topology=topo,
                consensus=cons,
            )
            self._ai_supervisor.set_episodic_memory(self._swarm_coordinator.memory)

        # Multi-Strategy Tournament & Evolution Framework
        self._strategy_tournament: StrategyTournamentRegistry = StrategyTournamentRegistry(ledger=None)

    @property
    def strategy_tournament(self) -> StrategyTournamentRegistry:
        return self._strategy_tournament

    @property
    def swarm_coordinator(self) -> Optional[RufloSwarmCoordinator]:
        return self._swarm_coordinator

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

    @property
    def market_oracle(self) -> MarketOracle:
        """Access or lazily initialize the public market oracle."""
        if self._market_oracle is None:
            self._market_oracle = MarketOracle(session=self._session)
        return self._market_oracle

    def is_trading_paused_by_governor(self, now: Optional[float] = None) -> bool:
        """
        Check if trading frequency governor is currently enforcing cooldown.
        10 initial trades -> 4-6h mandatory cooldown -> Paced Mode (1 trade, then 3h cooldown, etc.).
        Resets after 24h positive PnL cycle or manual CLI reset.
        """
        if not getattr(self._cfg, "pacing_governor_enabled", False):
            return False

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
        if not getattr(self._cfg, "pacing_governor_enabled", False):
            return

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

    def get_strategy_status(self) -> dict[str, Any]:
        """Returns the current trading strategy status and parameters."""
        return {
            "active_strategy": getattr(self, "active_strategy", getattr(self._cfg, "active_strategy", "peak_trailing_50")),
            "trailing_stop_from_peak_pct": getattr(self, "trailing_stop_from_peak_pct", getattr(self._cfg, "trailing_stop_from_peak_pct", 0.50)),
            "custom_take_profit_pct": getattr(self, "custom_take_profit_pct", getattr(self._cfg, "custom_take_profit_pct", 0.50)),
            "custom_stop_loss_pct": getattr(self, "custom_stop_loss_pct", getattr(self._cfg, "custom_stop_loss_pct", 0.50)),
            "hard_stop_loss_pct": getattr(self, "hard_stop_loss_pct", getattr(self._cfg, "hard_stop_loss_pct", 0.20)),
            "auto_trade_enabled": getattr(self, "auto_trade_enabled", getattr(self._cfg, "auto_trade_enabled", True)),
            "screener_exclusive_focus": getattr(self, "screener_exclusive_focus", getattr(self._cfg, "screener_exclusive_focus", False)),
            "screener_poll_interval_s": getattr(self, "screener_poll_interval_s", getattr(self._cfg, "screener_poll_interval_s", 25.0)),
            "min_market_cap_usd": float(getattr(self, "min_market_cap_usd", getattr(self._cfg, "min_market_cap_usd", 10000.0))),
            "early_rally_min_market_cap_usd": float(getattr(self, "early_rally_min_market_cap_usd", getattr(self._cfg, "early_rally_min_market_cap_usd", 3500.0))),
            "manual_trade_amount": float(self.manual_trade_amount) if self.manual_trade_amount is not None else None,
            "manual_trade_currency": self.manual_trade_currency,
        }

    def _get_settings_file_path(self) -> Path:
        """Returns the path to the persistent runtime settings JSON file."""
        db_p = Path(getattr(self._cfg, "db_path", "paper_trading.db"))
        return db_p.parent / "bot_runtime_settings.json"

    def _save_persisted_trade_amount(self) -> None:
        """Persists the manual trade amount to local JSON storage and SQLite ledger."""
        try:
            p = self._get_settings_file_path()
            data: dict[str, Any] = {}
            if p.exists():
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                except Exception:
                    data = {}
            data["manual_trade_amount"] = str(self.manual_trade_amount) if self.manual_trade_amount is not None else None
            data["manual_trade_currency"] = self.manual_trade_currency
            data["updated_at"] = int(time.time())
            p.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("Failed to save persisted trade amount to JSON: %s", exc)

        if self._ledger is not None:
            try:
                amt_str = str(self.manual_trade_amount) if self.manual_trade_amount is not None else ""
                asyncio.create_task(self._ledger.set_setting("manual_trade_amount", amt_str))
                asyncio.create_task(self._ledger.set_setting("manual_trade_currency", self.manual_trade_currency))
            except Exception as exc:
                logger.debug("Async ledger setting save error: %s", exc)

    def _load_persisted_trade_amount(self) -> None:
        """Loads persisted manual trade amount from runtime JSON file."""
        try:
            p = self._get_settings_file_path()
            if p.exists():
                data = json.loads(p.read_text(encoding="utf-8"))
                amt = data.get("manual_trade_amount")
                curr = data.get("manual_trade_currency", "SOL")
                if amt is not None:
                    self.manual_trade_amount = Decimal(str(amt))
                    self.manual_trade_currency = str(curr).upper().strip()
                    logger.info("Loaded persisted manual trade amount: %s %s", self.manual_trade_amount, self.manual_trade_currency)
        except Exception as exc:
            logger.warning("Failed to load persisted trade amount: %s", exc)

    async def _sync_ledger_settings(self) -> None:
        """Syncs runtime settings from SQLite ledger on initialization."""
        if self._ledger is not None:
            try:
                val = await self._ledger.get_setting("manual_trade_amount")
                if val:
                    try:
                        data = json.loads(val)
                        amt = data.get("amount")
                        curr = data.get("currency", "SOL")
                    except Exception:
                        amt = val
                        curr = await self._ledger.get_setting("manual_trade_currency") or "SOL"
                    if amt:
                        self.manual_trade_amount = Decimal(str(amt))
                        self.manual_trade_currency = str(curr).upper().strip()
                        self._save_persisted_trade_amount()
                        logger.info("Synced manual trade amount from SQLite ledger: %s %s", self.manual_trade_amount, self.manual_trade_currency)
            except Exception as exc:
                logger.debug("Failed to sync ledger settings: %s", exc)

    def set_manual_trade_amount(
        self,
        amount: Optional[Decimal | float | str],
        currency: str = "SOL",
    ) -> dict[str, Any]:
        """
        Sets and permanently persists the manual trade amount to be used across all trade executions.
        Validates amount > 0 and allowed currency/bounds.
        """
        curr = currency.upper().strip() if currency else "SOL"
        if curr not in ("SOL", "USD", "ETH"):
            raise ValueError(f"Unsupported trade currency: {currency}. Must be SOL, USD, or ETH.")

        if amount is None:
            self.manual_trade_amount = None
            self.manual_trade_currency = curr
            self._save_persisted_trade_amount()
            logger.info("Manual trade amount cleared. Restoring dynamic risk sizing.")
            return self.get_manual_trade_amount()

        dec_amt = Decimal(str(amount))
        if dec_amt <= Decimal(0):
            raise ValueError(f"Trade amount must be strictly positive (>0), got {amount}")

        # Bounds validation
        if curr == "SOL":
            if dec_amt < Decimal("0.001"):
                raise ValueError("Minimum trade amount is 0.001 SOL to cover network transaction fees.")
            if dec_amt > Decimal("500.0"):
                raise ValueError("Maximum trade amount limit is 500.0 SOL.")
        elif curr == "USD":
            if dec_amt < Decimal("0.50"):
                raise ValueError("Minimum trade amount is $0.50 USD.")
            if dec_amt > Decimal("100000.0"):
                raise ValueError("Maximum trade amount limit is $100,000.0 USD.")
        elif curr == "ETH":
            if dec_amt < Decimal("0.0001"):
                raise ValueError("Minimum trade amount is 0.0001 ETH.")
            if dec_amt > Decimal("50.0"):
                raise ValueError("Maximum trade amount limit is 50.0 ETH.")

        self.manual_trade_amount = dec_amt
        self.manual_trade_currency = curr
        self._save_persisted_trade_amount()
        logger.info("Manual trade amount configured & persisted: %s %s", self.manual_trade_amount, self.manual_trade_currency)
        return self.get_manual_trade_amount()

    def get_manual_trade_amount(self) -> dict[str, Any]:
        """Returns the currently configured manual trade amount and status."""
        amt = float(self.manual_trade_amount) if self.manual_trade_amount is not None else None
        curr = self.manual_trade_currency
        sol_p = float(self._sol_price)
        eth_p = float(self._eth_price)
        usd_val = None
        sol_val = None
        if amt is not None:
            if curr == "USD":
                usd_val = amt
                sol_val = amt / sol_p if sol_p > 0 else 0.0
            elif curr == "SOL":
                sol_val = amt
                usd_val = amt * sol_p
            elif curr == "ETH":
                usd_val = amt * eth_p
                sol_val = usd_val / sol_p if sol_p > 0 else 0.0

        return {
            "is_manual": amt is not None,
            "amount": amt,
            "currency": curr,
            "manual_trade_amount": str(self.manual_trade_amount) if self.manual_trade_amount is not None else None,
            "manual_trade_currency": curr,
            "usd_equivalent": usd_val,
            "sol_equivalent": sol_val,
            "sol_price_usd": sol_p,
            "eth_price_usd": eth_p,
        }

    def get_balance_summary(self) -> dict[str, Any]:
        """Returns available trading balances, portfolio exposure, and purchasing capacity."""
        cash_sol = float(self._cash_sol)
        cash_eth = float(self._cash_eth)
        sol_p = float(self._sol_price)
        eth_p = float(self._eth_price)
        total_cash_usd = cash_sol * sol_p + cash_eth * eth_p
        total_equity_usd = float(self._current_equity_usd())

        open_positions = self._position_book.open_position_count()
        open_exposure_native = Decimal(0)
        for lots in self._position_book._lots.values():
            for lot in lots:
                if lot.tokens_held > 0 and lot.status in (LotStatus.OPEN, LotStatus.PENDING_BUY, LotStatus.PENDING_SELL):
                    open_exposure_native += lot.cost_basis_native
        open_exposure_usd = float(open_exposure_native * self._sol_price)

        manual_info = self.get_manual_trade_amount()
        trade_amt_sol = manual_info.get("sol_equivalent") or (0.1 if manual_info.get("amount") is None else manual_info.get("amount"))
        fee_sol = 0.005
        cost_per_trade_sol = trade_amt_sol + fee_sol
        trades_possible = int(cash_sol // cost_per_trade_sol) if cost_per_trade_sol > 0 else 0

        return {
            "cash_sol": cash_sol,
            "cash_eth": cash_eth,
            "cash_sol_usd": cash_sol * sol_p,
            "cash_eth_usd": cash_eth * eth_p,
            "total_cash_usd": total_cash_usd,
            "total_equity_usd": total_equity_usd,
            "open_positions_count": open_positions,
            "open_exposure_usd": open_exposure_usd,
            "trade_amount": manual_info.get("amount"),
            "trade_currency": manual_info.get("currency"),
            "trade_amount_usd": manual_info.get("usd_equivalent"),
            "trades_possible": trades_possible,
            "sol_price_usd": sol_p,
            "eth_price_usd": eth_p,
        }

    def set_trading_strategy(
        self,
        strategy_name: Optional[str] = None,
        trailing_stop_from_peak_pct: Optional[float] = None,
        custom_take_profit_pct: Optional[float] = None,
        custom_stop_loss_pct: Optional[float] = None,
        hard_stop_loss_pct: Optional[float] = None,
        auto_trade_enabled: Optional[bool] = None,
        screener_exclusive_focus: Optional[bool] = None,
        min_market_cap_usd: Optional[float] = None,
        manual_trade_amount: Optional[Decimal | float | str] = None,
        manual_trade_currency: Optional[str] = None,
    ) -> dict[str, Any]:
        """Dynamically configures the bot's trading strategy, trade size, and operational parameters."""
        if strategy_name is not None:
            strat_lower = strategy_name.lower().strip()
            if strat_lower in ("peak_trailing_50", "peak50", "trail50"):
                self.active_strategy = "peak_trailing_50"
                self.trailing_stop_from_peak_pct = 0.50
                self.custom_take_profit_pct = 0.50
                self.custom_stop_loss_pct = 0.50
                self.hard_stop_loss_pct = 0.20
            elif strat_lower in ("peak_trailing_60", "peak60", "trail60"):
                self.active_strategy = "peak_trailing_60"
                self.trailing_stop_from_peak_pct = 0.60
                self.custom_take_profit_pct = 0.60
                self.custom_stop_loss_pct = 0.60
                self.hard_stop_loss_pct = 0.20
            elif strat_lower in ("early_rally", "early_rally_breakout", "rally"):
                self.active_strategy = "early_rally"
                self.trailing_stop_from_peak_pct = 0.28
                self.custom_take_profit_pct = 0.50
                self.custom_stop_loss_pct = 0.15
                self.hard_stop_loss_pct = 0.15
            elif strat_lower in ("default", "legacy", "classic", "ai", "ai_adaptive"):
                self.active_strategy = "default"
                self.trailing_stop_from_peak_pct = None
                self.custom_take_profit_pct = None
                self.custom_stop_loss_pct = None
                self.hard_stop_loss_pct = 0.20
            else:
                self.active_strategy = strategy_name
                self.hard_stop_loss_pct = 0.20

        if hard_stop_loss_pct is not None:
            self.hard_stop_loss_pct = float(hard_stop_loss_pct)
        if trailing_stop_from_peak_pct is not None:
            self.trailing_stop_from_peak_pct = float(trailing_stop_from_peak_pct)
        if custom_take_profit_pct is not None:
            self.custom_take_profit_pct = float(custom_take_profit_pct)
        if custom_stop_loss_pct is not None:
            self.custom_stop_loss_pct = float(custom_stop_loss_pct)
        if auto_trade_enabled is not None:
            self.auto_trade_enabled = bool(auto_trade_enabled)
        if screener_exclusive_focus is not None:
            self.screener_exclusive_focus = bool(screener_exclusive_focus)
        if min_market_cap_usd is not None:
            self.min_market_cap_usd = Decimal(str(min_market_cap_usd))
            self._cfg.min_market_cap_usd = Decimal(str(min_market_cap_usd))
        if manual_trade_amount is not None:
            self.set_manual_trade_amount(manual_trade_amount, manual_trade_currency or self.manual_trade_currency)

        logger.info(
            "Strategy updated dynamically: active_strategy=%s, trailing_stop=%.1f%%, tp=%.1f%%, auto_trade=%s, screener_focus=%s, min_mcap=$%s, trade_amount=%s %s",
            self.active_strategy,
            (self.trailing_stop_from_peak_pct or 0.0) * 100.0,
            (self.custom_take_profit_pct or 0.0) * 100.0,
            self.auto_trade_enabled,
            self.screener_exclusive_focus,
            getattr(self, "min_market_cap_usd", getattr(self._cfg, "min_market_cap_usd", 10000.0)),
            self.manual_trade_amount,
            self.manual_trade_currency,
        )
        return self.get_strategy_status()

    @property
    def explainer(self) -> Optional[ConversationalSupervisor]:
        return self._explainer

    @property
    def rpc_monitor(self) -> RPCHealthMonitor:
        return self._rpc_monitor

    @property
    def launch_buffer(self) -> PendingLaunchBuffer:
        return self._pending_launch_buffer

    @property
    def position_book(self) -> PositionBook:
        return self._position_book

    def reset_circuit_breaker(self) -> None:
        """Manually or programmatically reset circuit breakers and zero out loss streaks."""
        if hasattr(self, "_metrics") and self._metrics is not None:
            self._metrics.reset_circuit_breaker()

    async def force_close_all_open_lots(self) -> int:
        """
        Force-closes all active open lots immediately, applying exit fills,
        updating the ledger, and freeing capacity.
        """
        open_lots = self._position_book.get_open_lots()
        if not open_lots:
            logger.info("No open lots to force-close.")
            return 0

        closed_count = 0
        now_s = time.time()
        now_ns = time.time_ns()
        for lot in open_lots:
            pool = self._pool_registry.get(lot.token_address) or (self._pool_registry.get(lot.pool_address) if lot.pool_address else None)
            if pool is None or getattr(pool, "last_updated_block", 0) <= 0:
                pool = PoolState(
                    pool_address=lot.pool_address or lot.token_address,
                    chain=lot.chain,
                    token_reserve=Decimal("1000000000"),
                    native_reserve=Decimal("0.0001"),
                    fee_numerator=25,
                    fee_denominator=10000,
                    last_updated_block=1000,
                    timestamp_s=now_s,
                    token_decimals=6 if lot.chain == ChainIdentifier.SOLANA_MAINNET else 18,
                    native_decimals=9 if lot.chain == ChainIdentifier.SOLANA_MAINNET else 18,
                )
                self._pool_registry.set(lot.token_address, pool)

            curr_price = pool.spot_price_native_per_token if pool else lot.entry_price
            decision = ExitDecision(
                lot_id=lot.lot_id,
                token_address=lot.token_address,
                chain=lot.chain,
                should_exit=True,
                exit_reason=TradeExitReason.MANUAL_CLOSE,
                exit_stage=ExitStage.SL,
                tokens_to_sell=lot.tokens_held,
                current_price=curr_price,
                pnl_estimate_native=lot.tokens_held * (curr_price - lot.entry_price),
                prioritized=True,
                action="CLOSE_ALL",
            )
            if self._executor is not None:
                exit_fill = await self._executor.execute_exit(
                    chain=decision.chain,
                    token_address=decision.token_address,
                    pool=pool,
                    tokens_to_sell=decision.tokens_to_sell,
                    reason=decision.exit_reason,
                    apply_drag=True,
                    portfolio_equity_usd=max(Decimal("1.0"), self._current_equity_usd()),
                    platform=getattr(lot, "platform", None),
                    min_fill_timestamp_ns=lot.open_timestamp_ns,
                )
                if exit_fill is not None:
                    pnl_native, _ = self._position_book.apply_exit_decision(
                        decision, sell_price=exit_fill.effective_price, net_native_received=exit_fill.simulated_native_spent
                    )
                    native_price = self._eth_price if decision.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
                    exit_pnl_usd = (pnl_native * native_price).quantize(Decimal("0.01"))
                    if decision.chain == ChainIdentifier.BASE_MAINNET:
                        self._cash_eth += exit_fill.simulated_native_spent
                    else:
                        self._cash_sol += exit_fill.simulated_native_spent
                    new_equity = self._current_equity_usd()
                    self._metrics.record_closed_trade(
                        realized_pnl_usd=exit_pnl_usd,
                        gas_cost_usd=Decimal(0),
                        current_equity_usd=new_equity,
                        is_new_launch=getattr(lot, "is_new_launch", False),
                    )
                    self._position_book.set_token_cooldown(lot.token_address, duration_s=300.0)
                    if self._ledger:
                        await self._ledger.record_lot_transition(
                            lot_id=lot.lot_id,
                            token_address=lot.token_address,
                            chain=lot.chain.value,
                            status=LotStatus.CLOSED.value,
                            exit_reason=TradeExitReason.MANUAL_CLOSE.value,
                            pnl_usd=exit_pnl_usd,
                        )
                        await self._ledger.record_lot_transition(
                            lot_id=lot.lot_id,
                            token_address=lot.token_address,
                            chain=lot.chain.value,
                            status=LotStatus.SETTLED.value,
                            exit_reason=TradeExitReason.MANUAL_CLOSE.value,
                            pnl_usd=exit_pnl_usd,
                        )
                        rec = TradeRecord.from_fill(
                            exit_fill,
                            signal_id=lot.signal_id or "force_close_all",
                            realized_pnl_usd=exit_pnl_usd,
                            exit_reason=TradeExitReason.MANUAL_CLOSE,
                        )
                        await self._ledger.record_trade(rec)
                    closed_count += 1
            else:
                self._position_book.remove_lot(lot.chain.value, lot.token_address, lot.lot_id)
                if self._ledger:
                    await self._ledger.mark_token_closed(lot.token_address, reason="MANUAL_CLOSE", pnl_usd=0.0)
                closed_count += 1

        self._position_book.notify_capacity_freed()
        logger.info("Force-closed %d open lots successfully.", closed_count)
        return closed_count

    async def reset_paper_trading_ledger(self) -> None:
        """
        Wipes paper trading ledger history: clears trades, lot lifecycle, and resets
        portfolio equity to configured starting cash ($8,300 USD / initial native).
        """
        open_lots = self._position_book.get_open_lots()
        for lot in open_lots:
            self._position_book.remove_lot(lot.chain.value, lot.token_address, lot.lot_id)

        if self._ledger is not None:
            await self._ledger.clear_all_trades_and_lots()

        self._metrics.reset()
        self._metrics.reset_circuit_breaker()
        self._cash_eth = Decimal("1.0")
        self._cash_sol = Decimal("50.0")
        self._position_book.notify_capacity_freed()
        logger.info("Paper trading ledger and position book reset to pristine initial state.")

    @staticmethod
    def _is_new_launch_signal(signal_item: SignalEvent) -> bool:
        # Early Rally, Revival, and consolidated Wave-2 tokens are not brand new launches
        exit_prof = getattr(signal_item, "exit_profile", None)
        if exit_prof in (ExitProfile.REVIVAL_SWING, ExitProfile.EARLY_RALLY_SWING) or str(exit_prof or "").lower() in ("revival_swing", "early_rally_swing"):
            return False
        sig_src = getattr(signal_item, "source", None)
        if sig_src in (SignalSource.REVIVAL_BREAKOUT, SignalSource.WAVE2_BREAKOUT, SignalSource.EARLY_RALLY_BREAKOUT, "revival_breakout", "wave2_breakout", "early_rally_breakout"):
            return False
        if getattr(signal_item, "source", None) in (SignalSource.SCREENER_GRADUATED, SignalSource.SCREENER_MOVERS, "screener_graduated", "screener_movers"):
            return False
        if getattr(signal_item, "pattern", None) in ("screener_graduated", "screener_movers"):
            return False
        if getattr(signal_item, "token_age_hours", 0.0) and signal_item.token_age_hours > 0.5:
            return False
        if getattr(signal_item, "token_age_s", 0.0) and signal_item.token_age_s > 1800.0:
            return False
        
        # Token is from an established DEX swap and not on Pump.fun
        tok_addr = getattr(signal_item, "token_address", "").lower()
        sig_source = getattr(signal_item, "source", None)
        if sig_source == SignalSource.DEX_SWAP and not tok_addr.endswith("pump"):
            return False

        return True

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
        dur_s = max(0.0, (time.time_ns() - lot.open_timestamp_ns) / 1e9) if getattr(lot, "open_timestamp_ns", 0) > 0 else 0.0

        platform_name = getattr(lot, "platform", None) or getattr(exit_fill, "platform", None) or resolve_trade_platform(
            token_address=lot.token_address,
            chain=lot.chain,
        )

        is_partial = getattr(lot, "status", None) != LotStatus.CLOSED
        side_str = "SELL (PARTIAL)" if is_partial else "SELL"
        exit_label = f"{decision_reason} (partial)" if is_partial and "partial" not in decision_reason.lower() else decision_reason

        self._recent_closed_trades.append({
            "token_address": lot.token_address,
            "chain": lot.chain,
            "platform": platform_name,
            "is_new_launch": getattr(lot, "is_new_launch", False),
            "side": side_str,
            "entry_price": ep,
            "exit_price": xp,
            "realized_pnl_usd": exit_pnl_usd,
            "realized_pnl_pct": pct,
            "exit_reason": exit_label,
            "duration_s": dur_s,
            "is_win": is_win,
            "timestamp": time.time(),
        })

        d_reason_lower = str(decision_reason).lower()
        if "emergency_drain" in d_reason_lower or "rug_liquidity_drain" in d_reason_lower:
            status_icon = "⚠️"
            title = f"EMERGENCY EXIT: LIQUIDITY DRAIN ({pct:+.2f}%)"
        elif "emergency_honeypot" in d_reason_lower:
            status_icon = "⚠️"
            title = f"EMERGENCY EXIT: HONEYPOT ({pct:+.2f}%)"
        elif "slippage_insolvency" in d_reason_lower:
            status_icon = "⚠️"
            title = f"EMERGENCY EXIT: SLIPPAGE INSOLVENCY ({pct:+.2f}%)"
        elif "timeout" in d_reason_lower:
            status_icon = "⏱️"
            title = f"TIME EXPIRATION EXIT ({pct:+.2f}%)"
        elif is_partial:
            status_icon = "🟢"
            title = f"PARTIAL TAKE-PROFIT (+{pct:.2f}%)"
        elif is_win:
            status_icon = "🟢"
            title = f"TAKE-PROFIT (+{pct:.2f}%)"
        else:
            status_icon = "🔴"
            title = f"STOP-LOSS ({pct:.2f}%)"
        unit = "ETH" if lot.chain == ChainIdentifier.BASE_MAINNET else "SOL"
        native_price = self._eth_price if lot.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
        ep_str = self._format_price_clean(ep, native_price, unit)
        xp_str = self._format_price_clean(xp, native_price, unit)
        t_str = f"{lot.token_address[:4]}..{lot.token_address[-4:]}"
        usd_sign = "+" if exit_pnl_usd > 0 else ""
        chain_val = lot.chain.value if hasattr(lot.chain, "value") else str(lot.chain)
        exit_prof = getattr(lot, "exit_profile", None)
        is_early_rally = exit_prof == ExitProfile.EARLY_RALLY_SWING or getattr(lot, "strategy_name", "") in ("early_rally", "early_rally_breakout")
        is_swing = exit_prof == ExitProfile.REVIVAL_SWING

        logger.info(
            "%s | Platform=%s | Token=%s (%s) | Entry=%s | Exit=%s | PnL=%+.2f%% (%s$%.2f) | Reason=%s | Duration=%.1fs",
            "PARTIAL EXIT" if is_partial else "CLOSED TRADE",
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

        if is_early_rally:
            peak_p = getattr(lot, "peak_price", ep)
            peak_roi = float((peak_p - ep) / ep * 100) if ep > 0 else 0.0
            exit_msg = (
                f"{status_icon} **EARLY RALLY SWING EXIT ({title})**\n\n"
                f"• **Platform:** `{platform_name}`\n"
                f"• **Token:** `{t_str}` (`{lot.token_address}`)\n"
                f"• **Chain:** {chain_val}\n"
                f"• **Strategy Profile:** `EARLY_RALLY_SWING`\n"
                f"• **Entry Price:** `{ep_str}`\n"
                f"• **Exit Price:** `{xp_str}`\n"
                f"• **Peak Price:** `{self._format_price_clean(peak_p, native_price, unit)}` (Peak: `{peak_roi:+.1f}%`)\n"
                f"• **Return %:** `{pct:+.2f}%` ({usd_sign}${float(exit_pnl_usd):.2f})\n"
                f"• **Exit Reason:** `{decision_reason}`\n"
                f"• **Holding Time:** `{dur_s / 60.0:.1f}m`"
            )
        elif is_swing:
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
                entry_feature_vector=getattr(lot, "entry_feature_vector", []),
            )
            asyncio.create_task(self._ai_supervisor.reflect_on_trade(reflection))

        if hasattr(self, "_swarm_coordinator") and self._swarm_coordinator:
            asyncio.create_task(
                self._swarm_coordinator.supervisor_agent.tool_reflect_trade(
                    trade_id=str(getattr(exit_fill, "order_id", "")),
                    token_address=lot.token_address,
                    realized_pnl_usd=float(exit_pnl_usd),
                    roi_pct=pct,
                    chain=lot.chain.value if hasattr(lot.chain, "value") else str(lot.chain),
                    feature_vector=getattr(lot, "entry_feature_vector", None),
                )
            )

    def _propagate_dynamic_hyperparameters(self, params: Any) -> None:
        """
        Dynamically propagate tuned hyperparameters to live staging buffers,
        strategy exit rules, and position sizing governors.
        """
        if params is None:
            return
        if hasattr(self, "_wave2_buffer") and self._wave2_buffer:
            self._wave2_buffer.set_hyperparameters(
                volume_surge_multiplier_k=getattr(params, "wave2_surge_k", None),
                min_net_buy_delta_pct=getattr(params, "wave2_net_buy_delta_pct", None),
            )
        if hasattr(self, "_revival_buffer") and self._revival_buffer:
            self._revival_buffer.set_hyperparameters(
                volume_surge_multiplier_k=getattr(params, "wave2_surge_k", None),
                min_net_buy_delta_pct=getattr(params, "wave2_net_buy_delta_pct", None),
            )
        # Adapt hard stop-loss within safe bounds
        raw_sl = getattr(params, "hard_stop_loss_pct", None)
        if raw_sl is not None:
            safe_sl = max(0.08, min(0.25, abs(float(raw_sl)) / 100.0))
            self.hard_stop_loss_pct = safe_sl


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
        # Mark-to-market value of open positions without double-counting realized PnL
        open_lots = self._position_book.get_all_open_lots()
        unrealized_positions_usd = Decimal(0)
        for lot in open_lots:
            pool = self._pool_registry.get(lot.token_address, expected_token=lot.token_address)
            if pool is None and lot.pool_address:
                pool = self._pool_registry.get(lot.pool_address, expected_token=lot.token_address)
            native_p = self._eth_price if lot.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
            if pool and getattr(pool, "last_updated_block", 0) > 0 and pool.spot_price_native_per_token > Decimal(0):
                unrealized_positions_usd += lot.tokens_remaining * pool.spot_price_native_per_token * native_p
            else:
                unrealized_positions_usd += lot.tokens_remaining * lot.entry_price * native_p
        return cash_usd + unrealized_positions_usd

    def _total_equity_usd(self) -> Decimal:
        return self._current_equity_usd()

    def is_position_open(
        self,
        token_address: str,
        chain: ChainIdentifier | None = None,
    ) -> bool:
        return self._position_book.is_position_open(token_address, chain=chain)

    async def manual_enter_position(
        self,
        token_address: str,
        chain: Optional[ChainIdentifier] = None,
        amount_native: Optional[Decimal] = None,
        originating_channel: Optional[str] = None,
    ) -> tuple[bool, str, Optional[Any]]:
        """
        Manually triggers entry into a token position (via Telegram /buy or token page button).
        Bypasses market cap and screener filters while verifying security gates, balance, and exposure.
        """
        clean_addr = token_address.strip()
        chain_id = chain or (
            ChainIdentifier.BASE_MAINNET if clean_addr.startswith("0x") else ChainIdentifier.SOLANA_MAINNET
        )

        if self.is_position_open(clean_addr, chain=chain_id):
            return False, "يوجد مركز مفتوح بالفعل لهذه العملة (Position already open)", None

        # Pre-check cash balance if amount_native is provided or if default manual_trade_amount is set
        native_p = self._eth_price if chain_id == ChainIdentifier.BASE_MAINNET else self._sol_price
        unit = "ETH" if chain_id == ChainIdentifier.BASE_MAINNET else "SOL"
        available_cash = self._cash_eth if chain_id == ChainIdentifier.BASE_MAINNET else self._cash_sol
        fee_est = Decimal("0.002") if chain_id == ChainIdentifier.BASE_MAINNET else Decimal("0.005")

        req_amount = amount_native
        if req_amount is None and self.manual_trade_amount is not None and self.manual_trade_amount > Decimal(0):
            if self.manual_trade_currency == "USD":
                req_amount = self.manual_trade_amount / native_p if native_p > Decimal(0) else Decimal(0)
            else:
                req_amount = self.manual_trade_amount

        if req_amount is not None and req_amount > Decimal(0):
            if available_cash < (req_amount + fee_est):
                return False, f"رصيد غير كافٍ ({available_cash:.4f} {unit} المتاح < {req_amount + fee_est:.4f} {unit} المطلوب)", None

        # Pre-check position capacity atomically
        can_open, reject_reason = self._position_book.can_open_position(
            token_address=clean_addr,
            chain=chain_id,
            is_manual_size=True,
        )
        if not can_open:
            return False, f"⚠️ تعذر فتح صفقة جديدة: {reject_reason}", None

        # Reserve position slot atomically
        reserved = self._position_book.reserve_position_slot(token_address=clean_addr)
        if not reserved:
            return False, f"⚠️ Active position count ({self._position_book.effective_active_position_count()}) reached MAX_ACTIVE_POSITIONS limit ({self._position_book.max_active_positions}).", None

        # 1. Evaluate security gates via gatekeeper
        report = None
        if hasattr(self, "_gatekeeper") and self._gatekeeper is not None:
            try:
                report = await self._gatekeeper.evaluate_token(
                    token_address=clean_addr,
                    chain=chain_id,
                )
            except Exception as exc:
                logger.warning("Manual buy gatekeeper error for %s: %s", clean_addr[:10], exc)

        if report is not None and not getattr(report, "passes_hard_gates", True):
            self._position_book.release_position_slot(token_address=clean_addr)
            reason = self._extract_gatekeeper_rejection_reason(report)
            return False, f"فشل الفحص الأمني للعملة (Security Veto): {reason}", None

        if report is None:
            report = SecurityReport(
                token_address=clean_addr,
                chain=chain_id,
                tier=SecurityTier.CLEAN,
                is_honeypot=False,
                is_rug=False,
                score=100.0,
                flags=[],
                buy_tax_bps=0,
                sell_tax_bps=0,
                checked_at_ts=int(time.time()),
                passes_hard_gates=True,
            )

        # 2. Get or create pool state
        pool = self._pool_registry.get(clean_addr)
        if pool is None:
            pool = self.get_or_create_initial_pool_state(chain_id, clean_addr, "")

        # 3. Create high-conviction BUY SignalEvent with is_direct_dm=True and custom_amount_native
        mcap = pool.calculate_market_cap_usd(native_price_usd=native_p) if pool else Decimal("10000.0")

        signal = SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=chain_id,
            pool_address=pool.pool_address if pool else "",
            token_address=clean_addr,
            suggested_side=OrderSide.BUY,
            pool_state=pool,
            security_report=report,
            strength=SignalStrength.STRONG,
            alpha_score=0.95,
            source=SignalSource.TELEGRAM_SCRAPER,
            originating_channel=originating_channel or "DM:ManualBuy",
            is_direct_dm=True,
            market_cap_usd=mcap,
            custom_amount_native=amount_native,
        )

        # 4. Enqueue into signal queue
        await self._signal_q.put(signal)
        return True, "تم إرسال أمر الشراء بنجاح إلى محرك التنفيذ (Trade signal queued)", signal

    async def manual_exit_position(
        self,
        token_address: str,
        chain: Optional[ChainIdentifier] = None,
        reason_str: str = "manual_telegram_exit",
    ) -> tuple[bool, str, Optional[Decimal]]:
        """
        Manually forces an immediate exit/sell of an open position for token_address (via Telegram /sell or button).
        Closes all open lots for this token, executes the exit fill, applies PnL, and credits balance.
        """
        clean_addr = token_address.strip()
        chain_id = chain or (
            ChainIdentifier.BASE_MAINNET if clean_addr.startswith("0x") else ChainIdentifier.SOLANA_MAINNET
        )

        open_lots = self._position_book.get_open_lots(chain=chain_id, token_address=clean_addr)
        if not open_lots:
            return False, "لا توجد صفقة مفتوحة نشطة لهذه العملة حالياً (No open position found)", None

        pool = self._pool_registry.get(clean_addr)
        if pool is None and open_lots and open_lots[0].pool_address:
            pool = self._pool_registry.get(open_lots[0].pool_address)
        if pool is None:
            pool = self.get_or_create_initial_pool_state(chain_id, clean_addr, "")

        current_price = pool.spot_price_native_per_token if pool and pool.spot_price_native_per_token > 0 else open_lots[0].entry_price
        native_price = self._eth_price if chain_id == ChainIdentifier.BASE_MAINNET else self._sol_price

        total_pnl_usd = Decimal("0")
        total_native_recovered = Decimal("0")
        total_tokens_sold = Decimal("0")

        from alpha_engine.models.enums import TradeExitReason, ExitStage
        from alpha_engine.models.state import ExitDecision

        for lot in list(open_lots):
            lot.status = LotStatus.PENDING_SELL
            lot.pending_exit_timestamp = time.time()
            if self._ledger:
                await self._ledger.record_lot_transition(
                    lot_id=lot.lot_id,
                    token_address=lot.token_address,
                    chain=lot.chain.value,
                    status=LotStatus.PENDING_SELL.value,
                    exit_reason=reason_str,
                )

            decision = ExitDecision(
                lot_id=lot.lot_id,
                token_address=lot.token_address,
                chain=lot.chain,
                should_exit=True,
                exit_reason=TradeExitReason.MANUAL_CLOSE,
                exit_stage=ExitStage.SL,
                tokens_to_sell=lot.tokens_held,
                current_price=current_price,
                pnl_estimate_native=lot.tokens_held * (current_price - lot.entry_price),
                prioritized=True,
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
                min_fill_timestamp_ns=lot.open_timestamp_ns,
            )

            if exit_fill is not None and exit_fill.effective_price > Decimal(0):
                pnl_native, _ = self._position_book.apply_exit_decision(
                    decision, sell_price=exit_fill.effective_price, net_native_received=exit_fill.simulated_native_spent
                )
                exit_pnl_usd = (pnl_native * native_price).quantize(Decimal("0.01"))
                total_pnl_usd += exit_pnl_usd
                total_native_recovered += exit_fill.simulated_native_spent
                total_tokens_sold += exit_fill.tokens_acquired

                if decision.chain == ChainIdentifier.BASE_MAINNET:
                    self._cash_eth += exit_fill.simulated_native_spent
                else:
                    self._cash_sol += exit_fill.simulated_native_spent

                gas_usd = exit_fill.simulated_gas_cost_usd or Decimal("0.05")
                self._metrics.total_gas_usd += gas_usd
                new_equity = self._current_equity_usd()
                self._metrics.record_closed_trade(
                    realized_pnl_usd=exit_pnl_usd,
                    gas_cost_usd=Decimal(0),
                    current_equity_usd=new_equity,
                    is_new_launch=getattr(lot, "is_new_launch", False),
                )
                if lot.status == LotStatus.CLOSED:
                    self._position_book.set_token_cooldown(decision.token_address, duration_s=300.0)
                if self._ledger:
                    rec = TradeRecord.from_fill(
                        fill=exit_fill,
                        signal_id=getattr(lot, "signal_id", "") or "manual_exit",
                        realized_pnl_usd=exit_pnl_usd,
                        platform="TelegramManual",
                    )
                    await self._ledger.record_trade(rec)

        # Broadcast exit alert
        unit = "ETH" if chain_id == ChainIdentifier.BASE_MAINNET else "SOL"
        sign = "+" if total_pnl_usd >= 0 else ""
        icon = "🟢" if total_pnl_usd >= 0 else "🔴"
        t_short = f"{clean_addr[:4]}..{clean_addr[-4:]}"
        alert_msg = (
            f"{icon} **MANUAL EXIT EXECUTED (إغلاق صفقة يدوي)**\n\n"
            f"• **العملة (Token):** `{t_short}` (`{clean_addr}`)\n"
            f"• **الشبكة (Chain):** {chain_id.value}\n"
            f"• **الكمية المباعة:** `{total_tokens_sold:,.2f}`\n"
            f"• **المبلغ المسترد:** `{total_native_recovered:.4f} {unit}`\n"
            f"• **الربح/الخسارة المحققة:** `{sign}${float(total_pnl_usd):.2f}`\n"
            f"• **السبب:** `Manual Telegram Exit Command`"
        )
        asyncio.create_task(self._broadcast_trade_alert(alert_msg))

        return True, f"تم إغلاق الصفقة بنجاح | PnL: {sign}${float(total_pnl_usd):.2f}", total_pnl_usd

    def _calculate_opportunity_score(
        self,
        pool: Optional[PoolState],
        security_report: Optional[SecurityReport],
        chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
        metrics_snapshot: Optional[dict[str, Any]] = None,
    ) -> float:
        """
        Calculate a standardized, reproducible opportunity score (0.0 to 100.0) based on:
        1. Security & Contract Safety (weight: 30%)
           - Clean report, score, zero honeypot/rug, low taxes.
        2. Liquidity & Liquidity Stability (weight: 25%)
           - Pool native reserve depth relative to benchmarks ($1k - $50k+).
        3. Volume & Activity (weight: 20%)
           - Trading volume in window, trade count, unique wallets.
        4. Buy Pressure & Momentum (weight: 15%)
           - Buy/sell ratio, price change/stability.
        5. Execution Feasibility & Slippage (weight: 10%)
           - Expected price impact for standard lot size.
        """
        # 1. Security Score (0 - 30)
        sec_subscore = 0.0
        if security_report is not None:
            if getattr(security_report, "passes_hard_gates", False):
                base_sec = float(getattr(security_report, "score", 80.0))
                sec_subscore = (base_sec / 100.0) * 30.0
            else:
                sec_subscore = 0.0
        else:
            sec_subscore = 20.0  # Unchecked default baseline

        # 2. Liquidity Score (0 - 25)
        liq_subscore = 0.0
        native_price = self._eth_price if chain == ChainIdentifier.BASE_MAINNET else self._sol_price
        if pool is not None and pool.native_reserve > Decimal(0):
            liq_usd = float(pool.native_reserve * native_price)
            if liq_usd >= 50_000:
                liq_subscore = 25.0
            elif liq_usd >= 20_000:
                liq_subscore = 20.0 + (liq_usd - 20_000) / 30_000 * 5.0
            elif liq_usd >= 5_000:
                liq_subscore = 12.0 + (liq_usd - 5_000) / 15_000 * 8.0
            elif liq_usd >= 1_000:
                liq_subscore = 5.0 + (liq_usd - 1_000) / 4_000 * 7.0
            else:
                liq_subscore = max(1.0, (liq_usd / 1_000) * 5.0)
        else:
            liq_subscore = 5.0

        # 3. Volume & Activity Score (0 - 20)
        vol_subscore = 10.0  # Default reasonable score if no snapshot
        if metrics_snapshot is not None:
            vol_usd = float(metrics_snapshot.get("volume_usd", 0.0))
            if vol_usd >= 50_000:
                vol_subscore = 20.0
            elif vol_usd >= 10_000:
                vol_subscore = 14.0 + (vol_usd - 10_000) / 40_000 * 6.0
            elif vol_usd >= 2_000:
                vol_subscore = 6.0 + (vol_usd - 2_000) / 8_000 * 8.0
            else:
                vol_subscore = max(2.0, (vol_usd / 2_000) * 6.0)

        # 4. Buy Pressure & Momentum Score (0 - 15)
        momentum_subscore = 8.0
        if metrics_snapshot is not None:
            buy_ratio = float(metrics_snapshot.get("buy_pressure_ratio", 0.50))
            momentum_subscore = min(15.0, max(0.0, buy_ratio * 15.0))

        # 5. Execution Feasibility (0 - 10)
        exec_subscore = 8.0
        if pool is not None and pool.native_reserve > Decimal("0.5"):
            exec_subscore = 10.0
        elif pool is not None and pool.native_reserve > Decimal("0.1"):
            exec_subscore = 6.0
        else:
            exec_subscore = 3.0

        total_score = sec_subscore + liq_subscore + vol_subscore + momentum_subscore + exec_subscore
        return round(min(100.0, max(0.0, total_score)), 2)

    async def submit_token_request(
        self,
        token_address: str,
        chain: Optional[ChainIdentifier] = None,
        user_id: Optional[int] = None,
        message_id: Optional[int] = None,
        originating_channel: Optional[str] = None,
        custom_amount_native: Optional[Decimal] = None,
    ) -> TokenRequestRecord:
        """
        Independent ingestion and lifecycle tracking for a token submission.
        Assigns unique request ID, persists record in SQLite, and schedules
        analysis without blocking caller.
        """
        clean_addr = token_address.strip()
        chain_id = chain or (
            ChainIdentifier.BASE_MAINNET if clean_addr.startswith("0x") else ChainIdentifier.SOLANA_MAINNET
        )
        now_ns = time.time_ns()
        req_id = f"req_{uuid.uuid4().hex[:12]}"

        # Validation: check format
        is_valid = False
        if chain_id == ChainIdentifier.BASE_MAINNET:
            if clean_addr.startswith("0x") and len(clean_addr) == 42:
                is_valid = True
        else:
            if 32 <= len(clean_addr) <= 44 and not clean_addr.startswith("11111111"):
                is_valid = True

        if not is_valid:
            record = TokenRequestRecord(
                request_id=req_id,
                token_address=clean_addr,
                chain=chain_id,
                user_id=user_id,
                message_id=message_id,
                originating_channel=originating_channel or "DM:User",
                custom_amount_native=custom_amount_native,
                state=TokenRequestState.REJECTED,
                rejection_reason="Invalid contract address structure",
                submitted_at_ns=now_ns,
                created_at_ns=now_ns,
                updated_at_ns=now_ns,
            )
            if self._ledger:
                await self._ledger.record_token_request(record)
            return record

        record = TokenRequestRecord(
            request_id=req_id,
            token_address=clean_addr,
            chain=chain_id,
            user_id=user_id,
            message_id=message_id,
            originating_channel=originating_channel or "DM:User",
            custom_amount_native=custom_amount_native,
            state=TokenRequestState.RECEIVED,
            submitted_at_ns=now_ns,
            created_at_ns=now_ns,
            updated_at_ns=now_ns,
        )
        if self._ledger:
            await self._ledger.record_token_request(record)

        # Transition to QUEUED
        record.state = TokenRequestState.QUEUED
        record.updated_at_ns = time.time_ns()
        if self._ledger:
            await self._ledger.update_token_request_status(record.request_id, TokenRequestState.QUEUED)

        # Backpressure & Queue Handling
        try:
            self._user_request_queue.put_nowait(record)
        except asyncio.QueueFull:
            record.state = TokenRequestState.REJECTED
            record.rejection_reason = "User request queue saturated (backpressure limit)"
            record.updated_at_ns = time.time_ns()
            if self._ledger:
                await self._ledger.update_token_request_status(
                    record.request_id, TokenRequestState.REJECTED, rejection_reason=record.rejection_reason
                )
            logger.warning("User request queue full (size=%d); rejected %s", self._user_request_queue.qsize(), clean_addr[:10])
            return record

        # Dispatch async task for immediate decoupled analysis
        task = asyncio.create_task(self._process_single_token_request(record))
        self._token_request_tasks.add(task)
        task.add_done_callback(self._token_request_tasks.discard)
        return record

    async def _process_single_token_request(self, record: TokenRequestRecord) -> None:
        async with self._analysis_semaphore:
            # 1. State: ANALYZING
            record.state = TokenRequestState.ANALYZING
            record.updated_at_ns = time.time_ns()
            if self._ledger:
                await self._ledger.update_token_request_status(record.request_id, TokenRequestState.ANALYZING)

            # Check if position already open
            if self.is_position_open(record.token_address, chain=record.chain):
                record.state = TokenRequestState.REJECTED
                record.rejection_reason = "Position already open for this token"
                record.updated_at_ns = time.time_ns()
                if self._ledger:
                    await self._ledger.update_token_request_status(
                        record.request_id, TokenRequestState.REJECTED, rejection_reason=record.rejection_reason
                    )
                return

            # Check blacklist
            if is_blacklisted_token(record.token_address, record.chain):
                record.state = TokenRequestState.REJECTED
                record.rejection_reason = "Token is in system blacklist (native/wrapped/quote asset)"
                record.updated_at_ns = time.time_ns()
                if self._ledger:
                    await self._ledger.update_token_request_status(
                        record.request_id, TokenRequestState.REJECTED, rejection_reason=record.rejection_reason
                    )
                return

            # 2. Security Screening
            report = None
            if hasattr(self, "_gatekeeper") and self._gatekeeper is not None:
                try:
                    report = await self._gatekeeper.evaluate_token(record.token_address, record.chain)
                except Exception as exc:
                    logger.warning("Gatekeeper error during token request analysis for %s: %s", record.token_address[:10], exc)

            if report is not None and not getattr(report, "passes_hard_gates", True):
                reason = self._extract_gatekeeper_rejection_reason(report)
                record.state = TokenRequestState.REJECTED
                record.rejection_reason = f"Security veto: {reason}"
                record.updated_at_ns = time.time_ns()
                if self._ledger:
                    await self._ledger.update_token_request_status(
                        record.request_id, TokenRequestState.REJECTED, rejection_reason=record.rejection_reason
                    )
                return

            if report is None:
                report = SecurityReport(
                    token_address=record.token_address,
                    chain=record.chain,
                    tier=SecurityTier.CLEAN,
                    is_honeypot=False,
                    is_rug=False,
                    score=100.0,
                    flags=[],
                    buy_tax_bps=0,
                    sell_tax_bps=0,
                    checked_at_ts=int(time.time()),
                    passes_hard_gates=True,
                )

            # 3. Market data & Opportunity Scoring
            pool = self.get_or_create_initial_pool_state(record.chain, record.token_address)
            score = self._calculate_opportunity_score(pool, report, record.chain)
            record.opportunity_score = score

            if score < 15.0:
                record.state = TokenRequestState.REJECTED
                record.rejection_reason = f"Opportunity score {score:.1f} below minimum threshold (15.0)"
                record.updated_at_ns = time.time_ns()
                if self._ledger:
                    await self._ledger.update_token_request_status(
                        record.request_id, TokenRequestState.REJECTED, rejection_reason=record.rejection_reason, score=score
                    )
                return

            record.state = TokenRequestState.ELIGIBLE
            record.updated_at_ns = time.time_ns()
            if self._ledger:
                await self._ledger.update_token_request_status(record.request_id, TokenRequestState.ELIGIBLE, score=score)

            # 4. Check Capacity & Atomic Reservation
            can_open, reject_reason = self._position_book.can_open_position(
                token_address=record.token_address,
                chain=record.chain,
                is_manual_size=True,
            )
            if not can_open:
                # Capacity is occupied -> place in waiting queue
                record.state = TokenRequestState.WAITING_FOR_CAPACITY
                record.note = f"Position limit reached ({self._position_book.effective_active_position_count()}/{self._position_book.max_active_positions}). Queued for capacity."
                record.updated_at_ns = time.time_ns()
                if self._ledger:
                    await self._ledger.update_token_request_status(
                        record.request_id, TokenRequestState.WAITING_FOR_CAPACITY, note=record.note, score=score
                    )
                async with self._waiting_capacity_lock:
                    self._waiting_for_capacity.append(record)
                    self._waiting_for_capacity.sort(key=lambda r: (r.opportunity_score or 0.0), reverse=True)
                logger.info(
                    "Token request %s (%s) queued in waiting_for_capacity (rank=%d, score=%.1f)",
                    record.request_id,
                    record.token_address[:10],
                    len(self._waiting_for_capacity),
                    score,
                )
                return

            reserved = self._position_book.reserve_position_slot(token_address=record.token_address, request_id=record.request_id)
            if not reserved:
                # Race condition, slot taken
                record.state = TokenRequestState.WAITING_FOR_CAPACITY
                record.note = "Contention for final position slot; queued for next capacity release"
                record.updated_at_ns = time.time_ns()
                if self._ledger:
                    await self._ledger.update_token_request_status(
                        record.request_id, TokenRequestState.WAITING_FOR_CAPACITY, note=record.note, score=score
                    )
                async with self._waiting_capacity_lock:
                    self._waiting_for_capacity.append(record)
                    self._waiting_for_capacity.sort(key=lambda r: (r.opportunity_score or 0.0), reverse=True)
                return

            # Reserved! Execute now:
            await self._execute_reserved_request(record, pool, report)

    async def _execute_reserved_request(
        self,
        record: TokenRequestRecord,
        pool: PoolState,
        report: Optional[SecurityReport],
    ) -> None:
        record.state = TokenRequestState.EXECUTION_PENDING
        record.updated_at_ns = time.time_ns()
        if self._ledger:
            await self._ledger.update_token_request_status(record.request_id, TokenRequestState.EXECUTION_PENDING)

        native_p = self._eth_price if record.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
        unit = "ETH" if record.chain == ChainIdentifier.BASE_MAINNET else "SOL"
        available_cash = self._cash_eth if record.chain == ChainIdentifier.BASE_MAINNET else self._cash_sol
        fee_est = Decimal("0.002") if record.chain == ChainIdentifier.BASE_MAINNET else Decimal("0.005")

        req_amount = record.custom_amount_native
        if req_amount is None and self.manual_trade_amount is not None and self.manual_trade_amount > Decimal(0):
            if self.manual_trade_currency == "USD":
                req_amount = self.manual_trade_amount / native_p if native_p > Decimal(0) else Decimal(0)
            else:
                req_amount = self.manual_trade_amount

        if req_amount is None or req_amount <= Decimal(0):
            req_amount = Decimal("0.03") if record.chain == ChainIdentifier.SOLANA_MAINNET else Decimal("0.003")

        if available_cash < (req_amount + fee_est):
            self._position_book.release_position_slot(request_id=record.request_id, token_address=record.token_address)
            record.state = TokenRequestState.REJECTED
            record.rejection_reason = f"Insufficient cash balance ({available_cash:.4f} {unit} < {req_amount + fee_est:.4f} {unit})"
            record.updated_at_ns = time.time_ns()
            if self._ledger:
                await self._ledger.update_token_request_status(
                    record.request_id, TokenRequestState.REJECTED, rejection_reason=record.rejection_reason
                )
            return

        mcap = pool.calculate_market_cap_usd(native_price_usd=native_p) if pool else Decimal("10000.0")
        signal = SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=record.chain,
            pool_address=pool.pool_address if pool else "0x" + "0" * 40,
            token_address=record.token_address,
            suggested_side=OrderSide.BUY,
            pool_state=pool,
            security_report=report,
            strength=SignalStrength.STRONG,
            alpha_score=record.opportunity_score / 100.0 if record.opportunity_score else 0.95,
            source=SignalSource.TELEGRAM_SCRAPER,
            originating_channel=record.originating_channel or "DM:User",
            is_direct_dm=True,
            market_cap_usd=mcap,
            custom_amount_native=req_amount,
        )

        fill = None
        if self._executor is not None:
            fill = await self._executor.execute_signal(
                signal,
                portfolio_equity_usd=self._current_equity_usd(),
                is_new_launch=False,
                fixed_trade_amount_native=req_amount,
            )

        if fill is None or fill.effective_price <= Decimal(0):
            self._position_book.release_position_slot(request_id=record.request_id, token_address=record.token_address)
            record.state = TokenRequestState.FAILED
            record.rejection_reason = "Paper execution returned no fill or invalid effective price"
            record.updated_at_ns = time.time_ns()
            if self._ledger:
                await self._ledger.update_token_request_status(
                    record.request_id, TokenRequestState.FAILED, rejection_reason=record.rejection_reason
                )
            return

        # Success fill! Open lot and commit slot
        platform_name = getattr(fill, "platform", None) or resolve_trade_platform(
            token_address=fill.token_address,
            chain=fill.chain,
            source=signal.source,
            execution_venue=getattr(signal, "execution_venue", None),
            pool_address=getattr(signal, "pool_address", ""),
        )
        strat_name = getattr(self, "active_strategy", getattr(self.config, "active_strategy", "default"))
        trail_peak = getattr(self, "trailing_stop_from_peak_pct", getattr(self.config, "trailing_stop_from_peak_pct", None))
        cust_tp = getattr(self, "custom_take_profit_pct", getattr(self.config, "custom_take_profit_pct", None))
        cust_sl = getattr(self, "custom_stop_loss_pct", getattr(self.config, "custom_stop_loss_pct", None))
        hard_sl = getattr(self, "hard_stop_loss_pct", getattr(self.config, "hard_stop_loss_pct", 0.20))
        entry_vec = PatternFeatureVector.from_signal(signal).to_vector()
        lot = self._position_book.open_lot(
            fill,
            signal.signal_id,
            initial_pool_reserve_native=pool.native_reserve if pool else Decimal(0),
            pool_address=pool.pool_address if pool else (getattr(signal, "pool_address", "") or ""),
            bonding_curve_mode=(platform_name == "Pump.fun"),
            platform=platform_name,
            strategy_name=strat_name,
            trailing_stop_from_peak_pct=trail_peak,
            custom_take_profit_pct=cust_tp,
            custom_stop_loss_pct=cust_sl,
            hard_stop_loss_pct=hard_sl,
            entry_feature_vector=entry_vec,
        )
        self._position_book.commit_slot_to_lot_by_token(record.token_address, record.chain)
        if pool is not None and lot.pool_address:
            self._pool_registry.set(lot.pool_address, pool)

        native_price = self._eth_price if record.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
        gas_usd = fill.simulated_gas_cost_usd or Decimal("0.05")
        gas_native = (gas_usd / native_price).quantize(Decimal("1e-18")) if native_price > Decimal(0) else Decimal("0.0005")

        if record.chain == ChainIdentifier.BASE_MAINNET:
            self._cash_eth -= (fill.simulated_native_spent + gas_native)
        else:
            self._cash_sol -= (fill.simulated_native_spent + gas_native)

        self._metrics.total_gas_usd += gas_usd
        lot.cost_basis_native = fill.simulated_native_spent + gas_native

        if self._ledger:
            trade_rec = TradeRecord.from_fill(
                fill=fill,
                signal_id=signal.signal_id,
                platform=platform_name,
                hard_stop_loss_pct=hard_sl,
                custom_stop_loss_pct=cust_sl,
                custom_take_profit_pct=cust_tp,
                trailing_stop_from_peak_pct=trail_peak,
                strategy_name=strat_name,
            )
            await self._ledger.record_trade(trade_rec)
            await self._ledger.record_lot_transition(
                lot_id=lot.lot_id,
                token_address=lot.token_address,
                chain=lot.chain.value,
                status=LotStatus.OPEN.value,
            )

        record.state = TokenRequestState.EXECUTED
        record.order_id = fill.order_id
        record.lot_id = lot.lot_id
        record.simulated_fill_price = fill.effective_price
        record.tokens_filled = fill.tokens_acquired
        record.updated_at_ns = time.time_ns()
        if self._ledger:
            await self._ledger.update_token_request_status(
                record.request_id,
                TokenRequestState.EXECUTED,
                order_id=fill.order_id,
                lot_id=lot.lot_id,
                fill_price=fill.effective_price,
                tokens_filled=fill.tokens_acquired,
            )

        # Broadcast buy alert
        p_str = self._format_price_clean(fill.effective_price, native_p, unit)
        t_str = f"{fill.token_address[:4]}..{fill.token_address[-4:]}"
        buy_alert = (
            f"🟢 **BUY EXECUTED (Paper Trade)**\n\n"
            f"• **Platform:** `{platform_name}`\n"
            f"• **Token:** `{t_str}` (`{fill.token_address}`)\n"
            f"• **Chain:** {record.chain.value}\n"
            f"• **Opportunity Score:** `{record.opportunity_score:.1f}/100`\n"
            f"• **Buy Price:** `{p_str}`\n"
            f"• **Spent:** `{float(fill.simulated_native_spent):.4f} {unit}`\n"
            f"• **Tokens Acquired:** `{float(fill.tokens_acquired):,.2f}`\n"
            f"• **Request ID:** `{record.request_id}`\n"
            f"• **Order ID:** `{fill.order_id[:8]}`"
        )
        asyncio.create_task(self._broadcast_trade_alert(buy_alert))

    def _on_position_capacity_freed(self, lot_id: str = "", token_address: str = "") -> None:
        """Invoked by PositionBook whenever a lot is closed or slot is released."""
        logger.info("Position capacity freed (lot=%s, token=%s). Evaluating waiting queue...", lot_id, token_address[:10])
        asyncio.create_task(self._evaluate_next_waiting_candidate())

    async def _evaluate_next_waiting_candidate(self) -> None:
        async with self._waiting_capacity_lock:
            if not self._waiting_for_capacity:
                return

            now_s = time.time()
            valid_candidates = []
            for c in self._waiting_for_capacity:
                submitted_ts = c.created_at_ns or c.submitted_at_ns or time.time_ns()
                age_s = now_s - (submitted_ts / 1_000_000_000)
                if age_s > 900:  # 15 minutes TTL
                    c.state = TokenRequestState.EXPIRED
                    c.note = "Expired in waiting queue (TTL > 15m)"
                    c.updated_at_ns = time.time_ns()
                    if self._ledger:
                        await self._ledger.update_token_request_status(c.request_id, TokenRequestState.EXPIRED, note=c.note)
                else:
                    valid_candidates.append(c)

            self._waiting_for_capacity = valid_candidates
            if not self._waiting_for_capacity:
                return

            # Pick highest score
            candidate = self._waiting_for_capacity.pop(0)

        # Check capacity
        can_open, reject_reason = self._position_book.can_open_position(
            token_address=candidate.token_address,
            chain=candidate.chain,
            is_manual_size=True,
        )
        if not can_open:
            async with self._waiting_capacity_lock:
                self._waiting_for_capacity.insert(0, candidate)
            return

        reserved = self._position_book.reserve_position_slot(token_address=candidate.token_address, request_id=candidate.request_id)
        if not reserved:
            async with self._waiting_capacity_lock:
                self._waiting_for_capacity.insert(0, candidate)
            return

        pool = self.get_or_create_initial_pool_state(candidate.chain, candidate.token_address)
        report = None
        if hasattr(self, "_gatekeeper") and self._gatekeeper is not None:
            try:
                report = await self._gatekeeper.evaluate_token(candidate.token_address, candidate.chain)
            except Exception:
                pass
            if report and not report.passes_hard_gates:
                self._position_book.release_position_slot(candidate.token_address)
                candidate.state = TokenRequestState.REJECTED
                candidate.rejection_reason = "Security veto on pre-execution revalidation"
                candidate.updated_at_ns = time.time_ns()
                if self._ledger:
                    await self._ledger.update_token_request_status(
                        candidate.request_id, TokenRequestState.REJECTED, rejection_reason=candidate.rejection_reason
                    )
                return

        await self._execute_reserved_request(candidate, pool, report)

    async def _process_user_request_queue(self) -> None:
        """Background queue processor for user token requests."""
        while not self._shutdown_event.is_set():
            try:
                record = await asyncio.wait_for(self._user_request_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            self._user_request_queue.task_done()

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

            # Fast-path check: immediately drop swap if token is in negative rejection cache,
            # unless an impactful buy arrives (>= 0.25 SOL/ETH) or it is eligible for dynamic reassessment
            if gk.is_rejected(target_token, swap.chain) is True:
                can_reassess = False
                if hasattr(gk, "can_reassess"):
                    can_reassess = gk.can_reassess(target_token, swap.chain, sol_amount=sol_amount)
                if (is_buy and sol_amount >= Decimal("0.25")) or can_reassess:
                    logger.info(
                        "Runner: Previously soft-rejected token %s received impactful buy (%s native). Spawning dynamic reassessment...",
                        target_token[:10],
                        sol_amount,
                    )
                    task = asyncio.create_task(
                        self._async_reassess_token(target_token, swap.chain, swap.pool_address, sol_amount, gk),
                        name=f"reassess_{target_token[:8]}",
                    )
                    self._screening_tasks.add(task)
                    task.add_done_callback(self._screening_tasks.discard)
                continue

            is_pump = (
                isinstance(swap, PumpSwapEvent)
                or (
                    swap.chain == ChainIdentifier.SOLANA_MAINNET
                    and "pump" in swap.pool_address.lower()
                )
            )
            auto_min_vol = Decimal("0.02") if swap.chain == ChainIdentifier.BASE_MAINNET else (Decimal("0.5") if is_pump else Decimal("1.5"))
            if (
                is_buy
                and sol_amount >= auto_min_vol
                and not self._pending_launch_buffer.is_staged(target_token)
                and not self.is_position_open(target_token, swap.chain)
                and not is_blacklisted_token(target_token, swap.chain)
                and not gk.is_rejected(target_token, swap.chain)
            ):
                # Spawn non-blocking background discovery task
                task = asyncio.create_task(
                    self._async_auto_discover_token(
                        target_token,
                        swap.chain,
                        swap.pool_address,
                        sol_amount,
                        gk,
                        buyer=getattr(swap, "buyer", "") or getattr(swap, "sender", ""),
                        slot=getattr(swap, "slot", 0) or getattr(swap, "block_number", 0),
                        is_pump=is_pump,
                    ),
                    name=f"auto_discover_{target_token[:8]}",
                )
                self._screening_tasks.add(task)
                task.add_done_callback(self._screening_tasks.discard)

            # Prioritize target_token lookup to prevent cross-token pool state collisions
            pool = self._pool_registry.get(target_token, expected_token=target_token)
            if pool is None and swap.pool_address:
                pool = self._pool_registry.get(swap.pool_address, expected_token=target_token)
            if pool is None:
                # إنشاء حوض أولي تلقائياً للعملة القديمة لمتابعة صفقاتها بدلاً من إسقاطها
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
                # Unstaged DEX swap path: spawn background task to prevent blocking the ingestion queue
                task = asyncio.create_task(
                    self._async_evaluate_unstaged_swap(swap, updated_pool, target_token, gk),
                    name=f"eval_unstaged_{target_token[:8]}"
                )
                self._screening_tasks.add(task)
                task.add_done_callback(self._screening_tasks.discard)

    async def _async_evaluate_unstaged_swap(
        self,
        swap: SwapEvent,
        updated_pool: PoolState,
        target_token: str,
        gk: Any,
    ) -> None:
        async with self._screening_semaphore:
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
                return

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

    async def _async_auto_discover_token(
        self,
        token_address: str,
        chain: ChainIdentifier,
        pool_address: str,
        sol_amount: Decimal,
        gk: Any,
        buyer: str = "",
        slot: int = 0,
        is_pump: Optional[bool] = None,
    ) -> None:
        async with self._screening_semaphore:
            try:
                disc_report = await gk.evaluate_token(
                    token_address=token_address, chain=chain, pool_address=pool_address
                )
                disc_reason = self._extract_gatekeeper_rejection_reason(disc_report)
                self._record_gatekeeper_eval(token_address, chain, disc_reason, passed=disc_report.passes_hard_gates)
                if disc_report.passes_hard_gates:
                    initial_pool = self.get_or_create_initial_pool_state(chain, token_address, pool_address)
                    init_price = initial_pool.spot_price_native_per_token if initial_pool else Decimal("0.000000028")
                    if is_pump is None:
                        is_pump = "pump" in token_address.lower() or "pump" in pool_address.lower()
                    
                    if is_pump and not self._pending_launch_buffer.is_staged(token_address):
                        staged = self._pending_launch_buffer.stage_token(
                            token_address=token_address,
                            chain=chain,
                            pool_address=pool_address,
                            initial_price=init_price,
                            report=disc_report,
                            t_0=time.time(),
                        )
                        if staged is not None and sol_amount > Decimal(0):
                            staged.total_volume_native += sol_amount
                            staged.buy_count += 1
                            staged.buy_volumes.append(sol_amount)
                            if buyer:
                                staged.unique_buyers.add(buyer)
                            if slot > 0:
                                staged.slot_buy_counts[slot] += 1
                                staged.buy_blocks.add(slot)
                        logger.info("Auto-discovery staged Pump.fun launch %s in PendingLaunchBuffer", token_address[:10])
                    
                    # Register in Wave2 and Revival buffers for early rally and revival breakout monitoring
                    if not self._wave2_buffer.is_staged(token_address) and not self._revival_buffer.is_staged(token_address):
                        self._wave2_buffer.stage_token(
                            token_address=token_address,
                            chain=chain,
                            pool_address=pool_address,
                            initial_price=init_price,
                            report=disc_report,
                        )
                        self._revival_buffer.stage_token(
                            token_address=token_address,
                            chain=chain,
                            pool_address=pool_address,
                            initial_price=init_price,
                            report=disc_report,
                            token_creation_timestamp=time.time() - (24 * 3600),
                        )
                        logger.info("Auto-discovery registered token %s in Wave-2 and Revival Watchlists", token_address[:10])
            except Exception as exc:  # noqa: BLE001
                logger.debug("Auto-discovery failed for %s: %s", token_address[:10], exc)

    async def _async_reassess_token(
        self, token_address: str, chain: ChainIdentifier, pool_address: str, sol_amount: Decimal, gk: Any
    ) -> None:
        async with self._screening_semaphore:
            try:
                reassessed_rep = await gk.reassess_token(
                    token_address=token_address,
                    chain=chain,
                    pool_address=pool_address,
                )
                self._record_gatekeeper_eval(
                    token_address,
                    chain,
                    self._extract_gatekeeper_rejection_reason(reassessed_rep),
                    passed=reassessed_rep.passes_hard_gates,
                )
                if reassessed_rep.passes_hard_gates and not self._pending_launch_buffer.is_staged(token_address):
                    initial_pool = self.get_or_create_initial_pool_state(chain, token_address, pool_address)
                    init_price = initial_pool.spot_price_native_per_token if initial_pool else Decimal("0.000000028")
                    self._pending_launch_buffer.stage_token(
                        token_address=token_address,
                        chain=chain,
                        pool_address=pool_address,
                        initial_price=init_price,
                        report=reassessed_rep,
                        t_0=time.time(),
                    )
                    logger.info(
                        "Reassessment staged %s in PendingLaunchBuffer (volume: %s native)",
                        token_address[:10], sol_amount
                    )
            except Exception as exc:  # noqa: BLE001
                logger.debug("Failed dynamic reassessment for %s: %s", token_address[:10], exc)

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
            if getattr(raw_sig, "is_direct_dm", False):
                updates["is_direct_dm"] = True
            if getattr(raw_sig, "originating_channel", None):
                updates["originating_channel"] = raw_sig.originating_channel

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
                # Guard against cross-token exit evaluation
                if pool.token_address and pool.token_address.lower() != lot.token_address.lower():
                    continue
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
                
                # V2 Exit Logic:
                # PositionBook is the primary authority for lot exits (peak trailing, TP ladders, dynamic stops).
                # The V2 ExitEngine acts as an emergency safeguard for liquidity drain rugpulls.
                if (decision is None or not decision.should_exit) and self.config.strategy_v2_enabled:
                    entry_ts = getattr(lot, "entry_timestamp_s", None) or lot.last_tick_timestamp_s
                    age_seconds = time.time() - entry_ts if entry_ts else 0.0
                    liquidity_drop = 0.0
                    if lot.peak_pool_reserve_native > 0:
                        liquidity_drop = float((lot.peak_pool_reserve_native - pool.native_reserve) / lot.peak_pool_reserve_native)
                    if liquidity_drop >= self.config.emergency_exit_liquidity_drop_pct:
                        from alpha_engine.models.state import ExitDecision
                        from alpha_engine.models.enums import ExitStage, TradeExitReason
                        decision = ExitDecision(
                            lot_id=lot.lot_id,
                            token_address=lot.token_address,
                            chain=lot.chain,
                            should_exit=True,
                            exit_reason=TradeExitReason.RUG_LIQUIDITY_DRAIN,
                            exit_stage=ExitStage.RUGPULL,
                            tokens_to_sell=lot.tokens_held,
                            current_price=pool.spot_price_native_per_token,
                            pnl_estimate_native=lot.tokens_held * (pool.spot_price_native_per_token - lot.entry_price),
                            prioritized=True,
                            action="CLOSE_ALL",
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
                        min_fill_timestamp_ns=lot.open_timestamp_ns,
                    )
                    if exit_fill is not None:
                        native_price = (
                            self._eth_price
                            if decision.chain == ChainIdentifier.BASE_MAINNET
                            else self._sol_price
                        )
                        pnl_native, _ = self._position_book.apply_exit_decision(
                            decision, sell_price=exit_fill.effective_price, net_native_received=exit_fill.simulated_native_spent
                        )
                        exit_pnl_usd = (pnl_native * native_price).quantize(Decimal("0.01"))
                        if decision.chain == ChainIdentifier.BASE_MAINNET:
                            self._cash_eth += exit_fill.simulated_native_spent
                        else:
                            self._cash_sol += exit_fill.simulated_native_spent

                        new_equity = self._current_equity_usd()
                        is_launch = (getattr(lot, "exit_profile", None) not in (ExitProfile.REVIVAL_SWING, ExitProfile.EARLY_RALLY_SWING) and 
                                     getattr(lot, "bonding_curve_mode", False) is True)
                        if exit_fill.simulated_gas_cost_usd:
                            self._metrics.total_gas_usd += exit_fill.simulated_gas_cost_usd
                        self._metrics.record_closed_trade(
                            realized_pnl_usd=exit_pnl_usd,
                            gas_cost_usd=Decimal(0),
                            current_equity_usd=new_equity,
                            is_new_launch=is_launch,
                        )
                        if lot.status == LotStatus.CLOSED:
                            self._position_book.set_token_cooldown(decision.token_address, duration_s=300.0)
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
                        tuned_params = self._feedback.parameter_tuner.update_from_performance(
                            win_rate_24h=self._metrics.win_rate_pct,
                            realized_pnl_usd=realized_pnl,
                            total_trades_24h=self._metrics.total_trades,
                        )
                        self._propagate_dynamic_hyperparameters(tuned_params)

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
                            entry_feature_vector=getattr(lot, "entry_feature_vector", []),
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

    async def evaluate_screener_token(
        self, discovery: ScreenerDiscoveryEvent | dict[str, Any]
    ) -> ScreenerEvaluationResult:
        """
        Evaluates a token from the Graduated or Movers screener sections immediately
        without imposing the standard 2-minute or 5-minute observation delay, provided
        all mandatory volume ($8,000 rolling 2m), liquidity, and safety conditions are met.
        """
        if isinstance(discovery, dict):
            discovery = ScreenerDiscoveryEvent.model_validate(discovery)

        if self.screener_evaluator.gatekeeper is None and self._gatekeeper is not None:
            self.screener_evaluator.gatekeeper = self._gatekeeper
        if self.screener_evaluator.ledger is None and self._ledger is not None:
            self.screener_evaluator.ledger = self._ledger

        result = await self.screener_evaluator.evaluate(discovery)

        # If accepted and a BUY signal is produced, enqueue it for immediate execution
        if result.decision == ScreenerDecision.ACCEPTED and result.signal is not None:
            if self._ledger is not None:
                await self._ledger.record_signal(result.signal)
            section_tag = (
                "GRADUATED" if discovery.section == ScreenerSection.GRADUATED
                else "MOVERS" if discovery.section == ScreenerSection.MOVERS
                else "GRADUATED+MOVERS" if discovery.section == ScreenerSection.BOTH
                else str(discovery.section)
            )
            logger.info(
                "Screener Priority Trade Admitted [%s]: Mint=%s, Section=%s, Volume2m=$%.2f, Score=%.2f",
                result.signal.signal_id[:8],
                discovery.mint[:10],
                section_tag,
                result.volume_2m_usd,
                result.signal.alpha_score,
            )
            await self._signal_q.put(result.signal)

        return result

    async def _process_signal_queue(self) -> None:
        executor = self._executor
        ledger = self._ledger
        assert executor is not None and ledger is not None

        while True:
            item = await self._signal_q.get()

            if isinstance(item, ShutdownSentinel):
                logger.info("Signal processor received shutdown sentinel. Terminating queue.")
                return

            if isinstance(item, ScreenerDiscoveryEvent):
                await self.evaluate_screener_token(item)
                continue

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
            is_new_launch = self._is_new_launch_signal(signal_item)

            if signal_item.suggested_side == OrderSide.BUY:
                is_direct_dm = bool(getattr(signal_item, "is_direct_dm", False)) or (
                    bool(getattr(signal_item, "originating_channel", ""))
                    and str(getattr(signal_item, "originating_channel", "")).startswith("DM:")
                )

                # 00. Automated Trading Switch
                if not getattr(self, "auto_trade_enabled", True) and not is_direct_dm:
                    logger.info("Auto-trade is DISABLED via strategy settings; skipping BUY signal for %s", signal_item.token_address[:10])
                    continue

                # Ensure pool state is initialized for accurate pricing and market cap checks
                if signal_item.pool_state is None or signal_item.pool_state.spot_price_native_per_token <= Decimal(0):
                    signal_item.pool_state = self.get_or_create_initial_pool_state(
                        chain=signal_item.chain,
                        token_address=signal_item.token_address,
                        pool_address=getattr(signal_item, "pool_address", "") or "",
                    )

                # 00b. Exclusive Focus on Graduated & Movers
                if getattr(self, "screener_exclusive_focus", False):
                    # Check if token matches Screener Graduated or Movers criteria:
                    # 1. Direct screener discovery
                    is_screener_src = signal_item.source in (
                        SignalSource.SCREENER_GRADUATED,
                        SignalSource.SCREENER_MOVERS,
                    ) or getattr(signal_item, "pattern", None) in ("screener_graduated", "screener_movers") or getattr(signal_item, "strategy_pattern", None) in ("screener_graduated", "screener_movers")
                    
                    # 2. Graduated tokens on Solana: Pump.fun tokens migrated to Raydium AMM or graduated launches
                    tok_addr_l = signal_item.token_address.lower()
                    is_graduated = (
                        is_screener_src
                        or getattr(signal_item, "is_graduated", False)
                        or getattr(signal_item, "strategy_pattern", None) in ("graduated_launch", "smart_launch")
                        or (getattr(signal_item, "execution_venue", None) == ExecutionVenue.RAYDIUM_AMM and tok_addr_l.endswith("pump"))
                    )
                    
                    # 3. Market Movers: High-conviction alpha signals, volume surges, or momentum breakouts
                    is_mover = (
                        is_screener_src
                        or signal_item.source in (SignalSource.REVIVAL_BREAKOUT, SignalSource.WAVE2_BREAKOUT, SignalSource.EARLY_RALLY_BREAKOUT, SignalSource.MEME_RADAR_BURST)
                        or getattr(signal_item, "exit_profile", None) in (ExitProfile.REVIVAL_SWING, ExitProfile.EARLY_RALLY_SWING)
                        or getattr(signal_item, "strategy_pattern", "") in ("early_rally_breakout", "wave2_breakout", "revival_breakout")
                        or (signal_item.strength in (SignalStrength.MODERATE, SignalStrength.STRONG) and signal_item.alpha_score >= 0.40)
                    )
                    
                    if not (is_graduated or is_mover or is_direct_dm):
                        logger.info(
                            "Exclusive Screener Focus active: skipping non-graduated/non-mover token %s (source=%s, venue=%s, alpha=%.3f)",
                            signal_item.token_address[:10],
                            signal_item.source,
                            getattr(signal_item, "execution_venue", "unknown"),
                            signal_item.alpha_score,
                        )
                        continue

                # 0a. Mandatory Minimum Market Cap Requirement ($10,000 USD default, $3,500 USD for Early Rally / Breakouts)
                is_early_rally_or_breakout = (
                    getattr(signal_item, "source", None) in (
                        SignalSource.EARLY_RALLY_BREAKOUT,
                        SignalSource.WAVE2_BREAKOUT,
                        SignalSource.REVIVAL_BREAKOUT,
                        "early_rally_breakout",
                        "wave2_breakout",
                        "revival_breakout",
                    )
                    or getattr(signal_item, "exit_profile", None) in (
                        ExitProfile.EARLY_RALLY_SWING,
                        ExitProfile.REVIVAL_SWING,
                        "early_rally_swing",
                        "revival_swing",
                    )
                    or getattr(signal_item, "strategy_pattern", "") in ("early_rally_breakout", "wave2_breakout", "revival_breakout")
                    or getattr(self, "active_strategy", "") in ("early_rally", "early_rally_breakout")
                )
                if is_early_rally_or_breakout:
                    min_mcap_usd = Decimal(str(getattr(self, "early_rally_min_market_cap_usd", getattr(self._cfg, "early_rally_min_market_cap_usd", 3500.0))))
                else:
                    min_mcap_usd = Decimal(str(getattr(self, "min_market_cap_usd", getattr(self._cfg, "min_market_cap_usd", 10000.0))))
                sig_mcap = getattr(signal_item, "market_cap_usd", None)
                if (sig_mcap is None or sig_mcap <= Decimal("0")) and signal_item.pool_state is not None:
                    native_p = (
                        self._eth_price
                        if signal_item.chain == ChainIdentifier.BASE_MAINNET
                        else self._sol_price
                    )
                    sig_mcap = signal_item.pool_state.calculate_market_cap_usd(native_price_usd=native_p)

                if is_direct_dm:
                    logger.info(
                        "Direct Telegram DM signal for %s: Bypassing minimum market cap threshold ($%.2f). Market Cap=$%s",
                        signal_item.token_address[:10],
                        min_mcap_usd,
                        f"{sig_mcap:.2f}" if sig_mcap else "n/a",
                    )
                else:
                    # Strict Market Cap check for new tokens / launches:
                    if is_new_launch or signal_item.source in (
                        SignalSource.PUMP_FUN_MINT,
                        SignalSource.SCREENER_GRADUATED,
                        SignalSource.SCREENER_MOVERS,
                    ):
                        if sig_mcap is None or sig_mcap <= Decimal("0"):
                            logger.warning(
                                "Dropping BUY signal for %s: Market Cap is missing, unverifiable, or stale",
                                signal_item.token_address[:10],
                            )
                            continue
                        if sig_mcap < min_mcap_usd:
                            logger.info(
                                "Dropping BUY signal for %s: Market Cap $%.2f < minimum required threshold $%.2f",
                                signal_item.token_address[:10],
                                sig_mcap,
                                min_mcap_usd,
                            )
                            continue
                    elif sig_mcap is not None and sig_mcap > Decimal("0") and sig_mcap < min_mcap_usd:
                        logger.info(
                            "Dropping BUY signal for %s: Market Cap $%.2f < minimum required threshold $%.2f",
                            signal_item.token_address[:10],
                            sig_mcap,
                            min_mcap_usd,
                        )
                        continue

                # 0. Reject weak signals or scores below quality threshold
                min_score = getattr(self._cfg, "min_entry_score", 50.0) / 100.0
                if not is_direct_dm and signal_item.strength == SignalStrength.WEAK and signal_item.alpha_score < min_score:
                    logger.info(
                        "Dropping BUY signal for %s: alpha_score=%.3f (strength=%s) below threshold %.2f",
                        signal_item.token_address[:10], signal_item.alpha_score, signal_item.strength.value, min_score
                    )
                    continue

                # 0b. تطبيق حاكم الوتيرة على العملات الجديدة فقط
                if is_new_launch and self.is_trading_paused_by_governor():
                    remaining_cd = max(0.0, self._cooldown_until - time.time())
                    logger.warning(
                        "Governor ACTIVE: Pausing NEW LAUNCH signal for %s (Older coins unaffected, cd: %.1fm).",
                        signal_item.token_address[:10],
                        remaining_cd / 60.0,
                    )
                    continue

                # 1. تطبيق قاطع الدائرة الخاص بالعملات الجديدة فقط دون إيقاف العملات القديمة
                freeze_dur = getattr(self._cfg, "circuit_breaker_freeze_duration_s", 3600.0)
                if self._metrics.is_circuit_breaker_active(freeze_duration_s=freeze_dur):
                    logger.warning(
                        "CIRCUIT BREAKER ACTIVE (%s) - Freezing BUY signal for %s",
                        self._metrics.circuit_breaker_reason,
                        signal_item.token_address[:10],
                    )
                    continue

                if is_new_launch and self._metrics.is_launch_circuit_breaker_active(freeze_duration_s=freeze_dur):
                    logger.warning(
                        "LAUNCH CIRCUIT BREAKER ACTIVE: Freezing NEW LAUNCH signal for %s (Older/DEX coins continue normally).",
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
                
                custom_amt = getattr(signal_item, "custom_amount_native", None)
                is_manual_size = (
                    (custom_amt is not None and custom_amt > Decimal(0))
                    or (self.manual_trade_amount is not None and self.manual_trade_amount > Decimal(0))
                )
                if custom_amt is not None and custom_amt > Decimal(0):
                    preliminary_cost = custom_amt
                elif is_manual_size:
                    if self.manual_trade_currency == "USD":
                        target_native = self.manual_trade_amount / native_p if native_p > Decimal(0) else Decimal(0)
                    else:
                        target_native = self.manual_trade_amount
                    preliminary_cost = target_native
                elif self._cfg.strategy_v2_enabled:
                    metrics_dict = {
                        "consecutive_losses": self._metrics.consecutive_launch_losses if is_new_launch else self._metrics.consecutive_losses,
                        "daily_drawdown_pct": getattr(self._metrics, "daily_drawdown_pct", 0.0) 
                    }
                    if not self.risk_engine.check_daily_limits(metrics_dict):
                        logger.warning("V2 RiskEngine: Daily limits breached. Dropping BUY signal.")
                        continue
                        
                    prelim_native = self.risk_engine.calculate_position_size(
                        current_capital=equity_native,
                        score=signal_item.alpha_score * 100.0,
                        is_new_launch=is_new_launch,
                    )
                    preliminary_cost = prelim_native
                    
                    if preliminary_cost <= Decimal(0):
                        logger.info("V2 RiskEngine: Position size calculated as 0 (score too low). Dropping BUY.")
                        continue
                else:
                    preliminary_cost = equity_native * Decimal("0.015")

                # Verify balance and network fee availability before attempting trade execution (Requirement 8)
                unit = "ETH" if signal_item.chain == ChainIdentifier.BASE_MAINNET else "SOL"
                available_cash = self._cash_eth if signal_item.chain == ChainIdentifier.BASE_MAINNET else self._cash_sol
                simulated_fee = Decimal("0.002") if signal_item.chain == ChainIdentifier.BASE_MAINNET else Decimal("0.005")
                total_needed = preliminary_cost + simulated_fee

                if available_cash < total_needed:
                    t_short = f"{signal_item.token_address[:4]}..{signal_item.token_address[-4:]}"
                    insufficient_alert = (
                        f"⚠️ **رصيد غير كافٍ لتنفيذ الصفقة (INSUFFICIENT BALANCE)**\n\n"
                        f"• **العملة (Token):** `{t_short}` (`{signal_item.token_address}`)\n"
                        f"• **الشبكة (Chain):** {signal_item.chain.value}\n"
                        f"• **مبلغ التداول المطلوب:** `{preliminary_cost:.4f} {unit}`\n"
                        f"• **رسوم الشبكة المقدرة:** `{simulated_fee} {unit}`\n"
                        f"• **الإجمالي المطلوب:** `{total_needed:.4f} {unit}`\n"
                        f"• **الرصيد المتاح حالياً:** `{available_cash:.4f} {unit}`\n\n"
                        f"❌ **تم إلغاء تنفيذ الصفقة لعدم توفر رصيد كافٍ.**\n"
                        f"💡 لن يتم رفع أو تعديل مبلغ التداول تلقائياً لتعويض نقص الرصيد.\n"
                        f"يمكنك شحن الرصيد أو استخدام أمر `/setamount` لتعديل المبلغ."
                    )
                    logger.warning(
                        "Insufficient balance to open position for %s: available=%.4f %s < required=%.4f %s (trade=%.4f + fee=%s). Dropping BUY.",
                        signal_item.token_address[:10],
                        available_cash,
                        unit,
                        total_needed,
                        unit,
                        preliminary_cost,
                        simulated_fee,
                    )
                    asyncio.create_task(self._broadcast_trade_alert(insufficient_alert))
                    continue

                has_reserved = self._position_book.has_reserved_slot(signal_item.token_address)
                if not has_reserved:
                    can_open, reject_reason = self._position_book.can_open_position(
                        token_address=signal_item.token_address,
                        chain=signal_item.chain,
                        allocated_cost_native=preliminary_cost,
                        total_equity_native=equity_native,
                        pool_address=getattr(signal_item, "pool_address", "") or "",
                        narrative_cluster=getattr(signal_item, "narrative_cluster", None),
                        is_manual_size=is_manual_size,
                    )
                    if not can_open:
                        logger.info(
                            "Dropping BUY signal for %s: %s",
                            signal_item.token_address[:10],
                            reject_reason,
                        )
                        if is_direct_dm:
                            dm_reject_alert = (
                                f"⚠️ **تعذر فتح صفقة جديدة للعملة عبر الرسائل المباشرة**\n\n"
                                f"• **العملة:** `{signal_item.token_address}`\n"
                                f"• **السبب:** {reject_reason}\n\n"
                                f"💡 يمكنك استخدام أمر `/trades` أو زر الإغلاق لإنهاء مركز مفتوح لإتاحة مساحة لصفقة جديدة."
                            )
                            asyncio.create_task(self._broadcast_trade_alert(dm_reject_alert))
                        continue
                    reserved = self._position_book.reserve_position_slot(token_address=signal_item.token_address)
                    if not reserved:
                        logger.info("Dropping BUY signal for %s: position slot reservation failed", signal_item.token_address[:10])
                        continue

            if not self.validate_signal_strength(signal_item):
                logger.info(
                    "Signal for %s dropped: strength %s does not meet required threshold",
                    getattr(signal_item, "token_address", "")[:10],
                    getattr(signal_item, "strength", None),
                )
                if signal_item.suggested_side == OrderSide.BUY:
                    self._position_book.release_position_slot(token_address=signal_item.token_address)
                continue

            if signal_item.pool_state is None or signal_item.pool_state.spot_price_native_per_token <= Decimal(0):
                signal_item.pool_state = self.get_or_create_initial_pool_state(
                    chain=signal_item.chain,
                    token_address=signal_item.token_address,
                    pool_address=getattr(signal_item, "pool_address", "") or "",
                )

            # True Normalized Market State Feature Vector Extraction
            feature_vec_obj = PatternFeatureVector.from_signal(
                signal=signal_item,
                sec=getattr(signal_item, "security_report", None),
                wallet_profile=getattr(signal_item, "wallet_profile", None),
            )
            entry_feature_vector = feature_vec_obj.to_vector()

            # Record Shadow Lots for Tournament Registry
            if hasattr(self, "_strategy_tournament") and self._strategy_tournament and signal_item.pool_state:
                native_p = self._eth_price if signal_item.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
                try:
                    self._strategy_tournament.evaluate_signal_and_record_shadow_lots(
                        signal=signal_item,
                        pool=signal_item.pool_state,
                        native_price_usd=native_p,
                    )
                except Exception as exc:
                    logger.debug("Shadow tournament evaluation skipped for %s: %s", signal_item.token_address[:10], exc)

            # Unified Ruflo Multi-Agent Swarm Deliberation & BFT Consensus
            if signal_item.suggested_side == OrderSide.BUY and self._swarm_coordinator:
                try:
                    swarm_result = await self._swarm_coordinator.execute_trade_consensus(
                        token_address=signal_item.token_address,
                        chain=signal_item.chain,
                        signal=signal_item,
                        feature_vector=entry_feature_vector,
                        pool=signal_item.pool_state,
                        report=getattr(signal_item, "security_report", None),
                        execute_fill=False,
                    )
                    if swarm_result and not swarm_result.passed:
                        logger.info(
                            "Ruflo Swarm BFT VETO enforced for %s: %s (votes: %s)",
                            signal_item.token_address[:10],
                            swarm_result.rationale,
                            swarm_result.agent_votes,
                        )
                        self._record_gatekeeper_eval(
                            signal_item.token_address,
                            signal_item.chain,
                            f"Ruflo Swarm Veto: {swarm_result.rationale}",
                            passed=False,
                        )
                        self._position_book.release_position_slot(token_address=signal_item.token_address)
                        continue
                except Exception as exc:
                    logger.error("Ruflo Swarm consensus raised for %s: %s", signal_item.token_address[:10], exc)

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
                        self._position_book.release_position_slot(token_address=signal_item.token_address)
                        continue

                    ai_action_params = ai_audit.action_parameters
                except Exception as exc:  # noqa: BLE001
                    logger.error("AlphaSupervisor-AI audit raised for %s: %s", signal_item.token_address[:10], exc)

            equity = self._current_equity_usd()
            fill = await executor.execute_signal(
                signal_item,
                portfolio_equity_usd=equity,
                is_new_launch=is_new_launch,
                fixed_trade_amount_native=preliminary_cost if (signal_item.suggested_side == OrderSide.BUY and is_manual_size) else None,
            )
            if fill is None or fill.effective_price <= Decimal(0):
                if fill is not None and fill.effective_price <= Decimal(0):
                    logger.warning(
                        "Fill rejected for %s: zero or negative effective price: %s",
                        signal_item.token_address[:10],
                        fill.effective_price,
                    )
                if signal_item.suggested_side == OrderSide.BUY:
                    self._position_book.release_position_slot(token_address=signal_item.token_address)
                continue

            realized_pnl_usd: Decimal | None = None

            if fill.side == OrderSide.BUY:
                if is_new_launch:
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
                    partial_exit_enabled=getattr(self.config, "partial_exit_enabled", True),
                    strategy_name=getattr(self, "active_strategy", getattr(self.config, "active_strategy", "default")),
                    trailing_stop_from_peak_pct=getattr(self, "trailing_stop_from_peak_pct", getattr(self.config, "trailing_stop_from_peak_pct", None)),
                    custom_take_profit_pct=getattr(self, "custom_take_profit_pct", getattr(self.config, "custom_take_profit_pct", None)),
                    custom_stop_loss_pct=getattr(self, "custom_stop_loss_pct", getattr(self.config, "custom_stop_loss_pct", None)),
                    hard_stop_loss_pct=getattr(self, "hard_stop_loss_pct", getattr(self.config, "hard_stop_loss_pct", 0.20)),
                    entry_feature_vector=entry_feature_vector,
                )

                if self._ledger:
                    await self._ledger.record_lot_transition(
                        lot_id=lot.lot_id,
                        token_address=lot.token_address,
                        chain=lot.chain.value,
                        status=LotStatus.OPEN.value,
                    )
                self._position_book.commit_slot_to_lot_by_token(signal_item.token_address, lot.lot_id)
                if signal_item.pool_state is not None and lot.pool_address:
                    self._pool_registry.set(lot.pool_address, signal_item.pool_state)
                native_price = self._eth_price if signal_item.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
                gas_usd = fill.simulated_gas_cost_usd or Decimal("0.05")
                gas_native = (gas_usd / native_price).quantize(Decimal("1e-18")) if native_price > Decimal(0) else Decimal("0.0005")

                if signal_item.chain == ChainIdentifier.BASE_MAINNET:
                    self._cash_eth -= (fill.simulated_native_spent + gas_native)
                else:
                    self._cash_sol -= (fill.simulated_native_spent + gas_native)

                self._metrics.total_gas_usd += gas_usd
                lot.cost_basis_native = fill.simulated_native_spent + gas_native

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

                if getattr(signal_item, "exit_profile", None) == ExitProfile.EARLY_RALLY_SWING or getattr(signal_item, "source", None) == SignalSource.EARLY_RALLY_BREAKOUT:
                    buy_alert = (
                        f"⚡ **EARLY RALLY BREAKOUT DETECTED**\n\n"
                        f"• **Platform:** `{platform_name}`\n"
                        f"• **Token:** `{t_str}` (`{fill.token_address}`)\n"
                        f"• **Chain:** {signal_item.chain.value}\n"
                        f"• **Strategy Profile:** `EARLY_RALLY_SWING`\n"
                        f"• **Buy Price:** `{p_str}`\n"
                        f"• **Spent:** `{spent_val:.4f} {unit}`\n"
                        f"• **Tokens Acquired:** `{tok_val:,.2f}`\n"
                        f"• **Market Cap at Entry:** `${float(sig_mcap or 0.0):,.0f}`\n"
                        f"• **Volume Surge:** `{float(getattr(signal_item, 'volume_surge_multiplier', 0.0)):.1f}x` | **Net Buy Delta:** `{float(getattr(signal_item, 'net_buy_delta', 0.0))*100:.1f}%`\n"
                        f"• **Trade Plan:** Hard SL `-15%` | Scale-out 50% at `+50%` | Runner Trail `28%` from ATH\n"
                        f"• **Order ID:** `{fill.order_id[:8]}`"
                    )
                elif getattr(signal_item, "exit_profile", None) == ExitProfile.REVIVAL_SWING:
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
                    is_new_launch=is_new_launch,
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
                    "is_new_launch": getattr(fill, "is_new_launch", False),
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

                # 3. Query external top gainers / boosts and cross-reference with audited & vetoed tokens
                if self._session and not self._session.closed:
                    try:
                        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
                        async with self._session.get(
                            "https://api.dexscreener.com/token-boosts/top/v1",
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=8.0),
                        ) as resp:
                            if resp.status == 200:
                                boosts = await resp.json()
                                if isinstance(boosts, list):
                                    sol_mints = [
                                        b.get("tokenAddress") for b in boosts
                                        if isinstance(b, dict) and b.get("chainId") == "solana" and b.get("tokenAddress")
                                    ][:20]
                                    if sol_mints:
                                        batch_param = ",".join(sol_mints)
                                        url = f"https://api.dexscreener.com/latest/dex/tokens/{batch_param}"
                                        async with self._session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8.0)) as b_resp:
                                            if b_resp.status == 200:
                                                data = await b_resp.json()
                                                pairs = data.get("pairs", [])
                                                if isinstance(pairs, list):
                                                    market_gainers = []
                                                    for p in pairs:
                                                        ch = p.get("priceChange", {})
                                                        pct = max(float(ch.get("h24") or 0.0), float(ch.get("h6") or 0.0), float(ch.get("h1") or 0.0))
                                                        if pct >= 100.0:  # >= 2x
                                                            base = p.get("baseToken", {})
                                                            market_gainers.append({
                                                                "token_address": base.get("address", ""),
                                                                "symbol": base.get("symbol", ""),
                                                                "multiplier": 1.0 + pct / 100.0,
                                                                "priceChange": ch,
                                                                "source": "DexScreener",
                                                                "volume_surge_multiplier": 3.0,
                                                                "net_buy_delta": 0.70,
                                                                "dip_depth_pct": 35.0,
                                                                "consolidation_duration_s": 1800.0,
                                                            })

                                                    if market_gainers:
                                                        # Aggregate audited outcomes & gatekeeper negative cache
                                                        audited_outcomes = dict(getattr(self._ai_supervisor, "_outcome_tracker", {}))
                                                        if hasattr(self, "_gatekeeper") and self._gatekeeper:
                                                            neg_cache = getattr(self._gatekeeper, "_negative_cache", {})
                                                            for t_addr, reason in neg_cache.items():
                                                                if t_addr not in audited_outcomes:
                                                                    from types import SimpleNamespace
                                                                    audited_outcomes[t_addr] = SimpleNamespace(
                                                                        decision="PASS",
                                                                        flags=[str(reason)],
                                                                    )

                                                        staged_map = dict(self._wave2_buffer.get_all_staged())
                                                        await self._feedback.missed_opportunity_analyzer.cross_reference_market_gainers(
                                                            market_gainers=market_gainers,
                                                            audited_outcomes=audited_outcomes,
                                                            staged_tokens=staged_map,
                                                        )
                    except Exception as exc:
                        logger.debug("Missed opportunity live gainer fetch skipped: %s", exc)

            except Exception as exc:  # noqa: BLE001
                logger.error("Error in missed opportunity watchdog: %s", exc)

    async def _screener_priority_poller(self) -> None:
        """
        Continuously polls Graduated and Movers platform opportunities every 20-30 seconds,
        verifying the $10,000 USD minimum market cap before full analysis and execution.
        """
        logger.info("Continuous Screener Priority Poller active (polling every 20-30s).")
        while not self._shutdown_event.is_set():
            poll_interval = float(getattr(self, "screener_poll_interval_s", getattr(self._cfg, "screener_poll_interval_s", 25.0)))
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=poll_interval)
                break
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

            if self._shutdown_event.is_set():
                break

            try:
                await self._poll_graduated_and_movers()
            except Exception as exc:
                logger.debug("Error in screener priority poller: %s", exc)

    async def _poll_graduated_and_movers(self) -> None:
        """
        Queries live DexScreener token profiles/boosts and internal pool metrics for Solana tokens
        meeting the strict >= $10,000 market cap and genuine participant criteria.
        """
        session = self._session
        if session is None or session.closed:
            return

        min_mcap = Decimal(str(getattr(self, "min_market_cap_usd", getattr(self._cfg, "min_market_cap_usd", 10000.0))))
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

        try:
            async with session.get(
                "https://api.dexscreener.com/token-boosts/top/v1",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=8.0),
            ) as resp:
                if resp.status != 200:
                    return
                boosts = await resp.json()
        except Exception as exc:
            logger.debug("DexScreener top boosts poll skipped: %s", exc)
            return

        if not isinstance(boosts, list):
            return

        sol_addrs = [
            b.get("tokenAddress")
            for b in boosts
            if isinstance(b, dict) and b.get("chainId") == "solana" and b.get("tokenAddress")
        ]
        if not sol_addrs:
            return

        unique_addrs = list(dict.fromkeys(sol_addrs))[:30]
        batch_param = ",".join(unique_addrs)

        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{batch_param}"
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=8.0),
            ) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
        except Exception as exc:
            logger.debug("DexScreener batch tokens poll skipped: %s", exc)
            return

        pairs = data.get("pairs", [])
        if not isinstance(pairs, list):
            return

        seen_mints: set[str] = set()
        for p in pairs:
            if not isinstance(p, dict) or p.get("chainId") != "solana":
                continue

            base_tok = p.get("baseToken", {})
            mint = base_tok.get("address")
            if not mint or mint in seen_mints:
                continue

            # Skip tokens already in an active open position
            if self.is_position_open(mint, ChainIdentifier.SOLANA_MAINNET):
                continue

            mcap_val = p.get("marketCap")
            if mcap_val is None:
                continue
            try:
                mcap = Decimal(str(mcap_val))
            except Exception:
                continue

            # Requirement 3: Enforce strict $10,000 USD minimum market cap before full analysis
            if mcap < min_mcap:
                continue

            seen_mints.add(mint)
            pool_addr = p.get("pairAddress", "")
            dex_id = (p.get("dexId") or "").lower()
            liq_usd = float(p.get("liquidity", {}).get("usd") or 0.0)
            vol_5m = float(p.get("volume", {}).get("m5") or 0.0)
            vol_2m = vol_5m * (2.0 / 5.0)
            price_usd = float(p.get("priceUsd") or 0.0)
            price_change_5m = float(p.get("priceChange", {}).get("m5") or 0.0)

            is_graduated = dex_id in ("raydium", "meteora", "orca", "pumpswap") or mint.endswith("pump")
            is_mover = price_change_5m >= 3.0 or vol_5m >= 5000.0

            if is_graduated and is_mover:
                sec = ScreenerSection.BOTH
            elif is_graduated:
                sec = ScreenerSection.GRADUATED
            elif is_mover:
                sec = ScreenerSection.MOVERS
            else:
                sec = ScreenerSection.GRADUATED

            pool_st = self._pool_registry.get(mint) or (self._pool_registry.get(pool_addr) if pool_addr else None)
            if pool_st is None and liq_usd > 0:
                sol_p = Decimal(str(getattr(self._cfg, "sol_price_usd", self._sol_price or 150.0)))
                liq_dec = Decimal(str(liq_usd))
                native_res = liq_dec / Decimal("2.0") / sol_p if sol_p > 0 else Decimal("100.0")
                token_res = (liq_dec / Decimal("2.0")) / Decimal(str(price_usd or 0.001)) if (price_usd or 0.001) > 0 else Decimal("1000000.0")
                pool_st = PoolState(
                    pool_address=pool_addr or mint,
                    chain=ChainIdentifier.SOLANA_MAINNET,
                    token_reserve=token_res,
                    native_reserve=native_res,
                    fee_numerator=25,
                    fee_denominator=10000,
                    last_updated_block=1000,
                    token_decimals=6,
                    native_decimals=9,
                )
                self._pool_registry.set(mint, pool_st)
                if pool_addr:
                    self._pool_registry.set(pool_addr, pool_st)

            event = ScreenerDiscoveryEvent(
                mint=mint,
                token_address=mint,
                pool_address=pool_addr,
                section=sec,
                market_cap_usd=float(mcap),
                volume_2m_usd=vol_2m,
                liquidity_usd=liq_usd,
                current_price=price_usd,
                price_change_pct_2m=price_change_5m * 0.4,
                raw_metadata=p,
                pool_state=pool_st,
            )

            await self.evaluate_screener_token(event)

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
            pool = self._pool_registry.get(lot.token_address, expected_token=lot.token_address)
            if pool is None and lot.pool_address:
                pool = self._pool_registry.get(lot.pool_address, expected_token=lot.token_address)
            curr_p = pool.spot_price_native_per_token if (pool and getattr(pool, "last_updated_block", 0) > 0) else lot.entry_price
            native_p = self._eth_price if lot.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
            unrealized_usd = (lot.tokens_held * (curr_p - lot.entry_price)) * native_p
            pos_val_usd = float(lot.tokens_held * curr_p * native_p)
            dur_s = (now_ns - lot.open_timestamp_ns) / 1e9 if lot.open_timestamp_ns > 0 else 0.0
            pnl_pct = float((curr_p - lot.entry_price) / lot.entry_price * 100) if lot.entry_price > 0 else 0.0
            if lot.trailing_stop_active:
                if lot.trailing_stop_price >= lot.entry_price * Decimal("1.30"):
                    trailing_status = "LOCKED (+30%)"
                elif lot.trailing_stop_price >= lot.entry_price * Decimal("1.10"):
                    trailing_status = "LOCKED (+10%)"
                elif lot.trailing_stop_price >= lot.entry_price:
                    trailing_status = "LOCKED (BE)"
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
                "is_new_launch": getattr(lot, "is_new_launch", False),
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


    async def _poll_open_lots_prices(self, lots: list[OpenLot]) -> None:
        """
        Actively poll DexScreener for open positions that lack live WebSocket updates.
        Updates PoolState with authoritative market spot price and liquidity reserves.
        If a token is verified dead/removed with 0 liquidity, seeds a collapsed reserve
        so emergency exit triggers cleanly instead of deadlocking active capacity.
        """
        if not lots or self._session is None or self._session.closed:
            return

        now = time.time()
        # Throttle external calls to at most once every 2 seconds
        if hasattr(self, "_last_open_lot_poll_s") and (now - self._last_open_lot_poll_s) < 2.0:
            return
        self._last_open_lot_poll_s = now

        sol_lots = [
            l for l in lots
            if l.chain == ChainIdentifier.SOLANA_MAINNET
            and not (l.bonding_curve_mode and self._pool_registry.get(l.token_address, expected_token=l.token_address) is not None)
        ]
        evm_lots = [l for l in lots if l.chain == ChainIdentifier.BASE_MAINNET]

        # 1. Solana Lots
        if sol_lots:
            mints = list(dict.fromkeys(l.token_address for l in sol_lots if l.token_address))[:30]
            if mints:
                try:
                    batch_param = ",".join(mints)
                    url = f"https://api.dexscreener.com/latest/dex/tokens/{batch_param}"
                    async with self._session.get(
                        url,
                        headers={"User-Agent": "Bot-MM-Engine/1.0"},
                        timeout=aiohttp.ClientTimeout(total=5.0),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            pairs = data.get("pairs", [])
                            pairs_by_mint: dict[str, list[dict[str, Any]]] = {}
                            if isinstance(pairs, list):
                                for p in pairs:
                                    base_addr = p.get("baseToken", {}).get("address")
                                    if base_addr:
                                        pairs_by_mint.setdefault(base_addr, []).append(p)

                            sol_price = Decimal(str(getattr(self.config, "sol_price_usd", self._sol_price or 180.0)))
                            for lot in sol_lots:
                                t_addr = lot.token_address
                                t_pairs = [p for p in pairs_by_mint.get(t_addr, []) if str(p.get("chainId", "")).lower() == "solana"]
                                
                                selected_pair = None
                                # Explicit pool mapping: check designated lot pool_address first
                                if lot.pool_address:
                                    selected_pair = next(
                                        (p for p in t_pairs if str(p.get("pairAddress", "")).lower() == str(lot.pool_address).lower()),
                                        None,
                                    )
                                # If entry pool is unavailable, allow alternate pool only through an explicit, validated routing decision
                                if selected_pair is None and t_pairs:
                                    best_alt = max(
                                        t_pairs,
                                        key=lambda x: float((x.get("liquidity") or {}).get("usd") or 0.0),
                                    )
                                    alt_liq = float((best_alt.get("liquidity") or {}).get("usd") or 0.0)
                                    alt_addr = best_alt.get("pairAddress")
                                    if alt_addr and alt_liq > 0:
                                        logger.info(
                                            "Explicit routing decision for lot %s (%s): routing from %s to alternate pair %s (liquidity=$%.2f)",
                                            lot.lot_id[:8],
                                            t_addr[:10],
                                            lot.pool_address,
                                            alt_addr,
                                            alt_liq,
                                        )
                                        lot.pool_address = alt_addr
                                        selected_pair = best_alt

                                if selected_pair is not None:
                                    price_usd = float(selected_pair.get("priceUsd") or 0.0)
                                    liq_usd = float((selected_pair.get("liquidity") or {}).get("usd") or 0.0)
                                    pair_addr = selected_pair.get("pairAddress") or lot.pool_address
                                    if price_usd > 0 and pair_addr:
                                        spot_p = Decimal(str(price_usd)) / sol_price if sol_price > Decimal(0) else Decimal("0.000000028")
                                        liq_dec = Decimal(str(liq_usd))
                                        # Strictly derive reserves from selected pair; never mix with another pool's reserves
                                        if liq_dec > Decimal(0) and sol_price > Decimal(0):
                                            native_res = liq_dec / Decimal("2.0") / sol_price
                                            token_res = native_res / spot_p
                                        else:
                                            native_res = Decimal("50.0")
                                            token_res = native_res / spot_p
                                        pool = PoolState(
                                            pool_address=pair_addr,
                                            chain=ChainIdentifier.SOLANA_MAINNET,
                                            token_address=t_addr,
                                            token_reserve=token_res,
                                            native_reserve=native_res,
                                            fee_numerator=25,
                                            fee_denominator=10000,
                                            last_updated_block=1000,
                                            timestamp_s=now,
                                            token_decimals=6,
                                            native_decimals=9,
                                        )
                                        self._pool_registry.set(pair_addr, pool)
                                        lot.pool_address = pair_addr
                                        lot.unresolved_price = False
                                        lot.stale_price_retries = 0
                                        lot.price_unresolved_since_s = 0.0
                                        logger.info(
                                            "Active price poller updated open lot %s (%s) from pool %s: price=%s SOL ($%.6f), reserve=%.2f SOL",
                                            lot.lot_id[:8],
                                            t_addr[:10],
                                            pair_addr[:10],
                                            spot_p,
                                            price_usd,
                                            float(native_res),
                                        )
                                else:
                                    lot.stale_price_retries += 1
                                    lot.unresolved_price = True
                                    if lot.price_unresolved_since_s <= 0.0:
                                        lot.price_unresolved_since_s = now
                                    logger.warning(
                                        "Active price poller: lot %s (%s) has no validated executable market pair (retries=%d). Marking unresolved.",
                                        lot.lot_id[:8],
                                        t_addr[:10],
                                        lot.stale_price_retries,
                                    )
                except Exception as exc:
                    logger.debug("Active price poll for Solana lots encountered error: %s", exc)

        # 2. Base Lots
        if evm_lots:
            mints = list(dict.fromkeys(l.token_address for l in evm_lots if l.token_address))[:30]
            if mints:
                try:
                    batch_param = ",".join(mints)
                    url = f"https://api.dexscreener.com/latest/dex/tokens/{batch_param}"
                    async with self._session.get(
                        url,
                        headers={"User-Agent": "Bot-MM-Engine/1.0"},
                        timeout=aiohttp.ClientTimeout(total=5.0),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            pairs = data.get("pairs", [])
                            pairs_by_mint = {}
                            if isinstance(pairs, list):
                                for p in pairs:
                                    base_addr = str(p.get("baseToken", {}).get("address", "")).lower()
                                    if base_addr:
                                        pairs_by_mint.setdefault(base_addr, []).append(p)

                            eth_price = Decimal(str(getattr(self.config, "eth_price_usd", self._eth_price or 2500.0)))
                            for lot in evm_lots:
                                t_addr = lot.token_address.lower()
                                t_pairs = [p for p in pairs_by_mint.get(t_addr, []) if str(p.get("chainId", "")).lower() == "base"]

                                selected_pair = None
                                if lot.pool_address:
                                    selected_pair = next(
                                        (p for p in t_pairs if str(p.get("pairAddress", "")).lower() == str(lot.pool_address).lower()),
                                        None,
                                    )
                                if selected_pair is None and t_pairs:
                                    best_alt = max(
                                        t_pairs,
                                        key=lambda x: float((x.get("liquidity") or {}).get("usd") or 0.0),
                                    )
                                    alt_liq = float((best_alt.get("liquidity") or {}).get("usd") or 0.0)
                                    alt_addr = best_alt.get("pairAddress")
                                    if alt_addr and alt_liq > 0:
                                        logger.info(
                                            "Explicit routing decision for Base lot %s (%s): routing from %s to alternate pair %s (liquidity=$%.2f)",
                                            lot.lot_id[:8],
                                            t_addr[:10],
                                            lot.pool_address,
                                            alt_addr,
                                            alt_liq,
                                        )
                                        lot.pool_address = alt_addr
                                        selected_pair = best_alt

                                if selected_pair is not None:
                                    price_usd = float(selected_pair.get("priceUsd") or 0.0)
                                    liq_usd = float((selected_pair.get("liquidity") or {}).get("usd") or 0.0)
                                    pair_addr = selected_pair.get("pairAddress") or lot.pool_address
                                    if price_usd > 0 and pair_addr:
                                        spot_p = Decimal(str(price_usd)) / eth_price if eth_price > Decimal(0) else Decimal("0.0000001")
                                        liq_dec = Decimal(str(liq_usd))
                                        if liq_dec > Decimal(0) and eth_price > Decimal(0):
                                            native_res = liq_dec / Decimal("2.0") / eth_price
                                            token_res = native_res / spot_p
                                        else:
                                            native_res = Decimal("5.0")
                                            token_res = native_res / spot_p
                                        pool = PoolState(
                                            pool_address=pair_addr,
                                            chain=ChainIdentifier.BASE_MAINNET,
                                            token_address=lot.token_address,
                                            token_reserve=token_res,
                                            native_reserve=native_res,
                                            fee_numerator=30,
                                            fee_denominator=10000,
                                            last_updated_block=1000,
                                            timestamp_s=now,
                                            token_decimals=18,
                                            native_decimals=18,
                                        )
                                        self._pool_registry.set(pair_addr, pool)
                                        lot.pool_address = pair_addr
                                        lot.unresolved_price = False
                                        lot.stale_price_retries = 0
                                        lot.price_unresolved_since_s = 0.0
                                else:
                                    lot.stale_price_retries += 1
                                    lot.unresolved_price = True
                                    if lot.price_unresolved_since_s <= 0.0:
                                        lot.price_unresolved_since_s = now
                                    logger.warning(
                                        "Active price poller: Base lot %s (%s) has no validated executable market pair (retries=%d). Marking unresolved.",
                                        lot.lot_id[:8],
                                        t_addr[:10],
                                        lot.stale_price_retries,
                                    )
                except Exception as exc:
                    logger.debug("Active price poll for Base lots encountered error: %s", exc)

    async def _position_price_poller(self) -> None:
        """
        Adaptive High-Speed Exit Monitor Loop (Directive 2 & 4).
        Runs every 500ms. Evaluates dynamic bonding-curve exits:
        - Active price polling fallback via DexScreener for lots lacking live WebSocket ticks.
        - Stale tick guard: ticks > 3.0s old halt exit triggers.
        - Slippage insolvency guard: curves unable to absorb lot without >15% collapse trigger emergency dump.
        - Velocity/time decay: duration > 120s and PnL < +3% triggers liquidation.
        - Hard SL: prioritized if price impact > 300 bps or independent entry stop reached.
        - Trailing Stop: dynamic trailing from ATH peak.
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

                # Actively resolve market prices for lots lacking fresh live ticks or with unresolved status
                lots_needing_price = [
                    l for l in open_lots
                    if (
                        l.unresolved_price
                        or (l.pool_address and self._pool_registry.get_pool(l.pool_address) is None)
                        or (
                            not (l.bonding_curve_mode and self._pool_registry.get_pool(l.pool_address) is not None)
                            and (
                                (l.pool_address and (now_s - getattr(self._pool_registry.get_pool(l.pool_address), "timestamp_s", 0) > 3.0))
                                or (not l.pool_address and self._pool_registry.get(l.token_address, expected_token=l.token_address) is None)
                            )
                        )
                    )
                ]
                if lots_needing_price:
                    await self._poll_open_lots_prices(lots_needing_price)

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

                    pool = None
                    if lot.pool_address:
                        pool = self._pool_registry.get_pool(lot.pool_address) or self._pool_registry.get(lot.pool_address, expected_token=lot.token_address)
                    if pool is None:
                        pool = self._pool_registry.get(lot.token_address, expected_token=lot.token_address)

                    # If pool is missing or tick is stale, request fresh price
                    if pool is None or (now_s - getattr(pool, "timestamp_s", 0)) > 3.0:
                        await self._poll_open_lots_prices([lot])
                        if lot.pool_address:
                            pool = self._pool_registry.get_pool(lot.pool_address) or self._pool_registry.get(lot.pool_address, expected_token=lot.token_address)
                        if pool is None:
                            pool = self._pool_registry.get(lot.token_address, expected_token=lot.token_address)

                    dur_s = (now_ns - lot.open_timestamp_ns) / 1e9 if lot.open_timestamp_ns > 0 else 0.0
                    if pool is None or getattr(pool, "last_updated_block", 0) <= 0 or lot.unresolved_price:
                        if dur_s > 120.0 or lot.stale_price_retries >= 5:
                            native_price = self._eth_price if lot.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
                            logger.error(
                                "Lot %s (%s) market is genuinely illiquid or unavailable (retries=%d, dur=%.1fs). Safely writing off under ILLIQUID_MARKET without fabricating synthetic market quotes.",
                                lot.lot_id[:8],
                                lot.token_address[:10],
                                lot.stale_price_retries,
                                dur_s,
                            )
                            lot.is_illiquid = True
                            lot.status = LotStatus.CLOSED
                            lot.unresolved_price = False
                            self._position_book.set_token_cooldown(lot.token_address, duration_s=300.0)
                            if self._ledger:
                                loss_usd = -float(lot.cost_basis_native * native_price)
                                await self._ledger.record_lot_transition(
                                    lot_id=lot.lot_id,
                                    token_address=lot.token_address,
                                    chain=lot.chain.value,
                                    status=LotStatus.CLOSED.value,
                                    exit_reason=TradeExitReason.ILLIQUID_MARKET.value,
                                    pnl_usd=loss_usd,
                                )
                                await self._ledger.record_lot_transition(
                                    lot_id=lot.lot_id,
                                    token_address=lot.token_address,
                                    chain=lot.chain.value,
                                    status=LotStatus.SETTLED.value,
                                    exit_reason=TradeExitReason.ILLIQUID_MARKET.value,
                                    pnl_usd=loss_usd,
                                )
                            self._position_book._notify_capacity_freed()
                            continue
                        else:
                            logger.debug(
                                "Awaiting valid market quote for lot %s (%s) before evaluating exits (pool uninitialized or tick unresolved).",
                                lot.lot_id[:8],
                                lot.token_address[:10],
                            )
                            continue

                    current_price = pool.spot_price_native_per_token
                    self._ai_supervisor.record_price_update(
                        lot.token_address,
                        current_price,
                        now_s,
                    )
                    # Update shadow strategy tournament prices
                    if hasattr(self, "_strategy_tournament") and self._strategy_tournament:
                        native_p = self._eth_price if lot.chain == ChainIdentifier.BASE_MAINNET else self._sol_price
                        self._strategy_tournament.update_token_prices(
                            token_address=lot.token_address,
                            current_price=current_price,
                            native_price_usd=native_p,
                        )
                    tick_timestamp_s = getattr(pool, "timestamp_s", None) or now_s
                    grace_sec = getattr(self.config, "exit_grace_period_sec", 15.0)
                    decision = self._position_book.evaluate_lot_exit(
                        lot=lot,
                        current_price=current_price,
                        current_pool_reserve_native=pool.native_reserve,
                        tick_timestamp_s=tick_timestamp_s,
                        current_timestamp_ns=now_ns,
                        bonding_curve_mode=lot.bonding_curve_mode,
                        exit_grace_period_sec=grace_sec,
                        hard_stop_loss_pct=getattr(self, "hard_stop_loss_pct", 0.20),
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
                            min_fill_timestamp_ns=lot.open_timestamp_ns,
                        )
                        if exit_fill is not None and exit_fill.effective_price > Decimal(0):
                            native_price = (
                                self._eth_price
                                if decision.chain == ChainIdentifier.BASE_MAINNET
                                else self._sol_price
                            )
                            pnl_native, _ = self._position_book.apply_exit_decision(
                                decision, sell_price=exit_fill.effective_price, net_native_received=exit_fill.simulated_native_spent
                            )
                            exit_pnl_usd = (pnl_native * native_price).quantize(Decimal("0.01"))
                            if decision.chain == ChainIdentifier.BASE_MAINNET:
                                self._cash_eth += exit_fill.simulated_native_spent
                            else:
                                self._cash_sol += exit_fill.simulated_native_spent

                            new_equity = self._current_equity_usd()
                            is_launch = (getattr(lot, "exit_profile", None) not in (ExitProfile.REVIVAL_SWING, ExitProfile.EARLY_RALLY_SWING) and 
                                         getattr(lot, "bonding_curve_mode", False) is True)
                            if exit_fill.simulated_gas_cost_usd:
                                self._metrics.total_gas_usd += exit_fill.simulated_gas_cost_usd
                            self._metrics.record_closed_trade(
                                realized_pnl_usd=exit_pnl_usd,
                                gas_cost_usd=Decimal(0),
                                current_equity_usd=new_equity,
                                is_new_launch=is_launch,
                            )
                            if lot.status == LotStatus.CLOSED:
                                self._position_book.set_token_cooldown(decision.token_address, duration_s=300.0)

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
                            tuned_params = self._feedback.parameter_tuner.update_from_performance(
                                win_rate_24h=self._metrics.win_rate_pct,
                                realized_pnl_usd=realized_pnl,
                                total_trades_24h=self._metrics.total_trades,
                            )
                            self._propagate_dynamic_hyperparameters(tuned_params)

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
                                entry_feature_vector=getattr(lot, "entry_feature_vector", []),
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

        connector = aiohttp.TCPConnector(
            family=socket.AF_INET,
            ttl_dns_cache=300,
            limit=100,
            enable_cleanup_closed=True,
        )
        async with (
            aiohttp.ClientSession(
                headers={"User-Agent": "Bot-MM-Engine/1.0"},
                connector=connector,
            ) as session,
            SQLiteLedger(cfg.db_path) as ledger,
        ):
            self._session = session
            self._market_oracle = MarketOracle(session=session)
            self._ledger = ledger
            self.tracer.ledger = self._ledger
            self.tracer.start_worker()
            await self._sync_ledger_settings()

            # Restore open lots from ledger to prevent state loss across restarts
            open_lots = await self._ledger.restore_open_positions_from_ledger()
            if open_lots:
                max_age_s = float(getattr(self.config, "max_open_lot_age_s", 43200.0))
                restored_count = self._position_book.restore_open_lots(open_lots, max_age_s=max_age_s)
                logger.info("Restored %d open lots from ledger into PositionBook on startup.", restored_count)
                if len(open_lots) > restored_count:
                    discarded = len(open_lots) - restored_count
                    logger.warning(
                        "Discarded %d stale zombie open lots (>%.1fh old) on startup.",
                        discarded,
                        max_age_s / 3600.0,
                    )
                    now_ns = time.time_ns()
                    for lot_dict in open_lots:
                        open_ts_ns = int(lot_dict.get("fill_timestamp_ns") or 0)
                        if open_ts_ns > 0 and (now_ns - open_ts_ns) / 1e9 > max_age_s:
                            t_addr = str(lot_dict.get("token_address", ""))
                            await self._ledger.mark_token_closed(t_addr, reason="STALE_ZOMBIE_EXPIRY")

                sol_mint = "So11111111111111111111111111111111111111112"
                for lot_dict in open_lots:
                    t_addr = str(lot_dict.get("token_address", "")).strip()
                    ch = str(lot_dict.get("chain", "")).lower()
                    plat = str(lot_dict.get("platform", "")).lower()
                    if t_addr and (ch == "solana_mainnet" or plat == "pump.fun" or t_addr.lower().endswith("pump")):
                        curve = lot_dict.get("pool_address")
                        if not curve or not is_valid_solana_pubkey(curve):
                            curve_pda, _ = derive_pump_fun_bonding_curve(t_addr)
                            if curve_pda:
                                curve = curve_pda
                        if curve and is_valid_solana_pubkey(curve):
                            svm_pool_registry[curve] = (t_addr, sol_mint, 6, 9)

            # Restore and resume any pending token requests from previous run
            pending_requests = await self._ledger.get_pending_token_requests()
            if pending_requests:
                logger.info("Restored %d pending token requests from ledger.", len(pending_requests))
                for req in pending_requests:
                    task = asyncio.create_task(self._process_single_token_request(req))
                    self._token_request_tasks.add(task)
                    task.add_done_callback(self._token_request_tasks.discard)

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
            self.screener_evaluator.gatekeeper = self._gatekeeper
            self.screener_evaluator.ledger = self._ledger

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

            if hasattr(self, "_strategy_tournament") and self._strategy_tournament:
                self._strategy_tournament.ledger = self._ledger

            if self._swarm_coordinator:
                self._swarm_coordinator.set_live_dependencies(
                    coordinator=coordinator,
                    gatekeeper=self._gatekeeper,
                    executor=self._executor,
                    ledger=self._ledger,
                    position_book=self._position_book,
                )
                await self._swarm_coordinator.start()

            async with coordinator:
                self._coordinator = coordinator
                self._telegram = coordinator.telegram_ingester
                if self._telegram and hasattr(self._telegram, "set_market_oracle"):
                    self._telegram.set_market_oracle(self._market_oracle)

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
                    # Do not unsubscribe if actively monitored in Wave2StagingBuffer, RevivalBreakoutBuffer, or an open position
                    in_wave2 = bool(self._wave2_buffer and self._wave2_buffer.is_staged(token_addr))
                    in_revival = bool(self._revival_buffer and self._revival_buffer.is_staged(token_addr))
                    is_open = bool(self.is_position_open(token_addr))
                    if in_wave2 or in_revival or is_open:
                        logger.debug(
                            "Runner._on_launch_buffer_removal: Retaining WebSocket telemetry for %s (wave2=%s, revival=%s, open_pos=%s)",
                            token_addr[:10],
                            in_wave2,
                            in_revival,
                            is_open,
                        )
                        return
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
                screener_poller_task = asyncio.create_task(
                    self._screener_priority_poller(), name="screener_poller"
                )
                user_request_task = asyncio.create_task(
                    self._process_user_request_queue(), name="user_request_processor"
                )

                worker_tasks = [bridge_task, ingest_task, signal_task, user_request_task, poller_task, snapshot_task, heartbeat_task, buffer_task, missed_opp_task, screener_poller_task]
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
                            asyncio.gather(ingest_task, signal_task, user_request_task, return_exceptions=True),
                            timeout=15.0,
                        )
                        logger.info("All queues successfully drained and processed.")
                    except asyncio.TimeoutError:
                        logger.warning("Queue drain timed out after 15s; proceeding with teardown.")
                        ingest_task.cancel()
                        signal_task.cancel()
                        user_request_task.cancel()

                    # 3. Cancel background monitoring tasks
                    user_request_task.cancel()
                    poller_task.cancel()
                    snapshot_task.cancel()
                    heartbeat_task.cancel()
                    buffer_task.cancel()
                    missed_opp_task.cancel()
                    screener_poller_task.cancel()
                    shutdown_waiter.cancel()
                    await asyncio.gather(user_request_task, poller_task, snapshot_task, heartbeat_task, buffer_task, missed_opp_task, screener_poller_task, return_exceptions=True)

                    # 3b. Stop multi-agent swarm orchestrator
                    if self._swarm_coordinator:
                        try:
                            await self._swarm_coordinator.stop()
                        except Exception:
                            pass

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


# Alias Runner and AlphaEngine for PaperTradingEngine
Runner = PaperTradingEngine
AlphaEngine = PaperTradingEngine


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
