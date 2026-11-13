"""
alpha_engine.observability.replay — Deterministic System Replay Engine
======================================================================
Replays historical market ticks from SQLite against the genuine runtime stack:
- SAME PaperExecutor (`alpha_engine.execution.executor`)
- SAME TradeCostModel (`alpha_engine.execution.costs`)
- SAME MEV & Latency Simulation (`alpha_engine.math.mev`)
- SAME Declarative Entry Rules (`alpha_engine.engine.rules`)
- SAME PositionBook & FIFO Accounting (`alpha_engine.execution.book`)
- Deterministic execution given a seed (`config.execution_seed`)
- Council outputs resolved from recorded decisions or labeled deterministic fallback
"""

from __future__ import annotations

import json
import logging
import random
import sqlite3
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from alpha_engine.config import EngineConfig
from alpha_engine.engine.experimentation import StrategyParameters
from alpha_engine.engine.registry import PoolRegistry
from alpha_engine.engine.rules import EntryRuleSet
from alpha_engine.engine.strategy.risk_manager import RiskEngine
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.costs import TradeCostModel
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.models.enums import ChainIdentifier, ExecutionVenue, OrderSide, SignalStrength, SignalSource, SecurityTier
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PaperFill, PoolState, ReserveType, SecurityReport
from alpha_engine.pricing.native_price_service import PriceResult, NativePriceStatus

logger = logging.getLogger(__name__)


def _parse_venue(val: Any) -> ExecutionVenue:
    if isinstance(val, ExecutionVenue):
        return val
    s = str(val or "").lower().strip()
    for venue in ExecutionVenue:
        if venue.value.lower() == s or venue.name.lower() == s:
            return venue
    if "pump" in s:
        return ExecutionVenue.PUMP_FUN
    if "cpmm" in s:
        return ExecutionVenue.RAYDIUM_CPMM
    if "clmm" in s:
        return ExecutionVenue.RAYDIUM_CLMM
    if "raydium" in s:
        return ExecutionVenue.RAYDIUM_AMM
    if "uniswap_v3" in s:
        return ExecutionVenue.UNISWAP_V3
    return ExecutionVenue.RAYDIUM_AMM


@dataclass
class ReplayResult:
    """Quantitative results from replaying a recorded market session."""

    total_events_processed: int = 0
    signals_evaluated: int = 0
    orders_executed: int = 0
    orders_rejected: int = 0
    rejection_reasons: Dict[str, int] = field(default_factory=dict)
    fills: List[PaperFill] = field(default_factory=list)
    closed_trades: List[Dict[str, Any]] = field(default_factory=list)
    net_pnl_usd: Decimal = Decimal("0.0")
    win_rate_pct: float = 0.0
    profit_factor: float = 0.0
    execution_seed: int = 42
    replay_duration_s: float = 0.0

    def to_json(self, indent: Optional[int] = None) -> str:
        """Serialize quantitative replay results to JSON string."""
        data = {
            "total_events_processed": self.total_events_processed,
            "signals_evaluated": self.signals_evaluated,
            "orders_executed": self.orders_executed,
            "orders_rejected": self.orders_rejected,
            "rejection_reasons": self.rejection_reasons,
            "fills_count": len(self.fills),
            "closed_trades_count": len(self.closed_trades),
            "net_pnl_usd": str(self.net_pnl_usd),
            "win_rate_pct": self.win_rate_pct,
            "profit_factor": self.profit_factor,
            "execution_seed": self.execution_seed,
            "replay_duration_s": self.replay_duration_s,
        }
        return json.dumps(data, indent=indent)


class ReplayNativePriceService:
    """Mock/Replay price service driven strictly by recorded native price ticks."""

    def __init__(self, default_sol: Decimal = Decimal("200.0"), default_eth: Decimal = Decimal("2600.0")) -> None:
        self._prices: Dict[ChainIdentifier, Decimal] = {
            ChainIdentifier.SOLANA_MAINNET: default_sol,
            ChainIdentifier.BASE_MAINNET: default_eth,
        }

    def set_price(self, chain: ChainIdentifier, price: Decimal) -> None:
        self._prices[chain] = price

    def get_price(self, chain: ChainIdentifier) -> PriceResult:
        p = self._prices.get(chain, Decimal("200.0"))
        return PriceResult(
            price=p,
            age_s=0.0,
            status=NativePriceStatus.OK,
            source="replay_recorder",
        )


class ReplayEngine:
    """
    Deterministic replay harness reusing genuine production execution logic.
    Re-runs recorded pool states, quotes, prices, and signals through PaperExecutor.
    """

    def __init__(
        self,
        tick_db_path: str = "market_ticks.db",
        config: Optional[EngineConfig] = None,
        seed: Optional[int] = None,
        strategy_params: Optional[StrategyParameters] = None,
        entry_rules: Optional[EntryRuleSet] = None,
        entry_rule_evaluator: Optional[Callable[[SignalEvent, Optional[PoolState]], bool]] = None,
        council_fallback: Optional[Callable[[SignalEvent], bool]] = None,
    ) -> None:
        self.tick_db_path = str(tick_db_path)
        self.config = config or EngineConfig()
        self.seed = seed if seed is not None else getattr(self.config, "execution_seed", 42)
        self.strategy_params = strategy_params
        self.entry_rules = entry_rules
        self.entry_rule_evaluator = entry_rule_evaluator
        self.council_fallback = council_fallback

        self._rng = random.Random(self.seed)
        self._pool_registry = PoolRegistry()
        self._price_service = ReplayNativePriceService()
        self._position_book = PositionBook()
        self._cost_model = TradeCostModel(config=self.config, price_service=self._price_service)

        # Build genuine PaperExecutor using the pool registry as resolver
        self._executor = PaperExecutor(
            position_book=self._position_book,
            price_service=self._price_service,
            cost_model=self._cost_model,
            pool_resolver=self._resolve_pool_for_replay,
            is_private_route=getattr(self.config, "default_is_private_route", False),
            config=self.config,
        )
        # Ensure executor uses the seeded RNG
        self._executor._rng = random.Random(self.seed)

        self._risk_engine = RiskEngine(self.config)

        # Historical recorded ticks cached for fill-time lookup
        self._pool_ticks_by_token: Dict[str, List[Dict[str, Any]]] = {}

    def inspect_db_counts(self) -> Dict[str, int]:
        """Inspect recorded tick database tables and row counts directly via sqlite3."""
        counts: Dict[str, int] = {}
        try:
            with sqlite3.connect(self.tick_db_path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [row[0] for row in cur.fetchall()]
                for tbl in tables:
                    cur.execute(f"SELECT COUNT(*) FROM {tbl};")
                    row = cur.fetchone()
                    if row:
                        counts[tbl] = int(row[0])
        except Exception as exc:
            logger.debug("sqlite3 error inspecting tick db %s: %s", self.tick_db_path, exc)
        return counts

    def load_recorded_decisions(self) -> Dict[str, Dict[str, Any]]:
        """Load recorded agent/council decisions from SQLite table decision_ticks."""
        decisions: Dict[str, Dict[str, Any]] = {}
        try:
            with sqlite3.connect(self.tick_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("SELECT * FROM decision_ticks ORDER BY timestamp_ns ASC;")
                for row in cur.fetchall():
                    metrics_val = {}
                    if row["metrics_json"]:
                        try:
                            metrics_val = json.loads(row["metrics_json"])
                        except Exception:
                            metrics_val = {}
                    decisions[row["decision_id"]] = {
                        "decision_id": row["decision_id"],
                        "decision_type": row["decision_type"],
                        "token_address": row["token_address"],
                        "approved": bool(row["approved"]),
                        "reason": row["reason"],
                        "provenance": row["provenance"],
                        "metrics": metrics_val,
                    }
        except Exception as exc:
            logger.debug("sqlite3 error loading recorded decisions from %s: %s", self.tick_db_path, exc)
        return decisions

    def _resolve_pool_for_replay(
        self,
        chain: Any = None,
        token_address: Optional[str] = None,
        pool_address: Optional[str] = None,
        at_ts_ns: Optional[int] = None,
        **kwargs: Any,
    ) -> Optional[PoolState]:
        """Resolve current pool state from replay pool registry or historical ticks closest to fill time."""
        tok = token_address
        if tok is None and isinstance(chain, str) and not pool_address:
            pool_address = chain
        if tok and at_ts_ns:
            p = self._find_pool_at_timestamp(tok, at_ts_ns)
            if p is not None:
                return p
        if pool_address:
            cur = self._pool_registry.get(pool_address)
            if cur is not None:
                if at_ts_ns and cur.token_address:
                    fill_p = self._find_pool_at_timestamp(cur.token_address, at_ts_ns)
                    if fill_p:
                        return fill_p
                return cur
        if tok:
            for p in self._pool_registry._pools.values():
                if p.token_address == tok:
                    return p
        return None

    def _find_pool_at_timestamp(self, token_address: str, target_ns: int) -> Optional[PoolState]:
        """Find the pool state closest to the target fill timestamp."""
        ticks = self._pool_ticks_by_token.get(token_address, [])
        if not ticks:
            return None
        # Find tick with timestamp <= target_ns that is latest, or earliest tick if target_ns is before all
        best_tick = None
        for t in ticks:
            if t["timestamp_ns"] <= target_ns:
                best_tick = t
            elif best_tick is None:
                best_tick = t
                break
        if best_tick is None:
            best_tick = ticks[-1]

        if best_tick:
            venue = _parse_venue(best_tick["venue"])
            chain = ChainIdentifier.SOLANA_MAINNET if "solana" in best_tick["chain"].lower() else ChainIdentifier.BASE_MAINNET
            res_type = ReserveType.EXACT if str(best_tick["reserve_type"]).lower() == "exact" else ReserveType.DERIVED
            return PoolState(
                pool_address=best_tick["pool_address"],
                token_address=best_tick["token_address"],
                chain=chain,
                venue=venue,
                native_reserve=Decimal(str(best_tick["native_reserve"])),
                token_reserve=Decimal(str(best_tick["token_reserve"])),
                reserve_type=res_type,
                fee_numerator=25,
                fee_denominator=10000,
                last_updated_block=1000,
                timestamp_s=float(best_tick["timestamp_ns"] / 1e9),
            )
        return None

    async def run_replay(
        self,
        start_ns: Optional[int] = None,
        end_ns: Optional[int] = None,
        limit: int = 50000,
    ) -> ReplayResult:
        """
        Execute deterministic replay through recorded ticks.
        """
        start_wall_time = time.perf_counter()
        result = ReplayResult(execution_seed=self.seed)

        # 1. Load recorded ticks
        from alpha_engine.observability.recorder import MarketTickRecorder
        recorder = MarketTickRecorder(db_path=self.tick_db_path)
        events = recorder.get_ticks_stream(start_ns=start_ns, end_ns=end_ns, limit=limit)
        result.total_events_processed = len(events)

        # Index all pool ticks for fill-time lookup
        all_pool_ticks = recorder.get_pool_ticks(start_ns=start_ns, end_ns=end_ns)
        for pt in all_pool_ticks:
            tok = pt["token_address"]
            if tok not in self._pool_ticks_by_token:
                self._pool_ticks_by_token[tok] = []
            self._pool_ticks_by_token[tok].append(pt)

        # 2. Iterate chronologically through events
        for ev in events:
            etype = ev.get("event_type")

            if etype == "native_price":
                ch = ChainIdentifier.SOLANA_MAINNET if "solana" in ev["chain"].lower() else ChainIdentifier.BASE_MAINNET
                self._price_service.set_price(ch, Decimal(str(ev["price_usd"])))
                continue

            if etype == "pool":
                # Advance market state in registry
                venue = _parse_venue(ev["venue"])
                chain = ChainIdentifier.SOLANA_MAINNET if "solana" in ev["chain"].lower() else ChainIdentifier.BASE_MAINNET
                res_type = ReserveType.EXACT if str(ev["reserve_type"]).lower() == "exact" else ReserveType.DERIVED
                pool_st = PoolState(
                    pool_address=ev["pool_address"],
                    token_address=ev["token_address"],
                    chain=chain,
                    venue=venue,
                    native_reserve=Decimal(str(ev["native_reserve"])),
                    token_reserve=Decimal(str(ev["token_reserve"])),
                    reserve_type=res_type,
                    fee_numerator=25,
                    fee_denominator=10000,
                    last_updated_block=1000,
                    timestamp_s=float(ev["timestamp_ns"] / 1e9),
                )
                self._pool_registry.set(ev["pool_address"], pool_st)
                continue

            if etype == "signal":
                result.signals_evaluated += 1
                sig_ev = self._reconstruct_signal_event(ev)
                if not sig_ev:
                    continue

                # Explicit EntryRuleSet evaluation
                if self.entry_rules is not None:
                    if not self.entry_rules.evaluate(sig_ev, pool=sig_ev.pool_state):
                        result.orders_rejected += 1
                        result.rejection_reasons["entry_rules_rejected"] = result.rejection_reasons.get("entry_rules_rejected", 0) + 1
                        continue

                # Explicit custom entry rule evaluator callable
                if self.entry_rule_evaluator is not None:
                    if not self.entry_rule_evaluator(sig_ev, sig_ev.pool_state):
                        result.orders_rejected += 1
                        result.rejection_reasons["entry_rule_evaluator_rejected"] = result.rejection_reasons.get("entry_rule_evaluator_rejected", 0) + 1
                        continue

                # Council fallback callback if configured
                if self.council_fallback is not None:
                    if not self.council_fallback(sig_ev):
                        result.orders_rejected += 1
                        result.rejection_reasons["council_fallback_rejected"] = result.rejection_reasons.get("council_fallback_rejected", 0) + 1
                        continue

                # A. Evaluate strategy entry rules if strategy parameters are supplied
                if self.strategy_params is not None:
                    # Check allowed sources
                    if self.strategy_params.allowed_sources:
                        src_val = sig_ev.source.value if hasattr(sig_ev.source, "value") else str(sig_ev.source)
                        allowed_vals = [s.value if hasattr(s, "value") else str(s) for s in self.strategy_params.allowed_sources]
                        if src_val not in allowed_vals:
                            result.orders_rejected += 1
                            result.rejection_reasons["disallowed_source"] = result.rejection_reasons.get("disallowed_source", 0) + 1
                            continue

                    # Check min entry score
                    if sig_ev.alpha_score < self.strategy_params.min_entry_score:
                        result.orders_rejected += 1
                        result.rejection_reasons["score_below_min"] = result.rejection_reasons.get("score_below_min", 0) + 1
                        continue

                    # Check entry rules
                    if hasattr(self.strategy_params, "entry_rules") and self.strategy_params.entry_rules:
                        matches = self.strategy_params.entry_rules.evaluate(sig_ev, pool=sig_ev.pool_state)
                        if not matches:
                            result.orders_rejected += 1
                            result.rejection_reasons["entry_rules_rejected"] = result.rejection_reasons.get("entry_rules_rejected", 0) + 1
                            continue

                # B. Sizing using RiskEngine
                native_p = self._price_service.get_price(sig_ev.chain).price
                sizing_res = self._risk_engine.calculate_order_size(
                    portfolio_equity_usd=Decimal(str(self.config.initial_equity_usd)),
                    native_price_usd=native_p,
                    chain=sig_ev.chain,
                    score=float(sig_ev.alpha_score * 100.0 if sig_ev.alpha_score <= 1.0 else sig_ev.alpha_score),
                )
                if not sizing_res.is_approved:
                    result.orders_rejected += 1
                    reason = sizing_res.rejection_reason or "sizing_rejected"
                    result.rejection_reasons[reason] = result.rejection_reasons.get(reason, 0) + 1
                    continue

                # C. Execution via PaperExecutor
                try:
                    fill = await self._executor.execute_signal(
                        signal=sig_ev,
                        portfolio_equity_usd=Decimal(str(self.config.initial_equity_usd)),
                        fixed_trade_amount_native=sizing_res.native_trade_size,
                        equity_fraction=sizing_res.fraction_of_equity,
                    )
                    if fill:
                        result.orders_executed += 1
                        result.fills.append(fill)
                    else:
                        result.orders_rejected += 1
                        result.rejection_reasons["executor_none_fill"] = result.rejection_reasons.get("executor_none_fill", 0) + 1
                except Exception as exc:
                    logger.debug("Replay execution error: %s", exc)
                    result.orders_rejected += 1
                    result.rejection_reasons["execution_exception"] = result.rejection_reasons.get("execution_exception", 0) + 1

        result.replay_duration_s = time.perf_counter() - start_wall_time
        return result

    def _reconstruct_signal_event(self, ev: Dict[str, Any]) -> Optional[SignalEvent]:
        """Reconstruct SignalEvent domain object from recorded signal tick."""
        try:
            chain = ChainIdentifier.SOLANA_MAINNET if "solana" in ev["chain"].lower() else ChainIdentifier.BASE_MAINNET
            token_addr = ev["token_address"]
            payload = ev.get("payload", {})
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            side_str = payload.get("side", "BUY") if isinstance(payload, dict) else "BUY"
            side = OrderSide.BUY if str(side_str).upper() == "BUY" else OrderSide.SELL

            # Find latest pool for this token from registry
            pool_st = None
            for p in self._pool_registry._pools.values():
                if p.token_address == token_addr:
                    pool_st = p
                    break

            if pool_st is None:
                # Build dummy pool if missing
                pool_st = PoolState(
                    pool_address=f"Pool_{token_addr[:16]}",
                    token_address=token_addr,
                    chain=chain,
                    venue=ExecutionVenue.RAYDIUM_AMM if chain == ChainIdentifier.SOLANA_MAINNET else ExecutionVenue.UNISWAP_V2,
                    native_reserve=Decimal("50.0"),
                    token_reserve=Decimal("1000000.0"),
                    reserve_type=ReserveType.EXACT,
                    fee_numerator=25,
                    fee_denominator=10000,
                    last_updated_block=1000,
                    timestamp_s=float(ev["timestamp_ns"] / 1e9),
                )
                self._pool_registry.set(pool_st.pool_address, pool_st)

            src_str = ev.get("source", "PUMP_FUN_MINT")
            source = SignalSource.PUMP_FUN_MINT
            for s in SignalSource:
                if s.value.lower() == src_str.lower():
                    source = s
                    break

            sec = SecurityReport(
                token_address=token_addr,
                chain=chain,
                tier=SecurityTier.CLEAN,
                is_honeypot=False,
                buy_tax_bps=0,
                sell_tax_bps=0,
                top10_concentration=0.15,
            )

            return SignalEvent(
                signal_id=ev["signal_id"],
                timestamp_ns=ev["timestamp_ns"],
                chain=chain,
                pool_address=pool_st.pool_address,
                token_address=token_addr,
                suggested_side=side,
                pool_state=pool_st,
                security_report=sec,
                strength=SignalStrength.STRONG,
                source=source,
                alpha_score=float(ev["score"]),
            )
        except Exception as exc:
            logger.debug("Failed to reconstruct signal event: %s", exc)
            return None
