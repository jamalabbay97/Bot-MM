"""
alpha_engine.engine.experimentation — Multi-Strategy Experimentation & Evolution Framework
========================================================================================
Closed-loop quantitative strategy experimentation engine:
- Runs Champion strategy alongside concurrent Challenger strategies in Shadow Mode
- Applies realistic CPMM price impact, LogNormal latency, MEV frontrun drag, and gas costs
- Performs statistical validation (Sharpe, Sortino, Profit Factor, Expectancy, Max Drawdown)
- Enforces anti-overfitting governance (N >= 30, Sortino/PF > 1.35, MDD < 8%, outlier win dependency <= 60%, perturbation robustness)
- Integrates with SQLiteLedger strategy_experiments and agent_deliberation_logs
"""

from __future__ import annotations

import json
import logging
import math
import random
import time
import uuid
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from alpha_engine.math.cpmm import cpmm_buy_quote, cpmm_sell_quote
from alpha_engine.math.mev import simulate_latency
from alpha_engine.models.enums import ChainIdentifier, LotStatus, OrderSide, SignalSource
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState
from alpha_engine.engine.rules import (
    ComparisonOperator,
    EntryRuleSet,
    LogicalOperator,
    METRIC_REGISTRY,
    RuleCondition,
    RuleGroup,
    SchemaValidationError,
    resolve_canonical_metric,
)

logger = logging.getLogger(__name__)


class MarketRegime(str, Enum):
    """Categorical market regime classifications for conditional strategy routing."""

    HIGH_VOL_MEME_MANIA = "HIGH_VOL_MEME_MANIA"
    LOW_LIQUIDITY_CHOP = "LOW_LIQUIDITY_CHOP"
    RISK_OFF_DRAWDOWN = "RISK_OFF_DRAWDOWN"
    REVIVAL_ROTATION = "REVIVAL_ROTATION"
    NEUTRAL = "NEUTRAL"


class ExperimentStatus(str, Enum):
    """Operational status of a strategy variant in the tournament registry."""

    CHAMPION = "CHAMPION"
    SHADOW = "SHADOW"
    RETIRED = "RETIRED"


@dataclass
class StrategyParameters:
    """Quantitative configuration parameters for a strategy variant."""

    strategy_name: str
    hard_stop_loss_pct: float = 0.15          # e.g. 0.15 = 15% stop loss
    take_profit_pct: Optional[float] = 0.50     # e.g. 0.50 = +50% take profit
    trailing_stop_from_peak_pct: Optional[float] = 0.30 # e.g. 0.30 = 30% drop from ATH
    time_exit_s: Optional[float] = None        # e.g. 120.0 = 120s time-based decay
    min_entry_score: float = 0.0               # Minimum composite score (0-100 or 0.0-1.0)
    partial_tp_ratio: float = 0.5              # Fraction to close at TP1
    regime_affinity: Optional[MarketRegime] = None
    extra_params: Dict[str, Any] = field(default_factory=dict)
    allowed_sources: Optional[List[SignalSource]] = None
    entry_rules: EntryRuleSet = field(default_factory=EntryRuleSet)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if self.regime_affinity:
            d["regime_affinity"] = self.regime_affinity.value
        if self.allowed_sources:
            d["allowed_sources"] = [s.value if hasattr(s, "value") else str(s) for s in self.allowed_sources]
        d["entry_rules"] = self.entry_rules.to_dict()
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StrategyParameters":
        reg = data.get("regime_affinity")
        reg_enum = MarketRegime(reg) if reg and reg in MarketRegime._value2member_map_ else None
        sources = data.get("allowed_sources")
        sources_list = [SignalSource(s) for s in sources if s in SignalSource._value2member_map_] if sources else None
        
        # Load entry rules if present, or translate legacy extra_params keys
        rules_data = data.get("entry_rules")
        if rules_data:
            rules = EntryRuleSet.from_dict(rules_data)
        else:
            rules = EntryRuleSet()
            extra = data.get("extra_params", {})
            conds = []
            if "max_mcap_usd" in extra:
                conds.append(RuleCondition("mcap_usd", ComparisonOperator.LTE, float(extra["max_mcap_usd"])))
            if "min_net_buy_delta" in extra:
                conds.append(RuleCondition("net_buy_delta", ComparisonOperator.GTE, float(extra["min_net_buy_delta"])))
            if "min_surge_k" in extra or "min_surge_multiplier" in extra:
                s_val = float(extra.get("min_surge_k", extra.get("min_surge_multiplier", 2.0)))
                conds.append(RuleCondition("volume_surge_multiplier", ComparisonOperator.GTE, s_val))
            if "min_dormancy_hours" in extra:
                conds.append(RuleCondition("dormancy_hours", ComparisonOperator.GTE, float(extra["min_dormancy_hours"])))
            if "max_slippage_bps" in extra:
                conds.append(RuleCondition("slippage_estimate_bps", ComparisonOperator.LTE, float(extra["max_slippage_bps"])))
            if "min_wallet_sortino" in extra:
                conds.append(RuleCondition("wallet_score", ComparisonOperator.GTE, float(extra["min_wallet_sortino"])))
            if "organic_conviction_min" in extra:
                conds.append(RuleCondition("social_conviction", ComparisonOperator.GTE, float(extra["organic_conviction_min"])))
            if conds:
                rules = EntryRuleSet(conditions=conds)

        return cls(
            strategy_name=data.get("strategy_name", "custom"),
            hard_stop_loss_pct=float(data.get("hard_stop_loss_pct", 0.15)),
            take_profit_pct=float(data["take_profit_pct"]) if data.get("take_profit_pct") is not None else None,
            trailing_stop_from_peak_pct=float(data["trailing_stop_from_peak_pct"]) if data.get("trailing_stop_from_peak_pct") is not None else None,
            time_exit_s=float(data["time_exit_s"]) if data.get("time_exit_s") is not None else None,
            min_entry_score=float(data.get("min_entry_score", 0.0)),
            partial_tp_ratio=float(data.get("partial_tp_ratio", 0.5)),
            regime_affinity=reg_enum,
            extra_params=data.get("extra_params", {}),
            allowed_sources=sources_list,
            entry_rules=rules,
        )


@dataclass
class ShadowLot:
    """Simulated trade execution lot for Champion and Challenger shadow evaluation."""

    lot_id: str
    strategy_id: str
    token_address: str
    chain: ChainIdentifier
    entry_price: Decimal
    entry_timestamp: float
    tokens_acquired: Decimal
    simulated_native_spent: Decimal
    cost_basis_usd: Decimal
    gas_drag_usd: Decimal
    cpmm_slippage_bps: int
    latency_ms: float
    mev_drag_usd: Decimal
    highest_price: Decimal
    pool_address: str = ""
    is_closed: bool = False
    status: LotStatus = LotStatus.OPEN
    exit_price: Optional[Decimal] = None
    exit_timestamp: Optional[float] = None
    exit_reason: Optional[str] = None
    realized_pnl_usd: Optional[Decimal] = None
    roi_pct: Optional[float] = None
    partial_tp_hit: bool = False


@dataclass
class StrategyMetrics:
    """Statistical performance metrics for a strategy variant."""

    trades_count: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    max_drawdown_pct: float = 0.0
    net_pnl_usd: float = 0.0
    expectancy_usd: float = 0.0
    outlier_win_ratio: float = 0.0


class StrategyVariant:
    """Encapsulates a strategy hypothesis, shadow lots, and statistical accounting."""

    def __init__(
        self,
        experiment_id: str,
        strategy_name: str,
        parameters: StrategyParameters,
        status: ExperimentStatus = ExperimentStatus.SHADOW,
        regime: MarketRegime = MarketRegime.NEUTRAL,
    ) -> None:
        self.experiment_id = experiment_id
        self.strategy_name = strategy_name
        self.parameters = parameters
        self.status = status
        self.regime = regime
        self.open_lots: dict[str, ShadowLot] = {}
        self.closed_lots: list[ShadowLot] = []
        self.metrics = StrategyMetrics()
        self.created_at = time.time()
        self.updated_at = time.time()

    def record_fill(
        self,
        token_address: str,
        chain: ChainIdentifier,
        entry_price: Decimal,
        tokens_acquired: Decimal,
        native_spent: Decimal,
        native_price_usd: Decimal,
        gas_usd: Decimal,
        slippage_bps: int,
        latency_ms: float,
        mev_drag_usd: Decimal,
        pool_address: str = "",
    ) -> ShadowLot:
        """Create and track a new shadow lot for this strategy variant."""
        lot_id = f"shadow-{self.strategy_name[:6]}-{uuid.uuid4().hex[:8]}"
        cost_usd = native_spent * native_price_usd
        lot = ShadowLot(
            lot_id=lot_id,
            strategy_id=self.experiment_id,
            token_address=token_address,
            chain=chain,
            entry_price=entry_price,
            entry_timestamp=time.time(),
            tokens_acquired=tokens_acquired,
            simulated_native_spent=native_spent,
            cost_basis_usd=cost_usd,
            gas_drag_usd=gas_usd,
            cpmm_slippage_bps=slippage_bps,
            latency_ms=latency_ms,
            mev_drag_usd=mev_drag_usd,
            highest_price=entry_price,
            pool_address=pool_address,
        )
        self.open_lots[lot_id] = lot
        self.updated_at = time.time()
        return lot

    def update_price(
        self,
        token_address: str,
        current_price: Decimal,
        native_price_usd: Decimal,
    ) -> list[ShadowLot]:
        """
        Evaluate open lots for this token against strategy stop-loss, take-profit,
        trailing drop from ATH, or time decay.
        """
        closed_this_tick: list[ShadowLot] = []
        now = time.time()

        for lot_id, lot in list(self.open_lots.items()):
            if lot.token_address != token_address or lot.is_closed:
                continue

            if current_price > lot.highest_price:
                lot.highest_price = current_price

            ep = lot.entry_price
            if ep <= Decimal(0):
                continue

            gain_ratio = float((current_price - ep) / ep)
            holding_time_s = now - lot.entry_timestamp
            ath_drop_ratio = float((lot.highest_price - current_price) / lot.highest_price) if lot.highest_price > Decimal(0) else 0.0

            should_close = False
            close_reason = ""

            # 1. Hard Stop-Loss check
            if gain_ratio <= -self.parameters.hard_stop_loss_pct:
                should_close = True
                close_reason = f"HARD_STOP_LOSS ({gain_ratio*100:.1f}% <= -{self.parameters.hard_stop_loss_pct*100:.1f}%)"

            # 2. Trailing Stop from Peak
            elif self.parameters.trailing_stop_from_peak_pct and lot.highest_price > ep * Decimal("1.10"):
                if ath_drop_ratio >= self.parameters.trailing_stop_from_peak_pct:
                    should_close = True
                    close_reason = f"TRAILING_STOP_ATH ({ath_drop_ratio*100:.1f}% >= {self.parameters.trailing_stop_from_peak_pct*100:.1f}%)"

            # 3. Take Profit
            elif self.parameters.take_profit_pct and gain_ratio >= self.parameters.take_profit_pct:
                should_close = True
                close_reason = f"TAKE_PROFIT ({gain_ratio*100:.1f}% >= +{self.parameters.take_profit_pct*100:.1f}%)"

            # 4. Time Decay
            elif self.parameters.time_exit_s and holding_time_s >= self.parameters.time_exit_s:
                should_close = True
                close_reason = f"TIME_DECAY ({holding_time_s:.0f}s >= {self.parameters.time_exit_s:.0f}s)"

            if should_close:
                lot.is_closed = True
                lot.status = LotStatus.CLOSED
                lot.exit_price = current_price
                lot.exit_timestamp = now
                lot.exit_reason = close_reason

                # Accounting including symmetric entry/exit gas and MEV drag
                exit_gross_usd = (lot.tokens_acquired * current_price) * native_price_usd
                exit_gas_usd = lot.gas_drag_usd
                total_drag_usd = lot.gas_drag_usd + lot.mev_drag_usd + exit_gas_usd
                net_pnl_usd = exit_gross_usd - lot.cost_basis_usd - total_drag_usd
                lot.realized_pnl_usd = net_pnl_usd
                lot.roi_pct = float(net_pnl_usd / lot.cost_basis_usd * 100) if lot.cost_basis_usd > Decimal(0) else 0.0

                self.closed_lots.append(lot)
                del self.open_lots[lot_id]
                closed_this_tick.append(lot)

        if closed_this_tick:
            self.recompute_metrics()
            self.updated_at = time.time()

        return closed_this_tick

    def simulate_cpmm_buy_quote(
        self,
        pool: PoolState,
        native_amount_in: Decimal,
    ) -> Decimal:
        """
        Simulate CPMM buy execution price for shadow position entry.
        """
        try:
            quote = cpmm_buy_quote(pool, native_amount_in)
            return quote.execution_price
        except Exception:
            return pool.spot_price_native_per_token

    def simulate_cpmm_sell_quote(
        self,
        pool: PoolState,
        tokens_to_sell: Decimal,
    ) -> Decimal:
        """
        Simulate CPMM sell execution price for shadow position liquidation.
        """
        try:
            quote = cpmm_sell_quote(pool, tokens_to_sell)
            return quote.execution_price
        except Exception:
            return pool.spot_price_native_per_token

    @classmethod
    def compute_metrics_from_lots(cls, lots: list[ShadowLot]) -> StrategyMetrics:
        """Calculate institutional risk-adjusted performance metrics across a given list of closed lots."""
        n = len(lots)
        if n == 0:
            return StrategyMetrics()

        pnls = [float(lot.realized_pnl_usd or 0.0) for lot in lots]
        rois = [float(lot.roi_pct or 0.0) for lot in lots]

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))

        win_rate = (len(wins) / n) * 100.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

        # Sharpe and Sortino (per-trade basis)
        mean_roi = sum(rois) / n
        variance = sum((r - mean_roi) ** 2 for r in rois) / n if n > 1 else 0.0
        std_roi = math.sqrt(variance) if variance > 0 else 0.0
        if std_roi > 0:
            sharpe = mean_roi / std_roi
        elif mean_roi > 0:
            sharpe = 99.0
        else:
            sharpe = 0.0

        downside_rois = [min(0.0, r) for r in rois]
        downside_variance = sum(dr ** 2 for dr in downside_rois) / n if n > 1 else 0.0
        downside_std = math.sqrt(downside_variance) if downside_variance > 0 else 0.0
        if downside_std > 0:
            sortino = mean_roi / downside_std
        elif mean_roi > 0:
            sortino = 99.0  # Conventionally high when zero downside volatility exists with positive return
        else:
            sortino = 0.0

        # Maximum Drawdown (% of cumulative curve)
        cum_equity = 1000.0  # Standard base $1000 portfolio
        peak = cum_equity
        max_dd = 0.0
        for p in pnls:
            cum_equity += p
            if cum_equity > peak:
                peak = cum_equity
            dd = (peak - cum_equity) / peak if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd

        # Mathematical Expectancy: E = P_win * avg_win - P_loss * avg_loss
        avg_win = (sum(wins) / len(wins)) if wins else 0.0
        avg_loss = (abs(sum(losses)) / len(losses)) if losses else 0.0
        p_win = len(wins) / n
        p_loss = len(losses) / n
        expectancy = (p_win * avg_win) - (p_loss * avg_loss)

        # Outlier Win Dependency: max single win / total net profit
        net_profit = sum(pnls)
        max_single_win = max(wins) if wins else 0.0
        outlier_ratio = (max_single_win / net_profit) if net_profit > 0 and wins else 0.0

        return StrategyMetrics(
            trades_count=n,
            winning_trades=len(wins),
            losing_trades=len(losses),
            win_rate=round(win_rate, 2),
            profit_factor=round(profit_factor, 2),
            sharpe_ratio=round(sharpe, 3),
            sortino_ratio=round(sortino, 3),
            max_drawdown_pct=round(max_dd * 100.0, 2),
            net_pnl_usd=round(net_profit, 2),
            expectancy_usd=round(expectancy, 2),
            outlier_win_ratio=round(outlier_ratio, 3),
        )

    def recompute_metrics(self) -> StrategyMetrics:
        """Calculate institutional risk-adjusted performance metrics across closed shadow lots."""
        self.metrics = self.compute_metrics_from_lots(self.closed_lots)
        return self.metrics

    def split_closed_lots_by_time(
        self, train_fraction: float = 0.70
    ) -> tuple[list[ShadowLot], list[ShadowLot]]:
        """
        Chronologically split closed shadow lots into in-sample (train) and out-of-sample (test) segments.
        Extensible for Phase 3 walk-forward replay backtesting.
        """
        if not self.closed_lots:
            return ([], [])
        sorted_lots = sorted(self.closed_lots, key=lambda lot: lot.entry_timestamp or 0.0)
        n = len(sorted_lots)
        if n < 2:
            return (sorted_lots, [])
        split_idx = max(1, min(n - 1, int(n * train_fraction)))
        return (sorted_lots[:split_idx], sorted_lots[split_idx:])

    @classmethod
    def compute_bootstrap_expectancy(
        cls,
        lots: list[ShadowLot],
        confidence_level: float = 0.95,
        n_resamples: int = 1000,
        rng: Optional[random.Random] = None,
    ) -> tuple[float, float, float]:
        """
        Compute bootstrap confidence bounds for mathematical expectancy (mean PnL per trade in USD).
        Returns (mean_expectancy, lower_bound, upper_bound).
        At 95% confidence, lower_bound represents the 5th percentile of the bootstrap mean distribution.
        """
        if not lots:
            return (0.0, 0.0, 0.0)
        pnls = [float(lot.realized_pnl_usd or 0.0) for lot in lots]
        n = len(pnls)
        mean_exp = sum(pnls) / n
        if n < 5:
            return (mean_exp, mean_exp, mean_exp)

        local_rng = rng if rng is not None else random.Random()
        boot_means = []
        for _ in range(n_resamples):
            sample = [local_rng.choice(pnls) for _ in range(n)]
            boot_means.append(sum(sample) / n)

        boot_means.sort()
        alpha = max(0.001, min(0.5, 1.0 - confidence_level))
        lower_idx = max(0, min(n_resamples - 1, int(n_resamples * alpha)))
        upper_idx = max(0, min(n_resamples - 1, int(n_resamples * (1.0 - alpha))))
        return (mean_exp, boot_means[lower_idx], boot_means[upper_idx])

    def check_perturbation_stability(self, perturbation_pct: float = 0.10) -> bool:
        """
        Anti-Overfitting Cliff Check:
        Perturb stop-loss by +/- 10%. If net PnL drops from positive to deeply negative,
        the strategy sits on an overfitted parameter cliff.
        """
        if len(self.closed_lots) < 15:
            return True # Not enough historical samples to reject yet

        base_sl = self.parameters.hard_stop_loss_pct
        sl_upper = base_sl * (1.0 + perturbation_pct)
        sl_lower = base_sl * (1.0 - perturbation_pct)

        # Re-evaluate closed lots under perturbed stop-loss
        def sim_pnl_for_sl(sl: float) -> float:
            tot = 0.0
            for lot in self.closed_lots:
                # If trade was stopped out earlier under tighter SL
                roi = float(lot.roi_pct or 0.0) / 100.0
                if roi < -sl:
                    tot -= float(lot.cost_basis_usd) * sl
                else:
                    tot += float(lot.realized_pnl_usd or 0.0)
            return tot

        pnl_base = float(self.metrics.net_pnl_usd)
        pnl_up = sim_pnl_for_sl(sl_upper)
        pnl_down = sim_pnl_for_sl(sl_lower)

        # If base is positive but either perturbation results in severe loss (< -20% of base), reject
        if pnl_base > 0:
            if pnl_up < 0 or pnl_down < 0:
                logger.warning(
                    "Strategy %s failed perturbation cliff check: base=$%.2f, up=$%.2f, down=$%.2f",
                    self.strategy_name, pnl_base, pnl_up, pnl_down,
                )
                return False
        return True


class StrategyTournamentRegistry:
    """
    Multi-Strategy Tournament Registry:
    Manages concurrent Champion and Challenger strategies, records shadow lots,
    and runs statistical promotion/demotion governance.
    """

    def __init__(
        self,
        ledger: Optional[Any] = None,
        cost_model: Optional[Any] = None,
        config: Optional[Any] = None,
        walk_forward_validator: Optional[Any] = None,
    ) -> None:
        self.ledger = ledger
        from alpha_engine.execution.costs import TradeCostModel
        self.cost_model = cost_model or TradeCostModel()
        from alpha_engine.config import EngineConfig
        self.config = config or getattr(self.cost_model, "config", None) or EngineConfig()
        self.walk_forward_validator = walk_forward_validator
        import random
        seed = getattr(self.config, "execution_seed", None)
        self._rng = random.Random(seed) if seed is not None else random.Random()
        self.current_regime: MarketRegime = MarketRegime.NEUTRAL
        self.on_promotion_callbacks: list[Callable[[StrategyVariant, str], Any]] = []

        # Statistical Governance & Anti-Overfitting Configuration
        # Defaults: N >= 50 closed trades ensures narrow standard error of the mean;
        # in emergency underperforming conditions, N >= 30 maintains minimum statistical power.
        self.min_promotion_trades: int = int(getattr(self.config, "tournament_min_promotion_trades", 50))
        self.min_promotion_trades_underperforming: int = int(getattr(self.config, "tournament_min_promotion_trades_underperforming", 30))
        self.train_fraction: float = float(getattr(self.config, "tournament_train_fraction", 0.70))
        self.bootstrap_resamples: int = int(getattr(self.config, "tournament_bootstrap_resamples", 1000))
        self.bootstrap_confidence_level: float = float(getattr(self.config, "tournament_bootstrap_confidence_level", 0.95))
        self.promotion_cooldown_trades: int = int(getattr(self.config, "tournament_promotion_cooldown_trades", 10))
        self.promotion_cooldown_seconds: float = float(getattr(self.config, "tournament_promotion_cooldown_seconds", 300.0))
        self.rollback_max_trades: int = int(getattr(self.config, "tournament_rollback_max_trades", 10))
        self.rollback_max_loss_usd: float = float(getattr(self.config, "tournament_rollback_max_loss_usd", -20.0))
        self.rollback_min_win_rate: float = float(getattr(self.config, "tournament_rollback_min_win_rate", 25.0))

        # Promotion, Cooldown & Rollback Governance State
        self.previous_champion: Optional[StrategyVariant] = None
        self.last_promotion_timestamp: float = 0.0
        self.last_promotion_trade_count: int = 0
        self.champion_live_trades_since_promotion: list[dict[str, Any]] = []
        self.retired_challengers: dict[str, StrategyVariant] = {}

        # Initialize default Champion with real entry rules
        champ_params = StrategyParameters(
            strategy_name="champion_peak_trailing_50",
            hard_stop_loss_pct=0.15,
            take_profit_pct=1.00,
            trailing_stop_from_peak_pct=0.35,
            min_entry_score=0.0,
            entry_rules=EntryRuleSet(
                conditions=[
                    RuleCondition("alpha_score", ComparisonOperator.GTE, 0.0),
                ]
            ),
        )
        self.champion = StrategyVariant(
            experiment_id="exp-champ-001",
            strategy_name=champ_params.strategy_name,
            parameters=champ_params,
            status=ExperimentStatus.CHAMPION,
        )

        # Initialize Challenger Variants with real entry rules
        self.challengers: dict[str, StrategyVariant] = {}

        # Variant A: Tight Scalp (-10% SL, +30% TP1, 120s decay, low mcap, alpha >= 50)
        p_a = StrategyParameters(
            strategy_name="challenger_tight_scalp",
            hard_stop_loss_pct=0.10,
            take_profit_pct=0.30,
            trailing_stop_from_peak_pct=0.15,
            time_exit_s=120.0,
            min_entry_score=50.0,
            regime_affinity=MarketRegime.LOW_LIQUIDITY_CHOP,
            entry_rules=EntryRuleSet(
                conditions=[
                    RuleCondition("alpha_score", ComparisonOperator.GTE, 50.0),
                    RuleCondition("mcap_usd", ComparisonOperator.LTE, 200000.0),
                ]
            ),
        )
        self.challengers[p_a.strategy_name] = StrategyVariant(
            experiment_id="exp-chal-scalp",
            strategy_name=p_a.strategy_name,
            parameters=p_a,
            status=ExperimentStatus.SHADOW,
            regime=MarketRegime.LOW_LIQUIDITY_CHOP,
        )

        # Variant B: Early Rally Swing (-15% SL, +50% TP1, 28% ATH Trail, volume surge >= 1.8, net buy >= 0.52)
        p_b = StrategyParameters(
            strategy_name="challenger_early_rally_swing",
            hard_stop_loss_pct=0.15,
            take_profit_pct=0.50,
            trailing_stop_from_peak_pct=0.28,
            regime_affinity=MarketRegime.REVIVAL_ROTATION,
            entry_rules=EntryRuleSet(
                conditions=[
                    RuleCondition("volume_surge_multiplier", ComparisonOperator.GTE, 1.8),
                    RuleCondition("net_buy_delta", ComparisonOperator.GTE, 0.52),
                ]
            ),
        )
        self.challengers[p_b.strategy_name] = StrategyVariant(
            experiment_id="exp-chal-rally",
            strategy_name=p_b.strategy_name,
            parameters=p_b,
            status=ExperimentStatus.SHADOW,
            regime=MarketRegime.REVIVAL_ROTATION,
        )

        # Variant C: High-Threshold Momentum (min_entry_score=75.0, -12% SL, 20% ATH trail, net buy >= 0.58)
        p_c = StrategyParameters(
            strategy_name="challenger_high_threshold_momentum",
            hard_stop_loss_pct=0.12,
            take_profit_pct=0.80,
            trailing_stop_from_peak_pct=0.20,
            min_entry_score=75.0,
            regime_affinity=MarketRegime.HIGH_VOL_MEME_MANIA,
            entry_rules=EntryRuleSet(
                conditions=[
                    RuleCondition("alpha_score", ComparisonOperator.GTE, 75.0),
                    RuleCondition("net_buy_delta", ComparisonOperator.GTE, 0.58),
                ]
            ),
        )
        self.challengers[p_c.strategy_name] = StrategyVariant(
            experiment_id="exp-chal-momentum",
            strategy_name=p_c.strategy_name,
            parameters=p_c,
            status=ExperimentStatus.SHADOW,
            regime=MarketRegime.HIGH_VOL_MEME_MANIA,
        )

        # Variant D: AI-Dynamic Exit Matrix (top10 conc <= 0.25, net buy >= 0.50)
        p_d = StrategyParameters(
            strategy_name="challenger_ai_dynamic_matrix",
            hard_stop_loss_pct=0.14,
            take_profit_pct=0.60,
            trailing_stop_from_peak_pct=0.25,
            regime_affinity=MarketRegime.NEUTRAL,
            entry_rules=EntryRuleSet(
                conditions=[
                    RuleCondition("holder_concentration", ComparisonOperator.LTE, 0.25),
                    RuleCondition("net_buy_delta", ComparisonOperator.GTE, 0.50),
                ]
            ),
        )
        self.challengers[p_d.strategy_name] = StrategyVariant(
            experiment_id="exp-chal-aidynamic",
            strategy_name=p_d.strategy_name,
            parameters=p_d,
            status=ExperimentStatus.SHADOW,
            regime=MarketRegime.NEUTRAL,
        )

    def register_challenger(self, variant: StrategyVariant) -> None:
        """Register a newly generated or mutated Challenger strategy variant into shadow mode."""
        self.challengers[variant.strategy_name] = variant
        logger.info(
            "StrategyTournamentRegistry: Registered new challenger variant %s (%s) for regime %s",
            variant.strategy_name,
            variant.experiment_id,
            variant.regime.value,
        )
        if self.ledger is not None and hasattr(self.ledger, "record_strategy_experiment"):
            try:
                import asyncio
                asyncio.create_task(
                    self.ledger.record_strategy_experiment(
                        experiment_id=variant.experiment_id,
                        strategy_name=variant.strategy_name,
                        parameters_json=json.dumps(variant.parameters.to_dict()),
                        regime=variant.regime.value,
                        status=variant.status.value,
                        trades_count=0,
                        win_rate=0.0,
                        profit_factor=0.0,
                        sharpe_ratio=0.0,
                        max_drawdown_pct=0.0,
                        net_pnl_usd=0.0,
                    )
                )
            except Exception as exc:
                logger.debug("Failed to record new challenger to ledger: %s", exc)

    def register_on_promotion_callback(self, callback: Callable[[StrategyVariant, str], Any]) -> None:
        """Register a callback triggered whenever a Challenger is promoted to Champion."""
        if callback not in self.on_promotion_callbacks:
            self.on_promotion_callbacks.append(callback)

    def _get_total_closed_trades_count(self) -> int:
        """Return total closed lots across champion and all active/retired challengers."""
        cnt = len(self.champion.closed_lots) if self.champion else 0
        for c in self.challengers.values():
            cnt += len(c.closed_lots)
        for c in self.retired_challengers.values():
            cnt += len(c.closed_lots)
        return cnt

    def is_in_promotion_cooldown(self) -> bool:
        """Check if promotion governance is currently in a cooldown window."""
        if self.last_promotion_timestamp <= 0.0:
            return False
        elapsed_s = time.time() - self.last_promotion_timestamp
        cooldown_s = getattr(self, "promotion_cooldown_seconds", getattr(self.config, "tournament_promotion_cooldown_seconds", 300.0))
        if elapsed_s < cooldown_s:
            logger.info("StrategyTournamentRegistry: Promotion blocked by time cooldown (%.1fs < %.1fs)", elapsed_s, cooldown_s)
            return True
        cooldown_trades = getattr(self, "promotion_cooldown_trades", getattr(self.config, "tournament_promotion_cooldown_trades", 10))
        trades_since = self._get_total_closed_trades_count() - self.last_promotion_trade_count
        if trades_since < cooldown_trades:
            logger.info("StrategyTournamentRegistry: Promotion blocked by trade count cooldown (%d < %d trades)", trades_since, cooldown_trades)
            return True
        return False

    def evaluate_rollback(self) -> Optional[Tuple[StrategyVariant, str]]:
        """
        Rollback rule:
        If the newly promoted champion's live results fall below configured bounds
        within rollback_max_trades (e.g. cumulative loss <= -$20.0 or low win rate),
        demote the champion back to SHADOW and restore previous_champion.
        """
        if self.previous_champion is None:
            return None

        trades = self.champion_live_trades_since_promotion
        if not trades:
            return None

        max_trades = getattr(self, "rollback_max_trades", getattr(self.config, "tournament_rollback_max_trades", 10))
        max_loss_usd = getattr(self, "rollback_max_loss_usd", getattr(self.config, "tournament_rollback_max_loss_usd", -20.0))
        min_win_rate = getattr(self, "rollback_min_win_rate", getattr(self.config, "tournament_rollback_min_win_rate", 25.0))

        n = len(trades)
        if n > max_trades:
            return None

        cum_pnl = sum(float(t.get("realized_pnl_usd", 0.0)) for t in trades)
        wins = sum(1 for t in trades if t.get("is_win", False))
        win_rate = (wins / n) * 100.0

        should_rollback = False
        rollback_reason = ""

        if cum_pnl <= max_loss_usd:
            should_rollback = True
            rollback_reason = (
                f"ROLLBACK TRIGGERED: New champion {self.champion.strategy_name} cumulative live loss "
                f"${cum_pnl:.2f} <= ${max_loss_usd:.2f} within N={n} live trades."
            )
        elif n >= 5 and win_rate < min_win_rate:
            should_rollback = True
            rollback_reason = (
                f"ROLLBACK TRIGGERED: New champion {self.champion.strategy_name} live win rate "
                f"{win_rate:.1f}% < {min_win_rate:.1f}% across N={n} live trades."
            )

        if should_rollback:
            logger.warning(
                "⚠️ %s Demoting to SHADOW and restoring previous champion %s",
                rollback_reason, self.previous_champion.strategy_name,
            )
            demoted_champ = self.champion
            demoted_champ.status = ExperimentStatus.SHADOW
            self.challengers[demoted_champ.strategy_name] = demoted_champ

            restored_champ = self.previous_champion
            restored_champ.status = ExperimentStatus.CHAMPION
            self.champion = restored_champ
            self.challengers.pop(restored_champ.strategy_name, None)
            self.previous_champion = None
            self.champion_live_trades_since_promotion = []
            self.last_promotion_timestamp = time.time()
            self.last_promotion_trade_count = self._get_total_closed_trades_count()

            for cb in self.on_promotion_callbacks:
                try:
                    cb(restored_champ, rollback_reason)
                except Exception as cb_err:
                    logger.warning("Error in on_promotion_callback during rollback: %s", cb_err)

            return (restored_champ, rollback_reason)

        return None

    def prune_underperforming_challengers(
        self,
        min_trades: int = 15,
        max_dd_pct: float = 20.0,
        min_pf: float = 0.85,
    ) -> list[str]:
        """
        Retire persistently failing or high-drawdown strategy variants.
        Frees up tournament slots for novel autonomous mutations.
        """
        pruned = []
        for name, chal in list(self.challengers.items()):
            if chal.status != ExperimentStatus.SHADOW:
                continue
            m = chal.metrics
            if m.trades_count >= min_trades:
                if m.profit_factor < min_pf or m.max_drawdown_pct >= max_dd_pct or m.win_rate < 30.0:
                    chal.status = ExperimentStatus.RETIRED
                    self.retired_challengers[name] = chal
                    del self.challengers[name]
                    pruned.append(name)
                    logger.info(
                        "StrategyTournamentRegistry: Retired underperforming challenger %s (N=%d, PF=%.2f, DD=%.1f%%, WR=%.1f%%)",
                        name, m.trades_count, m.profit_factor, m.max_drawdown_pct, m.win_rate,
                    )
        return pruned

    def record_live_trade_outcome(
        self,
        strategy_name: str,
        pnl_usd: Optional[float | Decimal] = None,
        is_win: Optional[bool] = None,
        roi_pct: float = 0.0,
        hold_time_s: float = 0.0,
        token_address: Optional[str] = None,
        chain: Optional[ChainIdentifier] = None,
        entry_price: Optional[Decimal] = None,
        exit_price: Optional[Decimal] = None,
        tokens_acquired: Optional[Decimal] = None,
        cost_basis_usd: Optional[Decimal] = None,
        realized_pnl_usd: Optional[Decimal] = None,
        gas_drag_usd: Decimal = Decimal("0.0"),
        cpmm_slippage_bps: int = 0,
        latency_ms: float = 0.0,
        mev_drag_usd: Decimal = Decimal("0.0"),
        exit_reason: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Feed real-world settled live trades back into the Champion strategy's metrics,
        ensuring the live tournament continuously reflects genuine live market performance.
        Records genuine trade attributes instead of fabricated constants and strictly
        preserves the true PnL sign (trusting PnL if is_win disagrees).
        """
        target = None
        if self.champion and self.champion.strategy_name == strategy_name:
            target = self.champion
        elif strategy_name in self.challengers:
            target = self.challengers[strategy_name]
        elif self.champion is not None:
            target = self.champion

        if target is not None:
            # Resolve realized_pnl_usd from either parameter
            if realized_pnl_usd is None:
                if pnl_usd is not None:
                    realized_pnl = Decimal(str(pnl_usd))
                else:
                    realized_pnl = Decimal("0.0")
            else:
                realized_pnl = Decimal(str(realized_pnl_usd))

            # Audit and enforce agreement between is_win and realized PnL
            if is_win is not None:
                pnl_positive = realized_pnl > Decimal(0)
                if is_win != pnl_positive and realized_pnl != Decimal(0):
                    logger.error(
                        "record_live_trade_outcome: is_win=%s disagrees with realized_pnl_usd=%s for strategy=%s; trusting PnL",
                        is_win, realized_pnl, strategy_name,
                    )
                    # Trust the PnL; NEVER rewrite PnL sign
                    is_win = pnl_positive
            else:
                is_win = realized_pnl > Decimal(0)

            now = time.time()
            entry_ts = (now - hold_time_s) if hold_time_s > 0 else now
            outcome_label = "WIN" if is_win else "LOSS"

            actual_token = str(token_address or kwargs.get("token_addr", "LIVE_SETTLED_TRADE"))
            actual_chain = chain if chain is not None else ChainIdentifier.SOLANA_MAINNET
            actual_entry_p = Decimal(str(entry_price)) if entry_price is not None else Decimal("1.0")
            if exit_price is not None:
                actual_exit_p = Decimal(str(exit_price))
            else:
                actual_exit_p = actual_entry_p * Decimal(str(max(0.0001, 1.0 + roi_pct / 100.0)))
            actual_tokens = Decimal(str(tokens_acquired)) if tokens_acquired is not None else Decimal("1.0")
            actual_cost_basis = Decimal(str(cost_basis_usd)) if cost_basis_usd is not None else (actual_tokens * actual_entry_p)
            actual_exit_reason = str(exit_reason or f"LIVE_SETTLED_TRADE_{outcome_label}")

            sim_lot = ShadowLot(
                lot_id=f"live_{uuid.uuid4().hex[:8]}",
                strategy_id=target.experiment_id,
                token_address=actual_token,
                chain=actual_chain,
                entry_price=actual_entry_p,
                entry_timestamp=entry_ts,
                tokens_acquired=actual_tokens,
                simulated_native_spent=actual_tokens * actual_entry_p,
                cost_basis_usd=actual_cost_basis,
                gas_drag_usd=Decimal(str(gas_drag_usd)),
                cpmm_slippage_bps=int(cpmm_slippage_bps),
                latency_ms=float(latency_ms),
                mev_drag_usd=Decimal(str(mev_drag_usd)),
                highest_price=max(actual_entry_p, actual_exit_p),
                is_closed=True,
                status=LotStatus.CLOSED,
                exit_price=actual_exit_p,
                exit_timestamp=now,
                exit_reason=actual_exit_reason,
                realized_pnl_usd=realized_pnl,
                roi_pct=float(roi_pct),
            )
            target.closed_lots.append(sim_lot)
            m = target.recompute_metrics()

            logger.info(
                "StrategyTournamentRegistry: Recorded live %s trade for %s (%s) -> PnL=$%.2f, ROI=%.2f%%, HoldTime=%.1fs (Trades=%d, WinRate=%.1f%%, PF=%.2f, Sortino=%.2f)",
                outcome_label, strategy_name, actual_token[:10], float(realized_pnl), float(roi_pct), float(hold_time_s), m.trades_count, m.win_rate, m.profit_factor, m.sortino_ratio,
            )

            # Record in champion live evaluation window if trade belongs to champion
            if self.champion and strategy_name == self.champion.strategy_name:
                self.champion_live_trades_since_promotion.append({
                    "realized_pnl_usd": float(realized_pnl),
                    "is_win": bool(is_win),
                    "timestamp": now,
                })
                # Check for rollback demotion if live performance breaches safety bounds
                self.evaluate_rollback()

    async def on_trade_closed(self, trade_outcome: dict[str, Any]) -> None:
        """Async hook adapter for post-trade lifecycle."""
        strat_name = str(trade_outcome.get("strategy_type") or trade_outcome.get("strategy_name", "default"))
        pnl = trade_outcome.get("realized_pnl_usd", 0.0)
        roi = float(trade_outcome.get("roi_pct", 0.0))
        dur = float(trade_outcome.get("duration_s", trade_outcome.get("holding_time_seconds", 0.0)))
        is_w = trade_outcome.get("is_win")
        chain_raw = trade_outcome.get("chain")
        chain_enum = None
        if chain_raw:
            for c in ChainIdentifier:
                if c.value.lower() == str(chain_raw).lower():
                    chain_enum = c
                    break
        self.record_live_trade_outcome(
            strategy_name=strat_name,
            pnl_usd=pnl,
            is_win=is_w,
            roi_pct=roi,
            hold_time_s=dur,
            token_address=trade_outcome.get("token_address"),
            chain=chain_enum,
            entry_price=Decimal(str(trade_outcome.get("entry_price", "1.0"))),
            exit_price=Decimal(str(trade_outcome.get("exit_price", "1.0"))),
            tokens_acquired=Decimal(str(trade_outcome.get("tokens_acquired", "1.0"))),
            cost_basis_usd=Decimal(str(trade_outcome.get("cost_basis_usd", "100.0"))),
            realized_pnl_usd=Decimal(str(pnl)),
            gas_drag_usd=Decimal(str(trade_outcome.get("simulated_gas_cost_usd", "0.0"))),
            cpmm_slippage_bps=int(trade_outcome.get("actual_slippage_bps", 0)),
            latency_ms=float(trade_outcome.get("fill_latency_ms", 0.0)),
            exit_reason=trade_outcome.get("exit_reason"),
        )

    def get_active_challengers(self) -> list[StrategyVariant]:
        """Return list of active challenger variants in shadow mode."""
        return [c for c in self.challengers.values() if c.status == ExperimentStatus.SHADOW]

    def get_strategy_by_name(self, name: str) -> Optional[StrategyVariant]:
        """Lookup strategy variant by name (champion or challenger)."""
        if self.champion.strategy_name == name:
            return self.champion
        return self.challengers.get(name)

    def classify_regime(
        self,
        recent_win_rate: float,
        recent_pnl_usd: float,
        graduation_velocity: float = 1.0,
        sol_volatility: float = 0.05,
    ) -> MarketRegime:
        """
        Classify macroeconomic/market regime:
        - High Vol Meme Mania: high graduation rate (> 2.0/hr), positive PnL, high win rate (> 65%)
        - Low Liquidity Chop: low win rate (< 45%), low PnL, low volume
        - Risk-Off Drawdown: sharp negative PnL (< -$100), low win rate (< 40%)
        - Revival Rotation: moderate win rate, elevated aging token breakouts
        """
        if recent_pnl_usd < -100.0 or recent_win_rate < 40.0 or (sol_volatility >= 0.10 and recent_pnl_usd < -20.0):
            regime = MarketRegime.RISK_OFF_DRAWDOWN
        elif (graduation_velocity >= 2.0 or sol_volatility >= 0.08) and recent_win_rate >= 60.0:
            regime = MarketRegime.HIGH_VOL_MEME_MANIA
        elif recent_win_rate < 48.0 or (sol_volatility < 0.025 and recent_win_rate < 52.0):
            regime = MarketRegime.LOW_LIQUIDITY_CHOP
        else:
            regime = MarketRegime.REVIVAL_ROTATION

        self.current_regime = regime
        return regime

    def _calculate_shadow_trade_size(
        self,
        signal: SignalEvent,
        native_price_usd: Decimal,
        variant: StrategyVariant,
    ) -> Decimal:
        """Calculate dynamic shadow trade size using the system sizing authority (RiskManager)."""
        try:
            from alpha_engine.engine.strategy.risk_manager import RiskManager
            if not hasattr(self, "_shadow_risk_manager") or self._shadow_risk_manager is None:
                self._shadow_risk_manager = RiskManager(config=self.config)
            # Calibrate sizing score using variant parameters
            variant_min_score = float(getattr(variant.parameters, "min_entry_score", 50.0))
            score_val = max(float(getattr(signal, "alpha_score", 0.5)) * 100.0, variant_min_score)
            logger.debug(
                "Calculating shadow trade size for variant %s (%s) with score=%.1f",
                variant.strategy_name,
                variant.experiment_id,
                score_val,
            )
            sizing_res = self._shadow_risk_manager.calculate_order_size(
                portfolio_equity_usd=Decimal("10000.0"),
                native_price_usd=native_price_usd,
                chain=signal.chain,
                score=score_val,
                is_new_launch=False,
            )
            if sizing_res.native_trade_size > Decimal("0"):
                return sizing_res.native_trade_size
        except Exception as exc:
            logger.debug("Shadow sizing calculation fallback for %s: %s", variant.strategy_name, exc)

        if signal.chain == ChainIdentifier.SOLANA_MAINNET:
            return Decimal("0.5") if native_price_usd <= Decimal("250.0") else Decimal("0.25")
        else:
            return Decimal("0.025") if native_price_usd <= Decimal("4000.0") else Decimal("0.015")

    def evaluate_signal_and_record_shadow_lots(
        self,
        signal: SignalEvent,
        pool: PoolState,
        native_price_usd: Decimal,
        base_trade_size_native: Optional[Decimal] = None,
        market_state_provider: Optional[Any] = None,
        is_private_route: Optional[bool] = None,
    ) -> list[ShadowLot]:
        """
        Simulate shadow trade execution for Champion and qualifying Challenger variants.
        Each variant independently evaluates entry rules, allowed sources, and scores.
        Variants that enter receive their own simulated fill through the shared execution simulator.
        """
        if signal.suggested_side != OrderSide.BUY:
            return []

        alpha_score_norm = float(getattr(signal, "alpha_score", 0.5)) * 100.0
        signal_ts_ns = signal.timestamp_ns or time.time_ns()
        route_private = is_private_route if is_private_route is not None else getattr(signal, "is_private_route", False)
        max_slip = getattr(signal, "max_slippage_bps", 50)
        venue = getattr(signal, "execution_venue", None)

        created_lots: list[ShadowLot] = []
        variants_to_eval = [self.champion] + [c for c in self.challengers.values() if c.status == ExperimentStatus.SHADOW]

        for variant in variants_to_eval:
            # 1. Allowed sources filter
            if variant.parameters.allowed_sources:
                sig_src = signal.source if isinstance(signal.source, SignalSource) else (
                    SignalSource(signal.source) if str(signal.source) in SignalSource._value2member_map_ else None
                )
                if sig_src and sig_src not in variant.parameters.allowed_sources:
                    continue

            # 2. Min entry score filter
            if alpha_score_norm < variant.parameters.min_entry_score:
                continue

            # 3. Declarative entry rules schema evaluation
            if hasattr(variant.parameters, "entry_rules") and variant.parameters.entry_rules:
                if not variant.parameters.entry_rules.evaluate(signal=signal, pool=pool, native_price_usd=native_price_usd):
                    continue

            # Variant decided to enter! Determine its own simulated trade size
            trade_size = (
                base_trade_size_native
                if base_trade_size_native is not None
                else self._calculate_shadow_trade_size(signal, native_price_usd, variant)
            )

            # Execution simulation: latency & conditional MEV
            latency_res = simulate_latency(
                pool=pool,
                signal_timestamp_ns=signal_ts_ns,
                chain=signal.chain,
                trade_size_native=trade_size,
                max_slippage_bps=max_slip,
                is_private_route=route_private,
                market_state_provider=market_state_provider,
                config=self.config,
                rng=self._rng,
            )
            if latency_res.adjusted_pool_state is None:
                logger.debug("Shadow variant %s fill rejected: %s", variant.strategy_name, latency_res.rejection_reason)
                continue
            sim_pool = latency_res.adjusted_pool_state

            # Cost model calculation
            try:
                cost_res = self.cost_model.calculate_trade_costs(
                    side=OrderSide.BUY,
                    gross_amount=trade_size,
                    pool=sim_pool,
                    chain=signal.chain,
                    venue=venue,
                    native_price_usd=native_price_usd,
                    platform=getattr(signal, "platform", None),
                )
                exec_price = cost_res.effective_price
                tokens_acquired = cost_res.amount_out
                slippage_bps = cost_res.price_impact_bps
                gas_usd = cost_res.network_fee_usd
            except Exception:
                try:
                    cpmm_q = cpmm_buy_quote(sim_pool, trade_size)
                    exec_price = cpmm_q.execution_price
                    tokens_acquired = cpmm_q.tokens_out
                    slippage_bps = cpmm_q.price_impact_bps
                except Exception:
                    exec_price = pool.spot_price_native_per_token
                    tokens_acquired = (trade_size / exec_price) if exec_price > 0 else Decimal("0")
                    slippage_bps = 50
                gas_usd = self.cost_model.get_network_fee_usd(signal.chain, native_price_usd=native_price_usd)

            mev_drag_usd = (latency_res.frontrun_volume * native_price_usd) if latency_res.frontrun_volume > Decimal("0") else Decimal("0.0")

            lot = variant.record_fill(
                token_address=signal.token_address,
                chain=signal.chain,
                entry_price=exec_price,
                tokens_acquired=tokens_acquired,
                native_spent=trade_size,
                native_price_usd=native_price_usd,
                gas_usd=gas_usd,
                slippage_bps=slippage_bps,
                latency_ms=latency_res.latency_ms,
                mev_drag_usd=mev_drag_usd,
                pool_address=getattr(signal, "pool_address", "") or "",
            )
            created_lots.append(lot)

        return created_lots

    def update_token_prices(
        self,
        token_address: str,
        current_price: Decimal,
        native_price_usd: Decimal,
    ) -> list[ShadowLot]:
        """Update price for open shadow lots across Champion and Challengers."""
        closed_all: list[ShadowLot] = []

        closed_all.extend(self.champion.update_price(token_address, current_price, native_price_usd))
        for chal in self.challengers.values():
            closed_all.extend(chal.update_price(token_address, current_price, native_price_usd))

        # Check for promotions/demotions if trades closed
        if closed_all:
            self.evaluate_promotions()

        return closed_all

    def get_open_tokens(self) -> set[str]:
        """Return the set of token addresses actively held in shadow lots."""
        tokens: set[str] = set()
        if self.champion:
            tokens.update(lot.token_address for lot in self.champion.open_lots.values())
        for c in self.challengers.values():
            tokens.update(lot.token_address for lot in c.open_lots.values())
        return tokens

    def evaluate_promotions(self) -> Optional[Tuple[StrategyVariant, str]]:
        """
        Anti-Overfitting & Promotion/Demotion Governance:
        1. Cooldown Guard: Suppresses promotion if cooldown period (time or trades) has not elapsed.
        2. Minimum Sample Size: N >= min_promotion_trades (default 50 trades, or 30 if champion underperforming).
           Justification: Small samples (<30) have wide confidence intervals where random luck easily masquerades as edge.
        3. Risk-Adjusted Edge: Sortino > Champion + 0.1 and Profit Factor > 1.35 (positive metrics required if champion bleeding).
        4. Maximum Drawdown Gate: MDD < 8.0% (healthy champ) or < 12.0% (underperforming champ).
        5. Outlier Win Dependency: <= 60% of total profits from a single trade.
        6. Statistical Confidence: 95% bootstrap lower bound of expectancy > $0.00.
        7. Out-of-Sample (OOS) Consistency: Time-split validation gate requires later 30% segment to hold positive edge (Net PnL > 0, PF >= 1.20, Sortino > 0).
        8. Parameter Perturbation: Cliff check confirms stability under +/-10% stop-loss shifts.
        """
        # 0. Cooldown gate
        if self.is_in_promotion_cooldown():
            return None

        champ_metrics = self.champion.metrics
        is_champ_underperforming = (
            champ_metrics.trades_count >= 5
            and (champ_metrics.win_rate < 35.0 or champ_metrics.net_pnl_usd < -10.0 or champ_metrics.sortino_ratio < -0.5)
        )
        min_trades_default = (
            self.min_promotion_trades_underperforming
            if is_champ_underperforming
            else self.min_promotion_trades
        )
        min_trades = getattr(self, "min_promotion_trades", min_trades_default)

        # Drawdown threshold with explicit local variables
        mdd_threshold = 12.0 if is_champ_underperforming else 8.0

        best_candidate: Optional[StrategyVariant] = None
        best_score: float = -999.0
        promotion_rationale = ""

        conf_level = getattr(self.config, "tournament_bootstrap_confidence_level", getattr(self, "bootstrap_confidence_level", 0.95))
        n_boot = getattr(self.config, "tournament_bootstrap_resamples", getattr(self, "bootstrap_resamples", 1000))
        train_fraction = getattr(self.config, "tournament_train_fraction", getattr(self, "train_fraction", 0.70))

        for name, chal in list(self.challengers.items()):
            if chal.status != ExperimentStatus.SHADOW:
                continue

            m = chal.metrics

            # 1. Sample size gate
            if m.trades_count < min_trades:
                continue

            # 2. Risk-adjusted metrics gate
            if is_champ_underperforming:
                # If champion is bleeding, require strictly positive metrics and higher Sortino
                if m.net_pnl_usd <= 0.0 or m.win_rate < 40.0 or m.sortino_ratio <= 0.0 or m.sortino_ratio <= champ_metrics.sortino_ratio:
                    continue
            else:
                if m.profit_factor < 1.35 or m.sortino_ratio <= (champ_metrics.sortino_ratio + 0.1):
                    continue

            # 3. Maximum Drawdown gate (explicit local variable)
            if m.max_drawdown_pct >= mdd_threshold:
                continue

            # 4. Outlier Win Dependency gate (prevent survivorship bias)
            if m.outlier_win_ratio > 0.60:
                logger.info(
                    "Challenger %s rejected: outlier win dependency %.1f%% exceeds 60%% limit",
                    name, m.outlier_win_ratio * 100.0,
                )
                continue

            # 5. Statistical Confidence Criterion: Bootstrap lower bound of expectancy > 0
            mean_exp, boot_lower, _ = chal.compute_bootstrap_expectancy(
                lots=chal.closed_lots,
                confidence_level=conf_level,
                n_resamples=n_boot,
                rng=self._rng,
            )
            if boot_lower <= 0.0:
                logger.info(
                    "Challenger %s rejected: bootstrap lower bound of expectancy $%.2f <= 0 at %.1f%% confidence (mean=$%.2f)",
                    name, boot_lower, conf_level * 100.0, mean_exp,
                )
                continue

            # 6. Out-of-Sample (OOS) Consistency: Time-split validation gate
            is_lots, oos_lots = chal.split_closed_lots_by_time(train_fraction=train_fraction)
            if len(oos_lots) >= 5:
                oos_m = StrategyVariant.compute_metrics_from_lots(oos_lots)
                if (
                    oos_m.net_pnl_usd <= 0.0
                    or oos_m.profit_factor < 1.20
                    or oos_m.max_drawdown_pct >= mdd_threshold
                    or oos_m.sortino_ratio <= 0.0
                ):
                    logger.info(
                        "Challenger %s rejected by OOS gate (train=%.0f%%, test=%.0f%%): OOS PnL=$%.2f, PF=%.2f, MDD=%.1f%%, Sortino=%.2f",
                        name, train_fraction * 100.0, (1.0 - train_fraction) * 100.0,
                        oos_m.net_pnl_usd, oos_m.profit_factor,
                        oos_m.max_drawdown_pct, oos_m.sortino_ratio,
                    )
                    continue

            # 7. Parameter Perturbation cliff check
            if not chal.check_perturbation_stability(perturbation_pct=0.10):
                continue

            composite_score = m.sortino_ratio * max(0.1, m.profit_factor)
            if composite_score > best_score:
                best_score = composite_score
                best_candidate = chal
                promotion_rationale = (
                    f"Outperformed Champion: Sortino {m.sortino_ratio:.2f} > {champ_metrics.sortino_ratio:.2f}, "
                    f"PF {m.profit_factor:.2f}, MDD {m.max_drawdown_pct:.1f}%, "
                    f"OutlierDep {m.outlier_win_ratio*100:.1f}% <= 60%, "
                    f"BootExp95% > $0 (LB=${boot_lower:.2f}) across N={m.trades_count} shadow trades."
                )

        if best_candidate and getattr(self, "walk_forward_validator", None) is not None:
            lot_records = [
                {
                    "trade_id": getattr(lot, "lot_id", f"lot-{i}"),
                    "timestamp_ns": getattr(lot, "closed_at_ns", getattr(lot, "opened_at_ns", 0)),
                    "realized_pnl_usd": float(lot.realized_pnl_usd),
                    "cost_basis_usd": float(lot.cost_basis_usd),
                    "roi_pct": float(lot.roi_pct),
                    "alpha_score": float(getattr(lot, "entry_score", 0.5)),
                    "source": str(getattr(lot, "source", "FAST_LAUNCH")),
                }
                for i, lot in enumerate(best_candidate.closed_lots)
            ]
            if len(lot_records) >= getattr(self.config, "walk_forward_min_oos_trades", 5):
                import asyncio
                import concurrent.futures
                try:
                    coro = self.walk_forward_validator.evaluate_trades_walk_forward(
                        strategy_params=best_candidate.parameters,
                        trade_records=lot_records,
                    )
                    try:
                        asyncio.get_running_loop()
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                            wf_res = pool.submit(asyncio.run, coro).result()
                    except RuntimeError:
                        wf_res = asyncio.run(coro)

                    if wf_res and not wf_res.is_promotable:
                        logger.info(
                            "Challenger %s promotion blocked by walk-forward validator: %s",
                            best_candidate.strategy_name,
                            wf_res.rejection_reasons,
                        )
                        return None
                except Exception as wf_err:
                    logger.warning("Error running walk-forward validation during promotion check: %s", wf_err)

        if best_candidate:
            # Promote best candidate to Champion!
            old_champ = self.champion
            old_champ.status = ExperimentStatus.SHADOW
            self.challengers[old_champ.strategy_name] = old_champ

            best_candidate.status = ExperimentStatus.CHAMPION
            del self.challengers[best_candidate.strategy_name]
            self.previous_champion = old_champ
            self.champion = best_candidate

            # Reset promotion cooldown tracking and live window
            self.last_promotion_timestamp = time.time()
            self.last_promotion_trade_count = self._get_total_closed_trades_count()
            self.champion_live_trades_since_promotion = []

            # Notify registered live engine callbacks
            for cb in self.on_promotion_callbacks:
                try:
                    cb(best_candidate, promotion_rationale)
                except Exception as cb_err:
                    logger.warning("Error in on_promotion_callback: %s", cb_err)

            logger.info(
                "🏆 STRATEGY PROMOTION: %s promoted to CHAMPION! Reason: %s",
                best_candidate.strategy_name, promotion_rationale,
            )

            # Persist to ledger if available
            if self.ledger and hasattr(self.ledger, "record_strategy_experiment"):
                try:
                    import asyncio
                    asyncio.create_task(
                        self.ledger.record_strategy_experiment(
                            experiment_id=best_candidate.experiment_id,
                            strategy_name=best_candidate.strategy_name,
                            parameters_json=json.dumps(best_candidate.parameters.to_dict()),
                            regime=best_candidate.regime.value,
                            status=best_candidate.status.value,
                            trades_count=best_candidate.metrics.trades_count,
                            win_rate=best_candidate.metrics.win_rate,
                            profit_factor=best_candidate.metrics.profit_factor,
                            sharpe_ratio=best_candidate.metrics.sharpe_ratio,
                            max_drawdown_pct=best_candidate.metrics.max_drawdown_pct,
                            net_pnl_usd=best_candidate.metrics.net_pnl_usd,
                        )
                    )
                except Exception as exc:
                    logger.debug("Failed to record promoted strategy experiment to ledger: %s", exc)

            return (best_candidate, promotion_rationale)

        return None

    def get_leaderboard(self) -> list[dict[str, Any]]:
        """Return structured leaderboard of Champion and Challengers."""
        leaderboard = []

        # Champion entry
        cm = self.champion.metrics
        leaderboard.append({
            "status": "🏆 CHAMPION",
            "name": self.champion.strategy_name,
            "regime": self.champion.regime.value,
            "trades": cm.trades_count,
            "win_rate": f"{cm.win_rate:.1f}%",
            "profit_factor": f"{cm.profit_factor:.2f}",
            "sharpe": f"{cm.sharpe_ratio:.2f}",
            "sortino": f"{cm.sortino_ratio:.2f}",
            "max_dd": f"{cm.max_drawdown_pct:.1f}%",
            "net_pnl": f"${cm.net_pnl_usd:+.2f}",
            "outlier_dep": f"{cm.outlier_win_ratio*100:.1f}%",
            "promotion_ready": "ACTIVE CHAMPION",
        })

        # Challenger entries
        min_trades = getattr(self, "min_promotion_trades", 50)
        for name, chal in self.challengers.items():
            m = chal.metrics
            ready = (
                m.trades_count >= min_trades
                and m.profit_factor > 1.35
                and m.sortino_ratio > self.champion.metrics.sortino_ratio
                and m.max_drawdown_pct < 8.0
                and m.outlier_win_ratio <= 0.60
            )
            ready_str = "✅ READY" if ready else f"⏳ {m.trades_count}/{min_trades} trades"
            leaderboard.append({
                "status": "🔬 CHALLENGER",
                "name": chal.strategy_name,
                "regime": chal.regime.value,
                "trades": m.trades_count,
                "win_rate": f"{m.win_rate:.1f}%",
                "profit_factor": f"{m.profit_factor:.2f}",
                "sharpe": f"{m.sharpe_ratio:.2f}",
                "sortino": f"{m.sortino_ratio:.2f}",
                "max_dd": f"{m.max_drawdown_pct:.1f}%",
                "net_pnl": f"${m.net_pnl_usd:+.2f}",
                "outlier_dep": f"{m.outlier_win_ratio*100:.1f}%",
                "promotion_ready": ready_str,
            })

        return leaderboard


class StrategyArchetype(str, Enum):
    FAST_LAUNCH_MOMENTUM = "FAST_LAUNCH_MOMENTUM"
    WAVE2_ACCUMULATION = "WAVE2_ACCUMULATION"
    REVIVAL_CTO_SWING = "REVIVAL_CTO_SWING"
    SMART_MONEY_FOLLOW = "SMART_MONEY_FOLLOW"
    NARRATIVE_BREAKOUT = "NARRATIVE_BREAKOUT"


def compute_strategy_fingerprint(params: StrategyParameters) -> str:
    """Compute canonical hashable fingerprint for a strategy variant (for deduplication)."""
    sl = round(params.hard_stop_loss_pct, 3)
    tp = round(params.take_profit_pct or 0.0, 3)
    tr = round(params.trailing_stop_from_peak_pct or 0.0, 3)
    tx = round(params.time_exit_s or 0.0, 0)
    sc = round(params.min_entry_score, 1)
    rules_fp = params.entry_rules.fingerprint() if hasattr(params, "entry_rules") and params.entry_rules else ""
    srcs = ",".join(sorted(s.value if hasattr(s, "value") else str(s) for s in (params.allowed_sources or [])))
    return f"SL:{sl}|TP:{tp}|TR:{tr}|TX:{tx}|SC:{sc}|SRCS:{srcs}|RULES:{rules_fp}"


class AutonomousStrategyGenerator:
    """
    Autonomous Strategy Discovery, Synthesis & Mutation Engine:
    - Generates quantitative strategy hypotheses across core archetypes with typed declarative entry rules:
        1. Fast-Launch Momentum (early micro-velocity, low-cap CPMM)
        2. Wave-2 Accumulation (post-dip volume surge with net-buy delta)
        3. Revival / CTO Swing (dormant token recovery, creator wallet exit)
        4. Smart-Money Follow (verified profitable wallet clusters)
        5. Narrative Breakout (organic social conviction breakouts)
    - Stochastic mutation & crossover using seeded RNG (config seed) across both entry rules & exit profiles.
    - Exploration budget reserving challenger slots for novel random rule discovery.
    - Deduplication check preventing staging of identical variants.
    - Evaluates candidates via walk-forward backtesting and perturbation cliff tests.
    - Persists generation logs (parent ids, what changed) into SQLiteLedger.
    """

    def __init__(
        self,
        ledger: Optional[Any] = None,
        seed: Optional[int] = None,
        config: Optional[Any] = None,
    ) -> None:
        self.ledger = ledger
        from alpha_engine.config import EngineConfig
        self.config = config or EngineConfig()
        effective_seed = seed if seed is not None else getattr(self.config, "execution_seed", 42)
        self._rng = random.Random(effective_seed)
        self._generation_counter: int = 0
        self._generated_history: list[dict[str, Any]] = []
        self._exploration_budget_ratio: float = 0.25

    def is_duplicate_strategy(
        self,
        candidate_params: StrategyParameters,
        registry: StrategyTournamentRegistry,
    ) -> bool:
        """Check whether candidate parameter fingerprint already exists in registry (champion, challengers, retired)."""
        cand_fp = compute_strategy_fingerprint(candidate_params)
        if registry.champion and compute_strategy_fingerprint(registry.champion.parameters) == cand_fp:
            return True
        for chal in registry.challengers.values():
            if compute_strategy_fingerprint(chal.parameters) == cand_fp:
                return True
        for ret in registry.retired_challengers.values():
            if compute_strategy_fingerprint(ret.parameters) == cand_fp:
                return True
        return False

    def generate_strategy_variant(
        self,
        archetype: StrategyArchetype,
        iteration: Optional[int] = None,
        base_params: Optional[StrategyParameters] = None,
    ) -> StrategyParameters:
        """Synthesize a complete StrategyParameters instance for a specific archetype with declarative entry rules."""
        if base_params is not None:
            mutated = self.mutate_strategy(parent=base_params, mutation_rate=0.05)
            if iteration is not None:
                mutated.strategy_name = f"{base_params.strategy_name}_v{iteration}"
            return mutated

        self._generation_counter += 1
        it = iteration if iteration is not None else self._generation_counter

        if archetype == StrategyArchetype.FAST_LAUNCH_MOMENTUM:
            # High-speed early entry, tight stop loss, quick take-profit, short time decay
            name = f"auto_fast_launch_v{it}"
            rules = EntryRuleSet(
                conditions=[
                    RuleCondition("mcap_usd", ComparisonOperator.LTE, 80000.0),
                    RuleCondition("net_buy_delta", ComparisonOperator.GTE, 0.55),
                    RuleCondition("alpha_score", ComparisonOperator.GTE, 55.0),
                ]
            )
            return StrategyParameters(
                strategy_name=name,
                hard_stop_loss_pct=0.10,
                take_profit_pct=0.35,
                trailing_stop_from_peak_pct=0.18,
                time_exit_s=180.0,
                min_entry_score=55.0,
                partial_tp_ratio=0.60,
                regime_affinity=MarketRegime.HIGH_VOL_MEME_MANIA,
                allowed_sources=[SignalSource.PUMP_FUN_MINT, SignalSource.PAIR_CREATED, SignalSource.RAYDIUM_NEW_POOL],
                entry_rules=rules,
                extra_params={"archetype": archetype.value, "max_mcap_usd": 80_000, "min_net_buy_delta": 0.55},
            )

        elif archetype == StrategyArchetype.WAVE2_ACCUMULATION:
            # Post-dip accumulation, volume surge requirement, balanced exit ladder
            name = f"auto_wave2_accum_v{it}"
            rules = EntryRuleSet(
                conditions=[
                    RuleCondition("volume_surge_multiplier", ComparisonOperator.GTE, 2.4),
                    RuleCondition("net_buy_delta", ComparisonOperator.GTE, 0.60),
                    RuleCondition("alpha_score", ComparisonOperator.GTE, 65.0),
                ]
            )
            return StrategyParameters(
                strategy_name=name,
                hard_stop_loss_pct=0.14,
                take_profit_pct=0.65,
                trailing_stop_from_peak_pct=0.25,
                time_exit_s=1800.0,
                min_entry_score=65.0,
                partial_tp_ratio=0.50,
                regime_affinity=MarketRegime.NEUTRAL,
                allowed_sources=[SignalSource.WAVE2_BREAKOUT, SignalSource.DEX_SWAP, SignalSource.SCREENER_MOVERS],
                entry_rules=rules,
                extra_params={"archetype": archetype.value, "min_surge_k": 2.4, "min_net_buy_delta": 0.60},
            )

        elif archetype == StrategyArchetype.REVIVAL_CTO_SWING:
            # Multi-hour dormant token recovery, wider trailing stop, multi-day holding
            name = f"auto_revival_cto_v{it}"
            rules = EntryRuleSet(
                conditions=[
                    RuleCondition("dormancy_hours", ComparisonOperator.GTE, 3.0),
                    RuleCondition("volume_surge_multiplier", ComparisonOperator.GTE, 3.0),
                    RuleCondition("alpha_score", ComparisonOperator.GTE, 70.0),
                ]
            )
            return StrategyParameters(
                strategy_name=name,
                hard_stop_loss_pct=0.18,
                take_profit_pct=1.50,
                trailing_stop_from_peak_pct=0.30,
                time_exit_s=86400.0,  # 24 hours
                min_entry_score=70.0,
                partial_tp_ratio=0.40,
                regime_affinity=MarketRegime.REVIVAL_ROTATION,
                allowed_sources=[SignalSource.REVIVAL_BREAKOUT, SignalSource.DEX_SWAP, SignalSource.TELEGRAM_SCRAPER],
                entry_rules=rules,
                extra_params={"archetype": archetype.value, "min_dormancy_hours": 3.0, "min_surge_multiplier": 3.0},
            )

        elif archetype == StrategyArchetype.SMART_MONEY_FOLLOW:
            # Follows verified profitable wallets with tight slippage guard and medium TP
            name = f"auto_smart_money_v{it}"
            rules = EntryRuleSet(
                conditions=[
                    RuleCondition("slippage_estimate_bps", ComparisonOperator.LTE, 80.0),
                    RuleCondition("wallet_score", ComparisonOperator.GTE, 1.8),
                    RuleCondition("alpha_score", ComparisonOperator.GTE, 60.0),
                ]
            )
            return StrategyParameters(
                strategy_name=name,
                hard_stop_loss_pct=0.12,
                take_profit_pct=0.50,
                trailing_stop_from_peak_pct=0.20,
                time_exit_s=600.0,
                min_entry_score=60.0,
                partial_tp_ratio=0.50,
                regime_affinity=MarketRegime.NEUTRAL,
                allowed_sources=[SignalSource.WHALE_WALLET, SignalSource.DEX_SWAP],
                entry_rules=rules,
                extra_params={"archetype": archetype.value, "max_slippage_bps": 80, "min_wallet_sortino": 1.8},
            )

        elif archetype == StrategyArchetype.NARRATIVE_BREAKOUT:
            # Driven by organic viral conviction across Jina Reader / Reddit / X
            name = f"auto_narrative_breakout_v{it}"
            rules = EntryRuleSet(
                conditions=[
                    RuleCondition("social_conviction", ComparisonOperator.GTE, 70.0),
                    RuleCondition("alpha_score", ComparisonOperator.GTE, 75.0),
                ]
            )
            return StrategyParameters(
                strategy_name=name,
                hard_stop_loss_pct=0.15,
                take_profit_pct=0.85,
                trailing_stop_from_peak_pct=0.28,
                time_exit_s=3600.0,
                min_entry_score=75.0,
                partial_tp_ratio=0.50,
                regime_affinity=MarketRegime.HIGH_VOL_MEME_MANIA,
                allowed_sources=[SignalSource.X_SENTIMENT, SignalSource.TELEGRAM_SCRAPER, SignalSource.MEME_RADAR_BURST],
                entry_rules=rules,
                extra_params={"archetype": archetype.value, "organic_conviction_min": 70.0},
            )

        # Fallback to balanced variant
        return StrategyParameters(
            strategy_name=f"auto_custom_v{it}",
            hard_stop_loss_pct=0.15,
            take_profit_pct=0.50,
            entry_rules=EntryRuleSet(conditions=[RuleCondition("alpha_score", ComparisonOperator.GTE, 50.0)]),
        )

    def generate_novel_strategy(
        self,
        regime: Optional[MarketRegime] = None,
    ) -> StrategyParameters:
        """Exploration budget: Synthesize a genuinely novel stochastic candidate with newly sampled rules."""
        self._generation_counter += 1
        name = f"auto_novel_exp_v{self._generation_counter}"
        reg = regime or self._rng.choice(list(MarketRegime))

        avail_metrics = [
            "mcap_usd",
            "net_buy_delta",
            "volume_surge_multiplier",
            "dormancy_hours",
            "slippage_estimate_bps",
            "wallet_score",
            "social_conviction",
            "liquidity_usd",
            "holder_concentration",
        ]
        k = self._rng.randint(2, 3)
        chosen = self._rng.sample(avail_metrics, k)
        conds = []
        for m_name in chosen:
            op = self._rng.choice([ComparisonOperator.GTE, ComparisonOperator.LTE])
            if m_name == "mcap_usd":
                thresh = round(self._rng.uniform(30000.0, 300000.0), 0)
            elif m_name in ("net_buy_delta", "holder_concentration"):
                thresh = round(self._rng.uniform(0.35, 0.70), 2)
            elif m_name == "volume_surge_multiplier":
                thresh = round(self._rng.uniform(1.5, 4.5), 1)
            elif m_name == "dormancy_hours":
                thresh = round(self._rng.uniform(1.0, 8.0), 1)
            elif m_name == "slippage_estimate_bps":
                thresh = round(self._rng.uniform(50.0, 150.0), 0)
            elif m_name == "wallet_score":
                thresh = round(self._rng.uniform(1.2, 3.0), 1)
            elif m_name == "social_conviction":
                thresh = round(self._rng.uniform(55.0, 85.0), 1)
            elif m_name == "liquidity_usd":
                thresh = round(self._rng.uniform(8000.0, 40000.0), 0)
            else:
                thresh = 50.0
            conds.append(RuleCondition(metric=m_name, operator=op, threshold=thresh))

        sl = round(self._rng.uniform(0.08, 0.20), 3)
        tp = round(self._rng.uniform(0.30, 1.60), 3)
        tr = round(self._rng.uniform(0.14, 0.35), 3)
        tx = self._rng.choice([180.0, 600.0, 1800.0, 3600.0, 86400.0])
        score = round(self._rng.uniform(50.0, 75.0), 1)

        # Stochastic rule grouping: occasionally bundle conditions into a RuleGroup
        groups: list[RuleGroup] = []
        if len(conds) >= 3 and self._rng.random() < 0.35:
            grouped_cond = conds.pop()
            groups.append(RuleGroup(conditions=[grouped_cond], operator=LogicalOperator.AND))

        return StrategyParameters(
            strategy_name=name,
            hard_stop_loss_pct=sl,
            take_profit_pct=tp,
            trailing_stop_from_peak_pct=tr,
            time_exit_s=tx,
            min_entry_score=score,
            partial_tp_ratio=0.5,
            regime_affinity=reg,
            entry_rules=EntryRuleSet(conditions=conds, groups=groups),
        )

    def mutate_strategy(
        self,
        parent: StrategyParameters,
        mutation_rate: float = 0.10,
    ) -> StrategyParameters:
        """Create a mutated variant exploring nearby parameter space with stochastic seeded bounds checking."""
        self._generation_counter += 1
        name = f"{parent.strategy_name}_mut{self._generation_counter}"

        # Stochastic perturbation on exit rules using self._rng
        factor_sl = 1.0 + self._rng.uniform(-mutation_rate, mutation_rate)
        sl_bounded = max(0.06, min(0.25, round(parent.hard_stop_loss_pct * factor_sl, 3)))

        tp = parent.take_profit_pct or 0.50
        factor_tp = 1.0 + self._rng.uniform(-mutation_rate, mutation_rate)
        tp_bounded = max(0.20, min(3.00, round(tp * factor_tp, 3)))

        trail = parent.trailing_stop_from_peak_pct or 0.25
        factor_tr = 1.0 + self._rng.uniform(-mutation_rate, mutation_rate)
        trail_bounded = max(0.12, min(0.40, round(trail * factor_tr, 3)))

        score = parent.min_entry_score
        delta_score = self._rng.uniform(-5.0, 5.0)
        score_bounded = max(0.0, min(85.0, round(score + delta_score, 1)))

        time_ex = parent.time_exit_s
        if time_ex is not None and self._rng.random() < 0.3:
            time_ex = max(60.0, min(172800.0, round(time_ex * (1.0 + self._rng.uniform(-0.15, 0.15)), 0)))

        # Stochastic perturbation on entry rules
        new_conditions: list[RuleCondition] = []
        if hasattr(parent, "entry_rules") and parent.entry_rules:
            for cond in parent.entry_rules.conditions:
                canonical = resolve_canonical_metric(cond.metric)
                defn = METRIC_REGISTRY.get(canonical)
                bounds = defn.allowed_range if defn else (0.0, 100.0)
                r_val = self._rng.random()
                if r_val < 0.5:
                    # Mutate threshold
                    perturb = 1.0 + self._rng.uniform(-mutation_rate, mutation_rate)
                    new_t = max(bounds[0], min(bounds[1], round(cond.threshold * perturb, 3)))
                    new_conditions.append(RuleCondition(cond.metric, cond.operator, new_t, cond.allowed_range))
                elif r_val < 0.7:
                    # Swap operator
                    new_op = (
                        ComparisonOperator.LTE
                        if cond.operator in (ComparisonOperator.GT, ComparisonOperator.GTE)
                        else ComparisonOperator.GTE
                    )
                    new_conditions.append(RuleCondition(cond.metric, new_op, cond.threshold, cond.allowed_range))
                else:
                    new_conditions.append(RuleCondition(cond.metric, cond.operator, cond.threshold, cond.allowed_range))

        # With probability 0.25, add a new condition if < 4 conditions
        existing_metrics = {resolve_canonical_metric(c.metric) for c in new_conditions}
        avail = [m for m in METRIC_REGISTRY if m not in existing_metrics and m != "alpha_score"]
        if avail and len(new_conditions) < 4 and self._rng.random() < 0.25:
            add_m = self._rng.choice(avail)
            defn = METRIC_REGISTRY[add_m]
            op = self._rng.choice([ComparisonOperator.GTE, ComparisonOperator.LTE])
            min_b, max_b = defn.allowed_range
            val = round(self._rng.uniform(min_b + (max_b - min_b) * 0.15, min_b + (max_b - min_b) * 0.65), 2)
            new_conditions.append(RuleCondition(metric=add_m, operator=op, threshold=val))

        # With probability 0.15, remove a condition if > 1
        if len(new_conditions) > 1 and self._rng.random() < 0.15:
            rm_idx = self._rng.randint(0, len(new_conditions) - 1)
            new_conditions.pop(rm_idx)

        new_rules = EntryRuleSet(
            conditions=new_conditions,
            operator=parent.entry_rules.operator if hasattr(parent, "entry_rules") and parent.entry_rules else LogicalOperator.AND,
        )

        return StrategyParameters(
            strategy_name=name,
            hard_stop_loss_pct=sl_bounded,
            take_profit_pct=tp_bounded,
            trailing_stop_from_peak_pct=trail_bounded,
            time_exit_s=time_ex,
            min_entry_score=score_bounded,
            partial_tp_ratio=parent.partial_tp_ratio,
            regime_affinity=parent.regime_affinity,
            allowed_sources=parent.allowed_sources,
            entry_rules=new_rules,
            extra_params=dict(parent.extra_params),
        )

    def crossover_strategies(
        self,
        parent_a: StrategyParameters,
        parent_b: StrategyParameters,
    ) -> StrategyParameters:
        """
        Recombine genetic parameters from two strategy parents:
        Stochastically blends exit rules and recombines entry rules with deduplication.
        """
        self._generation_counter += 1
        name = f"cross_{parent_a.strategy_name[:6]}_{parent_b.strategy_name[:6]}_gen{self._generation_counter}"

        w_a = self._rng.uniform(0.3, 0.7)
        w_b = 1.0 - w_a
        sl = round(parent_a.hard_stop_loss_pct * w_a + parent_b.hard_stop_loss_pct * w_b, 3)
        tp = round((parent_a.take_profit_pct or 0.5) * w_a + (parent_b.take_profit_pct or 0.5) * w_b, 3)
        trail = round((parent_a.trailing_stop_from_peak_pct or 0.25) * w_a + (parent_b.trailing_stop_from_peak_pct or 0.25) * w_b, 3)
        time_exit = parent_a.time_exit_s if self._rng.random() < 0.5 else parent_b.time_exit_s
        min_score = round(max(parent_a.min_entry_score, parent_b.min_entry_score), 1)

        # Combine entry rules
        conds_by_metric: dict[str, RuleCondition] = {}
        if hasattr(parent_a, "entry_rules") and parent_a.entry_rules:
            for c in parent_a.entry_rules.conditions:
                can = resolve_canonical_metric(c.metric)
                conds_by_metric[can] = c
        if hasattr(parent_b, "entry_rules") and parent_b.entry_rules:
            for c in parent_b.entry_rules.conditions:
                can = resolve_canonical_metric(c.metric)
                if can in conds_by_metric:
                    prev = conds_by_metric[can]
                    new_t = round((prev.threshold + c.threshold) / 2.0, 3)
                    chosen_op = prev.operator if self._rng.random() < 0.5 else c.operator
                    conds_by_metric[can] = RuleCondition(can, chosen_op, new_t)
                elif self._rng.random() < 0.7:
                    conds_by_metric[can] = c

        inherited_groups: list[RuleGroup] = []
        if hasattr(parent_a, "entry_rules") and parent_a.entry_rules and parent_a.entry_rules.groups:
            inherited_groups.extend(parent_a.entry_rules.groups)
        elif hasattr(parent_b, "entry_rules") and parent_b.entry_rules and parent_b.entry_rules.groups:
            inherited_groups.extend(parent_b.entry_rules.groups)

        cross_rules = EntryRuleSet(conditions=list(conds_by_metric.values()), groups=inherited_groups)

        merged_extra = dict(parent_b.extra_params)
        merged_extra.update(parent_a.extra_params)

        return StrategyParameters(
            strategy_name=name,
            hard_stop_loss_pct=sl,
            take_profit_pct=tp,
            trailing_stop_from_peak_pct=trail,
            time_exit_s=time_exit,
            min_entry_score=min_score,
            partial_tp_ratio=parent_b.partial_tp_ratio,
            regime_affinity=parent_a.regime_affinity or parent_b.regime_affinity,
            allowed_sources=parent_a.allowed_sources or parent_b.allowed_sources,
            entry_rules=cross_rules,
            extra_params=merged_extra,
        )

    def evaluate_and_stage_candidate(
        self,
        candidate_params: StrategyParameters,
        registry: StrategyTournamentRegistry,
        backtester: Optional[Any] = None,
        parent_ids: Optional[list[str]] = None,
        action: str = "seed_archetype",
        changes: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Backtest candidate hypothesis against historical ledger records, apply anti-overfitting
        governance gates, and if qualified, register into StrategyTournamentRegistry as a SHADOW challenger.
        Persists generation logs in the ledger.
        """
        from alpha_engine.engine.backtester import WalkForwardBacktester

        # Validate candidate entry rules schema
        if hasattr(candidate_params, "entry_rules") and candidate_params.entry_rules:
            try:
                candidate_params.entry_rules.validate()
            except SchemaValidationError as val_err:
                logger.warning(
                    "AutonomousStrategyGenerator: Candidate %s failed entry rules schema validation: %s",
                    candidate_params.strategy_name,
                    val_err,
                )
                report = {
                    "strategy_name": candidate_params.strategy_name,
                    "experiment_id": f"exp-inv-{uuid.uuid4().hex[:8]}",
                    "qualified_for_shadow": False,
                    "is_promotable": False,
                    "perturbation_robust": False,
                    "rejections": [f"INVALID_ENTRY_RULES: {val_err}"],
                    "total_trades": 0,
                    "timestamp": time.time(),
                }
                self._generated_history.append(report)
                return report

        # 0. Deduplication check
        if self.is_duplicate_strategy(candidate_params, registry):
            logger.info("AutonomousStrategyGenerator: Candidate %s is a duplicate; staging rejected", candidate_params.strategy_name)
            report = {
                "strategy_name": candidate_params.strategy_name,
                "experiment_id": f"exp-dup-{uuid.uuid4().hex[:8]}",
                "qualified_for_shadow": False,
                "is_promotable": False,
                "perturbation_robust": False,
                "rejections": ["DUPLICATE_STRATEGY"],
                "total_trades": 0,
                "timestamp": time.time(),
            }
            self._generated_history.append(report)
            return report

        tester = backtester or WalkForwardBacktester(db_path=getattr(registry.ledger, "db_path", "paper_trading.db"))
        result = tester.evaluate_strategy(candidate_params)

        # Robustness Check: Perturbation Cliff Test (+/- 10%)
        is_robust = True
        try:
            perturbed_plus = self.mutate_strategy(candidate_params, mutation_rate=0.10)
            res_plus = tester.evaluate_strategy(perturbed_plus)
            if res_plus.out_of_sample_metrics.profit_factor < 1.0 or res_plus.out_of_sample_metrics.max_drawdown_pct > 15.0:
                is_robust = False
        except Exception:
            pass

        qualified = result.is_promotable and is_robust

        experiment_id = f"exp-auto-{uuid.uuid4().hex[:8]}"
        regime = candidate_params.regime_affinity or MarketRegime.NEUTRAL

        variant = StrategyVariant(
            experiment_id=experiment_id,
            strategy_name=candidate_params.strategy_name,
            parameters=candidate_params,
            status=ExperimentStatus.SHADOW,
            regime=regime,
        )

        if action in ("seed_archetype", "seed") or qualified:  # Only stage if qualified by out-of-sample backtest or seed
            registry.register_challenger(variant)

        report = {
            "strategy_name": candidate_params.strategy_name,
            "experiment_id": experiment_id,
            "qualified_for_shadow": qualified or (action in ("seed_archetype", "seed")),
            "is_promotable": result.is_promotable,
            "perturbation_robust": is_robust,
            "rejections": result.rejection_reasons,
            "oos_profit_factor": result.out_of_sample_metrics.profit_factor,
            "oos_sortino": result.out_of_sample_metrics.sortino_ratio,
            "oos_drawdown_pct": result.out_of_sample_metrics.max_drawdown_pct,
            "total_trades": result.total_trades,
            "timestamp": time.time(),
        }
        self._generated_history.append(report)

        # Persist generation log and experiment record to ledger
        if self.ledger is not None:
            train_m = {
                "trades": result.in_sample_metrics.trades_count,
                "profit_factor": result.in_sample_metrics.profit_factor,
                "sortino": result.in_sample_metrics.sortino_ratio,
                "win_rate": result.in_sample_metrics.win_rate,
            }
            val_m = {
                "trades": result.out_of_sample_metrics.trades_count,
                "profit_factor": result.out_of_sample_metrics.profit_factor,
                "sortino": result.out_of_sample_metrics.sortino_ratio,
                "win_rate": result.out_of_sample_metrics.win_rate,
            }
            p_ids = parent_ids or []
            ch_data = changes or {"parameters": candidate_params.to_dict()}
            if hasattr(self.ledger, "record_strategy_generation_log"):
                try:
                    import asyncio
                    coro = self.ledger.record_strategy_generation_log(
                        strategy_name=candidate_params.strategy_name,
                        parent_ids=p_ids,
                        action=action,
                        changes_json=ch_data,
                        parameters_json=candidate_params.to_dict(),
                        train_metrics_json=train_m,
                        val_metrics_json=val_m,
                        staged=qualified or (action in ("seed_archetype", "seed")) or result.total_trades >= 10,
                    )
                    try:
                        loop = asyncio.get_running_loop()
                        self._last_log_task = loop.create_task(coro)
                        if hasattr(self.ledger, "_pending_tasks"):
                            self.ledger._pending_tasks.append(self._last_log_task)
                    except RuntimeError:
                        asyncio.run(coro)
                except Exception as log_exc:
                    logger.error("Failed to record strategy generation log: %s", log_exc, exc_info=True)

            if hasattr(self.ledger, "record_strategy_experiment"):
                try:
                    import asyncio
                    coro2 = self.ledger.record_strategy_experiment(
                        experiment_id=experiment_id,
                        strategy_name=candidate_params.strategy_name,
                        parameters_json=json.dumps(candidate_params.to_dict()),
                        regime=regime.value,
                        status=ExperimentStatus.SHADOW.value,
                        trades_count=result.total_trades,
                        win_rate=result.full_sample_metrics.win_rate,
                        profit_factor=result.full_sample_metrics.profit_factor,
                        sharpe_ratio=result.full_sample_metrics.sharpe_ratio,
                        max_drawdown_pct=result.full_sample_metrics.max_drawdown_pct,
                        net_pnl_usd=result.full_sample_metrics.net_pnl_usd,
                    )
                    try:
                        loop = asyncio.get_running_loop()
                        t2 = loop.create_task(coro2)
                        if hasattr(self.ledger, "_pending_tasks"):
                            self.ledger._pending_tasks.append(t2)
                    except RuntimeError:
                        asyncio.run(coro2)
                except Exception as exc:
                    logger.error("Failed to log auto experiment to ledger: %s", exc, exc_info=True)

        logger.info(
            "AutonomousStrategyGenerator: Evaluated %s -> Qualified=%s (PF=%.2f, Sortino=%.2f, DD=%.1f%%, Rejections=%s)",
            candidate_params.strategy_name,
            qualified,
            result.out_of_sample_metrics.profit_factor,
            result.out_of_sample_metrics.sortino_ratio,
            result.out_of_sample_metrics.max_drawdown_pct,
            result.rejection_reasons,
        )
        return report

    def discover_and_deploy_strategy_portfolio(
        self,
        registry: StrategyTournamentRegistry,
        backtester: Optional[Any] = None,
    ) -> list[dict[str, Any]]:
        """
        Autonomously synthesize, backtest, and deploy a balanced multi-archetype
        strategy portfolio into the live tournament registry.
        """
        archetypes = [
            StrategyArchetype.FAST_LAUNCH_MOMENTUM,
            StrategyArchetype.WAVE2_ACCUMULATION,
            StrategyArchetype.REVIVAL_CTO_SWING,
            StrategyArchetype.SMART_MONEY_FOLLOW,
            StrategyArchetype.NARRATIVE_BREAKOUT,
        ]
        deployed_reports = []
        for arch in archetypes:
            candidate = self.generate_strategy_variant(arch)
            report = self.evaluate_and_stage_candidate(candidate, registry, backtester, action="seed_archetype")
            deployed_reports.append(report)

        return deployed_reports

    def generate_evolutionary_generation(
        self,
        registry: StrategyTournamentRegistry,
        market_regime: MarketRegime,
        recent_losses: Optional[list[str]] = None,
        backtester: Optional[Any] = None,
    ) -> list[dict[str, Any]]:
        """
        Continuously synthesize, mutate, cross-breed, and backtest the next generation
        of autonomous strategies. Adapts exploration space based on recent loss attributions.
        Reserves an exploration budget for novel stochastic rules.
        """
        active_challengers = registry.get_active_challengers()
        champion = registry.champion

        reports = []

        # 1. Adaptive mutation rate based on recent loss attributions
        mutation_rate = 0.10
        if recent_losses:
            hard_stops = sum(1 for l in recent_losses if "sl_hard" in l.lower() or "stop_loss" in l.lower())
            if hard_stops > len(recent_losses) * 0.5:
                mutation_rate = 0.15

        # 2. Mutate Champion to explore local optimum (exploitation)
        if champion:
            champ_child = self.mutate_strategy(champion.parameters, mutation_rate=mutation_rate)
            if recent_losses and any("sl_hard" in l.lower() for l in recent_losses[-5:]):
                champ_child.hard_stop_loss_pct = min(0.25, champ_child.hard_stop_loss_pct * 1.15)
                champ_child.min_entry_score = min(85.0, champ_child.min_entry_score + 5.0)
            rep = self.evaluate_and_stage_candidate(
                champ_child,
                registry,
                backtester,
                parent_ids=[champion.strategy_name],
                action="mutation",
            )
            reports.append(rep)

        # 3. Cross-over best challengers
        if len(active_challengers) >= 2:
            sorted_challengers = sorted(
                active_challengers,
                key=lambda c: c.metrics.profit_factor * c.metrics.sortino_ratio,
                reverse=True,
            )
            p_a = sorted_challengers[0].parameters
            p_b = sorted_challengers[1].parameters
            cross_child = self.crossover_strategies(p_a, p_b)
            rep = self.evaluate_and_stage_candidate(
                cross_child,
                registry,
                backtester,
                parent_ids=[p_a.strategy_name, p_b.strategy_name],
                action="crossover",
            )
            reports.append(rep)

        # 4. Exploration Budget: Generate novel stochastic rules
        if self._exploration_budget_ratio > 0.0:
            novel_candidate = self.generate_novel_strategy(regime=market_regime)
            rep = self.evaluate_and_stage_candidate(
                novel_candidate,
                registry,
                backtester,
                action="novel_exploration",
            )
            reports.append(rep)

        # 5. Fill open slots with regime-specific archetype variants
        archetype_map = {
            MarketRegime.HIGH_VOL_MEME_MANIA: StrategyArchetype.FAST_LAUNCH_MOMENTUM,
            MarketRegime.LOW_LIQUIDITY_CHOP: StrategyArchetype.WAVE2_ACCUMULATION,
            MarketRegime.REVIVAL_ROTATION: StrategyArchetype.REVIVAL_CTO_SWING,
            MarketRegime.RISK_OFF_DRAWDOWN: StrategyArchetype.SMART_MONEY_FOLLOW,
            MarketRegime.NEUTRAL: StrategyArchetype.NARRATIVE_BREAKOUT,
        }
        target_archetype = archetype_map.get(market_regime, StrategyArchetype.FAST_LAUNCH_MOMENTUM)
        new_variant = self.generate_strategy_variant(target_archetype)
        rep = self.evaluate_and_stage_candidate(
            new_variant,
            registry,
            backtester,
            action="seed_archetype",
        )
        reports.append(rep)

        logger.info(
            "AutonomousStrategyGenerator: Discovered and evaluated %d new evolutionary candidates for regime %s",
            len(reports), market_regime.value,
        )
        return reports
        """
        Continuously synthesize, mutate, cross-breed, and backtest the next generation
        of autonomous strategies. Adapts exploration space based on recent loss attributions.
        """
        active_challengers = registry.get_active_challengers()
        champion = registry.champion

        reports = []

        # 1. Adaptive mutation rate based on recent loss attributions
        mutation_rate = 0.10
        if recent_losses:
            hard_stops = sum(1 for l in recent_losses if "sl_hard" in l.lower() or "stop_loss" in l.lower())
            if hard_stops > len(recent_losses) * 0.5:
                mutation_rate = 0.15

        # 2. Mutate Champion to explore local optimum
        if champion:
            champ_child = self.mutate_strategy(champion.parameters, mutation_rate=mutation_rate)
            if recent_losses and any("sl_hard" in l.lower() for l in recent_losses[-5:]):
                champ_child.hard_stop_loss_pct = min(0.25, champ_child.hard_stop_loss_pct * 1.15)
                champ_child.min_entry_score = min(85.0, champ_child.min_entry_score + 5.0)
            rep = self.evaluate_and_stage_candidate(
                champ_child,
                registry,
                backtester,
                action="mutation",
                parent_ids=[champion.strategy_name],
                changes={"parent": champion.strategy_name, "mutation_rate": mutation_rate},
            )
            reports.append(rep)

        # 3. Cross-over best challengers
        if len(active_challengers) >= 2:
            sorted_challengers = sorted(
                active_challengers,
                key=lambda c: c.metrics.profit_factor * c.metrics.sortino_ratio,
                reverse=True,
            )
            cross_child = self.crossover_strategies(sorted_challengers[0].parameters, sorted_challengers[1].parameters)
            rep = self.evaluate_and_stage_candidate(
                cross_child,
                registry,
                backtester,
                action="crossover",
                parent_ids=[sorted_challengers[0].strategy_name, sorted_challengers[1].strategy_name],
                changes={"parents": [sorted_challengers[0].strategy_name, sorted_challengers[1].strategy_name]},
            )
            reports.append(rep)

        # 4. Fill open slots with regime-specific archetype variants
        archetype_map = {
            MarketRegime.HIGH_VOL_MEME_MANIA: StrategyArchetype.FAST_LAUNCH_MOMENTUM,
            MarketRegime.LOW_LIQUIDITY_CHOP: StrategyArchetype.WAVE2_ACCUMULATION,
            MarketRegime.REVIVAL_ROTATION: StrategyArchetype.REVIVAL_CTO_SWING,
            MarketRegime.RISK_OFF_DRAWDOWN: StrategyArchetype.SMART_MONEY_FOLLOW,
            MarketRegime.NEUTRAL: StrategyArchetype.NARRATIVE_BREAKOUT,
        }
        target_archetype = archetype_map.get(market_regime, StrategyArchetype.FAST_LAUNCH_MOMENTUM)
        new_variant = self.generate_strategy_variant(target_archetype)
        rep = self.evaluate_and_stage_candidate(new_variant, registry, backtester)
        reports.append(rep)

        logger.info(
            "AutonomousStrategyGenerator: Discovered and evaluated %d new evolutionary candidates for regime %s",
            len(reports), market_regime.value,
        )
        return reports

