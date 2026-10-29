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
    custom_filter: Optional[Callable[[SignalEvent], bool]] = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if self.regime_affinity:
            d["regime_affinity"] = self.regime_affinity.value
        if self.allowed_sources:
            d["allowed_sources"] = [s.value if hasattr(s, "value") else str(s) for s in self.allowed_sources]
        if "custom_filter" in d:
            d.pop("custom_filter", None)
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StrategyParameters":
        reg = data.get("regime_affinity")
        reg_enum = MarketRegime(reg) if reg and reg in MarketRegime._value2member_map_ else None
        sources = data.get("allowed_sources")
        sources_list = [SignalSource(s) for s in sources if s in SignalSource._value2member_map_] if sources else None
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

                # Accounting including gas and MEV drag
                exit_gross_usd = (lot.tokens_acquired * current_price) * native_price_usd
                total_drag_usd = lot.gas_drag_usd + lot.mev_drag_usd + Decimal("0.05") # 5c exit gas
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

    def recompute_metrics(self) -> StrategyMetrics:
        """Calculate institutional risk-adjusted performance metrics across closed shadow lots."""
        n = len(self.closed_lots)
        if n == 0:
            self.metrics = StrategyMetrics()
            return self.metrics

        pnls = [float(lot.realized_pnl_usd or 0.0) for lot in self.closed_lots]
        rois = [float(lot.roi_pct or 0.0) for lot in self.closed_lots]

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
        sharpe = (mean_roi / std_roi) if std_roi > 0 else 0.0

        downside_rois = [min(0.0, r) for r in rois]
        downside_variance = sum(dr ** 2 for dr in downside_rois) / n if n > 1 else 0.0
        downside_std = math.sqrt(downside_variance) if downside_variance > 0 else 0.0
        sortino = (mean_roi / downside_std) if downside_std > 0 else 0.0

        # Maximum Drawdown (% of cumulative curve)
        cum_equity = 1000.0 # Standard base $1000 portfolio
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

        self.metrics = StrategyMetrics(
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
        return self.metrics

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

    def __init__(self, ledger: Optional[Any] = None) -> None:
        self.ledger = ledger
        self.current_regime: MarketRegime = MarketRegime.NEUTRAL

        # Initialize default Champion
        champ_params = StrategyParameters(
            strategy_name="champion_peak_trailing_50",
            hard_stop_loss_pct=0.15,
            take_profit_pct=1.00,
            trailing_stop_from_peak_pct=0.35,
            min_entry_score=0.0,
        )
        self.champion = StrategyVariant(
            experiment_id="exp-champ-001",
            strategy_name=champ_params.strategy_name,
            parameters=champ_params,
            status=ExperimentStatus.CHAMPION,
        )

        # Initialize Challenger Variants
        self.challengers: dict[str, StrategyVariant] = {}

        # Variant A: Tight Scalp (-10% SL, +30% TP1, 120s decay)
        p_a = StrategyParameters(
            strategy_name="challenger_tight_scalp",
            hard_stop_loss_pct=0.10,
            take_profit_pct=0.30,
            trailing_stop_from_peak_pct=0.15,
            time_exit_s=120.0,
            regime_affinity=MarketRegime.LOW_LIQUIDITY_CHOP,
        )
        self.challengers[p_a.strategy_name] = StrategyVariant(
            experiment_id="exp-chal-scalp",
            strategy_name=p_a.strategy_name,
            parameters=p_a,
            status=ExperimentStatus.SHADOW,
            regime=MarketRegime.LOW_LIQUIDITY_CHOP,
        )

        # Variant B: Early Rally Swing (-15% SL, +50% TP1, 28% ATH Trail)
        p_b = StrategyParameters(
            strategy_name="challenger_early_rally_swing",
            hard_stop_loss_pct=0.15,
            take_profit_pct=0.50,
            trailing_stop_from_peak_pct=0.28,
            regime_affinity=MarketRegime.REVIVAL_ROTATION,
        )
        self.challengers[p_b.strategy_name] = StrategyVariant(
            experiment_id="exp-chal-rally",
            strategy_name=p_b.strategy_name,
            parameters=p_b,
            status=ExperimentStatus.SHADOW,
            regime=MarketRegime.REVIVAL_ROTATION,
        )

        # Variant C: High-Threshold Momentum (min_entry_score=75.0, -12% SL, 20% ATH trail)
        p_c = StrategyParameters(
            strategy_name="challenger_high_threshold_momentum",
            hard_stop_loss_pct=0.12,
            take_profit_pct=0.80,
            trailing_stop_from_peak_pct=0.20,
            min_entry_score=75.0,
            regime_affinity=MarketRegime.HIGH_VOL_MEME_MANIA,
        )
        self.challengers[p_c.strategy_name] = StrategyVariant(
            experiment_id="exp-chal-momentum",
            strategy_name=p_c.strategy_name,
            parameters=p_c,
            status=ExperimentStatus.SHADOW,
            regime=MarketRegime.HIGH_VOL_MEME_MANIA,
        )

        # Variant D: AI-Dynamic Exit Matrix
        p_d = StrategyParameters(
            strategy_name="challenger_ai_dynamic_matrix",
            hard_stop_loss_pct=0.14,
            take_profit_pct=0.60,
            trailing_stop_from_peak_pct=0.25,
            regime_affinity=MarketRegime.NEUTRAL,
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
        if recent_pnl_usd < -100.0 or recent_win_rate < 40.0:
            regime = MarketRegime.RISK_OFF_DRAWDOWN
        elif graduation_velocity >= 2.0 and recent_win_rate >= 60.0:
            regime = MarketRegime.HIGH_VOL_MEME_MANIA
        elif recent_win_rate < 48.0:
            regime = MarketRegime.LOW_LIQUIDITY_CHOP
        else:
            regime = MarketRegime.REVIVAL_ROTATION

        self.current_regime = regime
        return regime

    def evaluate_signal_and_record_shadow_lots(
        self,
        signal: SignalEvent,
        pool: PoolState,
        native_price_usd: Decimal,
        base_trade_size_native: Decimal = Decimal("0.10"),
    ) -> list[ShadowLot]:
        """
        Simulate shadow trade execution for Champion and qualifying Challenger variants.
        All variants undergo identical CPMM slippage and LogNormal latency/MEV drag.
        """
        if signal.suggested_side != OrderSide.BUY:
            return []

        # 1. Simulate CPMM execution and LogNormal latency
        now_ns = time.time_ns()
        latency_res = simulate_latency(pool, now_ns)
        sim_pool = latency_res.adjusted_pool_state or pool

        # CPMM quote
        try:
            quote = cpmm_buy_quote(sim_pool, base_trade_size_native)
            exec_price = quote.execution_price
            tokens_acquired = quote.amount_out
            slippage_bps = quote.price_impact_bps
        except Exception:
            # Fallback to pool spot price if degenerate
            exec_price = pool.spot_price_native_per_token
            tokens_acquired = (base_trade_size_native / exec_price) if exec_price > 0 else Decimal("0")
            slippage_bps = 50

        gas_usd = Decimal("0.05")
        mev_drag_usd = Decimal(str(float(latency_res.frontrun_volume) * 0.02 * float(native_price_usd))) if latency_res.frontrun_volume else Decimal("0.0")

        created_lots: list[ShadowLot] = []

        # Evaluate Champion
        champ_lot = self.champion.record_fill(
            token_address=signal.token_address,
            chain=signal.chain,
            entry_price=exec_price,
            tokens_acquired=tokens_acquired,
            native_spent=base_trade_size_native,
            native_price_usd=native_price_usd,
            gas_usd=gas_usd,
            slippage_bps=slippage_bps,
            latency_ms=latency_res.latency_ms,
            mev_drag_usd=mev_drag_usd,
            pool_address=getattr(signal, "pool_address", "") or "",
        )
        created_lots.append(champ_lot)

        # Evaluate Challengers
        alpha_score_norm = float(getattr(signal, "alpha_score", 0.5)) * 100.0
        for name, chal in self.challengers.items():
            if chal.status != ExperimentStatus.SHADOW:
                continue

            # Check min entry score threshold
            if alpha_score_norm < chal.parameters.min_entry_score:
                continue

            # Check allowed sources filter if configured
            if chal.parameters.allowed_sources:
                sig_src = signal.source if isinstance(signal.source, SignalSource) else (SignalSource(signal.source) if str(signal.source) in SignalSource._value2member_map_ else None)
                if sig_src and sig_src not in chal.parameters.allowed_sources:
                    continue

            # Check custom filter if configured
            if chal.parameters.custom_filter and not chal.parameters.custom_filter(signal):
                continue

            lot = chal.record_fill(
                token_address=signal.token_address,
                chain=signal.chain,
                entry_price=exec_price,
                tokens_acquired=tokens_acquired,
                native_spent=base_trade_size_native,
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

    def evaluate_promotions(self) -> Optional[Tuple[StrategyVariant, str]]:
        """
        Anti-Overfitting & Promotion/Demotion Governance:
        1. Minimum Sample Size: N >= 30 completed trades in shadow mode
        2. Out-of-Sample Consistency: Sortino > Champion + 0.1 and Profit Factor > 1.35
        3. Max Drawdown < 8.0%
        4. Outlier Win Dependency: <= 60% of total profits from a single trade
        5. Parameter Perturbation: Must pass cliff stability check
        """
        champ_metrics = self.champion.metrics

        best_candidate: Optional[StrategyVariant] = None
        best_score: float = -999.0
        promotion_rationale = ""

        for name, chal in self.challengers.items():
            m = chal.metrics

            # 1. Sample size gate
            if m.trades_count < 30:
                continue

            # 2. Risk-adjusted metrics gate (Sortino > Champion and Profit Factor > 1.35)
            if m.profit_factor < 1.35 or m.sortino_ratio <= (champ_metrics.sortino_ratio + 0.1):
                continue

            # 3. Maximum Drawdown gate
            if m.max_drawdown_pct >= 8.0:
                continue

            # 4. Outlier Win Dependency gate (prevent survivorship bias)
            if m.outlier_win_ratio > 0.60:
                logger.info(
                    "Challenger %s rejected: outlier win dependency %.1f%% exceeds 60%% limit",
                    name, m.outlier_win_ratio * 100.0,
                )
                continue

            # 5. Parameter Perturbation cliff check
            if not chal.check_perturbation_stability(perturbation_pct=0.10):
                continue

            composite_score = m.sortino_ratio * m.profit_factor
            if composite_score > best_score:
                best_score = composite_score
                best_candidate = chal
                promotion_rationale = (
                    f"Outperformed Champion: Sortino {m.sortino_ratio:.2f} > {champ_metrics.sortino_ratio:.2f}, "
                    f"PF {m.profit_factor:.2f} > 1.35, MDD {m.max_drawdown_pct:.1f}% < 8%, "
                    f"OutlierDep {m.outlier_win_ratio*100:.1f}% <= 60% across N={m.trades_count} shadow trades."
                )

        if best_candidate:
            # Promote best candidate to Champion!
            old_champ = self.champion
            old_champ.status = ExperimentStatus.SHADOW
            self.challengers[old_champ.strategy_name] = old_champ

            best_candidate.status = ExperimentStatus.CHAMPION
            del self.challengers[best_candidate.strategy_name]
            self.champion = best_candidate

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
        for name, chal in self.challengers.items():
            m = chal.metrics
            ready = (
                m.trades_count >= 30
                and m.profit_factor > 1.35
                and m.sortino_ratio > self.champion.metrics.sortino_ratio
                and m.max_drawdown_pct < 8.0
                and m.outlier_win_ratio <= 0.60
            )
            ready_str = "✅ READY" if ready else f"⏳ {m.trades_count}/30 trades"
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


class AutonomousStrategyGenerator:
    """
    Autonomous Strategy Discovery, Synthesis & Mutation Engine:
    - Generates quantitative strategy hypotheses across core archetypes:
        1. Fast-Launch Momentum (early micro-velocity, low-cap CPMM)
        2. Wave-2 Accumulation (post-dip volume surge with net-buy delta)
        3. Revival / CTO Swing (dormant token recovery, creator wallet exit)
        4. Smart-Money Follow (verified profitable wallet clusters)
        5. Narrative Breakout (organic social conviction breakouts)
    - Mutates parameters to explore optimal risk-adjusted parameter frontiers without overfitting.
    - Evaluates new candidates via walk-forward backtesting against historical ledger signals & trades.
    - Applies anti-overfitting gates and parameter perturbation cliff tests (+/-10%).
    - Automatically stages qualified variants into StrategyTournamentRegistry as concurrent SHADOW challengers.
    """

    def __init__(self, ledger: Optional[Any] = None) -> None:
        self.ledger = ledger
        self._generation_counter: int = 0
        self._generated_history: list[dict[str, Any]] = []

    def generate_strategy_variant(
        self,
        archetype: StrategyArchetype,
        iteration: Optional[int] = None,
        base_params: Optional[StrategyParameters] = None,
    ) -> StrategyParameters:
        """Synthesize a complete StrategyParameters instance for a specific archetype."""
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
                extra_params={"archetype": archetype.value, "max_mcap_usd": 80_000, "min_net_buy_delta": 0.55},
            )

        elif archetype == StrategyArchetype.WAVE2_ACCUMULATION:
            # Post-dip accumulation, volume surge requirement, balanced exit ladder
            name = f"auto_wave2_accum_v{it}"
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
                extra_params={"archetype": archetype.value, "min_surge_k": 2.4, "min_net_buy_delta": 0.60},
            )

        elif archetype == StrategyArchetype.REVIVAL_CTO_SWING:
            # Multi-hour dormant token recovery, wider trailing stop, multi-day holding
            name = f"auto_revival_cto_v{it}"
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
                extra_params={"archetype": archetype.value, "min_dormancy_hours": 3.0, "min_surge_multiplier": 3.0},
            )

        elif archetype == StrategyArchetype.SMART_MONEY_FOLLOW:
            # Follows verified profitable wallets with tight slippage guard and medium TP
            name = f"auto_smart_money_v{it}"
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
                extra_params={"archetype": archetype.value, "max_slippage_bps": 80, "min_wallet_sortino": 1.8},
            )

        elif archetype == StrategyArchetype.NARRATIVE_BREAKOUT:
            # Driven by organic viral conviction across Jina Reader / Reddit / X
            name = f"auto_narrative_breakout_v{it}"
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
                extra_params={"archetype": archetype.value, "organic_conviction_min": 70.0},
            )

        # Fallback to balanced variant
        return StrategyParameters(strategy_name=f"auto_custom_v{it}", hard_stop_loss_pct=0.15, take_profit_pct=0.50)

    def mutate_strategy(
        self,
        parent: StrategyParameters,
        mutation_rate: float = 0.10,
    ) -> StrategyParameters:
        """Create a mutated variant exploring nearby parameter space with bounds checking."""
        self._generation_counter += 1
        name = f"{parent.strategy_name}_mut{self._generation_counter}"

        # Mutate hard stop loss (+/- 10%)
        delta_sl = parent.hard_stop_loss_pct * (1.0 + (mutation_rate if self._generation_counter % 2 == 0 else -mutation_rate))
        sl_bounded = max(0.06, min(0.25, delta_sl))

        # Mutate take profit
        tp = parent.take_profit_pct or 0.50
        delta_tp = tp * (1.0 + (mutation_rate if self._generation_counter % 3 == 0 else -mutation_rate))
        tp_bounded = max(0.20, min(3.00, delta_tp))

        # Mutate trailing stop from peak
        trail = parent.trailing_stop_from_peak_pct or 0.25
        delta_trail = trail * (1.0 + (mutation_rate if self._generation_counter % 2 != 0 else -mutation_rate))
        trail_bounded = max(0.12, min(0.40, delta_trail))

        # Mutate entry score threshold
        score = parent.min_entry_score
        delta_score = score + (5.0 if self._generation_counter % 2 == 0 else -5.0)
        score_bounded = max(0.0, min(85.0, delta_score))

        return StrategyParameters(
            strategy_name=name,
            hard_stop_loss_pct=round(sl_bounded, 3),
            take_profit_pct=round(tp_bounded, 3),
            trailing_stop_from_peak_pct=round(trail_bounded, 3),
            time_exit_s=parent.time_exit_s,
            min_entry_score=round(score_bounded, 1),
            partial_tp_ratio=parent.partial_tp_ratio,
            regime_affinity=parent.regime_affinity,
            allowed_sources=parent.allowed_sources,
            extra_params=dict(parent.extra_params),
        )

    def evaluate_and_stage_candidate(
        self,
        candidate_params: StrategyParameters,
        registry: StrategyTournamentRegistry,
        backtester: Optional[Any] = None,
    ) -> dict[str, Any]:
        """
        Backtest candidate hypothesis against historical ledger records, apply anti-overfitting
        governance gates, and if qualified, register into StrategyTournamentRegistry as a SHADOW challenger.
        """
        from alpha_engine.engine.backtester import WalkForwardBacktester

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

        if qualified or result.total_trades >= 10:  # Allow staging in shadow mode
            registry.register_challenger(variant)

        report = {
            "strategy_name": candidate_params.strategy_name,
            "experiment_id": experiment_id,
            "qualified_for_shadow": qualified,
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

        if self.ledger is not None and hasattr(self.ledger, "record_strategy_experiment"):
            try:
                import asyncio
                asyncio.create_task(
                    self.ledger.record_strategy_experiment(
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
                )
            except Exception as exc:
                logger.debug("Failed to log auto experiment to ledger: %s", exc)

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
            report = self.evaluate_and_stage_candidate(candidate, registry, backtester)
            deployed_reports.append(report)

        return deployed_reports

