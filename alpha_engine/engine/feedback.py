"""
alpha_engine.engine.feedback — Self-Learning & Continuous Optimization Feedback Loop
=====================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.models.ai import TakeProfitStage
from alpha_engine.models.decisions import PatternFeatureVector, RevivalPatternFeatureVector
from alpha_engine.models.enums import (
    ChainIdentifier,
    SignalSource,
    StrategyHorizon,
    TradeExitReason,
    WhitelistStatus,
)
from alpha_engine.profiler.profiler import SmartMoneyProfiler

logger = logging.getLogger(__name__)


@dataclass
class TradeReflection:
    """
    Post-trade reflection record calculating actual vs. expected slippage,
    time-to-fill latency, and net realized PnL/ROI.
    """

    trade_id: str
    token_address: str
    chain: ChainIdentifier
    signal_source: SignalSource
    wallet_address: Optional[str] = None
    entry_price: Decimal = Decimal(0)
    exit_price: Decimal = Decimal(0)
    expected_slippage_bps: int = 500  # Default 500 bps (5%)
    actual_slippage_bps: int = 0
    time_to_fill_ms: float = 0.0
    realized_pnl_native: Decimal = Decimal(0)
    realized_pnl_usd: Decimal = Decimal(0)
    roi_pct: float = 0.0
    is_win: bool = False
    strategy_horizon: StrategyHorizon = StrategyHorizon.SHORT_TERM_SCALP
    custom_metadata: Optional[dict[str, Any]] = None
    timestamp_ns: int = field(default_factory=time.time_ns)

    @property
    def realized_slippage_bps(self) -> int:
        return self.actual_slippage_bps

    @property
    def net_pnl_usd(self) -> Decimal:
        return self.realized_pnl_usd

    @classmethod
    def from_trade(
        cls,
        trade_id: str,
        token_address: str,
        chain: ChainIdentifier,
        signal_source: SignalSource,
        entry_price: Decimal,
        exit_price: Decimal,
        realized_pnl_usd: Decimal,
        realized_pnl_native: Decimal,
        time_to_fill_ms: float = 50.0,
        expected_slippage_bps: int = 500,
        wallet_address: Optional[str] = None,
        strategy_horizon: StrategyHorizon = StrategyHorizon.SHORT_TERM_SCALP,
        custom_metadata: Optional[dict[str, Any]] = None,
    ) -> TradeReflection:
        if entry_price > Decimal(0):
            roi = float((exit_price - entry_price) / entry_price * 100)
            slippage_calc = abs(int((exit_price - entry_price) / entry_price * 10000))
        else:
            roi = 0.0
            slippage_calc = 0

        is_win = realized_pnl_usd > Decimal(0)

        return cls(
            trade_id=trade_id,
            token_address=token_address,
            chain=chain,
            signal_source=signal_source,
            wallet_address=wallet_address,
            entry_price=entry_price,
            exit_price=exit_price,
            expected_slippage_bps=expected_slippage_bps,
            actual_slippage_bps=slippage_calc,
            time_to_fill_ms=time_to_fill_ms,
            realized_pnl_native=realized_pnl_native,
            realized_pnl_usd=realized_pnl_usd,
            roi_pct=roi,
            is_win=is_win,
            strategy_horizon=strategy_horizon,
            custom_metadata=custom_metadata,
            timestamp_ns=time.time_ns(),
        )


class AdaptiveFeedbackEngine:
    """
    Self-learning feedback loop optimizing weights and whitelists:
    1. Post-Trade Reflection Ledger tracking slippage, latency, PnL.
    2. Social Signal Weight Decay: If 'X Sentiment' win-rate < 40% over the last 10 trades,
       decay its weighting in the decision matrix.
    3. Whitelist Pruning: If a whitelisted smart wallet suffers 3 consecutive losing trades
       or its 7-day win-rate drops below 50%, demote and remove it from whitelist_db.
    """

    def __init__(
        self,
        profiler: Optional[SmartMoneyProfiler] = None,
        initial_social_weight: float = 1.0,
        min_social_weight: float = 0.20,
        decay_factor: float = 0.80,
        lookback_trades: int = 10,
    ) -> None:
        self._profiler = profiler
        self.social_signal_weight = initial_social_weight
        self._min_social_weight = min_social_weight
        self._decay_factor = decay_factor
        self._lookback_trades = lookback_trades
        self._lock = asyncio.Lock()

        self._reflections: list[TradeReflection] = []
        self._source_history: dict[SignalSource, list[TradeReflection]] = defaultdict(list)
        self._wallet_history: dict[str, list[TradeReflection]] = defaultdict(list)

        # Autonomous learning & parameter adaptation modules
        self.pattern_store = PatternMemoryStore()
        self.parameter_tuner = DynamicParameterTuner()
        self.missed_opportunity_analyzer = MissedOpportunityAnalyzer(self.pattern_store)

    async def record_closed_trade(self, reflection: TradeReflection) -> None:
        """
        Record a closed trade reflection and trigger weight adaptations and wallet pruning.
        """
        async with self._lock:
            self._reflections.append(reflection)
            self._source_history[reflection.signal_source].append(reflection)

        logger.info(
            "Recorded TradeReflection [%s] %s | PnL=$%.2f (%.1f%%) | Source=%s | ActualSlippage=%dbps | FillLatency=%.1fms",
            reflection.trade_id[:8],
            reflection.token_address[:10],
            float(reflection.realized_pnl_usd),
            reflection.roi_pct,
            reflection.signal_source.value,
            reflection.actual_slippage_bps,
            reflection.time_to_fill_ms,
        )

        # 1. X Sentiment Weight Adaptation
        if reflection.signal_source == SignalSource.X_SENTIMENT:
            self._adapt_social_weight()

        # 2. Whitelisted Wallet Performance Monitoring & Pruning
        if reflection.wallet_address:
            self._wallet_history[reflection.wallet_address].append(reflection)
            await self._evaluate_wallet_health(reflection.wallet_address)

    def _adapt_social_weight(self) -> None:
        x_trades = self._source_history[SignalSource.X_SENTIMENT]
        if len(x_trades) < self._lookback_trades:
            return

        recent_x = x_trades[-self._lookback_trades:]
        wins = sum(1 for t in recent_x if t.is_win)
        win_rate = wins / len(recent_x)

        if win_rate < 0.40:
            old_weight = self.social_signal_weight
            self.social_signal_weight = max(
                self._min_social_weight,
                self.social_signal_weight * self._decay_factor,
            )
            logger.warning(
                "X Sentiment win-rate is %.1f%% (<40%% over last %d trades). "
                "Decayed social signal weight: %.3f -> %.3f",
                win_rate * 100,
                len(recent_x),
                old_weight,
                self.social_signal_weight,
            )
        elif win_rate >= 0.60 and self.social_signal_weight < 1.0:
            old_weight = self.social_signal_weight
            self.social_signal_weight = min(1.0, self.social_signal_weight / self._decay_factor)
            logger.info(
                "X Sentiment recovered win-rate to %.1f%%. Restored social weight: %.3f -> %.3f",
                win_rate * 100,
                old_weight,
                self.social_signal_weight,
            )

    async def _evaluate_wallet_health(self, wallet_address: str) -> None:
        trades = self._wallet_history[wallet_address]
        if not trades or self._profiler is None:
            return

        # Check 1: 3 consecutive losing trades
        if len(trades) >= 3 and all(not t.is_win for t in trades[-3:]):
            logger.warning(
                "Whitelisted wallet %s suffered 3 consecutive losing trades. Demoting to SUSPENDED.",
                wallet_address[:10],
            )
            await self._profiler.demote_or_ban_wallet(
                wallet_address=wallet_address,
                new_status=WhitelistStatus.SUSPENDED,
                reason="3 consecutive losing trades",
            )
            return

        # Check 2: 7-day win-rate drops below 50%
        seven_days_ns = 7 * 86400 * 1_000_000_000
        now_ns = time.time_ns()
        recent_7d = [t for t in trades if (now_ns - t.timestamp_ns) <= seven_days_ns]

        if len(recent_7d) >= 5:  # Require at least 5 trades to evaluate statistical rate
            win_rate_7d = sum(1 for t in recent_7d if t.is_win) / len(recent_7d)
            if win_rate_7d < 0.50:
                logger.warning(
                    "Whitelisted wallet %s 7-day win-rate dropped to %.1f%% (<50%%). Demoting to SUSPENDED.",
                    wallet_address[:10],
                    win_rate_7d * 100,
                )
                await self._profiler.demote_or_ban_wallet(
                    wallet_address=wallet_address,
                    new_status=WhitelistStatus.SUSPENDED,
                    reason=f"7-day win rate dropped to {win_rate_7d * 100:.1f}%",
                )

    def optimize_stop_loss_bayesian(
        self,
        realized_volatility: float,
        horizon: StrategyHorizon = StrategyHorizon.SHORT_TERM_SCALP,
        as_percentage: bool = True,
    ) -> float:
        """
        Bayesian parameter optimization for volatility-adjusted stop-loss:
        - SHORT_TERM_SCALP: 4% to 7% (volatility-adjusted).
        - LONG_TERM_SWING: 15% to 25% (volatility-adjusted).

        Uses realized volatility to compute the posterior stop-loss within the target band.
        """
        vol = abs(realized_volatility)
        if vol > 1.0:
            vol = vol / 100.0

        if horizon == StrategyHorizon.SHORT_TERM_SCALP:
            min_sl, max_sl = (4.0, 7.0) if as_percentage else (0.04, 0.07)
            norm_factor = min(1.0, max(0.0, vol / 0.06))
        else:
            min_sl, max_sl = (15.0, 25.0) if as_percentage else (0.15, 0.25)
            norm_factor = min(1.0, max(0.0, vol / 0.25))

        optimal = min_sl + norm_factor * (max_sl - min_sl)
        return round(optimal, 2 if as_percentage else 4)

    async def evaluate_14d_whitelist_deprecation(self, current_time_ns: Optional[int] = None) -> list[str]:
        """
        14-day rolling whitelist auto-deprecation:
        Drop / demote smart-money addresses whose 14-day rolling win-rate drops below 45%
        or whose 14-day net PnL is negative.
        """
        deprecated_wallets: list[str] = []
        now_ns = current_time_ns if current_time_ns is not None else time.time_ns()
        fourteen_days_ns = 14 * 86400 * 1_000_000_000

        for wallet_address, trades in list(self._wallet_history.items()):
            recent_14d = [t for t in trades if (now_ns - t.timestamp_ns) <= fourteen_days_ns]
            if not recent_14d:
                continue

            if len(recent_14d) >= 3:
                wins = sum(1 for t in recent_14d if t.is_win)
                win_rate = wins / len(recent_14d)
                net_pnl = sum(t.realized_pnl_usd for t in recent_14d)

                if win_rate < 0.45 or net_pnl < Decimal(0):
                    logger.warning(
                        "Whitelisted wallet %s auto-deprecated (14-day rolling win_rate=%.1f%% < 45%% or net PnL=$%.2f < 0).",
                        wallet_address[:10],
                        win_rate * 100,
                        float(net_pnl),
                    )
                    if self._profiler is not None:
                        await self._profiler.demote_or_ban_wallet(
                            wallet_address=wallet_address,
                            new_status=WhitelistStatus.SUSPENDED,
                            reason=f"14-day rolling win-rate {win_rate*100:.1f}% or net PnL ${float(net_pnl):.2f}",
                        )
                    deprecated_wallets.append(wallet_address)

        return deprecated_wallets

    def tune_sentiment_sensitivity_weights(
        self,
        social_volume_history: list[float] | tuple[float, ...],
        price_retention_history: list[float] | tuple[float, ...],
    ) -> float:
        """
        AI sentiment sensitivity weights:
        Correlates social volume spikes with 2-hour price retention.
        Reduces sentiment weight if correlation < 0.35.
        """
        if len(social_volume_history) < 3 or len(price_retention_history) < 3:
            return self.social_signal_weight

        n = min(len(social_volume_history), len(price_retention_history))
        x = [float(v) for v in social_volume_history[:n]]
        y = [float(v) for v in price_retention_history[:n]]

        mean_x = sum(x) / n
        mean_y = sum(y) / n

        cov = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        var_x = sum((x[i] - mean_x) ** 2 for i in range(n))
        var_y = sum((y[i] - mean_y) ** 2 for i in range(n))

        if var_x <= 0 or var_y <= 0:
            return self.social_signal_weight

        correlation = cov / ((var_x * var_y) ** 0.5)

        if correlation < 0.35:
            old_weight = self.social_signal_weight
            self.social_signal_weight = max(
                self._min_social_weight,
                round(self.social_signal_weight * self._decay_factor, 3),
            )
            logger.warning(
                "Social sentiment vs. 2h price retention correlation is low (r=%.3f < 0.35). "
                "Reduced sentiment weight: %.3f -> %.3f",
                correlation,
                old_weight,
                self.social_signal_weight,
            )
        elif correlation >= 0.60 and self.social_signal_weight < 1.0:
            old_weight = self.social_signal_weight
            self.social_signal_weight = min(1.0, round(self.social_signal_weight / self._decay_factor, 3))
            logger.info(
                "Strong social correlation (r=%.3f >= 0.60). Restored sentiment weight: %.3f -> %.3f",
                correlation,
                old_weight,
                self.social_signal_weight,
            )

        return self.social_signal_weight

    async def check_contract_mutation_and_emergency_exit(
        self,
        token_address: str,
        chain: ChainIdentifier,
        current_security_report: Any,
        tokens_to_sell: Optional[Decimal] = None,
        pool: Optional[Any] = None,
        executor: Optional[Any] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        Dynamic post-entry simulation:
        If contract variables mutate after block confirmation (e.g. tax raised > 3%,
        honeypot flag activated, freeze/mint authority re-enabled), triggers immediate
        private bundle emergency exit with TradeExitReason.EMERGENCY_HONEYPOT_MUTATION.
        """
        mutated = False
        reason_parts = []

        buy_tax = getattr(current_security_report, "buy_tax_bps", 0)
        sell_tax = getattr(current_security_report, "sell_tax_bps", 0)
        is_honeypot = getattr(current_security_report, "is_honeypot", False)
        freeze_disabled = getattr(current_security_report, "freeze_authority_disabled", True)
        mint_disabled = getattr(current_security_report, "mint_authority_disabled", True)

        if buy_tax > 300:
            mutated = True
            reason_parts.append(f"buy_tax_bps={buy_tax} > 300")
        if sell_tax > 300:
            mutated = True
            reason_parts.append(f"sell_tax_bps={sell_tax} > 300")
        if is_honeypot:
            mutated = True
            reason_parts.append("honeypot_active=True")
        if not freeze_disabled:
            mutated = True
            reason_parts.append("freeze_authority_enabled")
        if not mint_disabled:
            mutated = True
            reason_parts.append("mint_authority_enabled")

        if not mutated:
            return False, None

        mutation_reason = f"POST-ENTRY MUTATION DETECTED: {', '.join(reason_parts)}"
        logger.critical(
            "EMERGENCY HONEYPOT MUTATION EXIT TRIGGERED for %s on %s: %s",
            token_address[:10],
            chain.value,
            mutation_reason,
        )

        if executor is not None and pool is not None and tokens_to_sell is not None and tokens_to_sell > 0:
            await executor.execute_exit(
                chain=chain,
                token_address=token_address,
                pool=pool,
                tokens_to_sell=tokens_to_sell,
                reason=TradeExitReason.EMERGENCY_HONEYPOT_MUTATION,
            )

        return True, mutation_reason

    def get_social_weight(self) -> float:
        """Current weight for social signals [min_social_weight, 1.0]."""
        return self.social_signal_weight

    def get_reflections(self, limit: int = 50) -> list[TradeReflection]:
        """Return the latest trade reflections."""
        return self._reflections[-limit:]


# =============================================================================
# Vectorized Pattern Store & Feature Memory
# =============================================================================

class PatternMemoryStore:
    """
    Vectorized Pattern Store & Feature Memory:
    Maintains normalized market state vectors of successful breakout tokens
    (consolidation duration, dip depth, smart wallet inflows, liquidity-to-MC ratio, surge multiplier, etc.).
    Computes cosine similarity PatternMatchScore [0.0, 1.0] for candidate tokens.
    """

    def __init__(self, ledger: Optional[Any] = None) -> None:
        self._ledger = ledger
        self._patterns: list[PatternFeatureVector] = []
        self._revival_patterns: list[RevivalPatternFeatureVector] = []
        self._load_baseline_archetypes()

    def _load_baseline_archetypes(self) -> None:
        """Seed initial archetypes representing verified institutional wave-2 accumulation breakouts."""
        archetype_1 = PatternFeatureVector(
            token_address="Archetype_CTO_LongConsolidation",
            consolidation_duration_s=2700.0,  # 45 mins
            dip_depth_pct=55.0,              # 55% dip from peak
            volume_surge_multiplier=3.4,     # 3.4x volume surge
            net_buy_delta=0.74,              # 74% net buy volume
            top10_concentration=0.17,        # 17% top 10
            liquidity_to_mc_ratio=0.22,      # 22% liq / MC
            smart_wallet_inflows=18.5,       # 18.5 SOL smart inflow
            peak_gain_multiplier=5.2,
        )
        archetype_2 = PatternFeatureVector(
            token_address="Archetype_V_Reversal_HighVolume",
            consolidation_duration_s=1200.0,  # 20 mins
            dip_depth_pct=42.0,              # 42% dip
            volume_surge_multiplier=4.8,     # 4.8x volume surge
            net_buy_delta=0.82,              # 82% net buy volume
            top10_concentration=0.14,        # 14% top 10
            liquidity_to_mc_ratio=0.28,      # 28% liq / MC
            smart_wallet_inflows=35.0,       # 35.0 SOL smart inflow
            peak_gain_multiplier=8.5,
        )
        self._patterns.extend([archetype_1, archetype_2])

    def add_pattern(self, vector: PatternFeatureVector) -> None:
        """Add a confirmed breakout pattern vector to memory and ledger."""
        self._patterns.append(vector)
        if self._ledger is not None and hasattr(self._ledger, "record_pattern_vector"):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._ledger.record_pattern_vector(vector))
            except RuntimeError:
                pass
        logger.info(
            "PatternMemoryStore: Recorded new breakout pattern for %s (gain=%.2fx, surge=%.1fx, dip=%.1f%%)",
            vector.token_address[:10],
            vector.peak_gain_multiplier,
            vector.volume_surge_multiplier,
            vector.dip_depth_pct,
        )

    def add_revival_pattern(self, vector: RevivalPatternFeatureVector) -> None:
        """Add a closed revival swing breakout pattern vector to memory and ledger."""
        self._revival_patterns.append(vector)
        if self._ledger is not None and hasattr(self._ledger, "record_revival_pattern"):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._ledger.record_revival_pattern(vector))
            except RuntimeError:
                pass
        logger.info(
            "PatternMemoryStore: Recorded new revival swing pattern for %s (peak_roi=%.1f%%, realized_pnl=%.1f%%, surge=%.1fx, age=%.1fh)",
            vector.token_address[:10],
            vector.peak_roi_pct,
            vector.realized_pnl_pct,
            vector.volume_surge_multiplier,
            vector.token_age_hours,
        )

    def calculate_pattern_match(self, candidate_vector: PatternFeatureVector) -> float:
        """
        Compute PatternMatchScore [0.0, 1.0] by finding the highest cosine similarity
        against stored breakout vectors.
        """
        if not self._patterns:
            return 0.50
        max_similarity = 0.0
        for pattern in self._patterns:
            sim = candidate_vector.cosine_similarity(pattern)
            if sim > max_similarity:
                max_similarity = sim
        return round(max_similarity, 3)

    def get_patterns(self, limit: int = 50) -> list[PatternFeatureVector]:
        return self._patterns[-limit:]

    def get_revival_patterns(self, limit: int = 50) -> list[RevivalPatternFeatureVector]:
        return self._revival_patterns[-limit:]


# =============================================================================
# Missed Opportunity Post-Mortem Analyzer
# =============================================================================

@dataclass
class MissedOpportunity:
    """Post-mortem analysis record of a token that surged despite being skipped or vetoed."""

    token_address: str
    symbol: str
    peak_multiplier: float
    market_source: str
    first_seen_timestamp: float
    veto_or_skip_reason: str
    state_vector: Optional[PatternFeatureVector] = None
    bottleneck_rule: str = ""
    suggested_threshold_adjustment: str = ""
    analyzed_at: float = field(default_factory=time.time)


class MissedOpportunityAnalyzer:
    """
    Missed Opportunity Post-Mortem Loop:
    Periodically cross-references top market gainers against tokens that entered
    staging or preflight evaluation. Identifies false negatives (missed breakouts),
    extracts their market state vectors, and adjusts pattern memory.
    """

    def __init__(self, pattern_store: PatternMemoryStore) -> None:
        self._pattern_store = pattern_store
        self._missed_opportunities: deque[MissedOpportunity] = deque(maxlen=100)

    async def cross_reference_market_gainers(
        self,
        market_gainers: list[dict[str, Any]],
        audited_outcomes: dict[str, Any],
        staged_tokens: dict[str, Any],
    ) -> list[MissedOpportunity]:
        """
        Cross-reference top market gainers (e.g. from DexScreener/Birdeye)
        against tokens evaluated in staging or supervisor audits.
        """
        new_missed: list[MissedOpportunity] = []

        for gainer in market_gainers:
            token_addr = gainer.get("token_address") or gainer.get("baseToken", {}).get("address", "")
            if not token_addr:
                continue

            multiplier = float(gainer.get("multiplier", 1.0) or gainer.get("priceChange", {}).get("h24", 0.0) / 100.0 + 1.0)
            symbol = gainer.get("symbol", token_addr[:6])

            # Check if this token was audited or staged and subsequently skipped/vetoed
            was_skipped = False
            skip_reason = ""

            if token_addr in audited_outcomes:
                outcome = audited_outcomes[token_addr]
                dec = getattr(outcome, "decision", "")
                if dec in ("PASS", "SKIP"):
                    was_skipped = True
                    skip_reason = f"Supervisor audit veto: {', '.join(getattr(outcome, 'flags', []))}"
            elif token_addr in staged_tokens:
                staged = staged_tokens[token_addr]
                if getattr(staged, "dropped", False):
                    was_skipped = True
                    skip_reason = f"Staging dropped: {getattr(staged, 'drop_reason', '')}"

            if was_skipped and multiplier >= 2.0:  # Gained >= 2x after being passed/dropped
                # Post-mortem root cause identification
                bottleneck = "Early Dump Drop" if "DUMP" in skip_reason else ("Volume Surge Threshold" if "Surge" in skip_reason else "Security Gate")
                adjustment = "Relax surge threshold k" if "Surge" in skip_reason else "Extend Wave-2 staging TTL"

                # Extract market state vector at breakout
                vol_surge = float(gainer.get("volume_surge_multiplier", 2.2))
                net_buy = float(gainer.get("net_buy_delta", 0.68))
                dip_depth = float(gainer.get("dip_depth_pct", 48.0))
                cons_dur = float(gainer.get("consolidation_duration_s", 1800.0))

                vector = PatternFeatureVector(
                    token_address=token_addr,
                    consolidation_duration_s=cons_dur,
                    dip_depth_pct=dip_depth,
                    volume_surge_multiplier=vol_surge,
                    net_buy_delta=net_buy,
                    top10_concentration=float(gainer.get("top10_concentration", 0.18)),
                    liquidity_to_mc_ratio=float(gainer.get("liquidity_to_mc_ratio", 0.20)),
                    smart_wallet_inflows=float(gainer.get("smart_wallet_inflows", 10.0)),
                    peak_gain_multiplier=multiplier,
                )

                # Persist learned vector into pattern store
                self._pattern_store.add_pattern(vector)

                opp = MissedOpportunity(
                    token_address=token_addr,
                    symbol=symbol,
                    peak_multiplier=multiplier,
                    market_source=gainer.get("source", "DexScreener"),
                    first_seen_timestamp=time.time(),
                    veto_or_skip_reason=skip_reason,
                    state_vector=vector,
                    bottleneck_rule=bottleneck,
                    suggested_threshold_adjustment=adjustment,
                )
                self._missed_opportunities.append(opp)
                new_missed.append(opp)

                logger.warning(
                    "MissedOpportunity Post-Mortem: Token %s gained %.2fx after being skipped! "
                    "Bottleneck: %s | Vector saved to PatternMemoryStore.",
                    token_addr[:10],
                    multiplier,
                    bottleneck,
                )

        return new_missed

    def get_missed_opportunities(self, limit: int = 20) -> list[MissedOpportunity]:
        return list(self._missed_opportunities)[-limit:]


# =============================================================================
# Dynamic Hyperparameter Tuning Loop with EMA Smoothing & Safety Bounds
# =============================================================================

@dataclass
class DynamicHyperparameters:
    """
    Dynamically adapted system hyperparameters tuned based on trailing performance.
    """

    confidence_multiplier: float = 1.0           # Bounds: [0.70, 1.40]
    max_slippage_bps: int = 150                  # Bounds: [50, 350]
    priority_fee_multiplier: float = 1.2         # Bounds: [1.0, 2.5]
    hard_stop_loss_pct: float = -15.0            # Bounds: [-25.0, -8.0]
    trailing_stop_activation_pct: float = 40.0   # Bounds: [20.0, 75.0]
    wave2_surge_k: float = 2.5                   # Bounds: [1.8, 3.5]
    wave2_net_buy_delta_pct: float = 65.0        # Bounds: [55.0, 75.0]
    tp1_trigger_multiplier: float = 2.0          # Bounds: [1.4, 2.5]
    tp1_sell_pct: float = 40.0                   # Bounds: [20.0, 60.0]
    tp2_trigger_multiplier: float = 3.5          # Bounds: [2.5, 6.0]
    tp2_sell_pct: float = 30.0                   # Bounds: [15.0, 50.0]

    def get_take_profit_ladder(self) -> list[TakeProfitStage]:
        return [
            TakeProfitStage(trigger_multiplier=self.tp1_trigger_multiplier, sell_pct=self.tp1_sell_pct),
            TakeProfitStage(trigger_multiplier=self.tp2_trigger_multiplier, sell_pct=self.tp2_sell_pct),
        ]


class DynamicParameterTuner:
    """
    Dynamic Hyperparameter Tuning Engine:
    Dynamically tunes threshold weights (confidence multipliers, slippage allowances,
    exit multipliers, take-profit ladders, surge thresholds) based on trailing 24h win-rate
    and realized PnL.
    Applies decaying exponential moving averages (EMA alpha=0.15) to avoid overfitting
    and strictly enforces safety boundaries.
    """

    def __init__(self, smoothing_alpha: float = 0.15) -> None:
        self._alpha = smoothing_alpha
        self._params = DynamicHyperparameters()
        self._smoothed_win_rate: float = 65.0
        self._smoothed_pnl_usd: float = 0.0
        self._tuning_history: deque[dict[str, Any]] = deque(maxlen=50)

    @property
    def current_params(self) -> DynamicHyperparameters:
        return self._params

    @current_params.setter
    def current_params(self, params: DynamicHyperparameters) -> None:
        self._params = params

    def update_from_performance(
        self,
        win_rate_24h: float,
        realized_pnl_usd: Decimal | float,
        total_trades_24h: int = 10,
    ) -> DynamicHyperparameters:
        """
        Update hyperparameters using Asymmetric EMA smoothing across trailing win rate and PnL.
        Fast to cut risk (alpha=0.35), Slow to expand risk (alpha=0.10).
        """
        pnl_float = float(realized_pnl_usd)

        # Asymmetric EMA: Fast to cut risk (alpha=0.35), Slow to expand risk (alpha=0.10)
        is_losing_regime = win_rate_24h < self._smoothed_win_rate or pnl_float < self._smoothed_pnl_usd
        effective_alpha = 0.35 if is_losing_regime else 0.10

        # 1. Update decaying moving averages using dynamic alpha
        self._smoothed_win_rate = effective_alpha * win_rate_24h + (1.0 - effective_alpha) * self._smoothed_win_rate
        self._smoothed_pnl_usd = effective_alpha * pnl_float + (1.0 - effective_alpha) * self._smoothed_pnl_usd

        # 2. Compute dynamic adjustments based on regime
        if total_trades_24h >= 3:
            if self._smoothed_win_rate >= 70.0 and self._smoothed_pnl_usd > 0:
                # Strong winning regime: Expand risk appetite, loosen surge k to capture wave-2 earlier
                target_conf, target_slippage, target_surge_k, target_net_buy, target_tp1 = 1.15, 200, 2.2, 60.0, 2.2
            elif self._smoothed_win_rate < 45.0 or self._smoothed_pnl_usd < -50.0:
                # Losing regime: Defensive contraction, tighten stop loss, require higher surge
                target_conf, target_slippage, target_surge_k, target_net_buy, target_tp1 = 0.85, 100, 2.9, 70.0, 1.6
            else:
                # Balanced neutral regime
                target_conf, target_slippage, target_surge_k, target_net_buy, target_tp1 = 1.0, 150, 2.5, 65.0, 2.0

            # 3. Apply EMA smoothing to target parameter values
            p = self._params
            p.confidence_multiplier = round(effective_alpha * target_conf + (1.0 - effective_alpha) * p.confidence_multiplier, 3)
            p.max_slippage_bps = int(effective_alpha * target_slippage + (1.0 - effective_alpha) * p.max_slippage_bps)
            p.wave2_surge_k = round(effective_alpha * target_surge_k + (1.0 - effective_alpha) * p.wave2_surge_k, 2)
            p.wave2_net_buy_delta_pct = round(effective_alpha * target_net_buy + (1.0 - effective_alpha) * p.wave2_net_buy_delta_pct, 1)
            p.tp1_trigger_multiplier = round(effective_alpha * target_tp1 + (1.0 - effective_alpha) * p.tp1_trigger_multiplier, 2)

            # 4. Strictly enforce hard safety boundaries
            p.confidence_multiplier = max(0.70, min(1.40, p.confidence_multiplier))
            p.max_slippage_bps = max(50, min(350, p.max_slippage_bps))
            p.wave2_surge_k = max(1.8, min(3.5, p.wave2_surge_k))
            p.wave2_net_buy_delta_pct = max(55.0, min(75.0, p.wave2_net_buy_delta_pct))
            p.tp1_trigger_multiplier = max(1.4, min(2.5, p.tp1_trigger_multiplier))

        self._tuning_history.append({
            "timestamp": time.time(),
            "smoothed_win_rate": round(self._smoothed_win_rate, 1),
            "smoothed_pnl_usd": round(self._smoothed_pnl_usd, 2),
            "confidence_multiplier": self._params.confidence_multiplier,
            "max_slippage_bps": self._params.max_slippage_bps,
            "wave2_surge_k": self._params.wave2_surge_k,
            "wave2_net_buy_delta_pct": self._params.wave2_net_buy_delta_pct,
        })

        return self._params

    def get_tuning_history(self) -> list[dict[str, Any]]:
        return list(self._tuning_history)

    def tune_revival_parameters(self, patterns: list[RevivalPatternFeatureVector]) -> dict[str, float]:
        """
        Analyze closed revival swing patterns.
        Prioritizes token profiles whose dormant period and volume multiplier yield > 100% return
        with minimal peak drawdown.
        """
        high_gain_patterns = [p for p in patterns if p.realized_pnl_pct >= 100.0 or p.peak_roi_pct >= 100.0]
        if not high_gain_patterns:
            return {"recommended_surge_k": 3.0, "min_consolidation_hours": 2.0}
        avg_surge = sum(p.volume_surge_multiplier for p in high_gain_patterns) / len(high_gain_patterns)
        avg_cons = sum(p.consolidation_length_hours for p in high_gain_patterns) / len(high_gain_patterns)
        return {
            "recommended_surge_k": round(max(2.0, min(5.0, avg_surge)), 2),
            "min_consolidation_hours": round(max(1.0, min(24.0, avg_cons)), 2),
        }

