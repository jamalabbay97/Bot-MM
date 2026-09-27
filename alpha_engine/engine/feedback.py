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
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.models.enums import ChainIdentifier, SignalSource, WhitelistStatus
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
    timestamp_ns: int = field(default_factory=time.time_ns)

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

        self._reflections: list[TradeReflection] = []
        self._source_history: dict[SignalSource, list[TradeReflection]] = defaultdict(list)
        self._wallet_history: dict[str, list[TradeReflection]] = defaultdict(list)

    async def record_closed_trade(self, reflection: TradeReflection) -> None:
        """
        Record a closed trade reflection and trigger weight adaptations and wallet pruning.
        """
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

    def get_social_weight(self) -> float:
        """Current weight for social signals [min_social_weight, 1.0]."""
        return self.social_signal_weight

    def get_reflections(self, limit: int = 50) -> list[TradeReflection]:
        """Return the latest trade reflections."""
        return self._reflections[-limit:]
