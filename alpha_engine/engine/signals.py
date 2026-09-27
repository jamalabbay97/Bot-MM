"""
alpha_engine.engine.signals — Alpha Signal Generation & Scoring
===============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.math.sizing import classify_alpha_score
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
)
from alpha_engine.models.events import SignalEvent, SwapEvent
from alpha_engine.models.state import PoolState, SecurityReport

logger = logging.getLogger(__name__)


class SignalGenerator:
    """
    Converts a screened SwapEvent + SecurityReport into a SignalEvent.
    """

    def __init__(
        self,
        hold_seconds: float = 300.0,
    ) -> None:
        self._hold_s = hold_seconds

    def validate_signal_strength(
        self,
        signal_event: SignalEvent,
        min_strength: Optional[SignalStrength] = None,
    ) -> bool:
        """
        Validate whether the signal meets the minimum strength threshold.
        """
        if signal_event.suggested_side == OrderSide.SELL:
            return True
        if min_strength is None:
            return True

        rank = {
            SignalStrength.WEAK: 1,
            SignalStrength.MODERATE: 2,
            SignalStrength.STRONG: 3,
        }
        return rank.get(signal_event.strength, 0) >= rank.get(min_strength, 0)

    def score_swap(
        self,
        swap: SwapEvent,
        pool: PoolState,
        report: SecurityReport,
    ) -> float:
        """Compute a normalised alpha score [0.0, 1.0] for a swap event."""
        volume_ratio = float(
            min(Decimal("1"), swap.amount_in / pool.native_reserve)
        )
        spot = pool.spot_price_native_per_token
        exec_p = swap.amount_out / swap.amount_in if swap.amount_in > 0 else spot
        impact_raw = abs(1.0 - float(exec_p / spot)) if float(spot) > 0 else 0.0
        inverse_impact = max(0.0, 1.0 - impact_raw)

        tier_multiplier = 1.0 if report.tier == SecurityTier.CLEAN else 0.5

        raw_score = (0.6 * volume_ratio + 0.4 * inverse_impact) * tier_multiplier
        return min(1.0, max(0.0, raw_score))

    def generate_buy_signal(
        self,
        swap: SwapEvent,
        pool: PoolState,
        report: SecurityReport,
        source: SignalSource = SignalSource.DEX_SWAP,
        min_strength: Optional[SignalStrength] = None,
    ) -> SignalEvent | None:
        """Generate a BUY SignalEvent for a qualifying swap."""
        alpha = self.score_swap(swap, pool, report)
        if alpha < 0.10:
            logger.debug(
                "Alpha score %.4f too low for %s — skipping signal.",
                alpha,
                swap.tx_hash[:12],
            )
            return None

        strength_str = classify_alpha_score(alpha)
        strength = SignalStrength(strength_str)

        signal = SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=swap.chain,
            pool_address=swap.pool_address,
            token_address=swap.token_out,
            suggested_side=OrderSide.BUY,
            trigger_swap=swap,
            pool_state=pool,
            security_report=report,
            strength=strength,
            alpha_score=alpha,
            source=source,
        )

        if not self.validate_signal_strength(signal, min_strength=min_strength):
            logger.debug("Signal rejected: strength %s below threshold %s", strength, min_strength)
            return None

        return signal

    def generate_social_signal(
        self,
        raw_signal: Any,
        pool: PoolState,
        report: SecurityReport,
        social_weight: float = 1.0,
    ) -> SignalEvent | None:
        """Generate a BUY SignalEvent from social sentiment (X or Telegram) with dynamic weight adaptation."""
        tier_multiplier = 1.0 if report.tier == SecurityTier.CLEAN else 0.5
        alpha = min(1.0, max(0.0, 0.85 * social_weight * tier_multiplier))

        if alpha < 0.10:
            logger.debug(
                "Social alpha score %.4f too low for %s — skipping signal.",
                alpha,
                getattr(raw_signal, "token_address", "")[:10],
            )
            return None

        strength_str = classify_alpha_score(alpha)
        strength = SignalStrength(strength_str)

        source = getattr(raw_signal, "source", SignalSource.X_SENTIMENT)

        return SignalEvent(
            timestamp_ns=getattr(raw_signal, "timestamp_ns", time.time_ns()),
            chain=raw_signal.chain,
            pool_address=pool.pool_address,
            token_address=raw_signal.token_address,
            suggested_side=OrderSide.BUY,
            pool_state=pool,
            security_report=report,
            strength=strength,
            alpha_score=alpha,
            source=source,
        )

    def generate_sell_signal(
        self,
        token_address: str,
        chain: ChainIdentifier,
        pool: PoolState,
        pool_address: str,
        report: SecurityReport,
        trigger_swap: SwapEvent,
    ) -> SignalEvent:
        """Generate a SELL SignalEvent for position exit."""
        return SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=chain,
            pool_address=pool_address,
            token_address=token_address,
            suggested_side=OrderSide.SELL,
            trigger_swap=trigger_swap,
            pool_state=pool,
            security_report=report,
            strength=SignalStrength.STRONG,
            alpha_score=1.0,
        )
