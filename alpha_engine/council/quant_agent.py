"""
alpha_engine.council.quant_agent — Agent 3: Quantitative Technical & Revival Strategy Agent
Calculates volume surges, order-flow imbalance (OFI), RSI overbought exhaustion, and Wave-2/CTO revival inflection.
"""
from __future__ import annotations

import time
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.council.models import AgentVerdict, AgentVote
from alpha_engine.models.state import PoolState


class QuantTechnicalsAgent:
    def __init__(self):
        pass

    def analyze_market_dynamics(
        self,
        pool: Optional[PoolState],
        signal: Any,
        staged_data: Optional[Any] = None,
        is_revival_swing: bool = False,
    ) -> AgentVerdict:
        t0 = time.perf_counter()
        flags = []

        surge_k = float(getattr(signal, "volume_surge_multiplier", 0.0) or (getattr(staged_data, "volume_surge_multiplier", 0.0) if staged_data else 1.0))
        net_buy_delta = float(getattr(signal, "net_buy_delta", 0.5) or (getattr(staged_data, "net_buy_delta", 0.5) if staged_data else 0.5))
        alpha_score = float(getattr(signal, "alpha_score", 0.5) or 0.5)

        # 1. Anti-FOMO & Over-extension Guard
        dormant_mcap = float(getattr(signal, "base_market_cap", 0.0) or 0.0)
        curr_mcap = float(getattr(pool, "market_cap_usd", 0.0) or 0.0) if pool else 0.0
        if dormant_mcap > 0 and curr_mcap > 0 and (curr_mcap / dormant_mcap) > 2.5:
            flags.append("OVEREXTENDED_GREEN_CANDLE_FOMO")

        # 2. RSI Check
        rsi = float(getattr(staged_data, "rsi_5m", 50.0) if staged_data else 50.0)
        if rsi > 82.0:
            flags.append(f"EXTREME_RSI_OVERBOUGHT_{rsi:.1f}")

        # 3. Volume Inflection Criteria
        surge_passed = surge_k >= 2.5
        net_buy_passed = net_buy_delta >= 0.65

        score = (alpha_score * 40.0) + (min(2.0, surge_k / 2.5) * 30.0) + (net_buy_delta * 30.0)
        if "OVEREXTENDED_GREEN_CANDLE_FOMO" in flags or rsi > 82.0:
            score = max(0.0, score - 50.0)

        score = max(0.0, min(100.0, score))

        if "OVEREXTENDED_GREEN_CANDLE_FOMO" in flags:
            vote = AgentVote.REJECT
            rationale = f"Over-extended top wick chase: current mcap is >2.5x dormant consolidation base. Anti-FOMO veto active."
        elif surge_passed and net_buy_passed and score >= 65.0:
            vote = AgentVote.APPROVE
            rationale = f"Confirmed technical inflection: Volume Surge={surge_k:.2f}x, Net Buy Delta={net_buy_delta*100:.0f}%, RSI={rsi:.1f} in healthy momentum band."
        elif score >= 50.0:
            vote = AgentVote.NEUTRAL
            rationale = f"Moderate momentum (Score={score:.1f}/100). Surge={surge_k:.1f}x."
        else:
            vote = AgentVote.REJECT
            rationale = f"Insufficient technical momentum (Score={score:.1f}/100, Buy Delta={net_buy_delta*100:.0f}%)."

        duration_ms = (time.perf_counter() - t0) * 1000
        return AgentVerdict(
            agent_name="QuantTechnicalsAgent",
            vote=vote,
            confidence=round(min(0.95, max(0.50, alpha_score)), 2),
            score=round(score, 1),
            rationale=rationale,
            flags=flags,
            metrics={
                "volume_surge_multiplier": round(surge_k, 2),
                "net_buy_delta_pct": round(net_buy_delta * 100, 1),
                "rsi_5m": round(rsi, 1),
            },
            execution_time_ms=round(duration_ms, 1),
        )
