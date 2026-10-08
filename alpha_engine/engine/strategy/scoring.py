"""
alpha_engine.engine.strategy.scoring
====================================
Deterministic 0-100 Scoring Engine.
"""

from typing import Any, Dict
from .market_quality import MarketAnalyticsEngine
from alpha_engine.config import EngineConfig

class ScoringEngine:
    """
    Computes a composite 0-100 score based on multiple deterministic factors.
    """

    def __init__(self, config: EngineConfig) -> None:
        self.config = config
        self.market_analytics = MarketAnalyticsEngine()

    def evaluate_security(self, report: Any) -> float:
        """Score out of 100 based on security metrics."""
        if not report:
            return 0.0
        # If it's a honeypot, score is 0
        if getattr(report, "is_honeypot", False):
            return 0.0
        
        score = 100.0
        buy_tax = getattr(report, "buy_tax_bps", 0)
        sell_tax = getattr(report, "sell_tax_bps", 0)
        
        if buy_tax > 500 or sell_tax > 500:
            score -= 50
        elif buy_tax > 200 or sell_tax > 200:
            score -= 20
            
        lp_burned = getattr(report, "lp_burned_ratio", 0.0)
        if not getattr(report, "is_pump_fun", False):
            if lp_burned < 0.90:
                score -= 30
        
        top10 = getattr(report, "top10_concentration", 0.1)
        if top10 > 0.3:
            score -= (top10 - 0.3) * 100
            
        return max(0.0, min(100.0, score))

    def evaluate_structure(self, staging_data: Any) -> float:
        """Score out of 100 for price structure (breakout/retest)."""
        # Baseline heuristic.
        try:
            init_price = float(staging_data.initial_price)
            last_price = float(staging_data.latest_price)
        except AttributeError:
            return 50.0

        if init_price == 0:
            return 50.0
            
        growth = (last_price - init_price) / init_price
        if growth > 0.5:
            return 100.0
        if growth > 0.2:
            return 80.0
        if growth > 0:
            return 60.0
        return 40.0

    def evaluate_smart_money(self, staging_data: Any) -> float:
        """Score out of 100 for smart money."""
        score = 50.0
        
        # Check for whale cluster in wave2 StagedToken
        if hasattr(staging_data, "whale_cluster_detected"):
            if not staging_data.whale_cluster_detected:
                score += 20.0
            else:
                score -= 20.0

        raw = getattr(staging_data, "raw_signal", None)
        if raw and hasattr(raw, "smart_money_score"):
            # assuming smart_money_score is between 0 and 1
            score += getattr(raw, "smart_money_score", 0.0) * 30.0

        return max(0.0, min(100.0, score))

    def evaluate_narrative(self, staging_data: Any) -> float:
        """Score out of 100 for narrative/AI sentiment."""
        score = 50.0
        cluster = None
        
        if hasattr(staging_data, "narrative_cluster") and staging_data.narrative_cluster:
            cluster = staging_data.narrative_cluster
        else:
            raw = getattr(staging_data, "raw_signal", None)
            if raw and hasattr(raw, "narrative_cluster") and raw.narrative_cluster:
                cluster = raw.narrative_cluster
                
        if cluster:
            cluster_lower = cluster.lower()
            if "ai" in cluster_lower:
                score += 30.0
            elif "dog" in cluster_lower or "cat" in cluster_lower:
                score += 20.0
            elif "utility" in cluster_lower:
                score += 10.0
            else:
                score += 5.0
            
        return max(0.0, min(100.0, score))

    def compute_score(self, staging_data: Any, security_report: Any) -> Dict[str, Any]:
        """
        Computes the final weighted 0-100 score.
        """
        market_scores = self.market_analytics.get_market_quality_scores(staging_data)
        
        sec_score = self.evaluate_security(security_report)
        struct_score = self.evaluate_structure(staging_data)
        smart_score = self.evaluate_smart_money(staging_data)
        narrative_score = self.evaluate_narrative(staging_data)

        # Apply weights from config
        w_sec = self.config.score_weight_security
        w_liq = self.config.score_weight_liquidity
        w_vol = self.config.score_weight_volume
        w_flow = self.config.score_weight_flow
        w_holders = self.config.score_weight_holders
        w_smart = self.config.score_weight_smart_money
        w_struct = self.config.score_weight_structure
        w_narr = self.config.score_weight_narrative
        
        total_weight = (w_sec + w_liq + w_vol + w_flow + w_holders + 
                        w_smart + w_struct + w_narr)

        if total_weight == 0:
            total_weight = 100.0 # fallback

        # Liquidity score is somewhat tied to volume/holders here as a stub
        liq_score = market_scores["volume_momentum"]

        weighted_sum = (
            (sec_score * w_sec) +
            (liq_score * w_liq) + 
            (market_scores["volume_momentum"] * w_vol) +
            (market_scores["flow"] * w_flow) +
            (market_scores["holders"] * w_holders) +
            (smart_score * w_smart) +
            (struct_score * w_struct) +
            (narrative_score * w_narr)
        )

        final_score = weighted_sum / total_weight

        return {
            "total_score": final_score,
            "components": {
                "security": sec_score,
                "liquidity": liq_score,
                "volume": market_scores["volume_momentum"],
                "flow": market_scores["flow"],
                "holders": market_scores["holders"],
                "smart_money": smart_score,
                "structure": struct_score,
                "narrative": narrative_score
            }
        }
