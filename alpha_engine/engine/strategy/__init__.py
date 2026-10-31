"""
alpha_engine.engine.strategy
============================
Deterministic Strategy Engine (V2) & Institutional Strategy Suite
"""

from .market_quality import MarketAnalyticsEngine
from .scoring import ScoringEngine
from .risk_manager import RiskEngine
from .exit_engine import ExitEngine

# Institutional Trading Strategies Suite
from .base import (
    BaseStrategy,
    MarketRegime,
    StrategyExitSignal,
    StrategySignal,
)
from .momentum_breakout import MomentumBreakoutStrategy
from .mean_reversion import MeanReversionStrategy
from .liquidity_sweep import LiquiditySweepStrategy
from .whale_shadow import WhaleAccumulationShadowStrategy
from .narrative_velocity import NarrativeVelocityStrategy
from .multi_timeframe import MultiTimeframeConfluenceStrategy
from .adaptive_regime import AdaptiveRegimeStrategy
from .strategy_manager import StrategyManager, StrategyRegistry

__all__ = [
    "MarketAnalyticsEngine",
    "ScoringEngine",
    "RiskEngine",
    "ExitEngine",
    # Institutional Suite
    "BaseStrategy",
    "MarketRegime",
    "StrategySignal",
    "StrategyExitSignal",
    "MomentumBreakoutStrategy",
    "MeanReversionStrategy",
    "LiquiditySweepStrategy",
    "WhaleAccumulationShadowStrategy",
    "NarrativeVelocityStrategy",
    "MultiTimeframeConfluenceStrategy",
    "AdaptiveRegimeStrategy",
    "StrategyRegistry",
    "StrategyManager",
]
