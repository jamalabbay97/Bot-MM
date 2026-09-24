"""
alpha_engine.engine — System Coordinator & Event Loop
=====================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
"""

from alpha_engine.config import EngineConfig, _optional_env, _require_env
from alpha_engine.engine.registry import PoolRegistry
from alpha_engine.engine.runner import (
    PaperTradingEngine,
    _async_main,
    _build_example_config,
)
from alpha_engine.engine.signals import SignalGenerator

__all__ = [
    "EngineConfig",
    "_require_env",
    "_optional_env",
    "SignalGenerator",
    "PoolRegistry",
    "PaperTradingEngine",
    "_build_example_config",
    "_async_main",
]
