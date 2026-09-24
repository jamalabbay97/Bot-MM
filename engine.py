"""
engine.py — Asynchronous Main Loop & System Coordinator (Compatibility Facade)
==============================================================================
Re-exports all coordination and engine definitions from alpha_engine.engine.
"""

from alpha_engine.engine import (
    EngineConfig,
    PaperTradingEngine,
    PoolRegistry,
    SignalGenerator,
    _async_main,
    _build_example_config,
    _optional_env,
    _require_env,
)

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

if __name__ == "__main__":
    import asyncio
    asyncio.run(_async_main())
