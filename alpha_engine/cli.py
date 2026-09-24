"""
alpha_engine.cli — Command-Line Interface
=========================================
Multi-Chain Paper Trading & Alpha Analytics Engine
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine, _build_example_config


def main(args: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="alpha-engine",
        description="Multi-Chain Paper Trading & Alpha Analytics Engine",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run engine with example demo pool configurations",
    )
    parsed = parser.parse_args(args)

    try:
        config = EngineConfig()
    except EnvironmentError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1

    engine = PaperTradingEngine(config)
    watchlist, svm_reg, seed_states = _build_example_config()

    try:
        asyncio.run(
            engine.run(
                pool_watchlist=watchlist,
                svm_pool_registry=svm_reg,
                seed_pool_states=seed_states,
            )
        )
    except KeyboardInterrupt:
        print("\nShutdown by user.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
