"""
alpha_engine.models.base — Base Pydantic Configuration
======================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2
"""

from pydantic import ConfigDict

_STRICT_MODEL_CFG = ConfigDict(
    frozen=True,                    # Immutable after construction
    arbitrary_types_allowed=True,   # Allows Decimal without annotation games
    str_strip_whitespace=True,
    validate_default=True,
    populate_by_name=True,
)
