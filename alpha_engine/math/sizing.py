"""
alpha_engine.math.sizing — Position Sizing, Risk Gates, and Score Classification
================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pure Python (Decimal)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal

from alpha_engine.models.enums import ChainIdentifier, OrderSide

logger = logging.getLogger(__name__)

# Simulated priority fees charged per trade (USD)
GAS_COST_SOL_USD: Decimal = Decimal("0.02")
GAS_COST_BASE_USD: Decimal = Decimal("0.05")

# Maximum portfolio fraction per position (hard risk cap: 1% to 5%)
MAX_POSITION_FRACTION: Decimal = Decimal("0.01")  # Default 1%
MAX_AUTONOMOUS_CAP: Decimal = Decimal("0.05")     # Autonomous hard-stop cap 5%

# Realistic paper-trading drag constants:
DEX_SWAP_FEE_PCT: Decimal = Decimal("0.003")      # 0.3% DEX swap fee
SIMULATED_GAS_NATIVE: Decimal = Decimal("0.005")  # 0.005 SOL or ETH
ASSUMED_SLIPPAGE_PCT: Decimal = Decimal("0.05")   # 5% assumed execution slippage

# Gas drag gate: reject trade if gas > 5 % of position value (usd_at_risk < gas_cost * 20).
GAS_MULTIPLE_GATE: int = 20

_DECIMAL_ZERO = Decimal("0")



@dataclass(frozen=True)
class KellySizing:
    """
    Output of the Half-Kelly position-sizing calculation.
    """

    kelly_fraction: float
    capped_fraction: float
    native_trade_size: Decimal
    usd_at_risk: Decimal
    gas_rejected: bool


def gas_cost_usd(chain: ChainIdentifier) -> Decimal:
    """
    Return the fixed simulated gas / priority-fee cost in USD per trade.
    Solana : $0.03
    Base   : $0.05
    """
    match chain:
        case ChainIdentifier.SOLANA_MAINNET:
            return GAS_COST_SOL_USD
        case ChainIdentifier.BASE_MAINNET:
            return GAS_COST_BASE_USD
        case _:
            raise ValueError(f"Unknown chain: {chain}")


def half_kelly_fraction(
    win_rate: float,
    avg_win_native: Decimal,
    avg_loss_native: Decimal,
) -> float:
    """
    Compute the Half-Kelly bet fraction.
    """
    if win_rate <= 0.0 or win_rate >= 1.0:
        return 0.0
    if avg_loss_native <= _DECIMAL_ZERO or avg_win_native <= _DECIMAL_ZERO:
        return 0.0

    p = win_rate
    q = 1.0 - p
    b = float(avg_win_native / avg_loss_native)

    full_kelly = (p * b - q) / b

    if full_kelly <= 0.0:
        logger.debug(
            "Kelly edge negative (%.4f) — no bet. win_rate=%.2f b=%.4f",
            full_kelly, p, b,
        )
        return 0.0

    return min(full_kelly / 2.0, 1.0)


def compute_position_size(
    kelly_fraction: float,
    portfolio_equity_usd: Decimal,
    native_price_usd: Decimal,
    chain: ChainIdentifier,
    side: OrderSide,
    max_position_fraction: Decimal | None = None,
) -> KellySizing:
    """
    Translate a Kelly fraction into a concrete native-asset trade size,
    subject to:
      (a) Hard portfolio cap (default 1%, or custom 2-5% cap).
      (b) Gas-drag rejection gate: if gas > 5 % of position (i.e.
          usd_at_risk < gas_cost_usd × 20), set size = 0 and flag rejected.
    """
    if native_price_usd <= _DECIMAL_ZERO:
        raise ValueError(f"native_price_usd must be > 0, got {native_price_usd}")
    if portfolio_equity_usd <= _DECIMAL_ZERO:
        raise ValueError(f"portfolio_equity_usd must be > 0, got {portfolio_equity_usd}")

    cap = max_position_fraction if max_position_fraction is not None else MAX_POSITION_FRACTION
    kelly_dec = Decimal(str(kelly_fraction))
    capped_fraction = min(kelly_dec, cap)
    usd_at_risk = capped_fraction * portfolio_equity_usd

    trade_gas_usd = gas_cost_usd(chain)
    gas_threshold = trade_gas_usd * GAS_MULTIPLE_GATE

    if usd_at_risk < gas_threshold:
        logger.info(
            "Gas-drag gate REJECTED [%s %s]: usd_at_risk=%s < gas_threshold=%s "
            "(gas=%s × %d). Position too small to overcome fees.",
            side.value.upper(), chain.value,
            usd_at_risk, gas_threshold,
            trade_gas_usd, GAS_MULTIPLE_GATE,
        )
        return KellySizing(
            kelly_fraction=kelly_fraction,
            capped_fraction=float(capped_fraction),
            native_trade_size=_DECIMAL_ZERO,
            usd_at_risk=usd_at_risk,
            gas_rejected=True,
        )

    native_trade_size = usd_at_risk / native_price_usd

    logger.debug(
        "Kelly sizing [%s %s]: raw=%.4f capped=%.4f USD=%.4f native=%.8f",
        side.value, chain.value,
        kelly_fraction, float(capped_fraction),
        float(usd_at_risk), float(native_trade_size),
    )

    return KellySizing(
        kelly_fraction=kelly_fraction,
        capped_fraction=float(capped_fraction),
        native_trade_size=native_trade_size,
        usd_at_risk=usd_at_risk,
        gas_rejected=False,
    )


def apply_paper_trading_drag(
    gross_amount: Decimal,
    execution_price: Decimal,
    side: OrderSide,
    native_gas_cost: Decimal = SIMULATED_GAS_NATIVE,
    swap_fee_pct: Decimal = DEX_SWAP_FEE_PCT,
    slippage_pct: Decimal = ASSUMED_SLIPPAGE_PCT,
) -> tuple[Decimal, Decimal, Decimal]:
    """
    Deduct dynamic simulated fees to reflect live market drag:
    - 0.3% DEX swap fee
    - 0.005 SOL/ETH gas
    - 5% assumed execution slippage
    Returns: (net_amount, adjusted_price, fee_drag_usd)
    """
    if side == OrderSide.BUY:
        effective_price = execution_price * (Decimal("1") + slippage_pct)
        swap_fee = gross_amount * swap_fee_pct
        net_amount = (gross_amount - swap_fee) / effective_price
    else:
        effective_price = execution_price * (Decimal("1") - slippage_pct)
        gross_native = gross_amount * effective_price
        swap_fee = gross_native * swap_fee_pct
        net_amount = max(Decimal("0"), gross_native - swap_fee - native_gas_cost)

    return net_amount, effective_price, swap_fee



def classify_alpha_score(alpha_score: float) -> str:
    """
    Map a continuous alpha score [0, 1] to a discrete SignalStrength label string.
    """
    if alpha_score > 0.80:
        return "strong"
    elif alpha_score > 0.40:
        return "moderate"
    else:
        return "weak"
