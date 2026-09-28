"""
alpha_engine.math.cpmm — Constant Product Market Maker (CPMM) Mechanics
========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pure Python (Decimal)

Price convention: strictly NATIVE_PER_TOKEN throughout.
BUY  → spot = native_reserve / token_reserve, exec = amount_in  / amount_out
SELL → spot = native_reserve / token_reserve, exec = amount_out / amount_in
"""

from __future__ import annotations

import logging
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import NamedTuple

from alpha_engine.models.enums import ChainIdentifier, OrderSide
from alpha_engine.models.state import PoolState

logger = logging.getLogger(__name__)

BPS_DENOMINATOR: Decimal = Decimal("10000")
_MAX_IMPACT_BPS: int = 10_000
_DECIMAL_ZERO: Decimal = Decimal("0")
_DECIMAL_ONE: Decimal = Decimal("1")


class CpmmQuote(NamedTuple):
    """
    Output of a CPMM price-impact simulation.
    ALL price fields are in NATIVE_PER_TOKEN units regardless of trade direction.
    """

    amount_out: Decimal
    spot_price: Decimal
    execution_price: Decimal
    price_impact_bps: int
    fee_paid: Decimal
    new_reserve_in: Decimal
    new_reserve_out: Decimal
    side: OrderSide


def _compute_price_impact_bps(
    spot_native_per_token: Decimal,
    exec_native_per_token: Decimal,
) -> int:
    """
    Compute price impact in basis points, clamped to [0, 10_000].
    Formula: impact = |1 - (exec_price / spot_price)| * 10_000
    """
    if spot_native_per_token <= _DECIMAL_ZERO:
        return 0
    try:
        raw = abs(_DECIMAL_ONE - (exec_native_per_token / spot_native_per_token))
        bps = int((raw * BPS_DENOMINATOR).to_integral_value(ROUND_DOWN))
    except (InvalidOperation, ZeroDivisionError):
        return 0
    return min(bps, _MAX_IMPACT_BPS)


def cpmm_out(
    reserve_in: Decimal,
    reserve_out: Decimal,
    amount_in: Decimal,
    fee_numerator: int,
    fee_denominator: int,
    side: OrderSide,
    native_reserve: Decimal,
    token_reserve: Decimal,
) -> CpmmQuote:
    """
    Compute the exact output of a Uniswap v2 / CPMM swap and express all
    prices in NATIVE_PER_TOKEN units.
    """
    if reserve_in <= _DECIMAL_ZERO:
        raise ValueError(f"reserve_in must be > 0, got {reserve_in}")
    if reserve_out <= _DECIMAL_ZERO:
        raise ValueError(f"reserve_out must be > 0, got {reserve_out}")
    if amount_in <= _DECIMAL_ZERO:
        raise ValueError(f"amount_in must be > 0, got {amount_in}")
    if fee_numerator < 0 or fee_denominator <= 0:
        raise ValueError("Fee parameters invalid.")
    if native_reserve <= _DECIMAL_ZERO or token_reserve <= _DECIMAL_ZERO:
        raise ValueError("native_reserve and token_reserve must be > 0.")

    fee_denom = Decimal(fee_denominator)
    fee_num = Decimal(fee_numerator)

    amount_in_with_fee: Decimal = amount_in * (fee_denom - fee_num)
    numerator: Decimal = amount_in_with_fee * reserve_out
    denominator: Decimal = reserve_in * fee_denom + amount_in_with_fee

    if denominator == _DECIMAL_ZERO:
        raise ValueError("CPMM denominator is zero — degenerate pool.")

    amount_out: Decimal = numerator / denominator

    if amount_out <= _DECIMAL_ZERO:
        raise ValueError(f"CPMM produced non-positive output: {amount_out}.")
    if amount_out >= reserve_out:
        raise ValueError(
            f"CPMM output {amount_out} exhausts reserve_out {reserve_out}. "
            "Trade size too large for this pool."
        )

    # Spot price is always: how many native per 1 token.
    spot_price_val: Decimal = native_reserve / token_reserve

    if side == OrderSide.BUY:
        exec_price: Decimal = amount_in / amount_out      # NATIVE_PER_TOKEN
    else:
        exec_price = amount_out / amount_in               # NATIVE_PER_TOKEN

    impact_bps: int = _compute_price_impact_bps(spot_price_val, exec_price)

    fee_paid: Decimal = amount_in * (fee_num / fee_denom)
    new_reserve_in: Decimal = reserve_in + amount_in
    new_reserve_out: Decimal = reserve_out - amount_out

    original_k = reserve_in * reserve_out
    new_k = new_reserve_in * new_reserve_out
    if new_k < original_k * Decimal("0.9999"):
        logger.warning(
            "CPMM invariant softly violated: new_k=%s < original_k=%s",
            new_k, original_k,
        )

    return CpmmQuote(
        amount_out=amount_out,
        spot_price=spot_price_val,
        execution_price=exec_price,
        price_impact_bps=impact_bps,
        fee_paid=fee_paid,
        new_reserve_in=new_reserve_in,
        new_reserve_out=new_reserve_out,
        side=side,
    )


def cpmm_buy_quote(pool: PoolState, native_in: Decimal) -> CpmmQuote:
    """
    Simulate a BUY: trader spends `native_in` (ETH / SOL) to receive tokens.
    """
    return cpmm_out(
        reserve_in=pool.native_reserve,
        reserve_out=pool.token_reserve,
        amount_in=native_in,
        fee_numerator=pool.fee_numerator,
        fee_denominator=pool.fee_denominator,
        side=OrderSide.BUY,
        native_reserve=pool.native_reserve,
        token_reserve=pool.token_reserve,
    )


def cpmm_sell_quote(pool: PoolState, tokens_in: Decimal) -> CpmmQuote:
    """
    Simulate a SELL: trader sends `tokens_in` into the pool to receive native.
    """
    return cpmm_out(
        reserve_in=pool.token_reserve,
        reserve_out=pool.native_reserve,
        amount_in=tokens_in,
        fee_numerator=pool.fee_numerator,
        fee_denominator=pool.fee_denominator,
        side=OrderSide.SELL,
        native_reserve=pool.native_reserve,
        token_reserve=pool.token_reserve,
    )


def spot_price(pool: PoolState) -> Decimal:
    """Instantaneous spot price in NATIVE_PER_TOKEN units."""
    return pool.native_reserve / pool.token_reserve


def execution_price_buy(native_spent: Decimal, tokens_received: Decimal) -> Decimal:
    """Effective fill price for a BUY in NATIVE_PER_TOKEN."""
    if tokens_received <= _DECIMAL_ZERO:
        raise ValueError("tokens_received must be > 0.")
    return native_spent / tokens_received


def execution_price_sell(native_received: Decimal, tokens_sold: Decimal) -> Decimal:
    """Effective fill price for a SELL in NATIVE_PER_TOKEN."""
    if tokens_sold <= _DECIMAL_ZERO:
        raise ValueError("tokens_sold must be > 0.")
    return native_received / tokens_sold


def price_impact_bps(
    spot: Decimal,
    exec_price: Decimal,
    side: OrderSide,
) -> int:
    """Public wrapper: compute price impact in basis points, clamped [0, 10_000]."""
    bps = _compute_price_impact_bps(spot, exec_price)
    logger.debug(
        "price_impact_bps [%s]: spot=%s exec=%s bps=%d",
        side.value, spot, exec_price, bps,
    )
    return bps


def get_initial_bonding_curve_pool(
    token_address: str,
    chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
    pool_address: str = "",
) -> PoolState:
    """
    Establish legitimate initial bonding curve reserves when pool state has not yet arrived.
    For Solana pump.fun tokens, applies standard initial virtual reserves:
      - 30 SOL virtual reserve
      - 1,073,000,000 virtual tokens
      - Spot price = 30 / 1,073,000,000 ≈ 2.7959e-8 SOL/token
    For Base/EVM, seeds default initial liquidity.
    """
    if chain == ChainIdentifier.SOLANA_MAINNET:
        return PoolState(
            pool_address=pool_address or token_address,
            chain=chain,
            token_address=token_address,
            native_reserve=Decimal("30.0"),
            token_reserve=Decimal("1073000000.0"),
            fee_numerator=10,
            fee_denominator=1000,
            last_updated_block=0,
            token_decimals=6,
            native_decimals=9,
        )
    return PoolState(
        pool_address=pool_address or token_address,
        chain=chain,
        token_address=token_address,
        native_reserve=Decimal("1.0"),
        token_reserve=Decimal("1000000000.0"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=0,
        token_decimals=18,
        native_decimals=18,
    )
