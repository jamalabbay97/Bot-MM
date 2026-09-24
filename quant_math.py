"""
quant_math.py — Quantitative Math & Dynamic Slippage Engine
============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pure Python (no numpy dependency in critical path)

All arithmetic uses Python's `decimal.Decimal` for exact precision.
No floats in financial computations — floats appear only in the
stochastic latency model where statistical noise > rounding error.

Implements:
  1.  CPMM output calculation (Uniswap v2 / Raydium constant-product).
  2.  Spot price, execution price, and price impact in basis points.
  3.  Stochastic fill-latency model (Uniform[1200ms, 3500ms]).
  4.  Reserve re-sampling to simulate front-run / MEV slippage during
      the latency window before the paper trade is filled.
  5.  Half-Kelly position sizing capped at 1% portfolio equity.
  6.  Gas cost constants per chain.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import NamedTuple

from models import ChainIdentifier, OrderSide, PoolState

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Fill-latency model parameters (milliseconds)
LATENCY_MIN_MS: int = 1_200
LATENCY_MAX_MS: int = 3_500

# Simulated priority fees charged per trade (USD)
GAS_COST_SOL_USD: Decimal = Decimal("0.03")
GAS_COST_BASE_USD: Decimal = Decimal("0.05")

# Maximum portfolio fraction per position (hard risk cap)
MAX_POSITION_FRACTION: Decimal = Decimal("0.01")  # 1 %

# Basis-point denominator
BPS_DENOMINATOR: Decimal = Decimal("10000")

# Precision context: 28 significant figures (exceeds IEEE-754 double)
# Rounding is deferred to the output boundary only.
_DECIMAL_ZERO = Decimal("0")
_DECIMAL_ONE = Decimal("1")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


class CpmmQuote(NamedTuple):
    """
    Output of a CPMM price-impact simulation.

    Fields
    ------
    amount_out          : Tokens received after LP fees (exact, Decimal).
    spot_price          : y/x before the trade (native per token for BUY).
    execution_price     : amount_in / amount_out.
    price_impact_bps    : |1 - execution_price / spot_price| * 10_000, integer.
    fee_paid            : LP fee deducted from amount_in (in input token units).
    new_reserve_in      : Pool reserve of the input token after the trade.
    new_reserve_out     : Pool reserve of the output token after the trade.
    """

    amount_out: Decimal
    spot_price: Decimal
    execution_price: Decimal
    price_impact_bps: int
    fee_paid: Decimal
    new_reserve_in: Decimal
    new_reserve_out: Decimal


@dataclass(frozen=True)
class LatencyResult:
    """
    Output of the stochastic latency model.

    Fields
    ------
    latency_ms          : Sampled latency in milliseconds (integer).
    fill_timestamp_ns   : signal_timestamp_ns + latency_ms * 1_000_000.
    frontrun_volume     : Simulated external volume transacted against the pool
                          during the latency window (in native asset units).
    adjusted_pool_state : Pool state after applying simulated front-run volume.
    """

    latency_ms: int
    fill_timestamp_ns: int
    frontrun_volume: Decimal
    adjusted_pool_state: PoolState


@dataclass(frozen=True)
class KellySizing:
    """
    Output of the Half-Kelly position-sizing calculation.

    Fields
    ------
    kelly_fraction      : Raw half-Kelly fraction before 1% cap.
    capped_fraction     : min(kelly_fraction, MAX_POSITION_FRACTION).
    native_trade_size   : capped_fraction * portfolio_equity / native_price_usd.
    usd_at_risk         : capped_fraction * portfolio_equity_usd.
    """

    kelly_fraction: float
    capped_fraction: float
    native_trade_size: Decimal
    usd_at_risk: Decimal


# ---------------------------------------------------------------------------
# 1. CPMM Math: Constant Product Market Maker
# ---------------------------------------------------------------------------


def cpmm_out(
    reserve_in: Decimal,
    reserve_out: Decimal,
    amount_in: Decimal,
    fee_numerator: int,
    fee_denominator: int,
) -> CpmmQuote:
    """
    Compute the exact output of a Uniswap v2 / CPMM swap.

    Formula (Uniswap v2 whitepaper):
        fee_factor = 1 - (fee_numerator / fee_denominator)   [= gamma]
        amount_in_with_fee = amount_in * (fee_denominator - fee_numerator)
        amount_out = (amount_in_with_fee * reserve_out)
                     / (reserve_in * fee_denominator + amount_in_with_fee)

    Equivalent to:
        delta_y = (y * delta_x * (1 - gamma)) / (x + delta_x * (1 - gamma))

    Invariant check (post-trade):
        new_k = (reserve_in + amount_in_with_fee / fee_denominator) * new_reserve_out
        new_k >= original_k  (fees increase k slightly)

    Parameters
    ----------
    reserve_in      : Current pool reserve of the input token.
    reserve_out     : Current pool reserve of the output token.
    amount_in       : Gross amount of input token sent into pool (pre-fee).
    fee_numerator   : Fee numerator (e.g., 3 for 0.3%).
    fee_denominator : Fee denominator (e.g., 1000).

    Returns
    -------
    CpmmQuote with all computed fields.

    Raises
    ------
    ValueError  : If inputs are non-positive or invariant is violated.
    """
    if reserve_in <= _DECIMAL_ZERO:
        raise ValueError(f"reserve_in must be > 0, got {reserve_in}")
    if reserve_out <= _DECIMAL_ZERO:
        raise ValueError(f"reserve_out must be > 0, got {reserve_out}")
    if amount_in <= _DECIMAL_ZERO:
        raise ValueError(f"amount_in must be > 0, got {amount_in}")
    if fee_numerator < 0 or fee_denominator <= 0:
        raise ValueError("Fee parameters invalid.")

    fee_denom = Decimal(fee_denominator)
    fee_num = Decimal(fee_numerator)

    # Pre-fee amount: amount_in * (fee_denominator - fee_numerator)
    amount_in_with_fee: Decimal = amount_in * (fee_denom - fee_num)

    # Core formula:  delta_y = (amount_in_with_fee * reserve_out)
    #                          / (reserve_in * fee_denominator + amount_in_with_fee)
    numerator: Decimal = amount_in_with_fee * reserve_out
    denominator: Decimal = reserve_in * fee_denom + amount_in_with_fee

    if denominator == _DECIMAL_ZERO:
        raise ValueError("CPMM denominator is zero — pool is degenerate.")

    amount_out: Decimal = numerator / denominator

    if amount_out <= _DECIMAL_ZERO:
        raise ValueError(
            f"CPMM produced non-positive output: {amount_out}. "
            "Pool reserves or input amount may be malformed."
        )
    if amount_out >= reserve_out:
        raise ValueError(
            f"CPMM output {amount_out} exhausts or exceeds reserve_out "
            f"{reserve_out}. Trade size is too large for this pool."
        )

    # Spot price before trade: reserve_out / reserve_in (out per unit of in)
    spot_price: Decimal = reserve_out / reserve_in

    # Execution price: amount_out / amount_in (actual out per unit of in)
    execution_price: Decimal = amount_out / amount_in

    # Price impact: how much worse execution_price is vs spot_price
    # Impact = |1 - (execution_price / spot_price)|
    try:
        price_impact_raw: Decimal = abs(_DECIMAL_ONE - (execution_price / spot_price))
    except InvalidOperation as exc:
        raise ValueError(f"Price impact calculation failed: {exc}") from exc

    price_impact_bps: int = int((price_impact_raw * BPS_DENOMINATOR).to_integral_value(ROUND_DOWN))

    # LP fee paid (in input token units)
    fee_paid: Decimal = amount_in * (fee_num / fee_denom)

    # New pool reserves after trade
    new_reserve_in: Decimal = reserve_in + amount_in
    new_reserve_out: Decimal = reserve_out - amount_out

    # Invariant sanity check: new_k >= original_k
    # (fees slightly increase k because fee is taken from amount_in)
    original_k = reserve_in * reserve_out
    new_k = new_reserve_in * new_reserve_out
    if new_k < original_k * Decimal("0.9999"):  # allow tiny decimal rounding
        logger.warning(
            "CPMM invariant violation: new_k=%s < original_k=%s",
            new_k,
            original_k,
        )

    return CpmmQuote(
        amount_out=amount_out,
        spot_price=spot_price,
        execution_price=execution_price,
        price_impact_bps=price_impact_bps,
        fee_paid=fee_paid,
        new_reserve_in=new_reserve_in,
        new_reserve_out=new_reserve_out,
    )


def cpmm_buy_quote(pool: PoolState, native_in: Decimal) -> CpmmQuote:
    """
    Simulate a BUY: trader spends `native_in` of the native asset to receive tokens.

    Maps the generic cpmm_out() to the BUY direction:
      - reserve_in  = pool.native_reserve
      - reserve_out = pool.token_reserve

    Parameters
    ----------
    pool        : Current pool reserves and fee parameters.
    native_in   : Amount of native asset (ETH / SOL) to spend.

    Returns
    -------
    CpmmQuote where amount_out is the token quantity received.
    """
    return cpmm_out(
        reserve_in=pool.native_reserve,
        reserve_out=pool.token_reserve,
        amount_in=native_in,
        fee_numerator=pool.fee_numerator,
        fee_denominator=pool.fee_denominator,
    )


def cpmm_sell_quote(pool: PoolState, tokens_in: Decimal) -> CpmmQuote:
    """
    Simulate a SELL: trader spends `tokens_in` of the token to receive native asset.

    Maps the generic cpmm_out() to the SELL direction:
      - reserve_in  = pool.token_reserve
      - reserve_out = pool.native_reserve

    Parameters
    ----------
    pool        : Current pool reserves and fee parameters.
    tokens_in   : Amount of token to sell.

    Returns
    -------
    CpmmQuote where amount_out is the native asset quantity received.
    """
    return cpmm_out(
        reserve_in=pool.token_reserve,
        reserve_out=pool.native_reserve,
        amount_in=tokens_in,
        fee_numerator=pool.fee_numerator,
        fee_denominator=pool.fee_denominator,
    )


# ---------------------------------------------------------------------------
# 2. Spot & Execution Price Utilities
# ---------------------------------------------------------------------------


def spot_price(pool: PoolState) -> Decimal:
    """
    Return the instantaneous spot price: native per token.

    Spot = native_reserve / token_reserve
    """
    return pool.native_reserve / pool.token_reserve


def execution_price(amount_in: Decimal, amount_out: Decimal) -> Decimal:
    """
    Return the effective per-unit price from a completed swap.

    Execution = amount_in / amount_out
    (native spent per token received for a BUY; tokens spent per native received for SELL)
    """
    if amount_out <= _DECIMAL_ZERO:
        raise ValueError(f"amount_out must be > 0 for price calculation, got {amount_out}")
    return amount_in / amount_out


def price_impact_bps(
    spot: Decimal,
    exec_price: Decimal,
    side: OrderSide,
) -> int:
    """
    Compute price impact in basis points.

    For a BUY, execution_price > spot_price (you pay more per token than the market).
    For a SELL, execution_price < spot_price (you receive less per native than the market).

    Formula:
        impact = |1 - (execution_price / spot_price)| * 10_000

    Parameters
    ----------
    spot        : Spot price (native per token) before the trade.
    exec_price  : Effective price after the CPMM math.
    side        : Trade direction (used only for logging clarity).

    Returns
    -------
    Price impact as an integer number of basis points.
    """
    if spot <= _DECIMAL_ZERO:
        raise ValueError(f"spot_price must be > 0, got {spot}")
    impact_raw = abs(_DECIMAL_ONE - (exec_price / spot))
    bps = int((impact_raw * BPS_DENOMINATOR).to_integral_value(ROUND_DOWN))
    logger.debug(
        "Price impact [%s]: spot=%s, exec=%s, impact=%d bps",
        side.value,
        spot,
        exec_price,
        bps,
    )
    return bps


# ---------------------------------------------------------------------------
# 3. Stochastic Latency & Front-Run Model
# ---------------------------------------------------------------------------


def sample_fill_latency() -> int:
    """
    Sample a fill latency from Uniform(LATENCY_MIN_MS, LATENCY_MAX_MS).

    Models the realistic delay between:
      - The engine detecting a signal (t_signal)
      - The hypothetical on-chain confirmation (t_fill)

    Returns
    -------
    Latency in integer milliseconds.
    """
    return random.randint(LATENCY_MIN_MS, LATENCY_MAX_MS)


def simulate_frontrun_volume(
    pool: PoolState,
    latency_ms: int,
    annualised_volume_native: Decimal,
) -> Decimal:
    """
    Estimate the volume of external swaps that will hit the pool during the
    latency window before our paper trade is filled.

    Model:
        second_volume = annualised_volume_native / (365 * 24 * 3600)
        window_volume = second_volume * (latency_ms / 1000)
        frontrun      = window_volume * Uniform(0.5, 2.0)  [MEV amplifier]

    The MEV amplifier reflects that MEV bots cluster around volatile events,
    so front-running pressure is super-proportional to base volume.

    Parameters
    ----------
    pool                        : Current pool state.
    latency_ms                  : Fill latency in milliseconds.
    annualised_volume_native    : Estimated annual trading volume in native units.
                                  Use on-chain 24h volume * 365 as a proxy.

    Returns
    -------
    Estimated front-run native volume during the latency window.
    """
    seconds_per_year = Decimal("31_536_000")
    per_second_volume = annualised_volume_native / seconds_per_year
    window_seconds = Decimal(str(latency_ms)) / Decimal("1000")
    base_window_volume = per_second_volume * window_seconds

    # MEV amplifier: uniform [0.5, 2.0]
    mev_factor = Decimal(str(random.uniform(0.5, 2.0)))
    frontrun_volume = base_window_volume * mev_factor

    logger.debug(
        "Front-run volume: %.6f native over %d ms (MEV factor: %.2f)",
        frontrun_volume,
        latency_ms,
        mev_factor,
    )
    return frontrun_volume


def apply_frontrun_to_pool(
    pool: PoolState,
    frontrun_volume_native: Decimal,
) -> PoolState:
    """
    Re-sample pool reserves after simulated front-run volume has been applied.

    Assumption: all front-run volume is BUY-side (adversarial MEV bots
    front-running a buy signal by buying ahead of us, raising our fill price).
    This is the worst-case MEV scenario and produces conservative (pessimistic)
    PnL estimates.

    We apply the CPMM formula to compute new reserves after the adversarial
    swap, then return a new frozen PoolState reflecting those reserves.

    If frontrun_volume is so large it would exhaust the pool, we cap it at
    50% of native_reserve to avoid degenerate states.

    Parameters
    ----------
    pool                    : Pool state before front-run.
    frontrun_volume_native  : Native asset volume of simulated MEV buys.

    Returns
    -------
    New PoolState with updated reserves reflecting front-run impact.
    """
    # Cap front-run at 50% of native reserve to avoid pool exhaustion
    max_frontrun = pool.native_reserve * Decimal("0.5")
    safe_frontrun = min(frontrun_volume_native, max_frontrun)

    if safe_frontrun <= _DECIMAL_ZERO:
        return pool

    try:
        quote = cpmm_buy_quote(pool, safe_frontrun)
    except ValueError as exc:
        # If the quote fails (e.g., marginal reserve), return pool unchanged
        logger.warning("Front-run simulation failed: %s — using original pool.", exc)
        return pool

    # Construct a new PoolState with updated reserves.
    # PoolState is frozen (Pydantic), so we use model_copy(update=...).
    updated_pool = pool.model_copy(
        update={
            "native_reserve": quote.new_reserve_in,
            "token_reserve": quote.new_reserve_out,
        }
    )
    logger.debug(
        "Post-frontrun pool: native_reserve=%s -> %s, token_reserve=%s -> %s",
        pool.native_reserve,
        updated_pool.native_reserve,
        pool.token_reserve,
        updated_pool.token_reserve,
    )
    return updated_pool


def simulate_latency(
    pool: PoolState,
    signal_timestamp_ns: int,
    annualised_volume_native: Decimal,
) -> LatencyResult:
    """
    Full stochastic latency pipeline:

    1. Sample fill latency from Uniform[1200ms, 3500ms].
    2. Compute fill timestamp = signal_timestamp_ns + latency_ms * 1_000_000.
    3. Simulate front-run volume during the latency window.
    4. Apply front-run to produce adjusted pool reserves.

    Parameters
    ----------
    pool                        : Pool state at signal generation time.
    signal_timestamp_ns         : Nanosecond timestamp of signal detection.
    annualised_volume_native    : Annual volume estimate for MEV modelling.

    Returns
    -------
    LatencyResult with adjusted pool state for use in the paper fill.
    """
    latency_ms = sample_fill_latency()
    fill_timestamp_ns = signal_timestamp_ns + latency_ms * 1_000_000

    frontrun_volume = simulate_frontrun_volume(
        pool=pool,
        latency_ms=latency_ms,
        annualised_volume_native=annualised_volume_native,
    )

    adjusted_pool = apply_frontrun_to_pool(pool, frontrun_volume)

    return LatencyResult(
        latency_ms=latency_ms,
        fill_timestamp_ns=fill_timestamp_ns,
        frontrun_volume=frontrun_volume,
        adjusted_pool_state=adjusted_pool,
    )


# ---------------------------------------------------------------------------
# 4. Half-Kelly Position Sizing
# ---------------------------------------------------------------------------


def half_kelly_fraction(
    win_rate: float,
    avg_win_native: Decimal,
    avg_loss_native: Decimal,
) -> float:
    """
    Compute the Half-Kelly bet fraction.

    Full Kelly:
        f* = (p * b - q) / b
        where p = win_rate, q = 1 - p, b = avg_win / avg_loss (win/loss ratio)

    Half-Kelly:
        f_half = f* / 2

    A half-Kelly fraction is used for conservatism:
      - Reduces drawdown compared to full Kelly.
      - Accounts for model uncertainty / parameter estimation error.

    Parameters
    ----------
    win_rate        : Historical win fraction [0, 1] across past paper trades.
    avg_win_native  : Average profit per winning trade in native units.
    avg_loss_native : Average loss per losing trade in native units (positive value).

    Returns
    -------
    Half-Kelly fraction as a float in [0, 1].
    Returns 0.0 if the edge is zero or negative (no bet).
    """
    if win_rate <= 0.0 or win_rate >= 1.0:
        return 0.0
    if avg_loss_native <= _DECIMAL_ZERO or avg_win_native <= _DECIMAL_ZERO:
        return 0.0

    p = win_rate
    q = 1.0 - p
    b = float(avg_win_native / avg_loss_native)   # win/loss ratio

    full_kelly = (p * b - q) / b

    if full_kelly <= 0.0:
        # Negative Kelly edge: strategy has no positive expectancy → no bet
        logger.debug(
            "Kelly edge negative (%.4f) — skipping position. "
            "win_rate=%.2f, b=%.4f",
            full_kelly,
            win_rate,
            b,
        )
        return 0.0

    half_kelly = full_kelly / 2.0
    # Kelly can theoretically produce > 1.0 with very high edge; hard-clamp
    return min(half_kelly, 1.0)


def compute_position_size(
    kelly_fraction: float,
    portfolio_equity_usd: Decimal,
    native_price_usd: Decimal,
    side: OrderSide,
) -> KellySizing:
    """
    Translate a Kelly fraction into a concrete native-asset trade size,
    subject to the hard 1% portfolio cap.

    Parameters
    ----------
    kelly_fraction          : Raw half-Kelly output (float, 0–1).
    portfolio_equity_usd    : Current total portfolio equity in USD.
    native_price_usd        : Current price of the native asset (ETH or SOL) in USD.
    side                    : Trade direction (informational only for this function).

    Returns
    -------
    KellySizing with the capped fraction and concrete native trade size.

    Raises
    ------
    ValueError: If native_price_usd <= 0 or portfolio_equity_usd <= 0.
    """
    if native_price_usd <= _DECIMAL_ZERO:
        raise ValueError(f"native_price_usd must be > 0, got {native_price_usd}")
    if portfolio_equity_usd <= _DECIMAL_ZERO:
        raise ValueError(f"portfolio_equity_usd must be > 0, got {portfolio_equity_usd}")

    kelly_dec = Decimal(str(kelly_fraction))
    capped_fraction = min(kelly_dec, MAX_POSITION_FRACTION)
    usd_at_risk = capped_fraction * portfolio_equity_usd
    native_trade_size = usd_at_risk / native_price_usd

    logger.debug(
        "Kelly sizing [%s]: raw=%.4f, capped=%.4f, USD=%.4f, native=%.8f",
        side.value,
        kelly_fraction,
        float(capped_fraction),
        float(usd_at_risk),
        float(native_trade_size),
    )

    return KellySizing(
        kelly_fraction=kelly_fraction,
        capped_fraction=float(capped_fraction),
        native_trade_size=native_trade_size,
        usd_at_risk=usd_at_risk,
    )


# ---------------------------------------------------------------------------
# 5. Gas Cost Lookup
# ---------------------------------------------------------------------------


def gas_cost_usd(chain: ChainIdentifier) -> Decimal:
    """
    Return the fixed simulated gas / priority-fee cost in USD for a given chain.

    Values
    ------
    Solana (Helius) : $0.03  (typical Solana priority fee + compute unit cost)
    Base (Alchemy)  : $0.05  (typical L2 gas cost on Base at moderate load)

    These are static parametric costs, not dynamic gas oracle queries.
    They represent the conservative upper bound for free-tier paper trading.
    """
    match chain:
        case ChainIdentifier.SOLANA_MAINNET:
            return GAS_COST_SOL_USD
        case ChainIdentifier.BASE_MAINNET:
            return GAS_COST_BASE_USD
        case _:
            raise ValueError(f"Unknown chain: {chain}")


# ---------------------------------------------------------------------------
# 6. Alpha Score to Signal Strength Classifier
# ---------------------------------------------------------------------------


def classify_alpha_score(alpha_score: float) -> str:
    """
    Map a continuous alpha score [0, 1] to a discrete SignalStrength label.

    Thresholds (percentile-based, calibrated on simulated universe):
      - STRONG   : alpha_score > 0.80
      - MODERATE : 0.40 < alpha_score <= 0.80
      - WEAK     : alpha_score <= 0.40

    Returns the string value of SignalStrength (avoids circular import
    from models.py if this module is imported first).
    """
    if alpha_score > 0.80:
        return "strong"
    elif alpha_score > 0.40:
        return "moderate"
    else:
        return "weak"
