"""
alpha_engine.execution.executor — Paper Trading Execution Engine
================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import logging
import time
from decimal import Decimal

from alpha_engine.execution.book import PositionBook
from alpha_engine.math.cpmm import (
    cpmm_buy_quote,
    cpmm_sell_quote,
    get_initial_bonding_curve_pool,
)
from alpha_engine.math.mev import simulate_latency
from alpha_engine.math.sizing import (
    apply_paper_trading_drag,
    compute_position_size,
    gas_cost_usd,
    half_kelly_fraction,
)
from alpha_engine.models.enums import ChainIdentifier, OrderSide, TradeExitReason
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PaperFill, PoolState

logger = logging.getLogger(__name__)


class PaperExecutor:
    """
    Converts a validated SignalEvent into a PaperFill.
    """

    def __init__(
        self,
        position_book: PositionBook,
        native_price_usd: Decimal,
        historical_win_rate: float = 0.5,
        avg_win_native: Decimal = Decimal("0.1"),
        avg_loss_native: Decimal = Decimal("0.05"),
    ) -> None:
        self._positions = position_book
        self._native_price_usd = native_price_usd
        self._win_rate = historical_win_rate
        self._avg_win = avg_win_native
        self._avg_loss = avg_loss_native

    def update_native_price(self, new_price: Decimal) -> None:
        """Update the native asset price used for USD conversions."""
        self._native_price_usd = new_price

    def update_historical_stats(
        self,
        win_rate: float,
        avg_win: Decimal,
        avg_loss: Decimal,
    ) -> None:
        """Refresh Kelly parameters from the ledger after each closed trade."""
        self._win_rate = win_rate
        self._avg_win = avg_win
        self._avg_loss = avg_loss

    async def execute_signal(
        self,
        signal: SignalEvent,
        portfolio_equity_usd: Decimal,
        max_position_fraction: Decimal | None = None,
        apply_drag: bool = False,
    ) -> PaperFill | None:
        """
        Produce a PaperFill for the given signal, or return None if skipped.
        Optionally applies realistic paper market drag (0.3% fee + 0.005 gas + 5% slippage).
        """
        side = signal.suggested_side
        pool = signal.pool_state
        if pool is None or pool.token_reserve <= Decimal(0) or pool.native_reserve <= Decimal(0):
            pool = get_initial_bonding_curve_pool(
                token_address=signal.token_address,
                chain=signal.chain,
                pool_address=signal.pool_address,
            )

        latency_result = simulate_latency(
            pool=pool,
            signal_timestamp_ns=signal.timestamp_ns,
        )
        adjusted_pool = latency_result.adjusted_pool_state

        kelly_f = half_kelly_fraction(
            win_rate=self._win_rate,
            avg_win_native=self._avg_win,
            avg_loss_native=self._avg_loss,
        )

        if kelly_f <= 0.0 and side == OrderSide.BUY:
            logger.debug(
                "Kelly edge <= 0 for signal %s — skipping BUY.", signal.signal_id[:8]
            )
            return None

        sizing = compute_position_size(
            kelly_fraction=kelly_f,
            portfolio_equity_usd=portfolio_equity_usd,
            native_price_usd=self._native_price_usd,
            chain=signal.chain,
            side=side,
            max_position_fraction=max_position_fraction,
        )

        if sizing.gas_rejected:
            logger.debug(
                "Gas-drag gate rejected signal %s on %s — size too small.",
                signal.signal_id[:8], signal.chain.value,
            )
            return None

        if side == OrderSide.BUY:
            native_in = sizing.native_trade_size
            try:
                quote = cpmm_buy_quote(adjusted_pool, native_in)
            except ValueError as exc:
                logger.warning("CPMM BUY quote failed for %s: %s", signal.signal_id[:8], exc)
                return None

            simulated_native_spent = native_in
            tokens_acquired = quote.amount_out
            if tokens_acquired <= Decimal(0):
                logger.warning("CPMM output 0 tokens for %s — skipping.", signal.signal_id[:8])
                return None
            effective_price = native_in / tokens_acquired
            price_impact_bps = quote.price_impact_bps

            if apply_drag:
                tokens_acquired, effective_price, _ = apply_paper_trading_drag(
                    gross_amount=native_in,
                    execution_price=effective_price,
                    side=OrderSide.BUY,
                )

            if effective_price <= Decimal(0):
                logger.warning("Effective price <= 0 for %s — skipping.", signal.signal_id[:8])
                return None

        else:  # SELL
            tokens_held = self._positions.get_holdings(
                signal.chain, signal.token_address
            )
            if tokens_held <= 0:
                logger.debug(
                    "No open position for SELL signal %s — skipping.",
                    signal.signal_id[:8],
                )
                return None

            try:
                quote = cpmm_sell_quote(adjusted_pool, tokens_held)
            except ValueError as exc:
                logger.warning("CPMM SELL quote failed for %s: %s", signal.signal_id[:8], exc)
                return None

            tokens_acquired = tokens_held
            simulated_native_spent = quote.amount_out
            effective_price = simulated_native_spent / tokens_acquired
            price_impact_bps = quote.price_impact_bps

            if apply_drag:
                simulated_native_spent, effective_price, _ = apply_paper_trading_drag(
                    gross_amount=tokens_held,
                    execution_price=effective_price,
                    side=OrderSide.SELL,
                )

        gas = gas_cost_usd(signal.chain)

        fill = PaperFill(
            token_address=signal.token_address,
            pool_address=signal.pool_address,
            chain=signal.chain,
            side=side,
            simulated_native_spent=simulated_native_spent,
            tokens_acquired=tokens_acquired,
            effective_price=effective_price,
            price_impact_bps=price_impact_bps,
            simulated_gas_cost_usd=gas,
            fill_latency_ms=latency_result.latency_ms,
            signal_timestamp_ns=signal.timestamp_ns,
            fill_timestamp_ns=latency_result.fill_timestamp_ns,
            kelly_fraction=sizing.capped_fraction,
            portfolio_equity_usd=portfolio_equity_usd,
        )

        logger.info(
            "PaperFill [%s] %s: %s native <-> %s tokens @ %s | impact=%d bps | gas=$%s",
            fill.order_id[:8],
            fill.side.value.upper(),
            fill.simulated_native_spent,
            fill.tokens_acquired,
            fill.effective_price,
            fill.price_impact_bps,
            fill.simulated_gas_cost_usd,
        )
        return fill

    async def execute_exit(
        self,
        chain: ChainIdentifier,
        token_address: str,
        pool: PoolState,
        tokens_to_sell: Decimal,
        reason: TradeExitReason | None = None,
        apply_drag: bool = False,
        portfolio_equity_usd: Decimal = Decimal("10000.0"),
    ) -> PaperFill | None:
        """
        Produce a PaperFill for a dynamic exit SELL order (TP ladder, trailing stop, emergency drain).
        """
        if tokens_to_sell <= 0:
            return None

        if portfolio_equity_usd <= Decimal(0):
            portfolio_equity_usd = Decimal("10000.0")

        now_ns = time.time_ns()
        latency_result = simulate_latency(
            pool=pool,
            signal_timestamp_ns=now_ns,
        )
        adjusted_pool = latency_result.adjusted_pool_state

        try:
            quote = cpmm_sell_quote(adjusted_pool, tokens_to_sell)
        except ValueError as exc:
            logger.warning(
                "CPMM SELL quote failed for exit %s (%s): %s",
                token_address[:10],
                reason.value if reason else "manual",
                exc,
            )
            return None

        tokens_acquired = tokens_to_sell
        simulated_native_spent = quote.amount_out
        effective_price = simulated_native_spent / tokens_acquired
        price_impact_bps = quote.price_impact_bps

        if apply_drag:
            simulated_native_spent, effective_price, _ = apply_paper_trading_drag(
                gross_amount=tokens_to_sell,
                execution_price=effective_price,
                side=OrderSide.SELL,
            )

        gas = gas_cost_usd(chain)
        fill = PaperFill(
            token_address=token_address,
            pool_address=pool.pool_address,
            chain=chain,
            side=OrderSide.SELL,
            simulated_native_spent=simulated_native_spent,
            tokens_acquired=tokens_acquired,
            effective_price=effective_price,
            price_impact_bps=price_impact_bps,
            simulated_gas_cost_usd=gas,
            fill_latency_ms=latency_result.latency_ms,
            signal_timestamp_ns=now_ns,
            fill_timestamp_ns=latency_result.fill_timestamp_ns,
            kelly_fraction=0.0,
            portfolio_equity_usd=portfolio_equity_usd,
        )
        logger.info(
            "Exit PaperFill [%s] %s: %s tokens @ %s native | reason=%s | impact=%d bps",
            fill.order_id[:8],
            token_address[:10],
            tokens_acquired,
            effective_price,
            reason.value if reason else "manual",
            price_impact_bps,
        )
        return fill
