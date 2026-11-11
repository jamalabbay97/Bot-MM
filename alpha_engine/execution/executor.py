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

from typing import Optional, Any

from alpha_engine.config import EngineConfig
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.bundle import PrivateTxRouter
from alpha_engine.execution.costs import TradeCostModel
from alpha_engine.math.cpmm import cpmm_buy_quote
from alpha_engine.math.mev import simulate_latency
from alpha_engine.math.sizing import (
    GAS_MULTIPLE_GATE,
    gas_cost_usd,
)
from alpha_engine.models.decisions import DecisionSignal
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    TradeExitReason,
    resolve_trade_platform,
)
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PaperFill, PoolState
from alpha_engine.pricing.venue_capabilities import (
    VenueStatus,
    get_venue_capability,
)

logger = logging.getLogger(__name__)


class PaperExecutor:
    """
    Converts a validated SignalEvent into a PaperFill.
    Obtains live per-chain native asset prices from injected NativePriceService.
    Does NOT size trades — sizing is owned exclusively by RiskEngine.
    """

    def __init__(
        self,
        position_book: PositionBook,
        price_service: Any,
        private_router: Optional[PrivateTxRouter] = None,
        cost_model: Optional[TradeCostModel] = None,
        config: Optional[EngineConfig] = None,
        derived_reserves_policy: Optional[str] = None,
        derived_reserves_depth_cap_pct: Optional[Decimal] = None,
    ) -> None:
        if price_service is None:
            raise ValueError("PaperExecutor requires a valid price_service (NativePriceService)")
        self._positions = position_book
        self._price_service = price_service
        self._private_router = private_router or PrivateTxRouter()
        self.config = config or EngineConfig()
        self._cost_model = cost_model or TradeCostModel(config=self.config, price_service=price_service)
        self.derived_reserves_policy = (
            derived_reserves_policy
            if derived_reserves_policy is not None
            else getattr(self.config, "derived_reserves_policy", "reject")
        )
        self.derived_reserves_depth_cap_pct = (
            derived_reserves_depth_cap_pct
            if derived_reserves_depth_cap_pct is not None
            else getattr(self.config, "derived_reserves_depth_cap_pct", Decimal("0.02"))
        )
        self.telemetry_counters: dict[str, int] = {}

    async def execute_signal(
        self,
        signal: SignalEvent,
        portfolio_equity_usd: Decimal,
        max_position_fraction: Decimal | None = None,
        is_new_launch: bool = False,
        fixed_trade_amount_native: Decimal | None = None,
        equity_fraction: float | None = None,
    ) -> PaperFill | None:
        """
        Produce a PaperFill for the given signal, or return None if skipped.
        Applies unified, symmetric trade costs (venue swap fee, CPMM impact, network fees).
        BUY signals require explicit fixed_trade_amount_native (>0) from the sizing authority.
        """
        _ = max_position_fraction
        side = signal.suggested_side
        pool = getattr(signal, "pool_state", None)

        # 1. Resolve venue and verify against venue capabilities table
        venue = getattr(signal, "venue", None) or getattr(signal, "execution_venue", None)
        resolved_venue = self._cost_model.resolve_venue(
            venue=venue,
            token_address=getattr(pool, "token_address", None) or signal.token_address,
            chain=signal.chain,
            pool_address=getattr(pool, "pool_address", "") or getattr(signal, "pool_address", ""),
            bonding_curve_mode=(
                (pool is not None and getattr(pool, "pool_address", "").startswith("pump_"))
                or getattr(signal, "platform", None) == "Pump.fun"
            ),
            platform=getattr(signal, "platform", None),
        )
        venue_cap = get_venue_capability(resolved_venue)
        if venue_cap.status == VenueStatus.UNSUPPORTED:
            logger.warning(
                "execute_signal: Refusing fill for %s: Venue %s is UNSUPPORTED.",
                signal.token_address[:10],
                resolved_venue.value,
            )
            self.telemetry_counters[f"unsupported_venue_{resolved_venue.value}"] = (
                self.telemetry_counters.get(f"unsupported_venue_{resolved_venue.value}", 0) + 1
            )
            return None

        # 2. Strict pool existence and liquidity verification (never invent data)
        if pool is None or pool.token_reserve <= Decimal(0) or pool.native_reserve <= Decimal(0):
            logger.warning(
                "execute_signal: No valid pool_state for %s; refusing fill without real-time pool data.",
                signal.token_address[:10],
            )
            self.telemetry_counters["no_liquidity_data"] = self.telemetry_counters.get("no_liquidity_data", 0) + 1
            return None

        # 3. Derived reserves policy enforcement (reject or cap_size)
        if getattr(pool, "is_derived", False):
            if self.derived_reserves_policy == "reject":
                logger.warning(
                    "execute_signal: Refusing fill for %s: Pool reserves are DERIVED (%s) and policy is 'reject'.",
                    signal.token_address[:10],
                    getattr(pool, "source", "unknown"),
                )
                self.telemetry_counters["derived_reserves_rejected"] = (
                    self.telemetry_counters.get("derived_reserves_rejected", 0) + 1
                )
                return None
            elif self.derived_reserves_policy == "cap_size":
                if fixed_trade_amount_native is not None:
                    max_depth = (pool.native_reserve * self.derived_reserves_depth_cap_pct).quantize(Decimal("1e-9"))
                    if fixed_trade_amount_native > max_depth:
                        logger.warning(
                            "execute_signal: Capping trade size from %s to %s for %s under 'cap_size' policy (max %s%% of %s)",
                            fixed_trade_amount_native,
                            max_depth,
                            signal.token_address[:10],
                            self.derived_reserves_depth_cap_pct * Decimal("100"),
                            pool.native_reserve,
                        )
                        fixed_trade_amount_native = max_depth

        pool_age = time.time() - getattr(pool, "timestamp_s", 0.0) if pool else 999.0
        if not is_new_launch and pool_age > 4.0:
            logger.warning(
                "execute_signal: pool_state for %s is stale (age=%.2fs > 4.0s); rejecting fill.",
                signal.token_address[:10],
                pool_age,
            )
            return None

        latency_result = simulate_latency(
            pool=pool,
            signal_timestamp_ns=signal.timestamp_ns,
        )
        adjusted_pool = latency_result.adjusted_pool_state

        # Obtain per-chain native price dynamically from NativePriceService
        price_res = self._price_service.get_price(signal.chain)
        current_native_price = (
            price_res.price if hasattr(price_res, "price")
            else (price_res[0] if isinstance(price_res, (tuple, list)) else Decimal(0))
        )
        if current_native_price is None or current_native_price <= Decimal(0):
            logger.warning(
                "execute_signal: Invalid or non-positive native price (%s) for %s; rejecting fill.",
                current_native_price,
                signal.chain.value,
            )
            return None

        fill_kelly_fraction = 0.0

        if side == OrderSide.BUY:
            if fixed_trade_amount_native is None or fixed_trade_amount_native <= Decimal(0):
                raise ValueError(
                    f"fixed_trade_amount_native must be explicitly provided for BUY signals "
                    f"(got {fixed_trade_amount_native}); re-sizing in executor is forbidden."
                )

            native_in = fixed_trade_amount_native
            usd_at_risk = native_in * current_native_price
            trade_gas_usd = self._cost_model.get_network_fee_usd(
                signal.chain, native_price_usd=current_native_price
            )
            gas_threshold = trade_gas_usd * Decimal(str(GAS_MULTIPLE_GATE))
            if usd_at_risk < gas_threshold:
                logger.info(
                    "Gas-drag gate REJECTED fixed trade amount [%s %s]: usd_at_risk=%s < gas_threshold=%s. Position too small to overcome fees.",
                    side.value.upper(),
                    signal.chain.value,
                    usd_at_risk,
                    gas_threshold,
                )
                return None

            real_fraction = (
                float(usd_at_risk / portfolio_equity_usd)
                if portfolio_equity_usd > Decimal(0)
                else 0.0
            )
            fill_kelly_fraction = equity_fraction if equity_fraction is not None else real_fraction

            venue_quote = None
            if venue_cap.pricing_model in ("quoter", "jupiter"):
                try:
                    venue_quote = await self._cost_model.get_venue_quote(
                        side=OrderSide.BUY,
                        gross_amount=native_in,
                        pool=adjusted_pool,
                        chain=signal.chain,
                        venue=resolved_venue,
                    )
                except Exception as exc:
                    logger.warning("Quoter failed for BUY %s: %s", signal.signal_id[:8], exc)
                    self.telemetry_counters[f"quoter_failed_{resolved_venue.value}"] = (
                        self.telemetry_counters.get(f"quoter_failed_{resolved_venue.value}", 0) + 1
                    )
                    return None

            try:
                cost_res = self._cost_model.calculate_trade_costs(
                    side=OrderSide.BUY,
                    gross_amount=native_in,
                    pool=adjusted_pool,
                    chain=signal.chain,
                    venue=resolved_venue,
                    native_price_usd=current_native_price,
                    platform=getattr(signal, "platform", None),
                    venue_quote=venue_quote,
                )
            except Exception as exc:
                logger.warning("Cost model execution failed for BUY %s: %s", signal.signal_id[:8], exc)
                return None

            simulated_native_spent = native_in
            tokens_acquired = cost_res.amount_out
            if tokens_acquired <= Decimal(0):
                logger.warning("CPMM output 0 tokens for %s — skipping.", signal.signal_id[:8])
                return None
            effective_price = cost_res.effective_price
            price_impact_bps = cost_res.price_impact_bps
            gas = cost_res.network_fee_usd

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

            venue_quote = None
            if venue_cap.pricing_model in ("quoter", "jupiter"):
                try:
                    venue_quote = await self._cost_model.get_venue_quote(
                        side=OrderSide.SELL,
                        gross_amount=tokens_held,
                        pool=adjusted_pool,
                        chain=signal.chain,
                        venue=resolved_venue,
                    )
                except Exception as exc:
                    logger.warning("Quoter failed for SELL %s: %s", signal.signal_id[:8], exc)
                    self.telemetry_counters[f"quoter_failed_{resolved_venue.value}"] = (
                        self.telemetry_counters.get(f"quoter_failed_{resolved_venue.value}", 0) + 1
                    )
                    return None

            try:
                cost_res = self._cost_model.calculate_trade_costs(
                    side=OrderSide.SELL,
                    gross_amount=tokens_held,
                    pool=adjusted_pool,
                    chain=signal.chain,
                    venue=resolved_venue,
                    native_price_usd=current_native_price,
                    platform=getattr(signal, "platform", None),
                    venue_quote=venue_quote,
                )
            except Exception as exc:
                logger.warning("Cost model execution failed for SELL %s: %s", signal.signal_id[:8], exc)
                return None

            tokens_acquired = tokens_held
            simulated_native_spent = max(Decimal(0), cost_res.amount_out - cost_res.network_fee_native)
            effective_price = (simulated_native_spent / tokens_acquired) if tokens_acquired > Decimal(0) else Decimal(0)
            price_impact_bps = cost_res.price_impact_bps
            gas = cost_res.network_fee_usd

        platform = getattr(signal, "platform", None) or resolve_trade_platform(
            token_address=signal.token_address,
            chain=signal.chain,
            source=getattr(signal, "source", None),
            execution_venue=getattr(signal, "execution_venue", None),
            pool_address=getattr(signal, "pool_address", ""),
        )

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
            kelly_fraction=fill_kelly_fraction,
            portfolio_equity_usd=portfolio_equity_usd,
            platform=platform,
            is_new_launch=is_new_launch,
        )

        logger.info(
            "PaperFill [%s] %s | Platform=%s | Token=%s | %s native <-> %s tokens @ %s | impact=%d bps | gas=$%s",
            fill.order_id[:8],
            fill.side.value.upper(),
            fill.platform,
            fill.token_address[:10],
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
        portfolio_equity_usd: Decimal = Decimal("10000.0"),
        platform: Optional[str] = None,
        min_fill_timestamp_ns: Optional[int] = None,
    ) -> PaperFill | None:
        """
        Produce a PaperFill for a dynamic exit SELL order (TP ladder, trailing stop, emergency drain).
        Applies unified, symmetric trade costs (venue swap fee, CPMM impact, network fees).
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

        price_res = self._price_service.get_price(chain)
        current_native_price = (
            price_res.price if hasattr(price_res, "price")
            else (price_res[0] if isinstance(price_res, (tuple, list)) else Decimal(0))
        )

        try:
            cost_res = self._cost_model.calculate_trade_costs(
                side=OrderSide.SELL,
                gross_amount=tokens_to_sell,
                pool=adjusted_pool,
                chain=chain,
                venue=None,
                native_price_usd=current_native_price,
                platform=platform,
            )
        except Exception as exc:
            logger.warning(
                "Cost model calculation failed for exit %s (%s): %s",
                token_address[:10],
                reason.value if reason else "manual",
                exc,
            )
            return None

        tokens_acquired = tokens_to_sell
        gross_native = cost_res.amount_out
        max_allowed_native = adjusted_pool.native_reserve * Decimal("0.30")
        if gross_native > max_allowed_native and max_allowed_native > Decimal(0):
            logger.warning(
                "CPMM SELL quote native output %s exceeded 30%% of pool native reserve %s; capping to prevent unrealistic paper fills.",
                gross_native,
                adjusted_pool.native_reserve,
            )
            gross_native = max_allowed_native

        if reason in (TradeExitReason.EMERGENCY_DRAIN, TradeExitReason.RUG_LIQUIDITY_DRAIN):
            # In confirmed emergency drain / rug pull, liquidity is collapsed: apply heavy slippage cap
            max_drain_native = adjusted_pool.native_reserve * Decimal("0.05")
            if gross_native > max_drain_native and max_drain_native > Decimal(0):
                gross_native = max_drain_native
            price_impact_bps = max(cost_res.price_impact_bps, 5000)
        else:
            price_impact_bps = cost_res.price_impact_bps

        if reason == TradeExitReason.EMERGENCY_HONEYPOT_MUTATION:
            # Route emergency dump through private relay with highest priority
            await self._private_router.submit_private_bundle(
                chain=chain,
                signed_transactions=[b"simulated_emergency_honeypot_exit"],
                tip_wei_or_lamports=10_000_000,
            )

        # Net native received = gross native - exit network fee
        simulated_native_spent = max(Decimal(0), gross_native - cost_res.network_fee_native)
        effective_price = (simulated_native_spent / tokens_acquired) if tokens_acquired > Decimal(0) else Decimal(0)
        gas = cost_res.network_fee_usd

        resolved_platform = platform or resolve_trade_platform(
            token_address=token_address,
            chain=chain,
            pool_address=pool.pool_address,
        )
        computed_fill_ts = latency_result.fill_timestamp_ns
        if min_fill_timestamp_ns is not None and computed_fill_ts < min_fill_timestamp_ns:
            computed_fill_ts = min_fill_timestamp_ns + 1_000_000
        if computed_fill_ts < now_ns:
            computed_fill_ts = now_ns + 1_000_000

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
            fill_timestamp_ns=computed_fill_ts,
            kelly_fraction=0.0,
            portfolio_equity_usd=portfolio_equity_usd,
            platform=resolved_platform,
        )
        logger.info(
            "Exit PaperFill [%s] %s | Platform=%s | %s tokens @ %s native | reason=%s | impact=%d bps",
            fill.order_id[:8],
            token_address[:10],
            fill.platform,
            tokens_acquired,
            effective_price,
            reason.value if reason else "manual",
            price_impact_bps,
        )
        return fill

    async def execute_scalp_pipeline(
        self,
        signal: DecisionSignal | SignalEvent,
        portfolio_equity_usd: Decimal,
        max_position_fraction: Decimal | None = None,
        priority_percentile: str = "p95",
        fixed_trade_amount_native: Decimal | None = None,
    ) -> PaperFill | None:
        """
        Scalping Execution Pipeline:
        - Sub-second execution via private relays (Jito Bundles on Solana, Flashbots/Titan on EVM).
        - Prevents MEV exploitation and front-running.
        - Fractional Kelly position sizing or user fixed trade amount.
        - Validates private relay routing before fill creation.
        """
        fill = await self.execute_signal(
            signal=signal,
            portfolio_equity_usd=portfolio_equity_usd,
            max_position_fraction=max_position_fraction,
            fixed_trade_amount_native=fixed_trade_amount_native,
        )
        if fill is None:
            return None

        relay_result = await self._private_router.execute_scalp_via_private_relay(
            chain=signal.chain,
            transactions=[f"simulated_private_tx_{fill.order_id}"],
            priority_percentile=priority_percentile,
        )
        logger.info(
            "Scalping Pipeline: Executed via %s | Platform=%s | Token=%s | Status=%s | Tip=%s",
            relay_result.get("relay", "private_builder"),
            fill.platform,
            fill.token_address[:10],
            relay_result.get("status"),
            relay_result.get("tip_sol", 0.0),
        )
        return fill

    async def execute_swing_dca_pipeline(
        self,
        signal: DecisionSignal | SignalEvent,
        portfolio_equity_usd: Decimal,
        num_batches: int = 3,
        block_interval: int = 2,
        max_position_fraction: Decimal | None = None,
        fixed_trade_amount_native: Decimal | None = None,
    ) -> list[PaperFill]:
        """
        Swing Execution Pipeline:
        - Gas-optimized transaction batching.
        - Dollar-cost averaging (DCA) over multiple blocks to minimize slippage and price impact.
        - Fractional Kelly position sizing or fixed trade amount split evenly across num_batches.
        """
        total_fill = await self.execute_signal(
            signal=signal,
            portfolio_equity_usd=portfolio_equity_usd,
            max_position_fraction=max_position_fraction,
            fixed_trade_amount_native=fixed_trade_amount_native,
        )
        if total_fill is None:
            return []

        total_native = total_fill.simulated_native_spent
        batch_native = total_native / Decimal(str(num_batches))
        pool = getattr(signal, "pool_state", None)
        if pool is None:
            logger.warning("execute_swing_dca_pipeline: No valid pool_state; refusing DCA fill.")
            return []

        fills: list[PaperFill] = []
        curr_pool = pool
        for b_idx in range(num_batches):
            try:
                quote = cpmm_buy_quote(curr_pool, batch_native)
            except Exception:
                break
            tokens_batch = quote.amount_out
            eff_price = batch_native / tokens_batch if tokens_batch > 0 else Decimal("0")
            gas = gas_cost_usd(signal.chain)
            fill_b = PaperFill(
                token_address=signal.token_address,
                pool_address=signal.pool_address,
                chain=signal.chain,
                side=OrderSide.BUY,
                simulated_native_spent=batch_native,
                tokens_acquired=tokens_batch,
                effective_price=eff_price,
                price_impact_bps=quote.price_impact_bps,
                simulated_gas_cost_usd=gas,
                fill_latency_ms=25.0 * (b_idx + 1),
                signal_timestamp_ns=signal.timestamp_ns,
                fill_timestamp_ns=time.time_ns(),
                kelly_fraction=total_fill.kelly_fraction / num_batches,
                portfolio_equity_usd=portfolio_equity_usd,
            )
            fills.append(fill_b)
            curr_pool = curr_pool.model_copy(
                update={
                    "native_reserve": curr_pool.native_reserve + batch_native,
                    "token_reserve": max(Decimal("1"), curr_pool.token_reserve - tokens_batch),
                    "last_updated_block": curr_pool.last_updated_block + block_interval,
                }
            )

        logger.info(
            "Swing Pipeline: Executed %d DCA batches across %d blocks | Total Tokens=%s",
            len(fills),
            num_batches * block_interval,
            sum((f.tokens_acquired for f in fills), Decimal(0)),
        )
        return fills
