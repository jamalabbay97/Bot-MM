"""
alpha_engine.execution.book — In-Memory Position Book & Running Metrics
======================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import logging
import math
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal

from alpha_engine.models.enums import ChainIdentifier, ExitStage, LotStatus, TradeExitReason
from alpha_engine.models.state import PaperFill

logger = logging.getLogger(__name__)

# Strict Portfolio State Machine & Risk Gating Limits
MAX_ACTIVE_POSITIONS: int = 3
MAX_PORTFOLIO_EXPOSURE_PCT: Decimal = Decimal("0.05")
MAX_PER_TOKEN_RISK_PCT: Decimal = Decimal("0.015")


@dataclass
class OpenLot:
    """
    A single open position lot (one BUY fill not yet matched to a SELL).
    Tracks entry metrics, peak prices, and dynamic exit stage lifecycle.
    """

    lot_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    token_address: str = ""
    chain: ChainIdentifier = ChainIdentifier.BASE_MAINNET
    initial_tokens: Decimal = Decimal(0)
    tokens_held: Decimal = Decimal(0)
    cost_basis_native: Decimal = Decimal(0)
    entry_price: Decimal = Decimal(0)
    signal_id: str = ""
    open_timestamp_ns: int = 0
    peak_price: Decimal = Decimal(0)
    exit_stage: ExitStage = ExitStage.NONE
    trailing_stop_active: bool = False
    trailing_stop_price: Decimal = Decimal(0)
    last_pool_reserve_native: Decimal = Decimal(0)
    status: LotStatus = LotStatus.OPEN
    pending_exit_timestamp: float = 0.0
    scaled_out_50_pct: bool = False
    last_tick_timestamp_s: float = 0.0
    pool_address: str = ""
    bonding_curve_mode: bool = False


@dataclass
class ExitDecision:
    """
    Represents an autonomous exit decision for a position lot.
    """

    lot_id: str
    token_address: str
    chain: ChainIdentifier
    should_exit: bool
    exit_reason: TradeExitReason | None = None
    exit_stage: ExitStage = ExitStage.NONE
    tokens_to_sell: Decimal = Decimal(0)
    current_price: Decimal = Decimal(0)
    pnl_estimate_native: Decimal = Decimal(0)
    prioritized: bool = False


@dataclass
class PositionBook:
    """
    In-memory tracker for open positions across all chains.
    FIFO matching on SELL orders with adaptive dynamic exit lifecycle management.
    """

    _lots: dict[tuple[str, str], list[OpenLot]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def can_open_position(
        self,
        token_address: str,
        chain: ChainIdentifier,
        allocated_cost_native: Decimal,
        total_equity_native: Decimal,
        pool_address: str = "",
    ) -> tuple[bool, str]:
        """
        Enforce Strict Portfolio State Machine & Dynamic Risk Gating:
        1. Atomic concurrency check: duplicate position lot rejection for mint or curve address.
        2. MAX_ACTIVE_POSITIONS = 3 hard cap.
        3. MAX_PER_TOKEN_RISK_PCT = 1.5% max capital per signal.
        4. MAX_PORTFOLIO_EXPOSURE_PCT = 5% max capital across all open lots.
        """
        if self.is_position_open(token_address, chain):
            return False, f"Duplicate position already open for token {token_address[:10]}"

        if pool_address:
            for lots in self._lots.values():
                for lot in lots:
                    if (
                        lot.tokens_held > 0
                        and lot.pool_address
                        and lot.pool_address.lower() == pool_address.lower()
                    ):
                        return False, f"Duplicate position already open for curve {pool_address[:10]}"

        if self.open_position_count() >= MAX_ACTIVE_POSITIONS:
            return (
                False,
                f"Active position count ({self.open_position_count()}) reached MAX_ACTIVE_POSITIONS hard cap ({MAX_ACTIVE_POSITIONS})",
            )

        if total_equity_native > 0:
            if allocated_cost_native > total_equity_native * MAX_PER_TOKEN_RISK_PCT:
                return (
                    False,
                    f"Allocated size ({allocated_cost_native}) exceeds MAX_PER_TOKEN_RISK_PCT (1.5%) of equity ({total_equity_native})",
                )

            current_exposure = Decimal(0)
            for lots in self._lots.values():
                for lot in lots:
                    if lot.tokens_held > 0 and lot.status in (
                        LotStatus.OPEN,
                        LotStatus.PENDING_BUY,
                        LotStatus.PENDING_SELL,
                    ):
                        current_exposure += lot.cost_basis_native

            if (current_exposure + allocated_cost_native) > total_equity_native * MAX_PORTFOLIO_EXPOSURE_PCT:
                return (
                    False,
                    f"Total exposure ({current_exposure + allocated_cost_native}) exceeds MAX_PORTFOLIO_EXPOSURE_PCT (5%) of equity ({total_equity_native})",
                )

        return True, ""

    def open_lot(
        self,
        fill: PaperFill,
        signal_id: str,
        initial_pool_reserve_native: Decimal = Decimal(0),
        pool_address: str = "",
        bonding_curve_mode: bool = False,
    ) -> OpenLot:
        if fill.effective_price <= Decimal(0):
            raise ValueError(
                f"Cannot open lot for {fill.token_address} with non-positive fill price: {fill.effective_price}"
            )
        lot = OpenLot(
            token_address=fill.token_address,
            chain=fill.chain,
            initial_tokens=fill.tokens_acquired,
            tokens_held=fill.tokens_acquired,
            cost_basis_native=fill.simulated_native_spent,
            entry_price=fill.effective_price,
            signal_id=signal_id,
            open_timestamp_ns=fill.fill_timestamp_ns,
            peak_price=fill.effective_price,
            exit_stage=ExitStage.NONE,
            trailing_stop_active=False,
            trailing_stop_price=Decimal(0),
            last_pool_reserve_native=initial_pool_reserve_native,
            status=LotStatus.OPEN,
            pool_address=pool_address or fill.pool_address,
            bonding_curve_mode=bonding_curve_mode,
        )
        key = (fill.chain.value, fill.token_address)
        self._lots[key].append(lot)
        logger.debug(
            "Opened lot %s: %s tokens of %s @ %s native (reserve=%s, status=%s)",
            lot.lot_id[:8],
            lot.tokens_held,
            lot.token_address[:10],
            lot.entry_price,
            lot.last_pool_reserve_native,
            lot.status.value,
        )
        return lot

    def close_lots_fifo(
        self,
        chain: ChainIdentifier,
        token_address: str,
        tokens_to_sell: Decimal,
        sell_price: Decimal,
    ) -> tuple[Decimal, Decimal]:
        key = (chain.value, token_address)
        lots = self._lots.get(key, [])
        remaining = tokens_to_sell
        realized_pnl = Decimal(0)
        matched = Decimal(0)

        i = 0
        while i < len(lots) and remaining > 0:
            lot = lots[i]
            if lot.tokens_held <= remaining:
                pnl = lot.tokens_held * (sell_price - lot.entry_price)
                realized_pnl += pnl
                matched += lot.tokens_held
                remaining -= lot.tokens_held
                lots.pop(i)
                logger.debug(
                    "Closed lot %s fully | PnL=%.8f native",
                    lot.lot_id[:8],
                    float(pnl),
                )
            else:
                pnl = remaining * (sell_price - lot.entry_price)
                realized_pnl += pnl
                matched += remaining
                lot.tokens_held -= remaining
                lot.cost_basis_native -= remaining * lot.entry_price
                remaining = Decimal(0)
                logger.debug(
                    "Partially closed lot %s | PnL=%.8f native",
                    lot.lot_id[:8],
                    float(pnl),
                )

        if remaining > 0:
            logger.warning(
                "SELL exceeded open lots for %s — %s tokens unmatched.",
                token_address[:10],
                remaining,
            )

        return realized_pnl, matched

    def get_holdings(
        self, chain: ChainIdentifier, token_address: str
    ) -> Decimal:
        key = (chain.value, token_address)
        return sum(lot.tokens_held for lot in self._lots.get(key, []))

    def open_position_count(self) -> int:
        return sum(len(lots) for lots in self._lots.values())

    def unrealized_pnl_native(
        self,
        chain: ChainIdentifier,
        token_address: str,
        current_price: Decimal,
    ) -> Decimal:
        key = (chain.value, token_address)
        pnl = Decimal(0)
        for lot in self._lots.get(key, []):
            pnl += lot.tokens_held * (current_price - lot.entry_price)
        return pnl

    def get_open_lots(
        self,
        chain: ChainIdentifier | None = None,
        token_address: str | None = None,
    ) -> list[OpenLot]:
        """Return all matching open lots across chains or for a specific token."""
        result: list[OpenLot] = []
        for (c, t), lots in self._lots.items():
            if chain is not None and c != chain.value:
                continue
            if token_address is not None and t.lower() != token_address.lower():
                continue
            result.extend(lots)
        return result

    def is_position_open(
        self,
        token_address: str,
        chain: ChainIdentifier | None = None,
    ) -> bool:
        """
        Check whether an active open position lot exists for this token.
        Performs case-insensitive token address matching.
        """
        if not token_address:
            return False
        lots = self.get_open_lots(chain=chain, token_address=token_address)
        return any(lot.tokens_held > Decimal(0) for lot in lots)

    def evaluate_lot_exit(
        self,
        lot: OpenLot,
        current_price: Decimal,
        current_pool_reserve_native: Decimal | None = None,
        tick_timestamp_s: float | None = None,
        current_timestamp_ns: int | None = None,
        bonding_curve_mode: bool | None = None,
    ) -> ExitDecision | None:
        """
        Evaluate adaptive dynamic exit rules for an open lot:
        If bonding_curve_mode is True:
        1. Stale-Tick Guard: If tick > 3s old, halt triggers.
        2. Slippage Insolvency Guard: If curve reserve cannot absorb without > 15% price collapse, trigger emergency dump.
        3. Velocity / Time Decay: If duration > 120s and PnL < +3.0%, trigger immediate market exit.
        4. Hard Stop-Loss (-8%): Hard exit, prioritized if impact > 300 bps.
        5. Trailing Stop: Activates at +15%, locks at +8%, trails by 5% from peak.
        6. Take-Profit Ladder: +25% (scale out 50%), +50% (sell remaining 50%).

        If bonding_curve_mode is False (legacy AMM):
        Maintains backward compatibility with legacy 2x/3x/5x/10x TP ladder and +50% -> +20% trailing stop.
        """
        if lot.tokens_held <= 0 or lot.entry_price <= 0:
            return None

        is_bc = bonding_curve_mode if bonding_curve_mode is not None else lot.bonding_curve_mode

        import time
        now_s = time.time()
        now_ns = current_timestamp_ns if current_timestamp_ns is not None else time.time_ns()

        # 1. Stale-Tick Guard (Directive 2)
        if tick_timestamp_s is not None:
            lot.last_tick_timestamp_s = tick_timestamp_s
            if (now_s - tick_timestamp_s) > 3.0:
                logger.warning(
                    "STALE TICK GUARD: Tick for %s is %.2fs old (> 3.0s). Halting exit triggers.",
                    lot.token_address[:10],
                    now_s - tick_timestamp_s,
                )
                return None

        # Track peak price
        if current_price > lot.peak_price:
            lot.peak_price = current_price

        # Check pool reserves
        if current_pool_reserve_native is not None and current_pool_reserve_native > 0:
            # Emergency Liquidity Drain Trigger (> 30% reserve collapse)
            if lot.last_pool_reserve_native > 0 and current_pool_reserve_native < lot.last_pool_reserve_native:
                drain_pct = (
                    lot.last_pool_reserve_native - current_pool_reserve_native
                ) / lot.last_pool_reserve_native
                if drain_pct > Decimal("0.30"):
                    logger.warning(
                        "LIQUIDITY DRAIN DETECTED for lot %s (%s): reserve dropped %.2f%% from %s to %s. Front-running dump 100%%!",
                        lot.lot_id[:8],
                        lot.token_address[:10],
                        float(drain_pct * 100),
                        lot.last_pool_reserve_native,
                        current_pool_reserve_native,
                    )
                    return ExitDecision(
                        lot_id=lot.lot_id,
                        token_address=lot.token_address,
                        chain=lot.chain,
                        should_exit=True,
                        exit_reason=TradeExitReason.EMERGENCY_DRAIN,
                        exit_stage=ExitStage.RUGPULL,
                        tokens_to_sell=lot.tokens_held,
                        current_price=current_price,
                        pnl_estimate_native=lot.tokens_held * (current_price - lot.entry_price),
                        prioritized=True,
                    )

            # Slippage Insolvency Guard (Directive 2)
            if is_bc:
                pos_value = lot.tokens_held * current_price
                if (pos_value / current_pool_reserve_native) > Decimal("0.15"):
                    logger.warning(
                        "SLIPPAGE INSOLVENCY GUARD for lot %s (%s): position value %s is >15%% of curve reserve %s. Triggering emergency market liquidation dump!",
                        lot.lot_id[:8],
                        lot.token_address[:10],
                        pos_value,
                        current_pool_reserve_native,
                    )
                    return ExitDecision(
                        lot_id=lot.lot_id,
                        token_address=lot.token_address,
                        chain=lot.chain,
                        should_exit=True,
                        exit_reason=TradeExitReason.SLIPPAGE_INSOLVENCY,
                        exit_stage=ExitStage.RUGPULL,
                        tokens_to_sell=lot.tokens_held,
                        current_price=current_price,
                        pnl_estimate_native=lot.tokens_held * (current_price - lot.entry_price),
                        prioritized=True,
                    )

            lot.last_pool_reserve_native = current_pool_reserve_native

        gain_ratio = current_price / lot.entry_price

        # --- BONDING-CURVE TUNED EXITS ---
        if is_bc:
            # Velocity / Time Decay Exit (Crucial for Pump.fun):
            duration_s = (now_ns - lot.open_timestamp_ns) / 1e9 if lot.open_timestamp_ns > 0 else 0.0
            pnl_pct = (current_price - lot.entry_price) / lot.entry_price
            if duration_s > 120.0 and pnl_pct < Decimal("0.03"):
                logger.info(
                    "VELOCITY / TIME DECAY EXIT for lot %s (%s): duration=%.1fs (>120s) and PnL=%.2f%% (<+3%%). Liquidating stagnant lot.",
                    lot.lot_id[:8],
                    lot.token_address[:10],
                    duration_s,
                    float(pnl_pct * 100),
                )
                return ExitDecision(
                    lot_id=lot.lot_id,
                    token_address=lot.token_address,
                    chain=lot.chain,
                    should_exit=True,
                    exit_reason=TradeExitReason.TIMEOUT_VELOCITY_DECAY,
                    exit_stage=ExitStage.TIMEOUT,
                    tokens_to_sell=lot.tokens_held,
                    current_price=current_price,
                    pnl_estimate_native=lot.tokens_held * (current_price - lot.entry_price),
                )

            # Trailing Stop:
            # Activate when unrealized profit reaches +15%; lock stop at +8%. Trail by 5% as price makes new highs.
            if gain_ratio >= Decimal("1.15"):
                if not lot.trailing_stop_active:
                    lot.trailing_stop_active = True
                    lot.trailing_stop_price = lot.entry_price * Decimal("1.08")
                    logger.info(
                        "Lot %s (%s) up +15%% (price=%s) -> Locking trailing stop at +8%% (%s)",
                        lot.lot_id[:8],
                        lot.token_address[:10],
                        current_price,
                        lot.trailing_stop_price,
                    )
                trail_price = (lot.peak_price * Decimal("0.95")).quantize(Decimal("1e-18"))
                if trail_price > lot.trailing_stop_price:
                    lot.trailing_stop_price = trail_price

            if lot.trailing_stop_active and current_price < lot.trailing_stop_price:
                logger.info(
                    "TRAILING STOP TRIGGERED for lot %s (%s): price %s < trailing stop %s.",
                    lot.lot_id[:8],
                    lot.token_address[:10],
                    current_price,
                    lot.trailing_stop_price,
                )
                return ExitDecision(
                    lot_id=lot.lot_id,
                    token_address=lot.token_address,
                    chain=lot.chain,
                    should_exit=True,
                    exit_reason=TradeExitReason.SL_TRAILING,
                    exit_stage=ExitStage.TRAILING_SL,
                    tokens_to_sell=lot.tokens_held,
                    current_price=current_price,
                    pnl_estimate_native=lot.tokens_held * (current_price - lot.entry_price),
                )

            # Hard Stop-Loss: -8% hard stop. Calculate expected slippage; if estimated impact > 300 bps, use prioritized exit routing.
            if not lot.trailing_stop_active and gain_ratio <= Decimal("0.92"):
                is_prioritized = False
                if current_pool_reserve_native is not None and current_pool_reserve_native > 0:
                    pos_val = lot.tokens_held * current_price
                    if (pos_val / current_pool_reserve_native) > Decimal("0.03"):
                        is_prioritized = True
                logger.info(
                    "HARD STOP-LOSS (-8%%) TRIGGERED for lot %s (%s): price %s <= entry %s * 0.92 (prioritized=%s).",
                    lot.lot_id[:8],
                    lot.token_address[:10],
                    current_price,
                    lot.entry_price,
                    is_prioritized,
                )
                return ExitDecision(
                    lot_id=lot.lot_id,
                    token_address=lot.token_address,
                    chain=lot.chain,
                    should_exit=True,
                    exit_reason=TradeExitReason.SL_HARD,
                    exit_stage=ExitStage.SL,
                    tokens_to_sell=lot.tokens_held,
                    current_price=current_price,
                    pnl_estimate_native=lot.tokens_held * (current_price - lot.entry_price),
                    prioritized=is_prioritized,
                )

            # Take-Profit (TP): +25% (Scale out 50% of position size), +50% (Sell remaining 50%)
            # +50% Take Profit
            if gain_ratio >= Decimal("1.50") and lot.exit_stage in (ExitStage.NONE, ExitStage.TP1):
                return ExitDecision(
                    lot_id=lot.lot_id,
                    token_address=lot.token_address,
                    chain=lot.chain,
                    should_exit=True,
                    exit_reason=TradeExitReason.TP_50,
                    exit_stage=ExitStage.TP2,
                    tokens_to_sell=lot.tokens_held,
                    current_price=current_price,
                    pnl_estimate_native=lot.tokens_held * (current_price - lot.entry_price),
                )

            # +25% Take Profit (Scale out 50%)
            if gain_ratio >= Decimal("1.25") and not lot.scaled_out_50_pct and lot.exit_stage == ExitStage.NONE:
                target_sell = (lot.initial_tokens * Decimal("0.50")).quantize(Decimal("1e-18"))
                qty = min(lot.tokens_held, target_sell if target_sell > 0 else (lot.tokens_held / Decimal("2")))
                if qty > 0:
                    return ExitDecision(
                        lot_id=lot.lot_id,
                        token_address=lot.token_address,
                        chain=lot.chain,
                        should_exit=True,
                        exit_reason=TradeExitReason.TP_25,
                        exit_stage=ExitStage.TP1,
                        tokens_to_sell=qty,
                        current_price=current_price,
                        pnl_estimate_native=qty * (current_price - lot.entry_price),
                    )

            return None

        # --- LEGACY AMM EXITS (FOR BACKWARD COMPATIBILITY) ---
        # 2. Trailing Stop-Loss Management
        # Once up +50%, lock trailing stop at +20%
        if gain_ratio >= Decimal("1.50"):
            if not lot.trailing_stop_active:
                lot.trailing_stop_active = True
                lot.trailing_stop_price = lot.entry_price * Decimal("1.20")
                logger.info(
                    "Lot %s (%s) up +50%% (price=%.4f native) -> Locking trailing stop-loss at +20%% (stop=%.4f native)",
                    lot.lot_id[:8],
                    lot.token_address[:10],
                    float(current_price),
                    float(lot.trailing_stop_price),
                )

        # Trailing stop triggered
        if lot.trailing_stop_active and current_price < lot.trailing_stop_price:
            logger.info(
                "TRAILING STOP TRIGGERED for lot %s (%s): price %.4f < trailing stop %.4f.",
                lot.lot_id[:8],
                lot.token_address[:10],
                float(current_price),
                float(lot.trailing_stop_price),
            )
            return ExitDecision(
                lot_id=lot.lot_id,
                token_address=lot.token_address,
                chain=lot.chain,
                should_exit=True,
                exit_reason=TradeExitReason.SL_TRAILING,
                exit_stage=ExitStage.TRAILING_SL,
                tokens_to_sell=lot.tokens_held,
                current_price=current_price,
                pnl_estimate_native=lot.tokens_held * (current_price - lot.entry_price),
            )

        # Hard stop-loss (-15% from entry)
        if not lot.trailing_stop_active and gain_ratio <= Decimal("0.85"):
            logger.info(
                "HARD STOP-LOSS (-15%%) TRIGGERED for lot %s (%s): price %.4f <= entry %.4f * 0.85.",
                lot.lot_id[:8],
                lot.token_address[:10],
                float(current_price),
                float(lot.entry_price),
            )
            return ExitDecision(
                lot_id=lot.lot_id,
                token_address=lot.token_address,
                chain=lot.chain,
                should_exit=True,
                exit_reason=TradeExitReason.SL_INITIAL,
                exit_stage=ExitStage.SL,
                tokens_to_sell=lot.tokens_held,
                current_price=current_price,
                pnl_estimate_native=lot.tokens_held * (current_price - lot.entry_price),
            )

        # 3. Take Profit Ladder
        # 10x (+900%): Dump 100% of remaining tokens
        if gain_ratio >= Decimal("10.0") and lot.exit_stage in (
            ExitStage.NONE,
            ExitStage.TP1,
            ExitStage.TP2,
            ExitStage.TP3,
        ):
            return ExitDecision(
                lot_id=lot.lot_id,
                token_address=lot.token_address,
                chain=lot.chain,
                should_exit=True,
                exit_reason=TradeExitReason.TP_10X,
                exit_stage=ExitStage.TP4,
                tokens_to_sell=lot.tokens_held,
                current_price=current_price,
                pnl_estimate_native=lot.tokens_held * (current_price - lot.entry_price),
            )

        # 5x (+400%): Sell 50% of remaining tokens
        if gain_ratio >= Decimal("5.0") and lot.exit_stage in (
            ExitStage.NONE,
            ExitStage.TP1,
            ExitStage.TP2,
        ):
            qty = (lot.tokens_held / Decimal("2")).quantize(Decimal("1e-18"))
            if qty > 0:
                return ExitDecision(
                    lot_id=lot.lot_id,
                    token_address=lot.token_address,
                    chain=lot.chain,
                    should_exit=True,
                    exit_reason=TradeExitReason.TP_5X,
                    exit_stage=ExitStage.TP3,
                    tokens_to_sell=qty,
                    current_price=current_price,
                    pnl_estimate_native=qty * (current_price - lot.entry_price),
                )

        # 3x (+200%): Sell 25% of initial tokens
        if gain_ratio >= Decimal("3.0") and lot.exit_stage in (
            ExitStage.NONE,
            ExitStage.TP1,
        ):
            target_sell = (lot.initial_tokens * Decimal("0.25")).quantize(Decimal("1e-18"))
            qty = min(
                lot.tokens_held,
                target_sell if target_sell > 0 else (lot.tokens_held / Decimal("2")),
            )
            if qty > 0:
                return ExitDecision(
                    lot_id=lot.lot_id,
                    token_address=lot.token_address,
                    chain=lot.chain,
                    should_exit=True,
                    exit_reason=TradeExitReason.TP_3X,
                    exit_stage=ExitStage.TP2,
                    tokens_to_sell=qty,
                    current_price=current_price,
                    pnl_estimate_native=qty * (current_price - lot.entry_price),
                )

        # 2x (+100%): Sell 50% of initial tokens (recouping initial investment)
        if gain_ratio >= Decimal("2.0") and lot.exit_stage == ExitStage.NONE:
            target_sell = (lot.initial_tokens * Decimal("0.50")).quantize(Decimal("1e-18"))
            qty = min(
                lot.tokens_held,
                target_sell if target_sell > 0 else (lot.tokens_held / Decimal("2")),
            )
            if qty > 0:
                return ExitDecision(
                    lot_id=lot.lot_id,
                    token_address=lot.token_address,
                    chain=lot.chain,
                    should_exit=True,
                    exit_reason=TradeExitReason.TP_2X,
                    exit_stage=ExitStage.TP1,
                    tokens_to_sell=qty,
                    current_price=current_price,
                    pnl_estimate_native=qty * (current_price - lot.entry_price),
                )

        return None

    def evaluate_all_exits(
        self,
        current_prices: dict[tuple[str, str], Decimal],
        current_reserves: dict[tuple[str, str], Decimal] | None = None,
        bonding_curve_mode: bool = False,
    ) -> list[ExitDecision]:
        """Evaluate exits for all open lots across all tracked pools/tokens."""
        decisions: list[ExitDecision] = []
        for (c, t), lots in self._lots.items():
            price = current_prices.get((c, t))
            if price is None:
                continue
            reserve = (
                current_reserves.get((c, t))
                if current_reserves is not None
                else None
            )
            for lot in lots:
                decision = self.evaluate_lot_exit(
                    lot=lot,
                    current_price=price,
                    current_pool_reserve_native=reserve,
                    bonding_curve_mode=bonding_curve_mode,
                )
                if decision is not None and decision.should_exit:
                    decisions.append(decision)
        return decisions

    def apply_exit_decision(
        self,
        decision: ExitDecision,
        sell_price: Decimal,
        lot: OpenLot | None = None,
    ) -> tuple[Decimal, Decimal]:
        """
        Apply an ExitDecision to its corresponding lot.
        Returns: (realized_pnl_native, matched_tokens_sold)
        """
        key = (decision.chain.value, decision.token_address)
        lots = self._lots.get(key, [])
        target_lot: OpenLot | None = lot
        if target_lot is None:
            for l in lots:
                if l.lot_id == decision.lot_id:
                    target_lot = l
                    break
        if target_lot is not None:
            qty_to_sell = min(target_lot.tokens_held, decision.tokens_to_sell)
            pnl = qty_to_sell * (sell_price - target_lot.entry_price)
            target_lot.tokens_held -= qty_to_sell
            target_lot.cost_basis_native -= qty_to_sell * target_lot.entry_price
            target_lot.exit_stage = decision.exit_stage
            if decision.exit_reason == TradeExitReason.TP_25:
                target_lot.scaled_out_50_pct = True
            if target_lot.tokens_held <= Decimal("1e-18"):
                target_lot.status = LotStatus.CLOSED
                if target_lot in lots:
                    lots.remove(target_lot)
            logger.debug(
                "Applied exit decision [%s] on lot %s: sold %s @ %s | PnL=%.8f native | remaining=%s | status=%s",
                decision.exit_reason.value if decision.exit_reason else "custom",
                target_lot.lot_id[:8],
                qty_to_sell,
                sell_price,
                float(pnl),
                target_lot.tokens_held,
                target_lot.status.value,
            )
            return pnl, qty_to_sell
        return Decimal(0), Decimal(0)


@dataclass
class RunningMetrics:
    """
    In-memory running metrics updated after every trade.
    Enforces Daily Drawdown and Loss Streak Circuit Breakers.
    """

    peak_equity_usd: Decimal = field(default_factory=lambda: Decimal(0))
    max_drawdown_pct: float = 0.0
    gross_profit_usd: Decimal = field(default_factory=lambda: Decimal(0))
    gross_loss_usd: Decimal = field(default_factory=lambda: Decimal(0))
    total_trades: int = 0
    winning_trades: int = 0
    total_gas_usd: Decimal = field(default_factory=lambda: Decimal(0))
    cumulative_realized_usd: Decimal = field(default_factory=lambda: Decimal(0))

    # Circuit Breaker & Streak Tracking (Directive 1)
    consecutive_losses: int = 0
    daily_peak_equity_usd: Decimal = field(default_factory=lambda: Decimal(0))
    daily_start_equity_usd: Decimal = field(default_factory=lambda: Decimal(0))
    daily_drawdown_pct: float = 0.0
    circuit_breaker_active: bool = False
    circuit_breaker_tripped_at: float = 0.0
    circuit_breaker_reason: str = ""

    def is_circuit_breaker_active(self, freeze_duration_s: float = 3600.0) -> bool:
        """Check if circuit breaker is actively freezing BUY executions."""
        if not self.circuit_breaker_active:
            return False
        import time
        if time.time() - self.circuit_breaker_tripped_at > freeze_duration_s:
            self.circuit_breaker_active = False
            self.circuit_breaker_reason = ""
            logger.info("Circuit breaker freeze period elapsed (60 min). Resuming normal execution.")
            return False
        return True

    def trip_circuit_breaker(self, reason: str) -> None:
        """Trip circuit breaker flag and log critical alert."""
        import time
        self.circuit_breaker_active = True
        self.circuit_breaker_tripped_at = time.time()
        self.circuit_breaker_reason = reason
        logger.critical(
            "CIRCUIT_BREAKER_ACTIVE TRIPPED: %s. Freezing all BUY executions for 60 minutes.",
            reason,
        )

    def record_closed_trade(
        self,
        realized_pnl_usd: Decimal,
        gas_cost_usd: Decimal,
        current_equity_usd: Decimal,
    ) -> None:
        net_pnl = realized_pnl_usd - gas_cost_usd
        self.cumulative_realized_usd += net_pnl
        self.total_gas_usd += gas_cost_usd
        self.total_trades += 1

        if realized_pnl_usd > 0:
            self.gross_profit_usd += realized_pnl_usd
            self.winning_trades += 1
            self.consecutive_losses = 0
        elif realized_pnl_usd < 0:
            self.gross_loss_usd += abs(realized_pnl_usd)
            self.consecutive_losses += 1

        if current_equity_usd > self.peak_equity_usd:
            self.peak_equity_usd = current_equity_usd

        if self.daily_start_equity_usd == 0:
            self.daily_start_equity_usd = current_equity_usd

        if current_equity_usd > self.daily_peak_equity_usd:
            self.daily_peak_equity_usd = current_equity_usd

        if self.peak_equity_usd > 0:
            drawdown = float(
                (self.peak_equity_usd - current_equity_usd)
                / self.peak_equity_usd
                * Decimal("100")
            )
            self.max_drawdown_pct = max(self.max_drawdown_pct, drawdown)

        if self.daily_peak_equity_usd > 0:
            daily_dd = float(
                (self.daily_peak_equity_usd - current_equity_usd)
                / self.daily_peak_equity_usd
                * Decimal("100")
            )
            self.daily_drawdown_pct = max(self.daily_drawdown_pct, daily_dd)

        # Trip circuit breaker if consecutive_losses >= 3 OR daily_drawdown >= 8.0%
        if self.consecutive_losses >= 3:
            self.trip_circuit_breaker(f"Consecutive loss streak reached {self.consecutive_losses}")
        elif self.daily_drawdown_pct >= 8.0:
            self.trip_circuit_breaker(f"Daily drawdown reached {self.daily_drawdown_pct:.2f}% (>= 8.0%)")

    @property
    def win_rate_pct(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return (self.winning_trades / self.total_trades) * 100.0

    @property
    def profit_factor(self) -> float:
        if self.gross_loss_usd == 0:
            return math.inf if self.gross_profit_usd > 0 else 0.0
        return float(self.gross_profit_usd / self.gross_loss_usd)
