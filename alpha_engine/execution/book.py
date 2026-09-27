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

from alpha_engine.models.enums import ChainIdentifier, ExitStage, TradeExitReason
from alpha_engine.models.state import PaperFill

logger = logging.getLogger(__name__)


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


@dataclass
class PositionBook:
    """
    In-memory tracker for open positions across all chains.
    FIFO matching on SELL orders with adaptive dynamic exit lifecycle management.
    """

    _lots: dict[tuple[str, str], list[OpenLot]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def open_lot(
        self,
        fill: PaperFill,
        signal_id: str,
        initial_pool_reserve_native: Decimal = Decimal(0),
    ) -> OpenLot:
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
        )
        key = (fill.chain.value, fill.token_address)
        self._lots[key].append(lot)
        logger.debug(
            "Opened lot %s: %s tokens of %s @ %s native (reserve=%s)",
            lot.lot_id[:8],
            lot.tokens_held,
            lot.token_address[:10],
            lot.entry_price,
            lot.last_pool_reserve_native,
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

    def evaluate_lot_exit(
        self,
        lot: OpenLot,
        current_price: Decimal,
        current_pool_reserve_native: Decimal | None = None,
    ) -> ExitDecision | None:
        """
        Evaluate adaptive dynamic exit rules for an open lot:
        1. Liquidity Drain: If pool reserves drop > 30% in a single block/update, front-run dump 100%.
        2. Trailing Stop-Loss: Once up +50%, lock trailing stop at +20%. If drops below +20%, exit 100%.
        3. Hard Stop-Loss: If position drops -15% from entry (and trailing stop not yet active), exit 100%.
        4. Take Profit Ladder:
           - 2x (+100%): Sell 50% of initial bag (recoup initial investment).
           - 3x (+200%): Sell 25% of initial bag.
           - 5x (+400%): Sell 50% of remaining bag.
           - 10x (+900%): Dump 100% of remaining bag.
        """
        if lot.tokens_held <= 0 or lot.entry_price <= 0:
            return None

        # Track peak price
        if current_price > lot.peak_price:
            lot.peak_price = current_price

        # 1. Emergency Liquidity Drain Trigger (Highest priority)
        if lot.last_pool_reserve_native > 0 and current_pool_reserve_native is not None:
            if current_pool_reserve_native < lot.last_pool_reserve_native:
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
                    )

        # Update last observed reserve
        if current_pool_reserve_native is not None and current_pool_reserve_native > 0:
            lot.last_pool_reserve_native = current_pool_reserve_native

        gain_ratio = current_price / lot.entry_price

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
                )
                if decision is not None and decision.should_exit:
                    decisions.append(decision)
        return decisions

    def apply_exit_decision(
        self,
        decision: ExitDecision,
        sell_price: Decimal,
    ) -> tuple[Decimal, Decimal]:
        """
        Apply an ExitDecision to its corresponding lot.
        Returns: (realized_pnl_native, matched_tokens_sold)
        """
        key = (decision.chain.value, decision.token_address)
        lots = self._lots.get(key, [])
        for i, lot in enumerate(lots):
            if lot.lot_id == decision.lot_id:
                qty_to_sell = min(lot.tokens_held, decision.tokens_to_sell)
                pnl = qty_to_sell * (sell_price - lot.entry_price)
                lot.tokens_held -= qty_to_sell
                lot.cost_basis_native -= qty_to_sell * lot.entry_price
                lot.exit_stage = decision.exit_stage
                if lot.tokens_held <= Decimal("1e-18"):
                    lots.pop(i)
                logger.debug(
                    "Applied exit decision [%s] on lot %s: sold %s @ %s | PnL=%.8f native | remaining=%s",
                    decision.exit_reason.value if decision.exit_reason else "custom",
                    lot.lot_id[:8],
                    qty_to_sell,
                    sell_price,
                    float(pnl),
                    lot.tokens_held,
                )
                return pnl, qty_to_sell
        return Decimal(0), Decimal(0)


@dataclass
class RunningMetrics:
    """
    In-memory running metrics updated after every trade.
    """

    peak_equity_usd: Decimal = field(default_factory=lambda: Decimal(0))
    max_drawdown_pct: float = 0.0
    gross_profit_usd: Decimal = field(default_factory=lambda: Decimal(0))
    gross_loss_usd: Decimal = field(default_factory=lambda: Decimal(0))
    total_trades: int = 0
    winning_trades: int = 0
    total_gas_usd: Decimal = field(default_factory=lambda: Decimal(0))
    cumulative_realized_usd: Decimal = field(default_factory=lambda: Decimal(0))

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
        elif realized_pnl_usd < 0:
            self.gross_loss_usd += abs(realized_pnl_usd)

        if current_equity_usd > self.peak_equity_usd:
            self.peak_equity_usd = current_equity_usd

        if self.peak_equity_usd > 0:
            drawdown = float(
                (self.peak_equity_usd - current_equity_usd)
                / self.peak_equity_usd
                * 100
            )
            self.max_drawdown_pct = max(self.max_drawdown_pct, drawdown)

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
