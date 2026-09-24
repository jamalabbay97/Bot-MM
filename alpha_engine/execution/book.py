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

from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.models.state import PaperFill

logger = logging.getLogger(__name__)


@dataclass
class OpenLot:
    """
    A single open position lot (one BUY fill not yet matched to a SELL).
    """

    lot_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    token_address: str = ""
    chain: ChainIdentifier = ChainIdentifier.BASE_MAINNET
    tokens_held: Decimal = Decimal(0)
    cost_basis_native: Decimal = Decimal(0)
    entry_price: Decimal = Decimal(0)
    signal_id: str = ""
    open_timestamp_ns: int = 0


@dataclass
class PositionBook:
    """
    In-memory tracker for open positions across all chains.
    FIFO matching on SELL orders.
    """

    _lots: dict[tuple[str, str], list[OpenLot]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def open_lot(self, fill: PaperFill, signal_id: str) -> OpenLot:
        lot = OpenLot(
            token_address=fill.token_address,
            chain=fill.chain,
            tokens_held=fill.tokens_acquired,
            cost_basis_native=fill.simulated_native_spent,
            entry_price=fill.effective_price,
            signal_id=signal_id,
            open_timestamp_ns=fill.fill_timestamp_ns,
        )
        key = (fill.chain.value, fill.token_address)
        self._lots[key].append(lot)
        logger.debug(
            "Opened lot %s: %s tokens of %s @ %s native",
            lot.lot_id[:8],
            lot.tokens_held,
            lot.token_address[:10],
            lot.entry_price,
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
