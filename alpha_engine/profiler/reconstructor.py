"""
alpha_engine.profiler.reconstructor — Trade Reconstructor & FIFO Lot Matching
=============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Exact Decimal Math | FIFO Matching
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Sequence

from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.models.profiler import (
    NormalizedSwap,
    OpenPositionRecord,
    TradeReconstructionResult,
    WalletTradeRecord,
)
from alpha_engine.profiler.price_oracle import HistoricalPriceOracle

logger = logging.getLogger(__name__)


@dataclass
class _Lot:
    token_address: str
    amount_remaining: Decimal
    total_amount: Decimal
    cost_native: Decimal
    buy_tx_hash: str
    buy_timestamp: int
    entry_native_price_usd: Decimal
    entry_fee_native: Decimal
    venue: str
    is_external_transfer: bool = False


class TradeReconstructor:
    """
    Converts raw normalized swap events into closed round-trip WalletTradeRecord objects.
    Enforces:
      - FIFO lot matching per token
      - Historical native prices (DefiLlama / Price Oracle) for entry/exit USD conversion
      - Subtracting base & priority fees from realized PnL
      - Handling partial fills / partial sells
      - Explicit handling of transfers (never counted as sells)
      - Strict separation of open positions from closed trades & win rate
    """

    def __init__(self, price_oracle: HistoricalPriceOracle) -> None:
        self.price_oracle = price_oracle

    async def reconstruct_trades(
        self,
        swaps: Sequence[NormalizedSwap],
        chain: ChainIdentifier,
        wallet_address: str,
        max_closed_trades: Optional[int] = None,
    ) -> TradeReconstructionResult:
        """
        Execute deterministic FIFO lot matching across all historical swaps.
        Returns closed trades, open positions, and realized metrics.
        """
        sorted_swaps = sorted(swaps, key=lambda s: (s.block_time, s.tx_hash))
        open_lots: dict[str, list[_Lot]] = defaultdict(list)
        closed_trades: list[WalletTradeRecord] = []
        unmatched_sells = 0

        for swap in sorted_swaps:
            token_addr = swap.token_address

            # -------------------------------------------------------------
            # Case 1: Pure Transfer Events (Incoming or Outgoing)
            # -------------------------------------------------------------
            if swap.is_transfer:
                if swap.is_buy:
                    # Tokens received via external transfer (deposit)
                    open_lots[token_addr].append(
                        _Lot(
                            token_address=token_addr,
                            amount_remaining=swap.token_amount,
                            total_amount=swap.token_amount,
                            cost_native=Decimal(0),
                            buy_tx_hash=swap.tx_hash,
                            buy_timestamp=swap.block_time,
                            entry_native_price_usd=Decimal(0),
                            entry_fee_native=swap.fee_native,
                            venue="transfer",
                            is_external_transfer=True,
                        )
                    )
                else:
                    # Tokens transferred out of wallet: inventory removal, NOT a sell
                    remaining_to_remove = swap.token_amount
                    lots = open_lots.get(token_addr, [])
                    while lots and remaining_to_remove > Decimal(0):
                        lot = lots[0]
                        if remaining_to_remove >= lot.amount_remaining:
                            remaining_to_remove -= lot.amount_remaining
                            lots.pop(0)
                        else:
                            lot.amount_remaining -= remaining_to_remove
                            remaining_to_remove = Decimal(0)
                continue

            # -------------------------------------------------------------
            # Case 2: Market Buy (DEX Swap into Token with Native Asset)
            # -------------------------------------------------------------
            if swap.is_buy:
                p_native_usd = await self.price_oracle.get_historical_native_price(
                    chain, swap.block_time
                )
                # Buy cost basis in native currency includes transaction fee
                cost_native = swap.native_amount + swap.fee_native

                open_lots[token_addr].append(
                    _Lot(
                        token_address=token_addr,
                        amount_remaining=swap.token_amount,
                        total_amount=swap.token_amount,
                        cost_native=cost_native,
                        buy_tx_hash=swap.tx_hash,
                        buy_timestamp=swap.block_time,
                        entry_native_price_usd=p_native_usd,
                        entry_fee_native=swap.fee_native,
                        venue=swap.venue,
                        is_external_transfer=False,
                    )
                )

            # -------------------------------------------------------------
            # Case 3: Market Sell (DEX Swap from Token into Native Asset)
            # -------------------------------------------------------------
            else:
                p_native_usd = await self.price_oracle.get_historical_native_price(
                    chain, swap.block_time
                )
                # Exit net native proceeds subtract transaction fee
                net_exit_native = swap.native_amount - swap.fee_native
                tokens_to_sell = swap.token_amount
                total_tokens_sold_in_tx = swap.token_amount

                lots = open_lots.get(token_addr, [])

                while lots and tokens_to_sell > Decimal(0):
                    lot = lots[0]

                    # If this lot came from an external deposit, consume without realized trade
                    if lot.is_external_transfer:
                        matched_qty = min(tokens_to_sell, lot.amount_remaining)
                        tokens_to_sell -= matched_qty
                        lot.amount_remaining -= matched_qty
                        if lot.amount_remaining <= Decimal(0):
                            lots.pop(0)
                        continue

                    # FIFO Matching with partial fills
                    matched_qty = min(tokens_to_sell, lot.amount_remaining)
                    lot.amount_remaining -= matched_qty

                    # Cost basis pro-rated from original lot
                    lot_fraction = matched_qty / lot.total_amount
                    lot_cost_native = lot_fraction * lot.cost_native
                    lot_cost_usd = lot_cost_native * lot.entry_native_price_usd

                    # Net proceeds pro-rated from this sell tx
                    sell_fraction = matched_qty / total_tokens_sold_in_tx
                    exit_proceeds_native = sell_fraction * net_exit_native
                    exit_proceeds_usd = exit_proceeds_native * p_native_usd

                    realized_native_pnl = exit_proceeds_native - lot_cost_native
                    realized_pnl_usd = exit_proceeds_usd - lot_cost_usd

                    roi_pct = 0.0
                    if lot_cost_usd > Decimal(0):
                        roi_pct = float((realized_pnl_usd / lot_cost_usd) * Decimal(100))

                    holding_time_s = max(0.0, float(swap.block_time - lot.buy_timestamp))

                    entry_price_usd = (lot_cost_usd / matched_qty) if matched_qty > Decimal(0) else None
                    exit_price_usd = (exit_proceeds_usd / matched_qty) if matched_qty > Decimal(0) else None

                    # Build robust, closed WalletTradeRecord
                    closed_trades.append(
                        WalletTradeRecord(
                            token_address=token_addr,
                            buy_tx_hash=lot.buy_tx_hash,
                            sell_tx_hash=swap.tx_hash,
                            buy_timestamp=lot.buy_timestamp,
                            sell_timestamp=swap.block_time,
                            holding_time_seconds=holding_time_s,
                            invested_native=lot_cost_native,
                            realized_native_pnl=realized_native_pnl,
                            realized_pnl_usd=realized_pnl_usd,
                            roi_pct=roi_pct,
                            entry_price_usd=entry_price_usd,
                            exit_price_usd=exit_price_usd,
                            custom_metadata={
                                "matched_token_amount": str(matched_qty),
                                "venue": swap.venue,
                                "partial_sell": matched_qty < lot.total_amount,
                            },
                        )
                    )

                    tokens_to_sell -= matched_qty
                    if lot.amount_remaining <= Decimal(0):
                        lots.pop(0)

                if tokens_to_sell > Decimal(0):
                    unmatched_sells += 1
                    logger.debug(
                        "Wallet %s has %s unmatched sell volume on token %s",
                        wallet_address[:10], tokens_to_sell, token_addr[:10]
                    )

        # -----------------------------------------------------------------
        # Remaining Open Positions
        # -----------------------------------------------------------------
        open_positions: list[OpenPositionRecord] = []
        for token_addr, lots in open_lots.items():
            for lot in lots:
                if not lot.is_external_transfer and lot.amount_remaining > Decimal(0):
                    frac = lot.amount_remaining / lot.total_amount
                    basis_native = frac * lot.cost_native
                    basis_usd = basis_native * lot.entry_native_price_usd
                    entry_p_native = (basis_native / lot.amount_remaining) if lot.amount_remaining > 0 else Decimal(0)
                    entry_p_usd = (basis_usd / lot.amount_remaining) if lot.amount_remaining > 0 else None

                    open_positions.append(
                        OpenPositionRecord(
                            token_address=token_addr,
                            amount_remaining=lot.amount_remaining,
                            cost_basis_native=basis_native,
                            cost_basis_usd=basis_usd,
                            buy_timestamp=lot.buy_timestamp,
                            buy_tx_hash=lot.buy_tx_hash,
                            entry_price_native=entry_p_native,
                            entry_price_usd=entry_p_usd,
                            custom_metadata={"venue": lot.venue},
                        )
                    )

        if max_closed_trades is not None and max_closed_trades > 0:
            closed_trades = closed_trades[:max_closed_trades]

        total_realized_usd = sum((t.realized_pnl_usd for t in closed_trades), Decimal(0))
        total_realized_native = sum((t.realized_native_pnl for t in closed_trades), Decimal(0))
        wins = sum(1 for t in closed_trades if t.is_win)
        win_rate = (wins / len(closed_trades) * 100.0) if closed_trades else 0.0

        return TradeReconstructionResult(
            wallet_address=wallet_address,
            chain=chain,
            closed_trades=closed_trades,
            open_positions=open_positions,
            total_realized_pnl_usd=total_realized_usd,
            total_realized_native_pnl=total_realized_native,
            win_rate_pct=round(win_rate, 2),
            unmatched_sells_count=unmatched_sells,
        )
