"""
alpha_engine.ingestion.dex_metrics — Real-Time Sliding-Window DEX Metrics Aggregator
===================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | High-Performance In-Memory Sliding-Window Aggregator
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal
from typing import NamedTuple, Optional

from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.models.events import PoolStateUpdateEvent, SwapEvent
from alpha_engine.models.state import PoolState

logger = logging.getLogger(__name__)


class VolumeSpikeResult(NamedTuple):
    has_spike: bool
    ratio: float

    def __bool__(self) -> bool:
        return self.has_spike


@dataclass
class SwapMetricEntry:
    """A single trade record in the sliding window."""

    timestamp_s: float
    is_buy: bool
    volume_native: Decimal
    volume_token: Decimal
    price: Decimal
    block_number: int
    sender: str
    tx_hash: str


@dataclass
class PoolRollingMetrics:
    """Consolidated rolling window metrics for a monitored liquidity pool."""

    pool_address: str
    chain: ChainIdentifier
    token_address: str
    latest_price: Decimal = Decimal("0")
    # Rolling Volume (native units)
    volume_1m: Decimal = Decimal("0")
    volume_5m: Decimal = Decimal("0")
    volume_1h: Decimal = Decimal("0")
    # Buy vs Sell breakdowns (5m window)
    buy_volume_5m: Decimal = Decimal("0")
    sell_volume_5m: Decimal = Decimal("0")
    buy_count_5m: int = 0
    sell_count_5m: int = 0
    # Net Buy Pressure: (buy_vol - sell_vol) / total_vol in [-1.0, 1.0]
    net_buy_pressure: float = 0.0
    # Buy/Sell Transaction Count Ratio
    buy_sell_tx_ratio: float = 1.0
    # Liquidity depth & reserve shifts
    native_reserve: Decimal = Decimal("0")
    token_reserve: Decimal = Decimal("0")
    invariant_k: Decimal = Decimal("0")
    reserve_shift_pct: float = 0.0
    # Concentrated Liquidity / CLMM Ticks
    current_tick: Optional[int] = None
    tick_shift: int = 0
    concentrated_liquidity_depth: Decimal = Decimal("0")
    last_updated_s: float = 0.0


class DEXMetricsAggregator:
    """
    In-memory sliding-window metrics aggregator tracking real-time DEX liquidity pools:
    - 1m, 5m, and 1h rolling volume
    - Buy/Sell transaction count ratio and volume delta (Net Buy Pressure)
    - Liquidity depth and pool reserve shifts (x * y = k and concentrated liquidity ticks)
    """

    def __init__(self, max_history_s: float = 3600.0) -> None:
        self.max_history_s = max_history_s  # 1 hour
        # pool_address -> deque of SwapMetricEntry
        self._pool_swaps: dict[str, deque[SwapMetricEntry]] = defaultdict(deque)
        # pool_address -> PoolState
        self._pool_states: dict[str, PoolState] = {}
        # pool_address -> baseline initial reserves
        self._baseline_reserves: dict[str, tuple[Decimal, Decimal, Decimal]] = {}
        # pool_address -> (current_tick, initial_tick, concentrated_liq)
        self._concentrated_ticks: dict[str, tuple[int, int, Decimal]] = {}
        # token_address -> pool_address mapping
        self._token_to_pool: dict[str, str] = {}

    def record_pool_state(self, pool_state: PoolState) -> None:
        """Store or update the authoritative pool state and compute reserve shifts."""
        addr = pool_state.pool_address
        self._pool_states[addr] = pool_state
        if pool_state.token_address:
            self._token_to_pool[pool_state.token_address] = addr

        k = pool_state.invariant_k
        if addr not in self._baseline_reserves:
            self._baseline_reserves[addr] = (
                pool_state.native_reserve,
                pool_state.token_reserve,
                k,
            )

    def record_pool_update_event(self, event: PoolStateUpdateEvent) -> None:
        """Handler for on-chain Sync / reserve refresh events."""
        self.record_pool_state(event.new_pool_state)

    def record_concentrated_tick(
        self,
        pool_address: str,
        tick: int,
        liquidity: Optional[Decimal] = None,
    ) -> None:
        """Update concentrated liquidity tick data (Uniswap V3 / Raydium CLMM)."""
        liq = liquidity or Decimal("0")
        if pool_address not in self._concentrated_ticks:
            self._concentrated_ticks[pool_address] = (tick, tick, liq)
        else:
            _, init_tick, _ = self._concentrated_ticks[pool_address]
            self._concentrated_ticks[pool_address] = (tick, init_tick, liq)

    def record_swap(
        self,
        swap: SwapEvent,
        pool_state: Optional[PoolState] = None,
        is_quote_token_in: Optional[bool] = None,
    ) -> None:
        """
        Record a DEX swap event and update pool reserves and rolling history.
        """
        pool_addr = swap.pool_address
        if pool_state is not None:
            self.record_pool_state(pool_state)

        # Determine buy vs sell
        # By convention in alpha_engine, buying the token means token_out is the token
        # If pool_state is known:
        token_addr = ""
        if pool_state is not None and pool_state.token_address:
            token_addr = pool_state.token_address
            is_buy = swap.token_out == token_addr
        elif pool_addr in self._token_to_pool:
            token_addr = self._token_to_pool[pool_addr]
            is_buy = swap.token_out == token_addr
        else:
            # Native addresses
            weth = "0x4200000000000000000000000000000000000006"
            wsol = "So11111111111111111111111111111111111111112"
            if swap.token_in.lower() in (weth.lower(), wsol.lower()):
                is_buy = True
                token_addr = swap.token_out
            else:
                is_buy = False
                token_addr = swap.token_in

        now_s = (
            float(swap.timestamp_ns) / 1e9 if swap.timestamp_ns > 0 else time.time()
        )
        vol_native = swap.amount_in if is_buy else swap.amount_out
        vol_token = swap.amount_out if is_buy else swap.amount_in
        price = vol_native / vol_token if vol_token > 0 else Decimal("0")

        entry = SwapMetricEntry(
            timestamp_s=now_s,
            is_buy=is_buy,
            volume_native=vol_native,
            volume_token=vol_token,
            price=price,
            block_number=swap.block_number,
            sender=swap.sender,
            tx_hash=swap.tx_hash,
        )

        q = self._pool_swaps[pool_addr]
        q.append(entry)

        # Prune older than max_history_s
        cutoff = now_s - self.max_history_s
        while q and q[0].timestamp_s < cutoff:
            q.popleft()

    def get_metrics(
        self, pool_address: str, current_time_s: Optional[float] = None
    ) -> PoolRollingMetrics:
        """
        Aggregate and return real-time sliding-window metrics for the given pool.
        """
        now_s = current_time_s if current_time_s is not None else time.time()
        q = self._pool_swaps.get(pool_address, deque())

        vol_1m = Decimal("0")
        vol_5m = Decimal("0")
        vol_1h = Decimal("0")

        buy_vol_5m = Decimal("0")
        sell_vol_5m = Decimal("0")
        buy_cnt_5m = 0
        sell_cnt_5m = 0

        latest_price = Decimal("0")
        chain = ChainIdentifier.SOLANA_MAINNET

        # Reverse iterate for recent stats
        for entry in reversed(q):
            age = now_s - entry.timestamp_s
            if age > self.max_history_s:
                break
            if latest_price == Decimal("0") and entry.price > 0:
                latest_price = entry.price

            if age <= 60.0:
                vol_1m += entry.volume_native
            if age <= 300.0:
                vol_5m += entry.volume_native
                if entry.is_buy:
                    buy_vol_5m += entry.volume_native
                    buy_cnt_5m += 1
                else:
                    sell_vol_5m += entry.volume_native
                    sell_cnt_5m += 1
            if age <= 3600.0:
                vol_1h += entry.volume_native

        # Net Buy Pressure: (buy - sell) / total in [-1.0, 1.0]
        total_5m = buy_vol_5m + sell_vol_5m
        if total_5m > Decimal("0"):
            net_buy_pressure = float((buy_vol_5m - sell_vol_5m) / total_5m)
        else:
            net_buy_pressure = 0.0

        # Buy / Sell Tx Ratio
        if sell_cnt_5m > 0:
            tx_ratio = float(buy_cnt_5m) / float(sell_cnt_5m)
        elif buy_cnt_5m > 0:
            tx_ratio = float(buy_cnt_5m)
        else:
            tx_ratio = 1.0

        # Pool reserves & shift
        pool_state = self._pool_states.get(pool_address)
        native_res = pool_state.native_reserve if pool_state else Decimal("0")
        token_res = pool_state.token_reserve if pool_state else Decimal("0")
        k = pool_state.invariant_k if pool_state else Decimal("0")
        token_addr = pool_state.token_address or "" if pool_state else ""
        if pool_state:
            chain = pool_state.chain

        shift_pct = 0.0
        baseline = self._baseline_reserves.get(pool_address)
        if baseline and baseline[0] > Decimal("0") and native_res > Decimal("0"):
            init_native = baseline[0]
            shift_pct = float((native_res - init_native) / init_native * 100)

        # Concentrated liquidity ticks
        curr_tick = None
        tick_shift = 0
        conc_liq = Decimal("0")
        if pool_address in self._concentrated_ticks:
            c_tick, i_tick, liq = self._concentrated_ticks[pool_address]
            curr_tick = c_tick
            tick_shift = c_tick - i_tick
            conc_liq = liq

        return PoolRollingMetrics(
            pool_address=pool_address,
            chain=chain,
            token_address=token_addr,
            latest_price=latest_price,
            volume_1m=vol_1m,
            volume_5m=vol_5m,
            volume_1h=vol_1h,
            buy_volume_5m=buy_vol_5m,
            sell_volume_5m=sell_vol_5m,
            buy_count_5m=buy_cnt_5m,
            sell_count_5m=sell_cnt_5m,
            net_buy_pressure=net_buy_pressure,
            buy_sell_tx_ratio=tx_ratio,
            native_reserve=native_res,
            token_reserve=token_res,
            invariant_k=k,
            reserve_shift_pct=shift_pct,
            current_tick=curr_tick,
            tick_shift=tick_shift,
            concentrated_liquidity_depth=conc_liq,
            last_updated_s=now_s,
        )

    def get_order_flow_imbalance(
        self, pool_address: str, num_blocks: int = 15
    ) -> float:
        """
        Compute Order-Flow Imbalance: Buy volume percentage over the last `num_blocks` blocks.
        Returns float in [0.0, 1.0].
        """
        q = self._pool_swaps.get(pool_address, deque())
        if not q:
            return 0.50

        # Find max block number
        max_block = max(entry.block_number for entry in q)
        min_block = max_block - num_blocks

        recent_entries = [
            e for e in q if e.block_number >= min_block and e.block_number > 0
        ]
        if not recent_entries:
            # Fall back to latest entries
            recent_entries = list(q)[-min(len(q), num_blocks * 2) :]

        buy_vol = sum(
            (e.volume_native for e in recent_entries if e.is_buy), Decimal("0")
        )
        total_vol = sum((e.volume_native for e in recent_entries), Decimal("0"))

        if total_vol > Decimal("0"):
            return float(buy_vol / total_vol)
        return 0.50

    def check_volume_spike(
        self,
        pool_address: str,
        multiplier: float = 3.0,
        baseline_1h_volume: Optional[Decimal] = None,
    ) -> VolumeSpikeResult:
        """
        Volume Spike Detector: Trigger when V_5m >= multiplier * (V_1h / 12).
        Compares 5-minute volume to normalized 5-minute rate of 1-hour volume.
        """
        metrics = self.get_metrics(pool_address)
        base_1h = baseline_1h_volume if baseline_1h_volume is not None else metrics.volume_1h
        avg_5m_of_1h = base_1h / Decimal("12")
        if avg_5m_of_1h <= Decimal("0"):
            has_spike = metrics.volume_5m > Decimal("0")
            ratio = 3.0 if has_spike else 0.0
            return VolumeSpikeResult(has_spike=has_spike, ratio=ratio)
        ratio = float(metrics.volume_5m / avg_5m_of_1h)
        return VolumeSpikeResult(has_spike=(ratio >= multiplier), ratio=round(ratio, 2))
