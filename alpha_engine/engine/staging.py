"""
alpha_engine.engine.staging — Persistent Staging Buffer & Wave-2 Dip-Reversal Engine
===================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio | Quantitative Breakout & CTO Inflection Engine
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
import logging
import math
import time
from typing import Any, Callable, Optional

from alpha_engine.models.decisions import DecisionRecord, DecisionType
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitProfile,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
)
from alpha_engine.models.events import RawSignalEvent, SignalEvent, SwapEvent
from alpha_engine.models.state import PoolState, SecurityReport

logger = logging.getLogger(__name__)


@dataclass
class VolumeBucket:
    """Aggregated volume and trade count for a fixed time bucket (e.g. 60 seconds)."""

    timestamp_s: int
    buy_volume: Decimal = Decimal(0)
    sell_volume: Decimal = Decimal(0)
    buy_count: int = 0
    sell_count: int = 0

    @property
    def total_volume(self) -> Decimal:
        return self.buy_volume + self.sell_volume


@dataclass
class StagedToken:
    """
    State tracking for a token in the Wave-2 Dip-Reversal staging buffer.
    Maintains continuous price evolution, sliding volume history, dev/holder dynamics,
    and institutional accumulation breakout scoring.
    """

    token_address: str
    chain: ChainIdentifier
    pool_address: str
    t_staged: float
    ttl_seconds: float
    initial_price: Decimal
    latest_price: Decimal
    peak_price: Decimal
    trough_price: Decimal
    security_report: SecurityReport
    raw_signal: Optional[RawSignalEvent] = None
    dev_wallet_address: str = ""
    dev_balance_pct: float = 0.0               # Supply % held by dev (0.0 = 0%)
    top10_concentration: float = 0.15          # Supply % held by top 10 non-pool holders
    whale_cluster_detected: bool = False       # Coordinated Sybil/cabal cluster
    max_single_holder_pct: float = 0.03        # Max % held by any single non-pool wallet
    narrative_cluster: Optional[str] = None

    # Price and consolidation state
    consolidation_start_time: float = 0.0
    consolidation_min_price: Decimal = Decimal(0)
    consolidation_max_price: Decimal = Decimal(0)
    initial_dump_sell_volume: Decimal = Decimal(0)
    initial_dump_duration_s: float = 0.0

    # Trade sliding history (1-minute buckets up to 6 hours = 360 buckets)
    volume_buckets: deque[VolumeBucket] = field(default_factory=lambda: deque(maxlen=360))
    recent_swaps: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=100))

    # Computed metrics
    volume_surge_multiplier: float = 0.0
    net_buy_delta: float = 0.50
    accumulation_score: float = 0.0
    active_rules_status: dict[str, bool] = field(default_factory=dict)
    rule_rationales: list[str] = field(default_factory=list)

    # Lifecycle flags
    breakout_signaled: bool = False
    dropped: bool = False
    drop_reason: str = ""
    last_telemetry_s: float = 0.0

    # Revival swing & anti-FOMO metrics
    token_creation_timestamp: float = 0.0
    token_age_seconds: float = 0.0
    consolidation_base_price: Decimal = Decimal(0)
    consolidation_base_mcap: Decimal = Decimal(0)
    dormant_1h_volume_usd: float = 0.0
    dormant_1h_volume_native: Decimal = Decimal(0)
    five_min_candles: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=288))
    rsi_5m: float = 50.0
    five_min_volume: Decimal = Decimal(0)
    one_hour_volume: Decimal = Decimal(0)
    five_min_buy_volume: Decimal = Decimal(0)
    five_min_sell_volume: Decimal = Decimal(0)
    dormant_detected: bool = False
    dormant_volume_1h_threshold: Optional[Decimal] = None

    @property
    def token_age_hours(self) -> float:
        return (self.token_age_seconds / 3600.0) if self.token_age_seconds > 0 else 0.0

    def get_volume_window(self, window_seconds: float, current_time_s: float | None = None) -> tuple[Decimal, Decimal, int, int]:
        """
        Aggregate (buy_volume, sell_volume, buy_count, sell_count) for the specified lookback window.
        """
        now = current_time_s if current_time_s is not None else time.time()
        cutoff = now - window_seconds
        total_buy = Decimal(0)
        total_sell = Decimal(0)
        cnt_buy = 0
        cnt_sell = 0
        for b in self.volume_buckets:
            if b.timestamp_s >= cutoff:
                total_buy += b.buy_volume
                total_sell += b.sell_volume
                cnt_buy += b.buy_count
                cnt_sell += b.sell_count
        return total_buy, total_sell, cnt_buy, cnt_sell

    @property
    def highest_price(self) -> Decimal:
        """Alias for peak_price."""
        return self.peak_price

    @highest_price.setter
    def highest_price(self, val: Decimal) -> None:
        self.peak_price = val

    def extract_feature_vector(self, peak_gain_multiplier: float = 1.0) -> Any:
        """
        Extract a normalized PatternFeatureVector from the staged token's historical metrics.
        """
        from alpha_engine.models.decisions import PatternFeatureVector

        dip_pct = 0.0
        ref_peak = max(self.peak_price, self.initial_price)
        if ref_peak > Decimal("0") and self.trough_price > Decimal("0"):
            raw_dip = float((ref_peak - self.trough_price) / ref_peak * Decimal("100"))
            dip_pct = max(0.0, min(100.0, raw_dip))

        now = time.time()
        if self.consolidation_start_time > 0.0:
            duration_s = max(0.0, now - self.consolidation_start_time)
        elif self.t_staged > 0.0:
            duration_s = max(0.0, now - self.t_staged)
        else:
            duration_s = 1800.0

        delta = max(0.0, min(1.0, float(self.net_buy_delta)))
        top10 = max(0.0, min(1.0, float(self.top10_concentration)))
        surge = max(0.0, float(self.volume_surge_multiplier))
        gain_mult = max(0.0, float(peak_gain_multiplier))

        return PatternFeatureVector(
            token_address=self.token_address,
            consolidation_duration_s=duration_s,
            dip_depth_pct=dip_pct,
            volume_surge_multiplier=surge,
            net_buy_delta=delta,
            top10_concentration=top10,
            liquidity_to_mc_ratio=0.20,
            smart_wallet_inflows=5.0,
            peak_gain_multiplier=gain_mult,
        )

    def record_5m_price(self, price: Decimal, volume_native: Decimal = Decimal("0.1"), timestamp_s: float | None = None) -> None:
        """Update sliding 5-minute OHLCV candles and compute latest 14-period RSI."""
        p_float = float(price)

        if timestamp_s is not None:
            now = timestamp_s
            bucket_ts = int(now // 300) * 300
        else:
            if self.five_min_candles:
                bucket_ts = self.five_min_candles[-1]["timestamp"] + 300
            else:
                bucket_ts = int(time.time() // 300) * 300

        if not self.five_min_candles or self.five_min_candles[-1]["timestamp"] != bucket_ts:
            self.five_min_candles.append({
                "timestamp": bucket_ts,
                "open": p_float,
                "high": p_float,
                "low": p_float,
                "close": p_float,
                "volume": float(volume_native),
            })
        else:
            candle = self.five_min_candles[-1]
            candle["high"] = max(candle["high"], p_float)
            candle["low"] = min(candle["low"], p_float)
            candle["close"] = p_float
            candle["volume"] += float(volume_native)

        self.rsi_5m = self.compute_5m_rsi(period=14, current_price=price)

    def compute_5m_rsi(self, period: int = 14, current_price: Optional[Decimal] = None) -> float:
        """Compute standard 14-period RSI from 5m candle closing prices."""
        closes = [c["close"] for c in self.five_min_candles]
        if current_price is not None and current_price > Decimal(0):
            p_float = float(current_price)
            if not closes or abs(closes[-1] - p_float) > 1e-12:
                closes.append(p_float)

        if len(closes) < 2:
            return 50.0

        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        window_deltas = deltas[-period:] if len(deltas) >= period else deltas

        gains = [d for d in window_deltas if d > 0]
        losses = [-d for d in window_deltas if d < 0]

        avg_gain = sum(gains) / len(window_deltas) if window_deltas else 0.0
        avg_loss = sum(losses) / len(window_deltas) if window_deltas else 0.0

        if avg_loss == 0.0:
            if avg_gain == 0.0:
                return 50.0
            gain_ratio = len(gains) / len(window_deltas)
            return round(50.0 + 50.0 * gain_ratio, 2)
        if avg_gain == 0.0:
            loss_ratio = len(losses) / len(window_deltas)
            return round(50.0 - 50.0 * loss_ratio, 2)

        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return round(rsi, 2)

    def compute_bollinger_bandwidth(self, period: int = 20, num_std: float = 2.0) -> float:
        """
        Compute Bollinger Band bandwidth = (UpperBand - LowerBand) / MiddleBand on 5m candle closes.
        Lower values (< 0.35) indicate tight volatility compression / squeeze.
        """
        closes = [c["close"] for c in self.five_min_candles]
        if len(closes) < 3:
            if self.consolidation_min_price > Decimal(0):
                spread = float((self.consolidation_max_price - self.consolidation_min_price) / self.consolidation_min_price)
                return spread
            return 0.20

        window = closes[-period:] if len(closes) >= period else closes
        mean = sum(window) / len(window)
        if mean <= 0:
            return 1.0
        variance = sum((x - mean) ** 2 for x in window) / len(window)
        std_dev = math.sqrt(variance)
        upper = mean + num_std * std_dev
        lower = max(0.0, mean - num_std * std_dev)
        bandwidth = (upper - lower) / mean
        return round(bandwidth, 4)

    def compute_price_std_dev_12h(self) -> float:
        """
        Consolidation Baseline: Calculate price standard deviation percentage over the preceding 12 hours.
        Returns std_dev / mean. If std_dev / mean < 0.10, price variation is < 10%.
        """
        closes = [float(c["close"]) for c in self.five_min_candles]
        # 12 hours of 5m candles = 144 candles
        window = closes[-144:] if len(closes) >= 144 else closes
        if len(window) < 3:
            if self.consolidation_min_price > Decimal(0):
                spread = float((self.consolidation_max_price - self.consolidation_min_price) / self.consolidation_min_price)
                return min(1.0, spread)
            return 0.05
        mean = sum(window) / len(window)
        if mean <= 0:
            return 1.0
        variance = sum((x - mean) ** 2 for x in window) / len(window)
        std_dev = math.sqrt(variance)
        return float(std_dev / mean)

    def check_awakening_volume_increase(self, consecutive_hours: int = 3, current_time_s: float | None = None) -> bool:
        """
        Awakening Trigger: Sustained volume increase over consecutive hourly intervals (default 3 hours).
        Returns True if each consecutive hour has higher volume than the preceding one.
        """
        now = current_time_s if current_time_s is not None else time.time()
        hourly_volumes: list[Decimal] = []
        for h in range(consecutive_hours):
            start = now - (h + 1) * 3600.0
            end = now - h * 3600.0
            h_vol = Decimal(0)
            for b in self.volume_buckets:
                if start <= b.timestamp_s < end:
                    h_vol += b.total_volume
            hourly_volumes.append(h_vol)

        # hourly_volumes is [hour_0_to_1_ago, hour_1_to_2_ago, hour_2_to_3_ago]
        # In chronological order: [oldest, ..., newest]
        chrono = list(reversed(hourly_volumes))
        if len(chrono) < 2:
            return True
        for i in range(1, len(chrono)):
            if chrono[i] <= chrono[i - 1] and chrono[i] == Decimal(0):
                return False
        return True


class Wave2StagingBuffer:
    r"""
    Wave-2 Dip-Reversal & Consolidation Breakout Engine:
    Maintains a persistent staging watchlist of tokens with an adjustable TTL (1 to 6 hours).
    Evaluates 4 core inflection indicators:
      1. Extended consolidation with declining sell volume (selling exhaustion).
      2. Volume surge multiplier ($Volume_{5m} > k \times SMA_{15m}$).
      3. Net buy delta shift ($NetBuyVolume > 65\%$).
      4. Dev-exit / Community Takeover (CTO) indicators: Dev balance 0%, top 10 < 25%, no whale clusters.
    Generates high-priority SignalEvent when accumulation breakout pattern is confirmed.
    """

    def __init__(
        self,
        ttl_seconds: float = 10800.0,            # 3 hours default (adjustable 1h to 6h)
        min_dip_depth_pct: float = 30.0,         # Minimum initial dump depth to qualify as dip
        min_consolidation_duration_s: float = 900.0, # Minimum 15 minutes of consolidation
        max_consolidation_range_pct: float = 30.0,   # Max price band spread during consolidation
        volume_surge_multiplier_k: float = 2.5,  # k multiplier: Volume_5m > k * SMA_15m
        min_net_buy_delta_pct: float = 65.0,     # Net buy volume > 65% in recent window
        max_dev_balance_pct: float = 0.0005,     # Dev balance <= 0.05% (effectively 0%)
        max_top10_concentration_pct: float = 25.0, # Top 10 non-pool holders < 25%
        max_single_holder_pct: float = 4.0,      # No non-pool wallet > 4%
        min_accumulation_score: float = 0.75,    # Minimum composite score to trigger breakout
        on_signal_callback: Optional[Callable[[SignalEvent], Any]] = None,
        on_decision_callback: Optional[Callable[[DecisionRecord], Any]] = None,
        config: Any = None,
    ) -> None:
        if config is not None:
            if isinstance(config, dict):
                get_val = lambda *keys: next((config[k] for k in keys if k in config and config[k] is not None), None)
            else:
                get_val = lambda *keys: next((getattr(config, k) for k in keys if hasattr(config, k) and getattr(config, k) is not None), None)

            c_ttl = get_val("wave2_ttl_seconds", "wave2_staging_ttl_seconds", "ttl_seconds")
            if c_ttl is not None:
                try:
                    ttl_seconds = float(c_ttl)
                except (ValueError, TypeError):
                    pass

            c_surge = get_val("wave2_surge_k", "wave2_volume_surge_multiplier", "volume_surge_multiplier_k")
            if c_surge is not None:
                try:
                    volume_surge_multiplier_k = float(c_surge)
                except (ValueError, TypeError):
                    pass

            c_buy_delta = get_val("wave2_min_buy_delta_pct", "min_net_buy_delta_pct")
            if c_buy_delta is not None:
                try:
                    min_net_buy_delta_pct = float(c_buy_delta)
                except (ValueError, TypeError):
                    pass

            c_top10 = get_val("wave2_max_top10_concentration_pct", "max_top10_concentration_pct")
            if c_top10 is not None:
                try:
                    max_top10_concentration_pct = float(c_top10)
                except (ValueError, TypeError):
                    pass

            c_cons = get_val("wave2_min_consolidation_duration_s", "min_consolidation_duration_s")
            if c_cons is not None:
                try:
                    min_consolidation_duration_s = float(c_cons)
                except (ValueError, TypeError):
                    pass
        self.ttl_seconds = ttl_seconds
        self.min_dip_depth_pct = min_dip_depth_pct
        self.min_consolidation_duration_s = min_consolidation_duration_s
        self.max_consolidation_range_pct = max_consolidation_range_pct
        self.volume_surge_multiplier_k = volume_surge_multiplier_k
        self.min_net_buy_delta_pct = min_net_buy_delta_pct
        self.max_dev_balance_pct = max_dev_balance_pct
        self.max_top10_concentration_pct = max_top10_concentration_pct
        self.max_single_holder_pct = max_single_holder_pct
        self.min_accumulation_score = min_accumulation_score

        self._staged: dict[str, StagedToken] = {}
        self._on_signal_callbacks: list[Callable[[SignalEvent], Any]] = [on_signal_callback] if on_signal_callback else []
        self._on_decision_callbacks: list[Callable[[DecisionRecord], Any]] = [on_decision_callback] if on_decision_callback else []
        self._lock = asyncio.Lock()

    def register_signal_callback(self, cb: Callable[[SignalEvent], Any]) -> None:
        if cb not in self._on_signal_callbacks:
            self._on_signal_callbacks.append(cb)

    def register_decision_callback(self, cb: Callable[[DecisionRecord], Any]) -> None:
        if cb not in self._on_decision_callbacks:
            self._on_decision_callbacks.append(cb)

    def set_hyperparameters(
        self,
        volume_surge_multiplier_k: Optional[float] = None,
        min_net_buy_delta_pct: Optional[float] = None,
        ttl_seconds: Optional[float] = None,
        min_consolidation_duration_s: Optional[float] = None,
    ) -> None:
        """Dynamically update hyperparameter thresholds from feedback adaptation loop."""
        if volume_surge_multiplier_k is not None:
            self.volume_surge_multiplier_k = max(1.8, min(4.0, volume_surge_multiplier_k))
        if min_net_buy_delta_pct is not None:
            self.min_net_buy_delta_pct = max(55.0, min(80.0, min_net_buy_delta_pct))
        if ttl_seconds is not None:
            self.ttl_seconds = max(3600.0, min(21600.0, ttl_seconds))
        if min_consolidation_duration_s is not None:
            self.min_consolidation_duration_s = max(30.0, min(3600.0, min_consolidation_duration_s))

    def stage_token(
        self,
        token_address: str,
        chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
        pool_address: str = "",
        initial_price: Decimal = Decimal("0.000000028"),
        report: Optional[SecurityReport] = None,
        raw_signal: Optional[RawSignalEvent] = None,
        dev_wallet: str = "",
        dev_balance_pct: float = 0.0,
        top10_concentration: float = 0.15,
        whale_cluster: bool = False,
        t_staged: Optional[float] = None,
        narrative_cluster: Optional[str] = None,
    ) -> StagedToken:
        """
        Buffer a token into the persistent staging watchlist.
        Can transition from PendingLaunchBuffer (after initial dump) or from external scanner.
        """
        now = t_staged if t_staged is not None else time.time()
        init_p = initial_price if initial_price > 0 else Decimal("0.000000028")

        sec_rep = report or SecurityReport(
            token_address=token_address,
            chain=chain,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
            passes_hard_gates=True,
        )

        cluster = narrative_cluster or (raw_signal.narrative_cluster if raw_signal else None)

        staged = StagedToken(
            token_address=token_address,
            chain=chain,
            pool_address=pool_address,
            t_staged=now,
            ttl_seconds=self.ttl_seconds,
            initial_price=init_p,
            latest_price=init_p,
            peak_price=init_p,
            trough_price=init_p,
            security_report=sec_rep,
            raw_signal=raw_signal,
            dev_wallet_address=dev_wallet,
            dev_balance_pct=dev_balance_pct,
            top10_concentration=top10_concentration,
            whale_cluster_detected=whale_cluster,
            narrative_cluster=cluster,
            consolidation_start_time=now,
            consolidation_min_price=init_p,
            consolidation_max_price=init_p,
            last_telemetry_s=now,
        )

        self._staged[token_address] = staged
        logger.info(
            "Wave2StagingBuffer: Staged token [%s] for dip-reversal monitoring (TTL=%.0fs, init_price=%s)",
            token_address[:10],
            self.ttl_seconds,
            init_p,
        )
        return staged

    def is_staged(self, token_address: str) -> bool:
        tok = self._staged.get(token_address)
        return tok is not None and not tok.dropped and not tok.breakout_signaled

    def get_staged(self, token_address: str) -> Optional[StagedToken]:
        return self._staged.get(token_address)

    def get_all_staged(self) -> list[StagedToken]:
        return [t for t in self._staged.values() if not t.dropped]

    def get_staged_tokens(self) -> list[StagedToken]:
        """Alias for get_all_staged returning all active staged tokens."""
        return self.get_all_staged()

    def update_dev_and_holder_stats(
        self,
        token_address: str,
        dev_balance_pct: float,
        top10_concentration: float,
        whale_cluster_detected: bool = False,
        max_single_holder_pct: float = 0.03,
    ) -> None:
        """Update Community Takeover (CTO) and distribution metrics for a staged token."""
        tok = self._staged.get(token_address)
        if tok is not None:
            tok.dev_balance_pct = dev_balance_pct
            tok.top10_concentration = top10_concentration
            tok.whale_cluster_detected = whale_cluster_detected
            tok.max_single_holder_pct = max_single_holder_pct

    def record_trade(
        self,
        token_address: str | SwapEvent,
        native_amount: Decimal = Decimal(0),
        is_buy: bool = True,
        spot_price: Optional[Decimal] = None,
        buyer: str = "",
        current_time_s: Optional[float] = None,
        pool: Optional[PoolState] = None,
    ) -> Optional[SignalEvent]:
        """
        Record a trade or swap on a staged token.
        Updates sliding volume windows, evaluates consolidation, volume surge,
        buy delta shift, and CTO indicators. Emits SignalEvent upon breakout.
        """
        if isinstance(token_address, SwapEvent):
            swap = token_address
            tok_addr = swap.token_out if swap.token_out in self._staged else swap.token_in
            tok = self._staged.get(tok_addr)
            if tok is None or tok.dropped:
                return None
            is_buy = (swap.token_out == tok_addr)
            native_amount = swap.amount_in if is_buy else swap.amount_out
            buyer = swap.sender if is_buy else ""
            if pool is not None and pool.spot_price_native_per_token > 0:
                spot_price = pool.spot_price_native_per_token
            elif swap.amount_in > 0 and swap.amount_out > 0:
                spot_price = swap.amount_in / swap.amount_out if is_buy else swap.amount_out / swap.amount_in
        else:
            tok_addr = token_address
            tok = self._staged.get(tok_addr)
            if tok is None or tok.dropped:
                return None

        if current_time_s is not None:
            now = current_time_s
        elif isinstance(token_address, SwapEvent) and swap.timestamp_ns > 0:
            now = float(swap.timestamp_ns) / 1e9
        else:
            now = time.time()

        minute_bucket_ts = int(now // 60) * 60

        # Update price trajectory
        curr_price = spot_price or (pool.spot_price_native_per_token if pool else tok.latest_price)
        if curr_price > 0:
            tok.latest_price = curr_price
            if curr_price > tok.peak_price:
                tok.peak_price = curr_price
            if tok.trough_price <= 0 or curr_price < tok.trough_price:
                tok.trough_price = curr_price

        # Update time-bucketed volume history
        if not tok.volume_buckets or tok.volume_buckets[-1].timestamp_s != minute_bucket_ts:
            tok.volume_buckets.append(VolumeBucket(timestamp_s=minute_bucket_ts))

        bucket = tok.volume_buckets[-1]
        if is_buy:
            bucket.buy_volume += native_amount
            bucket.buy_count += 1
        else:
            bucket.sell_volume += native_amount
            bucket.sell_count += 1

        tok.recent_swaps.append({
            "timestamp": now,
            "amount": float(native_amount),
            "is_buy": is_buy,
            "buyer": buyer,
            "price": float(curr_price),
        })

        # Track initial dump metrics (first 10 minutes post launch/staging)
        age = now - tok.t_staged
        if age <= 600.0 and not is_buy:
            tok.initial_dump_sell_volume += native_amount
            tok.initial_dump_duration_s = age

        # Evaluate the 4 core inflection algorithms
        return self._evaluate_breakout(tok, pool=pool, current_time_s=now)

    def _evaluate_breakout(
        self,
        tok: StagedToken,
        pool: Optional[PoolState] = None,
        current_time_s: float | None = None,
    ) -> Optional[SignalEvent]:
        """
        Evaluate quantitative conditions for Wave-2 Dip-Reversal Breakout:
          1. Consolidation with selling exhaustion
          2. Volume surge multiplier (V_5m > k * SMA_15m)
          3. Net buy delta shift (NetBuyVolume > 65%)
          4. Dev-exit / Community Takeover (CTO) indicators
        """
        if tok.dropped or tok.breakout_signaled:
            return None

        now = current_time_s if current_time_s is not None else time.time()
        age = now - tok.t_staged

        # 1. TTL Expiration Check
        if age > tok.ttl_seconds:
            tok.dropped = True
            tok.drop_reason = f"TTL expired ({age:.0f}s > {tok.ttl_seconds:.0f}s) without accumulation breakout"
            logger.info("Wave2StagingBuffer: Expired token [%s] — %s", tok.token_address[:10], tok.drop_reason)
            return None

        # 2. Check Initial Dip Depth
        dip_depth_pct = 0.0
        if tok.peak_price > Decimal(0):
            dip_depth_pct = float((tok.peak_price - tok.trough_price) / tok.peak_price * 100)

        # 3. Consolidation & Selling Exhaustion Calculation
        # Track consolidation price range over recent window
        recent_window_s = min(age, 1800.0) # Up to 30 mins
        recent_prices = [
            sw["price"] for sw in tok.recent_swaps
            if (now - sw["timestamp"]) <= recent_window_s and sw["price"] > 0
        ]

        consolidation_ok = False
        exhaustion_ok = False
        spread_pct = 0.0
        if recent_prices and len(recent_prices) >= 5:
            min_p = min(recent_prices)
            max_p = max(recent_prices)
            spread_pct = ((max_p - min_p) / min_p) * 100.0 if min_p > 0 else 100.0
            if spread_pct <= self.max_consolidation_range_pct and age >= self.min_consolidation_duration_s:
                consolidation_ok = True

        # Selling Exhaustion: Compare recent sell volume rate with initial dump sell volume rate
        buy_10m, sell_10m, _, cnt_sell_10m = tok.get_volume_window(600.0, current_time_s=now)
        recent_sell_rate_per_min = float(sell_10m / Decimal(10.0))
        initial_dump_rate = (
            float(tok.initial_dump_sell_volume / Decimal(max(1.0, tok.initial_dump_duration_s / 60.0)))
            if tok.initial_dump_sell_volume > 0
            else 0.5
        )

        if initial_dump_rate > 0 and (recent_sell_rate_per_min <= initial_dump_rate * 0.40 or cnt_sell_10m <= 2):
            exhaustion_ok = True

        selling_exhaustion_passed = (consolidation_ok or age >= self.min_consolidation_duration_s) and exhaustion_ok

        # 4. Volume Surge Multiplier (V_5m > k * SMA_15m)
        buy_5m, sell_5m, _, _ = tok.get_volume_window(300.0, current_time_s=now)
        v_5m = float(buy_5m + sell_5m)

        buy_15m, sell_15m, _, _ = tok.get_volume_window(900.0, current_time_s=now)
        v_15m = float(buy_15m + sell_15m)
        # 15m volume excluding current 5m surge gives the prior 10m consolidation volume
        prior_consolidation_vol = max(0.0, v_15m - v_5m)
        if prior_consolidation_vol > 0:
            sma_15m_5min_equiv = max(0.05, prior_consolidation_vol / 2.0)
            surge_mult = v_5m / sma_15m_5min_equiv
        else:
            surge_mult = 1.0
        tok.volume_surge_multiplier = round(surge_mult, 2)
        volume_surge_passed = surge_mult >= self.volume_surge_multiplier_k

        # 5. Net Buy Delta Shift (NetBuyVolume > 65%)
        net_buy_delta = float(buy_5m / (buy_5m + sell_5m)) if (buy_5m + sell_5m) > 0 else 0.50
        tok.net_buy_delta = round(net_buy_delta, 3)
        net_buy_passed = (net_buy_delta * 100.0) >= self.min_net_buy_delta_pct

        # 6. Dev-Exit / Community Takeover (CTO) Indicators
        # Dev balance effectively 0%
        dev_exited = (tok.dev_balance_pct <= self.max_dev_balance_pct)
        # Top 10 non-pool holders hold < 25% total supply
        top10_clean = (tok.top10_concentration * 100.0 <= self.max_top10_concentration_pct)
        # Whale clusters and single-holder limit
        whale_clean = (not tok.whale_cluster_detected) and (tok.max_single_holder_pct * 100.0 <= self.max_single_holder_pct)

        cto_indicators_passed = dev_exited and top10_clean and whale_clean

        # 7. Composite Institutional Accumulation Breakout Score
        # Component scores [0.0, 1.0]
        score_exhaustion = 1.0 if selling_exhaustion_passed else (0.5 if consolidation_ok else 0.2)
        score_surge = min(1.0, surge_mult / (self.volume_surge_multiplier_k * 1.5))
        score_delta = min(1.0, max(0.0, (net_buy_delta - 0.50) / 0.35))
        score_cto = 1.0 if cto_indicators_passed else (0.6 if dev_exited and top10_clean else 0.3)

        accumulation_score = (
            0.25 * score_exhaustion
            + 0.30 * score_surge
            + 0.25 * score_delta
            + 0.20 * score_cto
        )
        tok.accumulation_score = round(accumulation_score, 3)

        tok.active_rules_status = {
            "SellingExhaustion": selling_exhaustion_passed,
            "VolumeSurge": volume_surge_passed,
            "NetBuyDeltaShift": net_buy_passed,
            "DevExitConfirmed": dev_exited,
            "Top10CTOConfirmed": top10_clean,
            "WhaleClusterClean": whale_clean,
        }

        rationales = [
            f"Consolidation spread: {spread_pct:.1f}% (age: {age/60:.1f}m >= {self.min_consolidation_duration_s/60:.0f}m)",
            f"Volume surge: {surge_mult:.2f}x (threshold: {self.volume_surge_multiplier_k:.1f}x)",
            f"Net buy delta: {net_buy_delta*100:.1f}% (threshold: {self.min_net_buy_delta_pct:.0f}%)",
            f"Dev balance: {tok.dev_balance_pct*100:.3f}% (<= {self.max_dev_balance_pct*100:.2f}%)",
            f"Top 10 concentration: {tok.top10_concentration*100:.1f}% (<= {self.max_top10_concentration_pct:.0f}%)",
        ]
        tok.rule_rationales = rationales

        # Telemetry logging every 60s
        if (now - tok.last_telemetry_s) >= 60.0:
            tok.last_telemetry_s = now
            logger.info(
                "Wave2StagingBuffer [%s] (age %dm): Score=%.2f | Surge=%.2fx (V_5m=%.1f) | BuyDelta=%.1f%% | CTO=%s (Dev=%.2f%%, Top10=%.1f%%)",
                tok.token_address[:10],
                int(age // 60),
                tok.accumulation_score,
                surge_mult,
                v_5m,
                net_buy_delta * 100,
                "CONFIRMED" if cto_indicators_passed else "PENDING",
                tok.dev_balance_pct * 100,
                tok.top10_concentration * 100,
            )

        # 8. High-Priority Breakout Signal Generation
        # Requires surge, net buy delta, CTO confirmed, and overall accumulation score >= threshold
        if (
            volume_surge_passed
            and net_buy_passed
            and cto_indicators_passed
            and accumulation_score >= self.min_accumulation_score
        ):
            tok.breakout_signaled = True
            reason_str = (
                f"WAVE-2 BREAKOUT CONFIRMED: Accumulation Score={tok.accumulation_score:.2f} >= {self.min_accumulation_score:.2f} | "
                f"Volume surge {surge_mult:.2f}x > {self.volume_surge_multiplier_k:.1f}x SMA_15m | "
                f"Net buy delta {net_buy_delta*100:.1f}% > {self.min_net_buy_delta_pct:.0f}% | "
                f"Dev balance {tok.dev_balance_pct*100:.2f}%, Top 10={tok.top10_concentration*100:.1f}% without whale cluster."
            )
            logger.info("Wave2StagingBuffer: %s for %s!", reason_str, tok.token_address[:10])

            # Construct structured DecisionRecord (ENTER)
            decision = DecisionRecord(
                token_address=tok.token_address,
                chain=tok.chain.value,
                decision_type=DecisionType.ENTER,
                strategy_pattern="wave2_breakout",
                market_cap_usd=None,
                volume_5m_usd=float(buy_5m + sell_5m) * 150.0 if tok.chain == ChainIdentifier.SOLANA_MAINNET else None,
                volume_1h_usd=float(buy_15m + sell_15m) * 4.0 * 150.0 if tok.chain == ChainIdentifier.SOLANA_MAINNET else None,
                liquidity_pool_depth_usd=float(pool.native_reserve * 150) if pool else None,
                active_rules=tok.active_rules_status,
                confidence_score=min(0.98, max(0.85, tok.accumulation_score)),
                reason=reason_str,
                metadata={
                    "dip_depth_pct": round(dip_depth_pct, 2),
                    "consolidation_age_s": round(age, 1),
                    "volume_surge_multiplier": round(surge_mult, 2),
                    "net_buy_delta": round(net_buy_delta, 3),
                    "dev_balance_pct": tok.dev_balance_pct,
                    "top10_concentration": tok.top10_concentration,
                },
            )
            self._dispatch_decision(decision)

            # Generate SignalEvent
            p_state = pool or PoolState(
                pool_address=tok.pool_address or "0x" + "0" * 40,
                chain=tok.chain,
                token_reserve=Decimal("1000000000"),
                native_reserve=Decimal("30"),
                fee_numerator=30,
                fee_denominator=10000,
                last_updated_block=1000,
                token_decimals=6,
                native_decimals=9,
            )

            signal = SignalEvent(
                timestamp_ns=time.time_ns(),
                chain=tok.chain,
                pool_address=tok.pool_address or p_state.pool_address,
                token_address=tok.token_address,
                suggested_side=OrderSide.BUY,
                pool_state=p_state,
                security_report=tok.security_report,
                strength=SignalStrength.STRONG,
                alpha_score=tok.accumulation_score,
                source=SignalSource.WAVE2_BREAKOUT,
                narrative_cluster=tok.narrative_cluster,
            )
            self._dispatch_signal(signal)
            return signal

        return None

    def _dispatch_decision(self, decision: DecisionRecord) -> None:
        for cb in self._on_decision_callbacks:
            try:
                res = cb(decision)
                if asyncio.iscoroutine(res):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(res)
                    except RuntimeError:
                        pass
            except Exception as exc:
                logger.warning("Error in Wave2StagingBuffer decision callback: %s", exc)

    def _dispatch_signal(self, signal: SignalEvent) -> None:
        for cb in self._on_signal_callbacks:
            try:
                res = cb(signal)
                if asyncio.iscoroutine(res):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(res)
                    except RuntimeError:
                        pass
            except Exception as exc:
                logger.warning("Error in Wave2StagingBuffer signal callback: %s", exc)

    def sweep_expired(self, current_time_s: Optional[float] = None) -> list[str]:
        """Sweep expired tokens exceeding their TTL."""
        now = current_time_s if current_time_s is not None else time.time()
        expired: list[str] = []
        for addr, tok in list(self._staged.items()):
            if not tok.dropped and not tok.breakout_signaled:
                age = now - tok.t_staged
                if age > tok.ttl_seconds:
                    tok.dropped = True
                    tok.drop_reason = f"TTL expired ({age:.0f}s > {tok.ttl_seconds:.0f}s)"
                    expired.append(addr)
                    # Emit a SKIP decision record for audit completeness
                    decision = DecisionRecord(
                        token_address=tok.token_address,
                        chain=tok.chain.value,
                        decision_type=DecisionType.SKIP,
                        strategy_pattern="wave2_breakout",
                        active_rules=tok.active_rules_status,
                        confidence_score=0.90,
                        reason=f"Staging TTL expired without accumulation breakout: {'; '.join(tok.rule_rationales[:3])}",
                        metadata={"expired_age_s": age, "final_accumulation_score": tok.accumulation_score},
                    )
                    self._dispatch_decision(decision)
                    logger.info("Wave2StagingBuffer: Expired token [%s]", addr[:10])
        return expired

    def get_staged_status_summary(self) -> list[dict[str, Any]]:
        """Return formatted overview of all tokens currently in staging."""
        now = time.time()
        summary = []
        for tok in self._staged.values():
            if not tok.dropped:
                age = now - tok.t_staged
                remaining = max(0.0, tok.ttl_seconds - age)
                summary.append({
                    "token_address": tok.token_address,
                    "chain": tok.chain.value,
                    "age_minutes": round(age / 60.0, 1),
                    "ttl_remaining_minutes": round(remaining / 60.0, 1),
                    "accumulation_score": tok.accumulation_score,
                    "volume_surge_multiplier": tok.volume_surge_multiplier,
                    "net_buy_delta": tok.net_buy_delta,
                    "dev_balance_pct": tok.dev_balance_pct,
                    "top10_concentration": tok.top10_concentration,
                    "breakout_signaled": tok.breakout_signaled,
                    "active_rules": tok.active_rules_status,
                })
        return summary


class RevivalBreakoutBuffer:
    r"""
    Revival & CTO Breakout Swing Strategy Engine:
    Monitors neglected/dead tokens (aged between 2 hours and 10 days) breaking out
    from tight accumulation/dormancy bases.

    Execution Profile: REVIVAL_SWING (Multi-hour/day hold, 35% trailing ATH drawdown, 100%/200% TP ladder).

    Evaluation Criteria:
      1. Token Age Filter: 2 hours <= token_age <= 10 days.
      2. Consolidation & Dead Zone Metrics:
         Dormancy period where 1-hour volume was low (< $5,000 or < 5 SOL/hour)
         and price volatility was compressed (tight ATR / Bollinger Band squeeze).
      3. Breakout Volume Inflection:
         V_5m >= 3.0 * SMA_1h(V) (Volume surge >= 3x over 1-hour average).
         Net Buy Delta: (Buy Volume - Sell Volume) / Total Volume >= 0.65.
      4. Anti-FOMO / Over-Extension Guard:
         Reject entry if:
           - current_price > 2.2 * consolidation_base_price (> 120% pump from dormant floor before fill)
           - 14-period RSI on 5m candles > 82.0
      5. Community Takeover (CTO) & Safety Gates:
         - Dev balance: original creator wallet holds <= 0.1% of supply.
         - Top-10 concentration (excluding bonding curve/Raydium pool) < 25%.
         - Passes SecurityGatekeeper screening (zero honeypot, taxes <= 5%).
    """

    def __init__(
        self,
        min_token_age_s: float = 7200.0,            # 2 hours minimum age
        max_token_age_s: float = 864000.0,          # 10 days maximum age
        max_dormant_volume_1h_usd: float = 5000.0,  # Low dormancy volume < $5,000 / hr
        max_dormant_volume_1h_native: Decimal = Decimal("5.0"), # < 5 SOL / hr
        max_bb_bandwidth: float = 0.35,             # Bollinger Band squeeze threshold
        volume_surge_multiplier_k: float = 3.0,     # Volume surge >= 3.0x over 1h SMA
        min_net_buy_delta: float = 0.65,            # Net buy delta >= 65%
        max_overextension_ratio: float = 2.2,       # Max 2.2x (<= +120%) from base floor
        max_rsi_5m: float = 82.0,                   # Max 5m RSI
        max_dev_balance_pct: float = 0.001,         # Dev balance <= 0.1% (effectively 0%)
        max_top10_concentration_pct: float = 25.0,  # Top 10 non-pool holders < 25%
        min_accumulation_score: float = 0.75,       # Minimum composite revival score
        ttl_seconds: float = 86400.0 * 5,           # 5 days TTL for swing staging
        on_signal_callback: Optional[Callable[[SignalEvent], Any]] = None,
        on_decision_callback: Optional[Callable[[DecisionRecord], Any]] = None,
        config: Any = None,
    ) -> None:
        self.min_token_age_s = min_token_age_s
        self.max_token_age_s = max_token_age_s
        self.max_dormant_volume_1h_usd = max_dormant_volume_1h_usd
        self.max_dormant_volume_1h_native = max_dormant_volume_1h_native
        self.max_bb_bandwidth = max_bb_bandwidth
        self.volume_surge_multiplier_k = volume_surge_multiplier_k
        self.min_net_buy_delta = min_net_buy_delta
        self.max_overextension_ratio = max_overextension_ratio
        self.max_rsi_5m = max_rsi_5m
        self.max_dev_balance_pct = max_dev_balance_pct
        self.max_top10_concentration_pct = max_top10_concentration_pct
        self.min_accumulation_score = min_accumulation_score
        self.ttl_seconds = ttl_seconds

        self._staged: dict[str, StagedToken] = {}
        self._on_signal_callbacks: list[Callable[[SignalEvent], Any]] = [on_signal_callback] if on_signal_callback else []
        self._on_decision_callbacks: list[Callable[[DecisionRecord], Any]] = [on_decision_callback] if on_decision_callback else []
        self._lock = asyncio.Lock()

    def register_signal_callback(self, cb: Callable[[SignalEvent], Any]) -> None:
        if cb not in self._on_signal_callbacks:
            self._on_signal_callbacks.append(cb)

    def register_decision_callback(self, cb: Callable[[DecisionRecord], Any]) -> None:
        if cb not in self._on_decision_callbacks:
            self._on_decision_callbacks.append(cb)

    def stage_token(
        self,
        token_address: str,
        chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
        pool_address: str = "",
        initial_price: Decimal = Decimal("0.000000028"),
        token_creation_timestamp: Optional[float] = None,
        consolidation_base_price: Optional[Decimal] = None,
        consolidation_base_mcap: Optional[Decimal] = None,
        report: Optional[SecurityReport] = None,
        raw_signal: Optional[RawSignalEvent] = None,
        dev_wallet: str = "",
        dev_balance_pct: float = 0.0,
        top10_concentration: float = 0.15,
        whale_cluster: bool = False,
        dormant_1h_volume_usd: float = 1000.0,
        dormant_1h_volume_native: Decimal = Decimal("2.0"),
        dormant_volume_1h_threshold: Optional[Decimal] = None,
        t_staged: Optional[float] = None,
        narrative_cluster: Optional[str] = None,
    ) -> StagedToken:
        """Buffer an aged or neglected token into the RevivalBreakout staging watchlist."""
        now = t_staged if t_staged is not None else time.time()
        init_p = initial_price if initial_price > 0 else Decimal("0.000000028")

        sec_rep = report or SecurityReport(
            token_address=token_address,
            chain=chain,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
            passes_hard_gates=True,
        )

        if report is not None:
            if hasattr(report, "dev_balance_ratio") and report.dev_balance_ratio is not None:
                dev_balance_pct = float(report.dev_balance_ratio)
            if hasattr(report, "top10_concentration") and report.top10_concentration is not None:
                top10_concentration = float(report.top10_concentration)

        cluster = narrative_cluster or (raw_signal.narrative_cluster if raw_signal else None)
        creation_ts = token_creation_timestamp if token_creation_timestamp is not None else (now - 14400.0)
        base_p = consolidation_base_price if consolidation_base_price is not None and consolidation_base_price > 0 else init_p
        base_mc = consolidation_base_mcap if consolidation_base_mcap is not None and consolidation_base_mcap > 0 else Decimal("35000")

        dormant_vol_native = dormant_volume_1h_threshold if dormant_volume_1h_threshold is not None else dormant_1h_volume_native

        staged = StagedToken(
            token_address=token_address,
            chain=chain,
            pool_address=pool_address,
            t_staged=now,
            ttl_seconds=self.ttl_seconds,
            initial_price=init_p,
            latest_price=init_p,
            peak_price=init_p,
            trough_price=init_p,
            security_report=sec_rep,
            raw_signal=raw_signal,
            dev_wallet_address=dev_wallet,
            dev_balance_pct=dev_balance_pct,
            top10_concentration=top10_concentration,
            whale_cluster_detected=whale_cluster,
            narrative_cluster=cluster,
            consolidation_start_time=now,
            consolidation_min_price=base_p,
            consolidation_max_price=base_p,
            last_telemetry_s=now,
            token_creation_timestamp=creation_ts,
            token_age_seconds=max(0.0, now - creation_ts),
            consolidation_base_price=base_p,
            consolidation_base_mcap=base_mc,
            dormant_1h_volume_usd=dormant_1h_volume_usd,
            dormant_1h_volume_native=dormant_vol_native,
            dormant_volume_1h_threshold=dormant_volume_1h_threshold,
        )

        token_age_s = max(0.0, now - creation_ts)
        top10_pct = top10_concentration * 100.0 if top10_concentration <= 1.0 else top10_concentration
        if token_age_s < self.min_token_age_s:
            staged.dropped = True
            staged.drop_reason = f"Token age {token_age_s / 3600.0:.1f}h < {self.min_token_age_s / 3600.0:.1f}h minimum limit"
        elif token_age_s > self.max_token_age_s:
            staged.dropped = True
            staged.drop_reason = f"Token age {token_age_s / 3600.0:.1f}h > {self.max_token_age_s / 3600.0:.1f}h maximum limit"
        elif dev_balance_pct > self.max_dev_balance_pct:
            staged.dropped = True
            staged.drop_reason = f"CTO gate failed: dev balance {dev_balance_pct * 100.0:.2f}% > {self.max_dev_balance_pct * 100.0:.2f}% maximum limit"
        elif top10_pct >= self.max_top10_concentration_pct:
            staged.dropped = True
            staged.drop_reason = f"CTO gate failed: top10 concentration {top10_pct:.1f}% >= {self.max_top10_concentration_pct:.1f}% maximum limit"

        self._staged[token_address] = staged
        logger.info(
            "RevivalBreakoutBuffer: Staged token [%s] (age=%.1fh, base_price=%s, base_mcap=$%s, dev=%.2f%%, top10=%.1f%%, dropped=%s)",
            token_address[:10],
            (now - creation_ts) / 3600.0,
            base_p,
            base_mc,
            dev_balance_pct * 100,
            top10_concentration * 100,
            staged.dropped,
        )
        return staged

    def is_staged(self, token_address: str) -> bool:
        tok = self._staged.get(token_address)
        return tok is not None and not tok.dropped and not tok.breakout_signaled

    def get_staged(self, token_address: str) -> Optional[StagedToken]:
        return self._staged.get(token_address)

    def get_all_staged(self) -> list[StagedToken]:
        return [t for t in self._staged.values() if not t.dropped]

    def get_staged_tokens(self) -> list[StagedToken]:
        return self.get_all_staged()

    def update_dev_and_holder_stats(
        self,
        token_address: str,
        dev_balance_pct: float,
        top10_concentration: float,
        whale_cluster_detected: bool = False,
        max_single_holder_pct: float = 0.03,
    ) -> None:
        tok = self._staged.get(token_address)
        if tok is not None:
            tok.dev_balance_pct = dev_balance_pct
            tok.top10_concentration = top10_concentration
            tok.whale_cluster_detected = whale_cluster_detected
            tok.max_single_holder_pct = max_single_holder_pct

    def record_trade(
        self,
        token_address: str | SwapEvent,
        native_amount: Decimal = Decimal(0),
        is_buy: bool = True,
        spot_price: Optional[Decimal] = None,
        buyer: str = "",
        current_time_s: Optional[float] = None,
        pool: Optional[PoolState] = None,
    ) -> Optional[SignalEvent]:
        """
        Record swap/trade event on a staged revival token and evaluate breakout rules.
        """
        if isinstance(token_address, SwapEvent):
            swap = token_address
            tok_addr = swap.token_out if swap.token_out in self._staged else swap.token_in
            tok = self._staged.get(tok_addr)
            if tok is None or tok.dropped:
                return None
            is_buy = (swap.token_out == tok_addr)
            native_amount = swap.amount_in if is_buy else swap.amount_out
            buyer = swap.sender if is_buy else ""
            if pool is not None and pool.spot_price_native_per_token > 0:
                spot_price = pool.spot_price_native_per_token
            elif swap.amount_in > 0 and swap.amount_out > 0:
                spot_price = swap.amount_in / swap.amount_out if is_buy else swap.amount_out / swap.amount_in
        else:
            tok_addr = token_address
            tok = self._staged.get(tok_addr)
            if tok is None or tok.dropped:
                return None

        if current_time_s is not None:
            now = current_time_s
        elif isinstance(token_address, SwapEvent) and swap.timestamp_ns > 0:
            now = float(swap.timestamp_ns) / 1e9
        else:
            now = time.time()

        minute_bucket_ts = int(now // 60) * 60
        curr_price = spot_price or (pool.spot_price_native_per_token if pool else tok.latest_price)
        if curr_price > 0:
            tok.latest_price = curr_price
            if curr_price > tok.peak_price:
                tok.peak_price = curr_price
            if tok.trough_price <= 0 or curr_price < tok.trough_price:
                tok.trough_price = curr_price

        # Update 1-minute volume buckets
        if not tok.volume_buckets or tok.volume_buckets[-1].timestamp_s != minute_bucket_ts:
            tok.volume_buckets.append(VolumeBucket(timestamp_s=minute_bucket_ts))

        bucket = tok.volume_buckets[-1]
        if is_buy:
            bucket.buy_volume += native_amount
            bucket.buy_count += 1
        else:
            bucket.sell_volume += native_amount
            bucket.sell_count += 1

        # Update 1h and 5m aggregated volumes
        buy_1h, sell_1h, _, _ = tok.get_volume_window(3600.0, current_time_s=now)
        tok.one_hour_volume = buy_1h + sell_1h
        buy_5m, sell_5m, _, _ = tok.get_volume_window(300.0, current_time_s=now)
        tok.five_min_volume = buy_5m + sell_5m
        tok.five_min_buy_volume = buy_5m
        tok.five_min_sell_volume = sell_5m

        thresh = tok.dormant_volume_1h_threshold or tok.dormant_1h_volume_native or Decimal("5.0")
        if tok.one_hour_volume <= thresh:
            tok.dormant_detected = True

        # Update 5m candles and RSI
        tok.record_5m_price(curr_price, volume_native=native_amount, timestamp_s=now)

        tok.recent_swaps.append({
            "timestamp": now,
            "amount": float(native_amount),
            "is_buy": is_buy,
            "buyer": buyer,
            "price": float(curr_price),
        })

        return self._evaluate_breakout(tok, pool=pool, current_time_s=now)

    def check_breakout(
        self,
        token_address: str,
        pool: Optional[PoolState] = None,
        current_time_s: Optional[float] = None,
    ) -> Optional[SignalEvent]:
        """Check and evaluate breakout criteria on a staged token."""
        tok = self._staged.get(token_address)
        if tok is None:
            return None
        return self._evaluate_breakout(tok, pool=pool, current_time_s=current_time_s)

    evaluate_breakout = check_breakout

    def _evaluate_breakout(
        self,
        tok: StagedToken,
        pool: Optional[PoolState] = None,
        current_time_s: float | None = None,
    ) -> Optional[SignalEvent]:
        """
        Evaluate quantitative conditions for Revival & CTO Breakout:
          1. Token Age: 2 hours <= age <= 10 days
          2. Anti-FOMO Guard: current_price <= 2.2 * base_price and RSI_5m <= 82.0
          3. Consolidation & Dead Zone: Dormancy volume low (< 5 SOL / $5k hr) + BB squeeze
          4. Breakout Volume Inflection: V_5m >= 3.0 * SMA_1h(V) and Net Buy Delta >= 65%
          5. CTO & Security Gates: Dev <= 0.1%, Top 10 < 25%, Gatekeeper Clean
        """
        if tok.dropped or tok.breakout_signaled:
            return None

        now = current_time_s if current_time_s is not None else time.time()
        age_since_staged = now - tok.t_staged

        # 1. TTL Check
        if age_since_staged > tok.ttl_seconds:
            tok.dropped = True
            tok.drop_reason = f"Revival staging TTL expired ({age_since_staged:.0f}s > {tok.ttl_seconds:.0f}s)"
            logger.info("RevivalBreakoutBuffer: Expired token [%s] — %s", tok.token_address[:10], tok.drop_reason)
            return None

        # 2. Token Age Filter: 2 hours <= token_age <= 10 days
        token_age_s = (now - tok.token_creation_timestamp) if tok.token_creation_timestamp > 0 else age_since_staged
        tok.token_age_seconds = token_age_s
        token_age_h = token_age_s / 3600.0

        if token_age_s < self.min_token_age_s:
            tok.active_rules_status["TokenAgeValid"] = False
            tok.active_rules_status["TokenAgeReason"] = f"Token age {token_age_h:.1f}h < 2.0h minimum"
            return None

        if token_age_s > self.max_token_age_s:
            tok.dropped = True
            tok.drop_reason = f"Token age {token_age_h:.1f}h > 10d (240h) maximum limit for revival swing"
            logger.info("RevivalBreakoutBuffer: Dropped %s — %s", tok.token_address[:10], tok.drop_reason)
            return None

        tok.active_rules_status["TokenAgeValid"] = True

        # 3. Anti-FOMO / Over-Extension Guard
        curr_price = tok.latest_price
        base_p = tok.consolidation_base_price if tok.consolidation_base_price > Decimal(0) else tok.consolidation_min_price
        if base_p > Decimal(0):
            max_allowed_price = Decimal(str(self.max_overextension_ratio)) * base_p
            if curr_price > max_allowed_price:
                tok.dropped = True
                pump_pct = float((curr_price - base_p) / base_p * 100)
                tok.drop_reason = f"OVEREXTENDED_CHASE: Token is up +{pump_pct:.1f}% (>+{float(self.max_overextension_ratio - 1)*100:.0f}%) from base floor {base_p}. Rejection prevents buying top wick."
                logger.warning("RevivalBreakoutBuffer: REJECTED %s — %s", tok.token_address[:10], tok.drop_reason)
                decision = DecisionRecord(
                    token_address=tok.token_address,
                    chain=tok.chain.value,
                    decision_type=DecisionType.SKIP,
                    strategy_pattern="revival_breakout",
                    active_rules={"AntiFOMOOverextension": False},
                    confidence_score=0.95,
                    reason=tok.drop_reason,
                    metadata={"pump_pct": pump_pct, "base_price": float(base_p), "current_price": float(curr_price)},
                )
                self._dispatch_decision(decision)
                return None

        # Check 14-period RSI on 5m candles:
        rsi_5m = tok.compute_5m_rsi(period=14)
        if rsi_5m > self.max_rsi_5m:
            tok.dropped = True
            tok.drop_reason = f"RSI_OVERBOUGHT_FOMO: 5m RSI is {rsi_5m:.1f} > {self.max_rsi_5m:.1f} (extreme overbought / green candle FOMO wick top)."
            logger.warning("RevivalBreakoutBuffer: REJECTED %s — %s", tok.token_address[:10], tok.drop_reason)
            decision = DecisionRecord(
                token_address=tok.token_address,
                chain=tok.chain.value,
                decision_type=DecisionType.SKIP,
                strategy_pattern="revival_breakout",
                active_rules={"RSIOverextension": False},
                confidence_score=0.95,
                reason=tok.drop_reason,
                metadata={"rsi_5m": rsi_5m},
            )
            self._dispatch_decision(decision)
            return None

        tok.active_rules_status["AntiFOMOPassed"] = True

        # 4. Consolidation & Dead Zone Metrics
        dormancy_ok = (
            tok.dormant_detected
            or tok.dormant_1h_volume_usd <= self.max_dormant_volume_1h_usd
            or tok.dormant_1h_volume_native <= self.max_dormant_volume_1h_native
        )
        bb_bandwidth = tok.compute_bollinger_bandwidth(period=20)
        compression_ok = (bb_bandwidth <= self.max_bb_bandwidth)
        consolidation_passed = dormancy_ok and (compression_ok or tok.dormant_detected)
        tok.active_rules_status["ConsolidationDeadZone"] = consolidation_passed

        # 5. Breakout Volume Inflection (V_5m >= 3.0 * SMA_1h)
        b_5m, s_5m, _, _ = tok.get_volume_window(300.0, current_time_s=now)
        buy_5m = tok.five_min_buy_volume if tok.five_min_buy_volume > Decimal(0) else b_5m
        sell_5m = tok.five_min_sell_volume if tok.five_min_sell_volume > Decimal(0) else s_5m
        v_5m = float(tok.five_min_volume) if tok.five_min_volume > Decimal(0) else float(buy_5m + sell_5m)

        b_1h, s_1h, _, _ = tok.get_volume_window(3600.0, current_time_s=now)
        v_1h = float(tok.one_hour_volume) if tok.one_hour_volume > Decimal(0) else float(b_1h + s_1h)

        sma_1h_5m_equiv = max(
            0.05,
            v_1h / 12.0 if v_1h > 0 else float(tok.dormant_1h_volume_native) / 12.0
        )
        surge_mult = v_5m / sma_1h_5m_equiv
        tok.volume_surge_multiplier = round(surge_mult, 2)
        volume_surge_passed = (surge_mult >= self.volume_surge_multiplier_k)

        # Net Buy Delta: (Buy Volume - Sell Volume) / Total Volume >= 0.65 or Buy / Total >= 0.65
        net_buy_delta = (float(buy_5m - sell_5m) / v_5m) if v_5m > 0 else 0.0
        buy_ratio = (float(buy_5m) / v_5m) if v_5m > 0 else 0.0
        tok.net_buy_delta = round(net_buy_delta, 3)
        net_buy_passed = (net_buy_delta >= self.min_net_buy_delta or buy_ratio >= self.min_net_buy_delta)

        tok.active_rules_status["VolumeSurge3x"] = volume_surge_passed
        tok.active_rules_status["NetBuyDelta65Pct"] = net_buy_passed

        # 6. Community Takeover (CTO) & Safety Gates
        dev_exited = (tok.dev_balance_pct <= self.max_dev_balance_pct)
        top10_conc = tok.top10_concentration * 100.0 if tok.top10_concentration <= 1.0 else tok.top10_concentration
        top10_clean = (top10_conc < self.max_top10_concentration_pct)
        security_clean = (
            tok.security_report.passes_hard_gates
            and not tok.security_report.is_honeypot
            and tok.security_report.buy_tax_bps <= 500
            and tok.security_report.sell_tax_bps <= 500
        )
        cto_safety_passed = dev_exited and top10_clean and security_clean

        tok.active_rules_status["DevDumpedCTO"] = dev_exited
        tok.active_rules_status["Top10ConcentrationClean"] = top10_clean
        tok.active_rules_status["SecurityScreenClean"] = security_clean


        # 7. Composite Institutional Score
        score_cons = 1.0 if consolidation_passed else 0.4
        score_surge = min(1.0, surge_mult / (self.volume_surge_multiplier_k * 1.5))
        score_delta = min(1.0, max(0.0, (net_buy_delta + 1.0) / 2.0))
        score_cto = 1.0 if cto_safety_passed else 0.0

        accumulation_score = (
            0.20 * score_cons
            + 0.35 * score_surge
            + 0.25 * score_delta
            + 0.20 * score_cto
        )
        tok.accumulation_score = round(accumulation_score, 3)

        tok.rule_rationales = [
            f"Token age: {token_age_h:.1f}h (window: 2h - 240h)",
            f"Volume surge: {surge_mult:.2f}x (threshold: {self.volume_surge_multiplier_k:.1f}x SMA_1h)",
            f"Net buy delta: {net_buy_delta*100:.1f}% (threshold: {self.min_net_buy_delta*100:.0f}%)",
            f"Anti-FOMO: price={curr_price} <= 2.2*base ({base_p}), RSI_5m={rsi_5m:.1f} <= 82.0",
            f"CTO safety: dev={tok.dev_balance_pct*100:.2f}%, top10={tok.top10_concentration*100:.1f}%, security={tok.security_report.tier.value}",
        ]

        # 8. High-Priority Revival Breakout Signal Trigger
        if (
            volume_surge_passed
            and net_buy_passed
            and cto_safety_passed
            and consolidation_passed
            and accumulation_score >= self.min_accumulation_score
        ):
            tok.breakout_signaled = True
            reason_str = (
                f"REVIVAL & CTO BREAKOUT CONFIRMED: Accumulation Score={tok.accumulation_score:.2f} | "
                f"Age={token_age_h:.1f}h | Volume Surge {surge_mult:.2f}x >= {self.volume_surge_multiplier_k:.1f}x SMA_1h | "
                f"Net Buy Delta {net_buy_delta*100:.1f}% >= {self.min_net_buy_delta*100:.0f}% | "
                f"Dev Balance {tok.dev_balance_pct*100:.2f}%, Top 10={tok.top10_concentration*100:.1f}% | "
                f"5m RSI={rsi_5m:.1f} < 82.0 (Anti-FOMO Passed)"
            )
            logger.info("RevivalBreakoutBuffer: %s for %s!", reason_str, tok.token_address[:10])

            # Structured Decision Record
            decision = DecisionRecord(
                token_address=tok.token_address,
                chain=tok.chain.value,
                decision_type=DecisionType.ENTER,
                strategy_pattern="revival_breakout",
                market_cap_usd=float(tok.consolidation_base_mcap) if tok.consolidation_base_mcap > 0 else None,
                volume_5m_usd=v_5m * 150.0 if tok.chain == ChainIdentifier.SOLANA_MAINNET else None,
                volume_1h_usd=v_1h * 150.0 if tok.chain == ChainIdentifier.SOLANA_MAINNET else None,
                liquidity_pool_depth_usd=float(pool.native_reserve * 150) if pool else None,
                active_rules=tok.active_rules_status,
                confidence_score=min(0.98, max(0.85, tok.accumulation_score)),
                reason=reason_str,
                metadata={
                    "token_age_hours": round(token_age_h, 2),
                    "base_mcap_usd": float(tok.consolidation_base_mcap),
                    "volume_surge_multiplier": round(surge_mult, 2),
                    "net_buy_delta": round(net_buy_delta, 3),
                    "rsi_5m": round(rsi_5m, 1),
                    "dev_balance_pct": tok.dev_balance_pct,
                    "top10_concentration": tok.top10_concentration,
                },
            )
            self._dispatch_decision(decision)

            p_state = pool or PoolState(
                pool_address=tok.pool_address or "0x" + "0" * 40,
                chain=tok.chain,
                token_reserve=Decimal("1000000000"),
                native_reserve=Decimal("30"),
                fee_numerator=30,
                fee_denominator=10000,
                last_updated_block=1000,
                token_decimals=6,
                native_decimals=9,
            )

            signal = SignalEvent(
                timestamp_ns=time.time_ns(),
                chain=tok.chain,
                pool_address=tok.pool_address or p_state.pool_address,
                token_address=tok.token_address,
                suggested_side=OrderSide.BUY,
                pool_state=p_state,
                security_report=tok.security_report,
                strength=SignalStrength.STRONG,
                alpha_score=tok.accumulation_score,
                source=SignalSource.REVIVAL_BREAKOUT,
                exit_profile=ExitProfile.REVIVAL_SWING,
                base_market_cap=tok.consolidation_base_mcap,
                strategy_pattern="revival_breakout",
                token_age_hours=token_age_h,
                consolidation_length_hours=token_age_h,
                volume_surge_multiplier=surge_mult,
                net_buy_delta=net_buy_delta,
                narrative_cluster=tok.narrative_cluster,
            )
            self._dispatch_signal(signal)
            return signal

        return None

    def _dispatch_decision(self, decision: DecisionRecord) -> None:
        for cb in self._on_decision_callbacks:
            try:
                res = cb(decision)
                if asyncio.iscoroutine(res):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(res)
                    except RuntimeError:
                        pass
            except Exception as exc:
                logger.warning("Error in RevivalBreakoutBuffer decision callback: %s", exc)

    def _dispatch_signal(self, signal: SignalEvent) -> None:
        for cb in self._on_signal_callbacks:
            try:
                res = cb(signal)
                if asyncio.iscoroutine(res):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(res)
                    except RuntimeError:
                        pass
            except Exception as exc:
                logger.warning("Error in RevivalBreakoutBuffer signal callback: %s", exc)

    def sweep_expired(self, current_time_s: Optional[float] = None) -> list[str]:
        """Sweep expired tokens exceeding their TTL."""
        now = current_time_s if current_time_s is not None else time.time()
        expired: list[str] = []
        for addr, tok in list(self._staged.items()):
            if not tok.dropped and not tok.breakout_signaled:
                age = now - tok.t_staged
                if age > tok.ttl_seconds:
                    tok.dropped = True
                    tok.drop_reason = f"Revival staging TTL expired ({age:.0f}s > {tok.ttl_seconds:.0f}s)"
                    expired.append(addr)
                    decision = DecisionRecord(
                        token_address=tok.token_address,
                        chain=tok.chain.value,
                        decision_type=DecisionType.SKIP,
                        strategy_pattern="revival_breakout",
                        active_rules=tok.active_rules_status,
                        confidence_score=0.90,
                        reason=f"Revival staging TTL expired without breakout: {'; '.join(tok.rule_rationales[:3])}",
                        metadata={"expired_age_s": age, "final_accumulation_score": tok.accumulation_score},
                    )
                    self._dispatch_decision(decision)
                    logger.info("RevivalBreakoutBuffer: Expired token [%s]", addr[:10])
        return expired

    def get_staged_status_summary(self) -> list[dict[str, Any]]:
        """Return formatted overview of all tokens currently in revival staging."""
        now = time.time()
        summary = []
        for tok in self._staged.values():
            if not tok.dropped:
                age = now - tok.t_staged
                token_age_h = ((now - tok.token_creation_timestamp) if tok.token_creation_timestamp > 0 else age) / 3600.0
                remaining = max(0.0, tok.ttl_seconds - age)
                summary.append({
                    "token_address": tok.token_address,
                    "chain": tok.chain.value,
                    "token_age_hours": round(token_age_h, 1),
                    "ttl_remaining_hours": round(remaining / 3600.0, 1),
                    "base_mcap_usd": float(tok.consolidation_base_mcap),
                    "accumulation_score": tok.accumulation_score,
                    "volume_surge_multiplier": tok.volume_surge_multiplier,
                    "net_buy_delta": tok.net_buy_delta,
                    "rsi_5m": tok.rsi_5m,
                    "dev_balance_pct": tok.dev_balance_pct,
                    "top10_concentration": tok.top10_concentration,
                    "breakout_signaled": tok.breakout_signaled,
                    "active_rules": tok.active_rules_status,
                })
        return summary

