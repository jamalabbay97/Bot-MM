"""
alpha_engine.engine.signals — Alpha Signal Generation & Scoring
===============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Optional

from alpha_engine.math.sizing import classify_alpha_score
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
)
from alpha_engine.models.events import RawSignalEvent, SignalEvent, SwapEvent
from alpha_engine.models.state import PoolState, SecurityReport

logger = logging.getLogger(__name__)


@dataclass
class StagedLaunch:
    token_address: str
    chain: ChainIdentifier
    pool_address: str
    t_0: float
    initial_price: Decimal
    latest_price: Decimal
    min_price: Decimal
    report: SecurityReport
    raw_signal: RawSignalEvent
    buy_count: int = 0
    total_volume_native: Decimal = Decimal(0)
    graduated: bool = False
    dropped: bool = False
    drop_reason: str = ""
    unique_buyers: set[str] = field(default_factory=set)
    slot_buy_counts: dict[int, int] = field(default_factory=lambda: defaultdict(int))
    buy_blocks: set[int] = field(default_factory=set)
    buy_volumes: list[Decimal] = field(default_factory=list)
    is_dev_bundled: bool = False
    last_telemetry_s: float = 0.0


class PendingLaunchBuffer:
    """
    In-memory staging buffer for new token launches (e.g. Pump.fun mints).
    Prevents instant mint sniping and enforces observation window, volume, and anti-bundle criteria:
      - Minimum age = 30 seconds, Maximum age = 90 seconds from mint creation (t_0).
      - Sustained swap activity: minimum of 3 buy transactions.
      - Confirmed volume spike: >= 0.8 SOL (or native) cumulative volume within window.
      - Dump filter: drops immediately if price dumps > 30% from initial bonding curve within first 30s.
      - Unique Buyer Entropy: >= min_unique_signers (default 2) distinct buyer wallets.
      - Slot Clustering / Bundle Detection: drops if >= 60% of buys cluster in the exact same slot.
      - Curve Completion Delta: verifies volume growth spans across multiple blocks (>= min_blocks_span).
    """

    def __init__(
        self,
        min_age_s: float = 30.0,
        max_age_s: float = 90.0,
        min_buys: int = 3,
        min_volume_native: Decimal = Decimal("0.8"),
        max_dump_pct: Decimal = Decimal("0.30"),
        min_unique_signers: int = 2,
        slot_bundle_threshold: float = 0.60,
        min_blocks_span: int = 3,
    ) -> None:
        self.min_age_s = min_age_s
        self.max_age_s = max_age_s
        self.min_buys = min_buys
        self.min_volume_native = min_volume_native
        self.max_dump_pct = max_dump_pct
        self.min_unique_signers = min_unique_signers
        self.slot_bundle_threshold = slot_bundle_threshold
        self.min_blocks_span = min_blocks_span
        self._staged: dict[str, StagedLaunch] = {}
        self._on_removal_callbacks: list[Callable[[str], Any]] = []

    def register_on_removal_callback(self, callback: Callable[[str], Any]) -> None:
        """Register callback invoked whenever a staged token is dropped, graduated, or removed."""
        if callback not in self._on_removal_callbacks:
            self._on_removal_callbacks.append(callback)

    def _notify_removal(self, token_address: str) -> None:
        """Dispatch removal callbacks asynchronously or synchronously."""
        for cb in self._on_removal_callbacks:
            try:
                res = cb(token_address)
                if asyncio.iscoroutine(res):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(res)
                    except RuntimeError:
                        pass
            except Exception as exc:
                logger.warning("Error in PendingLaunchBuffer removal callback for %s: %s", token_address[:10], exc)

    def remove_launch(self, token_address: str, reason: str = "manual_removal") -> StagedLaunch | None:
        """Manually remove and drop a launch, triggering removal callbacks."""
        staged = self._staged.get(token_address)
        if staged is not None and not staged.dropped:
            staged.dropped = True
            staged.drop_reason = reason
            self._notify_removal(token_address)
        return staged

    def add_launch(
        self,
        token_address: str,
        chain: ChainIdentifier,
        pool_address: str,
        initial_price: Decimal,
        report: SecurityReport,
        raw_signal: RawSignalEvent,
        t_0: float | None = None,
    ) -> StagedLaunch:
        now_s = t_0 if t_0 is not None else time.time()
        init_p = initial_price if initial_price > 0 else Decimal("0.000000028")
        staged = StagedLaunch(
            token_address=token_address,
            chain=chain,
            pool_address=pool_address,
            t_0=now_s,
            initial_price=init_p,
            latest_price=init_p,
            min_price=init_p,
            report=report,
            raw_signal=raw_signal,
            last_telemetry_s=now_s,
        )
        self._staged[token_address] = staged
        logger.info(
            "PendingLaunchBuffer: Staged launch for %s (t_0=%.1f, initial_price=%s)",
            token_address[:10],
            now_s,
            staged.initial_price,
        )
        return staged

    def stage_token(
        self,
        token_address: str,
        chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
        pool_address: str = "",
        initial_price: Decimal = Decimal("0.000000028"),
        report: SecurityReport | None = None,
        raw_signal: RawSignalEvent | None = None,
        t_0: float | None = None,
    ) -> StagedLaunch:
        """Stage a newly evaluated token launch into the observation buffer."""
        rep = report or SecurityReport(
            token_address=token_address,
            chain=chain,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
            passes_hard_gates=True,
        )
        sig = raw_signal or RawSignalEvent(
            chain=chain,
            token_address=token_address,
            pool_address=pool_address,
            source=SignalSource.PUMP_FUN_MINT,
        )
        return self.add_launch(
            token_address=token_address,
            chain=chain,
            pool_address=pool_address,
            initial_price=initial_price,
            report=rep,
            raw_signal=sig,
            t_0=t_0,
        )

    def is_staged(self, token_address: str) -> bool:
        launch = self._staged.get(token_address)
        return launch is not None and not launch.dropped and not launch.graduated

    def get_staged(self, token_address: str) -> StagedLaunch | None:
        return self._staged.get(token_address)

    def _evaluate_unmet_conditions(self, staged: StagedLaunch) -> list[str]:
        """Compile a list of exact graduation conditions currently unmet for a staged launch."""
        unmet: list[str] = []
        if staged.total_volume_native < self.min_volume_native:
            unmet.append(f"Vol: {staged.total_volume_native:.1f}/{self.min_volume_native:.1f} SOL")
        if staged.buy_count < self.min_buys:
            unmet.append(f"Buys: {staged.buy_count}/{self.min_buys}")
        if len(staged.unique_buyers) < self.min_unique_signers:
            unmet.append(f"Buyers: {len(staged.unique_buyers)}/{self.min_unique_signers}")
        if len(staged.buy_blocks) < self.min_blocks_span:
            unmet.append(f"Blocks: {len(staged.buy_blocks)}/{self.min_blocks_span}")
        if staged.is_dev_bundled:
            unmet.append("DEV_BUNDLED (slot clustering)")
        elif staged.buy_count > 0:
            max_slot_buys = max(staged.slot_buy_counts.values()) if staged.slot_buy_counts else 0
            slot_cluster_pct = max_slot_buys / staged.buy_count
            if slot_cluster_pct >= self.slot_bundle_threshold:
                unmet.append(f"Slot clustering ({slot_cluster_pct:.1%} >= {self.slot_bundle_threshold:.1%})")
        if staged.initial_price > Decimal(0):
            dump_pct = (staged.initial_price - staged.latest_price) / staged.initial_price
            if dump_pct > self.max_dump_pct:
                unmet.append(f"Price dump ({float(dump_pct*100):.1f}% > {float(self.max_dump_pct*100):.0f}%)")
        return unmet

    def check_telemetry(self, current_time_s: float | None = None, interval_s: float = 15.0) -> None:
        """
        Telemetry: Emits countdown and progress logging every interval_s seconds (default 15s)
        for all actively staged tokens.
        """
        now_s = current_time_s if current_time_s is not None else time.time()
        for staged in list(self._staged.values()):
            if not staged.dropped and not staged.graduated:
                age = now_s - staged.t_0
                if age <= self.max_age_s:
                    if staged.last_telemetry_s == 0.0 or (now_s - staged.last_telemetry_s) >= interval_s:
                        staged.last_telemetry_s = now_s
                        remaining_s = max(0.0, self.max_age_s - age)
                        logger.info(
                            "PendingLaunchBuffer: Token [%s]: %ds elapsed (%ds remaining) | Vol: %.1f/%.1f SOL | Buys: %d/%d | Buyers: %d/%d",
                            staged.token_address[:10],
                            int(age),
                            int(remaining_s),
                            staged.total_volume_native,
                            self.min_volume_native,
                            staged.buy_count,
                            self.min_buys,
                            len(staged.unique_buyers),
                            self.min_unique_signers,
                        )

    def record_trade(
        self,
        token_address: str | SwapEvent,
        sol_amount: Decimal | None = None,
        buyer: str = "",
        slot: int = 0,
        is_buy: bool = True,
        current_time_s: float | None = None,
        pool: PoolState | None = None,
        signal_generator: SignalGenerator | None = None,
    ) -> SignalEvent | None:
        """
        Record a trade (buy/sell) on the bonding curve or AMM for an actively staged token.
        Correctly aggregates:
          - Total SOL volume (native currency).
          - Total buy count.
          - Set of unique buyer addresses.
        Checks slot clustering, dynamic telemetry, dump threshold, and graduation criteria.
        Supports passing either a SwapEvent instance or explicit trade parameters.
        """
        if isinstance(token_address, SwapEvent):
            swap = token_address
            staged = self._staged.get(swap.token_out) or self._staged.get(swap.token_in)
            if staged is None or staged.dropped or staged.graduated:
                return None
            tok_addr = staged.token_address
            is_buy = (swap.token_out == tok_addr)
            sol_amount = swap.amount_in if is_buy else swap.amount_out
            buyer = swap.sender if is_buy else ""
            slot = swap.block_number
        else:
            tok_addr = token_address
            staged = self._staged.get(tok_addr)
            if staged is None or staged.dropped or staged.graduated:
                return None
            if sol_amount is None:
                sol_amount = Decimal("0")

        now_s = current_time_s if current_time_s is not None else time.time()
        age = now_s - staged.t_0

        if pool is not None:
            if not staged.pool_address and pool.pool_address:
                staged.pool_address = pool.pool_address
            spot_price = pool.spot_price_native_per_token
            if spot_price > 0:
                staged.latest_price = spot_price
                if staged.min_price <= 0 or spot_price < staged.min_price:
                    staged.min_price = spot_price

        # Update volume and trade stats
        staged.total_volume_native += sol_amount
        if is_buy:
            staged.buy_count += 1
            if buyer:
                staged.unique_buyers.add(buyer)
            if slot > 0:
                staged.slot_buy_counts[slot] += 1
                staged.buy_blocks.add(slot)
            staged.buy_volumes.append(sol_amount)

            # Slot Clustering / Bundle Detection:
            if staged.buy_count >= min(self.min_buys, 5):
                max_slot_buys = max(staged.slot_buy_counts.values()) if staged.slot_buy_counts else 0
                slot_cluster_pct = max_slot_buys / staged.buy_count
                if slot_cluster_pct >= self.slot_bundle_threshold:
                    staged.is_dev_bundled = True
                    staged.dropped = True
                    staged.drop_reason = (
                        f"DEV_BUNDLED: Slot clustering {slot_cluster_pct:.1%} >= {self.slot_bundle_threshold:.1%} "
                        f"({max_slot_buys}/{staged.buy_count} in block {slot})"
                    )
                    self._notify_removal(staged.token_address)
                    logger.warning(
                        "PendingLaunchBuffer: DROPPING launch %s — %s (Jito bundle footprint)",
                        staged.token_address[:10],
                        staged.drop_reason,
                    )
                    return None

        # Telemetry: emit countdown and progress logging every 15 seconds
        if age <= self.max_age_s and (staged.last_telemetry_s == 0.0 or (now_s - staged.last_telemetry_s) >= 15.0):
            staged.last_telemetry_s = now_s
            remaining_s = max(0.0, self.max_age_s - age)
            logger.info(
                "PendingLaunchBuffer: Token [%s]: %ds elapsed (%ds remaining) | Vol: %.1f/%.1f SOL | Buys: %d/%d | Buyers: %d/%d",
                staged.token_address[:10],
                int(age),
                int(remaining_s),
                staged.total_volume_native,
                self.min_volume_native,
                staged.buy_count,
                self.min_buys,
                len(staged.unique_buyers),
                self.min_unique_signers,
            )

        # Dump filter: drops if price dumps > max_dump_pct from initial bonding curve within the first min_age_s
        if staged.initial_price > Decimal(0):
            dump_pct = (staged.initial_price - staged.latest_price) / staged.initial_price
            if dump_pct > self.max_dump_pct and age <= self.min_age_s:
                staged.dropped = True
                staged.drop_reason = f"DUMP > {float(self.max_dump_pct*100):.0f}% ({float(dump_pct*100):.1f}%) within {age:.1f}s"
                self._notify_removal(staged.token_address)
                logger.warning(
                    "PendingLaunchBuffer: DROPPING launch %s — %s (initial=%s, curr=%s)",
                    staged.token_address[:10],
                    staged.drop_reason,
                    staged.initial_price,
                    staged.latest_price,
                )
                return None

        # Expired observation window (> max_age_s)
        if age > self.max_age_s:
            unmet = self._evaluate_unmet_conditions(staged)
            unmet_str = " | ".join(unmet) if unmet else "Criteria not satisfied"
            staged.dropped = True
            staged.drop_reason = f"Observation window expired ({age:.1f}s > {self.max_age_s}s) - Unmet: [{unmet_str}]"
            self._notify_removal(staged.token_address)
            logger.info("PendingLaunchBuffer: DROPPING launch %s — %s", staged.token_address[:10], staged.drop_reason)
            return None

        # Graduation criteria check:
        if age >= self.min_age_s and not staged.is_dev_bundled:
            max_slot_buys = max(staged.slot_buy_counts.values()) if staged.slot_buy_counts else 0
            slot_cluster_pct = (max_slot_buys / staged.buy_count) if staged.buy_count > 0 else 0.0

            if (
                staged.buy_count >= self.min_buys
                and staged.total_volume_native >= self.min_volume_native
                and staged.latest_price >= staged.initial_price * (Decimal(1) - self.max_dump_pct)
                and len(staged.unique_buyers) >= self.min_unique_signers
                and slot_cluster_pct < self.slot_bundle_threshold
                and len(staged.buy_blocks) >= self.min_blocks_span
            ):
                staged.graduated = True
                self._notify_removal(staged.token_address)
                logger.info(
                    "PendingLaunchBuffer: GRADUATING launch %s! Age=%.1fs, buys=%d, unique_buyers=%d, blocks=%d, vol=%s native",
                    staged.token_address[:10],
                    age,
                    staged.buy_count,
                    len(staged.unique_buyers),
                    len(staged.buy_blocks),
                    staged.total_volume_native,
                )
                if signal_generator is not None and pool is not None:
                    return signal_generator.generate_launch_signal(
                        staged=staged,
                        pool=pool,
                    )

        return None

    def record_swap(
        self,
        swap: SwapEvent,
        pool: PoolState,
        signal_generator: SignalGenerator,
        current_time_s: float | None = None,
    ) -> SignalEvent | None:
        return self.record_trade(
            token_address=swap,
            pool=pool,
            signal_generator=signal_generator,
            current_time_s=current_time_s,
        )

    def sweep_expired(self, current_time_s: float | None = None) -> list[str]:
        now_s = current_time_s if current_time_s is not None else time.time()
        expired: list[str] = []
        for token, staged in list(self._staged.items()):
            if not staged.dropped and not staged.graduated:
                age = now_s - staged.t_0
                if age > self.max_age_s:
                    unmet = self._evaluate_unmet_conditions(staged)
                    unmet_str = " | ".join(unmet) if unmet else "Criteria not satisfied"
                    staged.dropped = True
                    staged.drop_reason = f"Observation window expired ({age:.1f}s > {self.max_age_s}s) - Unmet: [{unmet_str}]"
                    self._notify_removal(token)
                    logger.info(
                        "PendingLaunchBuffer: DROPPING launch %s — %s",
                        staged.token_address[:10],
                        staged.drop_reason,
                    )
                    expired.append(token)
        return expired


class SignalGenerator:
    """
    Converts a screened SwapEvent + SecurityReport into a SignalEvent.
    """

    def __init__(
        self,
        hold_seconds: float = 300.0,
    ) -> None:
        self._hold_s = hold_seconds

    def validate_signal_strength(
        self,
        signal_event: SignalEvent,
        min_strength: Optional[SignalStrength] = None,
    ) -> bool:
        """
        Validate whether the signal meets the minimum strength threshold.
        """
        if signal_event.suggested_side == OrderSide.SELL:
            return True
        if min_strength is None:
            return True

        rank = {
            SignalStrength.WEAK: 1,
            SignalStrength.MODERATE: 2,
            SignalStrength.STRONG: 3,
        }
        return rank.get(signal_event.strength, 0) >= rank.get(min_strength, 0)

    def score_swap(
        self,
        swap: SwapEvent,
        pool: PoolState,
        report: SecurityReport,
    ) -> float:
        """Compute a normalised alpha score [0.0, 1.0] for a swap event."""
        volume_ratio = float(
            min(Decimal("1"), swap.amount_in / pool.native_reserve)
        )
        spot = pool.spot_price_native_per_token
        exec_p = swap.amount_out / swap.amount_in if swap.amount_in > 0 else spot
        impact_raw = abs(1.0 - float(exec_p / spot)) if float(spot) > 0 else 0.0
        inverse_impact = max(0.0, 1.0 - impact_raw)

        tier_multiplier = 1.0 if report.tier == SecurityTier.CLEAN else 0.5

        raw_score = (0.6 * volume_ratio + 0.4 * inverse_impact) * tier_multiplier
        return min(1.0, max(0.0, raw_score))

    def generate_buy_signal(
        self,
        swap: SwapEvent,
        pool: PoolState,
        report: SecurityReport,
        source: SignalSource = SignalSource.DEX_SWAP,
        min_strength: Optional[SignalStrength] = None,
    ) -> SignalEvent | None:
        """Generate a BUY SignalEvent for a qualifying swap."""
        alpha = self.score_swap(swap, pool, report)
        if alpha < 0.10:
            logger.debug(
                "Alpha score %.4f too low for %s — skipping signal.",
                alpha,
                swap.tx_hash[:12],
            )
            return None

        strength_str = classify_alpha_score(alpha)
        strength = SignalStrength(strength_str)

        signal = SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=swap.chain,
            pool_address=swap.pool_address,
            token_address=swap.token_out,
            suggested_side=OrderSide.BUY,
            trigger_swap=swap,
            pool_state=pool,
            security_report=report,
            strength=strength,
            alpha_score=alpha,
            source=source,
        )

        if not self.validate_signal_strength(signal, min_strength=min_strength):
            logger.debug("Signal rejected: strength %s below threshold %s", strength, min_strength)
            return None

        return signal

    def generate_social_signal(
        self,
        raw_signal: Any,
        pool: PoolState,
        report: SecurityReport,
        social_weight: float = 1.0,
    ) -> SignalEvent | None:
        """Generate a BUY SignalEvent from social sentiment (X or Telegram) with dynamic weight adaptation."""
        tier_multiplier = 1.0 if report.tier == SecurityTier.CLEAN else 0.5
        alpha = min(1.0, max(0.0, 0.85 * social_weight * tier_multiplier))

        if alpha < 0.10:
            logger.debug(
                "Social alpha score %.4f too low for %s — skipping signal.",
                alpha,
                getattr(raw_signal, "token_address", "")[:10],
            )
            return None

        strength_str = classify_alpha_score(alpha)
        strength = SignalStrength(strength_str)

        source = getattr(raw_signal, "source", SignalSource.X_SENTIMENT)

        return SignalEvent(
            timestamp_ns=getattr(raw_signal, "timestamp_ns", time.time_ns()),
            chain=raw_signal.chain,
            pool_address=pool.pool_address,
            token_address=raw_signal.token_address,
            suggested_side=OrderSide.BUY,
            pool_state=pool,
            security_report=report,
            strength=strength,
            alpha_score=alpha,
            source=source,
        )

    def generate_launch_signal(
        self,
        staged: StagedLaunch,
        pool: PoolState,
    ) -> SignalEvent:
        """Generate a validated BUY SignalEvent from a graduated smart launch."""
        tier_multiplier = 1.0 if staged.report.tier == SecurityTier.CLEAN else 0.5
        alpha = min(1.0, max(0.0, 0.90 * tier_multiplier))
        strength_str = classify_alpha_score(alpha)
        strength = SignalStrength(strength_str)
        return SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=staged.chain,
            pool_address=staged.pool_address or pool.pool_address,
            token_address=staged.token_address,
            suggested_side=OrderSide.BUY,
            pool_state=pool,
            security_report=staged.report,
            strength=strength,
            alpha_score=alpha,
            source=SignalSource.PUMP_FUN_MINT,
        )

    def generate_sell_signal(
        self,
        token_address: str,
        chain: ChainIdentifier,
        pool: PoolState,
        pool_address: str,
        report: SecurityReport,
        trigger_swap: SwapEvent,
    ) -> SignalEvent:
        """Generate a SELL SignalEvent for position exit."""
        return SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=chain,
            pool_address=pool_address,
            token_address=token_address,
            suggested_side=OrderSide.SELL,
            trigger_swap=trigger_swap,
            pool_state=pool,
            security_report=report,
            strength=SignalStrength.STRONG,
            alpha_score=1.0,
        )
