"""
alpha_engine.profiler.evaluator — Smart Money Behavioral Rules & Anomaly Detector
================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import statistics
import time
from decimal import Decimal
from typing import Optional, Sequence

from alpha_engine.models.enums import ChainIdentifier, WalletClassification
from alpha_engine.models.profiler import (
    FundingHop,
    InitialTxRecord,
    WalletProfile,
    WalletTradeRecord,
)


class WalletEvaluator:
    """
    Evaluates historical closed trades and initial funding provenance to classify
    target wallets and filter out traps (insiders, wash-traders, MEV bots, lucky outliers,
    and revived dead wallets).
    """

    # Rule Thresholds
    MIN_CLOSED_TRADES: int = 15
    MIN_WIN_RATE_PCT: float = 55.0
    MIN_ACTIVE_DAYS: float = 21.0
    MAX_OUTLIER_PNL_RATIO: float = 0.60
    MIN_MEDIAN_HOLDING_TIME_S: float = 45.0
    MAX_INACTIVE_DAYS_REVIVAL: float = 45.0
    MAX_FUNDING_HOPS: int = 3

    @classmethod
    def evaluate(
        cls,
        wallet_address: str,
        chain: ChainIdentifier,
        trades: Sequence[WalletTradeRecord],
        initial_txs: Optional[Sequence[InitialTxRecord]] = None,
        funding_hops: Optional[Sequence[FundingHop]] = None,
        current_timestamp: Optional[int] = None,
        cluster_tag: Optional[str] = None,
    ) -> WalletProfile:
        """
        Execute full behavioral evaluation and return a WalletProfile.
        """
        if current_timestamp is None:
            current_timestamp = int(time.time())

        rejection_reasons: list[str] = []
        classification = WalletClassification.APPROVED

        # -------------------------------------------------------------
        # 1. Evaluate Wash-Trading & Self-Funding Provenance (<= 3 hops)
        # -------------------------------------------------------------
        insider_detected = False
        detected_hops: list[FundingHop] = list(funding_hops) if funding_hops else []

        if detected_hops:
            for hop in detected_hops:
                if hop.hop_depth <= cls.MAX_FUNDING_HOPS:
                    if hop.is_deployer:
                        insider_detected = True
                        rejection_reasons.append(
                            f"Insider funding detected: hop {hop.hop_depth} traces to token deployer ({hop.source_address})"
                        )
                        break
                    elif hop.is_multisig:
                        insider_detected = True
                        rejection_reasons.append(
                            f"Shared multi-sig/treasury funding detected: hop {hop.hop_depth} traces to multi-sig ({hop.source_address})"
                        )
                        break

        # Also inspect the first 5 transactions for contract creation or direct self-funding
        if initial_txs:
            first_5 = list(initial_txs)[:5]
            for tx in first_5:
                if tx.is_contract_creation:
                    insider_detected = True
                    rejection_reasons.append(
                        f"Deployer detected in first 5 transactions (tx: {tx.tx_hash})"
                    )
                    break

        if insider_detected:
            classification = WalletClassification.INSIDER

        # -------------------------------------------------------------
        # 2. Minimum Trades & Win Rate Metrics
        # -------------------------------------------------------------
        total_trades = len(trades)
        winning_trades = sum(1 for t in trades if t.is_win)
        losing_trades = total_trades - winning_trades
        win_rate_pct = (winning_trades / total_trades * 100.0) if total_trades > 0 else 0.0

        total_pnl_usd = sum((t.realized_pnl_usd for t in trades), Decimal(0))
        max_single_trade_pnl_usd = (
            max((t.realized_pnl_usd for t in trades), default=Decimal(0))
            if trades
            else Decimal(0)
        )

        # -------------------------------------------------------------
        # 3. Holding Time Distribution
        # -------------------------------------------------------------
        holding_times = [t.holding_time_seconds for t in trades]
        median_holding_time = (
            float(statistics.median(holding_times)) if holding_times else 0.0
        )

        # -------------------------------------------------------------
        # 4. Active History & Dead-Wallet Revival
        # -------------------------------------------------------------
        if trades:
            first_tx_timestamp = min(t.buy_timestamp for t in trades)
            last_tx_timestamp = max(t.sell_timestamp for t in trades)
            active_duration_s = max(0, last_tx_timestamp - first_tx_timestamp)
            active_days = active_duration_s / 86400.0
            days_since_last_active = max(0, current_timestamp - last_tx_timestamp) / 86400.0
        else:
            first_tx_timestamp = current_timestamp
            last_tx_timestamp = current_timestamp
            active_days = 0.0
            days_since_last_active = 0.0

        # Check for historical dormancy gap > 45 days inside trading history
        has_dormancy_gap = False
        sorted_trades = sorted(trades, key=lambda t: t.buy_timestamp)
        for i in range(1, len(sorted_trades)):
            gap_days = (sorted_trades[i].buy_timestamp - sorted_trades[i - 1].sell_timestamp) / 86400.0
            if gap_days > cls.MAX_INACTIVE_DAYS_REVIVAL:
                has_dormancy_gap = True
                break

        # -------------------------------------------------------------
        # 5. Survivorship & Outlier Bias Check
        # -------------------------------------------------------------
        outlier_pnl_ratio = 0.0
        if total_pnl_usd > Decimal(0) and max_single_trade_pnl_usd > Decimal(0):
            outlier_pnl_ratio = float(max_single_trade_pnl_usd / total_pnl_usd)

        # -------------------------------------------------------------
        # 6. Apply Behavioral Filters & Classifications
        # -------------------------------------------------------------
        if not insider_detected:
            # Check Outlier Bias (> 60% PnL from one trade)
            if outlier_pnl_ratio > cls.MAX_OUTLIER_PNL_RATIO:
                classification = WalletClassification.LUCKY_OUTLIER
                rejection_reasons.append(
                    f"Survivorship/Outlier bias: single trade contributed {outlier_pnl_ratio:.1%} "
                    f"(> {cls.MAX_OUTLIER_PNL_RATIO:.0%}) of total lifetime PnL"
                )

            # Check Holding Time (< 45s MEV bot)
            elif total_trades > 0 and median_holding_time < cls.MIN_MEDIAN_HOLDING_TIME_S:
                classification = WalletClassification.MEV_BOT
                rejection_reasons.append(
                    f"HFT/MEV Bot: median holding time {median_holding_time:.1f}s is < "
                    f"{cls.MIN_MEDIAN_HOLDING_TIME_S:.1f}s (cannot replicate on free-tier RPC)"
                )

            # Check Dead-Wallet Revival (> 45 days dormant)
            elif has_dormancy_gap or days_since_last_active > cls.MAX_INACTIVE_DAYS_REVIVAL:
                classification = WalletClassification.DEAD_REVIVAL
                dormant_val = max(days_since_last_active, 45.1)
                rejection_reasons.append(
                    f"Dead-wallet revival: dormant for > {cls.MAX_INACTIVE_DAYS_REVIVAL:.0f} days "
                    f"({dormant_val:.1f}d) before sudden activity; possible sold/compromised private key"
                )

            # Check Minimum Criteria
            elif total_trades < cls.MIN_CLOSED_TRADES:
                classification = WalletClassification.INSUFFICIENT_HISTORY
                rejection_reasons.append(
                    f"Insufficient closed trades: {total_trades} < {cls.MIN_CLOSED_TRADES}"
                )

            elif win_rate_pct < cls.MIN_WIN_RATE_PCT:
                classification = WalletClassification.LOW_WIN_RATE
                rejection_reasons.append(
                    f"Low win rate: {win_rate_pct:.1f}% < {cls.MIN_WIN_RATE_PCT:.1f}%"
                )

            elif active_days < cls.MIN_ACTIVE_DAYS:
                classification = WalletClassification.INSUFFICIENT_HISTORY
                rejection_reasons.append(
                    f"Active trading history too short: {active_days:.1f} days < {cls.MIN_ACTIVE_DAYS:.1f} days"
                )

            elif total_pnl_usd <= Decimal(0):
                classification = WalletClassification.LOW_WIN_RATE
                rejection_reasons.append(
                    f"Unprofitable lifetime PnL: ${total_pnl_usd:,.2f} <= $0"
                )

        is_whitelisted = (classification == WalletClassification.APPROVED)

        return WalletProfile(
            wallet_address=wallet_address.lower(),
            chain=chain,
            classification=classification,
            is_whitelisted=is_whitelisted,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate_pct=round(win_rate_pct, 2),
            total_pnl_usd=total_pnl_usd,
            max_single_trade_pnl_usd=max_single_trade_pnl_usd,
            outlier_pnl_ratio=round(outlier_pnl_ratio, 4),
            median_holding_time_seconds=round(median_holding_time, 2),
            active_days=round(active_days, 2),
            days_since_last_active=round(days_since_last_active, 2),
            first_tx_timestamp=first_tx_timestamp,
            last_tx_timestamp=last_tx_timestamp,
            cluster_tag=cluster_tag,
            funding_hops=detected_hops,
            rejection_reasons=rejection_reasons,
        )
