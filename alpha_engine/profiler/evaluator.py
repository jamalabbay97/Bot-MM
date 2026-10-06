"""
alpha_engine.profiler.evaluator — Smart Money Behavioral Rules & Anomaly Detector
================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

from collections import defaultdict
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
    MAX_SINGLE_DEV_VOLUME_RATIO: float = 0.80

    @staticmethod
    def calculate_smart_money_score(
        win_rate: float,
        sharpe_ratio: float,
        holding_discipline: float,
        longevity: float,
    ) -> float:
        """
        Smart Money Score (S_wallet) Formula:
        S_wallet = (WinRate * 0.4) + (SharpeRatio * 0.3) + (HoldingDiscipline * 0.2) + (Longevity * 0.1)
        Inputs are normalized in [0.0, 1.0].
        """
        wr = max(0.0, min(1.0, win_rate / 100.0 if win_rate > 1.0 else win_rate))
        sr = max(0.0, min(1.0, sharpe_ratio))
        hd = max(0.0, min(1.0, holding_discipline))
        lg = max(0.0, min(1.0, longevity))
        return round(float((wr * 0.4) + (sr * 0.3) + (hd * 0.2) + (lg * 0.1)), 4)

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

        # Single-developer volume concentration check (> 80%)
        if not insider_detected and trades:
            total_vol = sum((t.invested_native for t in trades), Decimal(0))
            if total_vol > Decimal(0):
                dev_vols: dict[str, Decimal] = defaultdict(Decimal)
                has_dev_info = False
                for t in trades:
                    dev = (
                        (t.custom_metadata or {}).get("developer_address")
                        or (t.custom_metadata or {}).get("dev_wallet")
                        or (t.custom_metadata or {}).get("deployer_address")
                    )
                    if dev:
                        has_dev_info = True
                        dev_vols[dev.lower().strip()] += t.invested_native
                if has_dev_info and dev_vols:
                    max_dev_vol = max(dev_vols.values())
                    if (max_dev_vol / total_vol) > Decimal(str(cls.MAX_SINGLE_DEV_VOLUME_RATIO)):
                        classification = WalletClassification.INSIDER
                        rejection_reasons.append(
                            f"Single-developer token concentration: {float(max_dev_vol/total_vol):.1%} volume (> {cls.MAX_SINGLE_DEV_VOLUME_RATIO:.0%}) on single developer"
                        )

        # Circular wash trading check
        if classification == WalletClassification.APPROVED and cls.detect_circular_wash_trading(wallet_address, trades):
            classification = WalletClassification.CIRCULAR_WASH
            rejection_reasons.append("Circular fund routing / mixer clustering detected")

        # Smart Money Score formula:
        # S_wallet = (WinRate * 0.4) + (SharpeRatio * 0.3) + (HoldingDiscipline * 0.2) + (Longevity * 0.1)
        rois = [t.roi_pct / 100.0 for t in trades]
        if len(rois) > 1:
            mean_roi = sum(rois) / len(rois)
            try:
                std_roi = statistics.stdev(rois)
            except Exception:
                std_roi = 0.0
            raw_sharpe = (mean_roi / std_roi) if std_roi > 0 else 0.0
            sharpe_norm = max(0.0, min(1.0, raw_sharpe / 3.0))
        else:
            sharpe_norm = 0.50 if (trades and trades[0].is_win) else 0.0

        holding_discipline_norm = max(0.0, min(1.0, median_holding_time / 300.0))
        longevity_norm = max(0.0, min(1.0, active_days / 60.0))
        smart_money_score = cls.calculate_smart_money_score(
            win_rate=win_rate_pct,
            sharpe_ratio=sharpe_norm,
            holding_discipline=holding_discipline_norm,
            longevity=longevity_norm,
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
            smart_money_score=smart_money_score,
            sharpe_ratio=round(sharpe_norm, 4),
            holding_discipline=round(holding_discipline_norm, 4),
            longevity=round(longevity_norm, 4),
        )

    @classmethod
    def detect_circular_wash_trading(
        cls,
        wallet_address: str,
        trades: Sequence[WalletTradeRecord],
        transfer_graph: Optional[Sequence[tuple[str, str]]] = None,
    ) -> bool:
        """
        Detects wash-trading patterns and circular transaction loops:
        1. Repeated round-trip trades with negligible net PnL.
        2. Transaction transfer graph loops (A -> B -> C -> A).
        """
        if transfer_graph:
            adj: dict[str, set[str]] = {}
            for src, dst in transfer_graph:
                src_l, dst_l = src.lower(), dst.lower()
                adj.setdefault(src_l, set()).add(dst_l)

            visited: set[str] = set()
            rec_stack: set[str] = set()

            def has_cycle(node: str) -> bool:
                visited.add(node)
                rec_stack.add(node)
                for neighbor in adj.get(node, ()):
                    if neighbor not in visited:
                        if has_cycle(neighbor):
                            return True
                    elif neighbor in rec_stack:
                        return True
                rec_stack.remove(node)
                return False

            w_lower = wallet_address.lower()
            if w_lower in adj and has_cycle(w_lower):
                return True

        token_trade_counts: dict[str, int] = {}
        for t in trades:
            token_trade_counts[t.token_address] = token_trade_counts.get(t.token_address, 0) + 1

        for tok, count in token_trade_counts.items():
            if count >= 6:
                tok_trades = [t for t in trades if t.token_address == tok]
                total_abs_pnl = sum(abs(t.realized_pnl_usd) for t in tok_trades)
                if total_abs_pnl < Decimal("1.0"):
                    return True

        return False

    @classmethod
    def evaluate_autonomous(
        cls,
        wallet_address: str,
        chain: ChainIdentifier,
        trades: Sequence[WalletTradeRecord],
        transfer_graph: Optional[Sequence[tuple[str, str]]] = None,
        initial_txs: Optional[Sequence[InitialTxRecord]] = None,
        funding_hops: Optional[Sequence[FundingHop]] = None,
        current_timestamp: Optional[int] = None,
        cluster_tag: Optional[str] = None,
    ) -> WalletProfile:
        """
        Autonomous Smart Wallet Discovery & Machine Learning Profiler scoring:
        - 30-day lookback window
        - Minimum 25 total trades
        - Win Rate (trades closing > 50% ROI) > 60%
        - Profit Factor > 2.0
        - Maximum Drawdown (MDD) < 35%
        - Average Holding Time > 3 minutes (180s)
        - Wash-trading & circular loop detection -> permanent blacklist
        """
        if current_timestamp is None:
            current_timestamp = int(time.time())

        rejection_reasons: list[str] = []
        classification = WalletClassification.APPROVED

        # 1. Circular wash trading & loop check
        if cls.detect_circular_wash_trading(wallet_address, trades, transfer_graph):
            classification = WalletClassification.CIRCULAR_WASH
            rejection_reasons.append("Circular transaction loops / wash-trading detected")
            return WalletProfile(
                wallet_address=wallet_address.lower(),
                chain=chain,
                classification=classification,
                is_whitelisted=False,
                total_trades=len(trades),
                winning_trades=0,
                losing_trades=len(trades),
                win_rate_pct=0.0,
                total_pnl_usd=Decimal("0"),
                max_single_trade_pnl_usd=Decimal("0"),
                outlier_pnl_ratio=0.0,
                median_holding_time_seconds=0.0,
                active_days=0.0,
                days_since_last_active=0.0,
                first_tx_timestamp=current_timestamp,
                last_tx_timestamp=current_timestamp,
                cluster_tag=cluster_tag,
                funding_hops=list(funding_hops) if funding_hops else [],
                rejection_reasons=rejection_reasons,
            )

        # 2. Check insider funding provenance
        detected_hops: list[FundingHop] = list(funding_hops) if funding_hops else []
        for hop in detected_hops:
            if hop.hop_depth <= cls.MAX_FUNDING_HOPS and (hop.is_deployer or hop.is_multisig):
                classification = WalletClassification.INSIDER
                rejection_reasons.append(
                    f"Insider funding at hop {hop.hop_depth} to {hop.source_address}"
                )
                break

        # 3. 30-day lookback window filter
        lookback_s = 30 * 86400
        trades_30d = [t for t in trades if t.sell_timestamp >= current_timestamp - lookback_s]

        total_trades = len(trades_30d)
        if total_trades < 25:
            if classification == WalletClassification.APPROVED:
                classification = WalletClassification.INSUFFICIENT_HISTORY
            rejection_reasons.append(f"Insufficient trades in 30d lookback: {total_trades} < 25")

        # Win Rate (trades closing > 50% ROI) > 60%
        trades_above_50_roi = sum(1 for t in trades_30d if t.roi_pct > 50.0)
        win_rate_50pct = (trades_above_50_roi / total_trades * 100.0) if total_trades > 0 else 0.0
        if win_rate_50pct <= 60.0:
            if classification == WalletClassification.APPROVED:
                classification = WalletClassification.LOW_WIN_RATE
            rejection_reasons.append(
                f"Win rate (>50% ROI) {win_rate_50pct:.1f}% <= 60.0%"
            )

        # Profit Factor > 2.0
        gross_profit = sum((t.realized_pnl_usd for t in trades_30d if t.realized_pnl_usd > 0), Decimal("0"))
        gross_loss = abs(sum((t.realized_pnl_usd for t in trades_30d if t.realized_pnl_usd < 0), Decimal("0")))
        profit_factor = float(gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
        if profit_factor <= 2.0:
            if classification == WalletClassification.APPROVED:
                classification = WalletClassification.LOW_WIN_RATE
            rejection_reasons.append(f"Profit factor {profit_factor:.2f} <= 2.0")

        # Maximum Drawdown (MDD) < 35%
        sorted_trades = sorted(trades_30d, key=lambda x: x.sell_timestamp)
        cum_equity = Decimal("10000")
        peak_equity = Decimal("10000")
        max_dd_pct = 0.0
        for t in sorted_trades:
            cum_equity += t.realized_pnl_usd
            if cum_equity > peak_equity:
                peak_equity = cum_equity
            elif peak_equity > 0:
                dd = float((peak_equity - cum_equity) / peak_equity * 100)
                if dd > max_dd_pct:
                    max_dd_pct = dd
        if max_dd_pct >= 35.0:
            if classification == WalletClassification.APPROVED:
                classification = WalletClassification.LOW_WIN_RATE
            rejection_reasons.append(f"Maximum drawdown {max_dd_pct:.1f}% >= 35.0%")

        # Average Holding Time > 3 minutes (180s)
        avg_holding_time = (
            sum(t.holding_time_seconds for t in trades_30d) / total_trades
            if total_trades > 0 else 0.0
        )
        if avg_holding_time <= 180.0:
            if classification == WalletClassification.APPROVED:
                classification = WalletClassification.MEV_BOT
            rejection_reasons.append(
                f"Average holding time {avg_holding_time:.1f}s <= 180.0s (MEV bot filter)"
            )

        # Single-developer volume concentration check (> 80%)
        if trades_30d:
            total_vol_30d = sum((t.invested_native for t in trades_30d), Decimal(0))
            if total_vol_30d > Decimal(0):
                dev_vols_30d: dict[str, Decimal] = defaultdict(Decimal)
                has_dev_info = False
                for t in trades_30d:
                    dev = (
                        (t.custom_metadata or {}).get("developer_address")
                        or (t.custom_metadata or {}).get("dev_wallet")
                        or (t.custom_metadata or {}).get("deployer_address")
                    )
                    if dev:
                        has_dev_info = True
                        dev_vols_30d[dev.lower().strip()] += t.invested_native
                if has_dev_info and dev_vols_30d:
                    max_dev_vol_30d = max(dev_vols_30d.values())
                    if (max_dev_vol_30d / total_vol_30d) > Decimal(str(cls.MAX_SINGLE_DEV_VOLUME_RATIO)):
                        classification = WalletClassification.INSIDER
                        rejection_reasons.append(
                            f"Single-developer token concentration: {float(max_dev_vol_30d/total_vol_30d):.1%} volume (> {cls.MAX_SINGLE_DEV_VOLUME_RATIO:.0%}) on single developer"
                        )

        # Smart Money Score formula:
        rois = [t.roi_pct / 100.0 for t in trades_30d]
        if len(rois) > 1:
            mean_roi = sum(rois) / len(rois)
            try:
                std_roi = statistics.stdev(rois)
            except Exception:
                std_roi = 0.0
            raw_sharpe = (mean_roi / std_roi) if std_roi > 0 else 0.0
            sharpe_norm = max(0.0, min(1.0, raw_sharpe / 3.0))
        else:
            sharpe_norm = 0.50 if (trades_30d and trades_30d[0].is_win) else 0.0

        holding_discipline_norm = max(0.0, min(1.0, avg_holding_time / 300.0))
        longevity_norm = max(0.0, min(1.0, 30.0 / 60.0))
        smart_money_score = cls.calculate_smart_money_score(
            win_rate=win_rate_50pct,
            sharpe_ratio=sharpe_norm,
            holding_discipline=holding_discipline_norm,
            longevity=longevity_norm,
        )

        winning_trades = sum(1 for t in trades_30d if t.is_win)
        losing_trades = total_trades - winning_trades
        total_pnl_usd = sum((t.realized_pnl_usd for t in trades_30d), Decimal("0"))
        max_single = max((t.realized_pnl_usd for t in trades_30d), default=Decimal("0"))
        outlier_ratio = float(max_single / total_pnl_usd) if total_pnl_usd > 0 and max_single > 0 else 0.0

        is_whitelisted = (classification == WalletClassification.APPROVED)

        return WalletProfile(
            wallet_address=wallet_address.lower(),
            chain=chain,
            classification=classification,
            is_whitelisted=is_whitelisted,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate_pct=round(win_rate_50pct, 2),
            total_pnl_usd=total_pnl_usd,
            max_single_trade_pnl_usd=max_single,
            outlier_pnl_ratio=round(outlier_ratio, 4),
            median_holding_time_seconds=round(avg_holding_time, 2),
            active_days=30.0,
            days_since_last_active=0.0,
            first_tx_timestamp=sorted_trades[0].buy_timestamp if sorted_trades else current_timestamp,
            last_tx_timestamp=sorted_trades[-1].sell_timestamp if sorted_trades else current_timestamp,
            cluster_tag=cluster_tag,
            funding_hops=detected_hops,
            rejection_reasons=rejection_reasons,
            smart_money_score=smart_money_score,
            sharpe_ratio=round(sharpe_norm, 4),
            holding_discipline=round(holding_discipline_norm, 4),
            longevity=round(longevity_norm, 4),
        )


detect_circular_wash_trading = WalletEvaluator.detect_circular_wash_trading

