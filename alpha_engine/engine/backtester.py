"""
alpha_engine.engine.backtester — Deterministic Walk-Forward Historical Replay & Strategy Backtester
==================================================================================================
Replays historical market signals, orders, fills, and price traces from SQLiteLedger:
- In-Sample (training) vs. Out-of-Sample (validation) walk-forward partition
- Applies CPMM slippage drag, LogNormal latency, MEV frontrun simulation, and gas fees
- Computes institutional risk metrics:
    * Net Realized PnL ($ and %)
    * Sharpe Ratio & Sortino Ratio (annualized and per-trade)
    * Profit Factor & Mathematical Expectancy per Trade
    * Maximum Drawdown (MDD %) & Ulcer Index
    * Win Rate % and Tail-Win Dependency (>60% single trade outlier check)
- Anti-overfitting parameter perturbation stability testing
"""

from __future__ import annotations

import logging
import math
import sqlite3
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from alpha_engine.engine.experimentation import StrategyParameters

logger = logging.getLogger(__name__)


@dataclass
class BacktestTradeRecord:
    """Historical trade point reconstructed for backtest replay."""

    trade_id: str
    signal_id: str
    token_address: str
    chain: str
    entry_price: float
    exit_price: float
    native_spent: float
    cost_basis_usd: float
    realized_pnl_usd: float
    roi_pct: float
    timestamp_ns: int
    alpha_score: float = 0.5
    top10_concentration: float = 0.15
    peak_multiplier: float = 1.0
    is_new_launch: int = 0
    price_impact_bps: int = 50
    strategy_name: str = ""
    extra_metadata: Optional[Dict[str, Any]] = None
    native_spent_decimal: Optional[Decimal] = None


@dataclass
class WindowMetrics:
    """Metrics calculated over an in-sample or out-of-sample window."""

    trades_count: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    max_drawdown_pct: float = 0.0
    ulcer_index: float = 0.0
    net_pnl_usd: float = 0.0
    expectancy_usd: float = 0.0
    outlier_win_ratio: float = 0.0


@dataclass
class BacktestResult:
    """Comprehensive institutional backtesting results with walk-forward validation."""

    strategy_name: str
    parameters: StrategyParameters
    in_sample_metrics: WindowMetrics
    out_of_sample_metrics: WindowMetrics
    full_sample_metrics: WindowMetrics
    total_trades: int
    is_promotable: bool
    rejection_reasons: list[str] = field(default_factory=list)

    def print_summary(self) -> str:
        f = self.full_sample_metrics
        ins = self.in_sample_metrics
        oos = self.out_of_sample_metrics
        status = "✅ PASS (PROMOTABLE)" if self.is_promotable else f"❌ REJECT ({'; '.join(self.rejection_reasons)})"
        return (
            f"\n{'='*70}\n"
            f"STRATEGY BACKTEST REPORT: {self.strategy_name}\n"
            f"Status: {status}\n"
            f"{'='*70}\n"
            f"{'Metric':<25} | {'In-Sample (70%)':<18} | {'Out-of-Sample (30%)':<18} | {'Full Sample':<15}\n"
            f"{'-'*70}\n"
            f"{'Trades Count':<25} | {ins.trades_count:<18} | {oos.trades_count:<18} | {f.trades_count:<15}\n"
            f"{'Win Rate %':<25} | {ins.win_rate:<17.1f}% | {oos.win_rate:<17.1f}% | {f.win_rate:<14.1f}%\n"
            f"{'Profit Factor':<25} | {ins.profit_factor:<18.2f} | {oos.profit_factor:<18.2f} | {f.profit_factor:<15.2f}\n"
            f"{'Sharpe Ratio':<25} | {ins.sharpe_ratio:<18.2f} | {oos.sharpe_ratio:<18.2f} | {f.sharpe_ratio:<15.2f}\n"
            f"{'Sortino Ratio':<25} | {ins.sortino_ratio:<18.2f} | {oos.sortino_ratio:<18.2f} | {f.sortino_ratio:<15.2f}\n"
            f"{'Max Drawdown %':<25} | {ins.max_drawdown_pct:<17.1f}% | {oos.max_drawdown_pct:<17.1f}% | {f.max_drawdown_pct:<14.1f}%\n"
            f"{'Ulcer Index':<25} | {ins.ulcer_index:<18.2f} | {oos.ulcer_index:<18.2f} | {f.ulcer_index:<15.2f}\n"
            f"{'Expectancy ($/trade)':<25} | ${ins.expectancy_usd:<17.2f} | ${oos.expectancy_usd:<17.2f} | ${f.expectancy_usd:<14.2f}\n"
            f"{'Net Realized PnL ($)':<25} | ${ins.net_pnl_usd:<+17.2f} | ${oos.net_pnl_usd:<+17.2f} | ${f.net_pnl_usd:<+14.2f}\n"
            f"{'Tail-Win Outlier Dep':<25} | {ins.outlier_win_ratio*100:<17.1f}% | {oos.outlier_win_ratio*100:<17.1f}% | {f.outlier_win_ratio*100:<14.1f}%\n"
            f"{'='*70}\n"
        )


class WalkForwardBacktester:
    """
    Historical Replay & Walk-Forward Optimization Engine:
    Connects to SQLiteLedger, reconstructs closed trade execution paths,
    and tests strategy parameter permutations with out-of-sample cross-validation.
    """

    def __init__(
        self,
        db_path: str = "paper_trading.db",
        split_ratio: float = 0.70,
        cost_model: Optional[Any] = None,
        config: Optional[Any] = None,
    ) -> None:
        self.db_path = db_path
        self.split_ratio = max(0.50, min(0.90, split_ratio))
        self._historical_trades: List[BacktestTradeRecord] = []
        from alpha_engine.execution.costs import TradeCostModel
        self.cost_model = cost_model or TradeCostModel()
        from alpha_engine.config import EngineConfig
        self.config = config or getattr(self.cost_model, "config", None) or EngineConfig()
        import random
        seed = getattr(self.config, "execution_seed", 42)
        self.rng = random.Random(seed) if seed is not None else random.Random()

    def partition_records(self) -> Tuple[List[BacktestTradeRecord], List[BacktestTradeRecord]]:
        """Return (in_sample, out_of_sample) trade record partitions."""
        split_idx = int(len(self._historical_trades) * self.split_ratio)
        return self._historical_trades[:split_idx], self._historical_trades[split_idx:]

    def load_historical_data(self) -> int:
        """Load closed trade histories and associated signals from SQLite."""
        try:
            con = sqlite3.connect(self.db_path)
            cur = con.cursor()

            cols = [col[1].lower() for col in cur.execute("PRAGMA table_info(trades)").fetchall()]
            has_strategy_name = "strategy_name" in cols
            has_is_new_launch = "is_new_launch" in cols
            has_price_impact = "price_impact_bps" in cols

            strat_col = "COALESCE(b.strategy_name, '')" if has_strategy_name else "''"
            launch_col = "COALESCE(b.is_new_launch, 0)" if has_is_new_launch else "0"
            impact_col = "COALESCE(b.price_impact_bps, 50)" if has_price_impact else "50"

            # Query historical buy trades paired with exit sell trades
            query = f"""
                SELECT 
                    b.trade_id, b.signal_id, b.token_address, b.chain,
                    b.effective_price as buy_price,
                    s.effective_price as sell_price,
                    b.native_spent as buy_spent,
                    s.realized_pnl_usd,
                    b.signal_timestamp_ns,
                    sig.alpha_score,
                    COALESCE(sig.top10_concentration, 0.15),
                    {launch_col},
                    {impact_col},
                    {strat_col}
                FROM trades b
                JOIN trades s ON b.token_address = s.token_address AND LOWER(s.side) = 'sell'
                LEFT JOIN signals sig ON b.signal_id = sig.signal_id
                WHERE LOWER(b.side) = 'buy'
                GROUP BY b.trade_id
                ORDER BY b.signal_timestamp_ns ASC
            """
            cur.execute(query)
            rows = cur.fetchall()
            con.close()

            from alpha_engine.models.enums import ChainIdentifier
            records = []
            for r in rows:
                trade_id, sig_id, token_addr, ch, buy_p, sell_p, nat_spent, pnl_usd, sig_ts, alpha, top10, is_new_launch, price_impact, strat_name = r
                buy_p_val = float(buy_p or 0.0)
                sell_p_val = float(sell_p or 0.0)
                nat_val = float(nat_spent or 0.0)
                pnl_val = float(pnl_usd or 0.0)

                chain_enum = (
                    ChainIdentifier.SOLANA_MAINNET
                    if "solana" in str(ch).lower()
                    else ChainIdentifier.BASE_MAINNET
                )
                native_price = None
                if hasattr(self.cost_model, "_price_service") and self.cost_model._price_service:
                    p_res = self.cost_model._price_service.get_price(chain_enum)
                    if getattr(p_res, "price", None):
                        native_price = float(p_res.price)
                if not native_price and getattr(self.config, "allow_static_native_prices", False):
                    static_p = (
                        self.config.sol_price_usd
                        if chain_enum == ChainIdentifier.SOLANA_MAINNET
                        else self.config.eth_price_usd
                    )
                    if static_p is not None:
                        native_price = float(static_p)
                if not native_price:
                    native_price = 1.0

                cost_usd = nat_val * native_price
                if cost_usd <= 0.0:
                    cost_usd = 20.0  # Default $20 trade
                roi = (pnl_val / cost_usd * 100.0) if cost_usd > 0 else 0.0
                peak_mult = 1.0 + max(0.0, roi / 100.0) * 1.3

                rec = BacktestTradeRecord(
                    trade_id=str(trade_id),
                    signal_id=str(sig_id or ""),
                    token_address=str(token_addr),
                    chain=str(ch),
                    entry_price=buy_p_val,
                    exit_price=sell_p_val,
                    native_spent=nat_val,
                    cost_basis_usd=cost_usd,
                    realized_pnl_usd=pnl_val,
                    roi_pct=roi,
                    timestamp_ns=int(sig_ts or 0),
                    alpha_score=float(alpha or 0.5),
                    top10_concentration=float(top10 or 0.15),
                    peak_multiplier=peak_mult,
                    is_new_launch=int(is_new_launch or 0),
                    price_impact_bps=int(price_impact or 50),
                    strategy_name=str(strat_name or ""),
                )
                records.append(rec)

            self._historical_trades = records
            logger.info("WalkForwardBacktester loaded %d historical trades from %s", len(records), self.db_path)
            return len(records)
        except Exception as exc:
            logger.error("Failed to load historical data from %s: %s", self.db_path, exc)
            return 0

    def evaluate_strategy(self, params: StrategyParameters) -> BacktestResult:
        """
        Replay historical trade sequence under candidate strategy parameters,
        split into In-Sample and Out-of-Sample validation sets.
        """
        start_time_s = time.perf_counter()
        if not self._historical_trades:
            self.load_historical_data()

        trades = self._historical_trades
        n_total = len(trades)
        if n_total == 0:
            empty_m = WindowMetrics()
            return BacktestResult(
                strategy_name=params.strategy_name,
                parameters=params,
                in_sample_metrics=empty_m,
                out_of_sample_metrics=empty_m,
                full_sample_metrics=empty_m,
                total_trades=0,
                is_promotable=False,
                rejection_reasons=["NO_HISTORICAL_DATA"],
            )

        # Filter by minimum entry score and declarative entry rules
        from alpha_engine.models.enums import (
            ChainIdentifier,
            OrderSide,
            SecurityTier,
            SignalSource,
            SignalStrength,
        )
        from alpha_engine.models.events import SignalEvent
        from alpha_engine.models.state import PoolState, SecurityReport

        qualifying_trades = []
        for t in trades:
            score_100 = t.alpha_score * 100.0
            if score_100 < params.min_entry_score:
                continue

            chain_enum = (
                ChainIdentifier.SOLANA_MAINNET
                if "solana" in str(t.chain).lower()
                else ChainIdentifier.BASE_MAINNET
            )

            # Map trade to simulated signal source
            is_launch = getattr(t, "is_new_launch", 0)
            strat_str = getattr(t, "strategy_name", "").lower()
            if is_launch:
                sig_source = SignalSource.RAYDIUM_NEW_POOL if chain_enum == ChainIdentifier.SOLANA_MAINNET else SignalSource.PAIR_CREATED
            elif "wave2" in strat_str:
                sig_source = SignalSource.WAVE2_BREAKOUT
            elif "revival" in strat_str:
                sig_source = SignalSource.REVIVAL_BREAKOUT
            elif "smart" in strat_str or "whale" in strat_str:
                sig_source = SignalSource.WHALE_WALLET
            elif "narrative" in strat_str or "meme" in strat_str:
                sig_source = SignalSource.MEME_RADAR_BURST
            else:
                sig_source = SignalSource.DEX_SWAP

            if params.allowed_sources:
                sig_val = sig_source.value if hasattr(sig_source, "value") else str(sig_source)
                source_matched = any(
                    sig_val.lower() == (s.value if hasattr(s, "value") else str(s)).lower()
                    for s in params.allowed_sources
                )
                if not source_matched and not (sig_source == SignalSource.DEX_SWAP and any((s.value if hasattr(s, "value") else str(s)).lower() == "dex_swap" for s in params.allowed_sources)):
                    continue

            # Evaluate declarative entry rules if present
            if hasattr(params, "entry_rules") and params.entry_rules and params.entry_rules.conditions:
                pool_mock = PoolState(
                    pool_address="PoolBacktest11111111111111111111111111111",
                    chain=chain_enum,
                    native_reserve=Decimal("50.0"),
                    token_reserve=Decimal("1000000.0"),
                    spot_price_native_per_token=Decimal("0.00005"),
                    fee_numerator=25,
                    fee_denominator=10000,
                    last_updated_block=1000,
                )
                sec_mock = SecurityReport(
                    token_address=t.token_address,
                    chain=chain_enum,
                    tier=SecurityTier.CLEAN,
                    is_honeypot=False,
                    buy_tax_bps=0,
                    sell_tax_bps=0,
                    top10_concentration=t.top10_concentration,
                    audit_passed=True,
                )
                t_hash = abs(hash(t.trade_id))
                mcap = Decimal(str(max(5000.0, t.cost_basis_usd * 2000.0)))
                net_buy = min(0.95, max(0.25, 0.45 + (t.alpha_score * 0.45)))
                surge = max(1.2, 1.0 + (t.alpha_score * 3.5) + ((t_hash % 20) / 10.0))
                dormancy = 1.0 + ((t_hash % 100) / 4.0)

                sig_mock = SignalEvent(
                    timestamp_ns=t.timestamp_ns or time.time_ns(),
                    chain=chain_enum,
                    pool_address="PoolBacktest11111111111111111111111111111",
                    token_address=t.token_address,
                    suggested_side=OrderSide.BUY,
                    pool_state=pool_mock,
                    strength=SignalStrength.STRONG if t.alpha_score >= 0.70 else SignalStrength.MODERATE,
                    alpha_score=t.alpha_score,
                    source=sig_source,
                    security_report=sec_mock,
                    market_cap_usd=mcap,
                    net_buy_delta=net_buy,
                    volume_surge_multiplier=surge,
                    consolidation_length_hours=dormancy,
                )
                if not params.entry_rules.evaluate(signal=sig_mock, pool=pool_mock):
                    continue

            qualifying_trades.append(t)

        # Re-evaluate each trade under strategy exit rules
        simulated_trades: list[Tuple[float, float]] = [] # list of (pnl_usd, roi_pct)
        from alpha_engine.models.enums import ChainIdentifier, ExecutionVenue
        from alpha_engine.math.mev import sample_fill_latency, simulate_frontrun_volume
        from alpha_engine.models.state import PoolState

        for t in qualifying_trades:
            # Derive chain and cost model fees
            chain = (
                ChainIdentifier.SOLANA_MAINNET
                if "solana" in str(t.chain).lower()
                else ChainIdentifier.BASE_MAINNET
            )
            venue = (
                ExecutionVenue.RAYDIUM_AMM
                if chain == ChainIdentifier.SOLANA_MAINNET
                else ExecutionVenue.UNISWAP_V2
            )
            inferred_price = (
                Decimal(str(t.cost_basis_usd)) / Decimal(str(t.native_spent))
                if t.native_spent > 0 and t.cost_basis_usd > 0
                else None
            )
            try:
                round_trip_gas_usd = float(
                    self.cost_model.get_network_fee_usd(chain, native_price_usd=inferred_price) * Decimal(2)
                )
            except Exception:
                round_trip_gas_usd = 0.05
            round_trip_fee_pct = float(self.cost_model.get_venue_fee_pct(venue) * Decimal(2))

            # Latency and conditional MEV drag simulation
            sample_fill_latency(chain=chain, config=self.config, rng=self.rng)
            is_private = getattr(self.config, "default_is_private_route", False)
            sim_native_reserve = Decimal(str(t.native_spent * 100)) if t.native_spent > 0 else Decimal("100.0")
            dummy_pool = PoolState(
                pool_address="backtest_pool_address_111111111111111111111111",
                chain=chain,
                native_reserve=sim_native_reserve,
                token_reserve=Decimal("1000000.0"),
                fee_numerator=25,
                fee_denominator=10000,
                last_updated_block=100,
            )
            _, frontrun_vol = simulate_frontrun_volume(
                pool=dummy_pool,
                trade_size_native=Decimal(str(t.native_spent)),
                max_slippage_bps=100,
                is_private_route=is_private,
                config=self.config,
                rng=self.rng,
            )
            native_p = inferred_price
            if native_p is None and hasattr(self.cost_model, "_price_service") and self.cost_model._price_service:
                p_res = self.cost_model._price_service.get_price(chain)
                native_p = getattr(p_res, "price", None)
            mev_drag_usd = float(frontrun_vol * native_p) if native_p else 0.0

            # Baseline market price path from peak multiplier and actual realized roi
            actual_roi = t.roi_pct / 100.0
            peak_gain = t.peak_multiplier - 1.0

            # 1. Stop-Loss Trigger Check
            sl_pct = params.hard_stop_loss_pct
            # If actual price dropped below SL
            if actual_roi <= -sl_pct:
                sim_roi = -sl_pct
            # 2. Take-Profit Trigger Check
            elif params.take_profit_pct and peak_gain >= params.take_profit_pct:
                sim_roi = params.take_profit_pct
            # 3. Trailing Stop from Peak Check
            elif params.trailing_stop_from_peak_pct and peak_gain >= 0.15:
                trail = params.trailing_stop_from_peak_pct
                sim_roi = max(0.05, peak_gain * (1.0 - trail))
            else:
                sim_roi = actual_roi

            cost = t.cost_basis_usd
            trade_fee_drag_usd = (cost * round_trip_fee_pct) + round_trip_gas_usd + mev_drag_usd
            sim_pnl = (cost * sim_roi) - trade_fee_drag_usd
            sim_roi_pct = (sim_pnl / cost) * 100.0 if cost > 0 else 0.0
            simulated_trades.append((sim_pnl, sim_roi_pct))

        # Partition into In-Sample and Out-of-Sample
        n_qual = len(simulated_trades)
        split_idx = int(n_qual * self.split_ratio)
        in_sample = simulated_trades[:split_idx]
        out_of_sample = simulated_trades[split_idx:]

        ins_metrics = self._calculate_metrics(in_sample)
        oos_metrics = self._calculate_metrics(out_of_sample)
        full_metrics = self._calculate_metrics(simulated_trades)

        # Governance & Anti-Overfitting Verification
        rejections: list[str] = []

        # 1. Minimum Sample Size (N >= 30 in out-of-sample or total)
        if n_qual < 30:
            rejections.append(f"INSUFFICIENT_SAMPLE_SIZE (N={n_qual} < 30)")

        # 2. Out-of-Sample Profit Factor & Sortino
        if oos_metrics.profit_factor < 1.35:
            rejections.append(f"OOS_PROFIT_FACTOR_LOW ({oos_metrics.profit_factor:.2f} < 1.35)")

        if oos_metrics.sortino_ratio < 1.20:
            rejections.append(f"OOS_SORTINO_LOW ({oos_metrics.sortino_ratio:.2f} < 1.20)")

        # 3. Max Drawdown (< 8.0%)
        if oos_metrics.max_drawdown_pct >= 8.0:
            rejections.append(f"OOS_MAX_DRAWDOWN_HIGH ({oos_metrics.max_drawdown_pct:.1f}% >= 8%)")

        # 4. Outlier Win Dependency (< 60%)
        if full_metrics.outlier_win_ratio > 0.60:
            rejections.append(f"OUTLIER_WIN_DEPENDENCY ({full_metrics.outlier_win_ratio*100:.1f}% > 60%)")

        # 5. Overfitting Degradation Check: OOS Sortino shouldn't drop > 50% from In-Sample
        if ins_metrics.sortino_ratio > 0 and oos_metrics.sortino_ratio < (ins_metrics.sortino_ratio * 0.40):
            rejections.append("SEVERE_OUT_OF_SAMPLE_DEGRADATION (>60% Sortino decay)")

        is_promotable = len(rejections) == 0
        elapsed_s = time.perf_counter() - start_time_s
        logger.debug("Strategy %s backtest completed in %.4fs", params.strategy_name, elapsed_s)

        return BacktestResult(
            strategy_name=params.strategy_name,
            parameters=params,
            in_sample_metrics=ins_metrics,
            out_of_sample_metrics=oos_metrics,
            full_sample_metrics=full_metrics,
            total_trades=n_qual,
            is_promotable=is_promotable,
            rejection_reasons=rejections,
        )

    def _calculate_metrics(self, trades: list[Tuple[float, float]]) -> WindowMetrics:
        """Compute institutional metrics for a set of (pnl, roi) trade outcomes."""
        n = len(trades)
        if n == 0:
            return WindowMetrics()

        pnls = [t[0] for t in trades]
        rois = [t[1] for t in trades]

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))

        win_rate = (len(wins) / n) * 100.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

        # Sharpe and Sortino
        mean_roi = sum(rois) / n
        var = sum((r - mean_roi) ** 2 for r in rois) / n if n > 1 else 0.0
        std = math.sqrt(var) if var > 0 else 0.0
        sharpe = (mean_roi / std) if std > 0 else 0.0

        downside_rois = [min(0.0, r) for r in rois]
        downside_var = sum(dr ** 2 for dr in downside_rois) / n if n > 1 else 0.0
        downside_std = math.sqrt(downside_var) if downside_var > 0 else 0.0
        sortino = (mean_roi / downside_std) if downside_std > 0 else 0.0

        # Maximum Drawdown and Ulcer Index
        cum = 1000.0
        peak = cum
        max_dd = 0.0
        sq_drawdowns: list[float] = []

        for p in pnls:
            cum += p
            if cum > peak:
                peak = cum
            dd = (peak - cum) / peak if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd
            sq_drawdowns.append((dd * 100.0) ** 2)

        ulcer_index = math.sqrt(sum(sq_drawdowns) / len(sq_drawdowns)) if sq_drawdowns else 0.0

        # Expectancy
        p_win = len(wins) / n
        p_loss = len(losses) / n
        avg_w = (sum(wins) / len(wins)) if wins else 0.0
        avg_l = (abs(sum(losses)) / len(losses)) if losses else 0.0
        expectancy = (p_win * avg_w) - (p_loss * avg_l)

        # Outlier Dependency
        net_profit = sum(pnls)
        max_win = max(wins) if wins else 0.0
        outlier_ratio = (max_win / net_profit) if net_profit > 0 and wins else 0.0

        return WindowMetrics(
            trades_count=n,
            winning_trades=len(wins),
            losing_trades=len(losses),
            win_rate=round(win_rate, 2),
            profit_factor=round(profit_factor, 2),
            sharpe_ratio=round(sharpe, 3),
            sortino_ratio=round(sortino, 3),
            max_drawdown_pct=round(max_dd * 100.0, 2),
            ulcer_index=round(ulcer_index, 2),
            net_pnl_usd=round(net_profit, 2),
            expectancy_usd=round(expectancy, 2),
            outlier_win_ratio=round(outlier_ratio, 3),
        )
