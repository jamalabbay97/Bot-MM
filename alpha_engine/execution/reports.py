"""
alpha_engine.execution.reports — Quantitative Performance Reporter & Ledger Analytics
===================================================================================
Institutional Performance Reporting Engine:
- Automated Daily & Weekly Performance Digests (Markdown)
- Key Metrics: Win-Rate, Profit Factor, Sortino Ratio, Max Drawdown, Payoff Ratio
- Strategy Tournament Comparison & Leaderboard
- Clustered Loss Attribution Breakdown
- CSV & JSON Data Export for Institutional Auditing & Telemetry Dashboards
"""

from __future__ import annotations

import csv
import io
import json
import math
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.execution.ledger import SQLiteLedger


class QuantitativePerformanceReporter:
    """
    Computes institutional-grade quantitative trading performance metrics and
    formats automated daily/weekly digests and data exports from the SQLite ledger.
    """

    def __init__(self, ledger: SQLiteLedger) -> None:
        self.ledger = ledger

    async def compute_portfolio_metrics(
        self,
        lookback_hours: float = 24.0,
    ) -> dict[str, Any]:
        """
        Calculates comprehensive quantitative portfolio metrics over a lookback window:
        Total PnL, Win-Rate, Profit Factor, Sortino Ratio, Max Drawdown, Payoff Ratio.
        """
        trades = await self.ledger.get_closed_trades_dataset(lookback_hours=lookback_hours, limit=2000)
        clusters = await self.ledger.get_loss_attribution_clusters(lookback_hours=lookback_hours, limit=100)

        total_trades = len(trades)
        if total_trades == 0:
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate_pct": 0.0,
                "net_pnl_usd": 0.0,
                "gross_profit_usd": 0.0,
                "gross_loss_usd": 0.0,
                "profit_factor": 0.0,
                "sortino_ratio": 0.0,
                "max_drawdown_pct": 0.0,
                "max_drawdown_usd": 0.0,
                "avg_win_usd": 0.0,
                "avg_loss_usd": 0.0,
                "payoff_ratio": 0.0,
                "avg_duration_minutes": 0.0,
                "strategy_breakdown": {},
                "loss_clusters": clusters.get("clusters", {}) if isinstance(clusters, dict) else {},
            }

        wins = []
        losses = []
        rois = []
        durations = []
        strategy_stats: dict[str, dict[str, Any]] = {}

        cumulative_pnl = 0.0
        peak_pnl = 0.0
        max_drawdown_usd = 0.0

        for t in trades:
            pnl_val = t.get("realized_pnl_usd", 0.0)
            pnl = float(pnl_val) if isinstance(pnl_val, (int, float, Decimal)) else float(Decimal(str(pnl_val or 0.0)))
            roi = float(t.get("roi_pct", 0.0))
            dur = float(t.get("holding_time_s", 0.0))
            strat = t.get("strategy_name") or "default_strategy"

            durations.append(dur)
            rois.append(roi)

            # Cumulative drawdown tracking
            cumulative_pnl += pnl
            if cumulative_pnl > peak_pnl:
                peak_pnl = cumulative_pnl
            drawdown = peak_pnl - cumulative_pnl
            if drawdown > max_drawdown_usd:
                max_drawdown_usd = drawdown

            # Strat tracking
            if strat not in strategy_stats:
                strategy_stats[strat] = {"trades": 0, "wins": 0, "pnl_usd": 0.0}
            strategy_stats[strat]["trades"] += 1
            strategy_stats[strat]["pnl_usd"] += pnl

            if pnl > 0:
                wins.append(pnl)
                strategy_stats[strat]["wins"] += 1
            elif pnl < 0:
                losses.append(abs(pnl))

        win_count = len(wins)
        loss_count = len(losses)
        win_rate = (win_count / total_trades) * 100.0 if total_trades > 0 else 0.0

        gross_profit = sum(wins)
        gross_loss = sum(losses)
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (99.9 if gross_profit > 0 else 0.0)

        avg_win = (gross_profit / win_count) if win_count > 0 else 0.0
        avg_loss = (gross_loss / loss_count) if loss_count > 0 else 0.0
        payoff_ratio = (avg_win / avg_loss) if avg_loss > 0 else (99.9 if avg_win > 0 else 0.0)

        # Sortino Ratio calculation: (mean_roi - target) / downside_deviation
        mean_roi = sum(rois) / len(rois) if rois else 0.0
        downside_diffs = [min(0.0, r) ** 2 for r in rois]
        downside_dev = math.sqrt(sum(downside_diffs) / len(downside_diffs)) if downside_diffs else 0.0
        sortino = (mean_roi / downside_dev) if downside_dev > 0.0001 else 0.0

        # Max drawdown % based on peak or nominal equity ($10,000 base if starting from 0)
        base_equity = 10000.0
        max_dd_pct = (max_drawdown_usd / (base_equity + max(0.0, peak_pnl))) * 100.0

        avg_duration_min = (sum(durations) / len(durations) / 60.0) if durations else 0.0

        for s_data in strategy_stats.values():
            s_trades = s_data["trades"]
            s_data["win_rate_pct"] = round((s_data["wins"] / s_trades * 100.0), 1) if s_trades > 0 else 0.0
            s_data["pnl_usd"] = round(s_data["pnl_usd"], 2)

        return {
            "total_trades": total_trades,
            "winning_trades": win_count,
            "losing_trades": loss_count,
            "win_rate_pct": round(win_rate, 1),
            "net_pnl_usd": round(cumulative_pnl, 2),
            "gross_profit_usd": round(gross_profit, 2),
            "gross_loss_usd": round(gross_loss, 2),
            "profit_factor": round(profit_factor, 2),
            "sortino_ratio": round(sortino, 2),
            "max_drawdown_pct": round(max_dd_pct, 2),
            "max_drawdown_usd": round(max_drawdown_usd, 2),
            "avg_win_usd": round(avg_win, 2),
            "avg_loss_usd": round(avg_loss, 2),
            "payoff_ratio": round(payoff_ratio, 2),
            "avg_duration_minutes": round(avg_duration_min, 1),
            "strategy_breakdown": strategy_stats,
            "loss_clusters": clusters.get("clusters", {}) if isinstance(clusters, dict) else {},
        }

    async def generate_daily_digest(self, lookback_hours: float = 24.0) -> str:
        """
        Generates an executive institutional daily digest in markdown.
        """
        metrics = await self.compute_portfolio_metrics(lookback_hours=lookback_hours)
        risk_mode = "NORMAL"
        try:
            mode_val = await self.ledger.get_setting("global_risk_mode")
            if mode_val:
                risk_mode = mode_val.upper()
        except Exception:
            pass

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        net_pnl = metrics["net_pnl_usd"]
        pnl_icon = "🟢" if net_pnl >= 0 else "🔴"
        pnl_sign = "+" if net_pnl >= 0 else ""

        lines = [
            "## 📊 التقرير الكمي اليومي (Daily Quantitative Performance Digest)",
            f"**تاريخ الإصدار:** `{now_str}` | **النافذة الزمنية:** آخر `{lookback_hours:.0f}` ساعة",
            f"**وضع المخاطر العالمي:** `🛡️ {risk_mode}`\n",
            "### 📈 المؤشرات الكمية الأساسية (Core Executive Metrics)",
            f"- **صافي الأرباح المحققة:** {pnl_icon} `{pnl_sign}${net_pnl:,.2f}`",
            f"- **عدد الصفقات المنفذة:** `{metrics['total_trades']}` صفقات (`{metrics['winning_trades']}` رابحة / `{metrics['losing_trades']}` خاسرة)",
            f"- **نسبة الفوز (Win Rate):** `{metrics['win_rate_pct']}%`",
            f"- **معامل الربحية (Profit Factor):** `{metrics['profit_factor']:.2f}`",
            f"- **معدل سورتينو (Sortino Ratio):** `{metrics['sortino_ratio']:.2f}`",
            f"- **أقصى تراجع للمحفظة (Max Drawdown):** `{metrics['max_drawdown_pct']:.2f}%` (`-${metrics['max_drawdown_usd']:,.2f}`)",
            f"- **نسبة العائد إلى المخاطرة (Payoff Ratio):** `{metrics['payoff_ratio']:.2f}x` (متوسط ربح: `${metrics['avg_win_usd']:.2f}` / متوسط خسارة: `${metrics['avg_loss_usd']:.2f}`)",
            f"- **متوسط مدة الصفقة (Avg Duration):** `{metrics['avg_duration_minutes']:.1f}` دقيقة\n",
            "### 🏆 أداء الاستراتيجيات المتنافسة (Strategy Breakdown)",
        ]

        strat_breakdown = metrics["strategy_breakdown"]
        if strat_breakdown:
            lines.append("| الاستراتيجية | الصفقات | نسبة الفوز | صافي الأرباح |")
            lines.append("| :--- | :---: | :---: | :---: |")
            for strat_name, s in strat_breakdown.items():
                s_sign = "+" if s["pnl_usd"] >= 0 else ""
                lines.append(f"| `{strat_name}` | {s['trades']} | {s['win_rate_pct']}% | `{s_sign}${s['pnl_usd']:,.2f}` |")
        else:
            lines.append("- لا توجد تداولات مسجلة للاستراتيجيات في هذه النافذة.")

        lines.append("\n### 📉 التحليل الجذري لخسائر الصفقات (Loss Attribution Clusters)")
        clusters = metrics["loss_clusters"]
        if clusters:
            for reason, c in clusters.items():
                cnt = c.get("count", 0)
                tot = float(c.get("total_loss_usd", 0.0))
                avg_l = float(c.get("avg_loss_usd", 0.0))
                lines.append(f"- **`{reason}`**: {cnt} صفقات | الخسارة الكلية: `-${abs(tot):,.2f}` | متوسط: `-${abs(avg_l):,.2f}`")
        else:
            lines.append("- لا توجد خسائر مسجلة في نافذة التقرير.")

        lines.append("\n💡 للتصدير بصيغة ملف، استخدم `/report csv` أو `/report json`.")
        return "\n".join(lines)

    async def generate_weekly_digest(self, lookback_days: float = 7.0) -> str:
        """
        Generates a comprehensive weekly multi-day institutional performance report.
        """
        hours = lookback_days * 24.0
        metrics = await self.compute_portfolio_metrics(lookback_hours=hours)

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        net_pnl = metrics["net_pnl_usd"]
        pnl_icon = "🟢" if net_pnl >= 0 else "🔴"
        pnl_sign = "+" if net_pnl >= 0 else ""

        lines = [
            "## 🏛️ التقرير الكمي الأسبوعي المؤسسي (Weekly Quantitative Audit Report)",
            f"**تاريخ الإصدار:** `{now_str}` | **فترة التدقيق:** آخر `{lookback_days:.0f}` أيام",
            f"- **صافي العائد الأسبوعي:** {pnl_icon} `{pnl_sign}${net_pnl:,.2f}`",
            f"- **إجمالي الصفقات:** `{metrics['total_trades']}` (فوز: `{metrics['win_rate_pct']}%`)",
            f"- **معامل الربح الأسبوعي (Profit Factor):** `{metrics['profit_factor']:.2f}`",
            f"- **معدل سورتينو (Sortino Ratio):** `{metrics['sortino_ratio']:.2f}`",
            f"- **أقصى تراجع أسبوعي (Max Drawdown):** `{metrics['max_drawdown_pct']:.2f}%`",
            f"- **إجمالي الأرباح الإجمالية:** `+${metrics['gross_profit_usd']:,.2f}`",
            f"- **إجمالي الخسائر الإجمالية:** `-${metrics['gross_loss_usd']:,.2f}`\n",
            "### 🔬 تقييم استقرار الاستراتيجيات (Strategy Health Assessment)",
        ]

        strat_breakdown = metrics["strategy_breakdown"]
        if strat_breakdown:
            for s_name, s in strat_breakdown.items():
                health = "🟢 STABLE" if s["win_rate_pct"] >= 50.0 and s["pnl_usd"] >= 0 else "🟡 UNDER OBSERVATION" if s["pnl_usd"] >= 0 else "🔴 DEGRADED"
                lines.append(f"- **`{s_name}`** [{health}]: {s['trades']} صفقات | فوز: `{s['win_rate_pct']}%` | PnL: `${s['pnl_usd']:+,.2f}`")
        else:
            lines.append("- لا توجد بيانات كافية خلال فترة الأسبوع.")

        return "\n".join(lines)

    async def export_report_csv(self, lookback_hours: float = 24.0) -> str:
        """
        Exports all closed trades in the lookback window as a valid CSV string.
        """
        trades = await self.ledger.get_closed_trades_dataset(lookback_hours=lookback_hours, limit=5000)
        output = io.StringIO()
        fieldnames = [
            "trade_id",
            "token_address",
            "chain",
            "side",
            "entry_price",
            "exit_price",
            "realized_pnl_usd",
            "roi_pct",
            "holding_time_s",
            "exit_reason",
            "signal_source",
            "strategy_name",
            "timestamp_utc",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for t in trades:
            ts_ns = int(t.get("timestamp_ns", 0))
            dt_str = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if ts_ns else ""
            writer.writerow({
                "trade_id": t.get("trade_id", ""),
                "token_address": t.get("token_address", ""),
                "chain": t.get("chain", ""),
                "side": t.get("side", ""),
                "entry_price": t.get("entry_price", ""),
                "exit_price": t.get("exit_price", ""),
                "realized_pnl_usd": t.get("realized_pnl_usd", ""),
                "roi_pct": t.get("roi_pct", ""),
                "holding_time_s": t.get("holding_time_s", ""),
                "exit_reason": t.get("exit_reason", ""),
                "signal_source": t.get("signal_source", ""),
                "strategy_name": t.get("strategy_name", ""),
                "timestamp_utc": dt_str,
            })

        return output.getvalue()

    async def export_report_json(
        self,
        lookback_hours: float = 24.0,
        as_string: bool = False,
        indent: Optional[int] = 2,
    ) -> dict[str, Any] | str:
        """
        Exports structured metrics and individual trade rows in JSON-ready dictionary format or JSON string.
        """
        metrics = await self.compute_portfolio_metrics(lookback_hours=lookback_hours)
        trades = await self.ledger.get_closed_trades_dataset(lookback_hours=lookback_hours, limit=5000)

        formatted_trades = []
        for t in trades:
            ts_ns = int(t.get("timestamp_ns", 0))
            pnl_val = t.get("realized_pnl_usd", 0.0)
            pnl_dec = pnl_val if isinstance(pnl_val, Decimal) else Decimal(str(pnl_val or 0.0))
            formatted_trades.append({
                "trade_id": t.get("trade_id"),
                "token_address": t.get("token_address"),
                "chain": t.get("chain"),
                "side": t.get("side"),
                "entry_price": float(t.get("entry_price", 0.0)),
                "exit_price": float(t.get("exit_price", 0.0)),
                "realized_pnl_usd": float(pnl_dec),
                "roi_pct": float(t.get("roi_pct", 0.0)),
                "holding_time_s": float(t.get("holding_time_s", 0.0)),
                "exit_reason": t.get("exit_reason"),
                "strategy_name": t.get("strategy_name"),
                "timestamp_ns": ts_ns,
            })

        payload: dict[str, Any] = {
            "metadata": {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "generated_at_timestamp": time.time(),
                "lookback_hours": lookback_hours,
            },
            "summary_metrics": metrics,
            "closed_trades": formatted_trades,
        }
        if as_string:
            return json.dumps(payload, indent=indent)
        return payload
