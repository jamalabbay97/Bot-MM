"""
alpha_engine.chat_interface — Conversational AI Supervisor & Decision Explainer
==============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio | RAG over Decision Records | Interactive CLI & Bot
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
import re
from typing import Any, Optional

try:
    from rich.console import Console
    from rich.markdown import Markdown
    from rich.panel import Panel
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    Console = None  # type: ignore

from alpha_engine.config import EngineConfig
from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
from alpha_engine.engine.staging import Wave2StagingBuffer
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.decisions import DecisionType

logger = logging.getLogger(__name__)


class ConversationalSupervisor:
    """
    Institutional Conversational AI Copilot and Explainer.
    Executes RAG / tool-calling over the local SQLite decision ledger,
    Wave-2 staging buffer, outcome tracker, and execution state.
    """

    def __init__(
        self,
        ledger: SQLiteLedger,
        supervisor: Optional[AlphaSupervisorAI] = None,
        staging_buffer: Optional[Wave2StagingBuffer] = None,
        position_book: Optional[PositionBook] = None,
        config: Optional[EngineConfig] = None,
        parameter_tuner: Optional[Any] = None,
        metrics: Optional[Any] = None,
        ai_supervisor: Optional[Any] = None,
        revival_buffer: Optional[Any] = None,
        swarm_coordinator: Optional[Any] = None,
    ) -> None:
        self.ledger = ledger
        self.supervisor = supervisor or ai_supervisor
        self.staging_buffer = staging_buffer
        self.revival_buffer = revival_buffer
        self.position_book = position_book
        self.parameter_tuner = parameter_tuner
        self.metrics = metrics
        self.config = config or EngineConfig()
        self.swarm_coordinator = swarm_coordinator
        self._asset_map: dict[str, str] = {}

    # =========================================================================
    # RAG / Tool-Calling Implementations
    # =========================================================================

    async def tool_why_decision(self, token_address: str, time_filter: Optional[str] = None) -> str:
        """
        Explain the decision(s) made on a specific token.
        Compares multiple evaluation points (e.g. why skipped at 14:00 but entered at 16:00).
        """
        clean_addr = token_address.strip()
        records = await self.ledger.get_token_decision_history(clean_addr, limit=20)
        
        # Check Agent Deliberation Logs (Unified Ruflo Swarm + Council Arbiter)
        delibs = []
        if hasattr(self.ledger, "get_agent_deliberations"):
            try:
                delibs = await self.ledger.get_agent_deliberations(token_address=clean_addr, limit=3)
            except Exception as exc:
                logger.debug("Failed to fetch agent deliberations: %s", exc)

        if not records and not delibs:
            return f"❌ No decision records found in ledger for token `{clean_addr}`."

        lines = [f"### 📋 Audit Trail & Decision Analysis for `{clean_addr[:12]}...`\n"]

        if records:
            # Chronological order
            records = sorted(records, key=lambda r: r.timestamp_ns)
            lines.append(f"**Total Evaluations Recorded:** {len(records)}\n")

            for idx, rec in enumerate(records, 1):
                dt = datetime.fromtimestamp(rec.timestamp_ns / 1_000_000_000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                dt_color = "🟢 ENTER" if rec.decision_type == DecisionType.ENTER else "🔴 SKIP"
                lines.append(f"#### Evaluation #{idx}: {dt_color} at `{dt}`")
                lines.append(f"- **Strategy / Pattern:** `{rec.strategy_pattern}`")
                lines.append(f"- **Confidence Score:** `{rec.confidence_score:.2f}`")
                lines.append(f"- **Explicit Rationale:** {rec.reason}")

                if rec.active_rules:
                    rules_str = ", ".join(f"`{k}`: {v}" for k, v in rec.active_rules.items())
                    lines.append(f"- **Active Rule Triggers:** {rules_str}")

                if rec.metadata:
                    meta_items = [f"{k}={v}" for k, v in rec.metadata.items() if k != "global_risk_mode"]
                    if meta_items:
                        lines.append(f"- **State Snapshot:** {', '.join(meta_items)}")
                lines.append("")

            if len(records) >= 2:
                first = records[0]
                last = records[-1]
                if first.decision_type == DecisionType.SKIP and last.decision_type == DecisionType.ENTER:
                    t1 = datetime.fromtimestamp(first.timestamp_ns / 1_000_000_000, tz=timezone.utc).strftime("%H:%M")
                    t2 = datetime.fromtimestamp(last.timestamp_ns / 1_000_000_000, tz=timezone.utc).strftime("%H:%M")
                    lines.append("### 🔍 Inflection Shift Analysis:")
                    lines.append(
                        f"At **{t1}**, the token was **SKIPPED** due to unmet criteria ({first.reason}).\n"
                        f"By **{t2}**, the token underwent accumulation and confirmed an inflection "
                        f"breakout pattern ({last.reason}), satisfying all mathematical gates for **ENTER**."
                    )

        if delibs:
            lines.append("\n### 🏛️ مداولات سرب الوكلاء والذاكرة العرضية (Multi-Agent Deliberations & Episodic Memory):")
            for d in delibs:
                dt = datetime.fromtimestamp(d["timestamp_ns"] / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                lines.append(f"**جلسة التداول [{dt}]** — القرار: `{d['final_decision']}` | الاستراتيجية: `{d['selected_strategy_id']}`")
                lines.append(f"- **خلاصة القرار:** {d['rationale']}")
                try:
                    votes = json.loads(d["agent_votes_json"])
                    vote_parts = [f"`{role}`: {'🟢' if v else '🔴'}" for role, v in votes.items()]
                    lines.append(f"- **أصوات الوكلاء (Agent Votes):** {' | '.join(vote_parts)}")
                except Exception:
                    pass
                try:
                    matches = json.loads(d["episodic_memory_matches_json"])
                    if matches:
                        lines.append("- **تطابقات الذاكرة التاريخية (Cosine Similarity Matches):**")
                        for m in matches:
                            lines.append(
                                f"  * عملة `{m.get('token_address', '')[:10]}...` | تشابه كوزين: `{float(m.get('similarity', 0.0))*100:.1f}%` | العائد: `{float(m.get('realized_roi_pct', 0.0)):+.1f}%`"
                            )
                except Exception:
                    pass
                lines.append("")

        return "\n".join(lines)

    async def tool_strategy_performance(self, hours: float = 12.0) -> str:
        """
        Calculate and compare win-rates, total decisions, and realized PnL
        across strategies over the specified lookback window.
        """
        stats = await self.ledger.get_decision_stats(lookback_hours=hours)
        total_dec = stats.get("total_decisions", 0)
        enter_cnt = stats.get("enter_count", 0)
        skip_cnt = stats.get("skip_count", 0)
        strat_perf = stats.get("strategy_performance", {})

        lines = [f"### 📊 Strategy & Pattern Performance (Past {hours:.1f} Hours)\n"]
        lines.append(f"- **Total Evaluations:** {total_dec} | **Approved (ENTER):** {enter_cnt} | **Vetoed (SKIP):** {skip_cnt}\n")

        if not strat_perf:
            lines.append("No closed trades recorded in this lookback window to benchmark win-rates.")
            breakdown = stats.get("decision_breakdown", {})
            if breakdown:
                lines.append("\n**Evaluation Distribution by Pattern:**")
                for strat, d_counts in breakdown.items():
                    lines.append(f"- `{strat}`: {d_counts.get('total', 0)} evaluations ({d_counts.get('ENTER', 0)} ENTER, {d_counts.get('SKIP', 0)} SKIP)")
            return "\n".join(lines)

        lines.append("| Strategy Pattern | Closed Trades | Wins | Win Rate (%) | Total PnL ($) |")
        lines.append("| :--- | :---: | :---: | :---: | :---: |")

        best_pattern = ""
        best_win_rate = -1.0

        for pattern, pdata in strat_perf.items():
            wr = pdata.get("win_rate_pct", 0.0)
            trades = pdata.get("trades", 0)
            wins = pdata.get("wins", 0)
            pnl = pdata.get("total_pnl", 0.0)
            lines.append(f"| `{pattern}` | {trades} | {wins} | **{wr:.1f}%** | ${pnl:.2f} |")

            if wr > best_win_rate and trades > 0:
                best_win_rate = wr
                best_pattern = pattern

        if best_pattern:
            lines.append(f"\n🏆 **Top Performing Strategy:** `{best_pattern}` with a **{best_win_rate:.1f}% win rate**.")

        return "\n".join(lines)

    async def tool_list_staged_tokens(self) -> str:
        """
        List all tokens currently buffered in the Wave-2 staging watchlist
        along with their accumulation breakout scores and metrics.
        """
        if self.staging_buffer is None:
            return "⚠️ Wave-2 Staging Buffer is not initialized on this engine instance (مخزن المراقبة غير مهيأ)."

        staged_list = self.staging_buffer.get_staged_status_summary()
        if not staged_list:
            return "📭 **Wave-2 Staging Buffer | قائمة المراقبة:** No tokens currently undergoing accumulation monitoring (لا توجد عملات قيد المراقبة والتجميع حالياً)."

        lines = [f"### 🌊 Active Wave-2 Staging Watchlist | قائمة المراقبة والتجميع ({len(staged_list)} Tokens)\n"]
        lines.append("| Token Mint | Age | TTL Left | Accum. Score | Vol Surge | Buy Delta | Dev % | Top10 % | Breakout? |")
        lines.append("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")

        for s in staged_list:
            mint_short = f"`{s['token_address'][:8]}...`"
            age = f"{s['age_minutes']:.0f}m"
            ttl = f"{s['ttl_remaining_minutes']:.0f}m"
            score = f"**{s['accumulation_score']:.2f}**"
            surge = f"{s['volume_surge_multiplier']:.1f}x"
            delta = f"{s['net_buy_delta']*100:.0f}%"
            dev = f"{s['dev_balance_pct']*100:.1f}%"
            top10 = f"{s['top10_concentration']*100:.1f}%"
            signaled = "🚀 YES" if s['breakout_signaled'] else "⏳ NO"
            lines.append(f"| {mint_short} | {age} | {ttl} | {score} | {surge} | {delta} | {dev} | {top10} | {signaled} |")

        lines.append("\n*Tokens trigger a breakout signal when Accumulation Score >= 0.75, Surge >= 2.5x, Buy Delta >= 65%, and CTO verified.*")
        return "\n".join(lines)

    async def tool_explain_exit(self, identifier: str) -> str:
        """
        Explain the exit rationale for a specific position or trade.
        """
        clean_id = identifier.strip()
        # Query ledger for exit decisions
        decisions = await self.ledger.get_decisions(token_address=clean_id, limit=10)
        exit_decisions = [d for d in decisions if d.decision_type in (DecisionType.EXIT, DecisionType.TRAIL_STOP)]

        if exit_decisions:
            d = exit_decisions[0]
            dt = datetime.fromtimestamp(d.timestamp_ns / 1_000_000_000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            return (
                f"### 🛑 Exit Analysis for `{clean_id}`\n"
                f"- **Exit Type:** `{d.decision_type.value}` at `{dt}`\n"
                f"- **Strategy:** `{d.strategy_pattern}`\n"
                f"- **Explicit Rationale:** {d.reason}\n"
                f"- **Rules State:** {json.dumps(d.active_rules)}\n"
            )

        # Check PositionBook closed trades if book is attached
        if self.position_book:
            for lot in self.position_book.get_closed_lots():
                if clean_id.lower() in (lot.lot_id.lower(), lot.token_address.lower()):
                    peak_p = getattr(lot, "peak_price", getattr(lot, "highest_price_observed", lot.entry_price))
                    pnl_pct = float((peak_p - lot.entry_price) / lot.entry_price * 100) if lot.entry_price > 0 else 0.0
                    platform_str = getattr(lot, "platform", "DexScan")
                    stage = getattr(lot, "exit_stage", getattr(lot, "current_stage", None))
                    stage_str = stage.value if hasattr(stage, "value") else str(stage or "NONE")
                    sl_floor = getattr(lot, "trailing_stop_price", getattr(lot, "trailing_sl_price", Decimal(0)))
                    tp1 = getattr(lot, "scaled_out_50_pct", getattr(lot, "tp1_sold", False))
                    tp2 = getattr(lot, "ai_tp2_sold", getattr(lot, "tp2_sold", False))
                    return (
                        f"### 🛑 Closed Position Lot [{lot.lot_id[:8]}] for `{lot.token_address[:10]}`\n"
                        f"- **Platform:** `{platform_str}`\n"
                        f"- **Current Stage:** `{stage_str}`\n"
                        f"- **Entry Price:** `{lot.entry_price}` | **Highest Peak:** `{peak_p}`\n"
                        f"- **Trailing Stop Floor:** `{sl_floor}`\n"
                        f"- **Peak Gain:** `+{pnl_pct:.1f}%`\n"
                        f"- **TP1 Executed:** `{tp1}` | **TP2 Executed:** `{tp2}`\n"
                    )

        return f"ℹ️ No specific exit event found for `{clean_id}`."

    async def tool_list_revival_tokens(self) -> str:
        """
        List all tokens currently buffered in the RevivalBreakoutBuffer
        monitoring aged tokens (2h to 10d) breaking out from consolidation.
        """
        if self.revival_buffer is None:
            return "⚠️ Revival Breakout Buffer is not initialized on this engine instance (مخزن مراقبة الانبعاث غير مهيأ)."

        staged_dict = self.revival_buffer.get_all_staged()
        if not staged_dict:
            return "📭 **Revival Breakout Buffer:** No aged tokens currently undergoing dormancy/revival monitoring."

        lines = [f"### 🔄 Active Revival & CTO Breakout Watchlist ({len(staged_dict)} Tokens)\n"]
        lines.append("| Token Mint | Age (h) | 5m Vol ($) | Vol Surge | Buy Delta | 5m RSI | Base Price | Current Price | Status |")
        lines.append("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")

        for addr, tok in staged_dict.items():
            mint_short = f"`{tok.token_address[:8]}...`"
            age_h = tok.token_age_hours
            vol_5m = float(tok.five_min_volume)
            surge = tok.volume_surge_multiplier
            buy_delta = tok.net_buy_delta * 100
            rsi = tok.compute_5m_rsi(14)
            bp = f"{float(tok.consolidation_base_price):.8f}"
            cp = f"{float(tok.latest_price):.8f}"
            status = "🚨 SURGE" if tok.surge_detected else ("💤 DORMANT" if tok.dormant_detected else "MONITORING")
            lines.append(f"| {mint_short} | {age_h:.1f}h | ${vol_5m:,.0f} | {surge:.1f}x | {buy_delta:.0f}% | {rsi:.1f} | {bp} | {cp} | {status} |")

        return "\n".join(lines)

    async def tool_override_parameter(self, command_text: str, value: Any = None) -> str:
        """
        Execute interactive supervisory overrides:
          - risk <EXPAND|NEUTRAL|DEFENSIVE>
          - blacklist <mint_or_address>
          - whitelist <mint_or_address>
          - ttl <seconds>
          - surge_k <multiplier>
          - stage <mint> [initial_price]
          - strict_veto <true|false>
        """
        if value is not None:
            command_text = f"{command_text} {value}"

        parts = command_text.strip().split()
        if not parts:
            return "Usage: /override <risk|blacklist|whitelist|ttl|surge_k|stage|strict_veto> <value>"

        action = parts[0].lower()
        val = parts[1] if len(parts) > 1 else ""

        if action in ("surge_k", "surge"):
            try:
                k = float(val)
                if self.staging_buffer:
                    self.staging_buffer.set_hyperparameters(volume_surge_multiplier_k=k)
                if self.parameter_tuner and hasattr(self.parameter_tuner, "current_params"):
                    self.parameter_tuner.current_params.wave2_surge_k = k
                return f"✅ Wave-2 Volume Surge Multiplier k updated to `{k:.2f}x`."
            except ValueError:
                return "Invalid numeric value for surge_k."

        elif action == "risk":
            if val.upper() in ("EXPAND", "NEUTRAL", "DEFENSIVE"):
                if self.supervisor:
                    self.supervisor.set_global_risk_mode(val.upper())
                return f"✅ Global risk appetite successfully overridden to `{val.upper()}`."
            return "Invalid risk mode. Choose from EXPAND, NEUTRAL, DEFENSIVE."

        elif action == "blacklist":
            if not val:
                return "Specify address or mint to blacklist."
            if self.supervisor:
                self.supervisor._blacklisted_entities.add(val)
            return f"🚫 Address `{val}` added to blacklist."

        elif action == "whitelist":
            if not val:
                return "Specify address or mint to remove from blacklist."
            if self.supervisor and val in self.supervisor._blacklisted_entities:
                self.supervisor._blacklisted_entities.remove(val)
            return f"✅ Address `{val}` removed from blacklist."

        elif action == "ttl":
            try:
                ttl_s = float(val)
                if self.staging_buffer:
                    self.staging_buffer.set_hyperparameters(ttl_seconds=ttl_s)
                return f"⏱️ Wave-2 Staging TTL set to `{ttl_s:.0f} seconds` ({ttl_s/3600:.1f} hours)."
            except ValueError:
                return "TTL must be a valid number of seconds (e.g. 10800)."

        elif action == "stage":
            if not val:
                return "Specify token address to force into Wave-2 staging."
            price = Decimal(parts[2]) if len(parts) > 2 else Decimal("0.000000028")
            if self.staging_buffer:
                self.staging_buffer.stage_token(
                    token_address=val,
                    initial_price=price,
                )
                return f"🌊 Token `{val[:10]}` manually injected into Wave-2 staging buffer with initial price `{price}`."
            return "Staging buffer not attached."

        elif action == "strict_veto":
            flag = val.lower() in ("true", "1", "yes")
            if self.supervisor:
                self.supervisor.set_strict_veto(flag)
            return f"🛡️ Strict veto set to `{flag}`."

        return f"Unknown override action `{action}`. Supported: risk, blacklist, whitelist, ttl, stage, strict_veto."

    async def tool_list_assets(self) -> str:
        """List recently detected assets with a numerical ID for easy investigation."""
        tokens = await self.ledger.get_recent_assets(limit=30)
        if not tokens:
            return "📭 لا توجد عملات تم معالجتها مؤخراً (No assets processed recently)."

        self._asset_map.clear()
        lines = ["### 🔍 العملات المكتشفة مؤخراً (Recently Processed Assets)\n"]
        lines.append("قم بإرسال `/investigate <الرقم>` لعرض التتبع الكامل للعملة.\n")

        for idx, token in enumerate(tokens, 1):
            self._asset_map[str(idx)] = token
            lines.append(f"**{idx}.** `{token}`")

        return "\n".join(lines)

    async def tool_investigate_asset(self, identifier: str) -> str:
        """Provides the full lifecycle trace of a token."""
        # Check if identifier is a mapped number or direct address
        token_ca = self._asset_map.get(identifier.strip(), identifier.strip())

        traces = await self.ledger.get_asset_trace(token_ca)
        if not traces:
            return f"❌ لم يتم العثور على تتبع للعملة (No trace found for): `{token_ca}`"

        lines = [f"### 🔬 تحقيق شامل حول العملة (Lifecycle Trace for) `{token_ca}`\n"]

        for t in traces:
            dt = datetime.fromtimestamp(t['timestamp_ns'] / 1e9, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]

            # Icon styling based on status
            status = t['status']
            icon = "🟢" if status == "PASSED" else "🔴" if status == "REJECTED" else "🟡" if status == "IGNORED" else "⚠️"

            lines.append(f"**{icon} [{dt}] {t['stage']}**")
            lines.append(f"- **Component:** `{t['component']} -> {t['function_name']}()` [{t['duration_ms']:.1f}ms]")

            if status == "REJECTED" and t.get('reason'):
                lines.append(f"- **❌ Rejection Reason:** {t['reason']}")

            if t.get('output_data_json') and t['output_data_json'] != "{}":
                try:
                    out = json.loads(t['output_data_json'])
                    decision = out.get('decision')
                    conf = out.get('confidence_score')
                    if decision:
                        conf_str = f" (Confidence: {float(conf):.2f})" if conf is not None else ""
                        lines.append(f"- **AI Decision:** `{decision}`{conf_str}")
                    flags = out.get('flags') or (out.get('security_assessment', {}).get('flags') if isinstance(out.get('security_assessment'), dict) else [])
                    if flags:
                        lines.append(f"- **Security Flags:** {', '.join(flags)}")
                    rationale = out.get('rationale') or (out.get('wallet_audit', {}).get('rationale') if isinstance(out.get('wallet_audit'), dict) else '')
                    if rationale:
                        lines.append(f"- **AI Rationale:** {rationale}")
                except Exception:
                    pass
            lines.append("")

        return "\n".join(lines)

    async def tool_system_doctor(self) -> str:
        try:
            from alpha_engine.reach.doctor import SystemDoctor
            doctor = SystemDoctor()
            report = await doctor.run_diagnostics()
            return doctor.format_cli_summary(report)
        except Exception as e:
            return f"System Doctor encountered an error: {e}"
            
    async def tool_deep_research(self, query: str) -> str:
        try:
            if not self.supervisor:
                return "AlphaSupervisorAI is not attached, cannot perform deep web audit."
            result = await self.supervisor.deep_web_audit(query, query)
        except Exception as e:
            return f"Deep Research encountered an error: {e}"

    async def tool_swarm_status(self) -> str:
        """Query and display Ruflo multi-agent swarm operational state and metrics."""
        if not self.swarm_coordinator:
            from alpha_engine.ruflo.swarm import RufloSwarmCoordinator
            self.swarm_coordinator = RufloSwarmCoordinator(config=self.config)

        status = self.swarm_coordinator.get_swarm_status()
        lines = [
            "### 🐝 حالة سرب الوكلاء التداولي المتعدد (Ruflo Trading Swarm Status)\n",
            f"- **الهيكلية (Topology):** `{status['topology'].upper()}`",
            f"- **بروتوكول الإجماع (Consensus):** `{status['consensus'].upper()}`",
            f"- **حالة التشغيل:** `{'نشط (ACTIVE)' if status['is_running'] else 'متوقف (IDLE)'}`",
            f"- **الذاكرة العرضية (Episodic Memory):** `{status['episodic_memory_episodes']}` صفقات مسجلة",
            f"- **وزن إشارات السوشيال الحالي:** `{status['active_social_weight']:.3f}`\n",
            "#### 🤖 وكلاء السرب التخصصيون (Specialized Swarm Agents):",
        ]
        for role, a in status["agents"].items():
            lines.append(
                f"- **`[{role}]` {a['name']}**: الحالة `{a['status']}` | أدوات مفعّلة: `{a['tools_count']}` | مهام منفذة: `{a['tasks_executed']}`"
            )
        return "\n".join(lines)

    # =========================================================================
    # Conversational RAG Router
    # =========================================================================

    async def tool_experiments(self) -> str:
        """Display the live leaderboard of Champion vs Challenger strategies."""
        from alpha_engine.engine.experimentation import StrategyTournamentRegistry
        target = getattr(self, "execution_target", None)
        reg = getattr(target, "strategy_tournament", None) if target else None
        if reg is None:
            reg = StrategyTournamentRegistry(ledger=self.ledger)
        board = reg.get_leaderboard()
        lines = [
            "### 🏆 لوحة صدارة تجارب الاستراتيجيات (Strategy Tournament Leaderboard)\n",
            "| الحالة (Status) | الاستراتيجية | النظام (Regime) | الصفقات | نسبة الفوز | معامل الربح (PF) | Sortino | Max DD | صافي الأرباح | الترقية |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ]
        for b in board:
            lines.append(
                f"| {b['status']} | `{b['name']}` | `{b['regime']}` | {b['trades']} | {b['win_rate']} | {b['profit_factor']} | {b['sortino']} | {b['max_dd']} | {b['net_pnl']} | {b['promotion_ready']} |"
            )
        lines.append("\n💡 **شروط ترقية الاستراتيجية للبطولة (Champion Promotion Rules):**")
        lines.append("- إتمام ما لا يقل عن 30 صفقة في وضع الاختبار الظلي (Shadow Mode)")
        lines.append("- معامل ربح (PF) > 1.35 ومعدل Sortino أعلى من البطل الحالي")
        lines.append("- أقصى تراجع (Max Drawdown) < 8.0%")
        lines.append("- اعتماد أرباح الصفقات الشاذة (Outlier Dependency) <= 60%")
        lines.append("- اجتياز اختبار استقرار المعلمات (Perturbation Test +/-10%)")
        return "\n".join(lines)

    async def tool_council_debate(self, token_ca: str) -> str:
        """Runs the unified Multi-Agent Swarm & Council deliberation and formats transcript."""
        clean_ca = token_ca.strip()
        if not clean_ca:
            return "الرجاء إدخال عنوان العقد. مثال: `/council 7xKXtg...`"

        is_evm = clean_ca.startswith("0x") and len(clean_ca) == 42
        from alpha_engine.models.enums import ChainIdentifier
        chain = ChainIdentifier.BASE_MAINNET if is_evm else ChainIdentifier.SOLANA_MAINNET

        if not self.swarm_coordinator:
            from alpha_engine.ruflo.swarm import RufloSwarmCoordinator
            self.swarm_coordinator = RufloSwarmCoordinator(config=self.config)

        res = await self.swarm_coordinator.execute_trade_consensus(
            token_address=clean_ca,
            chain=chain,
            execute_fill=False,
        )

        status_icon = "🟢 APPROVED" if res.passed else "🔴 VETO / REJECT"
        lines = [
            f"### 🏛️ مجلس الوكلاء التداولي المتحد: تقرير المناظرة لعقد `{clean_ca[:12]}...`\n",
            f"**القرار النهائي:** `{status_icon}` | **السبب:** {res.rationale}",
            f"**الاستراتيجية المقترحة:** `{res.selected_strategy_id}`\n",
            "**أصوات الوكلاء (Unified Agent Votes):**",
        ]
        for role, passed in res.agent_votes.items():
            lines.append(f"- **`[{role}]`**: {'🟢 APPROVED' if passed else '🔴 VETO'}")

        if res.episodic_memory_matches:
            lines.append("\n**تطابقات الذاكرة العرضية (Episodic Memory Cosine Matches):**")
            for m in res.episodic_memory_matches:
                lines.append(
                    f"- عملة `{m.get('token_address', '')[:10]}...` | تشابه: `{float(m.get('similarity', 0.0))*100:.1f}%` | عائد تاريخي: `{float(m.get('realized_roi_pct', 0.0)):+.1f}%`"
                )

        if res.debate_transcript:
            lines.append("\n**محضر مناظرة الوكلاء (Council Debate Transcript):**")
            for line in res.debate_transcript:
                lines.append(f"- {line}")

        return "\n".join(lines)

    async def tool_trace_lifecycle(self, token_ca: str) -> str:
        """
        Trace the 7-stage lifecycle of a token through the engine:
        INGESTION -> SECURITY -> STAGING -> COUNCIL -> SUPERVISOR -> EXECUTION -> FEEDBACK.
        """
        clean_ca = token_ca.strip()
        if not clean_ca:
            return "الرجاء إدخال عنوان العقد المراد تتبعه. مثال: `/trace 7xKXtg...`"

        events = await self.ledger.get_asset_trace(clean_ca)
        if not events:
            delibs = await self.ledger.get_agent_deliberations(token_address=clean_ca, limit=1)
            if delibs:
                d = delibs[0]
                dt = datetime.fromtimestamp(d["timestamp_ns"] / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                return (
                    f"### 🔍 تتبع مسار العملة (Lifecycle Trace) لعقد `{clean_ca[:12]}...`\n"
                    f"- **تاريخ المعالجة:** `{dt}`\n"
                    f"- **مرحلة المجلس (COUNCIL):** `{d['final_decision']}` (استراتيجية: `{d['selected_strategy_id']}`)\n"
                    f"- **الخلاصة:** {d['rationale'][:250]}...\n\n"
                    f"💡 لمزيد من التفاصيل استخدم `/council_debate {clean_ca}` أو `/why {clean_ca}`"
                )
            return f"❌ لم يتم العثور على سجلات تتبع (Trace Events) للعملة `{clean_ca}`."

        stage_emojis = {
            "INGESTION": "📥",
            "SECURITY": "🛡️",
            "SECURITY_SCREENING": "🛡️",
            "STAGING": "🌊",
            "FILTERING": "🌊",
            "COUNCIL": "🏛️",
            "DECISION": "🏛️",
            "SUPERVISOR": "🤖",
            "AI_ANALYSIS": "🤖",
            "EXECUTION": "⚡",
            "FEEDBACK": "🔄",
        }

        status_emojis = {
            "PASSED": "✅",
            "REJECTED": "🔴",
            "ERROR": "⚠️",
            "PENDING": "⏳",
            "IGNORED": "⚪",
        }

        lines = [
            f"### 🔍 المسار المؤسسي للعملة (7-Stage Institutional Lifecycle Trace)\n",
            f"**عنوان العقد:** `{clean_ca}`\n",
            f"**عدد المحطات المسجلة:** {len(events)} محطة\n",
        ]

        total_duration = 0.0
        for idx, ev in enumerate(events, 1):
            stage = ev.get("stage", "UNKNOWN")
            status = ev.get("status", "PENDING")
            comp = ev.get("component", "engine")
            fn = ev.get("function_name", "")
            dur = float(ev.get("duration_ms", 0.0))
            reason = ev.get("reason") or "Passed checks"
            ts_ns = int(ev.get("timestamp_ns", 0))
            dt = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3] if ts_ns else "N/A"
            total_duration += dur

            s_icon = stage_emojis.get(stage, "📍")
            stat_icon = status_emojis.get(status, "❓")

            lines.append(f"**{idx}. {s_icon} [{stage}]** {stat_icon} `{status}` (`{dt}` | {dur:.1f}ms)")
            lines.append(f"   - **المكون:** `{comp}::{fn}`")
            lines.append(f"   - **النتيجة / السبب:** {reason}")
            out_data = ev.get("output_data")
            if out_data and out_data != "{}":
                try:
                    parsed_out = json.loads(out_data) if isinstance(out_data, str) else out_data
                    if isinstance(parsed_out, dict):
                        compact_out = ", ".join(f"{k}: {v}" for k, v in list(parsed_out.items())[:3] if k != "traceback")
                        if compact_out:
                            lines.append(f"   - **المخرجات:** `{compact_out}`")
                except Exception:
                    pass
            lines.append("")

        lines.append(f"⏱️ **إجمالي زمن المعالجة عبر المراحل:** `{total_duration:.1f}ms`")
        return "\n".join(lines)

    async def tool_pipeline_flow(self, lookback_hours: float = 24.0) -> str:
        """
        Visual ASCII & emoji pipeline state diagram displaying stage throughput
        and drop-off rates across all 7 stages over the lookback window.
        """
        summary = await self.ledger.get_stage_flow_summary(lookback_hours=lookback_hours)

        stages_order = [
            ("INGESTION", "📥 Ingestion", "Raw pool detections & mint listener"),
            ("SECURITY", "🛡️ Security", "Honeypot, mint/freeze authority & LP locks"),
            ("STAGING", "🌊 Staging", "Wave-2 micro-velocity & revival buffers"),
            ("COUNCIL", "🏛️ Council", "Reach, Forensic & Quant 4-round debate"),
            ("SUPERVISOR", "🤖 Supervisor", "Global risk posture & Bayesian telemetry"),
            ("EXECUTION", "⚡ Execution", "Dual-engine CPMM routing & MEV protection"),
            ("FEEDBACK", "🔄 Feedback", "Closed-loop ledger loss attribution & self-tuning"),
        ]

        lines = [
            f"### 🔬 مخطط تدفق خط الأنابيب المؤسسي (Institutional Pipeline Flow)\n",
            f"**النافذة الزمنية:** آخر `{lookback_hours:.0f}` ساعة | **المراحل السبع المؤسسية:**\n",
            "```text",
        ]

        for stage_key, label, desc in stages_order:
            data = summary.get(stage_key, {})
            if stage_key == "SECURITY" and "SECURITY_SCREENING" in summary:
                s_data = summary["SECURITY_SCREENING"]
                data = {k: data.get(k, 0) + s_data.get(k, 0) for k in ("PASSED", "REJECTED", "ERROR", "TOTAL")}
            elif stage_key == "STAGING" and "FILTERING" in summary:
                s_data = summary["FILTERING"]
                data = {k: data.get(k, 0) + s_data.get(k, 0) for k in ("PASSED", "REJECTED", "ERROR", "TOTAL")}
            elif stage_key == "COUNCIL" and "DECISION" in summary:
                s_data = summary["DECISION"]
                data = {k: data.get(k, 0) + s_data.get(k, 0) for k in ("PASSED", "REJECTED", "ERROR", "TOTAL")}
            elif stage_key == "SUPERVISOR" and "AI_ANALYSIS" in summary:
                s_data = summary["AI_ANALYSIS"]
                data = {k: data.get(k, 0) + s_data.get(k, 0) for k in ("PASSED", "REJECTED", "ERROR", "TOTAL")}

            total = data.get("TOTAL", 0)
            passed = data.get("PASSED", 0)
            rejected = data.get("REJECTED", 0)

            pass_rate = (passed / total * 100.0) if total > 0 else 0.0
            drop_rate = 100.0 - pass_rate if total > 0 else 0.0

            lines.append(f"[{label.upper()}] ─── ({total:,} tokens)")
            lines.append(f"  ├── ✅ Passed: {passed:,} ({pass_rate:.1f}%)")
            lines.append(f"  └── ❌ Rejected / Filtered: {rejected:,} ({drop_rate:.1f}%)")
            lines.append(f"        │")

        lines.append("        ▼")
        lines.append("  [CLOSED-LOOP REINFORCEMENT & SELF-LEARNING]")
        lines.append("```")

        lines.append("\n💡 **توجيه:** استخدم `/trace <CA>` لتتبع عملة بعينها عبر المحطات السبع بالتفصيل.")
        return "\n".join(lines)

    async def tool_status_detailed(self) -> str:
        """
        Deep institutional telemetry card: active risk mode, tournament leaderboard,
        open lots, cash balance, loss attribution clusters, and win-rate.
        """
        risk_mode = "NORMAL"
        try:
            mode_setting = await self.ledger.get_setting("global_risk_mode")
            if mode_setting:
                risk_mode = mode_setting.upper()
        except Exception:
            pass

        # Cash balance & open lots
        open_lots_count = len(self.position_book.get_open_lots()) if self.position_book else 0
        stats = await self.ledger.get_decision_stats(lookback_hours=24.0)

        # Loss attribution clusters
        clusters = await self.ledger.get_loss_attribution_clusters(lookback_hours=48.0, limit=3)

        # Tournament champions
        from alpha_engine.engine.experimentation import StrategyTournamentRegistry
        target = getattr(self, "execution_target", None)
        reg = getattr(target, "strategy_tournament", None) if target else None
        if reg is None:
            reg = StrategyTournamentRegistry(ledger=self.ledger)
        board = reg.get_leaderboard()
        champ = next((b for b in board if b["status"] == "CHAMPION"), None)
        champ_name = champ["name"] if champ else "champion_fast_snipe"

        lines = [
            "### 📊 التقرير التفصيلي المتكامل لحالة المحرك (Detailed Institutional Status)\n",
            f"- **وضع المخاطر العالمي (Global Risk Mode):** `🛡️ {risk_mode}`",
            f"- **بطل البطولة الحالي (Active Champion):** `{champ_name}`",
            f"- **المراكز المفتوحة (Open Positions):** `{open_lots_count}` صفقات قيد الإدارة",
            f"- **إجمالي القرارات (24h):** `{stats.get('total_decisions', 0)}` قرار",
            f"- **نسبة الفوز المحققة:** `{stats.get('win_rate_pct', 0.0):.1f}%`",
            f"- **معامل الربح (Profit Factor):** `{stats.get('profit_factor', 0.0):.2f}`\n",
            "#### 📉 تحليل أسباب الخسارة الجذري (Loss Root Cause Clusters - 48h):",
        ]
        clusters_map = {}
        if isinstance(clusters, dict):
            clusters_map = clusters.get("clusters", clusters)
        elif isinstance(clusters, list):
            for item in clusters:
                if isinstance(item, dict):
                    r_name = item.get("exit_reason", item.get("reason", "unknown"))
                    clusters_map[r_name] = item

        if clusters_map:
            for reason, c in clusters_map.items():
                if isinstance(c, dict):
                    cnt = c.get('count', c.get('cnt', 0))
                    tot = float(c.get('total_loss_usd', 0.0))
                    avg_l = float(c.get('avg_loss_usd', c.get('avg_loss', 0.0)))
                    lines.append(
                        f"- **`{reason}`**: {cnt} حالات | الخسارة الكلية: `${tot:,.2f}` | متوسط: `${avg_l:,.2f}`"
                    )
        else:
            lines.append("- لا توجد خسائر مسجلة في نافذة المراقبة الحالية.")

        lines.append("\n#### ⚙️ الأوامر المتاحة السريعة:")
        lines.append("• `/flow` — مخطط تدفق المراحل ونسب الاستبعاد")
        lines.append("• `/experiments` — لوحة صدارة تجارب الاستراتيجيات")
        lines.append("• `/trace <CA>` — تتبع عملة محددة عبر المحطات السبع")
        lines.append("• `/report` — التقرير الكمي المؤسسي (daily / weekly / csv / json)")

        return "\n".join(lines)

    async def tool_generate_report(self, report_type: str = "daily") -> str:
        """Generates quantitative performance digests or data exports."""
        from alpha_engine.execution.reports import QuantitativePerformanceReporter
        reporter = QuantitativePerformanceReporter(ledger=self.ledger)
        clean_type = report_type.strip().lower()

        if "weekly" in clean_type or "أسبوعي" in clean_type:
            return await reporter.generate_weekly_digest(lookback_days=7.0)
        elif "csv" in clean_type:
            csv_data = await reporter.export_report_csv(lookback_hours=24.0)
            return f"```csv\n{csv_data[:3500]}\n```"
        elif "json" in clean_type:
            json_data = await reporter.export_report_json(lookback_hours=24.0)
            return f"```json\n{json.dumps(json_data, indent=2, default=str)[:3500]}\n```"
        else:
            return await reporter.generate_daily_digest(lookback_hours=24.0)

    async def ask(self, query: str) -> str:
        """
        Process a natural language question or command using RAG over the
        audit ledger, position state, and staging memory.
        """
        q = query.strip()
        lower_q = q.lower()

        # Lifecycle trace
        if lower_q.startswith("/trace") or lower_q.startswith("trace ") or lower_q.startswith("تتبع"):
            target = re.sub(r"^(/trace|trace|تتبع)\s*", "", q, flags=re.IGNORECASE).strip()
            return await self.tool_trace_lifecycle(target)

        # Pipeline flow
        if lower_q in ("/flow", "flow", "تدفق", "مسار التدفق", "مخطط التدفق"):
            return await self.tool_pipeline_flow()

        # Detailed status
        if lower_q in ("/status_detailed", "/detailed_status", "status_detailed", "حالة مفصلة", "تقرير مفصل"):
            return await self.tool_status_detailed()

        if lower_q in ("/experiments", "experiments", "تجارب", "البطولة", "التجارب", "المنافسة"):
            return await self.tool_experiments()

        # Quantitative performance report
        if lower_q.startswith(("/report", "report", "تقرير")):
            parts = lower_q.split()
            rtype = parts[1] if len(parts) > 1 else "daily"
            return await self.tool_generate_report(rtype)

        if lower_q.startswith("/council") or lower_q.startswith("/debate") or lower_q.startswith("مناظرة") or lower_q.startswith("المجلس"):
            target = re.sub(r"^(/council|/debate|مناظرة|المجلس)\s*", "", q, flags=re.IGNORECASE).strip()
            return await self.tool_council_debate(target)

        # 1. Check for command overrides
        if q.startswith("/override") or lower_q.startswith("override ") or lower_q.startswith("تعديل ") or lower_q.startswith("تجاوز "):
            cmd = q.replace("/override", "").replace("override", "").replace("تعديل", "").replace("تجاوز", "").strip()
            return await self.tool_override_parameter(cmd)
            
        # 1b. Check for system doctor
        if lower_q in ("/doctor", "doctor", "فحص النظام", "فحص"):
            return await self.tool_system_doctor()
            
        # 1c. Check for deep research
        if lower_q.startswith("/research") or lower_q.startswith("research ") or lower_q.startswith("بحث"):
            target = re.sub(r"^(/research|research|بحث)\s*", "", q, flags=re.IGNORECASE).strip()
            if target:
                return await self.tool_deep_research(target)
            return "Please provide a target for research. Example: `/research $TOKEN`"

        # 1d. Check for swarm status
        if lower_q in ("/swarm", "swarm", "حالة السرب", "السرب", "swarm status", "/swarm status"):
            return await self.tool_swarm_status()

        # 2. Check for staging queries
        if (
            any(w in lower_q for w in ("staging", "staged", "watchlist", "/staging", "ستيج", "المرحلة", "قائمة الانتظار", "المراقبة"))
            or ("buffer" in lower_q and any(k in lower_q for k in ("current", "tokens", "wave", "list", "show", "what")))
            or ("مراقبة" in lower_q and any(k in lower_q for k in ("قائمة", "عرض", "ما هي", "العملات")))
        ):
            return await self.tool_list_staged_tokens()

        # 2b. Check for asset listing
        if (
            any(w in lower_q for w in ("/assets", "/tokens", "العملات المكتشفة", "العملات المعالجة", "أحدث العملات"))
            or lower_q in ("العملات", "عملات", "قائمة العملات", "عرض العملات")
        ):
            return await self.tool_list_assets()

        # 2c. Check for investigation
        if lower_q.startswith("/investigate") or lower_q.startswith("investigate ") or lower_q.startswith("تحقيق"):
            target = re.sub(r"^(/investigate|investigate|تحقيق)\s*", "", q, flags=re.IGNORECASE).strip()
            if target:
                return await self.tool_investigate_asset(target)
            return "الرجاء إدخال رقم العملة أو عنوان العقد. مثال: `/investigate 3`"

        # 2b. Check for revival breakout buffer
        if any(w in lower_q for w in ("revival", "cto", "swing", "/revival", "انبعاث", "سوانغ")):
            return await self.tool_list_revival_tokens()

        # 3. Check for strategy performance / best pattern
        if (
            any(w in lower_q for w in ("highest win rate", "best strategy", "best pattern", "strategy performance", "win rate", "/perf", "perf", "performance", "أعلى نسبة فوز", "أفضل استراتيجية", "أداء الاستراتيجية", "أداء", "الأداء", "نسبة الفوز", "أفضل نمط"))
        ):
            # Extract hours if specified (e.g. "past 12 hours" or "last 12 hours" or "خلال 12 ساعة")
            match = re.search(r"(\d+(\.\d+)?)\s*(?:hours?|ساعات?|ساعة)", lower_q)
            hrs = float(match.group(1)) if match else 12.0
            return await self.tool_strategy_performance(hours=hrs)

        # 4. Check for exit decision queries
        if (
            any(w in lower_q for w in ("why did you exit", "explain exit", "exit decision", "/exit", "لماذا خرجت", "سبب الخروج", "شرح الخروج", "قرار الخروج", "خروج"))
        ):
            # Extract token address or trade id (ASCII hex / base58 / lot id only)
            ca_match = re.search(r"\b([1-9A-HJ-NP-za-km-z]{32,44}|0x[a-fA-F0-9]{40}|[a-f0-9]{8})\b", q)
            if ca_match:
                return await self.tool_explain_exit(ca_match.group(1))
            arg_after = re.sub(r"^(/exit|exit|خروج)\s*", "", q, flags=re.IGNORECASE).strip()
            if arg_after:
                return await self.tool_explain_exit(arg_after)

        # 5. Check for "why did you skip / enter" queries with specific CA
        if any(w in lower_q for w in ("why did you skip", "why did you pass", "why entered", "why did you enter", "/why", "لماذا تجاوزت", "لماذا تخطيت", "لماذا اشتريت", "لماذا دخلت")):
            # First check full base58/hex addresses
            ca_match = re.search(r"\b([1-9A-HJ-NP-za-km-z]{32,44}|0x[a-fA-F0-9]{40})\b", q)
            if ca_match:
                token_ca = ca_match.group(1)
                return await self.tool_why_decision(token_ca)
            # Check after keyword skip/enter/pass/token for valid alphanumeric tokens
            kw_match = re.search(r"(?:skip(?:ped)?|enter(?:ed)?|pass(?:ed)?|token|on|تجاوز|تخطي|دخول|شراء)\s+([A-Za-z0-9_\-]{6,44})", q, re.IGNORECASE)
            if kw_match:
                cand = kw_match.group(1).strip()
                if cand.lower() not in ("token", "tokens", "at", "the", "on", "it", "this"):
                    return await self.tool_why_decision(cand)
            # Check for short alphanumeric token/lot identifiers (ASCII only, min 6 chars)
            words = [w.strip("?.,!\"'") for w in q.split()]
            potential_ca = [
                w for w in words
                if len(w) >= 6
                and re.match(r"^[A-Za-z0-9_\-]+$", w)
                and w.lower() not in ("why", "did", "you", "skip", "pass", "enter", "entered", "token", "tokens", "what", "which", "could")
            ]
            if potential_ca:
                return await self.tool_why_decision(potential_ca[-1])

        # 6. Conversational supervisor LLM endpoint (answer_user_query)
        if self.supervisor and hasattr(self.supervisor, "answer_user_query"):
            context = {}
            if self.position_book:
                try:
                    context["open_positions"] = [p.to_dict() if hasattr(p, "to_dict") else str(p) for p in self.position_book.get_all_positions()]
                except Exception:
                    pass
            if self.staging_buffer:
                try:
                    if hasattr(self.staging_buffer, "get_staged_tokens"):
                        context["staged_tokens_count"] = len(self.staging_buffer.get_staged_tokens())
                    elif hasattr(self.staging_buffer, "get_all_staged"):
                        context["staged_tokens_count"] = len(self.staging_buffer.get_all_staged())
                except Exception:
                    pass
            if self.parameter_tuner and hasattr(self.parameter_tuner, "current_params"):
                try:
                    context["risk_parameters"] = vars(self.parameter_tuner.current_params)
                except Exception:
                    pass
            try:
                ans = await self.supervisor.answer_user_query(query, context=context)
                if ans and ans.strip():
                    return ans
            except Exception as exc:
                logger.debug("Error in conversational supervisor ask: %s", exc)

        # 7. Fallback / General overview
        return (
            "🤖 **AlphaSupervisor-AI Conversational Copilot | مساعد التداول الذكي**\n\n"
            "I can answer quantitative questions about trading decisions, staging buffer, and strategy performance:\n"
            "يمكنني الإجابة عن القرارات الاستثمارية، الصفقات، وحالة السوق:\n"
            "- *'Why did you skip token [CA] at 14:00 but entered at 16:00?'* (لماذا تخطيت العقد [CA]؟)\n"
            "- *'Which strategy or pattern had the highest win rate in the past 12 hours?'* (ما هي أفضل استراتيجية خلال 12 ساعة؟)\n"
            "- *'List all tokens currently in the staging buffer and their accumulation scores.'* (عرض العملات قيد المراقبة والتجميع)\n"
            "- *'Explain the exit decision on trade [CA].'* (اشرح قرار الخروج من الصفقة)\n"
            "- *'/assets'* (عرض العملات المكتشفة مؤخراً للتحقيق)\n"
            "- *'/investigate <id|CA>'* (تحقيق شامل وتتبع دورة حياة العملة)\n"
            "- *'/swarm'* (عرض حالة سرب وكلاء رو فلو Ruflo Swarm والذاكرة العرضية)\n"
            "- *'/override risk <EXPAND|DEFENSIVE|NEUTRAL>'* (تعديل وضع المخاطرة)\n"
            "- *'/override blacklist <CA>'* (حظر عنوان عقد)\n"
            "- *'/override stage <CA>'* (إضافة عقد لمرحلة المراقبة)"
        )

    async def run_cli_loop(self) -> None:
        """Run an interactive CLI session with rich formatting."""
        if RICH_AVAILABLE and Console is not None:
            console = Console()
            console.print(
                Panel.fit(
                    "[bold cyan]AlphaSupervisor-AI Conversational Interface[/bold cyan]\n"
                    "[dim]Ask questions about decisions, staging tokens, strategy performance, or override risk params.[/dim]\n"
                    "[yellow]Type 'exit' or 'quit' to terminate session.[/yellow]",
                    border_style="cyan",
                )
            )
        else:
            print("=== AlphaSupervisor-AI Conversational Interface ===")
            print("Type 'exit' or 'quit' to leave.")

        loop = asyncio.get_running_loop()

        while True:
            try:
                user_input = await loop.run_in_executor(None, input, "\n🤖 AlphaCopilot > ")
                user_input = user_input.strip()
                if not user_input:
                    continue
                if user_input.lower() in ("exit", "quit", "q"):
                    print("Exiting conversational supervisor. Goodbye.")
                    break

                response = await self.ask(user_input)

                if RICH_AVAILABLE and Console is not None:
                    console.print(Markdown(response))
                else:
                    print("\n" + response + "\n")

            except (EOFError, KeyboardInterrupt):
                print("\nSession ended.")
                break
            except Exception as exc:
                logger.error("Error in conversational CLI loop: %s", exc)
                print(f"Error: {exc}")


ChatInterface = ConversationalSupervisor


async def main() -> None:
    """CLI Entrypoint for running the conversational explainer standalone."""
    config = EngineConfig()
    ledger = SQLiteLedger(db_path=config.db_path)
    async with ledger:
        supervisor = AlphaSupervisorAI(config)
        staging_buffer = Wave2StagingBuffer()
        explainer = ConversationalSupervisor(
            ledger=ledger,
            supervisor=supervisor,
            staging_buffer=staging_buffer,
            config=config,
        )
        await explainer.run_cli_loop()


if __name__ == "__main__":
    asyncio.run(main())
