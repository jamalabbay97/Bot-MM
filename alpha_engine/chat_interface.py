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
    ) -> None:
        self.ledger = ledger
        self.supervisor = supervisor or ai_supervisor
        self.staging_buffer = staging_buffer
        self.revival_buffer = revival_buffer
        self.position_book = position_book
        self.parameter_tuner = parameter_tuner
        self.metrics = metrics
        self.config = config or EngineConfig()

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
        if not records:
            return f"❌ No decision records found in ledger for token `{clean_addr}`."

        # Chronological order
        records = sorted(records, key=lambda r: r.timestamp_ns)

        lines = [f"### 📋 Audit Trail & Decision Analysis for `{clean_addr[:12]}...`\n"]
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
                    pnl_pct = float((lot.highest_price_observed - lot.entry_price) / lot.entry_price * 100) if lot.entry_price > 0 else 0.0
                    return (
                        f"### 🛑 Closed Position Lot [{lot.lot_id[:8]}] for `{lot.token_address[:10]}`\n"
                        f"- **Current Stage:** `{lot.current_stage.value}`\n"
                        f"- **Entry Price:** `{lot.entry_price}` | **Highest Peak:** `{lot.highest_price_observed}`\n"
                        f"- **Trailing Stop Floor:** `{lot.trailing_sl_price}`\n"
                        f"- **Peak Gain:** `+{pnl_pct:.1f}%`\n"
                        f"- **TP1 Executed:** `{lot.tp1_sold}` | **TP2 Executed:** `{lot.tp2_sold}`\n"
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

    # =========================================================================
    # Conversational RAG Router
    # =========================================================================

    async def ask(self, query: str) -> str:
        """
        Process a natural language question or command using RAG over the
        audit ledger, position state, and staging memory.
        """
        q = query.strip()
        lower_q = q.lower()

        # 1. Check for command overrides
        if q.startswith("/override") or lower_q.startswith("override ") or lower_q.startswith("تعديل ") or lower_q.startswith("تجاوز "):
            cmd = q.replace("/override", "").replace("override", "").replace("تعديل", "").replace("تجاوز", "").strip()
            return await self.tool_override_parameter(cmd)

        # 2. Check for staging queries
        if (
            any(w in lower_q for w in ("staging", "staged", "watchlist", "/staging", "ستيج", "المرحلة", "قائمة الانتظار", "المراقبة"))
            or ("buffer" in lower_q and any(k in lower_q for k in ("current", "tokens", "wave", "list", "show", "what")))
            or ("مراقبة" in lower_q and any(k in lower_q for k in ("قائمة", "عرض", "ما هي", "العملات")))
        ):
            return await self.tool_list_staged_tokens()

        # 2b. Check for revival breakout buffer
        if any(w in lower_q for w in ("revival", "cto", "swing", "/revival", "انبعاث", "سوانغ")):
            return await self.tool_list_revival_tokens()

        # 3. Check for strategy performance / best pattern
        if any(w in lower_q for w in ("highest win rate", "best strategy", "best pattern", "strategy performance", "win rate", "/perf", "أعلى نسبة فوز", "أفضل استراتيجية", "أداء الاستراتيجية", "نسبة الفوز", "أفضل نمط")):
            # Extract hours if specified (e.g. "past 12 hours" or "last 12 hours" or "خلال 12 ساعة")
            match = re.search(r"(\d+(\.\d+)?)\s*(?:hours?|ساعات?|ساعة)", lower_q)
            hrs = float(match.group(1)) if match else 12.0
            return await self.tool_strategy_performance(hours=hrs)

        # 4. Check for exit decision queries
        if any(w in lower_q for w in ("why did you exit", "explain exit", "exit decision", "/exit", "لماذا خرجت", "سبب الخروج", "شرح الخروج", "قرار الخروج")):
            # Extract token address or trade id (ASCII hex / base58 / lot id only)
            ca_match = re.search(r"\b([1-9A-HJ-NP-za-km-z]{32,44}|0x[a-fA-F0-9]{40}|[a-f0-9]{8})\b", q)
            if ca_match:
                return await self.tool_explain_exit(ca_match.group(1))

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
