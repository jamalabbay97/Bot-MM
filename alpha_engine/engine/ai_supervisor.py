"""
alpha_engine.engine.ai_supervisor — Institutional-Grade Autonomous Risk Engine
==============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | asyncio | aiohttp | Google Gemini / OpenAI / Deterministic Engine
"""

from __future__ import annotations

import asyncio
from collections import deque
from decimal import Decimal
import json
import logging
import time
from typing import Any, Optional

import aiohttp

from alpha_engine.config import EngineConfig
from alpha_engine.models.decisions import DecisionRecord, DecisionType, PatternFeatureVector
from alpha_engine.models.ai import (
    ActionParameters,
    AISupervisorDecisionEnum,
    AISupervisorResponse,
    AuditedTokenOutcome,
    FeedbackTuning,
    GlobalRiskMode,
    HoneypotRisk,
    LiquidityHealth,
    LossAttribution,
    OutcomeTrackerMetrics,
    SecurityAssessment,
    TakeProfitStage,
    WalletAudit,
    WalletRiskClassification,
)
from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.news import NewsEvent
from alpha_engine.models.profiler import WalletProfile
from alpha_engine.models.state import PoolState, SecurityReport

logger = logging.getLogger(__name__)

ALPHA_SUPERVISOR_SYSTEM_PROMPT = """You are "AlphaSupervisor-AI", an institutional-grade Quantitative Research Director and Autonomous Risk Engine supervising an algorithmic on-chain trading framework ("Bot-MM-main" / "alpha_engine"). Your role is to analyze multi-chain telemetry (Solana, EVM, Pump.fun, Raydium, Uniswap), audit wallet profiling metrics, evaluate social/news sentiment, identify hidden market manipulation, and output strictly deterministic risk and execution parameters.

Your objective is to eliminate structural blind spots that hardcoded algorithms miss, filter deceptive on-chain behaviors, and dynamically tune trade parameters for maximum risk-adjusted returns (Sharpe/Sortino) while strictly capping downside drawdown.

================================================================================
1. COMPREHENSIVE WALLET & ON-CHAIN PROFILING AUDIT (SMART MONEY DECEPTION FILTER)
================================================================================
When evaluating wallets flagged as "Smart Money" or profitable traders, analyze whether the wallet is genuinely skilled or a deceptive operator. You must evaluate:

A. Cabal & Insider Detection:
- Sybil Cluster Analysis: Check if the wallet received initial SOL/ETH from funding aggregators (Disperse.app, Cointool) or privacy protocols/mixers (Tornado, FixedFloat, Railgun) at the exact same block/timestamp as the deployer or top holders.
- First-Block Snipe Collusion: Did the wallet execute buys in block 0/1 within the same Jito bundle as the deployer? If yes, flag as INSIDER/DEV_POCKET, NOT smart money.
- Dump-Dispersal Behavior: Does the wallet systematically transfer acquired tokens to fresh secondary addresses before offloading to Dex pools? If detected, classify as stealth insider offloading.

B. MEV & Non-Replicable Mechanics:
- Sandwich & Backrun Bots: Does the wallet rely on 0-slot atomic execution (Jito MEV bundles, Flashbots private relays) to front-run and back-run human swaps? If so, flag as MEV_BOT (un-copyable; copying will result in severe slippage losses).
- Wash-Trading & Volume Manipulation: Does the wallet ping-pong transactions across related addresses to inflate volume metrics on DexScreener/Birdeye? Calculate net wallet profit minus gas; if near zero or negative despite high trading volume, flag as FAKE_VOLUME.

C. Legitimate Smart Money Criteria:
- Proven sample size: Minimum 40+ completed round-trip trades across different token narratives over >14 days.
- Independent entries: Buys occur at established consolidation ranges or on genuine narrative break-outs, not exclusively at block 0.
- Consistent Sortino Ratio (>1.8) and low maximum drawdown (<25% of account peak).
- Positive Net Realized PnL across multiple market regimes, not a single lucky 100x trade masking 50 consecutive wipeouts.

================================================================================
2. ON-CHAIN SECURITY, LIQUIDITY DEPTH & PRE-FLIGHT VERIFICATION
================================================================================
Before validating any trade signal, conduct deep structural vulnerability screening:

A. Contract & Authority Integrity:
- Solana (SPL): Mint Authority must be Revoked (None), Freeze Authority must be Revoked (None). If either is active, REJECT IMMEDIATELY.
- EVM: Verify contract bytecode against known Honeypots, blacklisting logic, balance-check modifiers, and dynamic fee exploits (transfer tax > 2% is REJECT).
- Liquidity Pool State: LP tokens must be 100% burned or locked via verifiable vesting escrow (e.g., Streamflow, UNCX) for a minimum of 6 months.

B. Liquidity Dynamics & Slippage Vulnerability:
- Constant Product (CPMM) Impact: Trade size must not exceed 0.5% - 1.0% of total pool reserve ($x \\cdot y = k$) to prevent crippling price impact.
- Bonding Curve State (e.g., Pump.fun): Track curve migration progress. Avoid buying within 90-99% progress if liquidity migration to Raydium exposes positions to migration-snipers and liquidity voids.
- Top-10 Concentration: Excluding burn addresses and DEX pools, top 10 holders must not own >18% combined. Single non-pool wallet holding >4% is an immediate HIGH RISK flag.

================================================================================
3. SENTIMENT VELOCITY, NEWS & NARRATIVE NOVELTY DECODER
================================================================================
When parsing Telegram channels, X/Twitter firehoses, and social feeds:

A. Bot Farm & Paid Raid Detection:
- Velocity Discrepancy: Spikes in mentions without corresponding on-chain unique buyer growth indicates paid X raid or Telegram bot-boosting.
- Template Invariance: Identical text syntax, coordinated hashtag distribution, or newly created accounts (<30 days old) pumping contract addresses must be scored as ORGANIC_SCORE: LOW (Reject).

B. Genuine Narrative Breakthroughs:
- Real-world catalyst integration: Authentic external news, Tier-1 cultural memes, or spontaneous adoption by high-follower independent accounts without financial disclosure tags.
- First-Order Derivative vs. Copycat Dilution: Distinguish original token meta from diluted knock-offs launched minutes later. Always prioritize the genesis contract with organic mindshare.

================================================================================
4. ADAPTIVE EXECUTION, POSITION SIZING & EXIT ARCHITECTURE
================================================================================
Dynamically calculate parameters based on current volatility and pool health:

A. Position Sizing:
- Utilize fractional Kelly Criterion: Size = (Win_Probability / Loss_Ratio) - ((1 - Win_Probability) / Win_Ratio), clamped between 0.25% and 2.0% of liquid deployable capital. NEVER authorize all-in or fixed-size bets during high-volatility regimes.

B. Dynamic Exit Matrix:
- Multi-Stage Take-Profit (TP):
  * TP1: Scale out 40% at +80% to +100% ROI (de-risks principal capital).
  * TP2: Scale out 30% at +250% ROI.
  * TP3 / Moonbag: Trail remaining 30% using dynamic ATR/trailing stop.
- Trailing Stop-Loss (SL):
  * Initial hard stop: -12% to -18% (adjusted to token volatility).
  * Breakeven Trigger: Once token achieves +40% gain, automatically shift stop-loss to entry price + estimated round-trip gas costs.
  * Time-Based Inactivity Stop: If token volume stalls for >15 minutes post-entry and price fails to achieve +15%, force exit to preserve opportunity cost.

C. Network Gas & Congestion Mitigation:
- Dynamically calibrate Jito tips (Solana) or Priority Gas Fees (EVM) to ensure transaction inclusion within the target slot without overpaying beyond expected transaction alpha.

================================================================================
5. CLOSED-LOOP FEEDBACK & HYPERPARAMETER AUTO-TUNING
================================================================================
When analyzing trade performance logs and execution ledgers:
- Calculate Slippage Leakage: Difference between quoted price at signal generation and actual fill price on-chain. If leakage exceeds 3%, recommend increasing node speed or reducing order size.
- Post-Mortem Win/Loss Attribution: Tag every loss with root causes (e.g., LATE_ENTRY, RUG_PULL, MEV_SANDWICH, VOLUME_DRYOUT, CABAL_DUMP).
- Continuous Adaptation: Update the token blacklist, whitelist top-performing wallets, and decrease exposure to degrading strategies.

================================================================================
OUTPUT SCHEMA INSTRUCTION (STRICT JSON ONLY)
================================================================================
You must respond ONLY with a single valid, well-formed JSON object. Do not include markdown code block tags outside the JSON, do not include preliminary explanations, and do not add closing commentary. Use this exact schema:

{
  "decision": "EXECUTE_BUY" | "EXECUTE_SELL" | "PASS" | "UPDATE_RISK_PARAMS",
  "confidence_score": 0.00 to 1.00,
  "action_parameters": {
    "target_token_address": "string",
    "recommended_position_pct": 0.00,
    "max_slippage_bps": 50 to 500,
    "priority_fee_multiplier": 1.0 to 3.0,
    "take_profit_ladder": [
      {"trigger_multiplier": 2.0, "sell_pct": 40},
      {"trigger_multiplier": 3.5, "sell_pct": 30}
    ],
    "hard_stop_loss_pct": -15.0,
    "trailing_stop_activation_pct": 40.0,
    "time_exit_minutes": 15
  },
  "wallet_audit": {
    "is_wallet_reputable": true | false,
    "risk_classification": "ORGANIC_SMART_MONEY" | "CABAL_INSIDER" | "MEV_BOT" | "WASH_TRADER" | "NOVICE_LUCKY",
    "rationale": "Concise summary of on-chain verification"
  },
  "security_assessment": {
    "is_secure": true | false,
    "honeypot_risk": "NONE" | "SUSPICIOUS" | "CONFIRMED",
    "liquidity_health": "OPTIMAL" | "THIN" | "UNLOCKED",
    "flags": ["list", "of", "detected", "anomalies"]
  },
  "feedback_tuning": {
    "adjust_global_risk": "EXPAND" | "NEUTRAL" | "DEFENSIVE",
    "blacklisted_entities": ["address_or_cabal_id"],
    "insights_learned": "Key strategic takeaway to persist into local state"
  }
}
"""


class AIOutcomeTracker:
    """
    Closed-loop outcome observer and self-learning calibration engine.
    Tracks price progression of audited tokens (both approved and vetoed)
    to calculate True Positives, False Positives, True Negatives, False Negatives,
    and dynamically tune risk posture and confidence calibration.
    """

    def __init__(self, max_history: int = 500) -> None:
        self._outcomes: dict[str, AuditedTokenOutcome] = {}
        self._history: deque[AuditedTokenOutcome] = deque(maxlen=max_history)
        self._tp: int = 0
        self._fp: int = 0
        self._tn: int = 0
        self._fn: int = 0

    def record_audit(
        self,
        token_address: str,
        chain: str,
        decision: str,
        initial_price: Decimal,
        flags: list[str],
        confidence: float,
    ) -> AuditedTokenOutcome:
        init_p = initial_price if initial_price > Decimal(0) else Decimal("0.000000028")
        outcome = AuditedTokenOutcome(
            token_address=token_address,
            chain=chain,
            decision=decision,
            initial_price=init_p,
            audit_timestamp=time.time(),
            flags=list(flags),
            confidence=confidence,
            peak_price=init_p,
            min_price=init_p,
            latest_price=init_p,
            peak_multiplier=1.0,
            max_drawdown_pct=0.0,
            is_finalized=False,
            outcome_label=None,
        )
        self._outcomes[token_address] = outcome
        self._history.append(outcome)
        return outcome

    def record_price_update(
        self,
        token_address: str,
        current_price: Decimal,
        timestamp: float | None = None,
    ) -> AuditedTokenOutcome | None:
        outcome = self._outcomes.get(token_address)
        if outcome is None:
            return None

        now = timestamp if timestamp is not None else time.time()
        if current_price > outcome.peak_price:
            outcome.peak_price = current_price
        if outcome.min_price <= Decimal(0) or (current_price > Decimal(0) and current_price < outcome.min_price):
            outcome.min_price = current_price
        outcome.latest_price = current_price

        if outcome.initial_price > Decimal(0):
            outcome.peak_multiplier = float(outcome.peak_price / outcome.initial_price)
            outcome.max_drawdown_pct = float(
                max(Decimal(0), (outcome.initial_price - outcome.min_price) / outcome.initial_price * 100)
            )

        elapsed = now - outcome.audit_timestamp
        if not outcome.is_finalized and (elapsed >= 45.0 or outcome.peak_multiplier >= 1.40 or outcome.max_drawdown_pct >= 25.0):
            if outcome.decision == AISupervisorDecisionEnum.EXECUTE_BUY.value:
                if outcome.peak_multiplier >= 1.35 and outcome.max_drawdown_pct < 25.0:
                    outcome.outcome_label = "TRUE_POSITIVE"
                    self._tp += 1
                else:
                    outcome.outcome_label = "FALSE_POSITIVE"
                    self._fp += 1
            else:  # PASS / VETO
                if outcome.peak_multiplier >= 1.80:
                    outcome.outcome_label = "FALSE_NEGATIVE"
                    self._fn += 1
                else:
                    outcome.outcome_label = "TRUE_NEGATIVE"
                    self._tn += 1
            outcome.is_finalized = True

        return outcome

    def get_metrics(self) -> OutcomeTrackerMetrics:
        total = self._tp + self._fp + self._tn + self._fn
        accuracy = ((self._tp + self._tn) / total * 100.0) if total > 0 else 100.0
        approved = self._tp + self._fp
        win_rate = (self._tp / approved * 100.0) if approved > 0 else 0.0
        vetoed = self._tn + self._fn
        veto_eff = (self._tn / vetoed * 100.0) if vetoed > 0 else 100.0
        fn_rate = (self._fn / vetoed * 100.0) if vetoed > 0 else 0.0
        fp_rate = (self._fp / approved * 100.0) if approved > 0 else 0.0

        if fn_rate > 20.0 and total >= 5:
            calib = "CALIBRATING_EXPAND"
        elif fp_rate > 30.0 and total >= 5:
            calib = "CALIBRATING_DEFENSIVE"
        else:
            calib = "BALANCED"

        return OutcomeTrackerMetrics(
            total_tracked=len(self._outcomes),
            finalized_count=total,
            true_positives=self._tp,
            false_positives=self._fp,
            true_negatives=self._tn,
            false_negatives=self._fn,
            accuracy_pct=round(accuracy, 2),
            win_rate_pct=round(win_rate, 2),
            veto_efficiency_pct=round(veto_eff, 2),
            false_negative_rate_pct=round(fn_rate, 2),
            false_positive_rate_pct=round(fp_rate, 2),
            calibration_status=calib,
        )

    def reset(self) -> None:
        self._tp = 0
        self._fp = 0
        self._tn = 0
        self._fn = 0
        for o in self._outcomes.values():
            o.is_finalized = False
            o.outcome_label = None


class AlphaSupervisorAI:
    """
    AlphaSupervisor-AI Engine:
    Dual-mode autonomous risk director:
    1. LLM-powered institutional telemetry analysis via Google Gemini API / OpenAI API.
    2. Zero-latency deterministic heuristics fallback strictly executing all 5 sections.
    """

    def __init__(
        self,
        config: EngineConfig,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self._cfg = config
        self._session = session
        self._owns_session = False

        # Provider and credentials
        self._enabled: bool = getattr(config, "ai_supervisor_enabled", True)
        self._provider: str = getattr(config, "ai_provider", "gemini").lower()
        self._api_key: Optional[str] = (
            getattr(config, "gemini_api_key", None)
            or getattr(config, "ai_api_key", None)
            or getattr(config, "openai_api_key", None)
        )
        model_val = getattr(config, "ai_model", "gemini-2.5-flash")
        if model_val in ("gemini", "default", ""):
            model_val = "gemini-2.5-flash"
        self._model: str = model_val
        self._timeout_s: float = getattr(config, "ai_timeout_s", 4.0)
        self._strict_veto: bool = getattr(config, "ai_strict_veto", True)

        # Dynamic state & memory
        self._blacklisted_entities: set[str] = set()
        self._global_risk_mode: GlobalRiskMode = GlobalRiskMode.NEUTRAL
        self._recent_audits: deque[dict[str, Any]] = deque(maxlen=50)
        self._recent_vetoes: deque[dict[str, Any]] = deque(maxlen=50)
        self._learned_insights: deque[str] = deque(maxlen=50)
        self._total_audits: int = 0
        self._veto_count: int = 0
        self._buy_approval_count: int = 0
        self._outcome_tracker = AIOutcomeTracker(max_history=500)

        # Extended persistence, pattern memory & dynamic tuning
        self._ledger: Optional[Any] = None
        self._pattern_store: Optional[Any] = None
        self._parameter_tuner: Optional[Any] = None

    def set_ledger(self, ledger: Any) -> None:
        """Register the SQLite ledger for structured decision audit trail persistence."""
        self._ledger = ledger

    def set_pattern_store(self, pattern_store: Any) -> None:
        """Register vectorized pattern memory store."""
        self._pattern_store = pattern_store

    def set_parameter_tuner(self, parameter_tuner: Any) -> None:
        """Register dynamic hyperparameter tuner."""
        self._parameter_tuner = parameter_tuner

    @property
    def outcome_tracker(self) -> AIOutcomeTracker:
        return self._outcome_tracker

    def record_price_update(
        self,
        token_address: str,
        current_price: Decimal,
        timestamp: float | None = None,
    ) -> AuditedTokenOutcome | None:
        """Feed on-chain price telemetry into self-learning outcome tracker."""
        outcome = self._outcome_tracker.record_price_update(token_address, current_price, timestamp)
        metrics = self._outcome_tracker.get_metrics()
        if metrics.calibration_status == "CALIBRATING_EXPAND" and self._global_risk_mode == GlobalRiskMode.DEFENSIVE:
            self._global_risk_mode = GlobalRiskMode.NEUTRAL
            self._learned_insights.append("Self-learning loop: Auto-relaxed risk stance to NEUTRAL (false-negative rate > 20%).")
        elif metrics.calibration_status == "CALIBRATING_DEFENSIVE" and self._global_risk_mode == GlobalRiskMode.EXPAND:
            self._global_risk_mode = GlobalRiskMode.NEUTRAL
            self._learned_insights.append("Self-learning loop: Auto-tightened risk stance to NEUTRAL (false-positive rate > 30%).")
        return outcome

    def set_global_risk_mode(self, mode: GlobalRiskMode | str) -> GlobalRiskMode:
        """Set or override the global risk stance."""
        if isinstance(mode, str):
            clean = mode.strip().upper()
            self._global_risk_mode = GlobalRiskMode(clean)
        elif isinstance(mode, GlobalRiskMode):
            self._global_risk_mode = mode
        return self._global_risk_mode

    def set_strict_veto(self, strict: bool) -> bool:
        """Toggle strict veto mode."""
        self._strict_veto = strict
        return self._strict_veto

    def get_recent_vetoes(self, limit: int = 10) -> list[dict[str, Any]]:
        """Return recently vetoed tokens with their specific rejection rationale."""
        return list(self._recent_vetoes)[-limit:]

    def reset_learning_metrics(self) -> None:
        """Reset outcome tracking counters and restore neutral calibration."""
        self._outcome_tracker.reset()
        self._global_risk_mode = GlobalRiskMode.NEUTRAL
        self._learned_insights.append("Self-learning metrics reset to baseline; stance restored to NEUTRAL.")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._timeout_s))
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    def get_status(self) -> dict[str, Any]:
        """Return real-time telemetry and supervisory state."""
        metrics = self._outcome_tracker.get_metrics()
        return {
            "enabled": self._enabled,
            "provider": self._provider,
            "model": self._model,
            "has_api_key": bool(self._api_key),
            "global_risk_mode": self._global_risk_mode.value,
            "strict_veto": self._strict_veto,
            "total_audits": self._total_audits,
            "approved_buys": self._buy_approval_count,
            "vetoed_signals": self._veto_count,
            "blacklisted_entities_count": len(self._blacklisted_entities),
            "recent_insights": list(self._learned_insights)[-5:],
            "outcome_tracker": metrics.model_dump(),
        }

    async def answer_user_query(self, query: str, context: Optional[dict[str, Any]] = None) -> str:
        """
        Conversational endpoint for user queries, explanations, and risk guidance.
        Queries the LLM (Gemini or OpenAI/OpenRouter) with strict Arabic localization
        for user-facing prose, while preserving English metrics, code, tokens, and tables.
        Falls back gracefully to intelligent deterministic Arabic response if offline.
        """
        clean_q = query.strip()
        if not clean_q:
            return "مرحباً! أنا المشرف الذكي **AlphaSupervisor-AI**، كيف يمكنني مساعدتك اليوم في تداولاتك؟"

        # Build comprehensive state context
        sys_status = self.get_status()
        merged_context: dict[str, Any] = dict(sys_status)
        if context:
            merged_context.update(context)

        system_prompt = (
            "You are AlphaSupervisor-AI, an expert Quantitative Trading Supervisor and Risk Copilot for Bot-MM.\n"
            "You monitor and supervise multi-chain paper trading across Solana (SVM) and Base (EVM), evaluating "
            "Wave-2 accumulation breakouts, liquidity depth, honey-pot risks, developer exits, top-10 concentration, "
            "and dynamic fractional Kelly position sizing.\n\n"
            "MANDATORY LOCALIZATION RULES:\n"
            "1. Write all conversational explanations, analysis, summaries, guidance, and greetings in clear, professional Arabic (العربية).\n"
            "2. Keep all token symbols (e.g. SOL, ETH, BTC), contract addresses (CAs), trade execution alerts, tables, formulas, "
            "numbers, metrics (e.g. PnL, Win Rate, Uptime, Kelly, Slippage, RSS MB), and code blocks STRICTLY in English.\n"
            "3. Be concise, precise, and quantitatively rigorous."
        )

        user_content = (
            f"Current System State & Telemetry:\n{json.dumps(merged_context, indent=2, default=str)}\n\n"
            f"User Question: {clean_q}"
        )

        # Attempt LLM call if configured
        if self._api_key:
            try:
                session = await self._get_session()
                if self._provider == "gemini":
                    url = f"https://generativelanguage.googleapis.com/v1beta/models/{self._model}:generateContent?key={self._api_key}"
                    body = {
                        "contents": [
                            {"role": "user", "parts": [{"text": user_content}]}
                        ],
                        "systemInstruction": {
                            "parts": [{"text": system_prompt}]
                        },
                        "generationConfig": {
                            "temperature": 0.4,
                            "maxOutputTokens": 1024,
                        },
                    }
                    async with session.post(url, json=body, timeout=self._timeout_s) as response:
                        if response.status == 200:
                            data = await response.json()
                            candidates = data.get("candidates", [])
                            if candidates:
                                text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                                if text.strip():
                                    return text.strip()
                        else:
                            resp_text = await response.text()
                            logger.warning("Gemini LLM call in answer_user_query returned status %s: %s", response.status, resp_text[:150])

                elif self._provider in ("openai", "openrouter"):
                    base_url = "https://api.openai.com/v1" if self._provider == "openai" else "https://openrouter.ai/api/v1"
                    url = f"{base_url}/chat/completions"
                    headers = {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}
                    body = {
                        "model": self._model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_content},
                        ],
                        "temperature": 0.4,
                        "max_tokens": 1024,
                    }
                    async with session.post(url, json=body, headers=headers, timeout=self._timeout_s) as response:
                        if response.status == 200:
                            data = await response.json()
                            text = data["choices"][0]["message"]["content"]
                            if text.strip():
                                return text.strip()
                        else:
                            resp_text = await response.text()
                            logger.warning("%s call in answer_user_query returned status %s: %s", self._provider, response.status, resp_text[:150])
            except Exception as exc:
                logger.debug("LLM call failed in answer_user_query: %s. Using heuristic fallback.", exc)

        # Deterministic localized fallback
        return self._deterministic_arabic_response(clean_q, merged_context)

    def _deterministic_arabic_response(self, query: str, context: dict[str, Any]) -> str:
        """
        Deterministic, mathematically grounded Arabic response when LLM is unavailable or offline.
        Keeps all English symbols, tables, metrics, tokens, and code blocks strictly in English.
        """
        lower_q = query.lower()
        mode = context.get("global_risk_mode", self._global_risk_mode.value)
        total_audits = context.get("total_audits", self._total_audits)
        approved = context.get("approved_buys", self._buy_approval_count)
        vetoed = context.get("vetoed_signals", self._veto_count)
        blacklisted = context.get("blacklisted_entities_count", len(self._blacklisted_entities))

        ot = context.get("outcome_tracker", {})
        win_rate = ot.get("win_rate_pct", 0.0) if isinstance(ot, dict) else 0.0

        if any(w in lower_q for w in ("status", "health", "state", "حالة", "وضع", "شغال", "نظام")):
            return (
                "📊 **تقرير حالة المشرف الذكي (AlphaSupervisor-AI Telemetry)**\n\n"
                f"• **وضع المخاطرة (Global Risk Mode):** `{mode}`\n"
                f"• **معدل النجاح (Win Rate):** `{win_rate:.1f}%`\n"
                f"• **إجمالي الفحوصات (Total Audits):** `{total_audits}` (مقبول: `{approved}` | مرفوض: `{vetoed}`)\n"
                f"• **الكيانات المحظورة (Blacklisted Entities):** `{blacklisted}`\n"
                f"• **النمط الصارم (Strict Veto):** `{'ON 🔒' if self._strict_veto else 'OFF 🔓'}`\n\n"
                "⚡ محرك المراقبة والتحليل الكمي يعمل بكفاءة تامة."
            )

        if any(w in lower_q for w in ("risk", "mode", "stance", "مخاطر", "مخاطرة", "تحفظ", "توسع")):
            return (
                f"🛡️ **وضع المخاطرة الحالي للمحرك هو `{mode}`**\n\n"
                "• `DEFENSIVE`: حظر أي عقد غير مؤكد، تقليل حجم لوتات كيلي بنسبة 50%، ووقف خسارة مشدد.\n"
                "• `NEUTRAL`: موازنة المخاطرة الكلاسيكية مع فحص شامل للسيولة والتمركز.\n"
                "• `EXPAND`: زيادة التحجيم للصفقات ذات الزخم العالي وطفرات الحجم Wave-2 المؤكدة.\n\n"
                "لتغيير الوضع يدوياً: `/ai stance <DEFENSIVE|NEUTRAL|EXPAND>`"
            )

        if any(w in lower_q for w in ("veto", "reject", "رفض", "فيتو", "حظر")):
            recent = self.get_recent_vetoes(limit=5)
            if not recent:
                return "🛡️ لا توجد عمليات رفض حديثة مسجلة في الذاكرة. يتم الرفض تلقائياً عند وجود مخاطر Honeypot أو تمركز حيتان > 25%."
            lines = ["🛡️ **أحدث عمليات الرفض (Recent Vetoes)**\n", "```", f"{'Token':<14} | {'Chain':<6} | {'Flags & Rationale'}", "-" * 55]
            for v in reversed(recent):
                t_str = str(v.get("token", ""))
                short_t = f"{t_str[:4]}..{t_str[-4:]}" if len(t_str) > 10 else t_str
                c_str = str(v.get("chain", "")).replace("ChainIdentifier.", "").replace("_mainnet", "")[:6]
                flags = v.get("flags", [])
                flag_str = ", ".join(flags[:2]) if flags else str(v.get("rationale", "Veto"))[:24]
                lines.append(f"{short_t:<14} | {c_str:<6} | {flag_str}")
            lines.append("```")
            return "\n".join(lines)

        if any(w in lower_q for w in ("wave2", "staging", "breakout", "تجميع", "موجة", "ستيج")):
            return (
                "🌊 **معايير انفجار الموجة الثانية (Wave-2 Accumulation Breakout)**:\n\n"
                "1. **Selling Exhaustion**: هبوط ضغط البيع تدريجياً خلال فترة التجميع.\n"
                "2. **Volume Surge Multiplier**: حجم آخر 5 دقائق $V_{5m} > 2.5 \\times SMA_{15m}$.\n"
                "3. **Net Buy Delta**: تفوق أوامر الشراء الصافية بنسبة تتجاوز `65%`.\n"
                "4. **Dev-Exit / CTO**: رصيد المطور $\\le 0.05\\%$ مع عدم وجود تكتلات حيتان (Top 10 $\\le 25\\%$)."
            )

        return (
            "🤖 **AlphaSupervisor-AI — المشرف الذكي والمساعد الكمي**\n\n"
            "أنا جاهز للإجابة على استفساراتك حول القرارات التحليلية وحالة المحرك وإدارة المخاطر:\n"
            "• لمعرفة أسباب فحص أو رفض أي عملة: أرسل عنوان العقد (CA) أو استفسر عن `/why <CA>`\n"
            "• لفحص قائمة المراقبة التجميعية: `/staging`\n"
            "• لعرض الصفقات المفتوحة والمغلقة: `/trades`\n"
            "• لتعديل شهية المخاطرة: `/ai stance <DEFENSIVE|NEUTRAL|EXPAND>`"
        )

    async def audit_trade_candidate(
        self,
        signal: Any,
        pool_state: Optional[PoolState] = None,
        security_report: Optional[SecurityReport] = None,
        news_event: Optional[NewsEvent] = None,
        wallet_profile: Optional[WalletProfile] = None,
        recent_win_rate: float = 65.0,
        recent_profit_factor: float = 2.4,
        sec: Optional[Any] = None,
    ) -> AISupervisorResponse:
        """
        Audit a trade candidate before execution.
        Safely accepts `sec` parameter and guards against AttributeError if `sec` is None.
        """
        effective_sec = sec if sec is not None else security_report
        return await self.audit_signal(
            signal=signal,
            pool_state=pool_state,
            security_report=effective_sec,
            news_event=news_event,
            wallet_profile=wallet_profile,
            recent_win_rate=recent_win_rate,
            recent_profit_factor=recent_profit_factor,
            sec=effective_sec,
        )

    async def audit_signal(
        self,
        signal: SignalEvent,
        pool_state: Optional[PoolState] = None,
        security_report: Optional[SecurityReport] = None,
        news_event: Optional[NewsEvent] = None,
        wallet_profile: Optional[WalletProfile] = None,
        recent_win_rate: float = 65.0,
        recent_profit_factor: float = 2.4,
        sec: Optional[Any] = None,
    ) -> AISupervisorResponse:
        """
        Comprehensive pre-flight signal audit.
        Evaluates wallet profiling, contract security, liquidity depth, sentiment,
        and produces adaptive Kelly sizing and dynamic exit matrices.
        """
        self._total_audits += 1
        target_token = signal.token_address
        chain = signal.chain

        # Build comprehensive telemetry dictionary
        telemetry = self._build_telemetry_payload(
            signal=signal,
            pool_state=pool_state,
            security_report=security_report,
            news_event=news_event,
            wallet_profile=wallet_profile,
            recent_win_rate=recent_win_rate,
            recent_profit_factor=recent_profit_factor,
        )

        resp: Optional[AISupervisorResponse] = None

        # 1. Check if token or initiator is already blacklisted
        initiator = telemetry.get("initiator_wallet", "")
        if target_token in self._blacklisted_entities or (initiator and initiator in self._blacklisted_entities):
            logger.warning("AlphaSupervisor-AI veto: entity %s is blacklisted", target_token[:10])
            resp = self._create_blacklist_veto_response(target_token, initiator)

        # 2. Try LLM inference if credentials exist and enabled
        if resp is None and self._enabled and self._api_key:
            try:
                resp = await asyncio.wait_for(self._call_llm(telemetry), timeout=self._timeout_s)
            except Exception as exc:  # noqa: BLE001
                logger.debug("AlphaSupervisor-AI LLM call failed or timed out (%s); falling back to deterministic engine", exc)

        # 3. Fallback to deterministic institutional heuristics engine
        if resp is None:
            resp = self.evaluate_deterministic(telemetry)

        # Record telemetry and update closed-loop state
        if resp.decision == AISupervisorDecisionEnum.PASS:
            self._veto_count += 1
            self._recent_vetoes.append({
                "token": target_token,
                "chain": chain.value,
                "flags": list(resp.security_assessment.flags) if resp.security_assessment else [],
                "rationale": resp.wallet_audit.rationale if resp.wallet_audit else "Security / Heuristic Veto",
                "timestamp": time.time(),
            })
        elif resp.decision == AISupervisorDecisionEnum.EXECUTE_BUY:
            self._buy_approval_count += 1

        init_price = Decimal("0.000000028")
        if pool_state and hasattr(pool_state, "spot_price_native_per_token") and pool_state.spot_price_native_per_token > 0:
            init_price = pool_state.spot_price_native_per_token
        elif getattr(signal, "spot_price", None) and signal.spot_price > 0:
            init_price = Decimal(str(signal.spot_price))

        decision_val = resp.decision.value if hasattr(resp.decision, "value") else str(resp.decision)
        flags = list(resp.security_assessment.flags) if resp.security_assessment else []
        self._outcome_tracker.record_audit(
            token_address=target_token,
            chain=chain.value,
            decision=decision_val,
            initial_price=init_price,
            flags=flags,
            confidence=resp.confidence_score,
        )

        if resp.feedback_tuning.blacklisted_entities:
            for entity in resp.feedback_tuning.blacklisted_entities:
                if entity and entity not in self._blacklisted_entities:
                    self._blacklisted_entities.add(entity)
                    logger.info("AlphaSupervisor-AI blacklisted toxic entity: %s", entity)

        if resp.feedback_tuning.adjust_global_risk:
            try:
                self._global_risk_mode = GlobalRiskMode(resp.feedback_tuning.adjust_global_risk)
            except Exception:
                pass

        if resp.feedback_tuning.insights_learned:
            self._learned_insights.append(resp.feedback_tuning.insights_learned)

        # Compute PatternMatchScore using PatternMemoryStore if available
        pattern_match_score = 0.85
        effective_sec = sec if sec is not None else (security_report or getattr(signal, "security_report", None))
        if self._pattern_store is not None and hasattr(self._pattern_store, "calculate_pattern_match"):
            try:
                candidate_vector = PatternFeatureVector.from_signal(
                    signal=signal,
                    sec=effective_sec,
                    wallet_profile=wallet_profile,
                )
                pattern_match_score = self._pattern_store.calculate_pattern_match(candidate_vector)
            except AttributeError:
                try:
                    top10 = getattr(effective_sec, "top10_concentration", 0.15) if effective_sec else 0.15
                    candidate_vector = PatternFeatureVector(
                        token_address=target_token,
                        consolidation_duration_s=1800.0,
                        dip_depth_pct=40.0,
                        volume_surge_multiplier=2.5,
                        net_buy_delta=0.70,
                        top10_concentration=float(top10 if top10 is not None else 0.15),
                        liquidity_to_mc_ratio=0.20,
                        smart_wallet_inflows=5.0,
                        peak_gain_multiplier=1.0,
                    )
                    pattern_match_score = self._pattern_store.calculate_pattern_match(candidate_vector)
                except Exception:
                    pattern_match_score = 0.85
            except Exception as exc:
                logger.debug("Failed computing pattern match score: %s", exc)
                pattern_match_score = 0.85

        # Apply dynamic parameter tuning if available
        if self._parameter_tuner is not None and hasattr(self._parameter_tuner, "current_params"):
            dyn_p = self._parameter_tuner.current_params
            try:
                new_action_params = resp.action_parameters.model_copy(
                    update={
                        "max_slippage_bps": dyn_p.max_slippage_bps,
                        "hard_stop_loss_pct": dyn_p.hard_stop_loss_pct,
                        "take_profit_ladder": dyn_p.get_take_profit_ladder(),
                        "trailing_stop_activation_pct": dyn_p.trailing_stop_activation_pct,
                    }
                )
                resp = resp.model_copy(update={"action_parameters": new_action_params})
            except Exception:
                try:
                    resp.action_parameters.max_slippage_bps = dyn_p.max_slippage_bps
                    resp.action_parameters.hard_stop_loss_pct = dyn_p.hard_stop_loss_pct
                    resp.action_parameters.take_profit_ladder = dyn_p.get_take_profit_ladder()
                    resp.action_parameters.trailing_stop_activation_pct = dyn_p.trailing_stop_activation_pct
                except Exception as ex_tuner:
                    logger.debug("Failed applying dynamic tuner parameters: %s", ex_tuner)

        # Construct and persist immutable DecisionRecord audit trail
        decision_type = (
            DecisionType.ENTER
            if resp.decision == AISupervisorDecisionEnum.EXECUTE_BUY
            else DecisionType.SKIP
        )
        if decision_type == DecisionType.ENTER:
            explicit_reason = (
                f"BUY APPROVED: AlphaScore={signal.alpha_score:.2f} | PatternMatch={pattern_match_score:.2f} | "
                f"KellySize={resp.action_parameters.recommended_position_pct}% | MaxSlippage={resp.action_parameters.max_slippage_bps}bps | "
                f"StopLoss={resp.action_parameters.hard_stop_loss_pct}% | {resp.wallet_audit.rationale}"
            )
        else:
            reasons = list(resp.security_assessment.flags) if resp.security_assessment else []
            explicit_reason = f"VETO SKIP: [{', '.join(reasons) if reasons else 'Unmet criteria'}]; {resp.wallet_audit.rationale}"

        decision_record = DecisionRecord(
            token_address=target_token,
            chain=chain.value,
            decision_type=decision_type,
            strategy_pattern=signal.source.value if hasattr(signal.source, "value") else str(signal.source),
            market_cap_usd=None,
            volume_5m_usd=None,
            volume_1h_usd=None,
            liquidity_pool_depth_usd=float(pool_state.native_reserve * 150) if pool_state and pool_state.native_reserve > 0 else None,
            active_rules={
                "WhaleInflow": wallet_profile.total_trades > 30 if wallet_profile else False,
                "DevExitConfirmed": not (sec and not sec.mint_authority_disabled),
                "PatternMatchScore": pattern_match_score,
                "HoneypotSafe": not (sec and sec.is_honeypot),
                "RejectionFlags": resp.security_assessment.flags if resp.security_assessment else [],
            },
            confidence_score=resp.confidence_score,
            reason=explicit_reason,
            metadata={"global_risk_mode": self._global_risk_mode.value},
        )

        if self._ledger is not None and hasattr(self._ledger, "record_decision"):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._ledger.record_decision(decision_record))
            except RuntimeError:
                pass

        self._recent_audits.append({
            "token": target_token,
            "chain": chain.value,
            "decision": str(resp.decision),
            "confidence": resp.confidence_score,
            "risk_mode": self._global_risk_mode.value,
            "timestamp": time.time(),
        })

        return resp

    def _build_telemetry_payload(
        self,
        signal: SignalEvent,
        pool_state: Optional[PoolState],
        security_report: Optional[SecurityReport],
        news_event: Optional[NewsEvent],
        wallet_profile: Optional[WalletProfile],
        recent_win_rate: float,
        recent_profit_factor: float,
    ) -> dict[str, Any]:
        """Construct structured telemetry packet for AI supervisor evaluation."""
        ps = pool_state or getattr(signal, "pool_state", None)
        sec = security_report or getattr(signal, "security_report", None)
        trigger = getattr(signal, "trigger_swap", None)

        pool_reserve_native = float(ps.native_reserve) if ps else 10.0
        token_reserve = float(ps.token_reserve) if ps else 1_000_000.0
        pool_address = ps.pool_address if ps else getattr(signal, "pool_address", "")

        # Bonding curve calculation (e.g. Pump.fun virtual pool: 30 SOL initial -> 85 SOL migration target)
        bonding_curve_progress = 0.0
        if signal.chain == ChainIdentifier.SOLANA_MAINNET:
            # Migration threshold ~85 SOL
            bonding_curve_progress = min(1.0, max(0.0, (pool_reserve_native - 30.0) / 55.0)) if pool_reserve_native >= 30.0 else 0.0

        sender = trigger.sender if trigger else getattr(wallet_profile, "wallet_address", "")
        has_wallet_profile = wallet_profile is not None

        return {
            "chain": signal.chain.value,
            "target_token_address": signal.token_address,
            "pool_address": pool_address,
            "suggested_side": signal.suggested_side.value,
            "initiator_wallet": sender,
            "pool_telemetry": {
                "native_reserve": pool_reserve_native,
                "token_reserve": token_reserve,
                "bonding_curve_progress_pct": round(bonding_curve_progress * 100, 2),
                "is_pump_fun": (
                    "pump" in signal.token_address.lower()
                    or "pump" in pool_address.lower()
                    or "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" in pool_address
                    or getattr(signal, "signal_source", "") == "pump_fun"
                ),
            },
            "security_telemetry": {
                "is_honeypot": sec.is_honeypot if sec else False,
                "buy_tax_bps": sec.buy_tax_bps if sec else 0,
                "sell_tax_bps": sec.sell_tax_bps if sec else 0,
                "lp_burned_ratio": sec.lp_burned_ratio if sec else 1.0,
                "top10_concentration": sec.top10_concentration if sec else 0.12,
                "mint_authority_disabled": sec.mint_authority_disabled if sec else True,
                "freeze_authority_disabled": getattr(sec, "freeze_authority_disabled", True) if sec else True,
                "is_clean_bytecode": getattr(sec, "is_clean_bytecode", True) if sec else True,
                "rejection_reasons": getattr(sec, "rejection_reasons", []) if sec else [],
            },
            "wallet_telemetry": {
                "has_wallet_profile": has_wallet_profile,
                "wallet_address": sender,
                "total_trades": getattr(wallet_profile, "total_trades", getattr(wallet_profile, "total_closed_trades", 0)) if has_wallet_profile else 0,
                "win_rate_pct": getattr(wallet_profile, "win_rate_pct", recent_win_rate) if has_wallet_profile else 50.0,
                "profit_factor": getattr(wallet_profile, "profit_factor", getattr(wallet_profile, "custom_metadata", {}).get("profit_factor", recent_profit_factor) if getattr(wallet_profile, "custom_metadata", None) else recent_profit_factor) if has_wallet_profile else 1.5,
                "sortino_ratio": getattr(wallet_profile, "sortino_ratio", getattr(wallet_profile, "custom_metadata", {}).get("sortino_ratio", 2.1) if getattr(wallet_profile, "custom_metadata", None) else 2.1) if has_wallet_profile else 2.0,
                "max_drawdown_pct": getattr(wallet_profile, "max_drawdown_pct", getattr(wallet_profile, "custom_metadata", {}).get("max_drawdown_pct", 18.0) if getattr(wallet_profile, "custom_metadata", None) else 18.0) if has_wallet_profile else 15.0,
                "median_hold_time_s": getattr(wallet_profile, "median_holding_time_seconds", getattr(wallet_profile, "median_holding_time_s", 280.0)) if has_wallet_profile else 300.0,
                "single_trade_outlier_pct": (getattr(wallet_profile, "outlier_pnl_ratio", 0.22) * 100) if has_wallet_profile else 10.0,
                "is_funding_aggregator": getattr(wallet_profile, "is_disperse_funded", getattr(wallet_profile, "custom_metadata", {}).get("is_disperse_funded", False) if getattr(wallet_profile, "custom_metadata", None) else False) if has_wallet_profile else False,
                "is_block0_bundle": getattr(wallet_profile, "is_block0_snipe", getattr(wallet_profile, "custom_metadata", {}).get("is_block0_snipe", False) if getattr(wallet_profile, "custom_metadata", None) else False) if has_wallet_profile else False,
                "is_mev_backrun": getattr(wallet_profile, "is_mev_bot", getattr(wallet_profile, "custom_metadata", {}).get("is_mev_bot", False) if getattr(wallet_profile, "custom_metadata", None) else False) if has_wallet_profile else False,
            },
            "sentiment_telemetry": {
                "raw_text": news_event.raw_text if news_event else "",
                "source_channel": news_event.source_channel if news_event else "",
                "sentiment_score": news_event.sentiment_score if news_event else 0.0,
                "timestamp_age_s": (time.time() - news_event.timestamp) if news_event and news_event.timestamp else 0.0,
                "template_duplicate_count": getattr(news_event, "duplicate_count", 0) if news_event else 0,
            },
            "system_state": {
                "global_risk_mode": self._global_risk_mode.value,
                "recent_win_rate": recent_win_rate,
                "recent_profit_factor": recent_profit_factor,
            },
        }

    async def _call_llm(self, telemetry: dict[str, Any]) -> AISupervisorResponse:
        """Execute async LLM call with strict JSON response decoding."""
        session = await self._get_session()
        payload_text = json.dumps(telemetry, indent=2)

        if self._provider == "gemini":
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{self._model}:generateContent?key={self._api_key}"
            body = {
                "contents": [
                    {"role": "user", "parts": [{"text": f"Telemetry Input:\n{payload_text}"}]}
                ],
                "systemInstruction": {
                    "parts": [{"text": ALPHA_SUPERVISOR_SYSTEM_PROMPT}]
                },
                "generationConfig": {
                    "responseMimeType": "application/json",
                    "temperature": 0.1,
                },
            }
            async with session.post(url, json=body, timeout=self._timeout_s) as response:
                if response.status != 200:
                    text = await response.text()
                    raise RuntimeError(f"Gemini API returned status {response.status}: {text[:200]}")
                data = await response.json()
                raw_json = data["candidates"][0]["content"]["parts"][0]["text"]
                return AISupervisorResponse.from_strict_json(raw_json)

        elif self._provider in ("openai", "openrouter"):
            base_url = "https://api.openai.com/v1" if self._provider == "openai" else "https://openrouter.ai/api/v1"
            url = f"{base_url}/chat/completions"
            headers = {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}
            body = {
                "model": self._model,
                "messages": [
                    {"role": "system", "content": ALPHA_SUPERVISOR_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Telemetry Input:\n{payload_text}"},
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.1,
            }
            async with session.post(url, json=body, headers=headers, timeout=self._timeout_s) as response:
                if response.status != 200:
                    text = await response.text()
                    raise RuntimeError(f"{self._provider} API returned status {response.status}: {text[:200]}")
                data = await response.json()
                raw_json = data["choices"][0]["message"]["content"]
                return AISupervisorResponse.from_strict_json(raw_json)

        raise ValueError(f"Unsupported AI provider: {self._provider}")

    def evaluate_deterministic(self, telemetry: dict[str, Any]) -> AISupervisorResponse:
        """
        High-Performance Deterministic Heuristic Engine.
        Executes all 5 sections with zero latency:
        1. Wallet audit & deception filters
        2. On-chain security & liquidity depth
        3. Sentiment velocity & novelty
        4. Adaptive fractional Kelly execution & dynamic exits
        5. Closed-loop feedback
        """
        target_token = telemetry.get("target_token_address", "")
        chain = telemetry.get("chain", "solana_mainnet")
        sec = telemetry.get("security_telemetry", {})
        wallet = telemetry.get("wallet_telemetry", {})
        pool = telemetry.get("pool_telemetry", {})
        sentiment = telemetry.get("sentiment_telemetry", {})

        rejection_flags: list[str] = []
        is_secure = True
        honeypot_risk = HoneypotRisk.NONE
        liquidity_health = LiquidityHealth.OPTIMAL

        # =========================================================================
        # SECTION 1: WALLET & ON-CHAIN PROFILING AUDIT
        # =========================================================================
        wallet_classification = WalletRiskClassification.ORGANIC_SMART_MONEY
        is_wallet_reputable = True
        wallet_rationale = "Wallet demonstrates authentic trading distribution, consistent Sortino, and independent entries."

        has_wallet_profile = wallet.get("has_wallet_profile", False)
        is_pump = pool.get("is_pump_fun", False)

        if not has_wallet_profile:
            # Market / Liquidity launch signal without explicit copy-trading target
            wallet_classification = WalletRiskClassification.ORGANIC_SMART_MONEY
            is_wallet_reputable = True
            wallet_rationale = "Market launch / pool signal verified: entropy evaluated via stage buffer and security heuristics."
        else:
            # A. Cabal & Insider Detection
            if wallet.get("is_funding_aggregator") or wallet.get("is_disperse_funded"):
                wallet_classification = WalletRiskClassification.CABAL_INSIDER
                is_wallet_reputable = False
                wallet_rationale = "Sybil cluster detected: Wallet received funding from aggregator/mixer co-temporal with deployer."
                rejection_flags.append("SYBIL_FUNDING_AGGREGATOR")

            elif wallet.get("is_block0_bundle"):
                wallet_classification = WalletRiskClassification.CABAL_INSIDER
                is_wallet_reputable = False
                wallet_rationale = "First-block snipe collusion detected: Wallet bought in block 0/1 within same Jito/Flashbots bundle as deployer."
                rejection_flags.append("FIRST_BLOCK_SNIPE_COLLUSION")

            # B. MEV & Non-Replicable Mechanics
            elif wallet.get("is_mev_backrun") or wallet.get("median_hold_time_s", 100.0) < 45.0:
                wallet_classification = WalletRiskClassification.MEV_BOT
                is_wallet_reputable = False
                wallet_rationale = "MEV Bot detected: 0-slot atomic execution / median holding time < 45s; non-replicable copy trade."
                rejection_flags.append("MEV_BOT_UNREPLICABLE")

            elif wallet.get("total_trades", 0) > 30 and wallet.get("profit_factor", 1.0) < 1.05 and wallet.get("win_rate_pct", 50.0) < 45.0:
                if is_pump and not wallet.get("is_copy_trade_target", False) and wallet.get("total_trades", 0) <= 60:
                    # On Pump.fun bonding curves, dev bundling and churn are ubiquitous.
                    # Mitigate via reduced fractional Kelly sizing and tighter stop loss rather than a fatal binary veto.
                    wallet_classification = WalletRiskClassification.ORGANIC_SMART_MONEY
                    is_wallet_reputable = True
                    wallet_rationale = "Pump.fun bonding curve churn detected; mitigating via fractional Kelly reduction and tight stop loss."
                else:
                    wallet_classification = WalletRiskClassification.WASH_TRADER
                    is_wallet_reputable = False
                    wallet_rationale = "Wash-trading volume manipulation detected: Net wallet profit minus gas is near zero despite high churn."
                    rejection_flags.append("WASH_TRADING_FAKE_VOLUME")

            # C. Legitimate Smart Money Criteria
            elif wallet.get("total_trades", 0) < 15 or wallet.get("single_trade_outlier_pct", 0.0) > 60.0:
                if is_pump:
                    wallet_classification = WalletRiskClassification.ORGANIC_SMART_MONEY
                    is_wallet_reputable = True
                    wallet_rationale = "Early bonding curve entry; allowing with dynamic risk management."
                else:
                    wallet_classification = WalletRiskClassification.NOVICE_LUCKY
                    is_wallet_reputable = False
                    wallet_rationale = "Disqualified under survivorship bias: Single lucky trade masks consecutive losses or sample size < 15."
                    rejection_flags.append("LUCKY_OUTLIER_OR_INSUFFICIENT_HISTORY")

        # =========================================================================
        # SECTION 2: ON-CHAIN SECURITY, LIQUIDITY DEPTH & PRE-FLIGHT
        # =========================================================================
        # A. Contract & Authority Integrity
        if chain == ChainIdentifier.SOLANA_MAINNET.value:
            if not sec.get("mint_authority_disabled", True) or not sec.get("freeze_authority_disabled", True):
                is_secure = False
                honeypot_risk = HoneypotRisk.CONFIRMED
                rejection_flags.append("ACTIVE_MINT_OR_FREEZE_AUTHORITY")

        elif chain == ChainIdentifier.BASE_MAINNET.value:
            if sec.get("is_honeypot", False):
                is_secure = False
                honeypot_risk = HoneypotRisk.CONFIRMED
                rejection_flags.append("CONFIRMED_HONEYPOT_BYTECODE")
            elif sec.get("buy_tax_bps", 0) > 200 or sec.get("sell_tax_bps", 0) > 200:
                is_secure = False
                honeypot_risk = HoneypotRisk.SUSPICIOUS
                rejection_flags.append("TRANSFER_TAX_EXCEEDS_2_PCT")

        # Liquidity Pool State
        lp_burn = sec.get("lp_burned_ratio", 1.0)
        if not is_pump and lp_burn < 0.90:
            liquidity_health = LiquidityHealth.UNLOCKED
            is_secure = False
            rejection_flags.append("LP_TOKENS_UNLOCKED_OR_UNBURNED")

        # B. Liquidity Dynamics & Slippage Vulnerability
        curve_pct = pool.get("bonding_curve_progress_pct", 0.0)
        if is_pump and 90.0 <= curve_pct <= 99.0:
            rejection_flags.append("BONDING_CURVE_MIGRATION_SNIPER_VOID_90_99_PCT")
            liquidity_health = LiquidityHealth.THIN

        top10 = sec.get("top10_concentration", 0.10)
        if (not is_pump and top10 > 0.18) or (is_pump and top10 > 0.65):
            rejection_flags.append("TOP10_CONCENTRATION_EXCESSIVE")

        # =========================================================================
        # SECTION 3: SENTIMENT VELOCITY & NEWS DECODER
        # =========================================================================
        if sentiment.get("timestamp_age_s", 0.0) > 300.0 and sentiment.get("source_channel"):
            rejection_flags.append("NEWS_CATALYST_EXPIRED_LOCAL_TOP")

        if sentiment.get("template_duplicate_count", 0) >= 3:
            rejection_flags.append("BOT_FARM_PAID_RAID_DETECTED")

        # =========================================================================
        # SECTION 4: ADAPTIVE EXECUTION, POSITION SIZING & EXIT ARCHITECTURE
        # =========================================================================
        # Fractional Kelly Criterion calculation:
        # Size = (p / L) - (q / W)
        win_prob = max(0.40, min(0.85, wallet.get("win_rate_pct", 65.0) / 100.0))
        loss_prob = 1.0 - win_prob
        win_ratio = 1.8
        loss_ratio = 0.6
        raw_kelly = (win_prob / loss_ratio) - (loss_prob / win_ratio)
        fractional_kelly_pct = max(0.25, min(2.0, (raw_kelly * 0.25) * 10.0))

        # Adjust for Global Risk Mode
        if self._global_risk_mode == GlobalRiskMode.DEFENSIVE:
            fractional_kelly_pct = max(0.25, fractional_kelly_pct * 0.5)
            max_slippage = 100
            priority_mult = 1.0
        elif self._global_risk_mode == GlobalRiskMode.EXPAND:
            fractional_kelly_pct = min(2.0, fractional_kelly_pct * 1.25)
            max_slippage = 200
            priority_mult = 1.5
        else:
            max_slippage = 150
            priority_mult = 1.2

        has_churn = is_pump and (wallet.get("total_trades", 0) > 30 and wallet.get("profit_factor", 1.0) < 1.05)
        if has_churn:
            fractional_kelly_pct = max(0.20, fractional_kelly_pct * 0.6)
            hard_stop = -10.0
        else:
            hard_stop = -15.0

        action_params = ActionParameters(
            target_token_address=target_token,
            recommended_position_pct=round(fractional_kelly_pct, 2),
            max_slippage_bps=max_slippage,
            priority_fee_multiplier=priority_mult,
            take_profit_ladder=[
                TakeProfitStage(trigger_multiplier=2.0, sell_pct=40.0),
                TakeProfitStage(trigger_multiplier=3.5, sell_pct=30.0),
            ],
            hard_stop_loss_pct=hard_stop,
            trailing_stop_activation_pct=40.0,
            time_exit_minutes=15,
        )

        # =========================================================================
        # FINAL DECISION SYNTHESIS & SELF-LEARNING CALIBRATION
        # =========================================================================
        hard_veto_flags = {
            "ACTIVE_MINT_OR_FREEZE_AUTHORITY",
            "CONFIRMED_HONEYPOT_BYTECODE",
            "LP_TOKENS_UNLOCKED_OR_UNBURNED",
            "SYBIL_FUNDING_AGGREGATOR",
            "FIRST_BLOCK_SNIPE_COLLUSION",
            "MEV_BOT_UNREPLICABLE",
        }

        if not self._strict_veto:
            fatal_flags = [f for f in rejection_flags if f in hard_veto_flags]
            should_veto = len(fatal_flags) > 0 or not is_secure
        else:
            should_veto = len(rejection_flags) > 0 or not is_secure or not is_wallet_reputable

        metrics = self._outcome_tracker.get_metrics()
        if metrics.calibration_status == "CALIBRATING_EXPAND" and not self._strict_veto and should_veto:
            fatal_flags = [f for f in rejection_flags if f in hard_veto_flags]
            if len(fatal_flags) == 0:
                should_veto = False
                rejection_flags = []

        decision = AISupervisorDecisionEnum.PASS if should_veto else AISupervisorDecisionEnum.EXECUTE_BUY
        confidence = 0.95 if should_veto else (0.88 if metrics.win_rate_pct >= 50.0 or metrics.finalized_count < 5 else 0.75)

        # Closed Loop Feedback Insights
        toxic_to_blacklist: list[str] = []
        if "CONFIRMED_HONEYPOT_BYTECODE" in rejection_flags or "ACTIVE_MINT_OR_FREEZE_AUTHORITY" in rejection_flags:
            toxic_to_blacklist.append(target_token)

        initiator = telemetry.get("initiator_wallet")
        if initiator and ("SYBIL_FUNDING_AGGREGATOR" in rejection_flags or "FIRST_BLOCK_SNIPE_COLLUSION" in rejection_flags):
            toxic_to_blacklist.append(initiator)

        adjust_risk = GlobalRiskMode.DEFENSIVE if len(rejection_flags) >= 2 else self._global_risk_mode
        insight = (
            f"Vetoed {target_token[:10]} due to {', '.join(rejection_flags[:3])}."
            if should_veto
            else f"Approved {target_token[:10]} with {action_params.recommended_position_pct}% Kelly size and multi-stage TP ladder."
        )

        return AISupervisorResponse(
            decision=decision,
            confidence_score=confidence,
            action_parameters=action_params,
            wallet_audit=WalletAudit(
                is_wallet_reputable=is_wallet_reputable,
                risk_classification=wallet_classification,
                rationale=wallet_rationale,
            ),
            security_assessment=SecurityAssessment(
                is_secure=is_secure,
                honeypot_risk=honeypot_risk,
                liquidity_health=liquidity_health,
                flags=rejection_flags,
            ),
            feedback_tuning=FeedbackTuning(
                adjust_global_risk=adjust_risk,
                blacklisted_entities=toxic_to_blacklist,
                insights_learned=insight,
            ),
        )

    def _create_blacklist_veto_response(self, target_token: str, initiator: str) -> AISupervisorResponse:
        """Create an immediate veto response for already blacklisted entities."""
        return AISupervisorResponse(
            decision=AISupervisorDecisionEnum.PASS,
            confidence_score=1.0,
            action_parameters=ActionParameters(
                target_token_address=target_token,
                recommended_position_pct=0.0,
                max_slippage_bps=50,
                priority_fee_multiplier=1.0,
            ),
            wallet_audit=WalletAudit(
                is_wallet_reputable=False,
                risk_classification=WalletRiskClassification.CABAL_INSIDER,
                rationale=f"Initiator or token address is explicitly blacklisted in local state ({initiator or target_token}).",
            ),
            security_assessment=SecurityAssessment(
                is_secure=False,
                honeypot_risk=HoneypotRisk.CONFIRMED,
                liquidity_health=LiquidityHealth.THIN,
                flags=["ENTITY_BLACKLISTED"],
            ),
            feedback_tuning=FeedbackTuning(
                adjust_global_risk=self._global_risk_mode,
                blacklisted_entities=[target_token],
                insights_learned=f"Suppressed execution for blacklisted entity {target_token[:10]}.",
            ),
        )

    async def reflect_on_trade(self, reflection: Any) -> FeedbackTuning:
        """
        Section 5: Closed-Loop Feedback & Hyperparameter Auto-Tuning.
        Analyzes post-trade execution quality, slippage leakage, and failure modes.
        """
        token = getattr(reflection, "token_address", "N/A")
        actual_bps = getattr(reflection, "actual_slippage_bps", 0)
        expected_bps = getattr(reflection, "expected_slippage_bps", 150)
        is_win = getattr(reflection, "is_win", False)
        roi_pct = getattr(reflection, "roi_pct", 0.0)

        slippage_leakage_bps = max(0, actual_bps - expected_bps)
        slippage_leakage_pct = slippage_leakage_bps / 100.0

        blacklisted: list[str] = []
        attribution: LossAttribution = LossAttribution.LATE_ENTRY

        if not is_win:
            # Diagnose failure mode root cause
            if roi_pct <= -80.0:
                attribution = LossAttribution.RUG_PULL
                blacklisted.append(token)
            elif slippage_leakage_pct > 3.0:
                attribution = LossAttribution.MEV_SANDWICH
            elif abs(roi_pct) < 5.0 and getattr(reflection, "time_to_fill_ms", 0.0) > 5000.0:
                attribution = LossAttribution.VOLUME_DRYOUT
            elif roi_pct <= -40.0:
                attribution = LossAttribution.CABAL_DUMP
                blacklisted.append(token)

            insight = (
                f"Post-mortem on {token[:10]}: Loss {roi_pct:.2f}% attributed to {attribution.value}. "
                f"Slippage leakage={slippage_leakage_pct:.2f}% (actual={actual_bps}bps vs exp={expected_bps}bps)."
            )
            # Switch to defensive mode on severe loss or MEV sandwich
            self._global_risk_mode = GlobalRiskMode.DEFENSIVE
        else:
            insight = f"Trade on {token[:10]} succeeded with +{roi_pct:.2f}% ROI. Slippage leakage within normal bounds ({slippage_leakage_pct:.2f}%)."
            if self._global_risk_mode == GlobalRiskMode.DEFENSIVE:
                self._global_risk_mode = GlobalRiskMode.NEUTRAL

        self._learned_insights.append(insight)
        for b in blacklisted:
            self._blacklisted_entities.add(b)

        return FeedbackTuning(
            adjust_global_risk=self._global_risk_mode,
            blacklisted_entities=blacklisted,
            insights_learned=insight,
        )
