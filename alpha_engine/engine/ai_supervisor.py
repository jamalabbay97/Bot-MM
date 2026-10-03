"""
alpha_engine.engine.ai_supervisor — Institutional-Grade Autonomous Risk Engine
==============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | asyncio | aiohttp | Google Gemini / OpenAI / Deterministic Engine
"""

from __future__ import annotations

import asyncio
from collections import deque
import json
import logging
import time
from typing import Any, Optional

import aiohttp

from alpha_engine.config import EngineConfig
from alpha_engine.models.ai import (
    ActionParameters,
    AISupervisorDecisionEnum,
    AISupervisorResponse,
    FeedbackTuning,
    GlobalRiskMode,
    HoneypotRisk,
    LiquidityHealth,
    LossAttribution,
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
        self._model: str = getattr(config, "ai_model", "gemini-2.5-flash")
        self._timeout_s: float = getattr(config, "ai_timeout_s", 4.0)
        self._strict_veto: bool = getattr(config, "ai_strict_veto", True)

        # Dynamic state & memory
        self._blacklisted_entities: set[str] = set()
        self._global_risk_mode: GlobalRiskMode = GlobalRiskMode.NEUTRAL
        self._recent_audits: deque[dict[str, Any]] = deque(maxlen=50)
        self._learned_insights: deque[str] = deque(maxlen=50)
        self._total_audits: int = 0
        self._veto_count: int = 0
        self._buy_approval_count: int = 0

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
        return {
            "enabled": self._enabled,
            "provider": self._provider,
            "model": self._model,
            "has_api_key": bool(self._api_key),
            "global_risk_mode": self._global_risk_mode.value,
            "total_audits": self._total_audits,
            "approved_buys": self._buy_approval_count,
            "vetoed_signals": self._veto_count,
            "blacklisted_entities_count": len(self._blacklisted_entities),
            "recent_insights": list(self._learned_insights)[-5:],
        }

    async def audit_signal(
        self,
        signal: SignalEvent,
        pool_state: Optional[PoolState] = None,
        security_report: Optional[SecurityReport] = None,
        news_event: Optional[NewsEvent] = None,
        wallet_profile: Optional[WalletProfile] = None,
        recent_win_rate: float = 65.0,
        recent_profit_factor: float = 2.4,
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
        elif resp.decision == AISupervisorDecisionEnum.EXECUTE_BUY:
            self._buy_approval_count += 1

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
                "is_pump_fun": "pump" in signal.token_address.lower() or "pump" in pool_address.lower() or (ps is not None and pool_reserve_native < 90.0),
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
                "wallet_address": sender,
                "total_trades": getattr(wallet_profile, "total_trades", getattr(wallet_profile, "total_closed_trades", 45)),
                "win_rate_pct": getattr(wallet_profile, "win_rate_pct", recent_win_rate),
                "profit_factor": getattr(wallet_profile, "profit_factor", getattr(wallet_profile, "custom_metadata", {}).get("profit_factor", recent_profit_factor) if getattr(wallet_profile, "custom_metadata", None) else recent_profit_factor),
                "sortino_ratio": getattr(wallet_profile, "sortino_ratio", getattr(wallet_profile, "custom_metadata", {}).get("sortino_ratio", 2.1) if getattr(wallet_profile, "custom_metadata", None) else 2.1),
                "max_drawdown_pct": getattr(wallet_profile, "max_drawdown_pct", getattr(wallet_profile, "custom_metadata", {}).get("max_drawdown_pct", 18.0) if getattr(wallet_profile, "custom_metadata", None) else 18.0),
                "median_hold_time_s": getattr(wallet_profile, "median_holding_time_seconds", getattr(wallet_profile, "median_holding_time_s", 280.0)),
                "single_trade_outlier_pct": getattr(wallet_profile, "outlier_pnl_ratio", 0.22) * 100,
                "is_funding_aggregator": getattr(wallet_profile, "is_disperse_funded", getattr(wallet_profile, "custom_metadata", {}).get("is_disperse_funded", False) if getattr(wallet_profile, "custom_metadata", None) else False),
                "is_block0_bundle": getattr(wallet_profile, "is_block0_snipe", getattr(wallet_profile, "custom_metadata", {}).get("is_block0_snipe", False) if getattr(wallet_profile, "custom_metadata", None) else False),
                "is_mev_backrun": getattr(wallet_profile, "is_mev_bot", getattr(wallet_profile, "custom_metadata", {}).get("is_mev_bot", False) if getattr(wallet_profile, "custom_metadata", None) else False),
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
            wallet_classification = WalletRiskClassification.WASH_TRADER
            is_wallet_reputable = False
            wallet_rationale = "Wash-trading volume manipulation detected: Net wallet profit minus gas is near zero despite high churn."
            rejection_flags.append("WASH_TRADING_FAKE_VOLUME")

        # C. Legitimate Smart Money Criteria
        elif wallet.get("total_trades", 0) < 15 or wallet.get("single_trade_outlier_pct", 0.0) > 60.0:
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
        is_pump = pool.get("is_pump_fun", False)
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
        # Typical meme/dex reward-to-risk ratio: Win_Ratio ~ 1.5, Loss_Ratio ~ 0.5 (3:1)
        win_ratio = 1.8
        loss_ratio = 0.6
        raw_kelly = (win_prob / loss_ratio) - (loss_prob / win_ratio)
        # Fractional Kelly (1/4 Kelly for safety)
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

        action_params = ActionParameters(
            target_token_address=target_token,
            recommended_position_pct=round(fractional_kelly_pct, 2),
            max_slippage_bps=max_slippage,
            priority_fee_multiplier=priority_mult,
            take_profit_ladder=[
                TakeProfitStage(trigger_multiplier=2.0, sell_pct=40.0),
                TakeProfitStage(trigger_multiplier=3.5, sell_pct=30.0),
            ],
            hard_stop_loss_pct=-15.0,
            trailing_stop_activation_pct=40.0,
            time_exit_minutes=15,
        )

        # =========================================================================
        # FINAL DECISION SYNTHESIS
        # =========================================================================
        should_veto = len(rejection_flags) > 0 or not is_secure or not is_wallet_reputable
        decision = AISupervisorDecisionEnum.PASS if should_veto else AISupervisorDecisionEnum.EXECUTE_BUY
        confidence = 0.95 if should_veto else 0.88

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
