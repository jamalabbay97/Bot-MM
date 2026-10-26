"""
alpha_engine.ruflo.agents.supervisor_agent — Self-Learning & Supervisory Loop Agent
==================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Ruflo Autonomous Supervisor Agent | Episodic Memory & Reflection
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
from alpha_engine.engine.feedback import AdaptiveFeedbackEngine, TradeReflection
from alpha_engine.engine.rpc_health import RPCHealthMonitor
from alpha_engine.models.ai import AISupervisorResponse
from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.ruflo.agents.base import BaseRufloAgent
from alpha_engine.ruflo.memory import MemoryEpisode, RufloEpisodicMemory
from alpha_engine.ruflo.protocol import AgentRole, SwarmMessage, ToolDefinition

logger = logging.getLogger(__name__)


class RufloSupervisorAgent(BaseRufloAgent):
    """
    Self-Learning & Supervisory Loop Agent:
    Coordinates multi-agent consensus, queries LLM / deterministic risk engines,
    manages Ruflo episodic memory (AgentDB), and executes continuous reflection loops
    over closed trades to adaptively update strategy weights, slippage allowances,
    and wallet whitelist statuses.
    """

    def __init__(
        self,
        ai_supervisor: Optional[AlphaSupervisorAI] = None,
        feedback_engine: Optional[AdaptiveFeedbackEngine] = None,
        rpc_monitor: Optional[RPCHealthMonitor] = None,
        episodic_memory: Optional[RufloEpisodicMemory] = None,
        name: str = "RufloSupervisor",
        config: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            name=name,
            role=AgentRole.SUPERVISOR,
            description="Autonomous swarm orchestrator, episodic memory reflection, and self-learning agent",
        )
        self.config = config
        self.ai_supervisor = ai_supervisor
        self.feedback_engine = feedback_engine or AdaptiveFeedbackEngine()
        self.rpc_monitor = rpc_monitor
        self.memory = episodic_memory or RufloEpisodicMemory()
        self._register_agent_tools()

    def _register_agent_tools(self) -> None:
        self.register_tool(
            ToolDefinition(
                name="supervisor_audit_signal",
                description="Perform deep autonomous forensics audit on a token signal before execution",
                category="supervision",
                parameters_schema={
                    "token_address": {"type": "string"},
                    "chain": {"type": "string"},
                    "confidence_score": {"type": "number"},
                },
            ),
            self.tool_audit_signal,
        )
        self.register_tool(
            ToolDefinition(
                name="supervisor_reflect_trade",
                description="Process post-trade reflection, store in episodic memory, and update self-learning weights",
                category="supervision",
                parameters_schema={
                    "trade_id": {"type": "string"},
                    "token_address": {"type": "string"},
                    "realized_pnl_usd": {"type": "number"},
                    "roi_pct": {"type": "number"},
                },
                requires_lock=True,
            ),
            self.tool_reflect_trade,
        )
        self.register_tool(
            ToolDefinition(
                name="supervisor_get_memory_insights",
                description="Query episodic memory store for historical performance patterns and source win-rates",
                category="supervision",
                parameters_schema={"lookback": {"type": "integer"}},
            ),
            self.tool_get_memory_insights,
        )
        self.register_tool(
            ToolDefinition(
                name="supervisor_check_rpc_health",
                description="Check multi-chain latency and block lag across Base EVM and Solana SVM nodes",
                category="supervision",
                parameters_schema={},
            ),
            self.tool_check_rpc_health,
        )
        self.register_tool(
            ToolDefinition(
                name="supervisor_evaluate_rpc_health",
                description="Evaluate multi-chain RPC latency and block lag across Base EVM and Solana SVM nodes",
                category="supervision",
                parameters_schema={},
            ),
            self.tool_check_rpc_health,
        )

    async def tool_audit_signal(
        self,
        token_address: str,
        chain: str = "solana_mainnet",
        confidence_score: float = 0.75,
        pool_address: str = "",
        liquidity_usd: float = 0.0,
        wallet_address: str = "",
        signal: Optional[Any] = None,
        pool_state: Optional[Any] = None,
        security_report: Optional[Any] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if not self.ai_supervisor:
            return {
                "decision": "EXECUTE_BUY" if confidence_score >= 0.70 else "PASS",
                "token_address": token_address,
                "chain": chain,
                "confidence_score": confidence_score,
                "reason": "Deterministic confidence threshold evaluation",
                "risk_score": round((1.0 - confidence_score) * 100.0, 2),
            }

        telemetry: dict[str, Any] = {
            "target_token_address": token_address,
            "chain": chain,
            "alpha_score": confidence_score * 100.0,
            "confidence_score": confidence_score,
            "pool_address": pool_address,
            "initiator_wallet": wallet_address,
            "pool_telemetry": {
                "native_reserve": liquidity_usd / (150.0 if "solana" in str(chain).lower() else 3400.0) if liquidity_usd > 0 else 10.0,
                "liquidity_usd": liquidity_usd,
                "is_pump_fun": "pump" in token_address.lower(),
            },
            "security_telemetry": {
                "is_honeypot": False,
                "mint_authority_disabled": True,
                "freeze_authority_disabled": True,
            },
            "wallet_telemetry": {
                "has_wallet_profile": bool(wallet_address),
                "wallet_address": wallet_address,
            },
        }
        if kwargs:
            telemetry.update(kwargs)

        try:
            if signal is not None and hasattr(self.ai_supervisor, "evaluate_signal"):
                resp = await self.ai_supervisor.evaluate_signal(
                    signal=signal,
                    pool_state=pool_state,
                    security_report=security_report,
                )
            else:
                resp = self.ai_supervisor.evaluate_deterministic(telemetry)

            decision_str = resp.decision.value if hasattr(resp.decision, "value") else str(resp.decision)
            rationale = getattr(resp, "risk_rationale", None) or "Vetted via Ruflo Supervisor audit pipeline"
            risk_sc = getattr(resp, "risk_score", None)
            if risk_sc is None:
                risk_sc = round((1.0 - getattr(resp, "confidence_score", confidence_score)) * 100.0, 2)
            action_params = resp.action_parameters.model_dump(mode="json") if hasattr(resp, "action_parameters") else None

            return {
                "decision": decision_str,
                "token_address": token_address,
                "chain": chain,
                "confidence_score": getattr(resp, "confidence_score", confidence_score),
                "reason": rationale,
                "risk_score": risk_sc,
                "action_parameters": action_params,
            }
        except Exception as exc:
            logger.warning("[%s] Deterministic audit fallback: %s", self.name, exc)
            return {
                "decision": "EXECUTE_BUY" if confidence_score >= 0.70 else "PASS",
                "token_address": token_address,
                "chain": chain,
                "confidence_score": confidence_score,
                "reason": f"Vetted via fallback heuristic: {exc}",
                "risk_score": round((1.0 - confidence_score) * 100.0, 2),
            }


    async def tool_reflect_trade(
        self,
        trade_id: str,
        token_address: str,
        realized_pnl_usd: float,
        roi_pct: float,
        chain: str = "solana_mainnet",
        signal_source: str = "DEX_SWAP",
        actual_slippage_bps: int = 50,
        fill_latency_ms: float = 45.0,
        root_cause: Optional[str] = None,
        feature_vector: Optional[list[float]] = None,
    ) -> dict[str, Any]:
        if root_cause is None:
            root_cause = "PROFIT_TARGET_HIT" if realized_pnl_usd > 0 else "STOP_LOSS_TRIGGERED"
        fv = list(feature_vector) if feature_vector and len(feature_vector) == 7 else [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
        episode = MemoryEpisode(
            episode_id=trade_id,
            token_address=token_address,
            chain=chain,
            decision_type="ENTER",
            signal_source=signal_source,
            confidence_score=0.85,
            feature_vector=fv,
            realized_pnl_usd=realized_pnl_usd,
            roi_pct=roi_pct,
            actual_slippage_bps=actual_slippage_bps,
            fill_latency_ms=fill_latency_ms,
            root_cause_tag=root_cause,
            summary_reflection=f"Closed trade with PnL ${realized_pnl_usd:.2f} ({roi_pct:+.1f}%). Root cause: {root_cause}",
        )
        await self.memory.record_episode(episode)

        # Trigger adaptive feedback engine
        reflection = TradeReflection(
            trade_id=trade_id,
            token_address=token_address,
            chain=ChainIdentifier.BASE_MAINNET if "base" in chain.lower() else ChainIdentifier.SOLANA_MAINNET,
            signal_source=getattr(alpha_engine_signal_source := __import__("alpha_engine.models.enums", fromlist=["SignalSource"]).SignalSource, signal_source.upper(), __import__("alpha_engine.models.enums", fromlist=["SignalSource"]).SignalSource.DEX_SWAP),
            realized_pnl_usd=__import__("decimal").Decimal(str(realized_pnl_usd)),
            roi_pct=roi_pct,
            is_win=realized_pnl_usd > 0,
            actual_slippage_bps=actual_slippage_bps,
            time_to_fill_ms=fill_latency_ms,
        )
        await self.feedback_engine.record_closed_trade(reflection)

        return {
            "status": "RECORDED",
            "episode_id": episode.episode_id,
            "social_weight": self.feedback_engine.get_social_weight(),
            "summary": episode.summary_reflection,
        }

    async def tool_get_memory_insights(self, lookback: int = 50) -> dict[str, Any]:
        source_perf = self.memory.get_source_performance(lookback=lookback)
        all_episodes = self.memory.get_all_episodes(limit=lookback)
        total_pnl = sum(e.realized_pnl_usd for e in all_episodes)
        wins = sum(1 for e in all_episodes if e.realized_pnl_usd > 0)
        win_rate = (wins / len(all_episodes)) if all_episodes else 0.0

        return {
            "total_episodes": len(all_episodes),
            "win_rate_pct": round(win_rate * 100, 2),
            "net_pnl_usd": round(total_pnl, 2),
            "by_source": source_perf,
            "active_social_weight": self.feedback_engine.get_social_weight(),
        }

    async def tool_check_rpc_health(self) -> dict[str, Any]:
        if not self.rpc_monitor:
            return {"status": "NO_RPC_MONITOR"}
        health_summary = {}
        for chain, eps in self.rpc_monitor._endpoints.items():
            health_summary[chain.value] = [
                {
                    "url": ep.url,
                    "is_healthy": ep.is_healthy,
                    "latency_ms": ep.latency_ms,
                    "latest_block": ep.latest_block,
                    "consecutive_errors": ep.consecutive_errors,
                }
                for ep in eps
            ]
        return health_summary

    tool_evaluate_rpc_health = tool_check_rpc_health

    async def handle_message(self, message: SwarmMessage) -> None:
        if message.message_type == "TRADE_CLOSED":
            payload = message.payload
            await self.tool_reflect_trade(
                trade_id=payload.get("trade_id", "unknown"),
                token_address=payload.get("token_address", ""),
                realized_pnl_usd=float(payload.get("realized_pnl_usd", 0.0)),
                roi_pct=float(payload.get("roi_pct", 0.0)),
                chain=payload.get("chain", "solana_mainnet"),
                signal_source=payload.get("signal_source", "DEX_SWAP"),
            )
