"""
alpha_engine.ruflo.agents.learning_agent — Ruflo Cognitive Learning & Optimization Agent
========================================================================================
Multi-Chain (SVM & EVM) Autonomous Trading Swarm
Python 3.11+ | ruflo agent harness | Retrieve -> Judge -> Distill -> Consolidate
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from alpha_engine.ruflo.agents.base import BaseRufloAgent
from alpha_engine.ruflo.memory import (
    RufloEpisodicMemory,
    RufloIntelligencePipeline,
    RufloSemanticMemory,
    SemanticRule,
)
from alpha_engine.ruflo.protocol import AgentRole, SwarmMessage, ToolDefinition

logger = logging.getLogger(__name__)


class RufloLearningAgent(BaseRufloAgent):
    """
    Ruflo Cognitive Learning & Optimization Agent:
    Specialized agent dedicated to the 4-phase Ruflo intelligence loop:
      - Retrieve: Fetches episodic nearest-neighbors and relevant semantic rules.
      - Judge: Formulates empirical probability and risk adjustments for proposed trades.
      - Distill: Extracts causal attribution heuristics from completed trades.
      - Consolidate: Merges distilled insights into persistent semantic memory.
    """

    def __init__(
        self,
        episodic_memory: Optional[RufloEpisodicMemory] = None,
        semantic_memory: Optional[RufloSemanticMemory] = None,
        pipeline: Optional[RufloIntelligencePipeline] = None,
    ) -> None:
        super().__init__(
            name="RufloLearner",
            role=AgentRole.LEARNER,
            description="Autonomous cross-agent distillation, semantic rule consolidation, and hyperparameter tuning.",
        )
        self.episodic_memory = episodic_memory or RufloEpisodicMemory()
        self.semantic_memory = semantic_memory or RufloSemanticMemory()
        self.pipeline = pipeline or RufloIntelligencePipeline(
            episodic_memory=self.episodic_memory,
            semantic_memory=self.semantic_memory,
        )

        # Register MCP-compatible tools
        self.register_tool(
            ToolDefinition(
                name="distill_and_consolidate",
                description="Synthesize causal heuristics from closed trades and merge into semantic memory.",
                category="LEARNING",
                requires_lock=True,
            ),
            self.tool_distill_and_consolidate,
        )
        self.register_tool(
            ToolDefinition(
                name="query_semantic_rules",
                description="Query active semantic heuristics by category with confidence thresholds.",
                category="LEARNING",
                requires_lock=False,
            ),
            self.tool_query_semantic_rules,
        )
        self.register_tool(
            ToolDefinition(
                name="get_pipeline_judgment",
                description="Execute Retrieve -> Judge pipeline for candidate signal risk adjustment.",
                category="DECISION",
                requires_lock=False,
            ),
            self.tool_get_pipeline_judgment,
        )

    async def tool_distill_and_consolidate(
        self,
        trade_outcome: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Distill causal lessons from trade outcome and consolidate into semantic memory."""
        new_rules = await self.pipeline.distill(trade_outcome)
        res = await self.pipeline.consolidate(new_rules)
        return {
            "distilled_rules_count": len(new_rules),
            "new_rules": [r.to_dict() for r in new_rules],
            "consolidation_summary": res,
        }

    async def tool_query_semantic_rules(
        self,
        category: Optional[str] = None,
        min_confidence: float = 0.50,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Query active semantic heuristics from semantic memory."""
        rules: list[SemanticRule] = self.semantic_memory.query_rules(
            category=category,
            active_only=True,
            min_confidence=min_confidence,
        )
        return [r.to_dict() for r in rules]

    def get_active_semantic_rules(self, category: Optional[str] = None) -> list[SemanticRule]:
        """Direct accessor returning active SemanticRule domain entities."""
        return self.semantic_memory.query_rules(category=category, active_only=True)


    async def tool_get_pipeline_judgment(
        self,
        query_vector: list[float],
        proposed_signal: dict[str, Any],
        top_k: int = 5,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute Retrieve -> Judge for candidate signal."""
        retrieval = await self.pipeline.retrieve(
            query_vector=query_vector,
            token_context=proposed_signal,
            top_k=top_k,
        )
        judgment = self.pipeline.judge(retrieval=retrieval, proposed_signal=proposed_signal)
        return {
            "verdict": judgment.verdict,
            "empirical_win_prob": judgment.empirical_win_prob,
            "size_multiplier": judgment.size_multiplier,
            "adjusted_parameters": judgment.adjusted_parameters,
            "warning_flags": judgment.warning_flags,
            "synthesis_reflection": judgment.synthesis_reflection,
            "matched_episodes_count": len(retrieval.matched_episodes),
        }

    async def handle_message(self, message: SwarmMessage) -> None:
        """Handle incoming swarm requests and broadcasts."""
        msg_type = message.message_type
        if msg_type in ("RULE_CONSOLIDATION", "CLOSED_TRADE"):
            trade_data = message.payload.get("trade_outcome", message.payload)
            if trade_data:
                await self.tool_distill_and_consolidate(trade_data)
        elif msg_type == "REQUEST" and message.payload.get("action") == "EVALUATE_SIGNAL":
            q_vec = message.payload.get("feature_vector", [0.5] * 7)
            sig_ctx = message.payload.get("signal_context", {})
            judg = await self.tool_get_pipeline_judgment(q_vec, sig_ctx)
            logger.debug("[RufloLearner] Computed pipeline judgment: %s", judg.get("verdict"))
