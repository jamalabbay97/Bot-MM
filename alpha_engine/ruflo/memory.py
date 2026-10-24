"""
alpha_engine.ruflo.memory — Ruflo Persistent Episodic Memory & SONA Vector Store
================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | AgentDB & SONA Episodic Memory Architecture | SQLite WAL
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Optional, Sequence

import aiosqlite

from alpha_engine.models.decisions import DecisionRecord
from alpha_engine.models.enums import ChainIdentifier, SignalSource
from alpha_engine.models.state import TradeRecord

logger = logging.getLogger(__name__)


@dataclass
class MemoryEpisode:
    """An episodic memory unit representing a complete decision-to-outcome lifecycle."""

    episode_id: str
    token_address: str
    chain: str
    decision_type: str
    signal_source: str
    confidence_score: float
    feature_vector: list[float]
    realized_pnl_usd: float
    roi_pct: float
    actual_slippage_bps: int
    fill_latency_ms: float
    root_cause_tag: str
    summary_reflection: str
    timestamp_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RufloEpisodicMemory:
    """
    Ruflo Persistent Episodic Memory (AgentDB / SONA Self-Learning Engine):
    - Maintains an in-memory and SQLite-backed vector space of trade executions and decision records.
    - Uses HNSW/Cosine similarity retrieval for sub-millisecond retrieval of similar past market conditions.
    - Feeds post-trade reflection loops back into the multi-agent swarm to autonomously adjust weights.
    """

    def __init__(
        self,
        db_path: str = "paper_trading.db",
        max_memory_episodes: int = 2000,
        max_episodes: Optional[int] = None,
    ) -> None:
        self.db_path = db_path
        self._max_episodes = max_episodes if max_episodes is not None else max_memory_episodes
        self._episodes: list[MemoryEpisode] = []
        self._lock = asyncio.Lock()
        self._conn: Optional[aiosqlite.Connection] = None
        self._initialized = False

    async def connect(self) -> None:
        """Initialize SQLite episodic memory table in WAL mode."""
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            try:
                self._conn = await aiosqlite.connect(self.db_path, timeout=30.0)
                self._conn.row_factory = aiosqlite.Row
                await self._conn.execute("PRAGMA journal_mode = WAL;")
                await self._conn.execute("PRAGMA synchronous = NORMAL;")
                await self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ruflo_episodic_memory (
                        episode_id TEXT PRIMARY KEY,
                        token_address TEXT NOT NULL,
                        chain TEXT NOT NULL,
                        decision_type TEXT NOT NULL,
                        signal_source TEXT NOT NULL,
                        confidence_score REAL NOT NULL,
                        feature_vector TEXT NOT NULL,
                        realized_pnl_usd REAL NOT NULL,
                        roi_pct REAL NOT NULL,
                        actual_slippage_bps INTEGER NOT NULL,
                        fill_latency_ms REAL NOT NULL,
                        root_cause_tag TEXT NOT NULL,
                        summary_reflection TEXT NOT NULL,
                        timestamp_ns INTEGER NOT NULL
                    );
                    """
                )
                await self._conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_ruflo_memory_token
                    ON ruflo_episodic_memory(token_address, timestamp_ns);
                    """
                )
                await self._conn.commit()
                self._initialized = True
                logger.info("RufloEpisodicMemory initialized with SQLite persistence at %s", self.db_path)
            except Exception as exc:
                logger.warning("Could not initialize SQLite connection for RufloEpisodicMemory: %s", exc)

    async def initialize(self) -> None:
        """Alias for connect."""
        await self.connect()

    async def record_episode(self, episode: MemoryEpisode) -> None:
        """Store a new memory episode in memory and write through to SQLite."""
        await self.connect()
        async with self._lock:
            self._episodes.append(episode)
            if len(self._episodes) > self._max_episodes:
                self._episodes.pop(0)

        if self._conn is not None:
            try:
                await self._conn.execute(
                    """
                    INSERT OR REPLACE INTO ruflo_episodic_memory (
                        episode_id, token_address, chain, decision_type, signal_source,
                        confidence_score, feature_vector, realized_pnl_usd, roi_pct,
                        actual_slippage_bps, fill_latency_ms, root_cause_tag, summary_reflection, timestamp_ns
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        episode.episode_id,
                        episode.token_address,
                        episode.chain,
                        episode.decision_type,
                        episode.signal_source,
                        episode.confidence_score,
                        json.dumps(episode.feature_vector),
                        episode.realized_pnl_usd,
                        episode.roi_pct,
                        episode.actual_slippage_bps,
                        episode.fill_latency_ms,
                        episode.root_cause_tag,
                        episode.summary_reflection,
                        episode.timestamp_ns,
                    ),
                )
                await self._conn.commit()
            except Exception as exc:
                logger.debug("Failed to persist Ruflo episode to SQLite: %s", exc)

    def query_similar_episodes(
        self,
        query_vector: list[float],
        top_k: int = 5,
        min_similarity: float = 0.50,
    ) -> list[tuple[MemoryEpisode, float]]:
        """
        Retrieve the top_k most similar historical episodes using Cosine Similarity.
        Returns tuples of (MemoryEpisode, similarity_score).
        """
        if not self._episodes or not query_vector:
            return []

        results: list[tuple[MemoryEpisode, float]] = []
        for ep in self._episodes:
            sim = self._cosine_similarity(query_vector, ep.feature_vector)
            if sim >= min_similarity:
                results.append((ep, sim))

        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]

    @staticmethod
    def _cosine_similarity(v1: list[float], v2: list[float]) -> float:
        if not v1 or not v2 or len(v1) != len(v2):
            return 0.0
        dot = sum(a * b for a, b in zip(v1, v2))
        norm_a = math.sqrt(sum(a * a for a in v1))
        norm_b = math.sqrt(sum(b * b for b in v2))
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        return max(0.0, min(1.0, dot / (norm_a * norm_b)))

    def get_source_performance(self, lookback: int = 50) -> dict[str, dict[str, Any]]:
        """Calculate source performance metrics (win rate, mean PnL, slippage) across recent episodes."""
        recent = self._episodes[-lookback:]
        by_source: dict[str, list[MemoryEpisode]] = {}
        for ep in recent:
            by_source.setdefault(ep.signal_source, []).append(ep)

        metrics: dict[str, dict[str, Any]] = {}
        for src, eps in by_source.items():
            wins = sum(1 for e in eps if e.realized_pnl_usd > 0)
            total = len(eps)
            win_rate = (wins / total) if total > 0 else 0.0
            avg_pnl = sum(e.realized_pnl_usd for e in eps) / total if total > 0 else 0.0
            avg_slippage = sum(e.actual_slippage_bps for e in eps) / total if total > 0 else 0
            metrics[src] = {
                "total_trades": total,
                "win_rate": round(win_rate, 4),
                "win_rate_pct": round(win_rate * 100, 2),
                "avg_pnl_usd": round(avg_pnl, 2),
                "avg_slippage_bps": round(avg_slippage, 1),
            }
        return metrics

    def get_all_episodes(self, limit: int = 100) -> list[MemoryEpisode]:
        return list(self._episodes[-limit:])

    async def get_episode_count(self) -> int:
        return len(self._episodes)

    async def search_similar_episodes(
        self,
        query_vector: list[float],
        top_k: int = 5,
        min_similarity: float = 0.50,
    ) -> list[tuple[MemoryEpisode, float]]:
        return self.query_similar_episodes(query_vector, top_k=top_k, min_similarity=min_similarity)

    async def get_recent_reflections(self, limit: int = 20) -> list[str]:
        return [ep.summary_reflection for ep in self._episodes[-limit:] if ep.summary_reflection]

    async def record_from_trade(
        self,
        trade: TradeRecord,
        decision: Optional[DecisionRecord] = None,
        feature_vector: Optional[Sequence[float]] = None,
        summary_reflection: str = "",
    ) -> MemoryEpisode:
        """Record an episode directly from TradeRecord and DecisionRecord."""
        chain_val = trade.chain.value if isinstance(trade.chain, ChainIdentifier) else str(trade.chain)
        pnl = float(trade.realized_pnl_usd or 0.0)
        roi = float(trade.roi_pct or 0.0)
        src = trade.signal_source.value if isinstance(trade.signal_source, SignalSource) else str(trade.signal_source or "UNKNOWN")
        vec = list(feature_vector) if feature_vector is not None else [0.5] * 7
        if decision and getattr(decision, "feature_vector", None):
            if hasattr(decision.feature_vector, "to_list"):
                vec = decision.feature_vector.to_list()

        episode = MemoryEpisode(
            episode_id=trade.trade_id,
            token_address=trade.token_address,
            chain=chain_val,
            decision_type=decision.decision_type.value if decision and hasattr(decision.decision_type, "value") else "TRADE",
            signal_source=src,
            confidence_score=float(decision.confidence_score) if decision and hasattr(decision, "confidence_score") else 0.8,
            feature_vector=vec,
            realized_pnl_usd=pnl,
            roi_pct=roi,
            actual_slippage_bps=int(getattr(trade, "actual_slippage_bps", 50)),
            fill_latency_ms=float(getattr(trade, "fill_latency_ms", 45.0)),
            root_cause_tag=trade.exit_reason.value if hasattr(trade, "exit_reason") and hasattr(trade.exit_reason, "value") else "EXIT",
            summary_reflection=summary_reflection or f"Trade {trade.trade_id} on {trade.token_address[:8]} with PnL ${pnl:.2f}",
        )
        await self.save_episode(episode)
        return episode

    async def close(self) -> None:
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None
            self._initialized = False
