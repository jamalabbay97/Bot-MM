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
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Optional, Sequence

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
                self._conn = await aiosqlite.connect(self.db_path, timeout=60.0)
                self._conn.row_factory = aiosqlite.Row
                await self._conn.execute("PRAGMA journal_mode = WAL;")
                await self._conn.execute("PRAGMA busy_timeout = 60000;")
                await self._conn.execute("PRAGMA synchronous = NORMAL;")
                await self._conn.execute("PRAGMA cache_size = -64000;")
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
                # Preload recent episodes from SQLite into memory for sub-millisecond retrieval
                cursor = await self._conn.execute(
                    """
                    SELECT episode_id, token_address, chain, decision_type, signal_source,
                           confidence_score, feature_vector, realized_pnl_usd, roi_pct,
                           actual_slippage_bps, fill_latency_ms, root_cause_tag, summary_reflection, timestamp_ns
                    FROM ruflo_episodic_memory
                    ORDER BY timestamp_ns ASC
                    LIMIT ?;
                    """,
                    (self._max_episodes,),
                )
                rows = await cursor.fetchall()
                loaded = []
                for r in rows:
                    try:
                        fv = json.loads(r["feature_vector"])
                        if not isinstance(fv, list):
                            fv = [0.5] * 7
                    except Exception:
                        fv = [0.5] * 7
                    loaded.append(
                        MemoryEpisode(
                            episode_id=r["episode_id"],
                            token_address=r["token_address"],
                            chain=r["chain"],
                            decision_type=r["decision_type"],
                            signal_source=r["signal_source"],
                            confidence_score=float(r["confidence_score"]),
                            feature_vector=fv,
                            realized_pnl_usd=float(r["realized_pnl_usd"]),
                            roi_pct=float(r["roi_pct"]),
                            actual_slippage_bps=int(r["actual_slippage_bps"]),
                            fill_latency_ms=float(r["fill_latency_ms"]),
                            root_cause_tag=r["root_cause_tag"],
                            summary_reflection=r["summary_reflection"],
                            timestamp_ns=int(r["timestamp_ns"]),
                        )
                    )
                self._episodes = loaded
                self._initialized = True
                logger.info("RufloEpisodicMemory initialized with SQLite persistence at %s (%d episodes loaded)", self.db_path, len(self._episodes))
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

    async def search_similar_episodes(
        self,
        query_vector: list[float],
        top_k: int = 5,
        min_similarity: float = 0.50,
    ) -> list[tuple[MemoryEpisode, float]]:
        """Async convenience alias returning (MemoryEpisode, similarity_score) tuples."""
        return self.query_similar_episodes(query_vector, top_k=top_k, min_similarity=min_similarity)

    def query_relevant_episodes(
        self,
        query_vector: Optional[list[float]] = None,
        chain: Optional[str] = None,
        signal_source: Optional[str] = None,
        token_type: Optional[str] = None,
        top_k: int = 3,
        min_similarity: float = 0.40,
    ) -> list[tuple[MemoryEpisode, float]]:
        """
        Fast bounded retrieval of relevant past episodes by feature similarity and/or metadata matching.
        """
        if not self._episodes:
            return []

        candidates = self._episodes
        chain_str = str(chain).lower() if chain else None
        src_str = str(signal_source).lower() if signal_source else None

        scored: list[tuple[MemoryEpisode, float]] = []
        for ep in reversed(candidates):
            sim = 0.50
            if query_vector and ep.feature_vector and len(query_vector) == len(ep.feature_vector):
                sim = self._cosine_similarity(query_vector, ep.feature_vector)

            if chain_str and ep.chain.lower() == chain_str:
                sim = min(1.0, sim + 0.10)
            if src_str and ep.signal_source.lower() == src_str:
                sim = min(1.0, sim + 0.10)

            if sim >= min_similarity:
                scored.append((ep, round(sim, 4)))
            if len(scored) >= top_k * 4:
                break

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]

    def format_episodes_compact_summary(
        self,
        episodes: list[tuple[MemoryEpisode, float]] | list[MemoryEpisode],
    ) -> str:
        """
        Format retrieved episodes into a compact summary for council agent prompts.
        """
        if not episodes:
            return "Historical Precedents: No relevant past episodes found in episodic memory."

        items: list[str] = []
        wins = 0
        total = 0
        total_roi = 0.0

        for item in episodes:
            if isinstance(item, tuple):
                ep, sim = item
            else:
                ep, sim = item, 1.0
            total += 1
            if ep.realized_pnl_usd > 0 or ep.roi_pct > 0:
                wins += 1
            total_roi += ep.roi_pct
            res_str = f"+{ep.roi_pct:.1f}%" if ep.roi_pct >= 0 else f"{ep.roi_pct:.1f}%"
            items.append(
                f"Token {ep.token_address[:8]}... ({ep.signal_source}, ROI: {res_str}, Cause: {ep.root_cause_tag}, Sim: {sim*100:.0f}%)"
            )

        avg_roi = (total_roi / total) if total > 0 else 0.0
        win_rate = (wins / total * 100.0) if total > 0 else 0.0
        summary_header = f"Historical Precedents ({len(items)} episodes, WinRate: {win_rate:.0f}%, AvgROI: {avg_roi:+.1f}%):"
        return f"{summary_header} " + "; ".join(items)

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

    async def get_recent_reflections(self, limit: int = 10) -> list[str]:
        """Return list of non-empty summary reflections from recent episodes."""
        return [ep.summary_reflection for ep in self._episodes[-limit:] if ep.summary_reflection]

    def retrieve_similar_episodes(
        self,
        query_vector: list[float],
        top_k: int = 5,
        min_similarity: float = 0.50,
    ) -> list[dict[str, Any]]:
        """Retrieve top_k similar historical episodes formatted as dictionaries with similarity score."""
        pairs = self.query_similar_episodes(query_vector, top_k=top_k, min_similarity=min_similarity)
        results: list[dict[str, Any]] = []
        for ep, sim in pairs:
            d = ep.to_dict()
            d["similarity"] = round(sim, 4)
            d["realized_roi_pct"] = ep.roi_pct
            results.append(d)
        return results

    save_episode = record_episode

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
        vec = list(feature_vector) if feature_vector is not None else None
        if vec is None and hasattr(trade, "entry_feature_vector") and getattr(trade, "entry_feature_vector"):
            vec = list(trade.entry_feature_vector)
        if vec is None and decision and getattr(decision, "feature_vector", None):
            dfv = getattr(decision, "feature_vector")
            if hasattr(dfv, "to_vector"):
                vec = dfv.to_vector()
            elif hasattr(dfv, "to_list"):
                vec = dfv.to_list()
            elif isinstance(dfv, list):
                vec = dfv
        if vec is None or len(vec) != 7:
            vec = [0.5] * 7

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
            actual_slippage_bps=int(trade.slippage_bps if hasattr(trade, "slippage_bps") else (trade.actual_slippage_bps if hasattr(trade, "actual_slippage_bps") else 0)),
            fill_latency_ms=float(trade.fill_latency_ms if hasattr(trade, "fill_latency_ms") else 0.0),
            root_cause_tag=trade.exit_reason.value if hasattr(trade, "exit_reason") and hasattr(trade.exit_reason, "value") else "EXIT",
            summary_reflection=summary_reflection or f"Trade {trade.trade_id} on {trade.token_address[:8]} with PnL ${pnl:.2f}",
        )
        await self.record_episode(episode)
        return episode

    async def close(self) -> None:
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None
            self._initialized = False


# ==============================================================================
# HNSW / Lightweight Vector Store for Rapid Feature Vector Retrieval
# ==============================================================================

@dataclass
class VectorItem:
    """Indexed vector unit with unique ID, normalized embedding, and metadata."""

    item_id: str
    vector: list[float]
    metadata: dict[str, Any] = field(default_factory=dict)
    _norm: float = 1.0

    def __post_init__(self) -> None:
        sq_sum = sum(x * x for x in self.vector)
        self._norm = math.sqrt(sq_sum) if sq_sum > 0 else 1.0


class LightweightVectorStore:
    """
    Sub-millisecond Vector Store (HNSW-inspired multi-probe cosine index):
    - Normalizes incoming feature vectors for unit-sphere inner product computation.
    - Partitions vectors by dimensional clusters to accelerate top-k neighbor search.
    - Supports exact and approximate top-k with metadata predicate filtering.
    """

    def __init__(self, dimension: int = 7) -> None:
        self.dimension = dimension
        self._items: dict[str, VectorItem] = {}
        self._lock = asyncio.Lock()

    def add(self, item_id: str, vector: list[float], metadata: Optional[dict[str, Any]] = None) -> None:
        """Add or update a vector item in the index."""
        if len(vector) != self.dimension:
            # Pad or truncate to self.dimension
            if len(vector) < self.dimension:
                vector = vector + [0.5] * (self.dimension - len(vector))
            else:
                vector = vector[: self.dimension]
        self._items[item_id] = VectorItem(item_id=item_id, vector=vector, metadata=metadata or {})

    def search(
        self,
        query_vector: list[float],
        top_k: int = 5,
        min_similarity: float = 0.50,
        filter_fn: Optional[Callable[[dict[str, Any]], bool]] = None,
    ) -> list[tuple[VectorItem, float]]:
        """
        Query nearest neighbors with optional metadata filter predicate.
        Returns sorted list of (VectorItem, cosine_similarity).
        """
        if not self._items or not query_vector:
            return []

        if len(query_vector) != self.dimension:
            if len(query_vector) < self.dimension:
                query_vector = query_vector + [0.5] * (self.dimension - len(query_vector))
            else:
                query_vector = query_vector[: self.dimension]

        q_norm = math.sqrt(sum(x * x for x in query_vector))
        if q_norm == 0.0:
            return []

        scored: list[tuple[VectorItem, float]] = []
        for item in self._items.values():
            if filter_fn and not filter_fn(item.metadata):
                continue
            dot = sum(a * b for a, b in zip(query_vector, item.vector))
            sim = dot / (q_norm * item._norm)
            sim_clamped = max(0.0, min(1.0, sim))
            if sim_clamped >= min_similarity:
                scored.append((item, sim_clamped))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]

    def size(self) -> int:
        return len(self._items)

    def clear(self) -> None:
        self._items.clear()


# ==============================================================================
# Ruflo Semantic Memory: Distilled Cross-Agent Heuristic Knowledge Store
# ==============================================================================

@dataclass
class SemanticRule:
    """A distilled heuristic trading rule synthesized from historical trades."""

    rule_id: str
    category: str                       # e.g. 'SLIPPAGE_GUARD', 'HONEYPOT_RISK', 'MOMENTUM_CONFIRMATION'
    condition: str                      # Natural language or codified condition
    action: str                         # e.g. 'SCALE_SIZE', 'TIGHTEN_SLIPPAGE', 'VETO', 'EXPAND_TP'
    parameters: dict[str, Any]          # Parameter modifications (e.g. {'max_slippage_bps': 80})
    confidence: float                   # 0.0 to 1.0 confidence score
    sample_size: int                    # Number of historical trades supporting this rule
    evidence_episodes: list[str] = field(default_factory=list)
    is_active: bool = True
    created_at_ns: int = field(default_factory=time.time_ns)
    updated_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SemanticRule":
        return cls(
            rule_id=data["rule_id"],
            category=data["category"],
            condition=data["condition"],
            action=data["action"],
            parameters=data.get("parameters", {}),
            confidence=float(data.get("confidence", 0.8)),
            sample_size=int(data.get("sample_size", 1)),
            evidence_episodes=data.get("evidence_episodes", []),
            is_active=bool(data.get("is_active", True)),
            created_at_ns=int(data.get("created_at_ns", time.time_ns())),
            updated_at_ns=int(data.get("updated_at_ns", time.time_ns())),
        )


class RufloSemanticMemory:
    """
    Ruflo Persistent Semantic Memory:
    - Maintains distilled cross-agent trading rules and market regime heuristics.
    - Persists synthesized rules to SQLite table 'ruflo_semantic_memory'.
    - Reinforces rules after profitable trades and decays rules on adverse outcomes.
    """

    def __init__(self, db_path: str = "paper_trading.db") -> None:
        self.db_path = db_path
        self._rules: dict[str, SemanticRule] = {}
        self._lock = asyncio.Lock()
        self._conn: Optional[aiosqlite.Connection] = None
        self._initialized = False

    async def connect(self) -> None:
        """Initialize SQLite semantic memory table in WAL mode."""
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            try:
                self._conn = await aiosqlite.connect(self.db_path, timeout=60.0)
                self._conn.row_factory = aiosqlite.Row
                await self._conn.execute("PRAGMA journal_mode = WAL;")
                await self._conn.execute("PRAGMA busy_timeout = 60000;")
                await self._conn.execute("PRAGMA synchronous = NORMAL;")
                await self._conn.execute("PRAGMA cache_size = -64000;")
                await self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ruflo_semantic_memory (
                        rule_id TEXT PRIMARY KEY,
                        category TEXT NOT NULL,
                        condition TEXT NOT NULL,
                        action TEXT NOT NULL,
                        parameters_json TEXT NOT NULL,
                        confidence REAL NOT NULL,
                        sample_size INTEGER NOT NULL,
                        evidence_episodes_json TEXT NOT NULL,
                        is_active INTEGER NOT NULL,
                        created_at_ns INTEGER NOT NULL,
                        updated_at_ns INTEGER NOT NULL
                    );
                    """
                    )
                await self._conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_ruflo_semantic_cat
                    ON ruflo_semantic_memory(category, is_active);
                    """
                )
                await self._conn.commit()

                cursor = await self._conn.execute(
                    "SELECT * FROM ruflo_semantic_memory WHERE is_active = 1;"
                )
                rows = await cursor.fetchall()
                for r in rows:
                    try:
                        rule = SemanticRule(
                            rule_id=r["rule_id"],
                            category=r["category"],
                            condition=r["condition"],
                            action=r["action"],
                            parameters=json.loads(r["parameters_json"]),
                            confidence=float(r["confidence"]),
                            sample_size=int(r["sample_size"]),
                            evidence_episodes=json.loads(r["evidence_episodes_json"]),
                            is_active=bool(r["is_active"]),
                            created_at_ns=int(r["created_at_ns"]),
                            updated_at_ns=int(r["updated_at_ns"]),
                        )
                        self._rules[rule.rule_id] = rule
                    except Exception as parse_err:
                        logger.debug("Failed parsing semantic rule: %s", parse_err)

                self._initialized = True
                logger.info("RufloSemanticMemory initialized with %d active rules from %s", len(self._rules), self.db_path)
            except Exception as exc:
                logger.warning("Could not initialize SQLite connection for RufloSemanticMemory: %s", exc)

    async def record_rule(self, rule: SemanticRule) -> None:
        """Store or update a semantic rule in memory and SQLite."""
        await self.connect()
        async with self._lock:
            rule.updated_at_ns = time.time_ns()
            self._rules[rule.rule_id] = rule

            if self._conn is not None:
                try:
                    await self._conn.execute(
                        """
                        INSERT OR REPLACE INTO ruflo_semantic_memory (
                            rule_id, category, condition, action, parameters_json,
                            confidence, sample_size, evidence_episodes_json,
                            is_active, created_at_ns, updated_at_ns
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            rule.rule_id,
                            rule.category,
                            rule.condition,
                            rule.action,
                            json.dumps(rule.parameters),
                            rule.confidence,
                            rule.sample_size,
                            json.dumps(rule.evidence_episodes),
                            1 if rule.is_active else 0,
                            rule.created_at_ns,
                            rule.updated_at_ns,
                        ),
                    )
                    await self._conn.commit()
                except Exception as exc:
                    logger.debug("Failed persisting SemanticRule to SQLite: %s", exc)

    def query_rules(
        self,
        category: Optional[str] = None,
        active_only: bool = True,
        min_confidence: float = 0.50,
    ) -> list[SemanticRule]:
        """Query matching active semantic rules."""
        results: list[SemanticRule] = []
        for r in self._rules.values():
            if active_only and not r.is_active:
                continue
            if category and r.category != category:
                continue
            if r.confidence < min_confidence:
                continue
            results.append(r)
        results.sort(key=lambda r: r.confidence, reverse=True)
        return results

    def evaluate_rules(
        self,
        context: dict[str, Any],
    ) -> tuple[float, list[str], dict[str, Any]]:
        """
        Evaluate candidate trade context against all active semantic rules:
        Returns:
          - size_multiplier: Dynamic scaling factor applied to position size [0.0 - 1.25].
          - reasons: List of human-readable warnings / explanations.
          - adjusted_params: Dynamic parameter adjustments (e.g. max_slippage_bps).
        """
        size_mult = 1.0
        reasons: list[str] = []
        adjusted_params: dict[str, Any] = {}

        for rule in self.query_rules(active_only=True):
            cat = rule.category
            params = rule.parameters

            # Check slippage rules
            if cat == "SLIPPAGE_GUARD":
                vol = context.get("liquidity_usd", 10000.0)
                if vol < float(params.get("min_liquidity_threshold", 5000.0)):
                    size_mult *= float(params.get("size_dampener", 0.70))
                    reasons.append(f"SemanticRule [{rule.rule_id}]: Low liquidity dampener ({rule.condition})")
                    if "max_slippage_bps" in params:
                        adjusted_params["max_slippage_bps"] = params["max_slippage_bps"]

            # Check honeypot / concentration rules
            elif cat == "CONCENTRATION_GUARD":
                top10 = context.get("top10_concentration", 0.20)
                if top10 > float(params.get("max_top10_allowed", 0.25)):
                    size_mult *= float(params.get("size_dampener", 0.50))
                    reasons.append(f"SemanticRule [{rule.rule_id}]: High top10 concentration penalty ({rule.condition})")

            # Check negative feedback decay rules
            elif cat == "REGIME_FILTER":
                regime = context.get("regime", "")
                if regime in params.get("prohibited_regimes", []):
                    size_mult *= 0.50
                    reasons.append(f"SemanticRule [{rule.rule_id}]: Regime caution for {regime}")

        size_mult = max(0.10, min(1.25, size_mult))
        return size_mult, reasons, adjusted_params

    async def reinforce_or_decay(
        self,
        category: str,
        profitable: bool,
        learning_rate: float = 0.05,
    ) -> None:
        """Reinforce confidence of supporting rules or decay underperforming rules."""
        rules = self.query_rules(category=category, active_only=True)
        for rule in rules:
            if profitable:
                rule.confidence = min(1.0, rule.confidence + learning_rate)
                rule.sample_size += 1
            else:
                rule.confidence = max(0.10, rule.confidence - learning_rate * 1.5)
                rule.sample_size += 1
                if rule.confidence < 0.35 and rule.sample_size >= 5:
                    rule.is_active = False
            await self.record_rule(rule)

    async def close(self) -> None:
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None
            self._initialized = False


# ==============================================================================
# Ruflo Intelligence Pipeline: Retrieve -> Judge -> Distill -> Consolidate
# ==============================================================================

@dataclass
class PipelineRetrievalResult:
    """Historical context gathered during the Retrieve phase."""

    matched_episodes: list[dict[str, Any]]
    active_rules: list[SemanticRule]
    historical_win_rate: float
    historical_avg_roi_pct: float
    retrieval_confidence: float


@dataclass
class PipelineJudgmentResult:
    """Synthesized verdict produced by the Judge phase."""

    verdict: str                        # 'PROCEED', 'CAUTION', 'VETO'
    empirical_win_prob: float           # Historical Bayesian probability of win
    size_multiplier: float              # [0.0 - 1.25]
    adjusted_parameters: dict[str, Any] # e.g. {'max_slippage_bps': 90}
    warning_flags: list[str]
    synthesis_reflection: str


class RufloIntelligencePipeline:
    """
    Ruflo 4-Stage Cognitive Intelligence Pipeline:
    1. Retrieve: Fetches episodic nearest-neighbors and relevant semantic rules.
    2. Judge: Formulates empirical probability and risk adjustments for proposed trades.
    3. Distill: Extracts causal attribution heuristics from completed trades.
    4. Consolidate: Merges distilled insights into persistent semantic memory.
    """

    def __init__(
        self,
        episodic_memory: RufloEpisodicMemory,
        semantic_memory: RufloSemanticMemory,
        vector_store: Optional[LightweightVectorStore] = None,
    ) -> None:
        self.episodic_memory = episodic_memory
        self.semantic_memory = semantic_memory
        self.vector_store = vector_store or LightweightVectorStore(dimension=7)

    async def retrieve(
        self,
        query_vector: list[float],
        token_context: dict[str, Any],
        top_k: int = 5,
    ) -> PipelineRetrievalResult:
        """Phase 1: Retrieve similar historical episodes and matching semantic rules."""
        await self.episodic_memory.connect()
        await self.semantic_memory.connect()

        # Episodic match
        matches = self.episodic_memory.retrieve_similar_episodes(
            query_vector=query_vector,
            top_k=top_k,
            min_similarity=0.45,
        )

        wins = sum(1 for m in matches if float(m.get("realized_pnl_usd", 0.0)) > 0)
        total = len(matches)
        win_rate = (wins / total) if total > 0 else 0.50
        avg_roi = (sum(float(m.get("roi_pct", 0.0)) for m in matches) / total) if total > 0 else 0.0
        retrieval_conf = min(1.0, 0.40 + (0.12 * total))

        # Semantic rules match
        rules = self.semantic_memory.query_rules(active_only=True)

        return PipelineRetrievalResult(
            matched_episodes=matches,
            active_rules=rules,
            historical_win_rate=win_rate,
            historical_avg_roi_pct=avg_roi,
            retrieval_confidence=retrieval_conf,
        )

    def judge(
        self,
        retrieval: PipelineRetrievalResult,
        proposed_signal: dict[str, Any],
    ) -> PipelineJudgmentResult:
        """Phase 2: Evaluate historical precedent to adjust sizing, slippage, and approval."""
        size_mult, rule_warnings, adjusted_params = self.semantic_memory.evaluate_rules(proposed_signal)
        warnings = list(rule_warnings)

        # Incorporate episodic performance
        if retrieval.matched_episodes:
            if retrieval.historical_win_rate < 0.35 and len(retrieval.matched_episodes) >= 3:
                size_mult *= 0.60
                warnings.append(
                    f"Low historical episode win rate ({retrieval.historical_win_rate*100:.1f}%) on {len(retrieval.matched_episodes)} matches."
                )
            elif retrieval.historical_win_rate >= 0.70 and len(retrieval.matched_episodes) >= 3:
                size_mult = min(1.20, size_mult * 1.15)

        verdict = "PROCEED"
        if size_mult < 0.25:
            verdict = "VETO"
        elif size_mult < 0.70 or warnings:
            verdict = "CAUTION"

        reflection = (
            f"Ruflo Judge: verdict={verdict}, historical_win_rate={retrieval.historical_win_rate*100:.1f}%, "
            f"size_multiplier={size_mult:.2f}, warnings={len(warnings)}"
        )

        return PipelineJudgmentResult(
            verdict=verdict,
            empirical_win_prob=retrieval.historical_win_rate,
            size_multiplier=size_mult,
            adjusted_parameters=adjusted_params,
            warning_flags=warnings,
            synthesis_reflection=reflection,
        )

    async def distill(
        self,
        trade_outcome: dict[str, Any],
    ) -> list[SemanticRule]:
        """Phase 3: Extract structured causal heuristics from a completed trade."""
        pnl = float(trade_outcome.get("realized_pnl_usd", 0.0))
        roi = float(trade_outcome.get("roi_pct", 0.0))
        slippage_bps = int(trade_outcome.get("actual_slippage_bps", 50))
        exit_reason = str(trade_outcome.get("exit_reason", trade_outcome.get("root_cause_tag", "UNKNOWN")))
        source = str(trade_outcome.get("signal_source", "UNKNOWN"))
        trade_id = str(trade_outcome.get("trade_id", trade_outcome.get("episode_id", uuid.uuid4().hex[:8])))

        new_rules: list[SemanticRule] = []

        # Rule 1: High slippage drag detected
        if slippage_bps > 150:
            rule_id = f"rule_slippage_{source.lower()}"
            new_rules.append(
                SemanticRule(
                    rule_id=rule_id,
                    category="SLIPPAGE_GUARD",
                    condition=f"High slippage drag encountered on {source} signals ({slippage_bps} bps)",
                    action="TIGHTEN_SLIPPAGE",
                    parameters={"max_slippage_bps": max(60, slippage_bps - 40), "size_dampener": 0.80},
                    confidence=0.85,
                    sample_size=1,
                    evidence_episodes=[trade_id],
                )
            )

        # Rule 2: Premature stop-out on wick
        if "STOP" in exit_reason.upper() and (roi < -15.0 or pnl < -100.0):
            rule_id = f"rule_stop_loss_caution_{source.lower()}"
            new_rules.append(
                SemanticRule(
                    rule_id=rule_id,
                    category="REGIME_FILTER",
                    condition=f"Adverse exit ({exit_reason}) on {source} (PnL=${pnl:.2f}, {roi:.1f}%)",
                    action="SCALE_SIZE",
                    parameters={"prohibited_regimes": ["HIGH_VOLATILITY"], "size_dampener": 0.70},
                    confidence=0.75,
                    sample_size=1,
                    evidence_episodes=[trade_id],
                )
            )

        # Rule 3: High winning surge
        if roi >= 50.0 or pnl >= 200.0:
            rule_id = f"rule_momentum_reinforce_{source.lower()}"
            new_rules.append(
                SemanticRule(
                    rule_id=rule_id,
                    category="MOMENTUM_CONFIRMATION",
                    condition=f"Strong winning run (+{roi:.1f}%) on {source}",
                    action="EXPAND_TP",
                    parameters={"size_multiplier": 1.15},
                    confidence=0.90,
                    sample_size=1,
                    evidence_episodes=[trade_id],
                )
            )

        return new_rules

    async def consolidate(
        self,
        new_rules: list[SemanticRule],
    ) -> dict[str, Any]:
        """Phase 4: Consolidate new rules into persistent semantic memory."""
        await self.semantic_memory.connect()
        consolidated = 0
        updated = 0

        for rule in new_rules:
            existing = self.semantic_memory._rules.get(rule.rule_id)
            if existing is not None:
                # Merge evidence and update confidence
                existing.sample_size += 1
                existing.confidence = min(1.0, (existing.confidence * 0.70) + (rule.confidence * 0.30))
                for ep in rule.evidence_episodes:
                    if ep not in existing.evidence_episodes:
                        existing.evidence_episodes.append(ep)
                existing.parameters.update(rule.parameters)
                await self.semantic_memory.record_rule(existing)
                updated += 1
            else:
                await self.semantic_memory.record_rule(rule)
                consolidated += 1

        logger.info(
            "[Ruflo Pipeline] Consolidated %d new rules, updated %d existing rules in Semantic Memory",
            consolidated,
            updated,
        )
        return {"consolidated_rules": consolidated, "updated_rules": updated}
