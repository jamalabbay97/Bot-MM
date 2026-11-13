"""
alpha_engine.observability.recorder — Real-Time Market Tick Recorder
=====================================================================
SQLite-backed high-throughput tick recorder in Write-Ahead Logging (WAL) mode.
Captures:
- Pool states (reserves, spot prices, liquidity, provenance)
- On-chain swaps (in/out, volumes, sender, tx hash)
- Execution quotes (venue, size, impact, effective price, fees)
- Native price samples (SOL/ETH USD samples, sources, status)
- Security reports (risk score, honeypot status, flags)
- Alpha signals (provenance, score, payloads)
- System decisions (vetoes, approvals, sizing, exits)

Designed with a bounded in-memory queue to apply non-blocking backpressure.
If the queue fills, incoming ticks are dropped and counted in drop counters,
guaranteeing that market recording NEVER blocks trade execution.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from alpha_engine.models.enums import ChainIdentifier, ExecutionVenue, OrderSide
from alpha_engine.models.events import SignalEvent, SwapEvent
from alpha_engine.models.state import PoolState, ReserveType, SecurityReport

logger = logging.getLogger(__name__)


class MarketTickRecorder:
    """
    High-performance asynchronous SQLite market tick recorder.
    Maintains bounded backpressure queues and persists in WAL mode.
    """

    def __init__(
        self,
        db_path: str | Path = "market_ticks.db",
        queue_size: int = 10000,
        batch_size: int = 100,
        flush_interval_s: float = 1.0,
        retention_days: int = 7,
    ) -> None:
        self.db_path_obj = Path(db_path)
        self.db_path = str(self.db_path_obj.resolve())
        self.queue_size = max(10, queue_size)
        self.batch_size = max(1, batch_size)
        self.flush_interval_s = max(0.01, flush_interval_s)
        self.retention_days = max(1, retention_days)

        self._queue: Optional[asyncio.Queue[Tuple[str, tuple]]] = None
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self._lock = asyncio.Lock()

        # Telemetry counters
        self._recorded_counts: Dict[str, int] = {
            "pool_ticks": 0,
            "swap_ticks": 0,
            "quote_ticks": 0,
            "native_price_ticks": 0,
            "security_report_ticks": 0,
            "signal_ticks": 0,
            "decision_ticks": 0,
        }
        self._drop_counters: Dict[str, int] = {
            "pool_ticks": 0,
            "swap_ticks": 0,
            "quote_ticks": 0,
            "native_price_ticks": 0,
            "security_report_ticks": 0,
            "signal_ticks": 0,
            "decision_ticks": 0,
        }

    def _init_sqlite_schema(self) -> None:
        """Create database tables and indices in WAL mode."""
        self.db_path_obj.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        try:
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode = WAL;")
            cur.execute("PRAGMA busy_timeout = 30000;")
            cur.execute("PRAGMA synchronous = NORMAL;")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS pool_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ns INTEGER NOT NULL,
                chain TEXT NOT NULL,
                venue TEXT NOT NULL,
                pool_address TEXT NOT NULL,
                token_address TEXT NOT NULL,
                native_reserve TEXT NOT NULL,
                token_reserve TEXT NOT NULL,
                spot_price TEXT NOT NULL,
                liquidity_usd TEXT NOT NULL,
                reserve_type TEXT NOT NULL,
                provenance TEXT NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pool_ticks_ts ON pool_ticks(timestamp_ns);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pool_ticks_token ON pool_ticks(token_address);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pool_ticks_pool ON pool_ticks(pool_address);")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS swap_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ns INTEGER NOT NULL,
                chain TEXT NOT NULL,
                venue TEXT NOT NULL,
                pool_address TEXT NOT NULL,
                token_address TEXT NOT NULL,
                side TEXT NOT NULL,
                native_amount TEXT NOT NULL,
                token_amount TEXT NOT NULL,
                sender TEXT NOT NULL,
                tx_hash TEXT NOT NULL,
                provenance TEXT NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_swap_ticks_ts ON swap_ticks(timestamp_ns);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_swap_ticks_token ON swap_ticks(token_address);")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS quote_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ns INTEGER NOT NULL,
                chain TEXT NOT NULL,
                venue TEXT NOT NULL,
                pool_address TEXT NOT NULL,
                token_address TEXT NOT NULL,
                side TEXT NOT NULL,
                size_native TEXT NOT NULL,
                effective_price TEXT NOT NULL,
                price_impact_bps INTEGER NOT NULL,
                fee_native TEXT NOT NULL,
                network_fee_native TEXT NOT NULL,
                provenance TEXT NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_quote_ticks_ts ON quote_ticks(timestamp_ns);")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS native_price_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ns INTEGER NOT NULL,
                chain TEXT NOT NULL,
                price_usd TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                provenance TEXT NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_native_price_ts ON native_price_ticks(timestamp_ns);")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS security_report_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ns INTEGER NOT NULL,
                chain TEXT NOT NULL,
                token_address TEXT NOT NULL,
                risk_score REAL NOT NULL,
                is_honeypot INTEGER NOT NULL,
                flags_json TEXT NOT NULL,
                provenance TEXT NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sec_report_ts ON security_report_ticks(timestamp_ns);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sec_report_token ON security_report_ticks(token_address);")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS signal_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ns INTEGER NOT NULL,
                signal_id TEXT NOT NULL,
                chain TEXT NOT NULL,
                token_address TEXT NOT NULL,
                source TEXT NOT NULL,
                score REAL NOT NULL,
                payload_json TEXT NOT NULL,
                provenance TEXT NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_ticks_ts ON signal_ticks(timestamp_ns);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_ticks_token ON signal_ticks(token_address);")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS decision_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ns INTEGER NOT NULL,
                decision_id TEXT NOT NULL,
                decision_type TEXT NOT NULL,
                chain TEXT NOT NULL,
                token_address TEXT NOT NULL,
                approved INTEGER NOT NULL,
                reason TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                provenance TEXT NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_decision_ticks_ts ON decision_ticks(timestamp_ns);")

            conn.commit()
        finally:
            conn.close()

    async def start(self) -> None:
        """Start recorder schema, bounded queue, and background writer task."""
        async with self._lock:
            if self._running:
                return
            self._init_sqlite_schema()
            self._queue = asyncio.Queue(maxsize=self.queue_size)
            self._running = True
            self._worker_task = asyncio.create_task(self._worker_loop(), name="market_tick_recorder")
            logger.info("MarketTickRecorder started on %s (queue_size=%d)", self.db_path, self.queue_size)

    async def stop(self) -> None:
        """Gracefully stop recorder and flush remaining items."""
        async with self._lock:
            if not self._running:
                return
            self._running = False
            if self._worker_task:
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass
            await self._drain_queue_sync()
            logger.info("MarketTickRecorder stopped on %s", self.db_path)

    async def flush(self) -> None:
        """Flush currently queued ticks directly to database."""
        await self._drain_queue_sync()

    def _enqueue(self, table: str, row: tuple) -> bool:
        """
        Non-blocking enqueue. If queue is full, increments drop counter and returns False.
        Guarantees trade execution is NEVER blocked.
        """
        if not self._running or self._queue is None:
            return False
        try:
            self._queue.put_nowait((table, row))
            self._recorded_counts[table] = self._recorded_counts.get(table, 0) + 1
            return True
        except asyncio.QueueFull:
            self._drop_counters[table] = self._drop_counters.get(table, 0) + 1
            if self._drop_counters[table] % 100 == 1:
                logger.warning(
                    "MarketTickRecorder: Bounded queue full! Dropped tick for %s (total dropped: %d)",
                    table,
                    self._drop_counters[table],
                )
            return False

    def record_pool_tick(
        self,
        pool: PoolState,
        provenance: str = "ingestion",
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """Record a pool state update tick."""
        ts = timestamp_ns or (int(pool.timestamp_s * 1e9) if getattr(pool, "timestamp_s", None) else time.time_ns())
        venue = getattr(pool, "venue", None)
        venue_str = venue.value if isinstance(venue, ExecutionVenue) else str(venue or "UNKNOWN")
        chain = getattr(pool, "chain", None)
        chain_str = chain.value if isinstance(chain, ChainIdentifier) else str(chain or "solana_mainnet")
        res_type = getattr(pool, "reserve_type", None)
        if isinstance(res_type, ReserveType):
            res_type_str = res_type.value
        else:
            res_type_str = str(getattr(res_type, "value", res_type) or ReserveType.EXACT.value)
        liq = getattr(pool, "liquidity_usd", None) or Decimal(0)

        row = (
            ts,
            chain_str,
            venue_str,
            str(pool.pool_address),
            str(getattr(pool, "token_address", "") or ""),
            str(pool.native_reserve),
            str(pool.token_reserve),
            str(pool.spot_price_native_per_token),
            str(liq),
            res_type_str,
            str(provenance),
        )
        return self._enqueue("pool_ticks", row)

    def record_swap_tick(
        self,
        swap: SwapEvent,
        provenance: str = "ingestion",
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """Record an on-chain swap event tick."""
        ts = timestamp_ns or getattr(swap, "timestamp_ns", None) or time.time_ns()
        venue = getattr(swap, "venue", None)
        venue_str = venue.value if isinstance(venue, ExecutionVenue) else str(venue or "UNKNOWN")
        chain = getattr(swap, "chain", None)
        chain_str = chain.value if isinstance(chain, ChainIdentifier) else str(chain or "solana_mainnet")
        side_str = "BUY" if "So111111" in swap.token_in or "0x4200000000000000000000000000000000000006" in swap.token_in.lower() else "SELL"
        target_token = swap.token_out if side_str == "BUY" else swap.token_in

        row = (
            ts,
            chain_str,
            venue_str,
            str(swap.pool_address),
            str(target_token),
            side_str,
            str(swap.amount_in if side_str == "BUY" else swap.amount_out),
            str(swap.amount_out if side_str == "BUY" else swap.amount_in),
            str(swap.sender or ""),
            str(swap.tx_hash or ""),
            str(provenance),
        )
        return self._enqueue("swap_ticks", row)

    def record_quote_tick(
        self,
        chain: str | ChainIdentifier,
        venue: str | ExecutionVenue,
        pool_address: str,
        token_address: str,
        side: str | OrderSide,
        size_native: Decimal,
        effective_price: Decimal,
        price_impact_bps: int,
        fee_native: Decimal,
        network_fee_native: Decimal,
        provenance: str = "cost_model",
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """Record an execution quote tick."""
        ts = timestamp_ns or time.time_ns()
        chain_str = chain.value if isinstance(chain, ChainIdentifier) else str(chain)
        venue_str = venue.value if isinstance(venue, ExecutionVenue) else str(venue)
        side_str = side.value if isinstance(side, OrderSide) else str(side)
        row = (
            ts,
            chain_str,
            venue_str,
            str(pool_address),
            str(token_address),
            side_str,
            str(size_native),
            str(effective_price),
            int(price_impact_bps),
            str(fee_native),
            str(network_fee_native),
            str(provenance),
        )
        return self._enqueue("quote_ticks", row)

    def record_native_price_tick(
        self,
        chain: str | ChainIdentifier,
        price_usd: Decimal,
        source: str,
        status: str,
        provenance: str = "price_service",
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """Record a native asset USD price sample tick."""
        ts = timestamp_ns or time.time_ns()
        chain_str = chain.value if isinstance(chain, ChainIdentifier) else str(chain)
        row = (
            ts,
            chain_str,
            str(price_usd),
            str(source),
            str(status),
            str(provenance),
        )
        return self._enqueue("native_price_ticks", row)
        row = (
            ts,
            str(chain),
            str(price_usd),
            str(source),
            str(status),
            str(provenance),
        )
        return self._enqueue("native_price_ticks", row)

    def record_security_report_tick(
        self,
        report: SecurityReport,
        provenance: str = "gatekeeper",
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """Record a token preflight security audit report tick."""
        ts = timestamp_ns or time.time_ns()
        chain = getattr(report, "chain", None)
        chain_str = chain.value if hasattr(chain, "value") else str(chain or "")
        flags_data = {
            "mint_authority_disabled": getattr(report, "mint_authority_disabled", True),
            "freeze_authority_disabled": getattr(report, "freeze_authority_disabled", True),
            "lp_burned_ratio": float(getattr(report, "lp_burned_ratio", 1.0)),
            "top10_concentration": float(getattr(report, "top10_concentration", 0.0)),
            "warning_flags": getattr(report, "warning_flags", []),
        }
        row = (
            ts,
            chain_str,
            str(report.token_address),
            float(getattr(report, "score", 0.0)),
            1 if getattr(report, "is_honeypot", False) else 0,
            json.dumps(flags_data),
            str(provenance),
        )
        return self._enqueue("security_report_ticks", row)

    def record_signal_tick(
        self,
        signal: SignalEvent,
        provenance: str = "signal_engine",
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """Record an alpha screening signal tick."""
        ts = timestamp_ns or getattr(signal, "timestamp_ns", None) or time.time_ns()
        chain = getattr(signal, "chain", None)
        chain_str = chain.value if hasattr(chain, "value") else str(chain or "")
        src = getattr(signal, "source", None)
        src_str = src.value if hasattr(src, "value") else str(src or "")
        side = getattr(signal, "suggested_side", getattr(signal, "side", None))
        side_str = side.value if hasattr(side, "value") else str(side or "BUY")
        strength = getattr(signal, "strength", None)
        strength_str = strength.value if hasattr(strength, "value") else str(strength or "")
        alpha_score = float(getattr(signal, "alpha_score", getattr(signal, "score", 0.0)))
        payload = {
            "side": side_str,
            "strength": strength_str,
            "pool_address": str(getattr(signal, "pool_address", "")),
            "exit_profile": str(getattr(getattr(signal, "exit_profile", None), "value", getattr(signal, "exit_profile", ""))),
            "strategy_pattern": getattr(signal, "strategy_pattern", None),
            "strategy_name": getattr(signal, "strategy_name", None),
        }
        row = (
            ts,
            str(signal.signal_id),
            chain_str,
            str(signal.token_address),
            src_str,
            alpha_score,
            json.dumps(payload),
            str(provenance),
        )
        return self._enqueue("signal_ticks", row)

    def record_decision_tick(
        self,
        decision_id: str,
        decision_type: str,
        chain: str | ChainIdentifier,
        token_address: str,
        approved: bool,
        reason: str,
        metrics: Optional[dict[str, Any]] = None,
        provenance: str = "council",
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """Record a structured trading decision tick (approval, veto, sizing)."""
        ts = timestamp_ns or time.time_ns()
        chain_str = chain.value if isinstance(chain, ChainIdentifier) else str(chain)
        row = (
            ts,
            str(decision_id),
            str(decision_type),
            chain_str,
            str(token_address),
            1 if approved else 0,
            str(reason),
            json.dumps(metrics or {}),
            str(provenance),
        )
        return self._enqueue("decision_ticks", row)

    async def _worker_loop(self) -> None:
        """Background worker batching queued records into SQLite transactions."""
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval_s)
                await self._drain_queue_sync()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("MarketTickRecorder worker error: %s", e)

    async def _drain_queue_sync(self) -> None:
        """Drain queue items and write in batch transactions to SQLite."""
        if self._queue is None or self._queue.empty():
            return

        batches: Dict[str, List[tuple]] = {
            "pool_ticks": [],
            "swap_ticks": [],
            "quote_ticks": [],
            "native_price_ticks": [],
            "security_report_ticks": [],
            "signal_ticks": [],
            "decision_ticks": [],
        }

        # Drain available items up to batch size
        count = 0
        while not self._queue.empty() and count < (self.batch_size * 10):
            try:
                table, row = self._queue.get_nowait()
                if table in batches:
                    batches[table].append(row)
                count += 1
            except asyncio.QueueEmpty:
                break

        if count == 0:
            return

        # Write to SQLite in thread executor to prevent blocking the event loop
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._write_batches_to_db, batches)

    def _write_batches_to_db(self, batches: Dict[str, List[tuple]]) -> None:
        """Synchronous write of prepared batches."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        try:
            cur = conn.cursor()
            cur.execute("PRAGMA synchronous = NORMAL;")

            if batches["pool_ticks"]:
                cur.executemany(
                    "INSERT INTO pool_ticks (timestamp_ns, chain, venue, pool_address, token_address, native_reserve, token_reserve, spot_price, liquidity_usd, reserve_type, provenance) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    batches["pool_ticks"],
                )
            if batches["swap_ticks"]:
                cur.executemany(
                    "INSERT INTO swap_ticks (timestamp_ns, chain, venue, pool_address, token_address, side, native_amount, token_amount, sender, tx_hash, provenance) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    batches["swap_ticks"],
                )
            if batches["quote_ticks"]:
                cur.executemany(
                    "INSERT INTO quote_ticks (timestamp_ns, chain, venue, pool_address, token_address, side, size_native, effective_price, price_impact_bps, fee_native, network_fee_native, provenance) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    batches["quote_ticks"],
                )
            if batches["native_price_ticks"]:
                cur.executemany(
                    "INSERT INTO native_price_ticks (timestamp_ns, chain, price_usd, source, status, provenance) VALUES (?, ?, ?, ?, ?, ?);",
                    batches["native_price_ticks"],
                )
            if batches["security_report_ticks"]:
                cur.executemany(
                    "INSERT INTO security_report_ticks (timestamp_ns, chain, token_address, risk_score, is_honeypot, flags_json, provenance) VALUES (?, ?, ?, ?, ?, ?, ?);",
                    batches["security_report_ticks"],
                )
            if batches["signal_ticks"]:
                cur.executemany(
                    "INSERT INTO signal_ticks (timestamp_ns, signal_id, chain, token_address, source, score, payload_json, provenance) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                    batches["signal_ticks"],
                )
            if batches["decision_ticks"]:
                cur.executemany(
                    "INSERT INTO decision_ticks (timestamp_ns, decision_id, decision_type, chain, token_address, approved, reason, metrics_json, provenance) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    batches["decision_ticks"],
                )
            conn.commit()
        except Exception as exc:
            logger.error("MarketTickRecorder DB commit failure on %s: %s", self.db_path, exc)
        finally:
            conn.close()

    def prune_older_than(self, days: Optional[int] = None) -> int:
        """Prune records older than specified days (default: self.retention_days)."""
        retention = days or self.retention_days
        cutoff_ns = time.time_ns() - int(retention * 86400 * 1e9)
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        total_pruned = 0
        try:
            cur = conn.cursor()
            for tbl in ["pool_ticks", "swap_ticks", "quote_ticks", "native_price_ticks", "security_report_ticks", "signal_ticks", "decision_ticks"]:
                cur.execute(f"DELETE FROM {tbl} WHERE timestamp_ns < ?;", (cutoff_ns,))
                total_pruned += cur.rowcount
            conn.commit()
            logger.info("MarketTickRecorder: Pruned %d records older than %d days from %s", total_pruned, retention, self.db_path)
            return total_pruned
        finally:
            conn.close()

    def get_stats(self) -> Dict[str, Any]:
        """Return operational telemetry for the tick recorder."""
        q_depth = self._queue.qsize() if self._queue else 0
        db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
        total_dropped = sum(self._drop_counters.values())
        return {
            "db_path": self.db_path,
            "running": self._running,
            "queue_depth": q_depth,
            "queue_maxsize": self.queue_size,
            "db_size_bytes": db_size,
            "recorded_ticks_total": sum(self._recorded_counts.values()),
            "dropped_ticks_total": total_dropped,
            "recorded_by_table": dict(self._recorded_counts),
            "dropped_by_table": dict(self._drop_counters),
        }

    # -------------------------------------------------------------------------
    # Replay & Model-vs-Reality Data Retrieval Methods
    # -------------------------------------------------------------------------
    def get_pool_ticks(
        self,
        token_address: Optional[str] = None,
        pool_address: Optional[str] = None,
        start_ns: Optional[int] = None,
        end_ns: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve historical pool state ticks."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            query = "SELECT * FROM pool_ticks WHERE 1=1"
            params: list[Any] = []
            if token_address:
                query += " AND token_address = ?"
                params.append(token_address)
            if pool_address:
                query += " AND pool_address = ?"
                params.append(pool_address)
            if start_ns:
                query += " AND timestamp_ns >= ?"
                params.append(start_ns)
            if end_ns:
                query += " AND timestamp_ns <= ?"
                params.append(end_ns)
            query += " ORDER BY timestamp_ns ASC;"
            cur.execute(query, params)
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_observed_market_state(
        self,
        token_address: str,
        target_timestamp_ns: int,
        max_window_ns: int = 5_000_000_000,
    ) -> Optional[Dict[str, Any]]:
        """
        Find observed market pool tick for token at or immediately after target timestamp.
        Used by Model-vs-Reality reporter to compare simulated fill price vs real market price.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            # 1. Search for tick at or immediately after target_timestamp_ns
            cur.execute(
                """
                SELECT * FROM pool_ticks
                WHERE token_address = ?
                  AND timestamp_ns >= ?
                  AND timestamp_ns <= ?
                ORDER BY timestamp_ns ASC
                LIMIT 1;
                """,
                (token_address, target_timestamp_ns, target_timestamp_ns + max_window_ns),
            )
            row = cur.fetchone()
            if row:
                return dict(row)
            # 2. Fallback: closest tick within max_window_ns prior
            cur.execute(
                """
                SELECT * FROM pool_ticks
                WHERE token_address = ?
                  AND timestamp_ns >= ?
                  AND timestamp_ns <= ?
                ORDER BY timestamp_ns DESC
                LIMIT 1;
                """,
                (token_address, target_timestamp_ns - max_window_ns, target_timestamp_ns),
            )
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_ticks_stream(
        self,
        start_ns: Optional[int] = None,
        end_ns: Optional[int] = None,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve chronologically unified event stream for replay:
        returns combined pool states, native prices, and signals ordered by timestamp_ns.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            events: List[Dict[str, Any]] = []

            # 1. Pool ticks
            p_q = "SELECT 'pool' as event_type, id, timestamp_ns, chain, venue, pool_address, token_address, native_reserve, token_reserve, spot_price, liquidity_usd, reserve_type FROM pool_ticks WHERE 1=1"
            p_params: list[Any] = []
            if start_ns:
                p_q += " AND timestamp_ns >= ?"
                p_params.append(start_ns)
            if end_ns:
                p_q += " AND timestamp_ns <= ?"
                p_params.append(end_ns)
            p_q += f" ORDER BY timestamp_ns ASC LIMIT {limit};"
            cur.execute(p_q, p_params)
            for r in cur.fetchall():
                events.append(dict(r))

            # 2. Native price ticks
            np_q = "SELECT 'native_price' as event_type, id, timestamp_ns, chain, price_usd, source, status FROM native_price_ticks WHERE 1=1"
            np_params: list[Any] = []
            if start_ns:
                np_q += " AND timestamp_ns >= ?"
                np_params.append(start_ns)
            if end_ns:
                np_q += " AND timestamp_ns <= ?"
                np_params.append(end_ns)
            np_q += f" ORDER BY timestamp_ns ASC LIMIT {limit};"
            cur.execute(np_q, np_params)
            for r in cur.fetchall():
                events.append(dict(r))

            # 3. Signal ticks
            s_q = "SELECT 'signal' as event_type, id, timestamp_ns, signal_id, chain, token_address, source, score, payload_json FROM signal_ticks WHERE 1=1"
            s_params: list[Any] = []
            if start_ns:
                s_q += " AND timestamp_ns >= ?"
                s_params.append(start_ns)
            if end_ns:
                s_q += " AND timestamp_ns <= ?"
                s_params.append(end_ns)
            s_q += f" ORDER BY timestamp_ns ASC LIMIT {limit};"
            cur.execute(s_q, s_params)
            for r in cur.fetchall():
                d = dict(r)
                try:
                    d["payload"] = json.loads(d["payload_json"])
                except Exception:
                    d["payload"] = {}
                events.append(d)

            # Sort unified events strictly by timestamp
            events.sort(key=lambda x: x["timestamp_ns"])
            return events[:limit]
        finally:
            conn.close()
