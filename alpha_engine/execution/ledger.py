"""
alpha_engine.execution.ledger — SQLite Ledger with WAL Mode Persistence
=======================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiosqlite + asyncio
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import math
import random
import sqlite3
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional, TypeVar

import aiosqlite

from alpha_engine.models.decisions import DecisionRecord, DecisionType, PatternFeatureVector
from alpha_engine.models.enums import LotStatus, TradeExitReason
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PortfolioSnapshot, TradeRecord

logger = logging.getLogger(__name__)

_DB_INIT_TIMEOUT_S = 10.0
_SNAPSHOT_INTERVAL_S = 60.0

F = TypeVar("F", bound=Callable[..., Coroutine[Any, Any, Any]])


def sqlite_retry(max_retries: int = 5, base_delay: float = 0.1) -> Callable[[F], F]:
    """
    Async retry decorator with exponential backoff and jitter
    specifically catching sqlite3.OperationalError when the database is locked or busy.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except sqlite3.OperationalError as exc:
                    err_msg = str(exc).lower()
                    if ("locked" in err_msg or "busy" in err_msg) and attempt < max_retries:
                        last_exc = exc
                        jitter = random.uniform(0.5, 1.5)
                        delay = base_delay * (2 ** attempt) * jitter
                        logger.warning(
                            "SQLite busy/locked in %s (attempt %d/%d), retrying in %.3fs: %s",
                            getattr(func, "__name__", str(func)),
                            attempt + 1,
                            max_retries,
                            exc,
                        )
                        await asyncio.sleep(delay)
                    else:
                        raise
            if last_exc is not None:
                raise last_exc
        return wrapper  # type: ignore[return-value]
    return decorator


class SQLiteLedger:
    """
    Persistent WAL-mode SQLite ledger for all engine state.
    """

    _CREATE_TRADES = """
    CREATE TABLE IF NOT EXISTS trades (
        trade_id            TEXT PRIMARY KEY,
        order_id            TEXT NOT NULL,
        signal_id           TEXT NOT NULL,
        chain               TEXT NOT NULL,
        token_address       TEXT NOT NULL,
        pool_address        TEXT NOT NULL,
        side                TEXT NOT NULL CHECK(side IN ('buy','sell')),
        native_spent        TEXT NOT NULL,
        tokens_delta        TEXT NOT NULL,
        effective_price     TEXT NOT NULL,
        price_impact_bps    INTEGER NOT NULL,
        gas_cost_usd        TEXT NOT NULL,
        fill_latency_ms     INTEGER NOT NULL,
        kelly_fraction      REAL NOT NULL,
        portfolio_equity_usd TEXT NOT NULL,
        realized_pnl_usd    TEXT,
        signal_timestamp_ns INTEGER NOT NULL,
        fill_timestamp_ns   INTEGER NOT NULL,
        created_at          INTEGER NOT NULL
    )
    """

    _CREATE_SIGNALS = """
    CREATE TABLE IF NOT EXISTS signals (
        signal_id           TEXT PRIMARY KEY,
        timestamp_ns        INTEGER NOT NULL,
        chain               TEXT NOT NULL,
        pool_address        TEXT NOT NULL,
        token_address       TEXT NOT NULL,
        suggested_side      TEXT NOT NULL,
        strength            TEXT NOT NULL,
        alpha_score         REAL NOT NULL,
        security_tier       TEXT NOT NULL,
        sell_tax_bps        INTEGER NOT NULL,
        lp_burned_ratio     REAL NOT NULL,
        top10_concentration REAL NOT NULL,
        created_at          INTEGER NOT NULL
    )
    """

    _CREATE_SNAPSHOTS = """
    CREATE TABLE IF NOT EXISTS portfolio_snapshots (
        snapshot_id         TEXT PRIMARY KEY,
        timestamp_ns        INTEGER NOT NULL,
        total_equity_usd    TEXT NOT NULL,
        cash_native_sol     TEXT NOT NULL,
        cash_native_eth     TEXT NOT NULL,
        open_positions      INTEGER NOT NULL,
        realized_pnl_usd    TEXT NOT NULL,
        unrealized_pnl_usd  TEXT NOT NULL,
        max_drawdown_pct    REAL NOT NULL,
        win_rate_pct        REAL NOT NULL,
        profit_factor       REAL NOT NULL,
        total_gas_spent_usd TEXT NOT NULL,
        created_at          INTEGER NOT NULL
    )
    """

    _CREATE_LOT_LIFECYCLE = """
    CREATE TABLE IF NOT EXISTS lot_lifecycle (
        lot_id          TEXT NOT NULL,
        token_address   TEXT NOT NULL,
        chain           TEXT NOT NULL,
        status          TEXT NOT NULL,
        exit_reason     TEXT,
        pnl_usd         TEXT,
        timestamp_ns    INTEGER NOT NULL,
        created_at      INTEGER NOT NULL
    )
    """

    _CREATE_DECISION_AUDIT_LOG = """
    CREATE TABLE IF NOT EXISTS decision_audit_log (
        decision_id             TEXT PRIMARY KEY,
        timestamp_ns            INTEGER NOT NULL,
        token_address           TEXT NOT NULL,
        chain                   TEXT NOT NULL,
        decision_type           TEXT NOT NULL CHECK(decision_type IN ('ENTER','SKIP','EXIT','TRAIL_STOP')),
        strategy_pattern        TEXT NOT NULL,
        market_cap_usd          REAL,
        volume_5m_usd           REAL,
        volume_1h_usd           REAL,
        liquidity_pool_depth_usd REAL,
        active_rules_json       TEXT NOT NULL,
        confidence_score        REAL NOT NULL,
        reason                  TEXT NOT NULL,
        metadata_json           TEXT NOT NULL,
        created_at              INTEGER NOT NULL
    )
    """

    _CREATE_PATTERN_STORE = """
    CREATE TABLE IF NOT EXISTS pattern_feature_store (
        token_address           TEXT PRIMARY KEY,
        consolidation_duration_s REAL NOT NULL,
        dip_depth_pct           REAL NOT NULL,
        volume_surge_multiplier REAL NOT NULL,
        net_buy_delta           REAL NOT NULL,
        top10_concentration     REAL NOT NULL,
        liquidity_to_mc_ratio   REAL NOT NULL,
        smart_wallet_inflows    REAL NOT NULL,
        peak_gain_multiplier    REAL NOT NULL,
        timestamp_ns            INTEGER NOT NULL,
        created_at              INTEGER NOT NULL
    )
    """

    def __init__(
        self,
        db_path: str | Path = "paper_trading.db",
        jsonl_path: Optional[str | Path] = None,
    ) -> None:
        self._db_path = str(db_path)
        self._jsonl_path = Path(jsonl_path) if jsonl_path else Path(self._db_path).with_suffix(".jsonl")
        self._conn: aiosqlite.Connection | None = None

    async def __aenter__(self) -> "SQLiteLedger":
        self._conn = await asyncio.wait_for(
            aiosqlite.connect(self._db_path, timeout=30.0),
            timeout=_DB_INIT_TIMEOUT_S,
        )
        await self._initialise()
        return self

    async def initialize(self) -> "SQLiteLedger":
        """Explicitly open connection and initialize database schema and PRAGMAs."""
        return await self.__aenter__()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def checkpoint(self) -> None:
        """
        Explicitly checkpoint the Write-Ahead Log (WAL) into the main database.
        """
        if self._conn:
            try:
                await self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                await self._conn.commit()
                logger.info("SQLite WAL checkpoint (TRUNCATE) successfully executed.")
            except Exception as exc:
                logger.warning("WAL checkpoint failed: %s", exc)

    async def __aexit__(self, *_: object) -> None:
        if self._conn:
            await self.checkpoint()
            await self._conn.close()
            self._conn = None

    async def close(self) -> None:
        """Explicitly checkpoint and close connection."""
        await self.__aexit__(None, None, None)

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def _initialise(self) -> None:
        conn = self._conn
        assert conn is not None, "Database not connected."
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA busy_timeout = 30000;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA cache_size = -64000;")
        await conn.execute("PRAGMA temp_store = MEMORY;")
        await conn.execute(self._CREATE_TRADES)
        await conn.execute(self._CREATE_SIGNALS)
        await conn.execute(self._CREATE_SNAPSHOTS)
        await conn.execute(self._CREATE_LOT_LIFECYCLE)
        await conn.execute(self._CREATE_DECISION_AUDIT_LOG)
        await conn.execute(self._CREATE_PATTERN_STORE)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_token ON trades(token_address);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_chain ON trades(chain);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(timestamp_ns);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_lot_lifecycle_id ON lot_lifecycle(lot_id);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_lot_lifecycle_status ON lot_lifecycle(status);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_token ON decision_audit_log(token_address);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_type ON decision_audit_log(decision_type);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_ts ON decision_audit_log(timestamp_ns);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_created ON decision_audit_log(created_at);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_pattern_store_ts ON pattern_feature_store(timestamp_ns);")
        await conn.commit()
        logger.info("SQLite ledger initialised at %s (WAL mode)", self._db_path)

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_trade(self, record: TradeRecord) -> None:
        conn = self._conn
        if conn is None:
            logger.debug("SQLiteLedger connection is closed; skipping trade.")
            return
        await conn.execute(
            """
            INSERT INTO trades VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                record.trade_id,
                record.order_id,
                record.signal_id,
                record.chain.value,
                record.token_address,
                record.pool_address,
                record.side.value,
                str(record.native_spent),
                str(record.tokens_delta),
                str(record.effective_price),
                record.price_impact_bps,
                str(record.gas_cost_usd),
                record.fill_latency_ms,
                record.kelly_fraction,
                str(record.portfolio_equity_usd),
                str(record.realized_pnl_usd) if record.realized_pnl_usd is not None else None,
                record.signal_timestamp_ns,
                record.fill_timestamp_ns,
                time.time_ns(),
            ),
        )
        await conn.commit()
        logger.debug("Trade %s recorded in ledger.", record.trade_id[:8])

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_signal(self, signal: SignalEvent) -> None:
        conn = self._conn
        if conn is None:
            logger.debug("SQLiteLedger connection is closed; skipping signal.")
            return
        sec = signal.security_report
        await conn.execute(
            """
            INSERT OR IGNORE INTO signals VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                signal.signal_id,
                signal.timestamp_ns,
                signal.chain.value,
                signal.pool_address,
                signal.token_address,
                signal.suggested_side.value,
                signal.strength.value,
                signal.alpha_score,
                sec.tier.value,
                sec.sell_tax_bps,
                sec.lp_burned_ratio,
                sec.top10_concentration,
                time.time_ns(),
            ),
        )
        await conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_snapshot(self, snapshot: PortfolioSnapshot) -> None:
        conn = self._conn
        if conn is None:
            logger.debug("SQLiteLedger connection is closed; skipping snapshot.")
            return
        pf = snapshot.profit_factor
        pf_stored = pf if math.isfinite(pf) else 999999.0
        await conn.execute(
            """
            INSERT INTO portfolio_snapshots VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                snapshot.snapshot_id,
                snapshot.timestamp_ns,
                str(snapshot.total_equity_usd),
                str(snapshot.cash_native_sol),
                str(snapshot.cash_native_eth),
                snapshot.open_positions,
                str(snapshot.realized_pnl_usd),
                str(snapshot.unrealized_pnl_usd),
                snapshot.max_drawdown_pct,
                snapshot.win_rate_pct,
                pf_stored,
                str(snapshot.total_gas_spent_usd),
                time.time_ns(),
            ),
        )
        await conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_closed_trade_stats(self) -> dict[str, Any]:
        conn = self._conn
        if conn is None:
            return {
                "win_rate": 0.5,
                "avg_win_native": Decimal("0.1"),
                "avg_loss_native": Decimal("0.05"),
                "total_trades": 0,
            }

        async with conn.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN CAST(realized_pnl_usd AS REAL) > 0 THEN 1 ELSE 0 END) as wins,
                AVG(CASE WHEN CAST(realized_pnl_usd AS REAL) > 0
                         THEN ABS(CAST(native_spent AS REAL)) END) as avg_win,
                AVG(CASE WHEN CAST(realized_pnl_usd AS REAL) <= 0
                         THEN ABS(CAST(native_spent AS REAL)) END) as avg_loss
            FROM trades
            WHERE side = 'sell' AND realized_pnl_usd IS NOT NULL
            """
        ) as cursor:
            row = await cursor.fetchone()

        total = row[0] or 0
        wins = row[1] or 0
        avg_win = Decimal(str(row[2])) if row[2] else Decimal("0.1")
        avg_loss = Decimal(str(row[3])) if row[3] else Decimal("0.05")
        win_rate = wins / total if total > 0 else 0.5

        return {
            "win_rate": win_rate,
            "avg_win_native": avg_win,
            "avg_loss_native": avg_loss,
            "total_trades": total,
        }

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_lot_transition(
        self,
        lot_id: str,
        token_address: str,
        chain: str,
        status: LotStatus | str,
        exit_reason: TradeExitReason | str | None = None,
        pnl_usd: Decimal | None = None,
    ) -> None:
        """Record monotonic lot state transition (PENDING_BUY -> OPEN -> PENDING_SELL -> CLOSED -> SETTLED)."""
        conn = self._conn
        if conn is None:
            return
        status_val = status.value if hasattr(status, "value") else str(status)
        reason_val = exit_reason.value if hasattr(exit_reason, "value") else (str(exit_reason) if exit_reason else None)
        await conn.execute(
            """
            INSERT INTO lot_lifecycle VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lot_id,
                token_address,
                chain,
                status_val,
                reason_val,
                str(pnl_usd) if pnl_usd is not None else None,
                time.time_ns(),
                int(time.time()),
            ),
        )
        await conn.commit()
        logger.debug("Recorded lot lifecycle transition: %s -> %s (reason=%s)", lot_id[:8], status_val, reason_val)

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_daily_loss_streak(self) -> int:
        """Count current consecutive loss streak from trades ledger."""
        conn = self._conn
        if conn is None:
            return 0
        async with conn.execute(
            """
            SELECT realized_pnl_usd FROM trades
            WHERE side = 'sell' AND realized_pnl_usd IS NOT NULL
            ORDER BY created_at DESC LIMIT 50
            """
        ) as cursor:
            rows = await cursor.fetchall()
        streak = 0
        for r in rows:
            try:
                pnl = float(r[0])
                if pnl < 0:
                    streak += 1
                else:
                    break
            except Exception:
                break
        return streak

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_rolling_daily_pnl_usd(self) -> Decimal:
        """Calculate rolling 24h realized PnL in USD."""
        conn = self._conn
        if conn is None:
            return Decimal(0)
        day_ago = int(time.time()) - 86400
        async with conn.execute(
            """
            SELECT SUM(CAST(realized_pnl_usd AS REAL)) FROM trades
            WHERE side = 'sell' AND realized_pnl_usd IS NOT NULL AND created_at >= ?
            """,
            (day_ago,),
        ) as cursor:
            row = await cursor.fetchone()
        return Decimal(str(row[0])) if row and row[0] is not None else Decimal(0)

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_daily_drawdown_pct(self) -> float:
        """Calculate maximum rolling daily drawdown percentage from portfolio snapshots."""
        conn = self._conn
        if conn is None:
            return 0.0
        day_ago = int(time.time()) - 86400
        async with conn.execute(
            """
            SELECT total_equity_usd FROM portfolio_snapshots
            WHERE created_at >= ?
            ORDER BY created_at ASC
            """,
            (day_ago,),
        ) as cursor:
            rows = await cursor.fetchall()
        if not rows:
            return 0.0
        equities = [float(r[0]) for r in rows if r[0]]
        if not equities:
            return 0.0
        peak = equities[0]
        max_dd = 0.0
        for eq in equities:
            if eq > peak:
                peak = eq
            if peak > 0:
                dd = (peak - eq) / peak * 100.0
                if dd > max_dd:
                    max_dd = dd
        return max_dd

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_decision(self, record: DecisionRecord) -> None:
        """
        Record an immutable structured decision record into the SQLite ledger
        and asynchronously append to local JSONL for zero-latency audit streaming.
        """
        conn = self._conn
        decision_type_val = (
            record.decision_type.value
            if hasattr(record.decision_type, "value")
            else str(record.decision_type)
        )
        meta_dict = dict(record.metadata or {})
        if record.token_symbol and "token_symbol" not in meta_dict:
            meta_dict["token_symbol"] = record.token_symbol
        if record.signal_id and "signal_id" not in meta_dict:
            meta_dict["signal_id"] = record.signal_id
        if record.pattern_match_score is not None and "pattern_match_score" not in meta_dict:
            meta_dict["pattern_match_score"] = record.pattern_match_score

        active_rules_json = json.dumps(record.active_rules or record.rule_triggers or {})
        metadata_json = json.dumps(meta_dict)

        # 1. Write to SQLite
        if conn is not None:
            try:
                await conn.execute(
                    """
                    INSERT OR REPLACE INTO decision_audit_log VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                    """,
                    (
                        record.decision_id,
                        record.timestamp_ns,
                        record.token_address,
                        record.chain,
                        decision_type_val,
                        record.strategy_pattern,
                        record.market_cap_usd,
                        record.volume_5m_usd,
                        record.volume_1h_usd,
                        record.liquidity_pool_depth_usd,
                        active_rules_json,
                        record.confidence_score,
                        record.reason,
                        metadata_json,
                        record.created_at,
                    ),
                )
                await conn.commit()
            except Exception as exc:
                logger.error("Failed to record decision into SQLite: %s", exc)

        # 2. Append to JSONL audit file
        try:
            line = json.dumps(record.model_dump(mode="json")) + "\n"
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._append_jsonl, line)
        except Exception as exc:
            logger.debug("Failed appending decision to JSONL audit log: %s", exc)

        logger.info(
            "DecisionRecord [%s] %s | Type=%s | Confidence=%.2f | Reason: %s",
            record.decision_id[:8],
            record.token_address[:10],
            decision_type_val,
            record.confidence_score,
            record.reason,
        )

    def _append_jsonl(self, line: str) -> None:
        try:
            self._jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._jsonl_path, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as exc:
            logger.warning("Could not write to jsonl file %s: %s", self._jsonl_path, exc)

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_decisions(
        self,
        token_address: Optional[str] = None,
        decision_type: Optional[DecisionType | str] = None,
        strategy_pattern: Optional[str] = None,
        start_time_ns: Optional[int] = None,
        limit: int = 50,
    ) -> list[DecisionRecord]:
        """Query structured decision records from the ledger."""
        conn = self._conn
        if conn is None:
            return []

        query = "SELECT * FROM decision_audit_log WHERE 1=1"
        params: list[Any] = []

        if token_address:
            query += " AND token_address = ?"
            params.append(token_address)
        if decision_type:
            dt_val = decision_type.value if hasattr(decision_type, "value") else str(decision_type)
            query += " AND decision_type = ?"
            params.append(dt_val)
        if strategy_pattern:
            query += " AND strategy_pattern = ?"
            params.append(strategy_pattern)
        if start_time_ns:
            query += " AND timestamp_ns >= ?"
            params.append(start_time_ns)

        query += " ORDER BY timestamp_ns DESC LIMIT ?"
        params.append(limit)

        async with conn.execute(query, tuple(params)) as cursor:
            rows = await cursor.fetchall()

        results: list[DecisionRecord] = []
        for r in rows:
            try:
                meta = json.loads(r[13]) if r[13] else {}
                rules = json.loads(r[10]) if r[10] else {}
                rec = DecisionRecord(
                    decision_id=r[0],
                    timestamp_ns=r[1],
                    token_address=r[2],
                    chain=r[3],
                    decision_type=DecisionType(r[4]),
                    strategy_pattern=r[5],
                    market_cap_usd=r[6],
                    volume_5m_usd=r[7],
                    volume_1h_usd=r[8],
                    liquidity_pool_depth_usd=r[9],
                    liquidity_usd=r[9],
                    active_rules=rules,
                    rule_triggers=rules,
                    confidence_score=r[11],
                    pattern_match_score=meta.get("pattern_match_score"),
                    token_symbol=meta.get("token_symbol"),
                    signal_id=meta.get("signal_id"),
                    reason=r[12],
                    metadata=meta,
                    created_at=r[14],
                )
                results.append(rec)
            except Exception as exc:
                logger.debug("Error parsing DecisionRecord from SQLite row: %s", exc)

        return results

    async def get_token_decision_history(self, token_address: str, limit: int = 50) -> list[DecisionRecord]:
        """Fetch all chronological decisions for a specific token mint/contract."""
        return await self.get_decisions(token_address=token_address, limit=limit)

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_decision_stats(self, lookback_hours: float = 12.0, hours: Optional[float] = None) -> dict[str, Any]:
        """
        Aggregate decision metrics, strategy win rates, skip vs enter ratios
        over the specified lookback window.
        """
        conn = self._conn
        if conn is None:
            return {
                "total_decisions": 0,
                "total_evaluations": 0,
                "enter_count": 0,
                "skip_count": 0,
                "breakdown": {},
                "strategy_stats": {},
            }

        effective_hours = hours if hours is not None else lookback_hours
        cutoff_ts = int(time.time()) - int(effective_hours * 3600)
        async with conn.execute(
            """
            SELECT decision_type, strategy_pattern, COUNT(*) as cnt
            FROM decision_audit_log
            WHERE created_at >= ?
            GROUP BY decision_type, strategy_pattern
            """,
            (cutoff_ts,),
        ) as cursor:
            rows = await cursor.fetchall()

        total = sum(r[2] for r in rows)
        enter_count = sum(r[2] for r in rows if r[0] == "ENTER")
        skip_count = sum(r[2] for r in rows if r[0] == "SKIP")
        exit_count = sum(r[2] for r in rows if r[0] in ("EXIT", "TRAIL_STOP"))

        # Strategy breakdown
        strat_counts: dict[str, dict[str, int]] = {}
        for r in rows:
            dt, strat, cnt = r[0], r[1], r[2]
            if strat not in strat_counts:
                strat_counts[strat] = {"ENTER": 0, "SKIP": 0, "EXIT": 0, "TRAIL_STOP": 0, "total": 0}
            strat_counts[strat][dt] = strat_counts[strat].get(dt, 0) + cnt
            strat_counts[strat]["total"] += cnt

        # Cross-reference with closed trades for realized win rate by strategy
        async with conn.execute(
            """
            SELECT
                t.signal_id,
                t.token_address,
                CAST(t.realized_pnl_usd AS REAL) as pnl,
                d.strategy_pattern
            FROM trades t
            LEFT JOIN decision_audit_log d ON t.token_address = d.token_address AND d.decision_type = 'ENTER'
            WHERE t.side = 'sell' AND t.realized_pnl_usd IS NOT NULL AND t.created_at >= ?
            """,
            (cutoff_ts,),
        ) as cursor:
            trade_rows = await cursor.fetchall()

        strat_perf: dict[str, dict[str, Any]] = {}
        for r in trade_rows:
            pnl = r[2] or 0.0
            pattern = r[3] or "unclassified"
            if pattern not in strat_perf:
                strat_perf[pattern] = {"trades": 0, "wins": 0, "total_pnl": 0.0}
            strat_perf[pattern]["trades"] += 1
            if pnl > 0:
                strat_perf[pattern]["wins"] += 1
            strat_perf[pattern]["total_pnl"] += pnl

        for pattern, stats in strat_perf.items():
            tr = stats["trades"]
            stats["win_rate_pct"] = round((stats["wins"] / tr * 100.0) if tr > 0 else 0.0, 1)

        return {
            "lookback_hours": lookback_hours,
            "total_decisions": total,
            "total_evaluations": total,
            "enter_count": enter_count,
            "skip_count": skip_count,
            "exit_count": exit_count,
            "breakdown": {"ENTER": enter_count, "SKIP": skip_count, "EXIT": exit_count},
            "decision_breakdown": strat_counts,
            "strategy_performance": strat_perf,
        }

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_pattern_vector(self, vector: PatternFeatureVector) -> None:
        """Store or update a breakout pattern feature vector in the pattern memory store."""
        conn = self._conn
        if conn is None:
            return
        await conn.execute(
            """
            INSERT OR REPLACE INTO pattern_feature_store VALUES (
                ?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                vector.token_address,
                vector.consolidation_duration_s,
                vector.dip_depth_pct,
                vector.volume_surge_multiplier,
                vector.net_buy_delta,
                vector.top10_concentration,
                vector.liquidity_to_mc_ratio,
                vector.smart_wallet_inflows,
                vector.peak_gain_multiplier,
                vector.timestamp_ns,
                int(time.time()),
            ),
        )
        await conn.commit()
        logger.debug("Recorded pattern vector for %s (gain=%.2fx)", vector.token_address[:10], vector.peak_gain_multiplier)

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_pattern_vectors(self, limit: int = 100) -> list[PatternFeatureVector]:
        """Retrieve recent historical breakout pattern feature vectors."""
        conn = self._conn
        if conn is None:
            return []
        async with conn.execute(
            """
            SELECT
                token_address,
                consolidation_duration_s,
                dip_depth_pct,
                volume_surge_multiplier,
                net_buy_delta,
                top10_concentration,
                liquidity_to_mc_ratio,
                smart_wallet_inflows,
                peak_gain_multiplier,
                timestamp_ns
            FROM pattern_feature_store
            ORDER BY peak_gain_multiplier DESC, created_at DESC
            LIMIT ?
            """,
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()

        results: list[PatternFeatureVector] = []
        for r in rows:
            try:
                results.append(
                    PatternFeatureVector(
                        token_address=r[0],
                        consolidation_duration_s=r[1],
                        dip_depth_pct=r[2],
                        volume_surge_multiplier=r[3],
                        net_buy_delta=r[4],
                        top10_concentration=r[5],
                        liquidity_to_mc_ratio=r[6],
                        smart_wallet_inflows=r[7],
                        peak_gain_multiplier=r[8],
                        timestamp_ns=r[9],
                    )
                )
            except Exception as exc:
                logger.debug("Error parsing PatternFeatureVector: %s", exc)
        return results


__all__ = [
    "SQLiteLedger",
    "sqlite_retry",
    "_DB_INIT_TIMEOUT_S",
    "_SNAPSHOT_INTERVAL_S",
]
