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
from alpha_engine.models.enums import ChainIdentifier, LotStatus, TokenRequestState, TradeExitReason
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PaperFill, PortfolioSnapshot, TokenRequestRecord, TradeRecord

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
        created_at          INTEGER NOT NULL,
        platform            TEXT NOT NULL DEFAULT 'DexScan',
        is_new_launch       INTEGER NOT NULL DEFAULT 0,
        hard_stop_loss_pct  REAL,
        custom_stop_loss_pct REAL,
        custom_take_profit_pct REAL,
        trailing_stop_from_peak_pct REAL,
        strategy_name       TEXT
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

    _CREATE_TRACE_LOG = """
    CREATE TABLE IF NOT EXISTS token_lifecycle_trace (
        trace_id TEXT PRIMARY KEY,
        token_address TEXT NOT NULL,
        chain TEXT NOT NULL,
        stage TEXT NOT NULL,
        component TEXT NOT NULL,
        function_name TEXT NOT NULL,
        status TEXT NOT NULL,
        reason TEXT,
        input_data_json TEXT NOT NULL,
        output_data_json TEXT NOT NULL,
        duration_ms REAL NOT NULL,
        timestamp_ns INTEGER NOT NULL
    )
    """

    _CREATE_SETTINGS = """
    CREATE TABLE IF NOT EXISTS bot_settings (
        key         TEXT PRIMARY KEY,
        value       TEXT NOT NULL,
        updated_at  INTEGER NOT NULL
    )
    """

    _CREATE_TOKEN_REQUESTS = """
    CREATE TABLE IF NOT EXISTS token_requests (
        request_id              TEXT PRIMARY KEY,
        token_address           TEXT NOT NULL,
        chain                   TEXT NOT NULL,
        submitted_at_ns         INTEGER NOT NULL,
        user_id                 TEXT,
        message_id              INTEGER,
        state                   TEXT NOT NULL,
        opportunity_score       REAL NOT NULL DEFAULT 0.0,
        opportunity_rank        INTEGER NOT NULL DEFAULT 0,
        validation_status       TEXT NOT NULL DEFAULT 'PENDING',
        rejection_reason        TEXT,
        queue_entry_ts_ns       INTEGER,
        queue_exit_ts_ns        INTEGER,
        analysis_start_ts_ns    INTEGER,
        analysis_end_ts_ns      INTEGER,
        execution_start_ts_ns   INTEGER,
        order_id                TEXT,
        lot_id                  TEXT,
        simulated_fill_price    TEXT,
        tokens_filled           TEXT,
        realized_pnl_usd        REAL,
        close_reason            TEXT,
        metadata_json           TEXT,
        created_at              INTEGER NOT NULL,
        updated_at_ns           INTEGER NOT NULL
    )
    """

    _CREATE_STRATEGY_EXPERIMENTS = """
    CREATE TABLE IF NOT EXISTS strategy_experiments (
        experiment_id       TEXT PRIMARY KEY,
        strategy_name       TEXT NOT NULL,
        parameters_json     TEXT NOT NULL,
        regime              TEXT NOT NULL DEFAULT 'ALL',
        status              TEXT NOT NULL CHECK(status IN ('SHADOW','CHAMPION','RETIRED')),
        trades_count        INTEGER NOT NULL DEFAULT 0,
        win_rate            REAL NOT NULL DEFAULT 0.0,
        profit_factor       REAL NOT NULL DEFAULT 0.0,
        sharpe_ratio        REAL NOT NULL DEFAULT 0.0,
        max_drawdown_pct    REAL NOT NULL DEFAULT 0.0,
        net_pnl_usd         REAL NOT NULL DEFAULT 0.0,
        created_at          INTEGER NOT NULL,
        updated_at          INTEGER NOT NULL
    )
    """

    _CREATE_AGENT_DELIBERATION_LOGS = """
    CREATE TABLE IF NOT EXISTS agent_deliberation_logs (
        deliberation_id             TEXT PRIMARY KEY,
        signal_id                   TEXT NOT NULL,
        token_address               TEXT NOT NULL,
        agent_votes_json            TEXT NOT NULL,
        episodic_memory_matches_json TEXT NOT NULL,
        selected_strategy_id        TEXT NOT NULL,
        final_decision              TEXT NOT NULL,
        rationale                   TEXT NOT NULL,
        timestamp_ns                INTEGER NOT NULL
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
        await conn.execute(self._CREATE_TRACE_LOG)
        await conn.execute(self._CREATE_SETTINGS)
        await conn.execute(self._CREATE_TOKEN_REQUESTS)
        await conn.execute(self._CREATE_STRATEGY_EXPERIMENTS)
        await conn.execute(self._CREATE_AGENT_DELIBERATION_LOGS)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_token_requests_token ON token_requests(token_address);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_token_requests_state ON token_requests(state);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_token_requests_created ON token_requests(created_at);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_token ON trades(token_address);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_chain ON trades(chain);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_experiments_status ON strategy_experiments(status);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_deliberations_token ON agent_deliberation_logs(token_address);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_deliberations_signal ON agent_deliberation_logs(signal_id);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_deliberations_ts ON agent_deliberation_logs(timestamp_ns);")
        try:
            cursor = await conn.execute("PRAGMA table_info(trades)")
            cols = [row[1] for row in await cursor.fetchall()]
            if cols:
                if "realized_pnl_usd" not in cols:
                    await conn.execute("ALTER TABLE trades ADD COLUMN realized_pnl_usd TEXT;")
                if "platform" not in cols:
                    await conn.execute("ALTER TABLE trades ADD COLUMN platform TEXT NOT NULL DEFAULT 'DexScan';")
                if "is_new_launch" not in cols:
                    await conn.execute("ALTER TABLE trades ADD COLUMN is_new_launch INTEGER NOT NULL DEFAULT 0;")
                if "hard_stop_loss_pct" not in cols:
                    await conn.execute("ALTER TABLE trades ADD COLUMN hard_stop_loss_pct REAL;")
                if "custom_stop_loss_pct" not in cols:
                    await conn.execute("ALTER TABLE trades ADD COLUMN custom_stop_loss_pct REAL;")
                if "custom_take_profit_pct" not in cols:
                    await conn.execute("ALTER TABLE trades ADD COLUMN custom_take_profit_pct REAL;")
                if "trailing_stop_from_peak_pct" not in cols:
                    await conn.execute("ALTER TABLE trades ADD COLUMN trailing_stop_from_peak_pct REAL;")
                if "strategy_name" not in cols:
                    await conn.execute("ALTER TABLE trades ADD COLUMN strategy_name TEXT;")
        except Exception as exc:
            logger.debug("Migration check for trades schema: %s", exc)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_platform ON trades(platform);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(timestamp_ns);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_lot_lifecycle_id ON lot_lifecycle(lot_id);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_lot_lifecycle_status ON lot_lifecycle(status);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_token ON decision_audit_log(token_address);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_type ON decision_audit_log(decision_type);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_ts ON decision_audit_log(timestamp_ns);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_created ON decision_audit_log(created_at);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_pattern_store_ts ON pattern_feature_store(timestamp_ns);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trace_token ON token_lifecycle_trace(token_address);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trace_ts ON token_lifecycle_trace(timestamp_ns DESC);")
        await conn.commit()
        logger.info("SQLite ledger initialised at %s (WAL mode)", self._db_path)

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_trade(self, record: TradeRecord | PaperFill) -> None:
        conn = self._conn
        if conn is None:
            logger.debug("SQLiteLedger connection is closed; skipping trade.")
            return
        if not isinstance(record, TradeRecord):
            record = TradeRecord.from_fill(record)
        await conn.execute(
            """
            INSERT INTO trades (
                trade_id, order_id, signal_id, chain, token_address, pool_address,
                side, native_spent, tokens_delta, effective_price, price_impact_bps,
                gas_cost_usd, fill_latency_ms, kelly_fraction, portfolio_equity_usd,
                realized_pnl_usd, signal_timestamp_ns, fill_timestamp_ns, created_at,
                platform, is_new_launch, hard_stop_loss_pct, custom_stop_loss_pct,
                custom_take_profit_pct, trailing_stop_from_peak_pct, strategy_name
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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
                getattr(record, "platform", "DexScan") or "DexScan",
                1 if getattr(record, "is_new_launch", False) else 0,
                getattr(record, "hard_stop_loss_pct", None),
                getattr(record, "custom_stop_loss_pct", None),
                getattr(record, "custom_take_profit_pct", None),
                getattr(record, "trailing_stop_from_peak_pct", None),
                getattr(record, "strategy_name", None),
            ),
        )
        await conn.commit()
        logger.debug("Trade %s recorded in ledger (%s).", record.trade_id[:8], getattr(record, "platform", "DexScan"))

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
    async def get_recent_trades(self, limit: int = 10) -> list[dict[str, Any]]:
        """Retrieve the most recent executed trades with their platform identification."""
        conn = self._conn
        if conn is None:
            return []
        try:
            cur = await conn.execute("PRAGMA table_info(trades)")
            cols = [row[1] for row in await cur.fetchall()]
            has_platform = "platform" in cols
            has_new_launch = "is_new_launch" in cols
            plat_col = "platform" if has_platform else "'DexScan'"
            nl_col = "is_new_launch" if has_new_launch else "0"
            query = (
                f"SELECT chain, token_address, side, effective_price, realized_pnl_usd, created_at, {plat_col}, {nl_col}, fill_timestamp_ns "
                f"FROM trades ORDER BY created_at DESC LIMIT ?"
            )
            async with conn.execute(query, (limit,)) as cursor:
                rows = await cursor.fetchall()
                res = []
                for r in rows:
                    res.append({
                        "chain": r[0],
                        "token_address": r[1],
                        "side": r[2],
                        "effective_price": r[3],
                        "realized_pnl_usd": r[4],
                        "created_at": r[5],
                        "platform": r[6] or "DexScan",
                        "is_new_launch": bool(r[7]),
                        "fill_timestamp_ns": r[8],
                    })
                return res
        except Exception as exc:
            logger.debug("Failed to get recent trades from ledger: %s", exc)
            return []

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_closed_trades_dataset(
        self,
        limit: int = 1000,
        lookback_hours: Optional[float] = None,
    ) -> list[dict[str, Any]]:
        """
        Extract institutional dataset of closed trades joined with exit lifecycle reasons.
        Serves as the mandatory source of truth for self-learning, attribution, and tuner calibration.
        """
        conn = self._conn
        if conn is None:
            return []
        try:
            time_filter = ""
            params: list[Any] = []
            if lookback_hours is not None and lookback_hours > 0:
                cutoff_ns = int((time.time() - (lookback_hours * 3600)) * 1_000_000_000)
                time_filter = " AND t.fill_timestamp_ns >= ? "
                params.append(cutoff_ns)
            params.append(limit)

            query = f"""
                SELECT 
                    t.trade_id, t.order_id, t.signal_id, t.chain, t.token_address, t.pool_address,
                    t.effective_price, t.realized_pnl_usd, t.native_spent, t.fill_latency_ms,
                    t.price_impact_bps, t.platform, t.is_new_launch, t.strategy_name,
                    t.fill_timestamp_ns, t.created_at,
                    l.exit_reason
                FROM trades t
                LEFT JOIN (
                    SELECT token_address, exit_reason, MAX(timestamp_ns) as max_ts
                    FROM lot_lifecycle 
                    WHERE exit_reason IS NOT NULL 
                    GROUP BY token_address
                ) l ON t.token_address = l.token_address
                WHERE t.side = 'sell' AND t.realized_pnl_usd IS NOT NULL
                {time_filter}
                ORDER BY t.created_at DESC
                LIMIT ?
            """
            async with conn.execute(query, tuple(params)) as cursor:
                rows = await cursor.fetchall()
                dataset = []
                for r in rows:
                    pnl_f = float(r[7]) if r[7] is not None else 0.0
                    dataset.append({
                        "trade_id": r[0],
                        "order_id": r[1],
                        "signal_id": r[2],
                        "chain": r[3],
                        "token_address": r[4],
                        "pool_address": r[5],
                        "effective_price": float(r[6]) if r[6] else 0.0,
                        "realized_pnl_usd": pnl_f,
                        "is_win": pnl_f > 0.0,
                        "native_spent": float(r[8]) if r[8] else 0.0,
                        "fill_latency_ms": int(r[9]) if r[9] else 0,
                        "price_impact_bps": int(r[10]) if r[10] else 0,
                        "platform": r[11] or "DexScan",
                        "is_new_launch": bool(r[12]),
                        "strategy_name": r[13] or "unassigned",
                        "fill_timestamp_ns": r[14],
                        "created_at": r[15],
                        "exit_reason": r[16] or "unspecified_exit",
                    })
                return dataset
        except Exception as exc:
            logger.error("Failed to query closed trades dataset: %s", exc)
            return []

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_loss_attribution_clusters(
        self,
        lookback_hours: Optional[float] = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        """
        Group closed losing trades into empirical root cause failure modes.
        Enables closed-loop feedback engine to adjust risk posture and entry filters.
        """
        conn = self._conn
        if conn is None:
            return {"total_losses": 0, "clusters": {}, "loss_pnl_usd": 0.0}
        try:
            time_filter = ""
            params: list[Any] = []
            if lookback_hours is not None and lookback_hours > 0:
                cutoff_ns = int((time.time() - (lookback_hours * 3600)) * 1_000_000_000)
                time_filter = " AND t.fill_timestamp_ns >= ? "
                params.append(cutoff_ns)
            params.append(limit)

            query = f"""
                SELECT 
                    COALESCE(l.exit_reason, 'unknown_loss') as reason,
                    COUNT(*) as cnt,
                    SUM(CAST(t.realized_pnl_usd AS REAL)) as total_loss,
                    AVG(CAST(t.realized_pnl_usd AS REAL)) as avg_loss,
                    AVG(t.fill_latency_ms) as avg_latency,
                    AVG(t.price_impact_bps) as avg_impact
                FROM trades t
                LEFT JOIN (
                    SELECT token_address, exit_reason, MAX(timestamp_ns) as max_ts
                    FROM lot_lifecycle 
                    WHERE exit_reason IS NOT NULL 
                    GROUP BY token_address
                ) l ON t.token_address = l.token_address
                WHERE t.side = 'sell' 
                  AND CAST(t.realized_pnl_usd AS REAL) < 0
                  {time_filter}
                GROUP BY COALESCE(l.exit_reason, 'unknown_loss')
                ORDER BY cnt DESC
                LIMIT ?
            """
            async with conn.execute(query, tuple(params)) as cursor:
                rows = await cursor.fetchall()
                clusters: dict[str, Any] = {}
                total_loss_count = 0
                total_loss_pnl = 0.0
                for r in rows:
                    cnt = r[1] or 0
                    t_loss = float(r[2] or 0.0)
                    clusters[r[0]] = {
                        "count": cnt,
                        "total_loss_usd": round(t_loss, 2),
                        "avg_loss_usd": round(float(r[3] or 0.0), 3),
                        "avg_fill_latency_ms": round(float(r[4] or 0.0), 1),
                        "avg_price_impact_bps": round(float(r[5] or 0.0), 1),
                    }
                    total_loss_count += cnt
                    total_loss_pnl += t_loss

                return {
                    "total_losses": total_loss_count,
                    "total_loss_usd": round(total_loss_pnl, 2),
                    "clusters": clusters,
                }
        except Exception as exc:
            logger.error("Failed to query loss attribution clusters: %s", exc)
            return {"total_losses": 0, "clusters": {}, "loss_pnl_usd": 0.0}

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_historical_winners(
        self,
        min_pnl_usd: float = 0.50,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Extract top historical profitable trades to seed PatternMemoryStore."""
        conn = self._conn
        if conn is None:
            return []
        try:
            query = """
                SELECT 
                    t.token_address, t.chain, t.effective_price, t.realized_pnl_usd,
                    t.fill_latency_ms, t.price_impact_bps, t.created_at,
                    COALESCE(s.alpha_score, 0.70), COALESCE(s.top10_concentration, 0.15)
                FROM trades t
                LEFT JOIN signals s ON t.signal_id = s.signal_id
                WHERE t.side = 'sell' AND CAST(t.realized_pnl_usd AS REAL) >= ?
                ORDER BY CAST(t.realized_pnl_usd AS REAL) DESC
                LIMIT ?
            """
            async with conn.execute(query, (min_pnl_usd, limit)) as cursor:
                rows = await cursor.fetchall()
                winners = []
                for r in rows:
                    winners.append({
                        "token_address": r[0],
                        "chain": r[1],
                        "effective_price": float(r[2] or 0.0),
                        "realized_pnl_usd": float(r[3] or 0.0),
                        "fill_latency_ms": int(r[4] or 0),
                        "price_impact_bps": int(r[5] or 0),
                        "created_at": r[6],
                        "alpha_score": float(r[7] or 0.7),
                        "top10_concentration": float(r[8] or 0.15),
                    })
                return winners
        except Exception as exc:
            logger.error("Failed to query historical winners: %s", exc)
            return []

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

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_trace_event(self, event: Any) -> None:
        """Persist a token lifecycle trace event asynchronously to SQLite."""
        conn = self._conn
        if conn is None:
            return
        if isinstance(event, dict):
            stg = event.get("stage")
            stage_val = stg.value if hasattr(stg, "value") else str(stg)
            stat = event.get("status")
            status_val = stat.value if hasattr(stat, "value") else str(stat)
            trace_id = event.get("trace_id")
            token_address = event.get("token_address")
            chain_val = event.get("chain")
            component = event.get("component")
            function_name = event.get("function_name")
            reason = event.get("reason")
            input_data = event.get("input_data", {})
            output_data = event.get("output_data", {})
            duration_ms = float(event.get("duration_ms", 0.0))
            timestamp_ns = int(event.get("timestamp_ns", time.time_ns()))
        else:
            stage_val = event.stage.value if hasattr(event.stage, "value") else str(event.stage)
            status_val = event.status.value if hasattr(event.status, "value") else str(event.status)
            trace_id = event.trace_id
            token_address = event.token_address
            chain_val = event.chain
            component = event.component
            function_name = event.function_name
            reason = event.reason
            input_data = event.input_data
            output_data = event.output_data
            duration_ms = event.duration_ms
            timestamp_ns = event.timestamp_ns

        await conn.execute(
            """INSERT INTO token_lifecycle_trace VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                trace_id,
                token_address,
                chain_val,
                stage_val,
                component,
                function_name,
                status_val,
                reason,
                json.dumps(input_data, default=str),
                json.dumps(output_data, default=str),
                duration_ms,
                timestamp_ns,
            ),
        )
        await conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_recent_assets(self, limit: int = 50) -> list[str]:
        """Returns unique token addresses recently processed."""
        if not self._conn:
            return []
        async with self._conn.execute(
            "SELECT DISTINCT token_address FROM token_lifecycle_trace ORDER BY timestamp_ns DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [r[0] for r in rows]

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_asset_trace(self, token_address: str) -> list[dict]:
        """Returns chronological timeline for a specific token."""
        if not self._conn:
            return []
        async with self._conn.execute(
            "SELECT * FROM token_lifecycle_trace WHERE token_address = ? OR LOWER(token_address) = LOWER(?) ORDER BY timestamp_ns ASC",
            (token_address, token_address),
        ) as cursor:
            cols = [d[0] for d in cursor.description] if cursor.description else []
            rows = await cursor.fetchall()
            return [dict(r) if hasattr(r, "keys") else dict(zip(cols, r)) for r in rows]

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_stage_flow_summary(self, lookback_hours: float = 24.0) -> dict[str, dict[str, int]]:
        """
        Calculates throughput and drop-off counts across lifecycle stages
        over the specified lookback window.
        """
        conn = self._conn
        if conn is None:
            return {}
        cutoff_ns = int((time.time() - (lookback_hours * 3600)) * 1e9)
        summary: dict[str, dict[str, int]] = {}
        try:
            async with conn.execute(
                """
                SELECT stage, status, COUNT(*)
                FROM token_lifecycle_trace
                WHERE timestamp_ns >= ?
                GROUP BY stage, status
                """,
                (cutoff_ns,),
            ) as cursor:
                rows = await cursor.fetchall()
                for stage, status, count in rows:
                    if stage not in summary:
                        summary[stage] = {"PASSED": 0, "REJECTED": 0, "ERROR": 0, "TOTAL": 0}
                    summary[stage][status] = summary[stage].get(status, 0) + count
                    summary[stage]["TOTAL"] = summary[stage].get("TOTAL", 0) + count
        except Exception as exc:
            logger.debug("Failed to query stage flow summary: %s", exc)
        return summary

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def set_setting(self, key: str, value: str) -> None:
        """Persists a key-value setting into bot_settings table."""
        conn = self._conn
        if conn is None:
            logger.debug("SQLiteLedger connection is closed; skipping set_setting.")
            return
        now_ts = int(time.time())
        await conn.execute(
            """
            INSERT INTO bot_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, str(value), now_ts),
        )
        await conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Retrieves a setting value by key from bot_settings table."""
        conn = self._conn
        if conn is None:
            return default
        async with conn.execute("SELECT value FROM bot_settings WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return str(row[0])
            return default

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_token_request(self, record: TokenRequestRecord) -> None:
        """Persists a new or updated TokenRequestRecord into token_requests table."""
        conn = self._conn
        if conn is None:
            return
        state_str = record.state.value if hasattr(record.state, "value") else str(record.state)
        chain_str = record.chain.value if hasattr(record.chain, "value") else str(record.chain)
        fill_p = str(record.simulated_fill_price) if record.simulated_fill_price is not None else None
        tokens_f = str(record.tokens_filled) if record.tokens_filled is not None else None

        await conn.execute(
            """
            INSERT INTO token_requests (
                request_id, token_address, chain, submitted_at_ns, user_id, message_id,
                state, opportunity_score, opportunity_rank, validation_status, rejection_reason,
                queue_entry_ts_ns, queue_exit_ts_ns, analysis_start_ts_ns, analysis_end_ts_ns,
                execution_start_ts_ns, order_id, lot_id, simulated_fill_price, tokens_filled,
                realized_pnl_usd, close_reason, metadata_json, created_at, updated_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(request_id) DO UPDATE SET
                state = excluded.state,
                opportunity_score = excluded.opportunity_score,
                opportunity_rank = excluded.opportunity_rank,
                validation_status = excluded.validation_status,
                rejection_reason = excluded.rejection_reason,
                queue_entry_ts_ns = excluded.queue_entry_ts_ns,
                queue_exit_ts_ns = excluded.queue_exit_ts_ns,
                analysis_start_ts_ns = excluded.analysis_start_ts_ns,
                analysis_end_ts_ns = excluded.analysis_end_ts_ns,
                execution_start_ts_ns = excluded.execution_start_ts_ns,
                order_id = excluded.order_id,
                lot_id = excluded.lot_id,
                simulated_fill_price = excluded.simulated_fill_price,
                tokens_filled = excluded.tokens_filled,
                realized_pnl_usd = excluded.realized_pnl_usd,
                close_reason = excluded.close_reason,
                metadata_json = excluded.metadata_json,
                updated_at_ns = excluded.updated_at_ns
            """,
            (
                record.request_id,
                record.token_address,
                chain_str,
                record.submitted_at_ns,
                str(record.user_id) if record.user_id else None,
                record.message_id,
                state_str,
                record.opportunity_score,
                record.opportunity_rank,
                record.validation_status,
                record.rejection_reason,
                record.queue_entry_ts_ns,
                record.queue_exit_ts_ns,
                record.analysis_start_ts_ns,
                record.analysis_end_ts_ns,
                record.execution_start_ts_ns,
                record.order_id,
                record.lot_id,
                fill_p,
                tokens_f,
                record.realized_pnl_usd,
                record.close_reason,
                record.metadata_json,
                int(record.submitted_at_ns // 1_000_000_000),
                record.updated_at_ns,
            ),
        )
        await conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def update_token_request_status(
        self,
        request_id: str,
        state: TokenRequestState | str,
        rejection_reason: Optional[str] = None,
        opportunity_score: Optional[float] = None,
        order_id: Optional[str] = None,
        lot_id: Optional[str] = None,
        simulated_fill_price: Optional[Decimal] = None,
        tokens_filled: Optional[Decimal] = None,
        realized_pnl_usd: Optional[float] = None,
        close_reason: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Updates specific fields of a TokenRequestRecord by request_id."""
        conn = self._conn
        if conn is None:
            return
        state_str = state.value if hasattr(state, "value") else str(state)
        updates = ["state = ?", "updated_at_ns = ?"]
        params: list[Any] = [state_str, time.time_ns()]

        if rejection_reason is not None:
            updates.append("rejection_reason = ?")
            params.append(rejection_reason)
        if opportunity_score is not None:
            updates.append("opportunity_score = ?")
            params.append(opportunity_score)
        if order_id is not None:
            updates.append("order_id = ?")
            params.append(order_id)
        if lot_id is not None:
            updates.append("lot_id = ?")
            params.append(lot_id)
        if simulated_fill_price is not None:
            updates.append("simulated_fill_price = ?")
            params.append(str(simulated_fill_price))
        if tokens_filled is not None:
            updates.append("tokens_filled = ?")
            params.append(str(tokens_filled))
        if realized_pnl_usd is not None:
            updates.append("realized_pnl_usd = ?")
            params.append(realized_pnl_usd)
        if close_reason is not None:
            updates.append("close_reason = ?")
            params.append(close_reason)

        for k, v in kwargs.items():
            if k in (
                "queue_entry_ts_ns",
                "queue_exit_ts_ns",
                "analysis_start_ts_ns",
                "analysis_end_ts_ns",
                "execution_start_ts_ns",
                "opportunity_rank",
                "validation_status",
                "metadata_json",
            ):
                updates.append(f"{k} = ?")
                params.append(v)

        params.append(request_id)
        sql = f"UPDATE token_requests SET {', '.join(updates)} WHERE request_id = ?"
        await conn.execute(sql, tuple(params))
        await conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_token_request(self, request_id: str) -> Optional[TokenRequestRecord]:
        """Fetch a single TokenRequestRecord by request_id."""
        conn = self._conn
        if conn is None:
            return None
        async with conn.execute("SELECT * FROM token_requests WHERE request_id = ?", (request_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cursor.description]
            d = dict(zip(cols, row))
            chain_val = (
                ChainIdentifier(d["chain"])
                if d["chain"] in ("base_mainnet", "solana_mainnet")
                else ChainIdentifier.SOLANA_MAINNET
            )
            return TokenRequestRecord(
                request_id=d["request_id"],
                token_address=d["token_address"],
                chain=chain_val,
                submitted_at_ns=d["submitted_at_ns"],
                user_id=d.get("user_id"),
                message_id=d.get("message_id"),
                state=(
                    TokenRequestState(d["state"])
                    if d["state"] in TokenRequestState._value2member_map_
                    else TokenRequestState.RECEIVED
                ),
                opportunity_score=float(d.get("opportunity_score") or 0.0),
                opportunity_rank=int(d.get("opportunity_rank") or 0),
                validation_status=d.get("validation_status") or "PENDING",
                rejection_reason=d.get("rejection_reason"),
                queue_entry_ts_ns=d.get("queue_entry_ts_ns"),
                queue_exit_ts_ns=d.get("queue_exit_ts_ns"),
                analysis_start_ts_ns=d.get("analysis_start_ts_ns"),
                analysis_end_ts_ns=d.get("analysis_end_ts_ns"),
                execution_start_ts_ns=d.get("execution_start_ts_ns"),
                order_id=d.get("order_id"),
                lot_id=d.get("lot_id"),
                simulated_fill_price=(
                    Decimal(d["simulated_fill_price"]) if d.get("simulated_fill_price") else None
                ),
                tokens_filled=Decimal(d["tokens_filled"]) if d.get("tokens_filled") else None,
                realized_pnl_usd=float(d["realized_pnl_usd"]) if d.get("realized_pnl_usd") is not None else None,
                close_reason=d.get("close_reason"),
                metadata_json=d.get("metadata_json"),
                updated_at_ns=d.get("updated_at_ns") or d["submitted_at_ns"],
            )

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_pending_token_requests(self) -> list[TokenRequestRecord]:
        """Fetch all requests in QUEUED, WAITING_FOR_CAPACITY, or ANALYZING state."""
        conn = self._conn
        if conn is None:
            return []
        states = (
            TokenRequestState.QUEUED.value,
            TokenRequestState.WAITING_FOR_CAPACITY.value,
            TokenRequestState.ANALYZING.value,
        )
        async with conn.execute(
            "SELECT * FROM token_requests WHERE state IN (?, ?, ?) ORDER BY opportunity_score DESC, submitted_at_ns ASC",
            states,
        ) as cursor:
            rows = await cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            records = []
            for row in rows:
                d = dict(zip(cols, row))
                chain_val = (
                    ChainIdentifier(d["chain"])
                    if d["chain"] in ("base_mainnet", "solana_mainnet")
                    else ChainIdentifier.SOLANA_MAINNET
                )
                records.append(
                    TokenRequestRecord(
                        request_id=d["request_id"],
                        token_address=d["token_address"],
                        chain=chain_val,
                        submitted_at_ns=d["submitted_at_ns"],
                        user_id=d.get("user_id"),
                        message_id=d.get("message_id"),
                        state=(
                            TokenRequestState(d["state"])
                            if d["state"] in TokenRequestState._value2member_map_
                            else TokenRequestState.QUEUED
                        ),
                        opportunity_score=float(d.get("opportunity_score") or 0.0),
                        opportunity_rank=int(d.get("opportunity_rank") or 0),
                        validation_status=d.get("validation_status") or "PENDING",
                        rejection_reason=d.get("rejection_reason"),
                        queue_entry_ts_ns=d.get("queue_entry_ts_ns"),
                        queue_exit_ts_ns=d.get("queue_exit_ts_ns"),
                        analysis_start_ts_ns=d.get("analysis_start_ts_ns"),
                        analysis_end_ts_ns=d.get("analysis_end_ts_ns"),
                        execution_start_ts_ns=d.get("execution_start_ts_ns"),
                        order_id=d.get("order_id"),
                        lot_id=d.get("lot_id"),
                        simulated_fill_price=(
                            Decimal(d["simulated_fill_price"]) if d.get("simulated_fill_price") else None
                        ),
                        tokens_filled=Decimal(d["tokens_filled"]) if d.get("tokens_filled") else None,
                        realized_pnl_usd=(
                            float(d["realized_pnl_usd"]) if d.get("realized_pnl_usd") is not None else None
                        ),
                        close_reason=d.get("close_reason"),
                        metadata_json=d.get("metadata_json"),
                        updated_at_ns=d.get("updated_at_ns") or d["submitted_at_ns"],
                    )
                )
            return records

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def restore_open_positions_from_ledger(self) -> list[dict[str, Any]]:
        """
        Queries all BUY trades that have no corresponding CLOSED record in lot_lifecycle or matching SELL trade.
        Returns a list of dicts to reconstruct OpenLot instances upon engine restart.
        """
        conn = self._conn
        if conn is None:
            return []
        query = """
            SELECT t.order_id, t.signal_id, t.chain, t.token_address, t.pool_address,
                   t.native_spent, t.tokens_delta, t.effective_price, t.fill_timestamp_ns,
                   t.platform, t.is_new_launch,
                   t.hard_stop_loss_pct, t.custom_stop_loss_pct, t.custom_take_profit_pct,
                   t.trailing_stop_from_peak_pct, t.strategy_name
            FROM trades t
            WHERE t.side = 'buy'
              AND t.order_id NOT IN (
                  SELECT lot_id FROM lot_lifecycle WHERE LOWER(status) IN ('closed', 'settled')
              )
            ORDER BY t.created_at ASC
        """
        async with conn.execute(query) as cursor:
            rows = await cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            lot_dicts = [dict(zip(cols, r)) for r in rows]

        restored_lots = []
        for d in lot_dicts:
            token = d["token_address"]
            bought_tokens = Decimal(str(d.get("tokens_delta", 0)))
            buy_ts = d.get("fill_timestamp_ns", 0)
            sell_query = """
                SELECT tokens_delta FROM trades
                WHERE side = 'sell' AND token_address = ? AND fill_timestamp_ns >= ?
            """
            async with conn.execute(sell_query, (token, buy_ts)) as s_cursor:
                s_rows = await s_cursor.fetchall()
                total_sold = sum(abs(Decimal(str(r[0]))) for r in s_rows)

            remaining_tokens = bought_tokens - total_sold
            if remaining_tokens <= Decimal("0"):
                continue  # All tokens already sold, lot is completely closed!

            d["initial_tokens"] = str(bought_tokens)
            d["tokens_delta"] = str(remaining_tokens)
            d["scaled_out_50_pct"] = (total_sold > Decimal("0"))
            restored_lots.append(d)

        return restored_lots

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def clear_all_trades_and_lots(self) -> None:
        """Clears all trades, lot lifecycles, and portfolio snapshots for a fresh paper run."""
        conn = self._conn
        if conn is None:
            return
        await conn.execute("DELETE FROM trades;")
        await conn.execute("DELETE FROM lot_lifecycle;")
        await conn.execute("DELETE FROM portfolio_snapshots;")
        await conn.execute("DELETE FROM signals;")
        await conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def mark_token_closed(self, token_address: str, reason: str = "MANUAL_CLOSE", pnl_usd: float = 0.0) -> None:
        """Marks all open instances of a token as closed and settled in lot_lifecycle."""
        conn = self._conn
        if conn is None:
            return
        import uuid
        now_ns = time.time_ns()
        now_s = int(time.time())
        await conn.execute(
            """
            INSERT INTO lot_lifecycle (lot_id, token_address, chain, status, exit_reason, pnl_usd, timestamp_ns, created_at)
            VALUES (?, ?, 'solana_mainnet', 'closed', ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), token_address, reason, str(pnl_usd), now_ns, now_s),
        )
        await conn.execute(
            """
            INSERT INTO lot_lifecycle (lot_id, token_address, chain, status, exit_reason, pnl_usd, timestamp_ns, created_at)
            VALUES (?, ?, 'solana_mainnet', 'settled', ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), token_address, reason, str(pnl_usd), now_ns + 1, now_s),
        )
        await conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_strategy_experiment(self, experiment: Optional[dict[str, Any]] = None, **kwargs: Any) -> None:
        """Insert or replace a strategy tournament experiment record."""
        conn = self._conn
        if conn is None:
            return
        exp = dict(experiment or {})
        exp.update(kwargs)
        now_s = int(time.time())
        params_json = exp.get("parameters_json") or json.dumps(exp.get("parameters", {}))
        await conn.execute(
            """
            INSERT INTO strategy_experiments (
                experiment_id, strategy_name, parameters_json, regime, status,
                trades_count, win_rate, profit_factor, sharpe_ratio, max_drawdown_pct,
                net_pnl_usd, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(experiment_id) DO UPDATE SET
                strategy_name=excluded.strategy_name,
                parameters_json=excluded.parameters_json,
                regime=excluded.regime,
                status=excluded.status,
                trades_count=excluded.trades_count,
                win_rate=excluded.win_rate,
                profit_factor=excluded.profit_factor,
                sharpe_ratio=excluded.sharpe_ratio,
                max_drawdown_pct=excluded.max_drawdown_pct,
                net_pnl_usd=excluded.net_pnl_usd,
                updated_at=excluded.updated_at;
            """,
            (
                str(exp["experiment_id"]),
                str(exp["strategy_name"]),
                params_json,
                str(exp.get("regime", "ALL")),
                str(exp.get("status", "SHADOW")),
                int(exp.get("trades_count", 0)),
                float(exp.get("win_rate", 0.0)),
                float(exp.get("profit_factor", 0.0)),
                float(exp.get("sharpe_ratio", 0.0)),
                float(exp.get("max_drawdown_pct", 0.0)),
                float(exp.get("net_pnl_usd", 0.0)),
                int(exp.get("created_at", now_s)),
                now_s,
            ),
        )
        await conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_strategy_experiments(self, status: Optional[str] = None) -> list[dict[str, Any]]:
        """Retrieve strategy experiments, optionally filtered by status (CHAMPION, SHADOW, RETIRED)."""
        conn = self._conn
        if conn is None:
            return []
        conn.row_factory = aiosqlite.Row
        if status:
            cursor = await conn.execute(
                "SELECT * FROM strategy_experiments WHERE status = ? ORDER BY net_pnl_usd DESC;",
                (status.upper(),),
            )
        else:
            cursor = await conn.execute("SELECT * FROM strategy_experiments ORDER BY net_pnl_usd DESC;")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_agent_deliberation(self, deliberation: Optional[dict[str, Any]] = None, **kwargs: Any) -> None:
        """Record a structured multi-agent deliberation audit log."""
        conn = self._conn
        if conn is None:
            return
        import uuid
        d = dict(deliberation or {})
        d.update(kwargs)
        delib_id = str(d.get("deliberation_id") or uuid.uuid4())
        sig_id = str(d.get("signal_id", ""))
        token_addr = str(d.get("token_address", ""))
        votes_json = d.get("agent_votes_json") or json.dumps(d.get("votes", {}))
        matches_json = d.get("episodic_memory_matches_json") or json.dumps(d.get("episodic_matches", []))
        strat_id = str(d.get("selected_strategy_id", "default"))
        decision = str(d.get("final_decision", "PASS"))
        rationale = str(d.get("rationale", ""))
        ts_ns = int(d.get("timestamp_ns", time.time_ns()))
        await conn.execute(
            """
            INSERT OR REPLACE INTO agent_deliberation_logs (
                deliberation_id, signal_id, token_address, agent_votes_json,
                episodic_memory_matches_json, selected_strategy_id, final_decision,
                rationale, timestamp_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (delib_id, sig_id, token_addr, votes_json, matches_json, strat_id, decision, rationale, ts_ns),
        )
        await conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_agent_deliberations(self, token_address: Optional[str] = None, limit: int = 20) -> list[dict[str, Any]]:
        """Retrieve recent agent deliberation logs."""
        conn = self._conn
        if conn is None:
            return []
        conn.row_factory = aiosqlite.Row
        if token_address:
            cursor = await conn.execute(
                "SELECT * FROM agent_deliberation_logs WHERE token_address = ? ORDER BY timestamp_ns DESC LIMIT ?;",
                (token_address, limit),
            )
        else:
            cursor = await conn.execute(
                "SELECT * FROM agent_deliberation_logs ORDER BY timestamp_ns DESC LIMIT ?;",
                (limit,),
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]



__all__ = [
    "SQLiteLedger",
    "sqlite_retry",
    "_DB_INIT_TIMEOUT_S",
    "_SNAPSHOT_INTERVAL_S",
]
