"""
alpha_engine.execution.ledger — SQLite Ledger with WAL Mode Persistence
=======================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiosqlite + asyncio
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiosqlite

from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PortfolioSnapshot, TradeRecord

logger = logging.getLogger(__name__)

_DB_INIT_TIMEOUT_S = 10.0
_SNAPSHOT_INTERVAL_S = 60.0


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

    def __init__(self, db_path: str | Path = "paper_trading.db") -> None:
        self._db_path = str(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def __aenter__(self) -> "SQLiteLedger":
        self._conn = await asyncio.wait_for(
            aiosqlite.connect(self._db_path),
            timeout=_DB_INIT_TIMEOUT_S,
        )
        await self._initialise()
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def _initialise(self) -> None:
        conn = self._conn
        assert conn is not None, "Database not connected."
        await conn.execute("PRAGMA journal_mode=WAL;")
        await conn.execute("PRAGMA synchronous=NORMAL;")
        await conn.execute("PRAGMA cache_size=-8000;")
        await conn.execute("PRAGMA temp_store=MEMORY;")
        await conn.execute(self._CREATE_TRADES)
        await conn.execute(self._CREATE_SIGNALS)
        await conn.execute(self._CREATE_SNAPSHOTS)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_token ON trades(token_address);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_chain ON trades(chain);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(timestamp_ns);")
        await conn.commit()
        logger.info("SQLite ledger initialised at %s (WAL mode)", self._db_path)

    async def record_trade(self, record: TradeRecord) -> None:
        conn = self._conn
        assert conn is not None
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

    async def record_signal(self, signal: SignalEvent) -> None:
        conn = self._conn
        assert conn is not None
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

    async def record_snapshot(self, snapshot: PortfolioSnapshot) -> None:
        conn = self._conn
        assert conn is not None
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

    async def get_closed_trade_stats(self) -> dict[str, Any]:
        conn = self._conn
        assert conn is not None

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
