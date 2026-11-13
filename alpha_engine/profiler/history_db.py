"""
alpha_engine.profiler.history_db — SQLite Persistence for Wallet History, Swaps & Reconstructed Trades
======================================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiosqlite | SQLite WAL Mode
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional, Sequence

import aiosqlite

from alpha_engine.execution.ledger import AsyncRLock, sqlite_retry
from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.models.profiler import NormalizedSwap, WalletTradeRecord

logger = logging.getLogger(__name__)


class WalletHistoryDatabase:
    """
    SQLite persistence layer for raw swaps, sync windows, quota usage,
    historical prices, and reconstructed wallet trade records.
    """

    def __init__(self, db_path: str = "paper_trading.db") -> None:
        self.db_path = db_path
        self._lock = AsyncRLock()
        self._conn: Optional[aiosqlite.Connection] = None

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def connect(self) -> None:
        """Connect to SQLite database with concurrency pragmas."""
        if self._conn is not None:
            return

        self._conn = await aiosqlite.connect(self.db_path, timeout=30.0)
        self._conn.row_factory = aiosqlite.Row

        # Concurrency & durability pragmas
        await self._conn.execute("PRAGMA journal_mode = WAL;")
        await self._conn.execute("PRAGMA busy_timeout = 30000;")
        await self._conn.execute("PRAGMA synchronous = NORMAL;")
        await self._conn.execute("PRAGMA cache_size = -64000;")

        await self._create_tables()
        await self._conn.commit()
        logger.info("WalletHistoryDatabase initialized at %s", self.db_path)

    async def _create_tables(self) -> None:
        assert self._conn is not None
        # 1. Raw / Normalized Swaps Cache
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_swaps_cache (
                tx_hash TEXT NOT NULL,
                chain TEXT NOT NULL,
                wallet_address TEXT NOT NULL,
                block_time INTEGER NOT NULL,
                venue TEXT NOT NULL,
                token_in_address TEXT NOT NULL,
                token_in_symbol TEXT,
                token_in_amount TEXT NOT NULL,
                token_out_address TEXT NOT NULL,
                token_out_symbol TEXT,
                token_out_amount TEXT NOT NULL,
                fee_native TEXT NOT NULL,
                is_buy INTEGER NOT NULL,
                token_address TEXT NOT NULL,
                token_amount TEXT NOT NULL,
                native_amount TEXT NOT NULL,
                is_transfer INTEGER NOT NULL DEFAULT 0,
                custom_metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at_s INTEGER NOT NULL,
                PRIMARY KEY (chain, tx_hash, wallet_address, token_address)
            );
            """
        )
        await self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_wallet_swaps_lookup
            ON wallet_swaps_cache(chain, wallet_address, block_time);
            """
        )

        # 2. Sync Window Status per Wallet
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_sync_status (
                chain TEXT NOT NULL,
                wallet_address TEXT NOT NULL,
                earliest_synced_time INTEGER NOT NULL,
                latest_synced_time INTEGER NOT NULL,
                last_synced_at INTEGER NOT NULL,
                PRIMARY KEY (chain, wallet_address)
            );
            """
        )

        # 3. Provider Quota Accounting (Free tier daily tracking)
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS provider_quota_usage (
                provider_name TEXT NOT NULL,
                date_key TEXT NOT NULL,
                request_count INTEGER NOT NULL DEFAULT 0,
                units_used INTEGER NOT NULL DEFAULT 0,
                updated_at_s INTEGER NOT NULL,
                PRIMARY KEY (provider_name, date_key)
            );
            """
        )

        # 4. Reconstructed Trades Persistence
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_reconstructed_trades (
                trade_id TEXT PRIMARY KEY,
                wallet_address TEXT NOT NULL,
                chain TEXT NOT NULL,
                token_address TEXT NOT NULL,
                buy_tx_hash TEXT NOT NULL,
                sell_tx_hash TEXT NOT NULL,
                buy_timestamp INTEGER NOT NULL,
                sell_timestamp INTEGER NOT NULL,
                holding_time_seconds REAL NOT NULL,
                invested_native TEXT NOT NULL,
                realized_native_pnl TEXT NOT NULL,
                realized_pnl_usd TEXT NOT NULL,
                roi_pct REAL NOT NULL,
                entry_price_usd TEXT,
                exit_price_usd TEXT,
                custom_metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at_s INTEGER NOT NULL
            );
            """
        )
        await self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reconstructed_trades_wallet
            ON wallet_reconstructed_trades(chain, wallet_address, sell_timestamp);
            """
        )

        # 5. Reconstructed Metrics per Wallet
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_reconstructed_metrics (
                wallet_address TEXT NOT NULL,
                chain TEXT NOT NULL,
                total_closed_trades INTEGER NOT NULL,
                winning_trades INTEGER NOT NULL,
                losing_trades INTEGER NOT NULL,
                win_rate_pct REAL NOT NULL,
                total_pnl_usd TEXT NOT NULL,
                max_pnl_usd TEXT NOT NULL,
                median_holding_time_s REAL NOT NULL,
                open_positions_count INTEGER NOT NULL,
                last_reconstructed_at INTEGER NOT NULL,
                PRIMARY KEY (chain, wallet_address)
            );
            """
        )

        # 6. Historical Native Price Cache (bucketed hourly: timestamp // 3600 * 3600)
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS historical_native_prices (
                chain TEXT NOT NULL,
                timestamp_bucket INTEGER NOT NULL,
                price_usd TEXT NOT NULL,
                created_at_s INTEGER NOT NULL,
                PRIMARY KEY (chain, timestamp_bucket)
            );
            """
        )

    # -------------------------------------------------------------------------
    # Swaps Cache Operations
    # -------------------------------------------------------------------------

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_cached_swaps(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        since: Optional[int] = None,
        until: Optional[int] = None,
    ) -> list[NormalizedSwap]:
        """Retrieve cached swaps for a wallet within [since, until], ordered by block_time ASC."""
        await self.connect()
        assert self._conn is not None

        query = """
            SELECT tx_hash, chain, wallet_address, block_time, venue,
                   token_in_address, token_in_symbol, token_in_amount,
                   token_out_address, token_out_symbol, token_out_amount,
                   fee_native, is_buy, token_address, token_amount,
                   native_amount, is_transfer, custom_metadata_json
            FROM wallet_swaps_cache
            WHERE chain = ? AND wallet_address = ?
        """
        params: list[Any] = [chain.value, wallet_address]

        if since is not None:
            query += " AND block_time >= ?"
            params.append(int(since))
        if until is not None:
            query += " AND block_time <= ?"
            params.append(int(until))

        query += " ORDER BY block_time ASC;"

        async with self._lock:
            cursor = await self._conn.execute(query, params)
            rows = await cursor.fetchall()

        swaps: list[NormalizedSwap] = []
        for r in rows:
            meta = {}
            try:
                meta = json.loads(r["custom_metadata_json"])
            except Exception:
                pass
            swaps.append(
                NormalizedSwap(
                    tx_hash=r["tx_hash"],
                    block_time=r["block_time"],
                    chain=ChainIdentifier(r["chain"]),
                    wallet_address=r["wallet_address"],
                    venue=r["venue"],
                    token_in_address=r["token_in_address"],
                    token_in_symbol=r["token_in_symbol"],
                    token_in_amount=Decimal(r["token_in_amount"]),
                    token_out_address=r["token_out_address"],
                    token_out_symbol=r["token_out_symbol"],
                    token_out_amount=Decimal(r["token_out_amount"]),
                    fee_native=Decimal(r["fee_native"]),
                    is_buy=bool(r["is_buy"]),
                    token_address=r["token_address"],
                    token_amount=Decimal(r["token_amount"]),
                    native_amount=Decimal(r["native_amount"]),
                    is_transfer=bool(r["is_transfer"]),
                    custom_metadata=meta,
                )
            )
        return swaps

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def upsert_swaps(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        swaps: Sequence[NormalizedSwap],
    ) -> None:
        """Batch upsert normalized swap events into the SQLite cache."""
        if not swaps:
            return
        await self.connect()
        assert self._conn is not None

        now = int(time.time())
        query = """
            INSERT INTO wallet_swaps_cache (
                tx_hash, chain, wallet_address, block_time, venue,
                token_in_address, token_in_symbol, token_in_amount,
                token_out_address, token_out_symbol, token_out_amount,
                fee_native, is_buy, token_address, token_amount,
                native_amount, is_transfer, custom_metadata_json, created_at_s
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chain, tx_hash, wallet_address, token_address) DO UPDATE SET
                block_time = excluded.block_time,
                venue = excluded.venue,
                token_in_amount = excluded.token_in_amount,
                token_out_amount = excluded.token_out_amount,
                fee_native = excluded.fee_native,
                is_buy = excluded.is_buy,
                token_amount = excluded.token_amount,
                native_amount = excluded.native_amount,
                is_transfer = excluded.is_transfer,
                custom_metadata_json = excluded.custom_metadata_json;
        """
        records = [
            (
                s.tx_hash,
                chain.value,
                wallet_address,
                s.block_time,
                s.venue,
                s.token_in_address,
                s.token_in_symbol,
                str(s.token_in_amount),
                s.token_out_address,
                s.token_out_symbol,
                str(s.token_out_amount),
                str(s.fee_native),
                1 if s.is_buy else 0,
                s.token_address,
                str(s.token_amount),
                str(s.native_amount),
                1 if s.is_transfer else 0,
                json.dumps(s.custom_metadata),
                now,
            )
            for s in swaps
        ]

        async with self._lock:
            await self._conn.executemany(query, records)
            await self._conn.commit()

    # -------------------------------------------------------------------------
    # Sync Status Tracking
    # -------------------------------------------------------------------------

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_sync_status(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
    ) -> Optional[dict[str, int]]:
        """Retrieve synchronization window status for wallet."""
        await self.connect()
        assert self._conn is not None

        query = """
            SELECT earliest_synced_time, latest_synced_time, last_synced_at
            FROM wallet_sync_status
            WHERE chain = ? AND wallet_address = ?;
        """
        async with self._lock:
            cursor = await self._conn.execute(query, (chain.value, wallet_address))
            row = await cursor.fetchone()
            if row:
                return {
                    "earliest_synced_time": row["earliest_synced_time"],
                    "latest_synced_time": row["latest_synced_time"],
                    "last_synced_at": row["last_synced_at"],
                }
        return None

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def update_sync_status(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        earliest_time: int,
        latest_time: int,
    ) -> None:
        """Update or insert sync window bounds for a wallet."""
        await self.connect()
        assert self._conn is not None

        now = int(time.time())
        query = """
            INSERT INTO wallet_sync_status (
                chain, wallet_address, earliest_synced_time, latest_synced_time, last_synced_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(chain, wallet_address) DO UPDATE SET
                earliest_synced_time = MIN(wallet_sync_status.earliest_synced_time, excluded.earliest_synced_time),
                latest_synced_time = MAX(wallet_sync_status.latest_synced_time, excluded.latest_synced_time),
                last_synced_at = excluded.last_synced_at;
        """
        async with self._lock:
            await self._conn.execute(
                query,
                (chain.value, wallet_address, earliest_time, latest_time, now),
            )
            await self._conn.commit()

    # -------------------------------------------------------------------------
    # Quota Accounting
    # -------------------------------------------------------------------------

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def check_and_increment_quota(
        self,
        provider_name: str,
        daily_limit: int,
        cost: int = 1,
    ) -> bool:
        """
        Atomic check & increment of daily free-tier request quota.
        Returns True if within limit and incremented, False if limit exceeded.
        """
        await self.connect()
        assert self._conn is not None

        date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now = int(time.time())

        async with self._lock:
            cursor = await self._conn.execute(
                "SELECT request_count FROM provider_quota_usage WHERE provider_name = ? AND date_key = ?;",
                (provider_name, date_key),
            )
            row = await cursor.fetchone()
            current_count = row["request_count"] if row else 0

            if current_count + cost > daily_limit:
                return False

            await self._conn.execute(
                """
                INSERT INTO provider_quota_usage (provider_name, date_key, request_count, units_used, updated_at_s)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(provider_name, date_key) DO UPDATE SET
                    request_count = provider_quota_usage.request_count + excluded.request_count,
                    units_used = provider_quota_usage.units_used + excluded.units_used,
                    updated_at_s = excluded.updated_at_s;
                """,
                (provider_name, date_key, cost, cost, now),
            )
            await self._conn.commit()
            return True

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_daily_quota_usage(self, target: Optional[Any] = None) -> int:
        """
        Get daily quota usage for a provider/chain or total for all providers for today.
        """
        await self.connect()
        assert self._conn is not None

        date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        target_str = target.value if hasattr(target, "value") else str(target or "")
        async with self._lock:
            if target_str:
                cursor = await self._conn.execute(
                    "SELECT SUM(request_count) as total FROM provider_quota_usage WHERE (provider_name = ? OR provider_name LIKE ?) AND date_key = ?;",
                    (target_str, f"%{target_str}%", date_key),
                )
            else:
                cursor = await self._conn.execute(
                    "SELECT SUM(request_count) as total FROM provider_quota_usage WHERE date_key = ?;",
                    (date_key,),
                )
            row = await cursor.fetchone()
            return int(row["total"]) if (row and row["total"]) else 0

    # -------------------------------------------------------------------------
    # Reconstructed Trades & Metrics
    # -------------------------------------------------------------------------

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def save_reconstructed_trades(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        trades: Sequence[WalletTradeRecord],
    ) -> None:
        """Persist reconstructed closed trade records."""
        if not trades:
            return
        await self.connect()
        assert self._conn is not None

        now = int(time.time())
        query = """
            INSERT INTO wallet_reconstructed_trades (
                trade_id, wallet_address, chain, token_address,
                buy_tx_hash, sell_tx_hash, buy_timestamp, sell_timestamp,
                holding_time_seconds, invested_native, realized_native_pnl,
                realized_pnl_usd, roi_pct, entry_price_usd, exit_price_usd,
                custom_metadata_json, created_at_s
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_id) DO UPDATE SET
                realized_native_pnl = excluded.realized_native_pnl,
                realized_pnl_usd = excluded.realized_pnl_usd,
                roi_pct = excluded.roi_pct,
                custom_metadata_json = excluded.custom_metadata_json;
        """
        records = []
        for t in trades:
            trade_id = f"{chain.value}_{t.buy_tx_hash[:16]}_{t.sell_tx_hash[:16]}_{t.token_address[:8]}"
            records.append((
                trade_id,
                wallet_address,
                chain.value,
                t.token_address,
                t.buy_tx_hash,
                t.sell_tx_hash,
                t.buy_timestamp,
                t.sell_timestamp,
                t.holding_time_seconds,
                str(t.invested_native),
                str(t.realized_native_pnl),
                str(t.realized_pnl_usd),
                t.roi_pct,
                str(t.entry_price_usd) if t.entry_price_usd is not None else None,
                str(t.exit_price_usd) if t.exit_price_usd is not None else None,
                json.dumps(t.custom_metadata),
                now,
            ))

        async with self._lock:
            await self._conn.executemany(query, records)
            await self._conn.commit()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_reconstructed_trades(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
    ) -> list[WalletTradeRecord]:
        """Retrieve persisted reconstructed trades for wallet."""
        await self.connect()
        assert self._conn is not None

        query = """
            SELECT token_address, buy_tx_hash, sell_tx_hash, buy_timestamp,
                   sell_timestamp, holding_time_seconds, invested_native,
                   realized_native_pnl, realized_pnl_usd, roi_pct,
                   entry_price_usd, exit_price_usd, custom_metadata_json
            FROM wallet_reconstructed_trades
            WHERE chain = ? AND wallet_address = ?
            ORDER BY buy_timestamp ASC;
        """
        async with self._lock:
            cursor = await self._conn.execute(query, (chain.value, wallet_address))
            rows = await cursor.fetchall()

        trades: list[WalletTradeRecord] = []
        for r in rows:
            meta = {}
            try:
                meta = json.loads(r["custom_metadata_json"])
            except Exception:
                pass
            trades.append(
                WalletTradeRecord(
                    token_address=r["token_address"],
                    buy_tx_hash=r["buy_tx_hash"],
                    sell_tx_hash=r["sell_tx_hash"],
                    buy_timestamp=r["buy_timestamp"],
                    sell_timestamp=r["sell_timestamp"],
                    holding_time_seconds=float(r["holding_time_seconds"]),
                    invested_native=Decimal(r["invested_native"]),
                    realized_native_pnl=Decimal(r["realized_native_pnl"]),
                    realized_pnl_usd=Decimal(r["realized_pnl_usd"]),
                    roi_pct=float(r["roi_pct"]),
                    entry_price_usd=Decimal(r["entry_price_usd"]) if r["entry_price_usd"] else None,
                    exit_price_usd=Decimal(r["exit_price_usd"]) if r["exit_price_usd"] else None,
                    custom_metadata=meta,
                )
            )
        return trades

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def save_reconstructed_metrics(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        metrics: dict[str, Any],
    ) -> None:
        """Persist high-level reconstructed metrics summary."""
        await self.connect()
        assert self._conn is not None

        now = int(time.time())
        query = """
            INSERT INTO wallet_reconstructed_metrics (
                wallet_address, chain, total_closed_trades, winning_trades,
                losing_trades, win_rate_pct, total_pnl_usd, max_pnl_usd,
                median_holding_time_s, open_positions_count, last_reconstructed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chain, wallet_address) DO UPDATE SET
                total_closed_trades = excluded.total_closed_trades,
                winning_trades = excluded.winning_trades,
                losing_trades = excluded.losing_trades,
                win_rate_pct = excluded.win_rate_pct,
                total_pnl_usd = excluded.total_pnl_usd,
                max_pnl_usd = excluded.max_pnl_usd,
                median_holding_time_s = excluded.median_holding_time_s,
                open_positions_count = excluded.open_positions_count,
                last_reconstructed_at = excluded.last_reconstructed_at;
        """
        async with self._lock:
            await self._conn.execute(
                query,
                (
                    wallet_address,
                    chain.value,
                    int(metrics.get("total_closed_trades", 0)),
                    int(metrics.get("winning_trades", 0)),
                    int(metrics.get("losing_trades", 0)),
                    float(metrics.get("win_rate_pct", 0.0)),
                    str(metrics.get("total_pnl_usd", "0")),
                    str(metrics.get("max_pnl_usd", "0")),
                    float(metrics.get("median_holding_time_s", 0.0)),
                    int(metrics.get("open_positions_count", 0)),
                    now,
                ),
            )
            await self._conn.commit()

    # -------------------------------------------------------------------------
    # Historical Price Cache
    # -------------------------------------------------------------------------

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_cached_price(
        self,
        chain: ChainIdentifier,
        timestamp: int,
    ) -> Optional[Decimal]:
        """Retrieve cached native token price for a timestamp bucket (hourly)."""
        await self.connect()
        assert self._conn is not None

        bucket = (timestamp // 3600) * 3600
        query = """
            SELECT price_usd FROM historical_native_prices
            WHERE chain = ? AND timestamp_bucket = ?;
        """
        async with self._lock:
            cursor = await self._conn.execute(query, (chain.value, bucket))
            row = await cursor.fetchone()
            if row:
                return Decimal(row["price_usd"])
        return None

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def cache_price(
        self,
        chain: ChainIdentifier,
        timestamp: int,
        price_usd: Decimal,
    ) -> None:
        """Cache historical native token price for timestamp bucket."""
        await self.connect()
        assert self._conn is not None

        bucket = (timestamp // 3600) * 3600
        now = int(time.time())
        query = """
            INSERT INTO historical_native_prices (chain, timestamp_bucket, price_usd, created_at_s)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chain, timestamp_bucket) DO UPDATE SET
                price_usd = excluded.price_usd;
        """
        async with self._lock:
            await self._conn.execute(query, (chain.value, bucket, str(price_usd), now))
            await self._conn.commit()

    async def close(self) -> None:
        """Close SQLite connection."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

