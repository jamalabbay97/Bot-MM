"""
alpha_engine.profiler.whitelist_db — Local Whitelist SQLite Database (WAL Mode)
==============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiosqlite | SQLite WAL Mode
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Optional

import aiosqlite

from alpha_engine.models.enums import ChainIdentifier, WalletClassification, WhitelistStatus
from alpha_engine.models.profiler import WalletProfile, WhitelistRecord

logger = logging.getLogger(__name__)


class WhitelistDatabase:
    """
    SQLite persistence layer for curated alpha smart money wallets.
    Enforces WAL mode, busy timeout, and asyncio.Lock for concurrency safety.
    """

    def __init__(self, db_path: str = "paper_trading.db") -> None:
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Connect to SQLite and initialize schema with WAL mode."""
        if self._conn is not None:
            return

        self._conn = await aiosqlite.connect(self.db_path, timeout=30.0)
        self._conn.row_factory = aiosqlite.Row

        # Concurrency & durability pragmas
        await self._conn.execute("PRAGMA journal_mode = WAL;")
        await self._conn.execute("PRAGMA busy_timeout = 30000;")
        await self._conn.execute("PRAGMA synchronous = NORMAL;")

        await self._create_tables()
        await self._conn.commit()
        logger.info("WhitelistDatabase initialized in WAL mode at %s", self.db_path)

    async def _create_tables(self) -> None:
        assert self._conn is not None
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_whitelist (
                wallet_address TEXT PRIMARY KEY,
                chain TEXT NOT NULL,
                classification TEXT NOT NULL,
                status TEXT NOT NULL,
                win_rate_pct REAL NOT NULL,
                total_trades INTEGER NOT NULL,
                total_pnl_usd TEXT NOT NULL,
                median_holding_time_s REAL NOT NULL,
                active_days REAL NOT NULL,
                cluster_tag TEXT,
                rejection_reasons TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL
            );
            """
        )
        await self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_whitelist_chain_status
            ON wallet_whitelist(chain, status);
            """
        )

    async def upsert_wallet(
        self,
        profile: WalletProfile,
        status: Optional[WhitelistStatus] = None,
    ) -> WhitelistRecord:
        """
        Insert or update a wallet profile in the whitelist database.
        """
        await self.connect()
        assert self._conn is not None

        if status is None:
            status = WhitelistStatus.ACTIVE if profile.is_whitelisted else WhitelistStatus.BANNED

        now_ns = time.time_ns()
        reasons_str = "; ".join(profile.rejection_reasons)

        async with self._lock:
            # Check existing to preserve created_at_ns
            cursor = await self._conn.execute(
                "SELECT created_at_ns FROM wallet_whitelist WHERE wallet_address = ?",
                (profile.wallet_address.lower(),),
            )
            row = await cursor.fetchone()
            created_at_ns = row["created_at_ns"] if row else now_ns

            await self._conn.execute(
                """
                INSERT INTO wallet_whitelist (
                    wallet_address, chain, classification, status, win_rate_pct,
                    total_trades, total_pnl_usd, median_holding_time_s, active_days,
                    cluster_tag, rejection_reasons, created_at_ns, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet_address) DO UPDATE SET
                    chain = excluded.chain,
                    classification = excluded.classification,
                    status = excluded.status,
                    win_rate_pct = excluded.win_rate_pct,
                    total_trades = excluded.total_trades,
                    total_pnl_usd = excluded.total_pnl_usd,
                    median_holding_time_s = excluded.median_holding_time_s,
                    active_days = excluded.active_days,
                    cluster_tag = excluded.cluster_tag,
                    rejection_reasons = excluded.rejection_reasons,
                    updated_at_ns = excluded.updated_at_ns;
                """,
                (
                    profile.wallet_address.lower(),
                    profile.chain.value,
                    profile.classification.value,
                    status.value,
                    float(profile.win_rate_pct),
                    int(profile.total_trades),
                    str(profile.total_pnl_usd),
                    float(profile.median_holding_time_seconds),
                    float(profile.active_days),
                    profile.cluster_tag,
                    reasons_str,
                    created_at_ns,
                    now_ns,
                ),
            )
            await self._conn.commit()

        return WhitelistRecord(
            wallet_address=profile.wallet_address.lower(),
            chain=profile.chain,
            classification=profile.classification,
            status=status,
            win_rate_pct=profile.win_rate_pct,
            total_trades=profile.total_trades,
            total_pnl_usd=profile.total_pnl_usd,
            median_holding_time_s=profile.median_holding_time_seconds,
            active_days=profile.active_days,
            cluster_tag=profile.cluster_tag,
            rejection_reasons=reasons_str,
            created_at_ns=created_at_ns,
            updated_at_ns=now_ns,
        )

    async def insert_wallet(
        self,
        wallet_address: str,
        chain: ChainIdentifier = ChainIdentifier.BASE_MAINNET,
        classification: WalletClassification = WalletClassification.SMART_MONEY,
        status: WhitelistStatus = WhitelistStatus.ACTIVE,
        total_trades: int = 0,
        win_rate: float = 0.0,
        profit_factor: float = 0.0,
        avg_holding_time_seconds: float = 0.0,
        max_drawdown_pct: float = 0.0,
        **kwargs: Any,
    ) -> WhitelistRecord:
        """Convenience method to insert or update a wallet with individual attributes."""
        total_t = total_trades or 30
        wr = win_rate if win_rate > 1.0 else win_rate * 100.0
        wins = int(total_t * (wr / 100.0))
        losses = total_t - wins
        profile = WalletProfile(
            wallet_address=wallet_address,
            chain=chain,
            classification=classification,
            is_whitelisted=(status == WhitelistStatus.ACTIVE),
            total_trades=total_t,
            winning_trades=wins,
            losing_trades=losses,
            win_rate_pct=wr,
            total_pnl_usd=Decimal(str(kwargs.get("total_pnl_usd", 1000))),
            max_single_trade_pnl_usd=Decimal(str(kwargs.get("max_single_trade_pnl_usd", 100))),
            outlier_pnl_ratio=float(kwargs.get("outlier_pnl_ratio", 0.10)),
            median_holding_time_seconds=avg_holding_time_seconds,
            active_days=float(kwargs.get("active_days", 30)),
            days_since_last_active=float(kwargs.get("days_since_last_active", 1.0)),
            first_tx_timestamp=int(kwargs.get("first_tx_timestamp", time.time() - 30 * 86400)),
            last_tx_timestamp=int(kwargs.get("last_tx_timestamp", time.time())),
            cluster_tag=kwargs.get("cluster_tag"),
            rejection_reasons=[],
        )
        return await self.upsert_wallet(profile, status=status)

    async def get_wallet(self, wallet_address: str) -> Optional[WhitelistRecord]:
        """Query a single wallet by address."""
        await self.connect()
        assert self._conn is not None

        cursor = await self._conn.execute(
            """
            SELECT wallet_address, chain, classification, status, win_rate_pct,
                   total_trades, total_pnl_usd, median_holding_time_s, active_days,
                   cluster_tag, rejection_reasons, created_at_ns, updated_at_ns
            FROM wallet_whitelist
            WHERE wallet_address = ?;
            """,
            (wallet_address.lower(),),
        )
        row = await cursor.fetchone()
        if not row:
            return None

        return WhitelistRecord(
            wallet_address=row["wallet_address"],
            chain=ChainIdentifier(row["chain"]),
            classification=WalletClassification(row["classification"]),
            status=WhitelistStatus(row["status"]),
            win_rate_pct=row["win_rate_pct"],
            total_trades=row["total_trades"],
            total_pnl_usd=Decimal(row["total_pnl_usd"]),
            median_holding_time_s=row["median_holding_time_s"],
            active_days=row["active_days"],
            cluster_tag=row["cluster_tag"],
            rejection_reasons=row["rejection_reasons"],
            created_at_ns=row["created_at_ns"],
            updated_at_ns=row["updated_at_ns"],
        )

    async def is_whitelisted(
        self,
        wallet_address: str,
        chain: Optional[ChainIdentifier] = None,
    ) -> bool:
        """
        Check if a wallet address is currently an active whitelisted alpha wallet.
        """
        await self.connect()
        assert self._conn is not None

        query = """
            SELECT 1 FROM wallet_whitelist
            WHERE wallet_address = ? AND status = ?
        """
        params = [wallet_address.lower(), WhitelistStatus.ACTIVE.value]
        if chain is not None:
            query += " AND chain = ?"
            params.append(chain.value)

        cursor = await self._conn.execute(query, params)
        row = await cursor.fetchone()
        return row is not None

    async def ban_wallet(self, wallet_address: str, reason: str) -> None:
        """Ban a compromised or insider wallet."""
        await self.update_status(wallet_address, status=WhitelistStatus.BANNED, reason=f"BAN: {reason}")

    async def update_status(
        self,
        wallet_address: str,
        status: WhitelistStatus,
        reason: str = "",
    ) -> None:
        """Update status of a wallet in whitelist DB (e.g. SUSPENDED, ACTIVE, or BANNED)."""
        await self.connect()
        assert self._conn is not None

        async with self._lock:
            await self._conn.execute(
                """
                UPDATE wallet_whitelist
                SET status = ?, rejection_reasons = rejection_reasons || ' | ' || ?, updated_at_ns = ?
                WHERE wallet_address = ?;
                """,
                (status.value, reason, time.time_ns(), wallet_address.lower()),
            )
            await self._conn.commit()


    async def list_active(
        self,
        chain: Optional[ChainIdentifier] = None,
    ) -> list[WhitelistRecord]:
        """List all active whitelisted wallets."""
        await self.connect()
        assert self._conn is not None

        query = """
            SELECT wallet_address, chain, classification, status, win_rate_pct,
                   total_trades, total_pnl_usd, median_holding_time_s, active_days,
                   cluster_tag, rejection_reasons, created_at_ns, updated_at_ns
            FROM wallet_whitelist
            WHERE status = ?
        """
        params = [WhitelistStatus.ACTIVE.value]
        if chain is not None:
            query += " AND chain = ?"
            params.append(chain.value)

        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        return [
            WhitelistRecord(
                wallet_address=r["wallet_address"],
                chain=ChainIdentifier(r["chain"]),
                classification=WalletClassification(r["classification"]),
                status=WhitelistStatus(r["status"]),
                win_rate_pct=r["win_rate_pct"],
                total_trades=r["total_trades"],
                total_pnl_usd=Decimal(r["total_pnl_usd"]),
                median_holding_time_s=r["median_holding_time_s"],
                active_days=r["active_days"],
                cluster_tag=r["cluster_tag"],
                rejection_reasons=r["rejection_reasons"],
                created_at_ns=r["created_at_ns"],
                updated_at_ns=r["updated_at_ns"],
            )
            for r in rows
        ]

    async def close(self) -> None:
        """Close SQLite database connection cleanly."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None


WhitelistDB = WhitelistDatabase
