"""
alpha_engine.profiler.pipeline — Smart Money Discovery, Prioritization, Evaluation & Tracking Pipeline
========================================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiosqlite | SQLite WAL Mode
"""

from __future__ import annotations

import asyncio
from collections import deque
from decimal import Decimal
import logging
import re
import time
from typing import Any, Optional, Sequence

import aiosqlite

from alpha_engine.config import EngineConfig
from alpha_engine.execution.ledger import AsyncRLock, sqlite_retry
from alpha_engine.models.enums import (
    ChainIdentifier,
    LotStatus,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    WalletClassification,
    WhitelistStatus,
)
from alpha_engine.models.events import SignalEvent, SwapEvent
from alpha_engine.models.profiler import WalletProfile, WhitelistRecord
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.profiler.evaluator import WalletEvaluator
from alpha_engine.profiler.history_db import WalletHistoryDatabase
from alpha_engine.profiler.history_provider import ProviderQuotaExceededError, ProviderUnavailableError
from alpha_engine.profiler.profiler import SmartMoneyProfiler
from alpha_engine.profiler.whitelist_db import WhitelistDatabase

logger = logging.getLogger(__name__)

EVM_ADDR_REGEX = re.compile(r"0x[a-fA-F0-9]{40}")
SVM_ADDR_REGEX = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")


class SmartMoneyPipeline:
    """
    End-to-End Smart Money Pipeline:
    1. Candidate Discovery (reverse-engineer winning tokens, recurring winners, news/whale feeds).
    2. Quota-aware prioritization queue (persisted in SQLite).
    3. Quantitative evaluation via WalletEvaluator (FIFO realized PnL, wash-trading, MEV, out-of-sample).
    4. Whitelist curation with periodic re-evaluation and automatic demotion.
    5. Live tracking on SVM/EVM swap events with cluster agreement & conviction calculation.
    6. SignalSource.WHALE_WALLET emission into the normal Gatekeeper -> Council -> Sizing -> Execution path.
    7. Copy-trade performance attribution feeding demotion.
    """

    def __init__(
        self,
        config: Optional[EngineConfig] = None,
        db_path: str = "paper_trading.db",
        profiler: Optional[SmartMoneyProfiler] = None,
        whitelist_db: Optional[WhitelistDatabase] = None,
        history_db: Optional[WalletHistoryDatabase] = None,
        evaluator: Optional[WalletEvaluator] = None,
        dedupe_cooldown_s: Optional[float] = None,
        re_eval_interval_s: Optional[float] = None,
    ) -> None:
        self.config = config or EngineConfig()
        self.db_path = db_path
        self._lock = AsyncRLock()
        self._conn: Optional[aiosqlite.Connection] = None

        self.whitelist_db = whitelist_db or WhitelistDatabase(db_path=db_path)
        self.history_db = history_db or WalletHistoryDatabase(db_path=db_path)
        self.profiler = profiler or SmartMoneyProfiler(
            whitelist_db=self.whitelist_db,
            history_db=self.history_db,
            db_path=db_path,
        )
        self.evaluator = evaluator or self.profiler.evaluator or WalletEvaluator()

        # Telemetry & Runtime State
        self._signals_emitted_count = 0
        self._evaluations_history: deque[float] = deque(maxlen=2000)
        self._recent_swaps_seen: dict[str, float] = {}  # tx_hash -> ts
        self._wallet_recent_buys: dict[tuple[str, str], float] = {}  # (wallet, token) -> ts
        self._cached_metrics: dict[str, Any] = {
            "candidates_count": 0,
            "queue_depth": 0,
            "whitelisted_count": 0,
            "evaluations_per_hour": 0,
            "quota_usage": {"used": 0, "limit": 1000},
            "signals_emitted": 0,
        }
        self._dedupe_cooldown_s = (
            float(dedupe_cooldown_s)
            if dedupe_cooldown_s is not None
            else float(getattr(self.config, "wallet_dedupe_cooldown_s", 60.0))
        )
        self._re_eval_interval_s = (
            float(re_eval_interval_s)
            if re_eval_interval_s is not None
            else float(getattr(self.config, "wallet_re_eval_interval_s", 1800.0))
        )

        # Background lifecycle tasks
        self._started = False
        self._eval_task: Optional[asyncio.Task[Any]] = None
        self._re_eval_task: Optional[asyncio.Task[Any]] = None
        self._stop_event = asyncio.Event()

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def connect(self) -> None:
        """Establish SQLite connection and ensure schema."""
        if self._conn is not None:
            return

        self._conn = await aiosqlite.connect(self.db_path, timeout=30.0)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode = WAL;")
        await self._conn.execute("PRAGMA busy_timeout = 30000;")
        await self._conn.execute("PRAGMA synchronous = NORMAL;")
        await self._conn.execute("PRAGMA cache_size = -64000;")

        await self._create_tables()
        await self._conn.commit()

    async def _create_tables(self) -> None:
        assert self._conn is not None
        # 1. Candidate Queue Table
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_candidates (
                wallet_address TEXT NOT NULL,
                chain TEXT NOT NULL,
                provenance TEXT NOT NULL,
                status TEXT NOT NULL,
                priority REAL NOT NULL DEFAULT 1.0,
                discovered_at_s INTEGER NOT NULL,
                evaluated_at_s INTEGER NOT NULL DEFAULT 0,
                rejection_reasons TEXT NOT NULL DEFAULT '',
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (chain, wallet_address)
            );
            """
        )
        await self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_candidates_status_priority
            ON wallet_candidates(status, priority DESC, discovered_at_s ASC);
            """
        )

        # 2. Live Copy-Trade Performance Table
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_copy_performance (
                wallet_address TEXT NOT NULL,
                chain TEXT NOT NULL,
                total_copied_trades INTEGER NOT NULL DEFAULT 0,
                winning_copied_trades INTEGER NOT NULL DEFAULT 0,
                consecutive_losses INTEGER NOT NULL DEFAULT 0,
                total_copied_pnl_usd TEXT NOT NULL DEFAULT '0',
                last_copied_at_s INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (chain, wallet_address)
            );
            """
        )

    async def initialize(self) -> None:
        """Initialize all backing databases and profiler components."""
        await self.connect()
        await self.whitelist_db.connect()
        if self.history_db is not None:
            await self.history_db.connect()
        await self.profiler.initialize()
        logger.info("SmartMoneyPipeline initialized successfully at %s", self.db_path)

    async def start(self) -> None:
        """Start scheduled background pipeline loops."""
        if self._started:
            return
        await self.initialize()
        self._started = True
        self._stop_event.clear()

        # Start Candidate Evaluator Worker
        self._eval_task = asyncio.create_task(
            self._candidate_evaluator_worker(),
            name="smart_wallet_candidate_evaluator",
        )
        # Start Periodic Re-evaluation Worker
        self._re_eval_task = asyncio.create_task(
            self._re_evaluation_worker(),
            name="smart_wallet_re_evaluator",
        )
        logger.info("SmartMoneyPipeline scheduled background workers started.")

    async def stop(self) -> None:
        """Stop background pipeline tasks and close connections."""
        if not self._started:
            return
        self._started = False
        self._stop_event.set()

        for t in (self._eval_task, self._re_eval_task):
            if t is not None and not t.done():
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

        await self.whitelist_db.close()
        if self.history_db is not None:
            await self.history_db.close()
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
        logger.info("SmartMoneyPipeline stopped cleanly.")

    async def close(self) -> None:
        """Alias for stop() to support standard closing interface."""
        await self.stop()

    # -----------------------------------------------------------------------
    # 1. Candidate Discovery & Ingestion
    # -----------------------------------------------------------------------

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def discover_candidate(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        provenance: str,
        priority_boost: float = 0.0,
    ) -> None:
        """
        Record a newly discovered smart wallet candidate with its provenance.
        If already known, increments occurrence count and boosts priority.
        """
        clean_addr = wallet_address.lower().strip()
        if not clean_addr:
            return

        # Basic format validation
        if chain == ChainIdentifier.BASE_MAINNET:
            if not clean_addr.startswith("0x") or len(clean_addr) != 42:
                return
        elif chain == ChainIdentifier.SOLANA_MAINNET:
            if len(clean_addr) < 32 or len(clean_addr) > 44 or clean_addr.startswith("0x"):
                return

        await self.connect()
        assert self._conn is not None
        now_s = int(time.time())

        async with self._lock:
            cursor = await self._conn.execute(
                "SELECT priority, occurrence_count, status FROM wallet_candidates WHERE chain = ? AND wallet_address = ?",
                (chain.value, clean_addr),
            )
            row = await cursor.fetchone()
            if row:
                old_prio = float(row["priority"])
                old_cnt = int(row["occurrence_count"])
                curr_status = row["status"]
                new_cnt = old_cnt + 1
                new_prio = old_prio + 1.0 + priority_boost
                # If was deferred or pending, boost priority; preserve evaluated/rejected unless explicitly re-queued
                new_status = "PENDING" if curr_status in ("PENDING", "DEFERRED") else curr_status
                await self._conn.execute(
                    """
                    UPDATE wallet_candidates
                    SET priority = ?, occurrence_count = ?, status = ?, provenance = provenance || ';' || ?
                    WHERE chain = ? AND wallet_address = ?;
                    """,
                    (new_prio, new_cnt, new_status, provenance, chain.value, clean_addr),
                )
            else:
                base_prio = 1.0 + priority_boost
                await self._conn.execute(
                    """
                    INSERT INTO wallet_candidates (
                        wallet_address, chain, provenance, status, priority, discovered_at_s, occurrence_count
                    ) VALUES (?, ?, ?, 'PENDING', ?, ?, 1);
                    """,
                    (clean_addr, chain.value, provenance, base_prio, now_s),
                )
            await self._conn.commit()

        logger.debug(
            "Discovered smart wallet candidate %s on %s via %s (boost=%.1f)",
            clean_addr[:10], chain.value, provenance, priority_boost
        )

    async def discover_from_winning_token(
        self,
        token_address: str,
        price_gain_pct: float,
        duration_seconds: float = 3600.0,
        early_trades: Sequence[dict[str, Any]] = (),
        creation_block: int = 0,
        creation_timestamp: int = 0,
        deployer_address: Optional[str] = None,
        deployer_cluster: Optional[Sequence[str]] = None,
        chain: Optional[ChainIdentifier] = None,
    ) -> list[str]:
        """
        Reverse-engineer early buyers of tokens that performed (>300% gain)
        and record each buyer as a high-priority candidate.
        """
        target_chain = chain or ChainIdentifier.SOLANA_MAINNET
        candidates = await self.profiler.reverse_engineer_winning_tokens(
            token_address=token_address,
            price_gain_pct=price_gain_pct,
            duration_seconds=duration_seconds,
            early_trades=early_trades,
            creation_block=creation_block,
            creation_timestamp=creation_timestamp,
            deployer_address=deployer_address,
            deployer_cluster=deployer_cluster,
            chain=target_chain,
        )

        for addr in candidates:
            await self.discover_candidate(
                wallet_address=addr,
                chain=target_chain,
                provenance=f"REVERSE_ENGINEER:{token_address[:10]}",
                priority_boost=2.5,
            )

        return candidates

    async def discover_from_news_or_feed(
        self,
        text: str,
        chain: ChainIdentifier,
        channel: str = "Telegram",
    ) -> list[str]:
        """
        Scan news or Telegram whale alerts for wallet addresses and store as candidates.
        Whale feeds are NEVER trusted blindly; they are candidate sources only.
        """
        found: list[str] = []
        if chain == ChainIdentifier.BASE_MAINNET:
            matches = EVM_ADDR_REGEX.findall(text)
            found.extend(matches)
        else:
            matches = SVM_ADDR_REGEX.findall(text)
            found.extend(matches)

        for addr in set(found):
            await self.discover_candidate(
                wallet_address=addr,
                chain=chain,
                provenance=f"FEED:{channel}",
                priority_boost=1.0,
            )

        return found

    # -----------------------------------------------------------------------
    # 2. Quota-Aware Prioritization Queue & Evaluation
    # -----------------------------------------------------------------------

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_candidate(
        self, wallet_address: str, chain: ChainIdentifier
    ) -> Optional[dict[str, Any]]:
        """Fetch candidate status by address and chain."""
        await self.connect()
        assert self._conn is not None
        cursor = await self._conn.execute(
            "SELECT * FROM wallet_candidates WHERE chain = ? AND wallet_address = ?",
            (chain.value, wallet_address.lower().strip()),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_next_candidate_for_evaluation(self) -> Optional[dict[str, Any]]:
        """Retrieve highest-priority pending candidate."""
        await self.connect()
        assert self._conn is not None
        async with self._lock:
            cursor = await self._conn.execute(
                """
                SELECT wallet_address, chain, provenance, status, priority, discovered_at_s, occurrence_count
                FROM wallet_candidates
                WHERE status IN ('PENDING', 'DEFERRED')
                ORDER BY priority DESC, discovered_at_s ASC
                LIMIT 1;
                """
            )
            row = await cursor.fetchone()
            if not row:
                return None
            return dict(row)

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_candidate_evaluation(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        profile: WalletProfile,
    ) -> None:
        """
        Record the quantitative evaluation outcome for a candidate wallet.
        - Whitelisted -> status='EVALUATED'
        - Insufficient history / provider unavailable -> status='DEFERRED' (not banned)
        - Trap / malicious -> status='REJECTED'
        """
        await self.connect()
        assert self._conn is not None
        now_s = int(time.time())
        clean_addr = wallet_address.lower().strip()
        reasons_str = "; ".join(profile.rejection_reasons)

        if profile.is_whitelisted:
            new_status = "EVALUATED"
            wl_status = WhitelistStatus.ACTIVE
        elif profile.classification in (
            WalletClassification.INSUFFICIENT_HISTORY,
            WalletClassification.INSUFFICIENT_DATA,
        ):
            new_status = "DEFERRED"
            wl_status = None  # Do NOT insert into whitelist DB as banned
        else:
            new_status = "REJECTED"
            wl_status = WhitelistStatus.BANNED

        async with self._lock:
            await self._conn.execute(
                """
                INSERT INTO wallet_candidates (
                    wallet_address, chain, provenance, status, priority, discovered_at_s, evaluated_at_s, rejection_reasons, occurrence_count
                ) VALUES (?, ?, 'EVALUATION', ?, 1.0, ?, ?, ?, 1)
                ON CONFLICT(chain, wallet_address) DO UPDATE SET
                    status = excluded.status,
                    evaluated_at_s = excluded.evaluated_at_s,
                    rejection_reasons = excluded.rejection_reasons;
                """,
                (clean_addr, chain.value, new_status, now_s, now_s, reasons_str),
            )
            await self._conn.commit()

        if wl_status is not None:
            await self.whitelist_db.upsert_wallet(profile, status=wl_status)

        self._evaluations_history.append(time.time())
        logger.info(
            "Candidate %s on %s evaluated -> Candidate Status: %s | Classification: %s | Whitelisted: %s",
            clean_addr[:10], chain.value, new_status, profile.classification.value, profile.is_whitelisted
        )

    async def evaluate_candidate(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
    ) -> Optional[WalletProfile]:
        """
        Execute full trade reconstruction and evaluation for a candidate wallet.
        Respects free-tier provider daily quota.
        """
        clean_addr = wallet_address.lower().strip()
        try:
            profile = await self.profiler.profile_wallet(
                wallet_address=clean_addr,
                chain=chain,
            )
        except (ProviderQuotaExceededError, ProviderUnavailableError) as exc:
            logger.warning(
                "History provider quota reached or unavailable for %s on %s: %s. Deferring candidate.",
                clean_addr[:10], chain.value, exc
            )
            now_ts = int(time.time())
            deferred_profile = WalletProfile(
                wallet_address=clean_addr,
                chain=chain,
                classification=WalletClassification.INSUFFICIENT_DATA,
                is_whitelisted=False,
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                win_rate_pct=0.0,
                total_pnl_usd=Decimal(0),
                max_single_trade_pnl_usd=Decimal(0),
                outlier_pnl_ratio=0.0,
                median_holding_time_seconds=0.0,
                active_days=0.0,
                days_since_last_active=0.0,
                first_tx_timestamp=now_ts,
                last_tx_timestamp=now_ts,
                rejection_reasons=[f"Quota exceeded / provider unavailable: {exc}"],
                smart_money_score=0.0,
            )
            await self.record_candidate_evaluation(clean_addr, chain, deferred_profile)
            return deferred_profile

        await self.record_candidate_evaluation(clean_addr, chain, profile)
        return profile

    async def _candidate_evaluator_worker(self) -> None:
        """Background worker that continuously pulls candidates and profiles them."""
        eval_interval = float(getattr(self.config, "wallet_eval_interval_s", 15.0))
        while not self._stop_event.is_set():
            try:
                cand = await self.get_next_candidate_for_evaluation()
                if cand:
                    addr = cand["wallet_address"]
                    ch = ChainIdentifier(cand["chain"])
                    await self.evaluate_candidate(addr, ch)
                else:
                    # Nothing in queue; sleep
                    await asyncio.sleep(eval_interval)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in candidate evaluator worker: %s", exc, exc_info=True)
                await asyncio.sleep(5.0)

    # -----------------------------------------------------------------------
    # 3. Whitelist Periodic Re-evaluation
    # -----------------------------------------------------------------------

    async def re_evaluate_whitelisted_wallets(self) -> int:
        """
        Re-evaluates all currently active whitelisted wallets.
        Demotes wallets that fell below performance thresholds or went dormant.
        """
        active_records: list[WhitelistRecord] = await self.whitelist_db.list_active()
        demoted = 0
        for rec in active_records:
            try:
                # Force refresh trades
                profile = await self.profiler.profile_wallet(
                    wallet_address=rec.wallet_address,
                    chain=rec.chain,
                )
                if not profile.is_whitelisted:
                    reasons = "; ".join(profile.rejection_reasons)
                    await self.whitelist_db.update_status(
                        wallet_address=rec.wallet_address,
                        status=WhitelistStatus.SUSPENDED,
                        reason=f"Periodic re-evaluation failed: {reasons}",
                    )
                    demoted += 1
                else:
                    # Refresh score and timestamp in DB
                    await self.whitelist_db.upsert_wallet(profile, status=WhitelistStatus.ACTIVE)
            except Exception as exc:
                logger.warning("Failed re-evaluating whitelisted wallet %s: %s", rec.wallet_address[:10], exc)

        logger.info(
            "Periodic re-evaluation completed: %d active wallets checked, %d demoted",
            len(active_records), demoted
        )
        return demoted

    async def _re_evaluation_worker(self) -> None:
        """Background loop executing periodic re-evaluation."""
        interval_s = float(getattr(self.config, "wallet_re_eval_interval_s", 3600.0))
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(interval_s)
                if self._stop_event.is_set():
                    break
                await self.re_evaluate_whitelisted_wallets()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in re-evaluation worker: %s", exc, exc_info=True)
                await asyncio.sleep(30.0)

    # -----------------------------------------------------------------------
    # 4. Live Tracking & WHALE_WALLET Signal Emission
    # -----------------------------------------------------------------------

    async def on_swap_event(
        self,
        swap: SwapEvent,
        pool_registry: Optional[Any] = None,
        gatekeeper: Optional[Any] = None,
    ) -> Optional[SignalEvent]:
        """
        Inspect incoming DEX swap events. If initiated by an active whitelisted wallet
        buying a token, dedupes, computes conviction & cluster size, and emits a
        SignalSource.WHALE_WALLET signal into the normal execution path.
        """
        sender_addr = swap.sender.lower().strip()
        chain = swap.chain
        now = time.time()

        # 1. Fast check against whitelist DB
        is_wl = await self.whitelist_db.is_whitelisted(sender_addr, chain)
        if not is_wl:
            return None

        # 2. Check if this swap is a BUY (buying token_out with native asset)
        # On Base: token_in is WETH; on Solana: token_in is WSOL / pump bonding curve
        # Or swap.amount_in > 0 and token_out is target traded token
        target_token = swap.token_out.strip()
        if not target_token:
            return None

        # 2b. Gatekeeper Security Screening (Fail-Closed)
        sec_report = None
        if gatekeeper is not None:
            try:
                sec_report = await gatekeeper.screen_token(
                    token_address=target_token,
                    chain=chain,
                    pool_address=swap.pool_address,
                )
            except Exception as exc:
                logger.debug("Gatekeeper screening raised for whale swap token %s: %s", target_token[:10], exc)
                return None

            if sec_report is not None and not sec_report.passes_hard_gates:
                logger.info(
                    "Whale swap token %s rejected by Gatekeeper: honeypot=%s buy_tax=%s sell_tax=%s",
                    target_token[:10], sec_report.is_honeypot, sec_report.buy_tax_bps, sec_report.sell_tax_bps,
                )
                return None

        # 3. Deduplication check
        if swap.tx_hash in self._recent_swaps_seen:
            return None
        self._recent_swaps_seen[swap.tx_hash] = now
        # Clean old tx hashes
        cutoff = now - 300.0
        self._recent_swaps_seen = {k: v for k, v in self._recent_swaps_seen.items() if v >= cutoff}

        wallet_key = (sender_addr, target_token.lower())
        last_buy = self._wallet_recent_buys.get(wallet_key, 0.0)
        if (now - last_buy) < self._dedupe_cooldown_s:
            logger.debug(
                "Skipping duplicate buy signal for %s on token %s (cooldown %.1fs)",
                sender_addr[:10], target_token[:10], now - last_buy
            )
            return None
        self._wallet_recent_buys[wallet_key] = now

        # 4. Retrieve Whitelist Record for Smart Money Score
        record: Optional[WhitelistRecord] = await self.whitelist_db.get_wallet(sender_addr)
        sm_score = float(record.smart_money_score) if record else 0.70

        # 5. Conviction calculation (size relative to 1.0 native baseline)
        base_amt = Decimal("1.0")
        size_ratio = float(swap.amount_in / base_amt) if base_amt > Decimal(0) else 1.0
        base_conviction = min(3.0, max(0.5, size_ratio))

        # 6. Cluster Agreement Check (multiple uncorrelated smart wallets buying same token in 120s)
        cluster_triggered, qualifying = self.profiler.record_wallet_purchase(
            token_address=target_token,
            wallet_address=sender_addr,
            smart_money_score=sm_score,
            timestamp_s=now,
            cluster_tag=getattr(record, "cluster_tag", None),
        )

        cluster_size = max(1, len(qualifying))
        if cluster_triggered and cluster_size >= 2:
            # Cluster agreement increases conviction multiplier
            cluster_mult = 1.0 + (0.5 * (cluster_size - 1))
            wallet_conviction = round(base_conviction * cluster_mult, 3)
            alpha_score = min(1.0, 0.65 + 0.15 * (cluster_size - 1) + 0.10 * sm_score)
            strength = SignalStrength.STRONG
        else:
            wallet_conviction = round(base_conviction, 3)
            alpha_score = min(1.0, 0.50 + 0.30 * sm_score)
            strength = SignalStrength.STRONG if sm_score >= 0.80 else SignalStrength.MODERATE

        # 7. Construct and emit SignalEvent
        pool_st = None
        if pool_registry is not None:
            pool_st = pool_registry.get(swap.pool_address)
        if pool_st is None:
            # Minimal pool state wrapper
            pool_st = PoolState(
                pool_address=swap.pool_address,
                chain=chain,
                token_address=target_token,
                native_reserve=swap.amount_in * Decimal("10"),
                token_reserve=swap.amount_out * Decimal("10"),
                spot_price_native_per_token=(swap.amount_in / swap.amount_out) if swap.amount_out > 0 else Decimal(0),
                fee_numerator=30,
                fee_denominator=10000,
                total_supply=Decimal("1000000000"),
                circulating_supply=Decimal("1000000000"),
                timestamp_s=now,
                last_updated_block=swap.block_number,
            )

        sig_mcap = None
        if pool_st is not None:
            base_p = Decimal("3000.0") if chain == ChainIdentifier.BASE_MAINNET else Decimal("200.0")
            sig_mcap = pool_st.calculate_market_cap_usd(native_price_usd=base_p)
        if sig_mcap is None or sig_mcap <= Decimal(0):
            sig_mcap = Decimal("50000.0")

        if sec_report is None:
            sec_report = SecurityReport(
                token_address=target_token,
                chain=chain,
                tier=SecurityTier.CLEAN,
                is_honeypot=False,
                buy_tax_bps=0,
                sell_tax_bps=0,
                lp_burned_ratio=1.0,
                top10_concentration=0.1,
                mint_authority_disabled=True,
                verified_source_code=True,
            )

        signal = SignalEvent(
            timestamp_ns=swap.timestamp_ns or time.time_ns(),
            chain=chain,
            pool_address=swap.pool_address,
            token_address=target_token,
            suggested_side=OrderSide.BUY,
            trigger_swap=swap,
            pool_state=pool_st,
            security_report=sec_report,
            market_cap_usd=sig_mcap,
            strength=strength,
            alpha_score=alpha_score,
            source=SignalSource.WHALE_WALLET,
            wallet_address=sender_addr,
            wallet_score=sm_score,
            cluster_size=cluster_size,
            wallet_conviction=wallet_conviction,
            wallet_provenance=getattr(record, "cluster_tag", "SMART_MONEY_INGEST"),
            qualifying_wallets=qualifying or [sender_addr],
        )

        self._signals_emitted_count += 1
        logger.info(
            "Emitted WHALE_WALLET signal for token %s on %s by wallet %s (score=%.2f, cluster=%d, conviction=%.2f, alpha=%.3f)",
            target_token[:10], chain.value, sender_addr[:10], sm_score, cluster_size, wallet_conviction, alpha_score
        )
        return signal

    # -----------------------------------------------------------------------
    # 5. Copy-Trade Attribution & Demotion on Real Outcomes
    # -----------------------------------------------------------------------

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def record_copied_trade_outcome(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        realized_pnl_usd: Decimal,
        is_win: bool,
        lot_status: Optional[LotStatus] = None,
    ) -> None:
        """
        Record closed paper trade outcome for a copied smart wallet.
        If wallet hits 3 consecutive losses or net negative copy PnL, demotes automatically.
        """
        if lot_status is not None and lot_status not in (LotStatus.CLOSED, LotStatus.SETTLED):
            logger.debug("Skipping unclosed lot outcome recording for %s (status=%s)", wallet_address[:10], lot_status)
            return

        clean_addr = wallet_address.lower().strip()
        await self.connect()
        assert self._conn is not None
        now_s = int(time.time())

        async with self._lock:
            cursor = await self._conn.execute(
                """
                SELECT total_copied_trades, winning_copied_trades, consecutive_losses, total_copied_pnl_usd
                FROM wallet_copy_performance
                WHERE chain = ? AND wallet_address = ?;
                """,
                (chain.value, clean_addr),
            )
            row = await cursor.fetchone()
            if row:
                tot = int(row["total_copied_trades"]) + 1
                wins = int(row["winning_copied_trades"]) + (1 if is_win else 0)
                streak = 0 if is_win else (int(row["consecutive_losses"]) + 1)
                pnl = Decimal(row["total_copied_pnl_usd"]) + realized_pnl_usd
                await self._conn.execute(
                    """
                    UPDATE wallet_copy_performance
                    SET total_copied_trades = ?, winning_copied_trades = ?, consecutive_losses = ?,
                        total_copied_pnl_usd = ?, last_copied_at_s = ?
                    WHERE chain = ? AND wallet_address = ?;
                    """,
                    (tot, wins, streak, str(pnl), now_s, chain.value, clean_addr),
                )
            else:
                tot = 1
                wins = 1 if is_win else 0
                streak = 0 if is_win else 1
                pnl = realized_pnl_usd
                await self._conn.execute(
                    """
                    INSERT INTO wallet_copy_performance (
                        wallet_address, chain, total_copied_trades, winning_copied_trades,
                        consecutive_losses, total_copied_pnl_usd, last_copied_at_s
                    ) VALUES (?, ?, ?, ?, ?, ?, ?);
                    """,
                    (clean_addr, chain.value, tot, wins, streak, str(pnl), now_s),
                )
            await self._conn.commit()

        # Automatic Demotion Hook:
        # If wallet suffered 3 consecutive losing copied trades OR net negative PnL with >= 3 trades
        if streak >= 3 or (tot >= 3 and pnl < Decimal("-100.0")):
            demote_reason = (
                f"Copy-trade performance failure: {streak} consecutive losses"
                if streak >= 3
                else f"Copy-trade negative net PnL: ${float(pnl):,.2f}"
            )
            await self.whitelist_db.update_status(
                wallet_address=clean_addr,
                status=WhitelistStatus.SUSPENDED,
                reason=demote_reason,
            )
            logger.warning(
                "Whitelisted wallet %s auto-demoted to SUSPENDED: %s",
                clean_addr[:10], demote_reason
            )

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_copy_performance(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
    ) -> Optional[dict[str, Any]]:
        """
        Retrieve persisted copy-trade performance for a wallet.
        """
        clean_addr = wallet_address.lower().strip()
        await self.connect()
        assert self._conn is not None

        async with self._lock:
            cursor = await self._conn.execute(
                """
                SELECT wallet_address, chain, total_copied_trades, winning_copied_trades,
                       consecutive_losses, total_copied_pnl_usd, last_copied_at_s
                FROM wallet_copy_performance
                WHERE chain = ? AND wallet_address = ?;
                """,
                (chain.value, clean_addr),
            )
            row = await cursor.fetchone()
            if not row:
                return None
            return {
                "wallet_address": row["wallet_address"],
                "chain": row["chain"],
                "total_copied_trades": int(row["total_copied_trades"]),
                "winning_copied_trades": int(row["winning_copied_trades"]),
                "consecutive_losses": int(row["consecutive_losses"]),
                "total_copied_pnl_usd": row["total_copied_pnl_usd"],
                "last_copied_at_s": int(row["last_copied_at_s"]),
            }

    # -----------------------------------------------------------------------
    # 6. Observability Metrics
    # -----------------------------------------------------------------------

    @sqlite_retry(max_retries=5, base_delay=0.1)
    async def get_metrics(self) -> dict[str, Any]:
        """
        Compile live pipeline state metrics for dashboard and /status:
        - candidates_count
        - queue_depth
        - whitelisted_count
        - evaluations_per_hour
        - quota_usage
        - signals_emitted
        """
        await self.connect()
        assert self._conn is not None

        # 1. Candidates & Queue Depth
        cursor = await self._conn.execute("SELECT COUNT(*) as total FROM wallet_candidates;")
        row = await cursor.fetchone()
        candidates_count = int(row["total"]) if row else 0

        cursor = await self._conn.execute(
            "SELECT COUNT(*) as pending FROM wallet_candidates WHERE status IN ('PENDING', 'DEFERRED');"
        )
        row = await cursor.fetchone()
        queue_depth = int(row["pending"]) if row else 0

        # 2. Whitelisted Count
        active_wl = await self.whitelist_db.list_active()
        whitelisted_count = len(active_wl)

        # 3. Evaluations Per Hour
        now = time.time()
        one_hour_ago = now - 3600.0
        while self._evaluations_history and self._evaluations_history[0] < one_hour_ago:
            self._evaluations_history.popleft()
        evaluations_per_hour = len(self._evaluations_history)

        # 4. Quota Usage
        quota_usage = {"used": 0, "limit": 1000}
        if self.history_db is not None:
            try:
                tot_used = await self.history_db.get_daily_quota_usage()
                limit = int(getattr(self.config, "wallet_history_free_tier_daily_quota", 1000))
                quota_usage = {"used": tot_used, "limit": limit}
            except Exception as q_err:
                logger.debug("Error computing quota usage: %s", q_err)

        res = {
            "candidates_count": candidates_count,
            "queue_depth": queue_depth,
            "whitelisted_count": whitelisted_count,
            "evaluations_per_hour": evaluations_per_hour,
            "quota_usage": quota_usage,
            "signals_emitted": self._signals_emitted_count,
        }
        self._cached_metrics = res
        return res

    def get_cached_metrics(self) -> dict[str, Any]:
        """
        Synchronous access to most recent pipeline metrics snapshot.
        """
        return dict(self._cached_metrics)
