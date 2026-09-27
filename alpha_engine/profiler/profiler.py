"""
alpha_engine.profiler.profiler — Smart Money Behavioral & Cluster Profiler
==========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | asyncio
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from decimal import Decimal
from typing import Any, Callable, Coroutine, Optional, Sequence, Set

from alpha_engine.models.enums import ChainIdentifier, WalletClassification, WhitelistStatus
from alpha_engine.models.profiler import (
    FundingHop,
    InitialTxRecord,
    WalletProfile,
    WalletTradeRecord,
    WhitelistRecord,
)
from alpha_engine.profiler.evaluator import WalletEvaluator
from alpha_engine.profiler.whitelist_db import WhitelistDatabase
from alpha_engine.rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)

# Type alias for external transaction / funding fetcher callable
FundingFetcher = Callable[
    [str, ChainIdentifier],
    Coroutine[None, None, list[tuple[str, Decimal, str]]],  # returns (source_address, amount, tx_hash)
]


class SmartMoneyProfiler:
    """
    Module A: Smart Money Behavioral & Cluster Profiler.

    Evaluates on-chain wallets, filters out traps (wash-trading, MEV bots,
    lucky outliers, insider deployer clusters, dead-wallet revivals), and maintains
    the curated alpha whitelist database.
    """

    def __init__(
        self,
        rate_limiters: Optional[RateLimiterRegistry] = None,
        whitelist_db: Optional[WhitelistDatabase] = None,
        db_path: str = "paper_trading.db",
        funding_fetcher: Optional[FundingFetcher] = None,
    ) -> None:
        self.rate_limiters = rate_limiters or RateLimiterRegistry.default()
        self.whitelist_db = whitelist_db or WhitelistDatabase(db_path=db_path)
        self.evaluator = WalletEvaluator()
        self.funding_fetcher = funding_fetcher

    async def initialize(self) -> None:
        """Initialize database tables and connections."""
        await self.whitelist_db.connect()

    async def profile_wallets_batch(
        self,
        tasks: Sequence[tuple[str, ChainIdentifier, list[WalletTradeRecord], list[InitialTxRecord]]],
    ) -> list[WalletProfile]:
        """Concurrently profile multiple candidate wallets."""
        coros = [
            self.profile_and_whitelist_wallet(
                wallet_address=addr,
                chain=chain,
                trade_records=trades,
                first_transactions=first_txs,
            )
            for addr, chain, trades, first_txs in tasks
        ]
        return list(await asyncio.gather(*coros))

    async def trace_funding_hops(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        deployer_addresses: Optional[Set[str]] = None,
        multisig_addresses: Optional[Set[str]] = None,
        max_hops: int = 3,
        initial_hops: Optional[Sequence[FundingHop]] = None,
    ) -> list[FundingHop]:
        """
        Execute Breadth-First Search (BFS) up to `max_hops` (<= 3) to trace
        the origin of initial gas/funds back to token deployers or shared multi-sigs.
        """
        deployers = {d.lower() for d in (deployer_addresses or set())}
        multisigs = {m.lower() for m in (multisig_addresses or set())}

        result_hops: list[FundingHop] = []

        # If static/known hops were provided (e.g. from indexed records)
        if initial_hops:
            for hop in initial_hops:
                is_dep = hop.source_address.lower() in deployers or hop.is_deployer
                is_ms = hop.source_address.lower() in multisigs or hop.is_multisig
                updated_hop = hop.model_copy(
                    update={"is_deployer": is_dep, "is_multisig": is_ms}
                )
                result_hops.append(updated_hop)
                if is_dep or is_ms:
                    return result_hops

        # If an asynchronous funding fetcher is configured, perform live BFS with rate limiting
        if self.funding_fetcher is not None:
            visited: Set[str] = {wallet_address.lower()}
            queue: deque[tuple[str, int]] = deque([(wallet_address.lower(), 1)])

            while queue:
                curr_addr, depth = queue.popleft()
                if depth > max_hops:
                    break

                # Enforce free-tier developer RPC rate limiter
                await self.rate_limiters.acquire_for(chain, cost=1.0)

                try:
                    parents = await self.funding_fetcher(curr_addr, chain)
                except Exception as exc:
                    logger.warning(
                        "Failed to fetch funding parents for %s on %s: %s",
                        curr_addr,
                        chain.value,
                        exc,
                    )
                    continue

                for parent_addr, amount, tx_hash in parents:
                    p_lower = parent_addr.lower()
                    is_dep = p_lower in deployers
                    is_ms = p_lower in multisigs

                    hop_rec = FundingHop(
                        source_address=parent_addr,
                        destination_address=curr_addr,
                        tx_hash=tx_hash,
                        hop_depth=depth,
                        amount_native=amount,
                        is_deployer=is_dep,
                        is_multisig=is_ms,
                    )
                    result_hops.append(hop_rec)

                    if is_dep or is_ms:
                        logger.warning(
                            "Insider/deployer link confirmed at hop %d: %s -> %s",
                            depth,
                            parent_addr,
                            curr_addr,
                        )
                        return result_hops

                    if p_lower not in visited and depth < max_hops:
                        visited.add(p_lower)
                        queue.append((p_lower, depth + 1))

        return result_hops

    async def profile_wallet(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        trades: Sequence[WalletTradeRecord],
        initial_txs: Optional[Sequence[InitialTxRecord]] = None,
        deployer_addresses: Optional[Set[str]] = None,
        multisig_addresses: Optional[Set[str]] = None,
        known_funding_hops: Optional[Sequence[FundingHop]] = None,
        current_timestamp: Optional[int] = None,
        cluster_tag: Optional[str] = None,
    ) -> WalletProfile:
        """
        Evaluate a target wallet against all quantitative alpha and behavioral filters.
        """
        # 1. Trace funding provenance <= 3 hops
        hops = await self.trace_funding_hops(
            wallet_address=wallet_address,
            chain=chain,
            deployer_addresses=deployer_addresses,
            multisig_addresses=multisig_addresses,
            max_hops=3,
            initial_hops=known_funding_hops,
        )

        # 2. Run behavioral evaluator rules
        profile = self.evaluator.evaluate(
            wallet_address=wallet_address,
            chain=chain,
            trades=trades,
            initial_txs=initial_txs,
            funding_hops=hops,
            current_timestamp=current_timestamp,
            cluster_tag=cluster_tag,
        )

        logger.info(
            "Profiled wallet %s on %s: %s | Win Rate: %.1f%% | Trades: %d | Whitelisted: %s",
            wallet_address,
            chain.value,
            profile.classification.value,
            profile.win_rate_pct,
            profile.total_trades,
            profile.is_whitelisted,
        )

        return profile

    async def evaluate_and_whitelist(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        trades: Sequence[WalletTradeRecord],
        initial_txs: Optional[Sequence[InitialTxRecord]] = None,
        deployer_addresses: Optional[Set[str]] = None,
        multisig_addresses: Optional[Set[str]] = None,
        known_funding_hops: Optional[Sequence[FundingHop]] = None,
        current_timestamp: Optional[int] = None,
        cluster_tag: Optional[str] = None,
    ) -> tuple[bool, WalletProfile]:
        """
        Profile a wallet and automatically persist approved wallets to the local SQLite whitelist.
        If flagged as insider or wash-trader, immediately marks as BANNED.
        """
        await self.initialize()

        profile = await self.profile_wallet(
            wallet_address=wallet_address,
            chain=chain,
            trades=trades,
            initial_txs=initial_txs,
            deployer_addresses=deployer_addresses,
            multisig_addresses=multisig_addresses,
            known_funding_hops=known_funding_hops,
            current_timestamp=current_timestamp,
            cluster_tag=cluster_tag,
        )

        status = WhitelistStatus.ACTIVE if profile.is_whitelisted else WhitelistStatus.BANNED
        await self.whitelist_db.upsert_wallet(profile, status=status)

        return profile.is_whitelisted, profile

    async def evaluate_batch(
        self,
        wallets_data: list[dict],
        chain: ChainIdentifier,
        deployer_addresses: Optional[Set[str]] = None,
        multisig_addresses: Optional[Set[str]] = None,
    ) -> dict[str, WalletProfile]:
        """
        Process a batch of candidate wallets sequentially to respect RPC rate limits.
        """
        results: dict[str, WalletProfile] = {}
        for item in wallets_data:
            addr = item["wallet_address"]
            trades = item.get("trades", [])
            initial_txs = item.get("initial_txs", None)
            cluster_tag = item.get("cluster_tag", None)
            hops = item.get("funding_hops", None)

            is_wl, prof = await self.evaluate_and_whitelist(
                wallet_address=addr,
                chain=chain,
                trades=trades,
                initial_txs=initial_txs,
                deployer_addresses=deployer_addresses,
                multisig_addresses=multisig_addresses,
                known_funding_hops=hops,
                cluster_tag=cluster_tag,
            )
            results[addr] = prof

        return results

    async def is_whitelisted(self, wallet_address: str, chain: Optional[ChainIdentifier] = None) -> bool:
        """Fast query against the local SQLite whitelist DB."""
        return await self.whitelist_db.is_whitelisted(wallet_address, chain=chain)

    async def get_whitelisted_wallets(self, chain: Optional[ChainIdentifier] = None) -> list[WhitelistRecord]:
        """Retrieve all active whitelisted wallets."""
        return await self.whitelist_db.list_active(chain=chain)

    async def close(self) -> None:
        """Release DB resources."""
        await self.whitelist_db.close()

    async def reverse_engineer_winning_tokens(
        self,
        token_address: str,
        price_gain_pct: float,
        duration_seconds: float = 3600.0,
        early_trades: Sequence[dict[str, Any]] = (),
        creation_block: int = 0,
        creation_timestamp: int = 0,
        deployer_address: Optional[str] = None,
        deployer_cluster: Optional[Set[str] | Sequence[str]] = None,
        chain: Optional[ChainIdentifier] = None,
    ) -> list[str]:
        """
        Reverse-Engineers Winning Tokens:
        Every time a token runs > 300% within 1 hour:
        1. Identify all wallets that bought within the first 10 blocks
           (or first 2 minutes / 120s of curve creation).
        2. Filter out developer, deployer, and wallets linked to deployer cluster.
        Returns candidate smart wallet addresses.
        """
        if price_gain_pct < 300.0 or duration_seconds > 3600.0:
            return []

        dep_set = {d.lower() for d in (deployer_cluster or set())}
        if deployer_address:
            dep_set.add(deployer_address.lower())

        candidates: list[str] = []
        seen: set[str] = set()

        for t in early_trades:
            wallet = str(t.get("wallet", "")).lower()
            if not wallet or wallet in dep_set or wallet in seen:
                continue

            block = int(t.get("block_number", 0))
            ts = int(t.get("timestamp", 0))

            is_first_10_blocks = (creation_block > 0 and block > 0 and block <= creation_block + 10)
            is_first_2_mins = (creation_timestamp > 0 and ts > 0 and ts <= creation_timestamp + 120)

            if is_first_10_blocks or is_first_2_mins:
                seen.add(wallet)
                candidates.append(wallet)

        logger.info(
            "Reverse-engineered winning token %s (+%.1f%% in %.0fs): found %d candidate smart wallets",
            token_address[:10], price_gain_pct, duration_seconds, len(candidates)
        )
        return candidates

    async def evaluate_and_whitelist_autonomous(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        trades: Sequence[WalletTradeRecord],
        transfer_graph: Optional[Sequence[tuple[str, str]]] = None,
        deployer_addresses: Optional[Set[str]] = None,
        multisig_addresses: Optional[Set[str]] = None,
        known_funding_hops: Optional[Sequence[FundingHop]] = None,
        current_timestamp: Optional[int] = None,
        cluster_tag: Optional[str] = None,
    ) -> tuple[bool, WalletProfile]:
        """
        Evaluate a candidate smart wallet under autonomous rules:
        - 30-day lookback, min 25 trades, WR > 60% (>50% ROI), PF > 2.0, MDD < 35%, hold time > 3m.
        - Automatically inserts into whitelist_db if qualifying.
        - If circular wash or insider, marks BANNED immediately.
        """
        await self.initialize()

        hops = await self.trace_funding_hops(
            wallet_address=wallet_address,
            chain=chain,
            deployer_addresses=deployer_addresses,
            multisig_addresses=multisig_addresses,
            max_hops=3,
            initial_hops=known_funding_hops,
        )

        profile = self.evaluator.evaluate_autonomous(
            wallet_address=wallet_address,
            chain=chain,
            trades=trades,
            transfer_graph=transfer_graph,
            funding_hops=hops,
            current_timestamp=current_timestamp,
            cluster_tag=cluster_tag,
        )

        if profile.classification in (WalletClassification.CIRCULAR_WASH, WalletClassification.INSIDER, WalletClassification.WASH_TRADER):
            status = WhitelistStatus.BANNED
        elif profile.is_whitelisted:
            status = WhitelistStatus.ACTIVE
        else:
            status = WhitelistStatus.SUSPENDED

        await self.whitelist_db.upsert_wallet(profile, status=status)
        return profile.is_whitelisted, profile

    async def demote_or_ban_wallet(
        self,
        wallet_address: str,
        reason: str = "",
        status: WhitelistStatus = WhitelistStatus.BANNED,
        new_status: Optional[WhitelistStatus] = None,
    ) -> None:
        """Demote or ban an underperforming or malicious wallet."""
        actual_status = new_status or status
        await self.initialize()
        await self.whitelist_db.update_status(wallet_address, status=actual_status)
        logger.warning("Wallet %s demoted/banned (%s): %s", wallet_address[:10], actual_status.value, reason)

