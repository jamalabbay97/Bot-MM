"""
alpha_engine.profiler.profiler — Smart Money Behavioral & Cluster Profiler
==========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | asyncio
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
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
from alpha_engine.profiler.history_db import WalletHistoryDatabase
from alpha_engine.profiler.history_provider import ProviderUnavailableError, WalletHistoryProvider
from alpha_engine.profiler.price_oracle import HistoricalPriceOracle
from alpha_engine.profiler.reconstructor import TradeReconstructor
from alpha_engine.profiler.whitelist_db import WhitelistDatabase
from alpha_engine.rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)


@dataclass
class WalletTradeActivity:
    wallet_address: str
    timestamp_s: float
    smart_money_score: float
    cluster_tag: Optional[str] = None
    funding_source: Optional[str] = None

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
        history_provider: Optional[WalletHistoryProvider] = None,
        reconstructor: Optional[TradeReconstructor] = None,
        history_db: Optional[WalletHistoryDatabase] = None,
        price_oracle: Optional[HistoricalPriceOracle] = None,
    ) -> None:
        self.rate_limiters = rate_limiters or RateLimiterRegistry.default()
        self.whitelist_db = whitelist_db or WhitelistDatabase(db_path=db_path)
        self.history_db = history_db or WalletHistoryDatabase(db_path=db_path)
        self.price_oracle = price_oracle or HistoricalPriceOracle(history_db=self.history_db)
        self.reconstructor = reconstructor or TradeReconstructor(price_oracle=self.price_oracle)
        self.history_provider = history_provider
        self.evaluator = WalletEvaluator()
        self.funding_fetcher = funding_fetcher
        self.db_path = db_path
        # token_address -> list of WalletTradeActivity
        self._recent_purchases: dict[str, list[WalletTradeActivity]] = defaultdict(list)

    async def initialize(self) -> None:
        """Initialize database tables and connections."""
        await self.whitelist_db.connect()
        if self.history_db is not None:
            await self.history_db.connect()

    async def get_wallet_trade_records(
        self,
        wallet_address: str,
        chain: ChainIdentifier,
        since: Optional[int] = None,
        until: Optional[int] = None,
    ) -> list[WalletTradeRecord]:
        """
        Fetch historical swaps via history_provider, reconstruct closed trades,
        and persist both raw swaps and reconstructed trade records to SQLite.
        """
        clean_wallet = wallet_address.strip()
        # 1. Check SQLite for already persisted reconstructed trades
        if self.history_db is not None:
            cached_trades = await self.history_db.get_reconstructed_trades(
                wallet_address=clean_wallet, chain=chain
            )
            if cached_trades:
                return cached_trades

        # 2. Instantiate default provider from configuration if none provided
        if self.history_provider is None:
            from alpha_engine.config import EngineConfig
            from alpha_engine.profiler.solana_history import HeliusSolanaHistoryProvider
            from alpha_engine.profiler.base_history import AlchemyBaseHistoryProvider

            cfg = EngineConfig()
            if chain == ChainIdentifier.SOLANA_MAINNET:
                api_key = cfg.helius_api_key
                if not api_key and "api-key=" in cfg.solana_rpc_http:
                    api_key = cfg.solana_rpc_http.split("api-key=")[-1].split("&")[0]
                self.history_provider = HeliusSolanaHistoryProvider(
                    api_key=api_key,
                    history_db=self.history_db,
                    rate_limiters=self.rate_limiters,
                    daily_request_quota=cfg.wallet_history_free_tier_daily_quota,
                    backfill_window_days=cfg.wallet_history_backfill_days,
                )
            else:
                api_key = cfg.alchemy_api_key
                if not api_key and "/v2/" in cfg.base_rpc_http:
                    api_key = cfg.base_rpc_http.split("/v2/")[-1].split("?")[0]
                self.history_provider = AlchemyBaseHistoryProvider(
                    api_key=api_key,
                    history_db=self.history_db,
                    rate_limiters=self.rate_limiters,
                    daily_request_quota=cfg.wallet_history_free_tier_daily_quota,
                    backfill_window_days=cfg.wallet_history_backfill_days,
                )

        swaps = await self.history_provider.fetch_swaps(
            wallet=clean_wallet, chain=chain, since=since, until=until
        )

        reconstruction = await self.reconstructor.reconstruct_trades(
            swaps=swaps, chain=chain, wallet_address=clean_wallet
        )

        if self.history_db is not None and reconstruction.closed_trades:
            await self.history_db.save_reconstructed_trades(
                wallet_address=clean_wallet,
                chain=chain,
                trades=reconstruction.closed_trades,
            )
            await self.history_db.save_reconstructed_metrics(
                wallet_address=clean_wallet,
                chain=chain,
                metrics={
                    "total_closed_trades": len(reconstruction.closed_trades),
                    "winning_trades": sum(1 for t in reconstruction.closed_trades if t.is_win),
                    "losing_trades": sum(1 for t in reconstruction.closed_trades if not t.is_win),
                    "win_rate_pct": reconstruction.win_rate_pct,
                    "total_pnl_usd": str(reconstruction.total_realized_pnl_usd),
                    "max_pnl_usd": str(max((t.realized_pnl_usd for t in reconstruction.closed_trades), default=Decimal(0))),
                    "median_holding_time_s": 0.0,
                    "open_positions_count": len(reconstruction.open_positions),
                },
            )

        return reconstruction.closed_trades

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
        trades: Optional[Sequence[WalletTradeRecord]] = None,
        initial_txs: Optional[Sequence[InitialTxRecord]] = None,
        deployer_addresses: Optional[Set[str]] = None,
        multisig_addresses: Optional[Set[str]] = None,
        known_funding_hops: Optional[Sequence[FundingHop]] = None,
        current_timestamp: Optional[int] = None,
        cluster_tag: Optional[str] = None,
    ) -> WalletProfile:
        """
        Evaluate a target wallet against all quantitative alpha and behavioral filters.
        If trades is None, fetches historical swaps via history_provider and reconstructs trades.
        """
        if trades is None:
            try:
                trades = await self.get_wallet_trade_records(
                    wallet_address=wallet_address,
                    chain=chain,
                )
            except ProviderUnavailableError as exc:
                logger.warning(
                    "Wallet %s history unavailable on %s: %s",
                    wallet_address[:10], chain.value, exc
                )
                now_ts = current_timestamp or int(time.time())
                return WalletProfile(
                    wallet_address=wallet_address,
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
                    cluster_tag=cluster_tag,
                    rejection_reasons=[f"insufficient data: history provider unavailable ({exc})"],
                    smart_money_score=0.0,
                )

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
        if self.history_db is not None:
            await self.history_db.close()

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

        target_chain = chain or ChainIdentifier.SOLANA_MAINNET

        # Filter candidate wallets by address format if chain is specified
        if chain == ChainIdentifier.BASE_MAINNET:
            candidates = [c for c in candidates if c.startswith("0x") and 40 <= len(c) <= 46]
        elif chain == ChainIdentifier.SOLANA_MAINNET:
            candidates = [c for c in candidates if 32 <= len(c) <= 44 and not c.startswith("0x")]

        logger.info(
            "Reverse-engineered winning token %s (+%.1f%% in %.0fs) on %s: found %d candidate smart wallets",
            token_address[:10], price_gain_pct, duration_seconds, target_chain.value, len(candidates)
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

    def record_wallet_purchase(
        self,
        token_address: str,
        wallet_address: str,
        smart_money_score: float,
        timestamp_s: Optional[float] = None,
        cluster_tag: Optional[str] = None,
        funding_source: Optional[str] = None,
    ) -> tuple[bool, list[str]]:
        """
        Record a purchase by a smart money wallet and evaluate copy-trade gating.
        Rule: Only execute copy-trade signals when at least 2 distinct, uncorrelated
        high-scoring wallets (S_wallet >= 0.75) purchase the same asset within a 120-second window.
        Returns (should_execute_copy_trade, qualifying_wallets).
        """
        now = timestamp_s if timestamp_s is not None else time.time()
        tok = token_address.lower().strip()
        wallet = wallet_address.lower().strip()

        # Clean entries older than 120 seconds
        cutoff = now - 120.0
        self._recent_purchases[tok] = [
            act for act in self._recent_purchases[tok] if act.timestamp_s >= cutoff
        ]

        # Record this purchase
        activity = WalletTradeActivity(
            wallet_address=wallet,
            timestamp_s=now,
            smart_money_score=smart_money_score,
            cluster_tag=cluster_tag,
            funding_source=funding_source,
        )
        self._recent_purchases[tok].append(activity)

        # Filter for high-scoring wallets S >= 0.75
        high_scorers = [
            act for act in self._recent_purchases[tok] if act.smart_money_score >= 0.75
        ]

        # Check for at least 2 distinct, uncorrelated wallets
        qualifying: list[WalletTradeActivity] = []
        for act in high_scorers:
            if not qualifying:
                qualifying.append(act)
                continue
            is_correlated = False
            for q in qualifying:
                if q.wallet_address == act.wallet_address:
                    is_correlated = True
                    break
                if q.cluster_tag and act.cluster_tag and q.cluster_tag.lower() == act.cluster_tag.lower():
                    is_correlated = True
                    break
                if q.funding_source and act.funding_source and q.funding_source.lower() == act.funding_source.lower():
                    is_correlated = True
                    break
            if not is_correlated:
                qualifying.append(act)

        if len(qualifying) >= 2:
            wallet_addrs = [q.wallet_address for q in qualifying]
            logger.info(
                "COPY-TRADE TRIGGERED for token %s: %d distinct uncorrelated wallets (S >= 0.75) within 120s: %s",
                tok[:10],
                len(wallet_addrs),
                wallet_addrs,
            )
            return True, wallet_addrs

        return False, []

    check_copy_trade_trigger = record_wallet_purchase

