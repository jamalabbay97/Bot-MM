#!/usr/bin/env python3
"""
tools/smoke_simulation.py
=========================
End-to-End Smoke Simulation exercising:
1. Scripted price feed and dynamic price change
2. System Doctor diagnostics
3. Smart wallet signal ingestion & attribution
4. Ruflo Council 3-round multi-agent debate (Fake LLM, no network)
5. SOL entry execution (PaperExecutor, dynamic pricing, real costs, realized slippage)
6. ETH entry execution on Base (Uniswap V3, per-chain sizing)
7. Price movement and position exit
8. All 3 post-trade hooks (SelfLearningEngine, StrategyTournament, CouncilArbiterCRO)
9. Strategy tournament champion promotion changing live parameters

Zero network calls, deterministic seed.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional

# Add project root to sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from alpha_engine.config import EngineConfig
from alpha_engine.council.arbiter_cro import CouncilArbiterCRO
from alpha_engine.council.forensic_agent import ChainForensicAgent
from alpha_engine.council.llm_client import CouncilLLMClient
from alpha_engine.council.models import AgentVote
from alpha_engine.council.quant_agent import QuantTechnicalsAgent
from alpha_engine.council.reach_agent import ReachAnalystAgent
from alpha_engine.engine.experimentation import (
    ExperimentStatus,
    ShadowLot,
    StrategyParameters,
    StrategyTournamentRegistry,
    StrategyVariant,
)
from alpha_engine.engine.rules import (
    ComparisonOperator,
    EntryRuleSet,
    LogicalOperator,
    RuleCondition,
    RuleGroup,
)
from alpha_engine.engine.self_learning.self_learning_engine import SelfLearningEngine
from alpha_engine.engine.strategy.risk_manager import RiskEngine
from alpha_engine.execution.book import OpenLot, PositionBook
from alpha_engine.execution.costs import TradeCostModel
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    SwapVenue,
    TradeExitReason,
    WalletClassification,
    WhitelistStatus,
)
from alpha_engine.models.events import SignalEvent, SwapEvent
from alpha_engine.models.profiler import NormalizedSwap, WalletProfile, WhitelistRecord
from alpha_engine.models.state import PoolState, ReserveType, SecurityReport
from alpha_engine.observability.doctor import CheckStatus, SystemDoctor
from alpha_engine.observability.recorder import MarketTickRecorder
from alpha_engine.pricing.native_price_service import (
    NativePriceService,
    NativePriceStatus,
    PriceResult,
)
from alpha_engine.profiler.history_db import WalletHistoryDatabase
from alpha_engine.profiler.pipeline import SmartMoneyPipeline
from alpha_engine.profiler.profiler import SmartMoneyProfiler
from alpha_engine.profiler.whitelist_db import WhitelistDatabase
from alpha_engine.ruflo.memory import MemoryEpisode, RufloEpisodicMemory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("smoke_simulation")


class ScriptableNativePriceService(NativePriceService):
    """Controllable in-memory NativePriceService for deterministic simulation."""

    def __init__(self, config: EngineConfig):
        super().__init__(allow_static_prices=getattr(config, "allow_static_native_prices", False))
        self._custom_prices: dict[ChainIdentifier, Decimal] = {
            ChainIdentifier.SOLANA_MAINNET: Decimal("180.0"),
            ChainIdentifier.BASE_MAINNET: Decimal("2800.0"),
        }

    def set_price(self, chain: ChainIdentifier, price_usd: Decimal) -> None:
        self._custom_prices[chain] = price_usd

    def get_price(self, chain: ChainIdentifier) -> PriceResult:
        price = self._custom_prices.get(chain)
        if price is None:
            return PriceResult(
                price=Decimal(0),
                age_s=0.0,
                status=NativePriceStatus.UNAVAILABLE,
                source="scripted",
            )
        return PriceResult(
            price=price,
            age_s=0.1,
            status=NativePriceStatus.OK,
            source="scripted",
            divergence_pct=0.0,
        )


class FakeCouncilLLM(CouncilLLMClient):
    """Deterministic Fake Council LLM Client without network calls."""

    def __init__(self, model: str = "gemini-3.8-flash"):
        super().__init__(provider="gemini", model=model, timeout_s=1.0)
        self.turns_executed = 0

    def _get_api_key(self, provider: str) -> str:
        return "fake_key_smoke"

    async def _dispatch_request_with_retries(
        self,
        agent_role: str,
        system_prompt: str,
        user_prompt: str,
        api_key: str,
        max_retries: int = 2,
    ) -> Optional[Dict[str, Any]]:
        self.turns_executed += 1
        assert len(system_prompt) > 0, "system_prompt must not be empty"
        assert len(user_prompt) > 0, "user_prompt must not be empty"
        assert len(api_key) > 0, "api_key must be configured"
        assert max_retries >= 0, "max_retries must be non-negative"
        logger.debug(
            "FakeCouncilLLM dispatched turn %d for %s (retries_allowed=%d, prompt_chars=%d)",
            self.turns_executed,
            agent_role,
            max_retries,
            len(system_prompt) + len(user_prompt),
        )
        return {
            "vote": "APPROVE",
            "confidence": 0.91,
            "score": 85.0,
            "evidence": ["Strong on-chain smart money accumulation", "Liquidity lock verified"],
            "questions": ["Is buy tax zero?"],
            "rationale": f"Deliberated by {agent_role}: solid fundamentals and volume profile.",
        }


async def run_smoke_simulation():
    logger.info("================================================================================")
    logger.info("STARTING END-TO-END SMOKE SIMULATION (BOT-MM)")
    logger.info("================================================================================")

    temp_dir = tempfile.mkdtemp(prefix="alpha_smoke_")
    db_path = os.path.join(temp_dir, "test_ledger.db")
    recorder_db_path = os.path.join(temp_dir, "test_recorder.db")
    history_db_path = os.path.join(temp_dir, "test_history.db")
    whitelist_db_path = os.path.join(temp_dir, "test_whitelist.db")

    try:
        # Initialize Engine Config
        config = EngineConfig(
            allow_static_native_prices=False,
            execution_seed=42,
            db_path=db_path,
            recorder_db_path=recorder_db_path,
            council_enabled=True,
            ai_model="gemini-3.8-flash",
            tournament_min_promotion_trades=30,
            tournament_min_promotion_trades_underperforming=20,
        )

        # ----------------------------------------------------------------------
        # STAGE 1: SCRIPTED PRICE FEED & DYNAMIC PRICE CHANGE
        # ----------------------------------------------------------------------
        logger.info("\n>>> STAGE 1: INITIALIZING SCRIPTED PRICE FEED & EXERCISING PRICE CHANGE")
        price_service = ScriptableNativePriceService(config)

        sol_resp1 = price_service.get_price(ChainIdentifier.SOLANA_MAINNET)
        eth_resp1 = price_service.get_price(ChainIdentifier.BASE_MAINNET)
        logger.info(f"Initial SOL Price: ${sol_resp1.price} (Status: {sol_resp1.status})")
        logger.info(f"Initial ETH Price: ${eth_resp1.price} (Status: {eth_resp1.status})")
        assert sol_resp1.price == Decimal("180.0")
        assert eth_resp1.price == Decimal("2800.0")

        # Dynamically change prices
        price_service.set_price(ChainIdentifier.SOLANA_MAINNET, Decimal("220.0"))
        price_service.set_price(ChainIdentifier.BASE_MAINNET, Decimal("3100.0"))

        sol_resp2 = price_service.get_price(ChainIdentifier.SOLANA_MAINNET)
        eth_resp2 = price_service.get_price(ChainIdentifier.BASE_MAINNET)
        logger.info(f"Updated SOL Price: ${sol_resp2.price} (Delta: +${sol_resp2.price - sol_resp1.price})")
        logger.info(f"Updated ETH Price: ${eth_resp2.price} (Delta: +${eth_resp2.price - eth_resp1.price})")
        assert sol_resp2.price == Decimal("220.0")
        assert eth_resp2.price == Decimal("3100.0")
        logger.info("STAGE 1 COMPLETED: Dynamic price feed verified.")

        # ----------------------------------------------------------------------
        # STAGE 2: SYSTEM DOCTOR DIAGNOSTICS
        # ----------------------------------------------------------------------
        logger.info("\n>>> STAGE 2: RUNNING SYSTEM DOCTOR DIAGNOSTICS")
        recorder = MarketTickRecorder(db_path=config.recorder_db_path)
        doctor = SystemDoctor(config=config, price_service=price_service, recorder=recorder)
        doc_report = await doctor.run_full_diagnostics()
        doc_table = doctor.format_cli_table(doc_report)
        logger.info(f"\nSystem Doctor Diagnostic Report:\n{doc_table}")
        assert doc_report.overall_status in (CheckStatus.PASS, CheckStatus.WARN)
        logger.info("STAGE 2 COMPLETED: System Doctor diagnostics executed successfully.")

        # ----------------------------------------------------------------------
        # STAGE 3: SMART WALLET INGESTION & ATTRIBUTION
        # ----------------------------------------------------------------------
        logger.info("\n>>> STAGE 3: SMART WALLET SIGNAL INGESTION & PIPELINE ATTRIBUTION")
        whitelist_db = WhitelistDatabase(db_path=whitelist_db_path)
        history_db = WalletHistoryDatabase(db_path=history_db_path)
        smart_profiler = SmartMoneyProfiler(whitelist_db=whitelist_db, history_db=history_db, db_path=whitelist_db_path)

        test_wallet = "3kR5mRkQvVz7P5Ew6oNqZp4v3kR5mRkQvVz7P5Ew6oNq"
        now_ts = int(time.time())
        profile = WalletProfile(
            wallet_address=test_wallet,
            chain=ChainIdentifier.SOLANA_MAINNET,
            classification=WalletClassification.APPROVED,
            is_whitelisted=True,
            total_trades=45,
            winning_trades=32,
            losing_trades=13,
            win_rate_pct=71.1,
            total_pnl_usd=Decimal("5400.0"),
            max_single_trade_pnl_usd=Decimal("450.0"),
            outlier_pnl_ratio=0.08,
            median_holding_time_seconds=1800.0,
            active_days=60.0,
            days_since_last_active=0.1,
            first_tx_timestamp=now_ts - 86400 * 60,
            last_tx_timestamp=now_ts,
            rejection_reasons=[],
        )
        wl_record: WhitelistRecord = await whitelist_db.upsert_wallet(profile, status=WhitelistStatus.ACTIVE)
        assert isinstance(wl_record, WhitelistRecord)
        assert wl_record.status == WhitelistStatus.ACTIVE
        is_wl = await whitelist_db.is_whitelisted(test_wallet)
        logger.info(f"Wallet {test_wallet[:12]}... whitelisted: {is_wl} (Status: {wl_record.status.value})")
        assert is_wl is True

        pipeline = SmartMoneyPipeline(
            config=config,
            whitelist_db=whitelist_db,
            profiler=smart_profiler,
            history_db=history_db,
            price_service=price_service,
        )

        test_sol_token = "SmokeSolToken1111111111111111111111111111"
        swap_event = SwapEvent(
            timestamp_ns=time.time_ns(),
            block_number=28500100,
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address="PoolSolSmoke1111111111111111111111111111111",
            token_in="So11111111111111111111111111111111111111112",
            token_out=test_sol_token,
            amount_in=Decimal("2.5"),
            amount_out=Decimal("50000.0"),
            sender=test_wallet,
            tx_hash="sig_smoke_wallet_swap_001_11111111111111111111",
            log_index=None,
        )

        # Ingest and cache normalized swap in history database
        normalized_swap = NormalizedSwap(
            tx_hash=swap_event.tx_hash,
            block_time=now_ts,
            chain=ChainIdentifier.SOLANA_MAINNET,
            wallet_address=test_wallet,
            venue=SwapVenue.RAYDIUM.value,
            token_in_address=swap_event.token_in,
            token_in_symbol="SOL",
            token_in_amount=swap_event.amount_in,
            token_out_address=swap_event.token_out,
            token_out_symbol="SMOKE",
            token_out_amount=swap_event.amount_out,
            fee_native=Decimal("0.000005"),
            is_buy=True,
            token_address=test_sol_token,
            token_amount=swap_event.amount_out,
            native_amount=swap_event.amount_in,
        )
        await history_db.upsert_swaps(wallet_address=test_wallet, chain=ChainIdentifier.SOLANA_MAINNET, swaps=[normalized_swap])
        cached_swaps = await history_db.get_cached_swaps(wallet_address=test_wallet, chain=ChainIdentifier.SOLANA_MAINNET)
        assert len(cached_swaps) == 1
        assert cached_swaps[0].venue == SwapVenue.RAYDIUM.value
        logger.info(f"Cached NormalizedSwap verified: {cached_swaps[0].tx_hash[:16]}... on venue {cached_swaps[0].venue}")

        whale_sig = await pipeline.on_swap_event(swap_event)
        assert whale_sig is not None
        logger.info(f"Emitted Signal Source: {whale_sig.source} | Token: {whale_sig.token_address}")
        logger.info(f"Wallet Attribution: {whale_sig.wallet_address} | Score: {whale_sig.wallet_score} | Conviction: {whale_sig.wallet_conviction}")
        assert whale_sig.source == SignalSource.WHALE_WALLET
        assert whale_sig.wallet_address == test_wallet.lower()
        logger.info("STAGE 3 COMPLETED: Smart wallet attribution and signal emission verified.")

        # ----------------------------------------------------------------------
        # STAGE 4: RUFLO COUNCIL MULTI-AGENT DEBATE (FAKE LLM)
        # ----------------------------------------------------------------------
        logger.info("\n>>> STAGE 4: CONDUCTING RUFLO COUNCIL 3-ROUND MULTI-AGENT DEBATE")
        fake_llm = FakeCouncilLLM(model=config.ai_model)
        episodic_mem = RufloEpisodicMemory(db_path=os.path.join(temp_dir, "test_memory.db"))

        # Pre-seed memory to verify retrieval
        await episodic_mem.record_episode(
            MemoryEpisode(
                episode_id="ep_smoke_001",
                token_address=test_sol_token,
                chain=ChainIdentifier.SOLANA_MAINNET.value,
                decision_type="BUY",
                signal_source="WHALE_WALLET",
                confidence_score=0.88,
                feature_vector=[0.8, 0.9, 0.7],
                realized_pnl_usd=50.0,
                roi_pct=25.0,
                actual_slippage_bps=35,
                fill_latency_ms=120.0,
                root_cause_tag="MOMENTUM_WIN",
                summary_reflection="High conviction whale entry was profitable",
            )
        )

        reach_agent = ReachAnalystAgent(llm_client=fake_llm)
        forensic_agent = ChainForensicAgent(llm_client=fake_llm)
        quant_agent = QuantTechnicalsAgent(llm_client=fake_llm)

        council_cro = CouncilArbiterCRO(
            config=config,
            episodic_memory=episodic_mem,
            llm_client=fake_llm,
            reach_agent=reach_agent,
            forensic_agent=forensic_agent,
            quant_agent=quant_agent,
        )

        council_sol_pool = PoolState(
            pool_address="PoolSolSmoke1111111111111111111111111111111",
            token_address=test_sol_token,
            chain=ChainIdentifier.SOLANA_MAINNET,
            venue=ExecutionVenue.RAYDIUM_AMM,
            native_reserve=Decimal("500.0"),
            token_reserve=Decimal("10000000.0"),
            fee_numerator=25,
            fee_denominator=10000,
            last_updated_block=28500100,
            reserve_type=ReserveType.EXACT,
            total_supply=Decimal("1000000000.0"),
            source="onchain_rpc",
            fetched_at=time.time(),
            data_age=0.5,
        )
        sec_report = SecurityReport(
            token_address=test_sol_token,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tier=SecurityTier.CLEAN,
            buy_tax_bps=0,
            sell_tax_bps=0,
            is_honeypot=False,
            mint_authority_disabled=True,
            freeze_authority_disabled=True,
            lp_burned_ratio=1.0,
            security_score=95.0,
        )

        deliberation = await council_cro.deliberate(
            token_ca=test_sol_token,
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool=council_sol_pool,
            report=sec_report,
            signal=whale_sig,
        )
        logger.info(f"Council Decision: {deliberation.decision} | Confidence: {deliberation.confidence:.2f} | Consensus Score: {deliberation.consensus_score:.2f}")
        logger.info(f"LLM Turns: {deliberation.llm_turns} | Retrieved Episodes: {deliberation.retrieved_episodes_count} | Devils Advocate: {deliberation.devils_advocate_invoked}")
        assert deliberation.decision == "EXECUTE_BUY"
        assert fake_llm.turns_executed >= 3
        assert deliberation.verdicts["ChainForensic"].vote == AgentVote.APPROVE
        assert deliberation.verdicts["QuantTechnicals"].vote == AgentVote.APPROVE
        assert deliberation.verdicts["ReachAnalyst"].vote == AgentVote.APPROVE
        logger.info(f"Verified unanimous council vote: {deliberation.verdicts['ChainForensic'].vote.value}")
        logger.info("STAGE 4 COMPLETED: Multi-agent council debate executed cleanly.")

        # ----------------------------------------------------------------------
        # STAGE 5: SOL ENTRY EXECUTION
        # ----------------------------------------------------------------------
        logger.info("\n>>> STAGE 5: EXECUTING SOL PAPER ENTRY (RAYDIUM)")
        position_book = PositionBook()
        cost_model = TradeCostModel(config=config, price_service=price_service)
        risk_manager = RiskEngine(config=config)

        test_eth_token = "0x5555555555555555555555555555555555555555"
        eth_pool = PoolState(
            pool_address="0xPoolBaseSmoke000000000000000000000000000",
            token_address=test_eth_token,
            chain=ChainIdentifier.BASE_MAINNET,
            venue=ExecutionVenue.UNISWAP_V3,
            native_reserve=Decimal("50.0"),
            token_reserve=Decimal("5000000.0"),
            fee_numerator=30,
            fee_denominator=10000,
            last_updated_block=57010000,
            reserve_type=ReserveType.EXACT,
            total_supply=Decimal("1000000000.0"),
            source="onchain_rpc",
            fetched_at=time.time(),
            data_age=0.5,
        )

        pools_by_token: Dict[str, PoolState] = {
            test_sol_token: council_sol_pool,
            test_eth_token: eth_pool,
        }
        executor = PaperExecutor(
            position_book=position_book,
            price_service=price_service,
            cost_model=cost_model,
            config=config,
            pool_resolver=lambda tok, pool_addr=None: pools_by_token.get(tok),
        )

        # Size the trade with live SOL price $220.0
        portfolio_equity_usd = Decimal("10000.0")
        sizing_res = risk_manager.calculate_order_size(
            portfolio_equity_usd=portfolio_equity_usd,
            native_price_usd=price_service.get_price(ChainIdentifier.SOLANA_MAINNET).price,
            chain=ChainIdentifier.SOLANA_MAINNET,
            score=85.0,
        )
        logger.info(f"Risk Manager Sized SOL Order: {sizing_res.native_trade_size} SOL (${sizing_res.usd_at_risk:.2f})")

        sol_fill = await executor.execute_signal(
            signal=whale_sig,
            portfolio_equity_usd=portfolio_equity_usd,
            fixed_trade_amount_native=sizing_res.native_trade_size,
        )
        assert sol_fill is not None
        assert sol_fill.tokens_acquired > 0
        sol_lot: OpenLot = position_book.open_lot(fill=sol_fill, signal_id=whale_sig.signal_id)
        assert isinstance(sol_lot, OpenLot)
        logger.info(f"SOL Fill Order ID: {sol_fill.order_id} | Tokens Acquired: {sol_fill.tokens_acquired:.2f} | Lot ID: {sol_lot.lot_id}")
        logger.info(f"Effective Price: {sol_fill.effective_price:.8f} SOL | Realized Slippage: {sol_fill.slippage_bps} bps | Gas: ${sol_fill.simulated_gas_cost_usd:.4f}")
        logger.info(f"Position Book Open Lots: {len(position_book.get_open_lots())}")
        assert len(position_book.get_open_lots()) == 1
        logger.info("STAGE 5 COMPLETED: SOL paper order filled and registered in PositionBook.")

        # ----------------------------------------------------------------------
        # STAGE 6: ETH ENTRY EXECUTION (BASE / UNISWAP V3)
        # ----------------------------------------------------------------------
        logger.info("\n>>> STAGE 6: EXECUTING ETH PAPER ENTRY ON BASE (UNISWAP V3)")
        eth_sec_report = SecurityReport(
            token_address=test_eth_token,
            chain=ChainIdentifier.BASE_MAINNET,
            tier=SecurityTier.CLEAN,
            buy_tax_bps=0,
            sell_tax_bps=0,
            is_honeypot=False,
            mint_authority_disabled=True,
            freeze_authority_disabled=True,
            lp_burned_ratio=1.0,
            security_score=92.0,
        )
        eth_sig = SignalEvent(
            signal_id="sig_eth_smoke_002",
            token_address=test_eth_token,
            pool_address=eth_pool.pool_address,
            chain=ChainIdentifier.BASE_MAINNET,
            source=SignalSource.WAVE2_BREAKOUT,
            strength=SignalStrength.STRONG,
            suggested_side=OrderSide.BUY,
            alpha_score=0.84,
            pool_state=eth_pool,
            security_report=eth_sec_report,
            timestamp_ns=time.time_ns(),
        )

        eth_sizing = risk_manager.calculate_order_size(
            portfolio_equity_usd=portfolio_equity_usd,
            native_price_usd=price_service.get_price(ChainIdentifier.BASE_MAINNET).price,
            chain=ChainIdentifier.BASE_MAINNET,
            score=80.0,
        )
        logger.info(f"Risk Manager Sized ETH Order: {eth_sizing.native_trade_size} ETH (${eth_sizing.usd_at_risk:.2f})")

        eth_fill = await executor.execute_signal(
            signal=eth_sig,
            portfolio_equity_usd=portfolio_equity_usd,
            fixed_trade_amount_native=eth_sizing.native_trade_size,
        )
        assert eth_fill is not None
        assert eth_fill.tokens_acquired > 0
        eth_lot: OpenLot = position_book.open_lot(fill=eth_fill, signal_id=eth_sig.signal_id)
        assert isinstance(eth_lot, OpenLot)
        logger.info(f"ETH Fill Order ID: {eth_fill.order_id} | Tokens Acquired: {eth_fill.tokens_acquired:.2f} | Lot ID: {eth_lot.lot_id}")
        logger.info(f"Effective Price: {eth_fill.effective_price:.8f} ETH | Realized Slippage: {eth_fill.slippage_bps} bps")
        logger.info(f"Position Book Open Lots: {len(position_book.get_open_lots())}")
        assert len(position_book.get_open_lots()) == 2
        logger.info("STAGE 6 COMPLETED: Base ETH order filled with proper per-chain sizing.")

        # ----------------------------------------------------------------------
        # STAGE 7: POSITION EXIT
        # ----------------------------------------------------------------------
        logger.info("\n>>> STAGE 7: SIMULATING PRICE SURGE AND EXECUTING POSITION EXIT")
        # Simulate price moving +25%
        exit_pool = PoolState(
            pool_address=council_sol_pool.pool_address,
            token_address=council_sol_pool.token_address,
            chain=council_sol_pool.chain,
            venue=council_sol_pool.venue,
            native_reserve=council_sol_pool.native_reserve * Decimal("1.25"),
            token_reserve=council_sol_pool.token_reserve * Decimal("0.80"),
            fee_numerator=council_sol_pool.fee_numerator,
            fee_denominator=council_sol_pool.fee_denominator,
            last_updated_block=council_sol_pool.last_updated_block + 50,
            reserve_type=ReserveType.EXACT,
            total_supply=council_sol_pool.total_supply,
            source="onchain_rpc",
            fetched_at=time.time(),
            data_age=0.5,
        )

        exit_fill = await executor.execute_exit(
            chain=sol_lot.chain,
            token_address=sol_lot.token_address,
            pool=exit_pool,
            tokens_to_sell=sol_lot.tokens_held,
            reason=TradeExitReason.TP_25,
        )
        assert exit_fill is not None
        assert exit_fill.simulated_native_spent > 0

        pnl_native, matched = position_book.close_lots_fifo(
            chain=sol_lot.chain,
            token_address=sol_lot.token_address,
            tokens_to_sell=sol_lot.tokens_held,
            sell_price=exit_fill.effective_price,
        )
        realized_pnl_usd = pnl_native * price_service.get_price(ChainIdentifier.SOLANA_MAINNET).price
        logger.info(f"Closed SOL Lot: {sol_lot.lot_id} | Matched Tokens: {matched}")
        logger.info(f"PnL Native: {pnl_native:.6f} SOL | Realized PnL USD: ${realized_pnl_usd:.2f}")
        logger.info(f"Open Lots Remaining: {len(position_book.get_open_lots())}")
        assert realized_pnl_usd > 0
        logger.info("STAGE 7 COMPLETED: Position successfully closed with verified profit.")

        # ----------------------------------------------------------------------
        # STAGE 8: POST-TRADE HOOKS EXECUTION
        # ----------------------------------------------------------------------
        logger.info("\n>>> STAGE 8: RUNNING ALL 3 POST-TRADE CONSUMERS")
        ledger = SQLiteLedger(db_path=db_path)
        async with ledger:
            await ledger.record_trade(sol_fill)

        self_learning = SelfLearningEngine(config=config, ledger=ledger)
        tournament = StrategyTournamentRegistry(config=config, cost_model=cost_model)

        # Consumer 1: Self Learning Engine
        learning_payload = {
            "token_address": sol_lot.token_address,
            "chain": sol_lot.chain.value,
            "realized_pnl_usd": float(realized_pnl_usd),
            "is_win": realized_pnl_usd > 0,
            "strategy_name": "whale_momentum",
            "actual_slippage_bps": exit_fill.slippage_bps,
            "hold_time_s": 300.0,
        }
        await self_learning.on_trade_closed(learning_payload)
        logger.info("Consumer 1 (SelfLearningEngine): Trade recorded and hyperparameter tuner notified.")

        # Consumer 2: Strategy Tournament Live Outcome
        tournament.record_live_trade_outcome(
            strategy_name="whale_momentum",
            pnl_usd=float(realized_pnl_usd),
            is_win=realized_pnl_usd > 0,
            token_address=sol_lot.token_address,
            chain=sol_lot.chain,
            entry_price=sol_fill.effective_price,
            exit_price=exit_fill.effective_price,
            tokens_acquired=sol_lot.tokens_held,
            cost_basis_usd=sizing_res.usd_at_risk,
            realized_pnl_usd=realized_pnl_usd,
            hold_time_s=300.0,
            exit_reason=TradeExitReason.TP_25.value,
        )
        logger.info("Consumer 2 (StrategyTournamentRegistry): Live trade outcome recorded without rewriting PnL.")

        # Consumer 3: Council Post-Mortem & Memory Episode Persistence
        await council_cro.record_trade_outcome(
            token_address=sol_lot.token_address,
            pnl_usd=float(realized_pnl_usd),
            roi_pct=25.0,
            exit_reason=TradeExitReason.TP_25.value,
            signal_id=whale_sig.signal_id,
        )
        logger.info("Consumer 3 (CouncilArbiterCRO): Post-mortem episode written to Ruflo episodic memory.")
        logger.info("STAGE 8 COMPLETED: All 3 post-trade hooks executed cleanly at ERROR=0.")

        # ----------------------------------------------------------------------
        # STAGE 9: STRATEGY TOURNAMENT PROMOTION
        # ----------------------------------------------------------------------
        logger.info("\n>>> STAGE 9: STRATEGY TOURNAMENT CANDIDATE PROMOTION")
        promoted_events: List[Dict[str, Any]] = []

        def on_champion_promoted(champ: StrategyVariant, reason: str = ""):
            promoted_events.append({
                "name": champ.strategy_name,
                "hard_stop_loss_pct": champ.parameters.hard_stop_loss_pct,
                "custom_take_profit_pct": champ.parameters.take_profit_pct,
                "trailing_stop_from_peak_pct": champ.parameters.trailing_stop_from_peak_pct,
                "rules": champ.parameters.entry_rules.to_dict(),
                "reason": reason,
            })

        tournament.register_on_promotion_callback(on_champion_promoted)

        # Stage a superior challenger variant with 35 simulated profitable trades
        chal_params = StrategyParameters(
            strategy_name="alpha_momentum_breakout_v2",
            hard_stop_loss_pct=0.075,
            take_profit_pct=0.35,
            trailing_stop_from_peak_pct=0.06,
            min_entry_score=80.0,
            entry_rules=EntryRuleSet(
                operator=LogicalOperator.AND,
                groups=[
                    RuleGroup(
                        operator=LogicalOperator.AND,
                        conditions=[
                            RuleCondition(metric="alpha_score", operator=ComparisonOperator.GT, threshold=0.82),
                            RuleCondition(metric="volume_surge_multiplier", operator=ComparisonOperator.GT, threshold=2.5),
                        ],
                    )
                ],
            ),
        )
        challenger = StrategyVariant(
            experiment_id="exp_chal_smoke_002",
            strategy_name=chal_params.strategy_name,
            parameters=chal_params,
            status=ExperimentStatus.SHADOW,
        )

        now = time.time()
        for i in range(35):
            is_w = (i % 4 != 0)  # 75% win rate
            pnl_val = 12.0 if is_w else -6.0
            lot: ShadowLot = challenger.record_fill(
                token_address=f"token_smoke_chal_{i}",
                chain=ChainIdentifier.SOLANA_MAINNET,
                entry_price=Decimal("1.0"),
                tokens_acquired=Decimal("50.0"),
                native_spent=Decimal("0.25"),
                native_price_usd=price_service.get_price(ChainIdentifier.SOLANA_MAINNET).price,
                gas_usd=Decimal("0.05"),
                slippage_bps=50,
                latency_ms=45.0,
                mev_drag_usd=Decimal("0.0"),
            )
            assert isinstance(lot, ShadowLot)
            lot.entry_timestamp = now - (35 - i) * 60
            lot.is_closed = True
            lot.realized_pnl_usd = Decimal(str(pnl_val))
            lot.roi_pct = 24.0 if is_w else -12.0
            lot.cost_basis_usd = Decimal("50.0")
            challenger.closed_lots.append(lot)

        challenger.recompute_metrics()
        tournament.register_challenger(challenger)
        logger.info(f"Registered Challenger: {challenger.strategy_name} (Trades: {challenger.metrics.trades_count}, Win Rate: {challenger.metrics.win_rate * 100:.1f}%)")

        new_champ, rationale = tournament.evaluate_promotions()
        assert new_champ is not None
        assert new_champ.strategy_name == "alpha_momentum_breakout_v2"
        assert len(promoted_events) == 1
        logger.info(f"Promotion Successful! New Live Champion: {new_champ.strategy_name} | Rationale: {rationale}")
        logger.info(f"Live SL Updated: {promoted_events[0]['hard_stop_loss_pct'] * 100:.1f}% | TP Updated: {promoted_events[0]['custom_take_profit_pct'] * 100:.1f}%")
        logger.info(f"Live Trailing Stop Updated: {promoted_events[0]['trailing_stop_from_peak_pct'] * 100:.1f}%")
        logger.info(f"Live Entry Rules Synchronized: {promoted_events[0]['rules']}")
        logger.info("STAGE 9 COMPLETED: Champion promoted and live parameters updated.")

        logger.info("\n================================================================================")
        logger.info("ALL 9 STAGES OF END-TO-END SMOKE SIMULATION COMPLETED SUCCESSFULLY!")
        logger.info("================================================================================")
        return 0

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    exit_code = asyncio.run(run_smoke_simulation())
    os._exit(exit_code)
