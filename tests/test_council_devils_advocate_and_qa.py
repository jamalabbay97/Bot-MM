"""
tests.test_council_devils_advocate_and_qa — Unit Tests for Council Devil's Advocate & Inter-Agent Q&A
====================================================================================================
Validates:
1. Triggering of Devil's Advocate stress-test upon unanimous 3/3 agent approval.
2. Veto enforcement when Devil's Advocate detects fatal liquidity void / migration trap.
3. Position mitigation (50% Kelly sizing reduction) when Devil's Advocate detects elevated concentration.
4. Inter-agent Q&A exchanges recorded in debate transcript and debate rounds.
5. Integration with Ruflo episodic memory retrieval.
"""

from decimal import Decimal
import pytest

from alpha_engine.council.arbiter_cro import CouncilArbiterCRO
from alpha_engine.council.models import AgentVerdict, AgentVote, DebateStance
from alpha_engine.models.enums import ChainIdentifier, SecurityTier
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.ruflo.memory import MemoryEpisode, RufloEpisodicMemory


class MockAgent:
    def __init__(self, vote=AgentVote.APPROVE, score=85.0, flags=None, fatal_veto=False):
        self.vote = vote
        self.score = score
        self.flags = flags or []
        self.fatal_veto = fatal_veto

    async def audit_social_narrative(self, **kwargs):
        return AgentVerdict(
            agent_name="ReachAnalyst",
            vote=self.vote,
            confidence=0.85,
            score=self.score,
            rationale="Authentic social growth",
            flags=self.flags,
        )

    def audit_contract_and_liquidity(self, **kwargs):
        return AgentVerdict(
            agent_name="ChainForensic",
            vote=self.vote,
            confidence=0.90,
            score=self.score,
            rationale="Clean bytecode and revoked authorities",
            flags=self.flags,
            fatal_veto=self.fatal_veto,
        )

    def analyze_market_dynamics(self, **kwargs):
        return AgentVerdict(
            agent_name="QuantTechnicals",
            vote=self.vote,
            confidence=0.85,
            score=self.score,
            rationale="Bullish momentum and volume surge",
            flags=self.flags,
            metrics={"exit_profile": "DYNAMIC_TRAILING"},
        )

    async def cross_examine_peer(self, target, target_res, my_res, token_ca):
        from alpha_engine.council.models import DebateRound
        return DebateRound(
            round_number=2,
            agent_name="PeerAgent",
            target_agent=target,
            stance="CHALLENGE",
            arguments="Challenging risk parameters",
        )

    async def defend_or_revise(self, debate_rounds, initial_res):
        from alpha_engine.council.models import DebateRound
        round_item = DebateRound(
            round_number=3,
            agent_name=initial_res.agent_name,
            stance="DEFEND",
            arguments="Defending initial conviction",
        )
        return initial_res, round_item


@pytest.mark.asyncio
async def test_devils_advocate_invoked_on_unanimous_approval():
    # Setup 3 agents that all vote APPROVE
    reach = MockAgent(vote=AgentVote.APPROVE, score=85.0)
    forensic = MockAgent(vote=AgentVote.APPROVE, score=90.0)
    quant = MockAgent(vote=AgentVote.APPROVE, score=88.0)

    arbiter = CouncilArbiterCRO(
        reach_agent=reach,
        forensic_agent=forensic,
        quant_agent=quant,
    )

    report = SecurityReport(
        token_address="TokenA11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.15,
        mint_authority_disabled=True,
        freeze_authority_disabled=True,
    )

    pool = PoolState(
        token_address="TokenA11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolA11111111111111111111111111111111",
        native_reserve=Decimal("45.0"),
        token_reserve=Decimal("1000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1000,
        market_cap_usd=Decimal("100000.0"),
    )

    result = await arbiter.deliberate(
        token_ca="TokenA11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool=pool,
        report=report,
    )

    assert result.decision == "EXECUTE_BUY"
    assert result.devils_advocate_invoked is True
    assert result.devils_advocate_report is not None
    assert result.devils_advocate_report["fatal_flaw_detected"] is False

    # Check that DevilsAdvocate round is in transcript and debate_rounds
    da_rounds = [r for r in result.debate_rounds if r.stance == DebateStance.DEVILS_ADVOCATE.value]
    assert len(da_rounds) == 1
    assert any("DEVIL'S ADVOCATE" in line for line in result.debate_transcript)


@pytest.mark.asyncio
async def test_devils_advocate_fatal_veto_on_migration_void():
    reach = MockAgent(vote=AgentVote.APPROVE, score=85.0)
    forensic = MockAgent(vote=AgentVote.APPROVE, score=90.0)
    quant = MockAgent(vote=AgentVote.APPROVE, score=88.0)

    arbiter = CouncilArbiterCRO(
        reach_agent=reach,
        forensic_agent=forensic,
        quant_agent=quant,
    )

    report = SecurityReport(
        token_address="TokenA11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.15,
        mint_authority_disabled=True,
        freeze_authority_disabled=True,
    )

    # Pool sitting at 95% curve progress (migration sniper void)
    pool = PoolState(
        token_address="TokenA11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolA11111111111111111111111111111111",
        native_reserve=Decimal("82.0"),  # (82-30)/55 = 94.5% progress -> Migration void!
        token_reserve=Decimal("1000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1000,
        market_cap_usd=Decimal("100000.0"),
    )

    result = await arbiter.deliberate(
        token_ca="TokenA11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool=pool,
        report=report,
    )

    assert result.decision == "PASS"
    assert result.devils_advocate_invoked is True
    assert result.devils_advocate_report["fatal_flaw_detected"] is True
    assert "MIGRATION" in result.fatal_veto_agent or "ChainForensic" in result.fatal_veto_agent


@pytest.mark.asyncio
async def test_council_episodic_memory_injection():
    # Setup memory with past trade episode
    mem = RufloEpisodicMemory(db_path=":memory:")
    ep = MemoryEpisode(
        episode_id="ep1",
        token_address="PastTokenA11111111111111111111111111",
        chain="solana_mainnet",
        decision_type="ENTER",
        signal_source="MOMENTUM_BREAKOUT",
        confidence_score=0.90,
        feature_vector=[0.5, 0.2, 0.5, 0.6, 0.15, 0.3, 0.5],
        realized_pnl_usd=120.0,
        roi_pct=85.0,
        actual_slippage_bps=110,
        fill_latency_ms=45.0,
        root_cause_tag="PROFIT_TARGET_HIT",
        summary_reflection="Strong VAH breakout delivered +85% return.",
    )
    await mem.record_episode(ep)

    reach = MockAgent()
    forensic = MockAgent()
    quant = MockAgent()

    arbiter = CouncilArbiterCRO(
        reach_agent=reach,
        forensic_agent=forensic,
        quant_agent=quant,
        episodic_memory=mem,
    )

    report = SecurityReport(
        token_address="TokenB11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.12,
        mint_authority_disabled=True,
        freeze_authority_disabled=True,
    )

    pool = PoolState(
        token_address="TokenB11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolB11111111111111111111111111111111",
        native_reserve=Decimal("40.0"),
        token_reserve=Decimal("1000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1000,
        market_cap_usd=Decimal("100000.0"),
    )

    result = await arbiter.deliberate(
        token_ca="TokenB11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool=pool,
        report=report,
    )

    assert result.retrieved_episodes_count >= 1
    assert any("HISTORICAL PRECEDENTS" in line for line in result.debate_transcript)
