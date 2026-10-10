import asyncio
import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.signals import SignalGenerator
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.models.enums import ChainIdentifier, SignalSource
from alpha_engine.models.state import PoolState
from alpha_engine.engine.strategy.scoring import ScoringEngine



@pytest.mark.anyio
async def test_v2_strategy_enabled():
    config = EngineConfig(strategy_v2_enabled=True)
    
    # 1. Test Signal Generator computes V2 Score
    sig_gen = SignalGenerator(config=config)
    assert sig_gen.config.strategy_v2_enabled is True
    assert isinstance(sig_gen.scoring_engine, ScoringEngine)
    
    from decimal import Decimal
    state = PoolState(
        pool_address="FakeTokenAddressFakeTokenAddress123",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("100.0"),
        fee_numerator=3000,
        fee_denominator=1000000,
        last_updated_block=123456,
        token_decimals=6,
        native_decimals=9,
    )
    
    # Mock StagedLaunch
    from alpha_engine.engine.signals import StagedLaunch
    from alpha_engine.models.state import SecurityReport, SecurityTier
    from alpha_engine.models.events import RawSignalEvent
    import time

    mock_report = SecurityReport(
        token_address="FakeTokenAddressFakeTokenAddress123",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        is_rug=False,
        score=100.0,
        flags=[],
        buy_tax_bps=0,
        sell_tax_bps=0,
        checked_at_ts=int(time.time()),
    )
    mock_raw_signal = RawSignalEvent(
        source=SignalSource.PUMP_FUN_MINT,
        raw_data={},
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="FakePoolAddressFakePoolAddress1234",
        token_address="FakeTokenAddressFakeTokenAddress123",
    )
    staged = StagedLaunch(
        token_address="FakeTokenAddressFakeTokenAddress123",
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="FakePoolAddressFakePoolAddress1234",
        t_0=time.time(),
        initial_price=Decimal("0.0001"),
        latest_price=Decimal("0.0002"),
        min_price=Decimal("0.0001"),
        report=mock_report,
        raw_signal=mock_raw_signal,
    )

    signal = sig_gen.generate_launch_signal(staged, state)
    
    # Verify score is injected
    assert signal.alpha_score >= 0.0
    assert signal.alpha_score <= 1.0
    
    print(f"Generated V2 Score: {signal.alpha_score * 100}")
    
    # 2. Test Runner integrates RiskEngine & ExitEngine
    runner = PaperTradingEngine(config)
    
    assert runner.risk_engine is not None
    assert runner.exit_engine is not None
    
    # Test check_daily_limits
    can_open = runner.risk_engine.check_daily_limits({"daily_drawdown_pct": 0.0, "consecutive_losses": 0})
    assert can_open is True
    
    # Test calculate_position_size
    from decimal import Decimal
    dynamic_size = runner.risk_engine.calculate_position_size(Decimal("10000.0"), signal.alpha_score * 100.0)
    print(f"RiskEngine size: {dynamic_size}")
    
    # 3. Test ExitEngine
    should_exit, reason = runner.exit_engine.check_exit_conditions(
        current_price=Decimal("0.5"), 
        entry_price=Decimal("1.0"), 
        liquidity_drop_pct=0.9, # 90% rug
        age_seconds=120.0
    )
    print(f"ExitEngine decision: {should_exit}, {reason}")
    assert should_exit is True

if __name__ == '__main__':
    asyncio.run(test_v2_strategy_enabled())
