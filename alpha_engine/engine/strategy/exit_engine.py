"""
alpha_engine.engine.strategy.exit_engine
========================================
Dynamic Exit & Emergency Liquidity Exit Engine.
"""

from decimal import Decimal
from alpha_engine.config import EngineConfig
from alpha_engine.models.enums import TradeExitReason

class ExitEngine:
    """
    Evaluates open positions against TP/SL and emergency criteria.
    """

    def __init__(self, config: EngineConfig) -> None:
        self.config = config

    def check_exit_conditions(
        self, 
        current_price: Decimal, 
        entry_price: Decimal, 
        liquidity_drop_pct: float = 0.0,
        age_seconds: float = 0.0
    ) -> tuple[bool, TradeExitReason]:
        """
        Returns (should_exit, exit_reason).
        """
        # 1. Emergency Exit (Liquidity Rug)
        if liquidity_drop_pct >= self.config.emergency_exit_liquidity_drop_pct:
            return True, TradeExitReason.RUG_LIQUIDITY_DRAIN

        if entry_price == Decimal(0):
            return False, TradeExitReason.MANUAL_CLOSE
            
        pnl_pct = float((current_price - entry_price) / entry_price)

        # 2. Hard Stop Loss
        # Skip if within grace period to avoid micro-tick noise
        if age_seconds > self.config.exit_grace_period_sec:
            if pnl_pct <= self.config.stop_loss_pct:
                return True, TradeExitReason.SL_HARD
            if pnl_pct <= self.config.emergency_stop_pct:
                return True, TradeExitReason.SL_HARD

        # 3. Take Profit
        if pnl_pct >= self.config.tp3_pct:
            return True, TradeExitReason.TP_2X
        elif pnl_pct >= self.config.tp2_pct:
            # Here we might want to scale out, but for now we just exit or hold
            pass 
        elif pnl_pct >= self.config.tp1_pct:
            pass

        return False, TradeExitReason.MANUAL_CLOSE
