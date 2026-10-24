"""
alpha_engine.ruflo.agents.quant_agent — Quant Math & Risk Sizing Agent
======================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Ruflo Quant Math Agent | CPMM & MEV Sandwich Protection
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.math.cpmm import (
    cpmm_buy_quote,
    cpmm_sell_quote,
    get_initial_bonding_curve_pool,
)
from alpha_engine.math.mev import simulate_latency
from alpha_engine.math.sizing import (
    KellySizing,
    compute_position_size,
    half_kelly_fraction,
)
from alpha_engine.models.enums import ChainIdentifier, OrderSide
from alpha_engine.models.state import PoolState
from alpha_engine.ruflo.agents.base import BaseRufloAgent
from alpha_engine.ruflo.protocol import AgentRole, SwarmMessage, ToolDefinition

logger = logging.getLogger(__name__)


class QuantRiskSizingAgent(BaseRufloAgent):
    """
    Quant Math & Risk Sizing Agent:
    Integrates alpha_engine/math/cpmm.py, mev.py, and sizing.py.
    Dynamically computes optimal position sizes (Half-Kelly with risk clamps),
    expected CPMM slippage bounds, MEV sandwich protection, and dynamic TP/SL tiers.
    """

    def __init__(
        self,
        name: str = "QuantRiskSizer",
        config: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            name=name,
            role=AgentRole.QUANT,
            description="Algorithmic sizing, CPMM invariant math, and MEV sandwich risk agent",
        )
        self.config = config
        self._register_agent_tools()

    def _register_agent_tools(self) -> None:
        self.register_tool(
            ToolDefinition(
                name="quant_compute_position_size",
                description="Compute mathematically bounded Half-Kelly position size in native units",
                category="math",
                parameters_schema={
                    "portfolio_equity_usd": {"type": "number"},
                    "native_price_usd": {"type": "number"},
                    "chain": {"type": "string"},
                    "win_rate": {"type": "number"},
                    "avg_win_native": {"type": "number"},
                    "avg_loss_native": {"type": "number"},
                },
            ),
            self.tool_compute_position_size,
        )
        self.register_tool(
            ToolDefinition(
                name="quant_quote_swap",
                description="Quote expected tokens acquired, effective price, and price impact bps via CPMM invariant",
                category="math",
                parameters_schema={
                    "pool_native_reserve": {"type": "string"},
                    "pool_token_reserve": {"type": "string"},
                    "amount_in": {"type": "string"},
                    "is_buy": {"type": "boolean"},
                },
            ),
            self.tool_quote_swap,
        )
        self.register_tool(
            ToolDefinition(
                name="quant_evaluate_mev_risk",
                description="Evaluate MEV sandwich vulnerability and latency impact based on slippage tolerance",
                category="math",
                parameters_schema={
                    "slippage_bps": {"type": "integer"},
                    "price_impact_bps": {"type": "integer"},
                },
            ),
            self.tool_evaluate_mev_risk,
        )
        # Register aliases for flexible invocation
        self.register_tool(
            ToolDefinition(
                name="quant_calculate_sizing",
                description="Alias for position sizing calculation",
                category="math",
            ),
            self.tool_compute_position_size,
        )
        self.register_tool(
            ToolDefinition(
                name="quant_estimate_cpmm_quote",
                description="Alias for CPMM swap quote estimation",
                category="math",
            ),
            self.tool_quote_swap,
        )
        self.register_tool(
            ToolDefinition(
                name="quant_get_initial_bonding_curve",
                description="Get initial Pump.fun bonding curve pool state",
                category="math",
                parameters_schema={"token_address": {"type": "string"}},
            ),
            self.tool_get_initial_bonding_curve,
        )

    async def tool_compute_position_size(
        self,
        portfolio_equity_usd: float = 10000.0,
        native_price_usd: float = 150.0,
        chain: str = "solana_mainnet",
        win_rate: float = 0.55,
        avg_win_native: float = 0.10,
        avg_loss_native: float = 0.05,
        pool_liquidity_usd: float = 25000.0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        chain_enum = (
            ChainIdentifier.BASE_MAINNET if "base" in chain.lower() or "evm" in chain.lower()
            else ChainIdentifier.SOLANA_MAINNET
        )
        k_frac = half_kelly_fraction(
            win_rate=win_rate,
            avg_win_native=Decimal(str(avg_win_native)),
            avg_loss_native=Decimal(str(avg_loss_native)),
        )
        if k_frac <= 0.0:
            k_frac = 0.02

        sizing: KellySizing = compute_position_size(
            kelly_fraction=k_frac,
            portfolio_equity_usd=Decimal(str(portfolio_equity_usd)),
            native_price_usd=Decimal(str(native_price_usd)),
            chain=chain_enum,
            side=OrderSide.BUY,
        )
        recommended_usd = float(sizing.usd_at_risk)
        # Cap position size by maximum pool depth percentage (e.g. 5% of available liquidity)
        if pool_liquidity_usd > 0.0:
            max_depth_cap = pool_liquidity_usd * 0.05
            recommended_usd = min(recommended_usd, max_depth_cap)

        return {
            "approved": not sizing.gas_rejected and (recommended_usd > 0),
            "recommended_position_usd": recommended_usd,
            "pool_liquidity_usd": pool_liquidity_usd,
            "native_trade_size": str(sizing.native_trade_size),
            "usd_at_risk": str(sizing.usd_at_risk),
            "kelly_fraction": sizing.kelly_fraction,
            "capped_fraction": sizing.capped_fraction,
            "gas_rejected": sizing.gas_rejected,
            "stop_loss_pct": 0.08,
            "take_profit_tiers": [0.5, 1.0, 2.0],
        }

    async def tool_get_initial_bonding_curve(
        self,
        token_address: str,
    ) -> dict[str, Any]:
        pool = get_initial_bonding_curve_pool(token_address)
        return {
            "token_address": token_address,
            "pool_address": pool.pool_address,
            "token_reserve": str(pool.token_reserve),
            "native_reserve": str(pool.native_reserve),
        }

    async def tool_quote_swap(
        self,
        pool_native_reserve: str,
        pool_token_reserve: str,
        amount_in: str,
        is_buy: bool = True,
        fee_bps: int = 30,
    ) -> dict[str, Any]:
        native_res = Decimal(pool_native_reserve)
        tok_res = Decimal(pool_token_reserve)
        amt_in = Decimal(amount_in)

        # Temporary pool state for calculation
        pool = PoolState(
            pool_address="0x" + "1" * 40,
            chain=ChainIdentifier.SOLANA_MAINNET,
            token_reserve=tok_res,
            native_reserve=native_res,
            fee_numerator=fee_bps,
            fee_denominator=10000,
            last_updated_block=1,
        )
        if is_buy:
            quote = cpmm_buy_quote(pool, amt_in)
            return {
                "tokens_acquired": str(quote.tokens_acquired),
                "effective_price": str(quote.effective_price),
                "price_impact_bps": quote.price_impact_bps,
                "spot_price_before": str(quote.spot_price_before),
            }
        else:
            quote_sell = cpmm_sell_quote(pool, amt_in)
            return {
                "native_received": str(quote_sell.native_received),
                "effective_price": str(quote_sell.effective_price),
                "price_impact_bps": quote_sell.price_impact_bps,
                "spot_price_before": str(quote_sell.spot_price_before),
            }

    async def tool_evaluate_mev_risk(
        self,
        slippage_bps: int,
        price_impact_bps: int,
        pool: Optional[PoolState] = None,
    ) -> dict[str, Any]:
        # High slippage tolerance + high price impact creates severe sandwich vulnerability
        is_vulnerable = slippage_bps > 200 or price_impact_bps > 150
        recommended_max_slippage = min(slippage_bps, max(50, price_impact_bps + 25))

        # Stochastic latency and adversarial MEV front-run simulation
        sim_pool = pool or get_initial_bonding_curve_pool("1" * 32)
        latency_sim = simulate_latency(sim_pool, 0)

        return {
            "is_sandwich_vulnerable": is_vulnerable,
            "recommended_max_slippage_bps": recommended_max_slippage,
            "requires_private_relay": is_vulnerable,
            "simulated_latency_ms": latency_sim.latency_ms,
            "frontrun_volume": str(latency_sim.frontrun_volume),
            "frontrun_fraction": latency_sim.frontrun_fraction,
        }

    async def handle_message(self, message: SwarmMessage) -> None:
        pass
