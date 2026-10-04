"""
alpha_engine.ingestion.svm — SVM (Solana) WebSocket Log Ingester
================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | websockets
"""

from __future__ import annotations

import asyncio
from collections import deque
import json
import logging
import time
from decimal import Decimal
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from alpha_engine.ingestion.decoders import (
    PUMP_FUN_PROGRAM_ID,
    RAYDIUM_AMM_PROGRAM_ID,
    SvmPoolMeta,
    _normalise,
    _parse_pump_fun_logs,
    _parse_pump_fun_trade_logs,
    _parse_raydium_initialize2_logs,
    _parse_raydium_log_line,
)
from alpha_engine.models.enums import ChainIdentifier, SignalSource
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    PumpMintEvent,
    PumpSwapEvent,
    RawSignalEvent,
    ShutdownSentinel,
    SwapEvent,
)
from alpha_engine.models.state import PoolState
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import SOLANA_SYSTEM_PROGRAM_IDS


logger = logging.getLogger(__name__)

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_MAP = {c: i for i, c in enumerate(_B58_ALPHABET)}


def _b58decode(s: str) -> bytes:
    if not s or not isinstance(s, str):
        raise ValueError("Invalid Base58 string")
    n = 0
    for c in s:
        if c not in _B58_MAP:
            raise ValueError(f"Invalid character in Base58: {c}")
        n = n * 58 + _B58_MAP[c]
    b = n.to_bytes((n.bit_length() + 7) // 8, "big") if n > 0 else b""
    pad = 0
    for c in s:
        if c == "1":
            pad += 1
        else:
            break
    return b"\x00" * pad + b


try:
    import base58  # type: ignore[import-not-found]
except ImportError:
    class _Base58Compat:
        @staticmethod
        def b58decode(v: str) -> bytes:
            return _b58decode(v)
    base58 = _Base58Compat()


def sanitize_solana_pubkey(addr: Any) -> str | None:
    """
    Sanitize and validate an address for Solana pubkey compliance.
    Strips raw byte slices, instruction data, brackets, quotes or padded arrays.
    Validates that the address is a valid base58 string decoding to exactly 32 bytes:
    len(base58.b58decode(addr)) == 32.
    """
    if not addr:
        return None
    if isinstance(addr, (bytes, bytearray)):
        try:
            addr = addr.decode("utf-8", errors="ignore")
        except Exception:
            return None
    if not isinstance(addr, str):
        return None
    # Strip any raw byte slices, instruction data, brackets, quotes or padded arrays
    clean = addr.strip().strip("'\"[]()<>,; \t\r\n")
    if not (32 <= len(clean) <= 44):
        return None
    try:
        decoded = base58.b58decode(clean)
        if len(decoded) == 32:
            return clean
    except Exception:
        return None
    return None


def is_valid_solana_pubkey(addr: str) -> bool:
    """Validate that an address is a genuine 32-byte Base58 Solana public key."""
    return sanitize_solana_pubkey(addr) is not None


class SubscriptionRegistry:
    """
    Dedicated bidirectional registry for SVM WebSocket subscriptions.
    Tracks mint/pool <-> subscription_id mappings, timestamps, and protects
    permanent infrastructure subscriptions (Pump.fun program, Raydium program).
    """

    def __init__(self, max_capacity: int = 800) -> None:
        self.max_capacity = max_capacity
        self._target_to_sub: dict[str, int] = {}
        self._sub_to_target: dict[int, str] = {}
        self._target_timestamps: dict[str, float] = {}
        self._permanent_targets: set[str] = set()

    def __len__(self) -> int:
        return len(self._target_to_sub)

    def register(self, target: str, sub_id: int, is_permanent: bool = False, timestamp: float | None = None) -> str | None:
        evicted = None
        if len(self._target_to_sub) >= self.max_capacity and target not in self._target_to_sub:
            non_perm = [
                (t, self._target_timestamps.get(t, 0.0))
                for t in self._target_to_sub
                if t not in self._permanent_targets
            ]
            if non_perm:
                oldest_target = min(non_perm, key=lambda kv: kv[1])[0]
                self.remove(oldest_target)
                evicted = oldest_target

        now = timestamp if timestamp is not None else time.time()
        self._target_to_sub[target] = sub_id
        self._sub_to_target[sub_id] = target
        self._target_timestamps[target] = now
        if is_permanent:
            self._permanent_targets.add(target)
        return evicted

    def touch(self, target: str, timestamp: float | None = None) -> None:
        if target in self._target_to_sub:
            self._target_timestamps[target] = timestamp if timestamp is not None else time.time()

    def get_sub_id(self, target: str) -> int | None:
        return self._target_to_sub.get(target)

    def get_target(self, sub_id: int) -> str | None:
        return self._sub_to_target.get(sub_id)

    def is_permanent(self, target: str) -> bool:
        return target in self._permanent_targets

    def remove(self, target: str) -> int | None:
        sub_id = self._target_to_sub.pop(target, None)
        if sub_id is not None:
            self._sub_to_target.pop(sub_id, None)
        self._target_timestamps.pop(target, None)
        return sub_id

    def remove_by_sub_id(self, sub_id: int) -> str | None:
        target = self._sub_to_target.pop(sub_id, None)
        if target is not None:
            self._target_to_sub.pop(target, None)
            self._target_timestamps.pop(target, None)
        return target

    def get_stale_targets(self, max_age_s: float = 120.0, current_time: float | None = None) -> list[tuple[str, int]]:
        """Return non-permanent targets with age > max_age_s."""
        now = current_time if current_time is not None else time.time()
        stale: list[tuple[str, int]] = []
        for target, ts in list(self._target_timestamps.items()):
            if target not in self._permanent_targets and (now - ts) > max_age_s:
                sub_id = self._target_to_sub.get(target)
                if sub_id is not None:
                    stale.append((target, sub_id))
        return stale

    def active_count(self) -> int:
        return len(self._target_to_sub)

    def clear(self) -> None:
        self._target_to_sub.clear()
        self._sub_to_target.clear()
        self._target_timestamps.clear()


class SVMIngester:
    """
    Ingests Raydium V4 swap events from Solana via Helius WebSocket.
    """

    def __init__(
        self,
        ws_url: str,
        pool_registry: dict[str, SvmPoolMeta],
        event_queue: asyncio.Queue[SwapEvent | PoolStateUpdateEvent | RawSignalEvent | ShutdownSentinel],
        limiter: RateLimiterRegistry,
        failover_urls: list[str] | None = None,
    ) -> None:
        self._primary_ws_url = ws_url
        candidates = [ws_url]
        if failover_urls:
            for u in failover_urls:
                if u and u not in candidates:
                    candidates.append(u)
        if "wss://api.mainnet-beta.solana.com" not in candidates:
            candidates.append("wss://api.mainnet-beta.solana.com")
        self._url_candidates = candidates
        self._url_idx = 0
        self._ws_url = self._url_candidates[0]
        self._consecutive_fails = 0
        self._pool_registry: dict[str, SvmPoolMeta] = dict(pool_registry)
        self._monitored_pools: set[str] = set(pool_registry.keys())
        self._queue = event_queue
        self._limiter = limiter
        self._running = False

        from alpha_engine.dns_resolver import patch_dns_resolvers
        patch_dns_resolvers()

        from alpha_engine.ingestion.coordinator import ExponentialBackoff
        # Exponential backoff starting at 1.0s, capped at 10.0s
        self._backoff = ExponentialBackoff(initial=1.0, max_delay=10.0)
        self._sub_to_pool: dict[int, str] = {}
        self._subscriptions = SubscriptionRegistry(max_capacity=800)
        self._ws: Any = None
        self._req_id = 1
        self._pending_ids: dict[int, str] = {}
        self._pending_unsubs: dict[int, str] = {}
        self._sweep_task: asyncio.Task[None] | None = None
        self._ssl_fallback = False
        self._trade_dedup_set: set[str] = set()
        self._trade_dedup_queue: deque[str] = deque(maxlen=5000)
        self._mint_dedup_set: set[str] = set()
        self._mint_dedup_queue: deque[str] = deque(maxlen=2000)

    @property
    def subscriptions(self) -> SubscriptionRegistry:
        return self._subscriptions

    async def unsubscribe(self, target: str) -> bool:
        """
        Asynchronously dispatch logsUnsubscribe for target token or pool.
        Removes subscription from registry and pool registry.
        """
        clean = sanitize_solana_pubkey(target) or target
        if not clean:
            return False

        if self._subscriptions.is_permanent(clean):
            logger.debug("Cannot unsubscribe permanent infrastructure target %s", clean)
            return False

        self._monitored_pools.discard(clean)
        self._monitored_pools.discard(target)
        sub_id = self._subscriptions.remove(clean)
        self._pool_registry.pop(clean, None)

        if sub_id is None:
            # Check if clean target is a token address mapped to a curve
            for curve, meta in list(self._pool_registry.items()):
                if meta[0] == clean:
                    self._monitored_pools.discard(curve)
                    self._pool_registry.pop(curve, None)
                    sub_id = self._subscriptions.remove(curve)
                    clean = curve
                    break

        if sub_id is not None:
            self._sub_to_pool.pop(sub_id, None)
            ws = self._ws
            if ws is not None:
                try:
                    self._req_id += 1
                    req_id = self._req_id
                    self._pending_unsubs[req_id] = clean
                    msg = json.dumps({
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "method": "logsUnsubscribe",
                        "params": [sub_id],
                    })
                    await ws.send(msg)
                    logger.info("SVM logsUnsubscribe dispatched for %s (sub_id=%d, req_id=%d)", clean[:10], sub_id, req_id)
                    return True
                except Exception as exc:
                    logger.warning("Failed to send logsUnsubscribe for %s: %s", clean[:10], exc)
            return True
        return False

    async def sweep_stale_subscriptions(self, max_age_s: float = 120.0) -> list[str]:
        """Evict stale non-permanent subscriptions older than max_age_s."""
        stale = self._subscriptions.get_stale_targets(max_age_s=max_age_s)
        unsubscribed: list[str] = []
        for target, _ in stale:
            success = await self.unsubscribe(target)
            if success:
                unsubscribed.append(target)
        if unsubscribed:
            logger.info(
                "Evicted %d stale SVM subscriptions (older than %.0fs): %s",
                len(unsubscribed),
                max_age_s,
                [t[:8] for t in unsubscribed[:5]],
            )
        return unsubscribed

    async def _eviction_loop(self) -> None:
        """Periodic background sweeper for stale subscriptions."""
        while self._running:
            try:
                await asyncio.sleep(20.0)
                if self._running:
                    await self.sweep_stale_subscriptions(max_age_s=120.0)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Error in SVM subscription eviction loop: %s", exc)

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._connect_and_stream()
                if self._running:
                    logger.info("SVM WebSocket closed cleanly — reconnecting.")
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                if not self._running:
                    break
                err_msg = str(exc)
                if ("CERTIFICATE_VERIFY_FAILED" in err_msg or "certificate verify failed" in err_msg) and not self._ssl_fallback:
                    logger.warning(
                        "SVM WebSocket SSL certificate verification failed (%s). Activating resilient unverified SSL fallback.",
                        exc,
                    )
                    self._ssl_fallback = True
                    await asyncio.sleep(0.5)
                    continue

                self._consecutive_fails += 1
                curr_url = self._ws_url
                close_code = None
                if hasattr(exc, "rcvd") and exc.rcvd:
                    close_code = getattr(exc.rcvd, "code", None)

                is_rate_limited = (
                    close_code in (1013, 1008)
                    or "429" in err_msg
                    or "1013" in err_msg
                    or "rate limit" in err_msg.lower()
                    or "too many subscriptions" in err_msg.lower()
                )
                if len(self._url_candidates) > 1 and (is_rate_limited or self._consecutive_fails >= 2):
                    self._url_idx = (self._url_idx + 1) % len(self._url_candidates)
                    self._ws_url = self._url_candidates[self._url_idx]
                    logger.warning(
                        "SVM WebSocket rate-limited/rejected on %s (%s). Failing over to %s",
                        curr_url,
                        exc,
                        self._ws_url,
                    )
                    await asyncio.sleep(1.0)
                    continue

                delay = self._backoff.next_delay()
                if is_rate_limited:
                    delay = max(delay, 5.0)
                logger.warning(
                    "SVM WebSocket ConnectionClosed/error: %s — retrying with exponential backoff in %.1fs",
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                logger.info("SVMIngester task cancelled.")
                break

    async def stop(self) -> None:
        self._running = False
        if self._sweep_task and not self._sweep_task.done():
            self._sweep_task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass

    async def _connect_and_stream(self) -> None:
        if "helius" in self._ws_url:
            await self._limiter.helius.acquire(cost=1.0)

        import ssl
        import certifi

        ssl_ctx = None
        if self._ws_url.startswith("wss://"):
            try:
                ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            except Exception:
                ssl_ctx = ssl.create_default_context()
            if self._ssl_fallback:
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

        async with websockets.connect(
            self._ws_url,
            ssl=ssl_ctx,
            ping_interval=20,
            ping_timeout=60,
            close_timeout=10,
            max_size=10 * 1024 * 1024,
            max_queue=4096,
        ) as ws:
            logger.info("SVM WebSocket connected: %s", self._ws_url)
            self._ws = ws
            self._sub_to_pool.clear()
            self._subscriptions.clear()

            self._sweep_task = asyncio.create_task(self._eviction_loop())

            self._req_id = 1
            self._pending_ids.clear()
            self._pending_unsubs.clear()
            confirmed = 0

            raw_targets = list(self._monitored_pools)
            if PUMP_FUN_PROGRAM_ID not in raw_targets:
                raw_targets.append(PUMP_FUN_PROGRAM_ID)
            if RAYDIUM_AMM_PROGRAM_ID not in raw_targets:
                raw_targets.append(RAYDIUM_AMM_PROGRAM_ID)

            valid_targets: list[str] = []
            for target in raw_targets:
                clean = sanitize_solana_pubkey(target)
                if clean:
                    if clean not in valid_targets:
                        valid_targets.append(clean)
                else:
                    logger.warning(
                        "Rejecting malformed or non-32-byte Solana address for mentions filter: '%s'",
                        target,
                    )

            if not valid_targets:
                valid_targets = [PUMP_FUN_PROGRAM_ID, RAYDIUM_AMM_PROGRAM_ID]

            # Bounded subscription safety: cap initial targets to subscription capacity (cap at 95 on solana.com to prevent 1013 limit)
            max_allowed = min(self._subscriptions.max_capacity, 95) if "solana.com" in self._ws_url else self._subscriptions.max_capacity
            valid_targets = valid_targets[:max_allowed]
            expected = len(valid_targets)
            batch_size = 20

            # Throttled Subscription Batching: chunks of 20 pools with 0.08s pause
            async def _send_subscriptions() -> None:
                try:
                    for i in range(0, len(valid_targets), batch_size):
                        if not self._running:
                            break
                        batch = valid_targets[i : i + batch_size]
                        for target_pubkey in batch:
                            curr_id = self._req_id
                            self._req_id += 1
                            self._pending_ids[curr_id] = target_pubkey
                            msg = json.dumps({
                                "jsonrpc": "2.0",
                                "id": curr_id,
                                "method": "logsSubscribe",
                                "params": [
                                    {"mentions": [target_pubkey]},
                                    {"commitment": "confirmed"},
                                ],
                            })
                            await ws.send(msg)
                        if i + batch_size < len(valid_targets):
                            await asyncio.sleep(0.08)
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logger.warning("SVM subscription sender task encountered error: %s", exc)

            sub_task = asyncio.create_task(_send_subscriptions())

            try:
                async for raw_msg in ws:
                    if not self._running:
                        return
                    try:
                        msg: dict[str, Any] = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue

                    # Handle unsubscription confirmation responses
                    resp_id = msg.get("id")
                    if resp_id in self._pending_unsubs:
                        unsub_target = self._pending_unsubs.pop(resp_id)
                        logger.debug("SVM logsUnsubscribe confirmed for req_id=%s (target=%s)", resp_id, unsub_target[:10])
                        continue

                    # Handle subscription confirmation responses
                    if resp_id in self._pending_ids:
                        sub_id = msg.get("result")
                        if isinstance(sub_id, int):
                            pool_key = self._pending_ids.pop(resp_id)
                            self._sub_to_pool[sub_id] = pool_key
                            is_permanent = pool_key in (PUMP_FUN_PROGRAM_ID, RAYDIUM_AMM_PROGRAM_ID)
                            self._subscriptions.register(pool_key, sub_id, is_permanent=is_permanent)
                            confirmed += 1
                            logger.debug(
                                "SVM pool %s…%s subscribed (sub_id=%d, confirmed=%d/%d)",
                                pool_key[:6], pool_key[-4:], sub_id, confirmed, expected,
                            )
                            if confirmed == expected:
                                logger.info(
                                    "SVM subscribed to all %d pool(s).",
                                    confirmed,
                                )
                                self._consecutive_fails = 0
                                self._backoff.reset()
                        elif "error" in msg:
                            pool_key = self._pending_ids.pop(resp_id, "unknown")
                            logger.error(
                                "SVM logsSubscribe error for req_id=%s: %s",
                                resp_id, msg["error"],
                            )
                            confirmed += 1
                        continue

                    if msg.get("method") != "logsNotification":
                        continue

                    # Healthy steady-state streaming: reset backoff and failure counter
                    if self._consecutive_fails > 0:
                        self._consecutive_fails = 0
                        self._backoff.reset()

                    params = msg.get("params", {})
                    sub_id = params.get("subscription")
                    pool_key = self._sub_to_pool.get(sub_id)
                    if pool_key is None:
                        continue
                    self._subscriptions.touch(pool_key)

                    result_ctx = params.get("result", {})
                    slot: int = result_ctx.get("context", {}).get("slot", 0)
                    value = result_ctx.get("value", {})
                    if not value or value.get("err") is not None:
                        continue

                    tx_sig: str = value.get("signature", "")
                    logs: list[str] = value.get("logs", [])

                    # 1. Check for Pump.fun Trade (Buy / Swap / Sell)
                    pump_trade = _parse_pump_fun_trade_logs(logs, tx_sig, slot=slot)
                    if pump_trade is not None:
                        mint_addr = sanitize_solana_pubkey(pump_trade.get("mint"))
                        buyer_addr = sanitize_solana_pubkey(pump_trade.get("buyer")) or (tx_sig[:44] if len(tx_sig) >= 32 else "PumpBuyer111111111111111111111111111111111")
                        sol_amt = pump_trade.get("sol_amount", Decimal("0"))
                        if (
                            mint_addr
                            and mint_addr not in SOLANA_SYSTEM_PROGRAM_IDS
                            and not mint_addr.startswith("11111111")
                        ):
                            # Deduplication set based on slot:mint:buyer:sol_amount
                            trade_dedup_key = f"{slot}:{mint_addr}:{buyer_addr}:{sol_amt}"
                            if trade_dedup_key in self._trade_dedup_set:
                                logger.debug("Duplicate SVM Pump.fun trade skipped: %s", trade_dedup_key)
                            else:
                                self._trade_dedup_set.add(trade_dedup_key)
                                self._trade_dedup_queue.append(trade_dedup_key)
                                if len(self._trade_dedup_queue) > 5000:
                                    old_key = self._trade_dedup_queue.popleft()
                                    self._trade_dedup_set.discard(old_key)

                                sol_mint = "So11111111111111111111111111111111111111112"
                                is_buy = pump_trade.get("is_buy", True)
                                tok_amt = pump_trade.get("token_amount", Decimal("0"))

                                curve_addr = mint_addr
                                if pool_key in self._pool_registry and pool_key != PUMP_FUN_PROGRAM_ID:
                                    curve_addr = pool_key
                                else:
                                    for c_addr, meta in self._pool_registry.items():
                                        if meta[0] == mint_addr:
                                            curve_addr = c_addr
                                            break

                                if is_buy:
                                    token_in = sol_mint
                                    token_out = mint_addr
                                    amt_in = sol_amt
                                    amt_out = tok_amt
                                else:
                                    token_in = mint_addr
                                    token_out = sol_mint
                                    amt_in = tok_amt
                                    amt_out = sol_amt

                                v_sol = pump_trade.get("virtual_sol_reserves", Decimal("0"))
                                v_tok = pump_trade.get("virtual_token_reserves", Decimal("0"))

                                if amt_in > Decimal(0) and amt_out > Decimal(0):
                                    swap_ev = PumpSwapEvent(
                                        timestamp_ns=time.time_ns(),
                                        block_number=slot,
                                        chain=ChainIdentifier.SOLANA_MAINNET,
                                        pool_address=curve_addr,
                                        token_in=token_in,
                                        token_out=token_out,
                                        amount_in=amt_in,
                                        amount_out=amt_out,
                                        sender=buyer_addr,
                                        tx_hash=tx_sig,
                                        log_index=None,
                                        mint=mint_addr,
                                        sol_amount=sol_amt,
                                        token_amount=tok_amt,
                                        buyer=buyer_addr,
                                        is_buy=is_buy,
                                        virtual_sol_reserves=v_sol if v_sol > Decimal(0) else Decimal("30.0"),
                                        virtual_token_reserves=v_tok if v_tok > Decimal(0) else Decimal("1073000000.0"),
                                        has_authoritative_reserves=bool(pump_trade.get("has_authoritative_reserves", False)),
                                        slot=slot,
                                    )
                                    await self._queue.put(swap_ev)
                                    logger.info(
                                        "SVM Pump.fun trade queued: %s mint=%s sol=%s buyer=%s slot=%d",
                                        "BUY" if is_buy else "SELL",
                                        mint_addr[:10],
                                        sol_amt,
                                        buyer_addr[:8],
                                        slot,
                                    )

                                if v_sol > Decimal(0) and v_tok > Decimal(0):
                                    pool_state = PoolState(
                                        pool_address=curve_addr,
                                        chain=ChainIdentifier.SOLANA_MAINNET,
                                        token_address=mint_addr,
                                        native_reserve=v_sol,
                                        token_reserve=v_tok,
                                        fee_numerator=10,
                                        fee_denominator=1000,
                                        last_updated_block=slot,
                                        token_decimals=6,
                                        native_decimals=9,
                                    )
                                    await self._queue.put(
                                        PoolStateUpdateEvent(
                                            timestamp_ns=time.time_ns(),
                                            chain=ChainIdentifier.SOLANA_MAINNET,
                                            pool_address=curve_addr,
                                            new_pool_state=pool_state,
                                        )
                                    )

                    # 2. Check for Raydium Swap
                    event = self._parse_raydium_transaction(tx_sig, logs, pool_key)
                    if event is not None:
                        await self._queue.put(event)
                        logger.debug(
                            "SVM Swap queued: pool=%s sig=%s",
                            pool_key[:10], tx_sig[:12],
                        )

                    # 3. Check for Pump.fun Mint / Bonding Curve Creation
                    pump_info = _parse_pump_fun_logs(logs, tx_sig, slot=slot)
                    if pump_info is not None:
                        mint_addr = sanitize_solana_pubkey(pump_info.get("mint") or pump_info.mint)
                        curve_addr = sanitize_solana_pubkey(pump_info.get("bonding_curve") or pump_info.bonding_curve)
                        if (
                            mint_addr
                            and curve_addr
                            and mint_addr not in SOLANA_SYSTEM_PROGRAM_IDS
                            and not mint_addr.startswith("11111111")
                        ):
                            mint_dedup_key = f"{slot}:{mint_addr}"
                            if mint_dedup_key in self._mint_dedup_set:
                                logger.debug("Duplicate SVM Pump.fun mint skipped: %s", mint_dedup_key)
                            else:
                                self._mint_dedup_set.add(mint_dedup_key)
                                self._mint_dedup_queue.append(mint_dedup_key)
                                if len(self._mint_dedup_queue) > 2000:
                                    old_key = self._mint_dedup_queue.popleft()
                                    self._mint_dedup_set.discard(old_key)

                                tok_dec = pump_info.get("token_decimals", 6)
                                nat_dec = pump_info.get("native_decimals", 9)
                                v_sol = pump_info.get("virtual_sol_reserves", Decimal("30.0"))
                                v_tok = pump_info.get("virtual_token_reserves", Decimal("1073000000.0"))

                                self._pool_registry[curve_addr] = (
                                    mint_addr,
                                    "So11111111111111111111111111111111111111112",
                                    tok_dec,
                                    nat_dec,
                                )
                                pool_state = PoolState(
                                    pool_address=curve_addr,
                                    chain=ChainIdentifier.SOLANA_MAINNET,
                                    token_address=mint_addr,
                                    native_reserve=v_sol,
                                    token_reserve=v_tok,
                                    fee_numerator=10,
                                    fee_denominator=1000,
                                    last_updated_block=slot,
                                    token_decimals=tok_dec,
                                    native_decimals=nat_dec,
                                )
                                await self._queue.put(
                                    PoolStateUpdateEvent(
                                        timestamp_ns=time.time_ns(),
                                        chain=ChainIdentifier.SOLANA_MAINNET,
                                        pool_address=curve_addr,
                                        new_pool_state=pool_state,
                                    )
                                )
                                mint_ev = PumpMintEvent(
                                    timestamp_ns=time.time_ns(),
                                    chain=ChainIdentifier.SOLANA_MAINNET,
                                    mint=mint_addr,
                                    bonding_curve=curve_addr,
                                    virtual_sol_reserves=v_sol,
                                    virtual_token_reserves=v_tok,
                                    token_decimals=tok_dec,
                                    native_decimals=nat_dec,
                                    tx_hash=tx_sig,
                                    slot=slot,
                                )
                                await self._queue.put(mint_ev)
                                await self._queue.put(
                                    RawSignalEvent(
                                        chain=ChainIdentifier.SOLANA_MAINNET,
                                        token_address=mint_addr,
                                        pool_address=curve_addr,
                                        source=SignalSource.PUMP_FUN_MINT,
                                        originating_channel="pump_fun_stream",
                                        raw_text=f"Pump.fun New Mint: {mint_addr} curve={curve_addr}",
                                    )
                                )
                                logger.info("SVM Pump.fun mint detected: %s (curve=%s)", mint_addr[:10], curve_addr[:10])

                    # 4. Check for Raydium AMM Pool Creation
                    ray_init = _parse_raydium_initialize2_logs(logs, tx_sig)
                    if ray_init is not None:
                        pool_addr = sanitize_solana_pubkey(ray_init.get("pool_address"))
                        if (
                            pool_addr
                            and pool_addr not in SOLANA_SYSTEM_PROGRAM_IDS
                            and not pool_addr.startswith("11111111")
                        ):
                            await self._queue.put(
                                RawSignalEvent(
                                    chain=ChainIdentifier.SOLANA_MAINNET,
                                    token_address=pool_addr,
                                    pool_address=pool_addr,
                                    source=SignalSource.PAIR_CREATED,
                                    originating_channel="raydium_stream",
                                    raw_text=f"Raydium AMM CreatePool: {pool_addr}",
                                )
                            )
                            logger.info("SVM Raydium CreatePool detected: %s", pool_addr[:10])
            finally:
                if self._sweep_task and not self._sweep_task.done():
                    self._sweep_task.cancel()
                    try:
                        await self._sweep_task
                    except (asyncio.CancelledError, Exception):
                        pass
                if not sub_task.done():
                    sub_task.cancel()
                    try:
                        await sub_task
                    except (asyncio.CancelledError, Exception):
                        pass
                self._ws = None


    def _parse_raydium_transaction(
        self,
        tx_signature: str,
        logs: list[str],
        pool_pubkey: str,
    ) -> SwapEvent | None:
        meta = self._pool_registry.get(pool_pubkey)
        if meta is None:
            return None

        coin_mint, pc_mint, coin_dec, pc_dec = meta

        for line in logs:
            parsed = _parse_raydium_log_line(line)
            if parsed is None:
                continue

            direction: int = parsed["direction"]

            if direction == 0:
                token_in = pc_mint
                token_out = coin_mint
                amount_in = _normalise(parsed["amount_in"], pc_dec)
                amount_out = _normalise(parsed["amount_out"], coin_dec)
            else:
                token_in = coin_mint
                token_out = pc_mint
                amount_in = _normalise(parsed["amount_in"], coin_dec)
                amount_out = _normalise(parsed["amount_out"], pc_dec)

            if amount_in <= Decimal(0) or amount_out <= Decimal(0):
                continue

            return SwapEvent(
                timestamp_ns=time.time_ns(),
                block_number=0,
                chain=ChainIdentifier.SOLANA_MAINNET,
                pool_address=pool_pubkey,
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                amount_out=amount_out,
                sender=tx_signature[:44],
                tx_hash=tx_signature,
                log_index=None,
            )

        return None
