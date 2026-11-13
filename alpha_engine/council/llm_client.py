"""
alpha_engine.council.llm_client — Multi-Model / Multi-Provider LLM client for Council debate
Supports Google Gemini, OpenAI, Anthropic, Groq, and DeepSeek with strict latency budgets,
explicit concurrency/rate limits, schema validation, honest model passing, and deterministic fallback.
"""
from __future__ import annotations

import asyncio
from collections import deque
import json
import logging
import os
import re
import time
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)


class SchemaValidationError(ValueError):
    """Raised when LLM output violates the strict debate turn JSON schema."""


class InvalidModelError(ValueError):
    """Raised when configured model is rejected by the provider (no silent fallback)."""


def validate_debate_turn_schema(raw_dict: Any) -> dict[str, Any]:
    """
    Validate that an LLM response strictly conforms to the debate turn JSON schema:
      - vote: 'APPROVE', 'REJECT', or 'NEUTRAL'
      - confidence: float in [0.0, 1.0]
      - score: float in [0.0, 100.0]
      - evidence: list of strings citing concrete metrics
      - questions: list of strings (requests for peers)
      - flags: list of strings
    """
    if not isinstance(raw_dict, dict):
        raise SchemaValidationError(f"Expected JSON object dict, got {type(raw_dict).__name__}")

    # 1. Vote validation
    raw_vote = raw_dict.get("vote")
    if not raw_vote:
        raise SchemaValidationError("Missing required 'vote' field")
    vote_str = str(raw_vote).strip().upper()
    if vote_str not in ("APPROVE", "REJECT", "NEUTRAL"):
        raise SchemaValidationError(f"Invalid vote '{vote_str}'; must be APPROVE, REJECT, or NEUTRAL")

    # 2. Confidence validation
    raw_conf = raw_dict.get("confidence")
    if raw_conf is None:
        raise SchemaValidationError("Missing required 'confidence' field")
    try:
        conf = float(raw_conf)
        if not (0.0 <= conf <= 1.0):
            raise SchemaValidationError(f"Confidence {conf} out of bounds [0.0, 1.0]")
    except (ValueError, TypeError) as exc:
        raise SchemaValidationError(f"Invalid confidence value: {raw_conf}") from exc

    # 3. Score validation
    raw_score = raw_dict.get("score")
    if raw_score is None:
        raise SchemaValidationError("Missing required 'score' field")
    try:
        score = float(raw_score)
        if not (0.0 <= score <= 100.0):
            raise SchemaValidationError(f"Score {score} out of bounds [0.0, 100.0]")
    except (ValueError, TypeError) as exc:
        raise SchemaValidationError(f"Invalid score value: {raw_score}") from exc

    # 4. Evidence validation
    raw_ev = raw_dict.get("evidence", [])
    if not isinstance(raw_ev, list):
        raise SchemaValidationError(f"'evidence' must be a list of strings, got {type(raw_ev).__name__}")
    clean_evidence = [str(e).strip() for e in raw_ev if str(e).strip()]

    # 5. Questions validation
    raw_q = raw_dict.get("questions", [])
    clean_questions: list[str] = []
    if isinstance(raw_q, list):
        clean_questions = [str(q).strip() for q in raw_q if str(q).strip()]
    elif isinstance(raw_q, dict):
        clean_questions = [f"{k}: {v}" for k, v in raw_q.items() if str(v).strip()]
    elif raw_q:
        clean_questions = [str(raw_q).strip()]

    # 6. Flags validation
    raw_flags = raw_dict.get("flags", [])
    if not isinstance(raw_flags, list):
        raise SchemaValidationError(f"'flags' must be a list of strings, got {type(raw_flags).__name__}")
    clean_flags = [str(f).strip() for f in raw_flags if str(f).strip()]

    rationale = str(raw_dict.get("rationale") or "").strip()
    if not rationale:
        if clean_evidence:
            rationale = "; ".join(clean_evidence)
        else:
            rationale = f"Voted {vote_str} with score {score:.1f} (confidence {conf:.2f})"

    return {
        "vote": vote_str,
        "confidence": round(conf, 4),
        "score": round(score, 2),
        "evidence": clean_evidence,
        "questions": clean_questions,
        "flags": clean_flags,
        "rationale": rationale,
    }


def delimit_untrusted_content(text: str, source: str) -> str:
    """
    Wrap untrusted user, social, or metadata text in XML boundary tags
    to defend against prompt injection.
    """
    safe_text = str(text or "").replace("</untrusted_external_content>", "")
    return f'<untrusted_external_content source="{source}">\n{safe_text}\n</untrusted_external_content>'


class CouncilLLMClient:
    """
    Lightweight, resilient LLM client tailored for the Council of 4 Agents.
    Executes fast cross-examination prompts with strict latency budgets,
    rate limits, concurrency limits, schema validation, and honest model naming.
    """

    def __init__(
        self,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        timeout_s: float = 3.5,
        session: Optional[aiohttp.ClientSession] = None,
        max_concurrency: int = 4,
        rate_limit_rpm: int = 60,
        model_aliases: Optional[dict[str, str]] = None,
        max_cost_per_signal_usd: float = 0.05,
        fallback_warning_threshold: float = 0.50,
        fallback_window_size: int = 50,
    ):
        self._provider = (provider or os.getenv("AI_PROVIDER") or "gemini").lower()
        self._model = model or os.getenv("AI_MODEL") or self._default_model_for_provider(self._provider)
        self._timeout_s = timeout_s
        self._session = session
        self._owns_session = False
        self._model_aliases = dict(model_aliases or {})
        self._max_cost_per_signal_usd = max_cost_per_signal_usd
        self._fallback_warning_threshold = fallback_warning_threshold
        self._fallback_window_size = fallback_window_size

        # Concurrency & Rate Limiting
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._rate_limit_rpm = rate_limit_rpm
        self._request_timestamps: deque[float] = deque()
        self._rate_lock = asyncio.Lock()

        # Telemetry & Diagnostics
        self._llm_turns: int = 0
        self._fallback_turns: int = 0
        self._total_latency_ms: float = 0.0
        self._fallback_reasons: dict[str, int] = {}
        self._turn_history: deque[str] = deque(maxlen=fallback_window_size)
        self.last_turn_path: str = "fallback"
        self.last_fallback_reason: Optional[str] = None
        self.last_latency_ms: float = 0.0

    def _default_model_for_provider(self, provider: str) -> str:
        p = provider.lower()
        if p == "gemini":
            return "gemini-3.8-flash"
        elif p == "anthropic":
            return "claude-3-5-sonnet-latest"
        elif p == "groq":
            return "llama-3.3-70b-versatile"
        elif p == "deepseek":
            return "deepseek-chat"
        return "gpt-5"

    def _get_api_key(self, provider: str) -> Optional[str]:
        p = provider.lower()
        if p == "gemini":
            return os.getenv("GEMINI_API_KEY") or os.getenv("AI_API_KEY")
        elif p == "anthropic":
            return os.getenv("ANTHROPIC_API_KEY") or os.getenv("AI_API_KEY")
        elif p == "groq":
            return os.getenv("GROQ_API_KEY") or os.getenv("AI_API_KEY")
        elif p == "deepseek":
            return os.getenv("DEEPSEEK_API_KEY") or os.getenv("AI_API_KEY")
        return os.getenv("OPENAI_API_KEY") or os.getenv("AI_API_KEY")

    def _resolve_model(self) -> str:
        """Resolve model name, applying explicit alias map if configured."""
        if self._model_aliases and self._model in self._model_aliases:
            return self._model_aliases[self._model]
        return self._model

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": "CouncilOfAgents/2.0"},
                timeout=aiohttp.ClientTimeout(total=self._timeout_s + 1.0),
            )
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    async def _acquire_rate_slot(self) -> None:
        """Rate limiter for calls per minute."""
        async with self._rate_lock:
            now = time.monotonic()
            while self._request_timestamps and now - self._request_timestamps[0] >= 60.0:
                self._request_timestamps.popleft()
            if len(self._request_timestamps) >= self._rate_limit_rpm:
                wait_time = 60.0 - (now - self._request_timestamps[0])
                if wait_time > 0:
                    logger.debug("Council LLM rate limit reached; waiting %.2fs", wait_time)
                    await asyncio.sleep(min(wait_time, 2.0))
            self._request_timestamps.append(time.monotonic())

    def _record_success(self, latency_ms: float) -> None:
        self._llm_turns += 1
        self._total_latency_ms += latency_ms
        self._turn_history.append("llm")
        self.last_turn_path = "llm"
        self.last_fallback_reason = None
        self.last_latency_ms = latency_ms

    def _record_fallback(self, reason: str, latency_ms: float = 0.0) -> None:
        self._fallback_turns += 1
        self._fallback_reasons[reason] = self._fallback_reasons.get(reason, 0) + 1
        self._turn_history.append("fallback")
        self.last_turn_path = "fallback"
        self.last_fallback_reason = reason
        self.last_latency_ms = latency_ms

        ratio = self.get_fallback_ratio()
        if len(self._turn_history) >= 10 and ratio > self._fallback_warning_threshold:
            logger.warning(
                "Council LLM fallback ratio %.1f%% exceeds threshold %.1f%% over last %d turns (reason: %s)",
                ratio * 100.0,
                self._fallback_warning_threshold * 100.0,
                len(self._turn_history),
                reason,
            )

    def get_fallback_ratio(self) -> float:
        if not self._turn_history:
            total = self._llm_turns + self._fallback_turns
            return (self._fallback_turns / total) if total > 0 else 0.0
        return sum(1 for p in self._turn_history if p == "fallback") / len(self._turn_history)

    def get_telemetry(self) -> dict[str, Any]:
        total_turns = self._llm_turns + self._fallback_turns
        avg_lat = (self._total_latency_ms / self._llm_turns) if self._llm_turns > 0 else 0.0
        ratio = self.get_fallback_ratio()
        warning = len(self._turn_history) >= 5 and ratio >= self._fallback_warning_threshold
        return {
            "total_turns": total_turns,
            "llm_turns": self._llm_turns,
            "fallback_turns": self._fallback_turns,
            "fallback_ratio": round(ratio, 4),
            "avg_llm_latency_ms": round(avg_lat, 2),
            "fallback_reasons": dict(self._fallback_reasons),
            "warning_triggered": warning,
            "configured_model": self._model,
            "provider": self._provider,
        }

    async def execute_debate_turn(
        self,
        agent_role: str,
        system_prompt: str,
        user_prompt: str,
        round_number: int,
    ) -> Optional[dict[str, Any]]:
        """
        Queries the configured LLM provider for a debate turn.
        Enforces strict schema validation, timeout, concurrency, rate limits, and fallback.
        Returns parsed JSON dict or None on timeout/failure.
        """
        api_key = self._get_api_key(self._provider)
        if not api_key:
            logger.debug("No API key for provider %s, using deterministic council logic", self._provider)
            self._record_fallback("NO_API_KEY")
            return None

        t0 = time.perf_counter()
        try:
            await self._acquire_rate_slot()
            async with self._semaphore:
                raw_response = await asyncio.wait_for(
                    self._dispatch_request_with_retries(agent_role, system_prompt, user_prompt, api_key),
                    timeout=self._timeout_s,
                )
            latency_ms = (time.perf_counter() - t0) * 1000.0

            if raw_response is None:
                self._record_fallback("EMPTY_RESPONSE", latency_ms)
                return None

            # Enforce strict schema validation
            try:
                validated = validate_debate_turn_schema(raw_response)
                self._record_success(latency_ms)
                return validated
            except SchemaValidationError as s_err:
                logger.warning("[%s] Schema validation failed for Round %d: %s", agent_role, round_number, s_err)
                self._record_fallback("SCHEMA_VALIDATION_ERROR", latency_ms)
                return None

        except asyncio.TimeoutError:
            latency_ms = (time.perf_counter() - t0) * 1000.0
            logger.warning("Council LLM call timed out after %.1fs for %s (Round %d)", self._timeout_s, agent_role, round_number)
            self._record_fallback("TIMEOUT", latency_ms)
            return None
        except InvalidModelError as im_err:
            latency_ms = (time.perf_counter() - t0) * 1000.0
            logger.error("Council LLM invalid model for %s: %s", agent_role, im_err)
            self._record_fallback("INVALID_MODEL", latency_ms)
            return None
        except Exception as exc:
            latency_ms = (time.perf_counter() - t0) * 1000.0
            logger.warning("Council LLM request error for %s: %s", agent_role, exc)
            self._record_fallback("EXCEPTION", latency_ms)
            return None

    async def _dispatch_request_with_retries(
        self,
        agent_role: str,
        system_prompt: str,
        user_prompt: str,
        api_key: str,
        max_retries: int = 2,
    ) -> Optional[dict[str, Any]]:
        """Dispatch with exponential backoff on HTTP 429 rate limit."""
        delay = 0.2
        for attempt in range(max_retries + 1):
            try:
                return await self._dispatch_request(agent_role, system_prompt, user_prompt, api_key)
            except aiohttp.ClientResponseError as cre:
                if cre.status == 429 and attempt < max_retries:
                    logger.debug("[%s] Rate limited (429), retrying in %.2fs (attempt %d)", agent_role, delay, attempt + 1)
                    await asyncio.sleep(delay)
                    delay *= 2.0
                    continue
                raise
        return None

    async def _dispatch_request(
        self,
        agent_role: str,
        system_prompt: str,
        user_prompt: str,
        api_key: str,
    ) -> Optional[dict[str, Any]]:
        session = await self._get_session()
        p = self._provider
        clean_model = self._resolve_model()
        logger.debug("[%s] Dispatching LLM deliberation request via provider=%s, model=%s", agent_role, p, clean_model)

        if p == "gemini":
            if clean_model.startswith("models/"):
                clean_model = clean_model.replace("models/", "", 1)

            url = f"https://generativelanguage.googleapis.com/v1beta/models/{clean_model}:generateContent?key={api_key}"
            body = {
                "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
                "systemInstruction": {"parts": [{"text": system_prompt}]},
                "generationConfig": {
                    "responseMimeType": "application/json",
                    "temperature": 0.2,
                },
            }
            async with session.post(url, json=body) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    if resp.status in (404, 400) and any(w in text.lower() for w in ("not found", "invalid model", "does not exist")):
                        raise InvalidModelError(f"Model '{clean_model}' rejected by Gemini: {text[:150]}")
                    if resp.status == 429:
                        raise aiohttp.ClientResponseError(
                            request_info=resp.request_info,
                            history=resp.history,
                            status=429,
                            message="Gemini Rate Limit",
                        )
                    logger.debug("[%s] Gemini returned %d: %s", agent_role, resp.status, text[:150])
                    return None
                data = await resp.json()
                try:
                    raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
                    return self._parse_json(raw_text)
                except (KeyError, IndexError):
                    return None

        elif p in ("openai", "openrouter", "groq", "deepseek"):
            base_url = "https://api.openai.com/v1"
            if p == "openrouter":
                base_url = "https://openrouter.ai/api/v1"
            elif p == "groq":
                base_url = "https://api.groq.com/openai/v1"
            elif p == "deepseek":
                base_url = "https://api.deepseek.com/v1"

            url = f"{base_url}/chat/completions"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
            body = {
                "model": clean_model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.2,
            }
            async with session.post(url, json=body, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    if resp.status in (404, 400) and any(w in text.lower() for w in ("model", "not found", "invalid")):
                        raise InvalidModelError(f"Model '{clean_model}' rejected by {p}: {text[:150]}")
                    if resp.status == 429:
                        raise aiohttp.ClientResponseError(
                            request_info=resp.request_info,
                            history=resp.history,
                            status=429,
                            message=f"{p} Rate Limit",
                        )
                    logger.debug("[%s] %s returned %d: %s", agent_role, p, resp.status, text[:150])
                    return None
                data = await resp.json()
                try:
                    raw_text = data["choices"][0]["message"]["content"]
                    return self._parse_json(raw_text)
                except (KeyError, IndexError):
                    return None

        elif p == "anthropic":
            url = "https://api.anthropic.com/v1/messages"
            headers = {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }
            body = {
                "model": clean_model,
                "max_tokens": 512,
                "system": f"{system_prompt}\nRespond STRICTLY with valid JSON matching the requested structure.",
                "messages": [{"role": "user", "content": user_prompt}],
                "temperature": 0.2,
            }
            async with session.post(url, json=body, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    if resp.status in (404, 400) and any(w in text.lower() for w in ("model", "not found", "invalid")):
                        raise InvalidModelError(f"Model '{clean_model}' rejected by Anthropic: {text[:150]}")
                    if resp.status == 429:
                        raise aiohttp.ClientResponseError(
                            request_info=resp.request_info,
                            history=resp.history,
                            status=429,
                            message="Anthropic Rate Limit",
                        )
                    logger.debug("[%s] Anthropic returned %d: %s", agent_role, resp.status, text[:150])
                    return None
                data = await resp.json()
                try:
                    raw_text = data["content"][0]["text"]
                    return self._parse_json(raw_text)
                except (KeyError, IndexError):
                    return None

        return None

    def _parse_json(self, text: str) -> Optional[dict[str, Any]]:
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            m = re.search(r"\{.*\}", text, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(0))
                except Exception:
                    pass
        return None
