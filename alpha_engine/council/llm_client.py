"""
alpha_engine.council.llm_client — Multi-Model / Multi-Provider LLM client for Council debate
Supports Google Gemini, OpenAI, Anthropic, and Groq with strict latency budgets and fallback.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)


class CouncilLLMClient:
    """
    Lightweight, resilient LLM client tailored for the Council of 4 Agents.
    Executes fast cross-examination prompts with strict latency budgets (<4.0s).
    """

    def __init__(
        self,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        timeout_s: float = 3.5,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self._provider = (provider or os.getenv("AI_PROVIDER") or "gemini").lower()
        self._model = model or os.getenv("AI_MODEL") or self._default_model_for_provider(self._provider)
        self._timeout_s = timeout_s
        self._session = session
        self._owns_session = False

    def _default_model_for_provider(self, provider: str) -> str:
        p = provider.lower()
        if p == "gemini":
            return "gemini-3.8-flash"
        elif p == "anthropic":
            return "claude-3-5-sonnet-20241022"
        elif p == "groq":
            return "llama-3.3-70b-versatile"
        return "gpt-5.1"

    def _get_api_key(self, provider: str) -> Optional[str]:
        p = provider.lower()
        if p == "gemini":
            return os.getenv("GEMINI_API_KEY") or os.getenv("AI_API_KEY")
        elif p == "anthropic":
            return os.getenv("ANTHROPIC_API_KEY") or os.getenv("AI_API_KEY")
        elif p == "groq":
            return os.getenv("GROQ_API_KEY") or os.getenv("AI_API_KEY")
        return os.getenv("OPENAI_API_KEY") or os.getenv("AI_API_KEY")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": "CouncilOfAgents/2.0"},
                timeout=aiohttp.ClientTimeout(total=self._timeout_s),
            )
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    async def execute_debate_turn(
        self,
        agent_role: str,
        system_prompt: str,
        user_prompt: str,
        round_number: int,
    ) -> Optional[dict[str, Any]]:
        """
        Queries the configured LLM provider for a debate turn.
        Returns parsed JSON dict or None on timeout/failure.
        """
        api_key = self._get_api_key(self._provider)
        if not api_key:
            logger.debug("No API key for provider %s, using deterministic council logic", self._provider)
            return None

        try:
            return await asyncio.wait_for(
                self._dispatch_request(agent_role, system_prompt, user_prompt, api_key),
                timeout=self._timeout_s,
            )
        except asyncio.TimeoutError:
            logger.warning("Council LLM call timed out after %.1fs for %s (Round %d)", self._timeout_s, agent_role, round_number)
            return None
        except Exception as exc:
            logger.debug("Council LLM request error for %s: %s", agent_role, exc)
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
        logger.debug("[%s] Dispatching LLM deliberation request via provider=%s", agent_role, p)

        if p == "gemini":
            clean_model = self._model.replace("models/", "", 1) if self._model.startswith("models/") else self._model
            # Map legacy or internal model names to current public models
            if "3.8" in clean_model or "flash" in clean_model:
                clean_model = "gemini-3.8-flash"
            elif "pro" in clean_model:
                clean_model = "gemini-3.1-pro"

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
                    logger.debug("[%s] Gemini returned %d: %s", agent_role, resp.status, text[:150])
                    return None
                data = await resp.json()
                raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
                return self._parse_json(raw_text)

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
                "model": self._model,
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
                    logger.debug("[%s] OpenAI-compatible returned %d: %s", agent_role, resp.status, text[:150])
                    return None
                data = await resp.json()
                raw_text = data["choices"][0]["message"]["content"]
                return self._parse_json(raw_text)

        elif p == "anthropic":
            url = "https://api.anthropic.com/v1/messages"
            headers = {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }
            body = {
                "model": self._model,
                "max_tokens": 512,
                "system": f"{system_prompt}\nRespond STRICTLY with valid JSON matching the requested structure.",
                "messages": [{"role": "user", "content": user_prompt}],
                "temperature": 0.2,
            }
            async with session.post(url, json=body, headers=headers) as resp:
                if resp.status != 200:
                    logger.debug("[%s] Anthropic returned %d", agent_role, resp.status)
                    return None
                data = await resp.json()
                raw_text = data["content"][0]["text"]
                return self._parse_json(raw_text)

        return None

    def _parse_json(self, text: str) -> Optional[dict[str, Any]]:
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
