"""
alpha_engine.council.reach_agent — Agent 1: Web & Social Intelligence Agent (The "Reach" Analyst)
Inspired by Panniantong/Agent-Reach:
- Zero-API-Key Jina Reader (r.jina.ai -> Markdown)
- Public Reddit Crypto Multi-Subreddit Search JSON
- Public DuckDuckGo/X Web Syndication Fallback
- Bot-Farm & Paid Shilling Deception Filter
- Multi-Turn Cross-Examination and Rebuttal
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
import urllib.parse
from typing import Any, Optional

import aiohttp

from alpha_engine.council.llm_client import CouncilLLMClient
from alpha_engine.council.models import AgentVerdict, AgentVote, DebateRound, DebateStance

logger = logging.getLogger(__name__)

BULLISH_KEYWORDS = {"bull", "gem", "alpha", "buy", "holding", "undervalued", "solana", "launch", "pump", "breakout", "listing", "cto", "dev burned"}
BEARISH_KEYWORDS = {"rug", "scam", "dump", "honeypot", "fake", "drainer", "bot", "sell", "exploit", "hack", "abandoned"}
BOT_PROMO_KEYWORDS = {"dm to promote", "fast pump", "airdrop bot", "free giveaway", "automated raid", "send dm", "boost service", "trend boost"}


class ReachAnalystAgent:
    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        timeout_s: float = 4.0,
        llm_client: Optional[CouncilLLMClient] = None,
    ):
        self._session = session
        self._owns_session = False
        self._timeout_s = timeout_s
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        self.llm_client = llm_client or CouncilLLMClient(timeout_s=timeout_s)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers=self._headers,
                timeout=aiohttp.ClientTimeout(total=self._timeout_s),
            )
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    async def read_website(self, url: str) -> str:
        """Reads project website or docs via Jina Reader without API fees."""
        if not url or not url.startswith("http"):
            return ""
        session = await self._get_session()
        jina_url = f"https://r.jina.ai/{url.strip()}"
        try:
            async with session.get(jina_url, headers={"Accept": "text/markdown"}) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return re.sub(r"\n{3,}", "\n\n", text)[:3000].strip()
        except Exception as exc:
            logger.debug("Jina reader failed for %s: %s", url, exc)
        return ""

    async def search_reddit(self, query: str) -> list[str]:
        """Free public Reddit search in r/solana, r/CryptoCurrency, and r/memecoins."""
        session = await self._get_session()
        encoded = urllib.parse.quote(query)
        url = f"https://www.reddit.com/r/CryptoCurrency+solana+memecoins/search.json?q={encoded}&sort=new&limit=6&restrict_sr=1"
        posts = []
        try:
            headers = {"User-Agent": "BotMM-QuantResearch/2.0"}
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for child in data.get("data", {}).get("children", []):
                        p = child.get("data", {})
                        body = f"{p.get('title', '')} {p.get('selftext', '')}".strip()
                        if body:
                            posts.append(body[:400])
        except Exception as exc:
            logger.debug("Reddit search error for %s: %s", query, exc)
        return posts

    async def search_x_syndication(self, query: str) -> list[str]:
        """Free Twitter search via DuckDuckGo open search snippets."""
        session = await self._get_session()
        encoded = urllib.parse.quote(f"site:x.com OR site:twitter.com {query}")
        url = f"https://html.duckduckgo.com/html/?q={encoded}"
        tweets = []
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    snippets = re.findall(r'<a class="result__snippet"[^>]*>(.*?)</a>', html, re.DOTALL)
                    for s in snippets[:6]:
                        clean = re.sub(r"<[^>]+>", "", s).strip()
                        if clean:
                            tweets.append(clean[:300])
        except Exception as exc:
            logger.debug("X syndication search error for %s: %s", query, exc)
        return tweets

    async def audit_social_narrative(
        self,
        token_ca: str,
        symbol: str = "",
        website_url: Optional[str] = None,
        extra_metrics: Optional[dict[str, Any]] = None,
    ) -> AgentVerdict:
        """Round 1: Initial evaluation of sentiment, organic mindshare, and bot spam."""
        t0 = time.perf_counter()
        query = f"${symbol}" if symbol else token_ca[:12]

        reddit_task = self.search_reddit(query)
        x_task = self.search_x_syndication(query)
        site_task = self.read_website(website_url) if website_url else asyncio.sleep(0, result="")

        reddit_posts, x_posts, site_md = await asyncio.gather(reddit_task, x_task, site_task)
        all_posts = (reddit_posts or []) + (x_posts or [])

        # Sentiment scoring
        pos_hits = sum(1 for p in all_posts for w in BULLISH_KEYWORDS if w in p.lower())
        neg_hits = sum(1 for p in all_posts for w in BEARISH_KEYWORDS if w in p.lower())
        promo_hits = sum(1 for p in all_posts for w in BOT_PROMO_KEYWORDS if w in p.lower())

        flags = []
        if promo_hits >= 2:
            flags.append("PAID_BOT_RAID_DETECTED")
        if neg_hits > pos_hits and neg_hits >= 3:
            flags.append("COMMUNITY_FUD_OR_SCAM_WARNING")
        if pos_hits >= 4 and promo_hits == 0:
            flags.append("ORGANIC_NARRATIVE_BREAKOUT")

        total = pos_hits + neg_hits
        sentiment = (pos_hits - neg_hits) / total if total > 0 else 0.0

        score = 50.0 + (sentiment * 30.0)
        if len(all_posts) >= 5:
            score += 15.0  # High organic mindshare
        if promo_hits >= 2:
            score -= 35.0
        if site_md and len(site_md) > 150:
            score += 10.0  # Legitimate site exists
        score = max(0.0, min(100.0, score))

        if len(all_posts) == 0:
            # Newly launched tokens with no web mentions receive a clean-slate score (65.0)
            score = 65.0
            vote = AgentVote.APPROVE
            rationale = "Clean-slate reputation: newly launched token with zero web mentions and clean sentiment slate."
        else:
            if "PAID_BOT_RAID_DETECTED" in flags or score < 40.0:
                vote = AgentVote.REJECT
                rationale = f"Unfavorable social momentum: {len(all_posts)} mentions analyzed. Flags: {', '.join(flags) or 'Low sentiment'}"
            elif score >= 65.0:
                vote = AgentVote.APPROVE
                rationale = f"Strong organic sentiment ({sentiment:+.2f}) across {len(all_posts)} posts on Reddit/X with verified project identity."
            else:
                vote = AgentVote.NEUTRAL
                rationale = f"Moderate sentiment ({sentiment:+.2f}) with limited mentions ({len(all_posts)} posts)."

        reach_metrics: dict[str, Any] = {
            "mentions_found": len(all_posts),
            "sentiment": round(sentiment, 2),
            "website_verified": bool(site_md),
            "promo_hits": promo_hits,
        }
        if extra_metrics:
            reach_metrics.update(extra_metrics)

        duration_ms = (time.perf_counter() - t0) * 1000
        return AgentVerdict(
            agent_name="ReachAnalystAgent",
            vote=vote,
            confidence=round(min(0.95, max(0.50, len(all_posts) * 0.1 + 0.5)), 2),
            score=round(score, 1),
            rationale=rationale,
            flags=flags,
            metrics=reach_metrics,
            execution_time_ms=round(duration_ms, 1),
        )

    async def cross_examine_peer(
        self,
        peer_name: str,
        peer_verdict: AgentVerdict,
        my_verdict: AgentVerdict,
        token_ca: str,
    ) -> DebateRound:
        """Round 2: Cross-examine peer findings."""
        logger.debug("ReachAnalyst cross-examining peer %s for token %s", peer_name, token_ca)
        if "PAID_BOT_RAID_DETECTED" in my_verdict.flags:
            stance = DebateStance.CHALLENGE
            args = f"Challenge to {peer_name}: Social telemetry confirms artificial paid bot raid. Any volume or buy delta is likely manufactured exit liquidity."
        elif "ORGANIC_NARRATIVE_BREAKOUT" in my_verdict.flags and peer_verdict.vote != AgentVote.APPROVE:
            stance = DebateStance.CHALLENGE
            args = f"Counter-argument to {peer_name}: Genuine viral organic community conviction is spreading. Microstructure latency should not prevent early positioning."
        elif peer_verdict.vote == AgentVote.REJECT and "FATAL" in peer_verdict.rationale:
            stance = DebateStance.REVISE
            args = f"Conceding to {peer_name}: Narrative momentum cannot override severe security breach ({peer_verdict.rationale})."
        else:
            stance = DebateStance.APPROVE
            args = f"Social narrative aligns with {peer_name} analysis."

        return DebateRound(
            round_number=2,
            agent_name="ReachAnalyst",
            stance=stance.value,
            arguments=args,
            counter_arguments=peer_verdict.rationale,
            target_agent=peer_name,
            vote_weight=1.0,
            confidence=my_verdict.confidence,
            score=my_verdict.score,
            flags=my_verdict.flags,
        )

    async def defend_or_revise(
        self,
        peer_rounds: list[DebateRound],
        current_verdict: AgentVerdict,
    ) -> tuple[AgentVerdict, DebateRound]:
        """Round 3: Rebuttal/Revision based on peer cross-examinations."""
        revised_score = current_verdict.score
        revised_vote = current_verdict.vote

        # If Forensic declared a fatal veto or critical risk, downgrade Reach score
        forensic_vetos = [r for r in peer_rounds if r.agent_name == "ChainForensic" and (r.stance == "VETO" or "FATAL" in r.arguments)]
        if forensic_vetos:
            revised_score = min(30.0, current_verdict.score * 0.5)
            revised_vote = AgentVote.REJECT
            stance = DebateStance.REVISE
            args = "Revising downward: Narrative conviction yields to irrevocable forensic risk."
        elif "PAID_BOT_RAID_DETECTED" in current_verdict.flags:
            stance = DebateStance.DEFEND
            args = "Defending veto stance: Paid shill raid presents extreme dump hazard for entry."
        else:
            stance = DebateStance.DEFEND
            args = f"Defending social evaluation (Score: {revised_score:.1f}/100)."

        revised_verdict = AgentVerdict(
            agent_name="ReachAnalystAgent",
            vote=revised_vote,
            confidence=current_verdict.confidence,
            score=round(revised_score, 1),
            rationale=args,
            flags=current_verdict.flags,
            metrics=current_verdict.metrics,
            execution_time_ms=current_verdict.execution_time_ms,
        )
        round_entry = DebateRound(
            round_number=3,
            agent_name="ReachAnalyst",
            stance=stance.value,
            arguments=args,
            confidence=revised_verdict.confidence,
            score=revised_verdict.score,
            flags=revised_verdict.flags,
        )
        return revised_verdict, round_entry
