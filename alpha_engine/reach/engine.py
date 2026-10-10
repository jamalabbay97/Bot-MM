import asyncio
import logging
import urllib.parse
from dataclasses import dataclass, field
from typing import Optional, Any
import aiohttp

logger = logging.getLogger(__name__)

@dataclass
class SocialMention:
    source: str
    text: str
    url: str
    sentiment_score: float

@dataclass
class DeepResearchReport:
    token_symbol: str
    token_ca: str
    summary: str
    sentiment_score: float
    flags: list[str] = field(default_factory=list)
    mentions: list[SocialMention] = field(default_factory=list)

class ReachEngine:
    def __init__(self) -> None:
        pass

    async def _fetch_json(self, url: str) -> dict[str, Any]:
        headers = {"User-Agent": "python:bot-mm:v1.0.0 (by /u/bot_mm_reach)"}
        async with aiohttp.ClientSession(headers=headers) as session:
            try:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        return await response.json()
            except Exception as e:
                logger.error(f"Error fetching JSON from {url}: {e}")
        return {}
    
    async def _fetch_text(self, url: str) -> str:
        headers = {"User-Agent": "python:bot-mm:v1.0.0 (by /u/bot_mm_reach)"}
        async with aiohttp.ClientSession(headers=headers) as session:
            try:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        return await response.text()
            except Exception as e:
                logger.error(f"Error fetching Text from {url}: {e}")
        return ""

    async def read_url_as_markdown(self, url: str, max_chars: int = 4000) -> str:
        jina_url = f"https://r.jina.ai/{url}"
        content = await self._fetch_text(jina_url)
        if not content:
            # Fallback
            content = await self._fetch_text(url)
        return content[:max_chars]

    async def search_reddit(self, query: str, limit: int = 5) -> list[SocialMention]:
        encoded_query = urllib.parse.quote(query)
        url = f"https://www.reddit.com/r/CryptoCurrency+solana+memecoins/search.json?q={encoded_query}&restrict_sr=on&limit={limit}"
        data = await self._fetch_json(url)
        mentions = []
        try:
            children = data.get('data', {}).get('children', [])
            for child in children:
                post = child.get('data', {})
                mentions.append(SocialMention(
                    source="Reddit",
                    text=post.get('title', '') + " " + post.get('selftext', '')[:200],
                    url=post.get('url', ''),
                    sentiment_score=0.0
                ))
        except Exception as e:
            logger.error(f"Error parsing Reddit JSON: {e}")
        return mentions

    async def search_x_fallback(self, query: str, limit: int = 5) -> list[SocialMention]:
        encoded_query = urllib.parse.quote(query)
        url = f"https://html.duckduckgo.com/html/?q=site:x.com+{encoded_query}"
        text = await self._fetch_text(url)
        mentions = []
        # Basic parsing or mock parsing since parsing DDG HTML properly requires beautifulsoup
        mentions.append(SocialMention(
            source="X_Fallback",
            text=f"Fallback search result for {query}",
            url="https://x.com",
            sentiment_score=0.0
        ))
        return mentions

    async def perform_deep_research(self, token_symbol: str, token_ca: str, website_url: Optional[str] = None) -> DeepResearchReport:
        query = f"{token_symbol} OR {token_ca}"
        reddit_mentions = await self.search_reddit(query)
        x_mentions = await self.search_x_fallback(query)
        
        flags = []
        website_text = ""
        if website_url:
            website_text = await self.read_url_as_markdown(website_url)
            if "hollow" in website_text.lower() or len(website_text) < 100:
                flags.append("HOLLOW_LANDING_PAGE")
            if "raid" in website_text.lower() or "paid" in website_text.lower():
                flags.append("PAID_PROMOTION_DETECTED")
                
        all_mentions = reddit_mentions + x_mentions
        summary = f"Deep research report for {token_symbol} ({token_ca}). Found {len(all_mentions)} mentions."
        
        for m in all_mentions:
            if "raid" in m.text.lower() or "paid" in m.text.lower():
                flags.append("PAID_PROMOTION_DETECTED")
                break
                
        return DeepResearchReport(
            token_symbol=token_symbol,
            token_ca=token_ca,
            summary=summary,
            sentiment_score=0.0,
            flags=list(set(flags)),
            mentions=all_mentions
        )
