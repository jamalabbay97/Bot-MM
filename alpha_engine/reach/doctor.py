import time
import aiohttp
import asyncio
import socket
from typing import Any

class SystemDoctor:
    async def _ping_url(self, url: str, is_rpc: bool = False, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        start = time.perf_counter()
        headers = {"User-Agent": "python:bot-mm:v1.0.0 (by /u/bot_mm_reach)"}
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
            try:
                if is_rpc or payload:
                    async with session.post(url, json=payload, timeout=15) as response:
                        status = "OK" if response.status in (200, 401, 403, 405) else f"Error: {response.status}"
                else:
                    async with session.get(url, timeout=15) as response:
                        status = "OK" if response.status in (200, 401, 403, 405) else f"Error: {response.status}"
            except Exception as e:
                status = f"Failed: {str(e)}"
        latency = (time.perf_counter() - start) * 1000
        return {"status": status, "latency_ms": round(latency, 2)}

    async def run_diagnostics(self) -> dict[str, Any]:
        tasks = {
            "Base EVM RPC": self._ping_url("https://mainnet.base.org", is_rpc=True, payload={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}),
            "Solana SVM RPC": self._ping_url("https://api.mainnet-beta.solana.com", is_rpc=True, payload={"jsonrpc": "2.0", "method": "getHealth", "params": [], "id": 1}),
            "RugCheck API": self._ping_url("https://api.rugcheck.xyz/v1/tokens/So11111111111111111111111111111111111111112/report/summary"),
            "GoPlus API": self._ping_url("https://api.gopluslabs.io/api/v1/supported_chains"),
            "Jina Reader": self._ping_url("https://r.jina.ai/https://example.com"),
            "Reddit Search": self._ping_url("https://www.reddit.com/r/CryptoCurrency/search.json?q=test&limit=1"),
        }
        
        results = await asyncio.gather(*tasks.values())
        return dict(zip(tasks.keys(), results))

    def format_cli_summary(self, report: dict[str, Any]) -> str:
        summary = "### System Doctor Diagnostics ###\n"
        summary += "| Service | Status | Latency (ms) |\n"
        summary += "|---|---|---|\n"
        for name, data in report.items():
            summary += f"| {name} | {data['status']} | {data['latency_ms']} |\n"
        return summary
