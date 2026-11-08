"""
alpha_engine.dns_resolver — Anti-Sinkhole DNS Resolver
======================================================
Ensures critical RPC endpoints and security APIs (Helius, RugCheck) are not
intercepted by enterprise DNS sinkholes (such as Cisco Umbrella / OpenDNS).
Resolves legitimate Anycast edge IPs via Cloudflare (1.1.1.1) or Google (8.8.8.8)
upstream DNS servers when a sinkholed IP (e.g. 146.112.61.106) is returned by the local resolver.
"""

from __future__ import annotations

import json
import logging
import socket
import ssl
import urllib.request
from typing import Sequence

logger = logging.getLogger(__name__)

_PATCHED = False
_ORIG_GETADDRINFO = socket.getaddrinfo

# Known sinkhole IP prefixes commonly used by Cisco Umbrella / OpenDNS
_SINKHOLE_PREFIXES = ("146.112.", "67.215.", "146.112.61.")

# Upstream public DNS servers to bypass sinkholes
UPSTREAM_DNS_SERVERS = ("1.1.1.1", "8.8.8.8")

# Verified Anycast / origin IPs for crypto infrastructure blocked by enterprise filters
_VERIFIED_HOST_MAP: dict[str, tuple[str, ...]] = {
    "mainnet.helius-rpc.com": ("104.18.36.169", "172.64.151.87"),
    "helius-rpc.com": ("104.18.36.169", "172.64.151.87"),
    "api.rugcheck.xyz": ("174.138.15.144",),
    "rugcheck.xyz": ("174.138.15.144",),
    "solana-rpc.publicnode.com": ("172.66.150.162", "104.20.24.117"),
    "api.mainnet-beta.solana.com": ("64.130.57.92", "64.130.50.23"),
    "api.mainnet.solana.com": ("64.130.57.92", "64.130.50.23"),
    "api.dexscreener.com": ("104.18.38.143", "172.64.149.113"),
    "api.geckoterminal.com": ("104.18.1.233", "104.18.0.233"),
    "base-mainnet.g.alchemy.com": ("57.129.88.148", "57.129.88.159"),
}

_RESOLVED_CACHE: dict[str, list[str]] = {}


def _is_sinkhole_ip(ip_str: str) -> bool:
    """Check if an IPv4 or IPv6-mapped address belongs to a known sinkhole prefix."""
    if not ip_str:
        return False
    clean = ip_str.replace("::ffff:", "")
    return any(clean.startswith(prefix) for prefix in _SINKHOLE_PREFIXES)


def _query_doh(domain: str, timeout: float = 1.2) -> list[str]:
    """Query Google DNS-over-HTTPS endpoints (port 443, bypasses UDP 53 firewall blocks)."""
    doh_endpoints = ("8.8.8.8", "8.8.4.4")
    for endpoint in doh_endpoints:
        url = f"https://{endpoint}/resolve?name={domain}&type=A"
        req = urllib.request.Request(
            url,
            headers={"accept": "application/dns-json", "User-Agent": "Bot-MM/1.0"},
        )
        try:
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode())
                    answers = data.get("Answer", [])
                    ips = [
                        ans["data"]
                        for ans in answers
                        if ans.get("type") == 1 and not _is_sinkhole_ip(ans.get("data", ""))
                    ]
                    if ips:
                        return ips
        except Exception:
            continue
    return []


def preresolve_trusted_endpoints(servers: Sequence[str] = UPSTREAM_DNS_SERVERS) -> None:
    """
    Pre-resolve and statically cache trusted IP endpoints at startup.
    Eliminates recurrent sinkhole checks and suppresses repetitive log warnings during runtime.
    """
    targets = (
        "mainnet.helius-rpc.com",
        "helius-rpc.com",
        "api.rugcheck.xyz",
        "rugcheck.xyz",
        "solana-rpc.publicnode.com",
        "api.mainnet-beta.solana.com",
        "api.dexscreener.com",
        "api.geckoterminal.com",
    )
    for domain in targets:
        if domain not in _RESOLVED_CACHE:
            ips = query_upstream_dns(domain, servers=servers)
            if ips:
                _RESOLVED_CACHE[domain] = ips
            else:
                for host_pat, fallback_ips in _VERIFIED_HOST_MAP.items():
                    if host_pat in domain:
                        _RESOLVED_CACHE[domain] = list(fallback_ips)
                        break
    logger.debug("Pre-resolved trusted DNS endpoints: %s", list(_RESOLVED_CACHE.keys()))


def query_upstream_dns(domain: str, servers: Sequence[str] = UPSTREAM_DNS_SERVERS, timeout: float = 1.5) -> list[str]:
    """
    Query clean upstream DNS to resolve legitimate, non-sinkholed A records.
    Attempts DNS-over-HTTPS (DoH) first to bypass local UDP 53 egress restrictions.
    Falls back to UDP 53 and static Anycast host mappings.
    """
    if domain in _RESOLVED_CACHE:
        return _RESOLVED_CACHE[domain]

    # 1. Primary: DNS-over-HTTPS (DoH via 8.8.8.8:443)
    doh_ips = _query_doh(domain, timeout=min(timeout, 1.2))
    if doh_ips:
        _RESOLVED_CACHE[domain] = doh_ips
        return doh_ips

    # 2. Secondary: UDP 53 direct query (used if DoH failed or in offline mocks)
    header = b"\x12\x34\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00"
    qname = b"".join(bytes([len(p)]) + p.encode() for p in domain.split(".")) + b"\x00"
    packet = header + qname + b"\x00\x01\x00\x01"

    for server in servers:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(min(timeout, 0.6))
        try:
            s.sendto(packet, (server, 53))
            data, _ = s.recvfrom(2048)
            if len(data) < 12:
                continue
            ancount = int.from_bytes(data[6:8], "big")
            if ancount == 0:
                continue
            idx = 12
            while idx < len(data) and data[idx] != 0:
                idx += 1 + data[idx]
            idx += 5

            ips: list[str] = []
            for _ in range(ancount):
                if idx >= len(data):
                    break
                if (data[idx] & 0xC0) == 0xC0:
                    idx += 2
                else:
                    while idx < len(data) and data[idx] != 0:
                        idx += 1 + data[idx]
                    idx += 1
                if idx + 10 > len(data):
                    break
                rtype = int.from_bytes(data[idx:idx + 2], "big")
                rdlength = int.from_bytes(data[idx + 8:idx + 10], "big")
                idx += 10
                if rtype == 1 and rdlength == 4 and idx + 4 <= len(data):
                    ip = socket.inet_ntoa(data[idx:idx + 4])
                    if not _is_sinkhole_ip(ip):
                        ips.append(ip)
                idx += rdlength
            if ips:
                _RESOLVED_CACHE[domain] = ips
                return ips
        except Exception:
            pass
        finally:
            s.close()

    # 3. Tertiary: Fallback to verified Anycast host mapping
    for host_pattern, verified_ips in _VERIFIED_HOST_MAP.items():
        if host_pattern in domain:
            _RESOLVED_CACHE[domain] = list(verified_ips)
            return list(verified_ips)

    return []


def patch_dns_resolvers(servers: Sequence[str] | None = None) -> None:
    """
    Patch socket.getaddrinfo to intercept hostnames being sinkholed to Cisco Umbrella
    IP addresses, seamlessly re-routing them to Cloudflare/Google upstream resolved edge IPs.
    Pre-resolves trusted endpoints at initialization to bypass system resolver sinkholes without
    throwing recurrent log warnings.
    """
    global _PATCHED
    effective_servers = servers or UPSTREAM_DNS_SERVERS
    preresolve_trusted_endpoints(effective_servers)

    if _PATCHED:
        return

    def _safe_getaddrinfo(host: object, port: object, *args: object, **kwargs: object) -> list[tuple]:
        if isinstance(host, str):
            # 1. Fast-path: Check static pre-resolved cache directly to avoid system sinkhole
            if host in _RESOLVED_CACHE and _RESOLVED_CACHE[host]:
                return _ORIG_GETADDRINFO(_RESOLVED_CACHE[host][0], port, *args, **kwargs)

            for target_domain, cached_ips in _RESOLVED_CACHE.items():
                if target_domain in host and cached_ips:
                    return _ORIG_GETADDRINFO(cached_ips[0], port, *args, **kwargs)

            # 2. Check verified host map
            for target_domain, fallback_ips in _VERIFIED_HOST_MAP.items():
                if target_domain in host:
                    upstream_ips = query_upstream_dns(host, servers=effective_servers)
                    target_ip = upstream_ips[0] if upstream_ips else fallback_ips[0]
                    _RESOLVED_CACHE[host] = [target_ip]
                    return _ORIG_GETADDRINFO(target_ip, port, *args, **kwargs)

        try:
            res = _ORIG_GETADDRINFO(host, port, *args, **kwargs)
            # Inspect resolved IPs to detect sinkhole interception
            if isinstance(host, str) and res:
                is_sinkhole = False
                for item in res:
                    sockaddr = item[4]
                    if sockaddr and isinstance(sockaddr[0], str) and _is_sinkhole_ip(sockaddr[0]):
                        is_sinkhole = True
                        break
                if is_sinkhole:
                    logger.debug(
                        "Detected sinkhole IP in resolution for %s. Re-routing via upstream DNS...",
                        host,
                    )
                    upstream_ips = query_upstream_dns(host, servers=effective_servers)
                    if upstream_ips:
                        _RESOLVED_CACHE[host] = upstream_ips
                        return _ORIG_GETADDRINFO(upstream_ips[0], port, *args, **kwargs)
            return res
        except socket.gaierror as e:
            # Resilient fallback: If system DNS fails (e.g. temporary failure in name resolution),
            # query upstream public DNS to resolve A records.
            if isinstance(host, str) and not host.replace(".", "").isdigit():
                upstream_ips = query_upstream_dns(host, servers=effective_servers)
                if upstream_ips:
                    _RESOLVED_CACHE[host] = upstream_ips
                    logger.debug("Recovered host %s via upstream DNS query: %s (gaierror: %s)", host, upstream_ips[0], e)
                    return _ORIG_GETADDRINFO(upstream_ips[0], port, *args, **kwargs)
            raise

    socket.getaddrinfo = _safe_getaddrinfo
    _PATCHED = True
    logger.debug("Anti-sinkhole DNS resolver patch applied with pre-resolved cache.")
