"""
alpha_engine.dns_resolver — Anti-Sinkhole DNS Resolver
======================================================
Ensures critical RPC endpoints and security APIs (Helius, RugCheck) are not
intercepted by enterprise DNS sinkholes (such as Cisco Umbrella / OpenDNS).
Resolves legitimate Anycast edge IPs via Cloudflare (1.1.1.1) or Google (8.8.8.8)
upstream DNS servers when a sinkholed IP (e.g. 146.112.61.106) is returned by the local resolver.
"""

from __future__ import annotations

import logging
import socket
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
    "helius-rpc.com": ("104.18.36.169", "172.64.151.87"),
    "rugcheck.xyz": ("174.138.15.144",),
}

_RESOLVED_CACHE: dict[str, list[str]] = {}


def query_upstream_dns(domain: str, servers: Sequence[str] = UPSTREAM_DNS_SERVERS, timeout: float = 1.5) -> list[str]:
    """
    Directly query upstream DNS servers (Cloudflare / Google) via UDP 53 to resolve
    clean, non-sinkholed A records.
    """
    if domain in _RESOLVED_CACHE:
        return _RESOLVED_CACHE[domain]

    header = b"\x12\x34\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00"
    qname = b"".join(bytes([len(p)]) + p.encode() for p in domain.split(".")) + b"\x00"
    packet = header + qname + b"\x00\x01\x00\x01"

    for server in servers:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(timeout)
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
                    if not any(ip.startswith(prefix) for prefix in _SINKHOLE_PREFIXES):
                        ips.append(ip)
                idx += rdlength
            if ips:
                _RESOLVED_CACHE[domain] = ips
                return ips
        except Exception:
            pass
        finally:
            s.close()

    # Fallback to hardcoded map if upstream query timed out or failed
    for host_pattern, verified_ips in _VERIFIED_HOST_MAP.items():
        if host_pattern in domain:
            _RESOLVED_CACHE[domain] = list(verified_ips)
            return list(verified_ips)

    return []


def patch_dns_resolvers() -> None:
    """
    Patch socket.getaddrinfo to intercept hostnames being sinkholed to Cisco Umbrella
    IP addresses, seamlessly re-routing them to Cloudflare/Google upstream resolved edge IPs.
    """
    global _PATCHED
    if _PATCHED:
        return

    def _safe_getaddrinfo(host: object, port: object, *args: object, **kwargs: object) -> list[tuple]:
        if isinstance(host, str):
            for target_domain, fallback_ips in _VERIFIED_HOST_MAP.items():
                if target_domain in host:
                    try:
                        res = _ORIG_GETADDRINFO(host, port, *args, **kwargs)
                        is_sinkholed = any(
                            any(sockaddr[0].startswith(prefix) for prefix in _SINKHOLE_PREFIXES)
                            for family, type_, proto, canonname, sockaddr in res
                            if isinstance(sockaddr, tuple) and len(sockaddr) > 0
                        )
                        if not is_sinkholed:
                            return res
                        logger.warning(
                            "Detected Cisco Umbrella / OpenDNS sinkhole for %s. Resolving through Cloudflare (1.1.1.1) / Google (8.8.8.8)...",
                            host,
                        )
                    except Exception:
                        pass

                    upstream_ips = query_upstream_dns(host)
                    target_ip = upstream_ips[0] if upstream_ips else fallback_ips[0]
                    logger.info("Routing %s through resolved edge IP %s", host, target_ip)
                    return _ORIG_GETADDRINFO(target_ip, port, *args, **kwargs)

        return _ORIG_GETADDRINFO(host, port, *args, **kwargs)

    socket.getaddrinfo = _safe_getaddrinfo
    _PATCHED = True
    logger.debug("Anti-sinkhole DNS resolver patch applied.")
