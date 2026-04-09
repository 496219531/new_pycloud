from __future__ import annotations

"""Networking helpers for choosing externally reachable local addresses."""

import socket
from typing import Iterable, Tuple


_WILDCARD_HOSTS = {"", "0.0.0.0", "::", "[::]"}


def strip_host_brackets(host: str) -> str:
    text = str(host or "").strip()
    if text.startswith("[") and text.endswith("]"):
        return text[1:-1].strip()
    return text


def split_host_port(bind: str) -> Tuple[str, int]:
    text = str(bind or "").strip()
    if ":" not in text:
        raise ValueError("bind must be host:port")
    host, port = text.rsplit(":", 1)
    return strip_host_brackets(host), int(port)


def format_host_port(host: str, port: int) -> str:
    text = strip_host_brackets(host)
    if ":" in text and not (text.startswith("[") and text.endswith("]")):
        text = f"[{text}]"
    return f"{text}:{int(port)}"


def _non_loopback_ipv4(host: str) -> str:
    text = strip_host_brackets(host)
    if not text or text.startswith("127."):
        return ""
    if ":" in text:
        return ""
    return text


def _detect_via_udp_targets(targets: Iterable[Tuple[str, int]]) -> str:
    for host, port in targets:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((host, int(port)))
                candidate = _non_loopback_ipv4(sock.getsockname()[0])
                if candidate:
                    return candidate
        except Exception:
            continue
    return ""


def _detect_via_hostname() -> str:
    candidates = []
    with_hostnames = [socket.gethostname(), socket.getfqdn()]
    for name in with_hostnames:
        if not name:
            continue
        candidates.append(name)
        short = name.split(".", 1)[0].strip()
        if short and short != name:
            candidates.append(short)
    for name in candidates:
        try:
            infos = socket.getaddrinfo(name, None, family=socket.AF_INET, type=socket.SOCK_DGRAM)
        except Exception:
            continue
        for info in infos:
            host = _non_loopback_ipv4(info[4][0] if len(info) >= 5 else "")
            if host:
                return host
    return ""


def detect_local_ip(*, remote_hint: str = "") -> str:
    targets: list[Tuple[str, int]] = []
    hint = str(remote_hint or "").strip()
    if hint:
        try:
            targets.append(split_host_port(hint))
        except Exception:
            pass
    # UDP connect here is used only for route selection; no packets are sent unless write() is called.
    targets.extend(
        [
            ("10.0.0.1", 80),
            ("192.168.0.1", 80),
            ("172.16.0.1", 80),
            ("8.8.8.8", 80),
            ("1.1.1.1", 80),
            ("114.114.114.114", 80),
        ]
    )
    detected = _detect_via_udp_targets(targets)
    if detected:
        return detected
    detected = _detect_via_hostname()
    if detected:
        return detected
    return "127.0.0.1"


def resolve_public_host(host: str, *, remote_hint: str = "") -> str:
    text = strip_host_brackets(host)
    if text in _WILDCARD_HOSTS:
        return detect_local_ip(remote_hint=remote_hint)
    return text
