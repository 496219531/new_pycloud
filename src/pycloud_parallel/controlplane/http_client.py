from __future__ import annotations

"""Shared HTTP/JSON helpers for control-plane clients."""

import json
import logging
import socket
from http.client import RemoteDisconnected
from typing import Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .client_transport import _normalize_http_response_body
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible

logger = logging.getLogger(__name__)


def target_to_base_url(target: str) -> str:
    text = str(target or "").strip()
    if not text:
        raise ValueError("target is required")
    parsed = urlparse(text)
    if parsed.scheme in ("http", "https"):
        return text.rstrip("/")
    return f"http://{text}"


def _friendly_http_connect_error(*, url: str, exc: BaseException) -> RuntimeError:
    parsed = urlparse(url)
    endpoint = parsed.netloc or parsed.path or url
    reason = getattr(exc, "reason", exc)
    if isinstance(reason, ConnectionRefusedError):
        return RuntimeError(
            f"cannot connect to {endpoint}; check that the target is correct and the service is listening on that address"
        )
    if isinstance(reason, socket.timeout):
        return RuntimeError(
            f"request to {endpoint} timed out; check that the target is reachable and the service is responding"
        )
    if isinstance(exc, RemoteDisconnected):
        return RuntimeError(
            f"connection to {endpoint} was closed by the remote service; check that the target points to a healthy compatible server"
        )
    if isinstance(exc, URLError):
        return RuntimeError(f"http request to {endpoint} failed: {reason}")
    return RuntimeError(f"http request to {endpoint} failed: {exc}")


def http_json_request(
    *,
    base_url: str,
    path: str,
    method: str,
    timeout_sec: float,
    payload: Optional[Dict[str, object]] = None,
    headers: Optional[Dict[str, str]] = None,
    raise_on_error_response: bool = True,
) -> Dict[str, object]:
    raw = None
    request_headers = dict(headers or {})
    if payload is not None:
        payload = serialize_arrow_compatible(payload)
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request_headers["Content-Type"] = "application/json; charset=utf-8"

    url = f"{base_url.rstrip('/')}{path}"
    logger.debug(
        "http request method=%s url=%s payload=%s headers=%s",
        method.upper(),
        url,
        payload if payload is not None else None,
        request_headers,
    )

    req = Request(
        url,
        method=method.upper(),
        headers=request_headers,
        data=raw,
    )
    try:
        with urlopen(req, timeout=max(0.1, float(timeout_sec))) as resp:
            data = _normalize_http_response_body(json.loads(resp.read().decode("utf-8") or "{}"))
    except HTTPError as exc:
        try:
            body = _normalize_http_response_body(json.loads((exc.read() or b"{}").decode("utf-8") or "{}"))
        except Exception:
            body = {"ok": False, "error": exc.reason}
        raise RuntimeError(str(body.get("error", exc.reason))) from exc
    except URLError as exc:
        raise _friendly_http_connect_error(url=url, exc=exc) from exc
    except RemoteDisconnected as exc:
        raise _friendly_http_connect_error(url=url, exc=exc) from exc
    if data.get("ok", False) is False and raise_on_error_response:
        raise RuntimeError(str(data.get("error", "request failed")))
    return data


__all__ = ["http_json_request", "target_to_base_url"]
