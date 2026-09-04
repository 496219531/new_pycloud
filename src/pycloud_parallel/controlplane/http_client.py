from __future__ import annotations

"""Shared HTTP/JSON helpers for control-plane clients."""

import json
import http.client
import logging
import socket
from http.client import RemoteDisconnected
from typing import Dict, Optional
from urllib.error import URLError
from urllib.parse import urlparse

from .client_transport import _normalize_http_response_body
from pycloud_parallel.controlplane.http_connection_pool import pooled_http_request
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

    try:
        response = pooled_http_request(
            url=url,
            method=method,
            headers=request_headers,
            body=raw,
            timeout_sec=timeout_sec,
        )
        try:
            data = _normalize_http_response_body(json.loads(response.body.decode("utf-8") or "{}"))
        except json.JSONDecodeError as exc:
            if response.status >= 400:
                raise RuntimeError(response.reason or f"HTTP {response.status}") from exc
            raise RuntimeError(f"invalid JSON response from {urlparse(url).netloc or url}: {exc}") from exc
        if response.status >= 400:
            raise RuntimeError(str(data.get("error", response.reason)))
    except RuntimeError:
        raise
    except (OSError, http.client.HTTPException) as exc:
        raise _friendly_http_connect_error(url=url, exc=exc) from exc
    except URLError as exc:
        # Kept for compatible error normalization if a custom pool adapter uses urllib.
        raise _friendly_http_connect_error(url=url, exc=exc) from exc
    if data.get("ok", False) is False and raise_on_error_response:
        raise RuntimeError(str(data.get("error", "request failed")))
    return data


__all__ = ["http_json_request", "target_to_base_url"]
