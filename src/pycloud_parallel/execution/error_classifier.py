from __future__ import annotations

import errno
from enum import Enum
from typing import Any, Iterator, Tuple
from urllib.error import URLError


class ErrorCategory(str, Enum):
    UNKNOWN = "unknown"
    SERVICE_TERMINAL = "service_terminal"
    TASK_POOL_TERMINAL = "task_pool_terminal"
    NODE_FENCE = "node_fence"
    IDENTITY_MISMATCH = "identity_mismatch"
    HEARTBEAT = "heartbeat"
    TRANSIENT_NETWORK = "transient_network"
    PERMANENT_ARTIFACT = "permanent_artifact"
    IMPORT_ERROR = "import_error"


_NODE_FENCE_MARKERS = (
    "node instance execution is fenced",
    "node_instance_id fenced",
    "execution is fenced",
    "control_addr replaced",
    "node instance reset required",
    "registration lost",
    "http error 410",
    "http 410",
    "status 410",
    "410 gone",
)

_IDENTITY_MISMATCH_MARKERS = (
    "node control_addr instance mismatch",
    "node control_addr is still served by another node instance",
    "identity mismatch",
    "expected_node_instance_id mismatch",
    "http error 409",
    "http 409",
    "status 409",
    "409 conflict",
)

_SERVICE_TERMINAL_MARKERS = (
    "service is stopped",
    "service not found",
    "service not running",
    "service executor stopped",
)

_TASK_POOL_TERMINAL_MARKERS = (
    "task pool not running",
    "task pool not found",
    "pool is stopped",
    "pool not found",
)

_IMPORT_ERROR_MARKERS = (
    "modulenotfounderror",
    "module not found error",
    "importerror",
    "import error",
    "no module named ",
    "cannot import name ",
)

_PERMANENT_ARTIFACT_MARKERS = (
    "artifact validation failed",
    "dependency install failed",
    "runtime mismatch",
    "syntaxerror",
    "syntax error",
    "method `",
    "not exported",
    "no exported methods found",
    "duplicate exported method",
    "entry_module is required",
    "not callable",
    "cannot load python module",
    "artifact missing",
    "code artifact missing",
    "object not found on node",
)

_HEARTBEAT_MARKERS = (
    "heartbeat",
    "keepalive",
)

_TRANSIENT_NETWORK_MARKERS = (
    "connection refused",
    "connection reset",
    "connection aborted",
    "connection error",
    "cannot connect to ",
    "closed by the remote service",
    "remote end closed connection",
    "temporarily unavailable",
    "temporary server unavailable",
    "server unavailable",
    "timed out",
    "timeout",
    "host unreachable",
    "network unreachable",
    "broken pipe",
    "http request to ",
    "http error 503",
    "http 503",
    "status 503",
    "503 service unavailable",
    "service unavailable",
    "max retries exceeded",
    "winerror 10054",
    "winerror 10060",
    "winerror 10061",
    "errno 104",
    "errno 110",
    "errno 111",
)

_TRANSIENT_ERRNOS = {
    errno.ECONNREFUSED,
    errno.ECONNRESET,
    errno.ETIMEDOUT,
    errno.EHOSTUNREACH,
    errno.ENETUNREACH,
}


def classify_error(error_or_message: Any, *, resource_kind: str = "") -> ErrorCategory:
    text = _error_text(error_or_message)
    normalized_resource_kind = _normalize_resource_kind(resource_kind)
    if not text:
        return ErrorCategory.UNKNOWN

    if _has_marker(text, _NODE_FENCE_MARKERS):
        return ErrorCategory.NODE_FENCE
    if _has_marker(text, _IDENTITY_MISMATCH_MARKERS):
        return ErrorCategory.IDENTITY_MISMATCH
    if _has_marker(text, _SERVICE_TERMINAL_MARKERS):
        return ErrorCategory.SERVICE_TERMINAL
    if _has_marker(text, _TASK_POOL_TERMINAL_MARKERS):
        return ErrorCategory.TASK_POOL_TERMINAL
    if _has_marker(text, _IMPORT_ERROR_MARKERS):
        return ErrorCategory.IMPORT_ERROR
    if _has_marker(text, _PERMANENT_ARTIFACT_MARKERS):
        return ErrorCategory.PERMANENT_ARTIFACT
    if normalized_resource_kind == "service" and _has_marker(text, ("not found", "not running", "is stopped")):
        return ErrorCategory.SERVICE_TERMINAL
    if normalized_resource_kind == "task_pool" and _has_marker(text, ("not found", "not running", "is stopped")):
        return ErrorCategory.TASK_POOL_TERMINAL
    if _is_transient_network_error(error_or_message, text):
        return ErrorCategory.TRANSIENT_NETWORK
    if _has_marker(text, _HEARTBEAT_MARKERS):
        return ErrorCategory.HEARTBEAT
    return ErrorCategory.UNKNOWN


def is_terminal_heartbeat_error(error_or_message: Any, *, resource_kind: str = "") -> bool:
    category = classify_error(error_or_message, resource_kind=resource_kind)
    return category in {
        ErrorCategory.NODE_FENCE,
        ErrorCategory.IDENTITY_MISMATCH,
        ErrorCategory.SERVICE_TERMINAL,
        ErrorCategory.TASK_POOL_TERMINAL,
    }


def is_retryable_compensation_failure(error_or_message: Any, *, resource_kind: str = "") -> bool:
    category = classify_error(error_or_message, resource_kind=resource_kind)
    if category in {
        ErrorCategory.IMPORT_ERROR,
        ErrorCategory.PERMANENT_ARTIFACT,
        ErrorCategory.UNKNOWN,
    }:
        return False
    return category in {
        ErrorCategory.NODE_FENCE,
        ErrorCategory.IDENTITY_MISMATCH,
        ErrorCategory.SERVICE_TERMINAL,
        ErrorCategory.TASK_POOL_TERMINAL,
        ErrorCategory.HEARTBEAT,
        ErrorCategory.TRANSIENT_NETWORK,
    }


def _normalize_resource_kind(resource_kind: str) -> str:
    text = str(resource_kind or "").strip().lower().replace("-", "_")
    if text in {"taskpool", "task_pool", "pool"}:
        return "task_pool"
    if text in {"service", "svc"}:
        return "service"
    return text


def _has_marker(text: str, markers: Tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def _error_text(error_or_message: Any) -> str:
    parts = []
    for item in _iter_error_chain(error_or_message):
        if isinstance(item, str):
            parts.append(item)
            continue
        parts.append(item.__class__.__name__)
        message = str(item)
        if message:
            parts.append(message)
        representation = repr(item)
        if representation and representation != message:
            parts.append(representation)
    if not parts and error_or_message is not None:
        parts.append(str(error_or_message))
    return " ".join(part for part in parts if part).strip().lower()


def _iter_error_chain(error_or_message: Any) -> Iterator[Any]:
    if isinstance(error_or_message, BaseException):
        seen = set()
        current = error_or_message
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            yield current
            if isinstance(current, URLError) and getattr(current, "reason", None) is not None:
                yield getattr(current, "reason")
            current = current.__cause__ or current.__context__
        return
    yield str(error_or_message or "")


def _is_transient_network_error(error_or_message: Any, text: str) -> bool:
    if _has_marker(text, _TRANSIENT_NETWORK_MARKERS):
        return True
    for item in _iter_error_chain(error_or_message):
        candidate = item
        if isinstance(candidate, URLError):
            candidate = candidate.reason
        if isinstance(candidate, TimeoutError):
            return True
        if isinstance(candidate, ConnectionError):
            return True
        if isinstance(candidate, OSError) and getattr(candidate, "errno", None) in _TRANSIENT_ERRNOS:
            return True
    return False


__all__ = [
    "ErrorCategory",
    "classify_error",
    "is_retryable_compensation_failure",
    "is_terminal_heartbeat_error",
]
