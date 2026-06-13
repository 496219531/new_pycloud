from __future__ import annotations

"""Shared dependency-runtime failover helpers for Service and TaskPool owners."""

import re
from typing import Any

from pycloud_parallel.execution.error_classifier import is_dependency_runtime_error


_NO_MODULE_RE = re.compile(r"no module named ['\"]([^'\"]+)['\"]", re.IGNORECASE)
_MISSING_DEP_RE = re.compile(r"missing dependency [`'\"]([^`'\"]+)[`'\"]", re.IGNORECASE)
_NODE_MISSING_RE = re.compile(r"node environment is missing [`'\"]([^`'\"]+)[`'\"]", re.IGNORECASE)


def is_dependency_failure(error_or_message: Any) -> bool:
    return is_dependency_runtime_error(error_or_message)


def dependency_missing_module(error_or_message: Any) -> str:
    text = _dependency_error_text(error_or_message)
    for pattern in (_NO_MODULE_RE, _MISSING_DEP_RE, _NODE_MISSING_RE):
        match = pattern.search(text)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def dependency_failure_reason(error_or_message: Any, *, method: str = "") -> str:
    text = _dependency_error_text(error_or_message)
    missing_module = dependency_missing_module(text)
    parts = ["dependency runtime error"]
    if str(method or "").strip():
        parts.append(f"method={str(method).strip()}")
    if missing_module:
        parts.append(f"missing_module={missing_module}")
    if text:
        parts.append(text)
    return ": ".join([parts[0], " ".join(parts[1:])]) if len(parts) > 1 else parts[0]


def _dependency_error_text(error_or_message: Any) -> str:
    if isinstance(error_or_message, BaseException):
        pieces = [error_or_message.__class__.__name__, str(error_or_message), repr(error_or_message)]
    else:
        pieces = [str(error_or_message or "")]
    return " ".join(piece for piece in pieces if piece).strip()


def dependency_method_blocked(
    method_failures: Any,
    *,
    method: str,
) -> bool:
    normalized_method = str(method or "").strip()
    if not normalized_method or not isinstance(method_failures, dict):
        return False
    return normalized_method in method_failures


def dependency_failure_detail(
    method_failures: Any,
    *,
    method: str,
) -> str:
    normalized_method = str(method or "").strip()
    if not normalized_method or not isinstance(method_failures, dict):
        return ""
    raw = method_failures.get(normalized_method)
    if isinstance(raw, dict):
        return str(raw.get("reason") or raw.get("error") or "").strip()
    return str(raw or "").strip()


__all__ = [
    "dependency_failure_reason",
    "dependency_failure_detail",
    "dependency_method_blocked",
    "dependency_missing_module",
    "is_dependency_failure",
]
