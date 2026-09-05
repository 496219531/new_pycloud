from __future__ import annotations

"""Unified runtime compatibility errors for the V1 migration path."""

from dataclasses import dataclass
from enum import Enum
from typing import Sequence


class PycloudModelError(ValueError):
    """Base class for stable user-facing model/contract errors."""


class RuntimeMismatchError(PycloudModelError):
    """Raised when requested python runtime cannot be satisfied."""


class DataRefPayloadError(PycloudModelError):
    """Raised when a payload is not a canonical DataRef payload."""


class ExecutionErrorCode(str, Enum):
    USER_CODE_ERROR = "UserCodeError"
    DEPENDENCY_ERROR = "DependencyError"
    WORKER_CRASHED = "WorkerCrashed"
    EXECUTOR_UNAVAILABLE = "ExecutorUnavailable"
    CALL_TIMEOUT = "CallTimeout"
    QUEUE_FULL = "QueueFull"
    SERIALIZATION_ERROR = "SerializationError"


def execution_error_code(status_text: str = "", *, error_type: str = "", error_message: str = "") -> ExecutionErrorCode:
    status = str(status_text or "").strip().upper()
    text = f"{error_type} {error_message}".strip().lower()
    if status == "FAILED_DEPENDENCY" or "dependency" in text or "no module named" in text:
        return ExecutionErrorCode.DEPENDENCY_ERROR
    if "timeout" in text or "timed out" in text:
        return ExecutionErrorCode.CALL_TIMEOUT
    if "queue" in text and ("full" in text or "capacity" in text):
        return ExecutionErrorCode.QUEUE_FULL
    if "serializ" in text or "pickle" in text or "pickl" in text or "codec" in text:
        return ExecutionErrorCode.SERIALIZATION_ERROR
    if "brokenprocesspool" in text or "worker" in text and ("died" in text or "crash" in text):
        return ExecutionErrorCode.WORKER_CRASHED
    if status == "FAILED_INFRA" or "executor" in text or "unavailable" in text:
        return ExecutionErrorCode.EXECUTOR_UNAVAILABLE
    return ExecutionErrorCode.USER_CODE_ERROR


@dataclass(frozen=True)
class RuntimeMismatchCandidate:
    label: str
    python_version: str


def format_runtime_mismatch_message(
    *,
    requested_runtime: str,
    candidates: Sequence[RuntimeMismatchCandidate],
    scope: str = "nodes",
) -> str:
    normalized_runtime = str(requested_runtime or "").strip() or "(unspecified)"
    serialized_candidates = []
    for item in candidates:
        label = str(item.label or "").strip() or "unknown"
        python_version = str(item.python_version or "").strip() or "unknown"
        serialized_candidates.append(f"{label}({python_version})")
    discovered = ", ".join(serialized_candidates) if serialized_candidates else "(none discovered)"
    fix = (
        f"Fix: upgrade target nodes to satisfy {normalized_runtime}, "
        "relax the requested runtime, or choose nodes whose python_version already matches."
    )
    return (
        f"python runtime mismatch: requested_runtime={normalized_runtime}; "
        f"discovered_{str(scope or 'nodes').strip().replace(' ', '_')}={discovered}; "
        f"{fix}"
    )


def format_dataref_payload_error(*, detail: str, fix: str = "Use canonical DataRef payloads only.") -> str:
    normalized_detail = str(detail or "").strip() or "invalid DataRef payload"
    normalized_fix = str(fix or "").strip()
    if normalized_fix:
        return f"dataref payload error: {normalized_detail} Fix: {normalized_fix}"
    return f"dataref payload error: {normalized_detail}"


def normalize_invoke_error(
    status_text: str,
    *,
    error_type: str = "",
    error_message: str = "",
    user_fallback: str = "user error",
    infra_fallback: str = "infra error",
) -> tuple[bool, str, str]:
    normalized_status = str(status_text or "").strip().upper()
    if normalized_status == "FAILED_USER":
        return False, str(error_type or "UserError"), str(error_message or user_fallback)
    if normalized_status == "FAILED_DEPENDENCY":
        return False, str(error_type or "DependencyError"), str(error_message or infra_fallback)
    if normalized_status == "FAILED_INFRA":
        return False, str(error_type or "InfraError"), str(error_message or infra_fallback)
    return True, "", ""
