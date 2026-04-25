from __future__ import annotations

"""Shared failure classification and candidate breaker helpers."""

from dataclasses import dataclass
import time
from typing import Optional, Tuple


ROUTE_UNAVAILABLE = "ROUTE_UNAVAILABLE"
STAGING_FAILED = "STAGING_FAILED"
STATUS_LOOKUP_FAILED = "STATUS_LOOKUP_FAILED"
CONTROLPLANE_UNAVAILABLE = "CONTROLPLANE_UNAVAILABLE"
SUBMIT_FAILED = "SUBMIT_FAILED"
REMOTE_INFRA_FAILED = "REMOTE_INFRA_FAILED"
REMOTE_USER_FAILED = "REMOTE_USER_FAILED"
RESULT_FETCH_FAILED = "RESULT_FETCH_FAILED"


@dataclass
class CandidateBreakerState:
    state: str = "closed"  # closed | open | half_open
    consecutive_failures: int = 0
    disabled_until_monotonic: float = 0.0
    open_count: int = 0
    probe_in_flight: bool = False
    last_error: str = ""
    last_failure_kind: str = ""


def classify_service_error(exc: Exception, *, route_failure: bool = False) -> str:
    message = str(exc or "").strip()
    data = getattr(exc, "data", None)
    if isinstance(data, dict):
        error_type = str(data.get("error_type", "") or "")
        error = str(data.get("error", "") or "")
        message = " ".join(part for part in (message, error_type, error) if part)
    lowered = message.lower()
    if route_failure:
        return ROUTE_UNAVAILABLE
    if "usererror" in lowered or "failed_user" in lowered or "user error" in lowered:
        return REMOTE_USER_FAILED
    if "infraerror" in lowered or "failed_infra" in lowered or "infra failure" in lowered:
        return REMOTE_INFRA_FAILED
    return CONTROLPLANE_UNAVAILABLE


def classify_task_result_status(status_name: str) -> str:
    normalized = str(status_name or "").strip().upper()
    if normalized == "FAILED_INFRA":
        return REMOTE_INFRA_FAILED
    if normalized == "FAILED_USER":
        return REMOTE_USER_FAILED
    return ""


def should_failover(
    failure_kind: str,
    *,
    has_alternative_candidate: bool,
) -> bool:
    if not has_alternative_candidate:
        return False
    normalized = str(failure_kind or "").strip().upper()
    return normalized in {
        ROUTE_UNAVAILABLE,
        STAGING_FAILED,
        STATUS_LOOKUP_FAILED,
        CONTROLPLANE_UNAVAILABLE,
        SUBMIT_FAILED,
        REMOTE_INFRA_FAILED,
    }


def should_degrade(
    failure_kind: str,
    *,
    has_cached_candidate: bool = False,
    requires_route_aware_staging: bool = False,
) -> bool:
    normalized = str(failure_kind or "").strip().upper()
    if normalized == STATUS_LOOKUP_FAILED:
        if requires_route_aware_staging:
            return bool(has_cached_candidate)
        return True
    return False


def mark_candidate_success(state: CandidateBreakerState) -> None:
    state.state = "closed"
    state.consecutive_failures = 0
    state.disabled_until_monotonic = 0.0
    state.open_count = 0
    state.probe_in_flight = False
    state.last_error = ""
    state.last_failure_kind = ""


def _cooldown(open_count: int, *, cooldown_sec: float, max_cooldown_sec: float) -> float:
    exp = max(0, int(open_count or 0) - 1)
    value = max(0.1, float(cooldown_sec)) * (2.0**exp)
    return min(max(0.1, float(max_cooldown_sec)), value)


def mark_candidate_failure(
    state: CandidateBreakerState,
    *,
    failure_kind: str,
    error: object,
    failure_threshold: int,
    cooldown_sec: float,
    max_cooldown_sec: float,
) -> None:
    now = time.monotonic()
    state.last_failure_kind = str(failure_kind or "")
    state.last_error = repr(error)
    if state.state == "half_open":
        state.consecutive_failures = max(int(state.consecutive_failures or 0), int(failure_threshold or 1))
    elif state.state == "closed":
        state.consecutive_failures = int(state.consecutive_failures or 0) + 1
    state.probe_in_flight = False
    if state.consecutive_failures < max(1, int(failure_threshold or 1)):
        return
    state.state = "open"
    state.open_count = int(state.open_count or 0) + 1
    state.disabled_until_monotonic = now + _cooldown(
        state.open_count,
        cooldown_sec=max(0.1, float(cooldown_sec or 0.1)),
        max_cooldown_sec=max(0.1, float(max_cooldown_sec or cooldown_sec or 0.1)),
    )


def candidate_allowed(state: CandidateBreakerState, *, now: Optional[float] = None) -> Tuple[str, bool]:
    current = time.monotonic() if now is None else float(now)
    if state.state == "open":
        if current >= float(state.disabled_until_monotonic or 0.0):
            state.state = "half_open"
            state.probe_in_flight = False
        else:
            return state.state, False
    if state.state == "half_open" and state.probe_in_flight:
        return state.state, False
    return state.state, True


def before_probe(state: CandidateBreakerState, *, now: Optional[float] = None) -> bool:
    _state, allowed = candidate_allowed(state, now=now)
    if not allowed:
        return False
    if state.state == "half_open":
        if state.probe_in_flight:
            return False
        state.probe_in_flight = True
    return True


__all__ = [
    "CONTROLPLANE_UNAVAILABLE",
    "CandidateBreakerState",
    "REMOTE_INFRA_FAILED",
    "REMOTE_USER_FAILED",
    "RESULT_FETCH_FAILED",
    "ROUTE_UNAVAILABLE",
    "STAGING_FAILED",
    "STATUS_LOOKUP_FAILED",
    "SUBMIT_FAILED",
    "before_probe",
    "candidate_allowed",
    "classify_service_error",
    "classify_task_result_status",
    "mark_candidate_failure",
    "mark_candidate_success",
    "should_degrade",
    "should_failover",
]
