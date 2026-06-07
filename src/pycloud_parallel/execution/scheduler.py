from __future__ import annotations

"""Shared candidate filtering and scoring helpers for execution scheduling."""

from dataclasses import dataclass, field
import logging
from typing import Dict, Sequence


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchedulerCandidate:
    id: str
    kind: str
    node_id: str
    node_instance_id: str
    healthy: bool
    schedulable: bool
    drain: bool
    breaker_state: str
    predicted_busy: float
    node_inflight: int
    alive_workers: int
    worker_capacity: int
    credit: int
    recent_failures: int = 0
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class SchedulerState:
    local_inflight_by_candidate: dict[str, int] = field(default_factory=dict)
    disabled_candidates: set[str] = field(default_factory=set)
    recent_submit_failures: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyProfile:
    name: str
    weights: dict[str, float]
    tie_break: str = "round_robin"
    failure_penalty: float = 10.0


SERVICE_DEFAULT = StrategyProfile(
    name="service_default",
    weights={
        "predicted_busy": 5.0,
        "node_inflight": 2.0,
        "alive_workers": 1.5,
        "worker_capacity": 0.5,
        "recent_failures": 1.5,
    },
)

SERVICE_LATENCY_FIRST = StrategyProfile(
    name="service_latency_first",
    weights={
        "predicted_busy": 6.0,
        "node_inflight": 2.5,
        "alive_workers": 1.0,
        "recent_failures": 1.5,
    },
)

TASKPOOL_DEFAULT = StrategyProfile(
    name="taskpool_default",
    weights={
        "local_inflight": 6.0,
        "predicted_busy": 3.0,
        "node_inflight": 1.5,
        "alive_workers": 1.0,
        "worker_capacity": 0.75,
        "recent_failures": 2.0,
    },
)

TASKPOOL_THROUGHPUT = StrategyProfile(
    name="taskpool_throughput",
    weights={
        "local_inflight": 5.0,
        "alive_workers": 2.0,
        "worker_capacity": 1.5,
        "predicted_busy": 2.0,
        "recent_failures": 2.0,
    },
)

JOBQUEUE_DEFAULT = StrategyProfile(
    name="jobqueue_default",
    weights={
        "predicted_busy": 4.0,
        "node_inflight": 2.0,
        "alive_workers": 1.5,
        "worker_capacity": 1.0,
        "credit": 1.0,
        "recent_failures": 1.5,
    },
)

_HIGHER_IS_BETTER = {"alive_workers", "worker_capacity", "credit"}


def resolve_service_strategy(strategy: str) -> tuple[str, StrategyProfile | None]:
    normalized = str(strategy or "").strip().lower() or "predicted_busy"
    if normalized in {"predicted_busy", "service_default"}:
        return "predicted_busy", SERVICE_DEFAULT
    if normalized == "service_latency_first":
        return "service_latency_first", SERVICE_LATENCY_FIRST
    if normalized in {"least_inflight", "round_robin"}:
        return normalized, None
    logger.warning(
        "unsupported service strategy=%r; using fallback='predicted_busy'",
        normalized,
    )
    return "predicted_busy", SERVICE_DEFAULT


def resolve_taskpool_strategy(strategy: str) -> StrategyProfile:
    normalized = str(strategy or "").strip().lower() or "taskpool_default"
    if normalized in {"taskpool_default", "least_inflight", "predicted_busy"}:
        return TASKPOOL_DEFAULT
    if normalized in {"taskpool_throughput", "throughput"}:
        return TASKPOOL_THROUGHPUT
    if normalized == "round_robin":
        return StrategyProfile(
            name="taskpool_round_robin",
            weights=dict(TASKPOOL_DEFAULT.weights),
            tie_break="round_robin",
            failure_penalty=TASKPOOL_DEFAULT.failure_penalty,
        )
    logger.warning(
        "unsupported taskpool strategy=%r; using fallback='taskpool_default'",
        normalized,
    )
    return TASKPOOL_DEFAULT


def filter_candidates(
    candidates: Sequence[SchedulerCandidate],
    state: SchedulerState,
) -> list[SchedulerCandidate]:
    out: list[SchedulerCandidate] = []
    for candidate in candidates:
        if candidate.id in state.disabled_candidates:
            continue
        if not candidate.healthy:
            continue
        if not candidate.schedulable:
            continue
        if candidate.drain:
            continue
        if str(candidate.breaker_state or "").strip().lower() == "open":
            continue
        if candidate.worker_capacity <= 0 and candidate.alive_workers <= 0:
            continue
        out.append(candidate)
    return out


def _raw_feature_value(candidate: SchedulerCandidate, *, feature: str, state: SchedulerState) -> float:
    if feature == "local_inflight":
        return float(state.local_inflight_by_candidate.get(candidate.id, 0) or 0)
    if feature == "node_inflight":
        return float(candidate.node_inflight or 0)
    if feature == "predicted_busy":
        return float(candidate.predicted_busy or 0.0)
    if feature == "alive_workers":
        return float(candidate.alive_workers or 0)
    if feature == "worker_capacity":
        return float(candidate.worker_capacity or 0)
    if feature == "credit":
        return float(candidate.credit or 0)
    if feature == "recent_failures":
        value = state.recent_submit_failures.get(candidate.id)
        return float(candidate.recent_failures if value is None else value)
    return 0.0


def _normalize_feature_values(
    candidates: Sequence[SchedulerCandidate],
    *,
    feature: str,
    state: SchedulerState,
) -> Dict[str, float]:
    raw_values = {candidate.id: _raw_feature_value(candidate, feature=feature, state=state) for candidate in candidates}
    if not raw_values:
        return {}
    minimum = min(raw_values.values())
    maximum = max(raw_values.values())
    if maximum <= minimum:
        return {candidate_id: 0.0 for candidate_id in raw_values}
    normalized: Dict[str, float] = {}
    for candidate_id, value in raw_values.items():
        ratio = (value - minimum) / (maximum - minimum)
        normalized[candidate_id] = 1.0 - ratio if feature in _HIGHER_IS_BETTER else ratio
    return normalized


def _normalize_features(
    candidates: Sequence[SchedulerCandidate],
    *,
    features: Sequence[str],
    state: SchedulerState,
) -> Dict[str, Dict[str, float]]:
    return {
        str(feature): _normalize_feature_values(candidates, feature=str(feature), state=state)
        for feature in features
    }


def score_candidate(
    candidate: SchedulerCandidate,
    *,
    profile: StrategyProfile,
    state: SchedulerState,
    candidates: Sequence[SchedulerCandidate],
    normalized_features: Dict[str, Dict[str, float]] | None = None,
) -> float:
    score = 0.0
    feature_maps = normalized_features or _normalize_features(
        candidates,
        features=tuple(profile.weights.keys()),
        state=state,
    )
    for feature, weight in profile.weights.items():
        normalized = feature_maps.get(str(feature), {})
        score += float(weight) * float(normalized.get(candidate.id, 0.0))
    failures = _raw_feature_value(candidate, feature="recent_failures", state=state)
    score += float(profile.failure_penalty) * max(0.0, failures)
    return score


def select_one_candidate(
    candidates: Sequence[SchedulerCandidate],
    *,
    profile: StrategyProfile,
    state: SchedulerState,
    round_robin_counter: int = 0,
) -> SchedulerCandidate:
    filtered = filter_candidates(candidates, state)
    if not filtered:
        raise RuntimeError("no available scheduler candidates")
    normalized_features = _normalize_features(
        filtered,
        features=tuple(profile.weights.keys()),
        state=state,
    )
    scored = [
        (
            score_candidate(
                candidate,
                profile=profile,
                state=state,
                candidates=filtered,
                normalized_features=normalized_features,
            ),
            candidate,
        )
        for candidate in filtered
    ]
    best_score = min(score for score, _candidate in scored)
    best = [candidate for score, candidate in scored if abs(score - best_score) <= 1e-9]
    if profile.tie_break == "round_robin" and best:
        ordered = sorted(best, key=lambda item: (str(item.node_instance_id or item.id), str(item.id)))
        return ordered[int(round_robin_counter or 0) % len(ordered)]
    return sorted(best, key=lambda item: (str(item.node_instance_id or item.id), str(item.id)))[0]


__all__ = [
    "JOBQUEUE_DEFAULT",
    "SERVICE_DEFAULT",
    "SERVICE_LATENCY_FIRST",
    "TASKPOOL_DEFAULT",
    "TASKPOOL_THROUGHPUT",
    "SchedulerCandidate",
    "SchedulerState",
    "StrategyProfile",
    "filter_candidates",
    "resolve_service_strategy",
    "resolve_taskpool_strategy",
    "score_candidate",
    "select_one_candidate",
]
