from __future__ import annotations

from pycloud_parallel.execution.scheduler import (
    JOBQUEUE_DEFAULT,
    SERVICE_DEFAULT,
    TASKPOOL_DEFAULT,
    TASKPOOL_THROUGHPUT,
    SchedulerCandidate,
    SchedulerState,
    filter_candidates,
    resolve_taskpool_strategy,
    score_candidate,
    select_one_candidate,
)


def _candidate(
    candidate_id: str,
    *,
    predicted_busy: float = 0.0,
    node_inflight: int = 0,
    alive_workers: int = 1,
    worker_capacity: int = 1,
    credit: int = 1,
    healthy: bool = True,
    schedulable: bool = True,
    drain: bool = False,
    breaker_state: str = "closed",
    recent_failures: int = 0,
) -> SchedulerCandidate:
    return SchedulerCandidate(
        id=candidate_id,
        kind="service",
        node_id=candidate_id,
        node_instance_id=candidate_id,
        healthy=healthy,
        schedulable=schedulable,
        drain=drain,
        breaker_state=breaker_state,
        predicted_busy=predicted_busy,
        node_inflight=node_inflight,
        alive_workers=alive_workers,
        worker_capacity=worker_capacity,
        credit=credit,
        recent_failures=recent_failures,
    )


def test_filter_candidates_excludes_unhealthy_drain_breaker_open_and_disabled():
    candidates = [
        _candidate("ok"),
        _candidate("unhealthy", healthy=False),
        _candidate("drain", drain=True),
        _candidate("open", breaker_state="open"),
        _candidate("disabled"),
    ]
    state = SchedulerState(disabled_candidates={"disabled"})

    filtered = filter_candidates(candidates, state)

    assert [candidate.id for candidate in filtered] == ["ok"]


def test_service_default_prefers_lower_predicted_busy():
    candidates = [
        _candidate("busy", predicted_busy=5.0, node_inflight=1, alive_workers=2),
        _candidate("less-busy", predicted_busy=1.0, node_inflight=3, alive_workers=2),
    ]
    state = SchedulerState()

    selected = select_one_candidate(candidates, profile=SERVICE_DEFAULT, state=state)

    assert selected.id == "less-busy"


def test_taskpool_default_prefers_lower_local_inflight():
    candidates = [
        _candidate("node-a", predicted_busy=1.0, node_inflight=1, alive_workers=2, worker_capacity=2),
        _candidate("node-b", predicted_busy=1.0, node_inflight=1, alive_workers=2, worker_capacity=2),
    ]
    state = SchedulerState(local_inflight_by_candidate={"node-a": 3, "node-b": 0})

    selected = select_one_candidate(candidates, profile=TASKPOOL_DEFAULT, state=state)

    assert selected.id == "node-b"


def test_jobqueue_default_considers_credit_and_capacity():
    candidates = [
        _candidate("low-credit", predicted_busy=0.5, node_inflight=1, alive_workers=2, worker_capacity=2, credit=1),
        _candidate("high-credit", predicted_busy=0.5, node_inflight=1, alive_workers=2, worker_capacity=4, credit=8),
    ]
    state = SchedulerState()

    selected = select_one_candidate(candidates, profile=JOBQUEUE_DEFAULT, state=state)

    assert selected.id == "high-credit"


def test_select_one_candidate_uses_round_robin_for_ties():
    candidates = [
        _candidate("node-a", predicted_busy=1.0, node_inflight=1, alive_workers=2),
        _candidate("node-b", predicted_busy=1.0, node_inflight=1, alive_workers=2),
    ]
    state = SchedulerState()

    first = select_one_candidate(candidates, profile=SERVICE_DEFAULT, state=state, round_robin_counter=0)
    second = select_one_candidate(candidates, profile=SERVICE_DEFAULT, state=state, round_robin_counter=1)

    assert first.id == "node-a"
    assert second.id == "node-b"


def test_score_candidate_penalizes_recent_failures():
    candidates = [
        _candidate("stable", predicted_busy=1.0, node_inflight=1, alive_workers=2, recent_failures=0),
        _candidate("flaky", predicted_busy=1.0, node_inflight=1, alive_workers=2, recent_failures=2),
    ]
    state = SchedulerState(recent_submit_failures={"flaky": 2})

    stable_score = score_candidate(candidates[0], profile=SERVICE_DEFAULT, state=state, candidates=candidates)
    flaky_score = score_candidate(candidates[1], profile=SERVICE_DEFAULT, state=state, candidates=candidates)

    assert stable_score < flaky_score


def test_resolve_taskpool_strategy_accepts_throughput_profile():
    profile = resolve_taskpool_strategy("taskpool_throughput")
    assert profile == TASKPOOL_THROUGHPUT

    default_profile = resolve_taskpool_strategy("taskpool_default")
    assert default_profile == TASKPOOL_DEFAULT
