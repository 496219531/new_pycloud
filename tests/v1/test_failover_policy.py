from __future__ import annotations

from pycloud_parallel.execution.failover import (
    REMOTE_INFRA_FAILED,
    REMOTE_USER_FAILED,
    ROUTE_UNAVAILABLE,
    STAGING_FAILED,
    STATUS_LOOKUP_FAILED,
    should_degrade,
    should_failover,
)


def test_should_failover_allows_infra_and_route_failures_when_alternative_exists():
    assert should_failover(ROUTE_UNAVAILABLE, has_alternative_candidate=True) is True
    assert should_failover(STAGING_FAILED, has_alternative_candidate=True) is True
    assert should_failover(REMOTE_INFRA_FAILED, has_alternative_candidate=True) is True


def test_should_failover_rejects_user_failures():
    assert should_failover(REMOTE_USER_FAILED, has_alternative_candidate=True) is False


def test_should_failover_rejects_any_failure_without_alternative():
    assert should_failover(ROUTE_UNAVAILABLE, has_alternative_candidate=False) is False
    assert should_failover(REMOTE_INFRA_FAILED, has_alternative_candidate=False) is False


def test_should_degrade_allows_status_lookup_failure_only_with_cache_or_small_payload():
    assert should_degrade(
        STATUS_LOOKUP_FAILED,
        has_cached_candidate=True,
        requires_route_aware_staging=True,
    ) is True
    assert should_degrade(
        STATUS_LOOKUP_FAILED,
        has_cached_candidate=False,
        requires_route_aware_staging=False,
    ) is True
    assert should_degrade(
        STATUS_LOOKUP_FAILED,
        has_cached_candidate=False,
        requires_route_aware_staging=True,
    ) is False
