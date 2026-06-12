from __future__ import annotations

from urllib.error import URLError

import pytest

from pycloud_parallel.execution.error_classifier import (
    ErrorCategory,
    classify_error,
    is_retryable_compensation_failure,
    is_terminal_heartbeat_error,
)
from pycloud_parallel.execution.support import (
    _is_node_identity_mismatch_error,
    _is_transient_infocenter_error,
)


@pytest.mark.parametrize(
    ("message", "resource_kind", "expected"),
    [
        ("service is stopped", "service", ErrorCategory.SERVICE_TERMINAL),
        ("service not found: demo", "service", ErrorCategory.SERVICE_TERMINAL),
        ("task pool not running", "task_pool", ErrorCategory.TASK_POOL_TERMINAL),
        ("pool not found", "taskpool", ErrorCategory.TASK_POOL_TERMINAL),
        ("node instance execution is fenced; NodeControl host should exit", "", ErrorCategory.OLD_INSTANCE_IDENTITY_LOST),
        ("node_instance_id fenced", "node", ErrorCategory.OLD_INSTANCE_IDENTITY_LOST),
        ("node control_addr instance mismatch", "", ErrorCategory.IDENTITY_MISMATCH),
        (
            "node control_addr is still served by another node instance",
            "",
            ErrorCategory.IDENTITY_MISMATCH,
        ),
        ("http request to 127.0.0.1:9 failed: connection refused", "", ErrorCategory.TRANSIENT_NETWORK),
        ("probe timed out", "", ErrorCategory.TRANSIENT_NETWORK),
        (
            "artifact validation failed while loading: method `run` not exported",
            "",
            ErrorCategory.PERMANENT_ARTIFACT,
        ),
        ("temporary server unavailable", "", ErrorCategory.TRANSIENT_NETWORK),
        (
            "ConnectionResetError(10054, '远程主机强迫关闭了一个现有的连接。', None, 10054, None)",
            "",
            ErrorCategory.TRANSIENT_NETWORK,
        ),
        ("python runtime mismatch: requested_runtime=py3.12", "", ErrorCategory.PERMANENT_ARTIFACT),
        ("ModuleNotFoundError: No module named 'missing_pkg'", "", ErrorCategory.IMPORT_ERROR),
        ("ImportError: cannot import name 'run'", "", ErrorCategory.IMPORT_ERROR),
    ],
)
def test_classify_error_categories(message, resource_kind, expected):
    assert classify_error(message, resource_kind=resource_kind) == expected


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("heartbeat pending queued_wait_sec=0.000 rpc_running_sec=3.000"),
        ConnectionResetError(10054, "connection reset"),
        URLError(ConnectionRefusedError("temporarily unavailable")),
    ],
)
def test_classify_error_handles_exception_instances(error):
    assert classify_error(error) == ErrorCategory.TRANSIENT_NETWORK
    assert is_retryable_compensation_failure(error) is True


def test_support_error_helpers_use_central_classifier():
    assert _is_transient_infocenter_error(URLError(ConnectionRefusedError("connection refused"))) is True
    assert _is_node_identity_mismatch_error("expected_node_instance_id mismatch") is True
    assert _is_node_identity_mismatch_error("ordinary user ValueError") is False


def test_resource_kind_disambiguates_short_terminal_messages():
    assert classify_error("not found", resource_kind="service") == ErrorCategory.SERVICE_TERMINAL
    assert classify_error("not running", resource_kind="taskpool") == ErrorCategory.TASK_POOL_TERMINAL


def test_permanent_artifact_markers_win_over_resource_kind():
    assert classify_error("object not found on node: sha256:abc", resource_kind="service") == ErrorCategory.PERMANENT_ARTIFACT


def test_classify_error_walks_exception_cause_chain():
    cause = ModuleNotFoundError("No module named 'missing_demo_dep'")
    error = RuntimeError("artifact validation failed while loading")
    error.__cause__ = cause

    assert classify_error(error) == ErrorCategory.IMPORT_ERROR
    assert is_retryable_compensation_failure(error) is False


@pytest.mark.parametrize(
    ("message", "resource_kind"),
    [
        ("node instance execution is fenced", ""),
        ("node control_addr instance mismatch", ""),
        ("service not found", "service"),
        ("task pool not running", "task_pool"),
    ],
)
def test_terminal_heartbeat_error_markers(message, resource_kind):
    assert is_terminal_heartbeat_error(message, resource_kind=resource_kind) is True


@pytest.mark.parametrize(
    "message",
    [
        "heartbeat unavailable",
        "connection reset by peer",
        "service is stopped",
        "task pool not found",
        "node_instance_id fenced",
    ],
)
def test_retryable_compensation_failure_positive_markers(message):
    assert is_retryable_compensation_failure(message) is True


@pytest.mark.parametrize(
    "message",
    [
        "",
        "user code failed with ValueError",
        "ModuleNotFoundError: missing_pkg",
        "ImportError: cannot import name 'demo'",
        "SyntaxError at demo.py:1: invalid syntax",
        "dependency install failed for ['missing_pkg']",
        "artifact missing",
        "object not found on node: sha256:abc",
    ],
)
def test_retryable_compensation_failure_rejects_permanent_and_unknown(message):
    assert is_retryable_compensation_failure(message) is False
