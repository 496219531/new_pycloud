from __future__ import annotations

import pytest

from pycloud_parallel.controlplane.node.execution import _validate_python_runtime_or_raise
from pycloud_parallel.data.ref import coerce_data_ref
from pycloud_parallel.runtime.errors import DataRefPayloadError, RuntimeMismatchError, normalize_invoke_error


def test_runtime_mismatch_uses_shared_runtime_error_type():
    with pytest.raises(RuntimeMismatchError) as exc_info:
        _validate_python_runtime_or_raise(node_python_version="py3.10", runtime=">=py3.11")

    message = str(exc_info.value)
    assert "requested_runtime=>=py3.11" in message
    assert "current_node(py3.10)" in message
    assert "Fix:" in message


def test_dataref_payload_errors_use_shared_error_type():
    with pytest.raises(DataRefPayloadError) as exc_info:
        coerce_data_ref({"legacy": True})

    message = str(exc_info.value)
    assert "dataref payload error:" in message
    assert "DataRef payloads only" in message


def test_invoke_error_normalization_is_shared():
    ok, error_type, error_message = normalize_invoke_error(
        "FAILED_USER",
        error_type="",
        error_message="",
        user_fallback="user function failed",
        infra_fallback="infra failure",
    )
    assert ok is False
    assert error_type == "UserError"
    assert error_message == "user function failed"

    ok, error_type, error_message = normalize_invoke_error(
        "FAILED_INFRA",
        error_type="",
        error_message="",
        user_fallback="user function failed",
        infra_fallback="infra failure",
    )
    assert ok is False
    assert error_type == "InfraError"
    assert error_message == "infra failure"

    ok, error_type, error_message = normalize_invoke_error("SUCCEEDED")
    assert ok is True
    assert error_type == ""
    assert error_message == ""
