from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from pycloud_parallel.controlplane.node.execution import ExecuteSpec, _build_execute_spec, _build_execute_spec_model
from pycloud_parallel.controlplane.node.timing import (
    ExecutionTimingSample,
    build_execution_timing_event,
    update_execution_timing_metrics,
)


def test_build_execute_spec_model_round_trips_to_payload():
    artifact = SimpleNamespace(
        path="/tmp/demo.py",
        entry_module="demo_mod",
        package_format="py",
        dependency_path="/tmp/deps",
        dependency_policy_mode="prebuilt",
        export_mode="single",
        export_methods=("run",),
        export_decorator="pycloud_export",
        entry_callable="run",
    )

    model = _build_execute_spec_model(
        artifact,
        object_dir=Path("/tmp/objects"),
        work_dir=Path("/tmp/work"),
        method_name="run",
        payload={"value": 7},
        payload_mode="task_submit",
        managed_globals_scope_dir="/tmp/globals",
        managed_globals_digest="sha256:digest",
        warmup_only=True,
    )

    assert isinstance(model, ExecuteSpec)
    assert model.artifact_path == "/tmp/demo.py"
    assert model.export_methods == ("run",)
    assert model.payload == {"value": 7}
    assert model.warmup_only is True

    payload = model.to_payload()
    assert payload == _build_execute_spec(
        artifact,
        object_dir=Path("/tmp/objects"),
        work_dir=Path("/tmp/work"),
        method_name="run",
        payload={"value": 7},
        payload_mode="task_submit",
        managed_globals_scope_dir="/tmp/globals",
        managed_globals_digest="sha256:digest",
        warmup_only=True,
    )


def test_shared_execution_timing_helpers_produce_consistent_metrics_and_event():
    sample = ExecutionTimingSample(
        method="run",
        ok=True,
        http_status=200,
        setup_ms=1.0,
        build_execute_spec_ms=2.0,
        executor_ms=9.0,
        finalize_ms=1.5,
        total_ms=12.0,
        subprocess_timings={
            "decode_ms": 1.0,
            "invoke_ms": 5.0,
            "invoke_wrapper_ms": 0.5,
            "user_fn_ms": 4.5,
            "encode_ms": 1.0,
        },
    )

    metrics = update_execution_timing_metrics(
        {},
        sample=sample,
        include_http_status=True,
        include_queue_wait=True,
    )
    assert metrics["call_count"] == 1
    assert metrics["error_count"] == 0
    assert metrics["last_http_status"] == 200
    assert metrics["last_method"] == "run"
    assert metrics["last_queue_wait_ms"] == 2.0
    assert metrics["avg_invoke_ms"] == 5.0
    assert metrics["avg_invoke_wrapper_ms"] == 0.5
    assert metrics["avg_user_fn_ms"] == 4.5

    event = build_execution_timing_event(
        event="service_timing",
        id_key="service_id",
        id_value="svc-1",
        name_key="service_name",
        name_value="svc-demo",
        sample=sample,
        include_http_status=True,
        include_queue_wait=True,
    )
    assert event["event"] == "service_timing"
    assert event["service_id"] == "svc-1"
    assert event["service_name"] == "svc-demo"
    assert event["http_status"] == 200
    assert event["queue_wait_ms"] == 2.0
