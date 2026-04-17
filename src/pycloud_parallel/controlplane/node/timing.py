from __future__ import annotations

"""Shared timing/metrics helpers for service and task-pool execution flows."""

from dataclasses import dataclass
from typing import Dict, Mapping, Optional


@dataclass(frozen=True)
class ExecutionTimingSample:
    method: str
    ok: bool
    setup_ms: float
    build_execute_spec_ms: float
    executor_ms: float
    finalize_ms: float
    total_ms: float
    subprocess_timings: Optional[Dict[str, object]] = None
    error_type: str = ""
    error_message: str = ""
    http_status: int = 0


def _normalize_timing_sample(
    sample: ExecutionTimingSample,
) -> tuple[float, float, float, float, float, float, float, int, float]:
    setup_ms = float(sample.setup_ms or 0.0)
    build_execute_spec_ms = float(sample.build_execute_spec_ms or 0.0)
    executor_ms = float(sample.executor_ms or 0.0)
    finalize_ms = float(sample.finalize_ms or 0.0)
    total_ms = float(sample.total_ms or 0.0)
    decode_ms = float((sample.subprocess_timings or {}).get("decode_ms", 0.0) or 0.0)
    invoke_ms = float((sample.subprocess_timings or {}).get("invoke_ms", 0.0) or 0.0)
    encode_ms = float((sample.subprocess_timings or {}).get("encode_ms", 0.0) or 0.0)
    sample_count = 0
    queue_wait_ms = max(0.0, executor_ms - decode_ms - invoke_ms - encode_ms)
    return (
        setup_ms,
        build_execute_spec_ms,
        executor_ms,
        finalize_ms,
        total_ms,
        decode_ms,
        invoke_ms,
        int(sample.http_status or 0),
        queue_wait_ms,
    )


def update_execution_timing_metrics(
    metrics: Mapping[str, object] | None,
    *,
    sample: ExecutionTimingSample,
    include_http_status: bool,
    include_queue_wait: bool,
) -> Dict[str, object]:
    updated = dict(metrics or {})
    call_count = int(updated.get("call_count", 0) or 0) + 1
    error_count = int(updated.get("error_count", 0) or 0) + (0 if sample.ok else 1)
    updated["call_count"] = call_count
    updated["error_count"] = error_count
    updated["last_method"] = str(sample.method or "")
    updated["last_ok"] = bool(sample.ok)
    if include_http_status:
        updated["last_http_status"] = int(sample.http_status or 0)

    (
        setup_ms,
        build_execute_spec_ms,
        executor_ms,
        finalize_ms,
        total_ms,
        decode_ms,
        invoke_ms,
        _http_status,
        queue_wait_ms,
    ) = _normalize_timing_sample(sample)

    updated["last_total_ms"] = round(total_ms, 3)
    updated["last_setup_ms"] = round(setup_ms, 3)
    updated["last_build_execute_spec_ms"] = round(build_execute_spec_ms, 3)
    updated["last_executor_ms"] = round(executor_ms, 3)
    updated["last_finalize_ms"] = round(finalize_ms, 3)
    updated["max_total_ms"] = round(max(float(updated.get("max_total_ms", 0.0) or 0.0), total_ms), 3)

    def _avg(key: str, value: float) -> float:
        return round(((float(updated.get(key, 0.0) or 0.0) * (call_count - 1)) + value) / call_count, 3)

    updated["avg_setup_ms"] = _avg("avg_setup_ms", setup_ms)
    updated["avg_build_execute_spec_ms"] = _avg("avg_build_execute_spec_ms", build_execute_spec_ms)
    updated["avg_executor_ms"] = _avg("avg_executor_ms", executor_ms)
    updated["avg_finalize_ms"] = _avg("avg_finalize_ms", finalize_ms)
    updated["avg_total_ms"] = _avg("avg_total_ms", total_ms)

    if sample.subprocess_timings:
        encode_ms = float(sample.subprocess_timings.get("encode_ms", 0.0) or 0.0)
        alpha = 0.2
        prev_ema = float(updated.get("ema_child_invoke_ms", 0.0) or 0.0)
        ema_samples = int(updated.get("ema_samples", 0) or 0) + 1
        ema = invoke_ms if ema_samples <= 1 else ((alpha * invoke_ms) + ((1.0 - alpha) * prev_ema))
        updated["last_child_decode_ms"] = round(decode_ms, 3)
        updated["last_invoke_ms"] = round(invoke_ms, 3)
        updated["last_child_invoke_ms"] = round(invoke_ms, 3)
        updated["last_child_encode_ms"] = round(encode_ms, 3)
        updated["ema_child_invoke_ms"] = round(ema, 3)
        updated["ema_samples"] = ema_samples
        updated["avg_child_decode_ms"] = _avg("avg_child_decode_ms", decode_ms)
        updated["avg_invoke_ms"] = _avg("avg_invoke_ms", invoke_ms)
        updated["avg_child_invoke_ms"] = updated["avg_invoke_ms"]
        updated["avg_child_encode_ms"] = _avg("avg_child_encode_ms", encode_ms)
        if include_queue_wait:
            updated["last_queue_wait_ms"] = round(queue_wait_ms, 3)
            updated["avg_queue_wait_ms"] = _avg("avg_queue_wait_ms", queue_wait_ms)
    else:
        updated.setdefault("last_child_decode_ms", 0.0)
        updated.setdefault("last_invoke_ms", 0.0)
        updated.setdefault("last_child_invoke_ms", updated.get("last_invoke_ms", 0.0))
        updated.setdefault("last_child_encode_ms", 0.0)
        updated.setdefault("ema_child_invoke_ms", 0.0)
        updated.setdefault("ema_samples", 0)
        updated.setdefault("avg_child_decode_ms", 0.0)
        updated.setdefault("avg_invoke_ms", 0.0)
        updated.setdefault("avg_child_invoke_ms", updated.get("avg_invoke_ms", 0.0))
        updated.setdefault("avg_child_encode_ms", 0.0)
        if include_queue_wait:
            updated.setdefault("last_queue_wait_ms", 0.0)
            updated.setdefault("avg_queue_wait_ms", 0.0)

    updated["last_error_type"] = str(sample.error_type or "")
    updated["last_error_message"] = str(sample.error_message or "")
    return updated


def build_execution_timing_event(
    *,
    event: str,
    id_key: str,
    id_value: str,
    name_key: str,
    name_value: str,
    sample: ExecutionTimingSample,
    include_http_status: bool,
    include_queue_wait: bool,
) -> Dict[str, object]:
    (
        setup_ms,
        build_execute_spec_ms,
        executor_ms,
        finalize_ms,
        total_ms,
        _decode_ms,
        _invoke_ms,
        http_status,
        queue_wait_ms,
    ) = _normalize_timing_sample(sample)

    payload: Dict[str, object] = {
        "event": str(event or ""),
        id_key: str(id_value or ""),
        name_key: str(name_value or ""),
        "method": str(sample.method or ""),
        "ok": bool(sample.ok),
        "setup_ms": round(setup_ms, 3),
        "build_execute_spec_ms": round(build_execute_spec_ms, 3),
        "executor_ms": round(executor_ms, 3),
        "finalize_ms": round(finalize_ms, 3),
        "total_ms": round(total_ms, 3),
        "error_type": str(sample.error_type or ""),
        "error_message": str(sample.error_message or ""),
    }
    if include_http_status:
        payload["http_status"] = int(http_status)
    if sample.subprocess_timings:
        payload["subprocess"] = dict(sample.subprocess_timings)
        if include_queue_wait:
            payload["queue_wait_ms"] = round(queue_wait_ms, 3)
    return payload


__all__ = [
    "ExecutionTimingSample",
    "build_execution_timing_event",
    "update_execution_timing_metrics",
]
