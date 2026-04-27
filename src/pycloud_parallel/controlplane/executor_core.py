from __future__ import annotations

"""Shared ProcessPool execution core for node executor backends."""

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
import logging
import multiprocessing as mp
import queue
import threading
import time
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

EmitFunc = Callable[[Dict[str, Any]], None]


def submit_callable_to_worker(executor: ProcessPoolExecutor, args: Dict[str, Any]):
    from pycloud_parallel.controlplane.node.execution import _execute_payload_in_subprocess

    return executor.submit(
        _execute_payload_in_subprocess,
        args["artifact_path"],
        args["entry_module"],
        args["package_format"],
        args["dependency_path"],
        str(args.get("dependency_policy_mode", "prebuilt") or "prebuilt"),
        args["object_dir"],
        args.get("work_dir", ""),
        args.get("managed_globals_scope_dir", ""),
        args.get("managed_globals_digest", ""),
        args["export_mode"],
        args["export_methods"],
        args["export_decorator"],
        args["method_name"],
        args["entry_callable"],
        args["payload"],
        bool(args.get("warmup_only", False)),
        str(args.get("payload_mode", "task_submit") or "task_submit"),
        str(args.get("serialization_mode", "") or "").strip().lower(),
        args.get("use_transport_result", None),
    )


def unpack_subprocess_result(value: Any) -> tuple[str, Any, str, str, Dict[str, Any]]:
    if isinstance(value, tuple):
        if len(value) == 5:
            status_text, result, err_type, err_message, timings = value
            return (
                str(status_text or ""),
                result,
                str(err_type or ""),
                str(err_message or ""),
                dict(timings or {}),
            )
        if len(value) == 4:
            status_text, result, err_type, err_message = value
            return (
                str(status_text or ""),
                result,
                str(err_type or ""),
                str(err_message or ""),
                {},
            )
    raise RuntimeError(f"unexpected subprocess result shape: {type(value).__name__}")


def is_recoverable_pool_error(exc: BaseException) -> bool:
    if isinstance(exc, BrokenProcessPool):
        return True
    text = repr(exc)
    return "terminated abruptly while the future was running or pending" in text or "BrokenProcessPool" in text


class ExecutorCore:
    """Owns ProcessPoolExecutor instances and emits host-compatible events."""

    def __init__(
        self,
        *,
        task_worker_capacity: int = 1,
        emit_response: Optional[EmitFunc] = None,
        emit_event: Optional[EmitFunc] = None,
    ) -> None:
        self._task_worker_capacity = max(1, int(task_worker_capacity or 1))
        self._emit_response_func = emit_response
        self._emit_event_func = emit_event
        self._service_executors: Dict[str, ProcessPoolExecutor] = {}
        self._service_workers: Dict[str, int] = {}
        self._pool_executors: Dict[str, ProcessPoolExecutor] = {}
        self._pool_workers: Dict[str, int] = {}
        self._task_executor: Optional[ProcessPoolExecutor] = None
        self._inflight: Dict[object, Dict[str, Any]] = {}
        self._shutdown_q: "queue.Queue[object]" = queue.Queue()
        self._completed_futures: "queue.Queue[object]" = queue.Queue()
        self._shutdown_sentinel = object()
        self._shutdown_thread = threading.Thread(target=self._shutdown_worker, name="executor-core-shutdown", daemon=True)
        self._shutdown_thread.start()

    def close(self) -> None:
        for executor in list(self._service_executors.values()):
            self._shutdown_executor(executor, wait=True)
        for executor in list(self._pool_executors.values()):
            self._shutdown_executor(executor, wait=True)
        self._shutdown_executor(self._task_executor, wait=True)
        self._service_executors.clear()
        self._service_workers.clear()
        self._pool_executors.clear()
        self._pool_workers.clear()
        self._task_executor = None
        self._shutdown_q.put(self._shutdown_sentinel)
        self._shutdown_thread.join(timeout=5.0)

    @staticmethod
    def _ensure_mp_context():
        return mp.get_context("spawn")

    @staticmethod
    def _shutdown_executor(executor: Optional[ProcessPoolExecutor], *, wait: bool = False) -> None:
        if executor is None:
            return
        processes = list(getattr(executor, "_processes", {}).values())
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        for proc in processes:
            try:
                if proc.is_alive():
                    proc.terminate()
                proc.join(timeout=2.0 if wait else 0.2)
                if proc.is_alive() and hasattr(proc, "kill"):
                    proc.kill()
                    proc.join(timeout=1.0)
            except Exception:
                continue

    def _shutdown_executor_async(self, executor: Optional[ProcessPoolExecutor], *, wait: bool = False) -> None:
        if executor is not None:
            self._shutdown_q.put((executor, bool(wait)))

    def _shutdown_worker(self) -> None:
        while True:
            item = self._shutdown_q.get()
            if item is self._shutdown_sentinel:
                return
            executor, wait = item
            self._shutdown_executor(executor, wait=bool(wait))

    def _emit_response(self, request_id: str, **payload: object) -> None:
        if self._emit_response_func is not None:
            self._emit_response_func({"kind": "response", "request_id": request_id, **payload})

    def _emit_event(self, item: Dict[str, Any]) -> None:
        if self._emit_event_func is not None:
            self._emit_event_func(dict(item))

    def _enqueue_completed_future(self, future) -> None:
        try:
            self._completed_futures.put_nowait(future)
        except Exception:
            pass

    def _track_inflight(self, future, meta: Dict[str, Any]) -> None:
        self._inflight[future] = meta
        future.add_done_callback(self._enqueue_completed_future)

    def _emit_executor_workers(self, *, scope: str, key: str, executor: Optional[ProcessPoolExecutor]) -> None:
        if executor is None:
            return
        worker_pids = []
        for proc in list((getattr(executor, "_processes", {}) or {}).values()):
            pid = int(getattr(proc, "pid", 0) or 0)
            if pid > 0:
                worker_pids.append(pid)
        if worker_pids:
            self._emit_event(
                {
                    "kind": "executor_worker_pids",
                    "scope": str(scope or ""),
                    "key": str(key or ""),
                    "worker_pids": worker_pids,
                }
            )

    def _ensure_task_executor(self) -> ProcessPoolExecutor:
        if self._task_executor is None:
            self._task_executor = ProcessPoolExecutor(
                max_workers=self._task_worker_capacity,
                mp_context=self._ensure_mp_context(),
            )
        return self._task_executor

    def _rebuild_service_executor(self, service_id: str) -> Optional[ProcessPoolExecutor]:
        worker_count = max(1, int(self._service_workers.get(service_id, 1) or 1))
        existing = self._service_executors.pop(service_id, None)
        self._shutdown_executor_async(existing, wait=True)
        executor = ProcessPoolExecutor(max_workers=worker_count, mp_context=self._ensure_mp_context())
        self._service_executors[service_id] = executor
        return executor

    def _rebuild_pool_executor(self, pool_id: str) -> Optional[ProcessPoolExecutor]:
        worker_count = max(1, int(self._pool_workers.get(pool_id, 1) or 1))
        existing = self._pool_executors.pop(pool_id, None)
        self._shutdown_executor_async(existing, wait=True)
        executor = ProcessPoolExecutor(max_workers=worker_count, mp_context=self._ensure_mp_context())
        self._pool_executors[pool_id] = executor
        return executor

    def _submit_service_future(self, service_id: str, payload: Dict[str, Any]):
        executor = self._service_executors.get(service_id)
        if executor is None and service_id in self._service_workers:
            executor = self._rebuild_service_executor(service_id)
        if executor is None:
            raise RuntimeError("service executor missing")
        try:
            future = submit_callable_to_worker(executor, payload)
            self._emit_executor_workers(scope="service", key=service_id, executor=executor)
            return future
        except Exception as exc:
            if not is_recoverable_pool_error(exc):
                raise
            executor = self._rebuild_service_executor(service_id)
            if executor is None:
                raise RuntimeError("service executor missing after rebuild") from exc
            future = submit_callable_to_worker(executor, payload)
            self._emit_executor_workers(scope="service", key=service_id, executor=executor)
            return future

    def _submit_pool_future(self, pool_id: str, payload: Dict[str, Any]):
        executor = self._pool_executors.get(pool_id)
        if executor is None and pool_id in self._pool_workers:
            executor = self._rebuild_pool_executor(pool_id)
        if executor is None:
            raise RuntimeError("task pool missing")
        try:
            future = submit_callable_to_worker(executor, payload)
            self._emit_executor_workers(scope="pool", key=pool_id, executor=executor)
            return future
        except Exception as exc:
            if not is_recoverable_pool_error(exc):
                raise
            executor = self._rebuild_pool_executor(pool_id)
            if executor is None:
                raise RuntimeError("task pool missing after rebuild") from exc
            future = submit_callable_to_worker(executor, payload)
            self._emit_executor_workers(scope="pool", key=pool_id, executor=executor)
            return future

    def _submit_runtime_future(self, payload: Dict[str, Any]):
        executor = self._ensure_task_executor()
        try:
            future = submit_callable_to_worker(executor, payload)
            self._emit_executor_workers(scope="runtime", key=str(payload.get("runtime_key", "") or ""), executor=executor)
            return future
        except Exception as exc:
            if not is_recoverable_pool_error(exc):
                raise
            old_executor = self._task_executor
            self._task_executor = None
            self._shutdown_executor_async(old_executor, wait=True)
            executor = self._ensure_task_executor()
            future = submit_callable_to_worker(executor, payload)
            self._emit_executor_workers(scope="runtime", key=str(payload.get("runtime_key", "") or ""), executor=executor)
            return future

    def _submit_background_calls(
        self,
        executor: Optional[ProcessPoolExecutor],
        args: Dict[str, Any],
        *,
        fanout: int,
        kind: str,
        label: str,
    ) -> int:
        if executor is None:
            return 0
        submitted = max(1, int(fanout or 1))
        for _ in range(submitted):
            future = submit_callable_to_worker(executor, args)

            def _consume_background_result(done_future, *, call_kind: str = kind, call_label: str = label) -> None:
                try:
                    status_text, _result, err_type, err_message, _timings = unpack_subprocess_result(done_future.result())
                except Exception as exc:
                    logger.debug("%s future failed kind=%s err=%r", call_label, call_kind, exc)
                    return
                if status_text != "SUCCEEDED":
                    logger.debug(
                        "%s future returned non-success kind=%s status=%s err_type=%s err_message=%s",
                        call_label,
                        call_kind,
                        status_text,
                        err_type,
                        err_message,
                    )

            future.add_done_callback(_consume_background_result)
        self._emit_executor_workers(scope=kind, key=str(args.get(f"{kind}_id", args.get("runtime_key", "")) or ""), executor=executor)
        return submitted

    def handle_request(self, request_id: str, action: str, payload: Dict[str, Any]) -> bool:
        if action == "shutdown":
            self._emit_response(request_id, ok=True)
            return False
        payload = dict(payload or {})
        try:
            return self._handle_request_or_raise(request_id, action, payload)
        except Exception as exc:
            self._emit_response(request_id, ok=False, error=repr(exc))
            return True

    def _handle_request_or_raise(self, request_id: str, action: str, payload: Dict[str, Any]) -> bool:
        if action == "create_service":
            service_id = str(payload.get("service_id", "") or "")
            worker_count = max(1, int(payload.get("worker_count", 1) or 1))
            self._service_workers[service_id] = worker_count
            self._service_executors[service_id] = self._rebuild_service_executor(service_id)
            self._emit_response(request_id, ok=True)
            return True

        if action == "stop_service":
            service_id = str(payload.get("service_id", "") or "")
            executor = self._service_executors.pop(service_id, None)
            self._service_workers.pop(service_id, None)
            self._shutdown_executor(executor, wait=True)
            self._emit_response(request_id, ok=True)
            return True

        if action == "create_task_pool":
            pool_id = str(payload.get("pool_id", "") or "")
            worker_count = max(1, int(payload.get("worker_count", 1) or 1))
            self._pool_workers[pool_id] = worker_count
            self._pool_executors[pool_id] = self._rebuild_pool_executor(pool_id)
            self._emit_response(request_id, ok=True)
            return True

        if action == "stop_task_pool":
            pool_id = str(payload.get("pool_id", "") or "")
            executor = self._pool_executors.pop(pool_id, None)
            self._pool_workers.pop(pool_id, None)
            self._shutdown_executor(executor, wait=True)
            self._emit_response(request_id, ok=True)
            return True

        if action == "call_service":
            service_id = str(payload.get("service_id", "") or "")
            if service_id not in self._service_workers:
                self._emit_response(request_id, ok=False, error="service executor missing")
                return True
            future = self._submit_service_future(service_id, payload)
            self._track_inflight(
                future,
                {
                    "kind": "service",
                    "service_id": service_id,
                    "request_id": request_id,
                    "start_at": time.monotonic(),
                    "timeout_sec": max(0.1, float(payload.get("timeout_sec", 60.0) or 60.0)),
                    "payload": dict(payload),
                    "recoveries": 0,
                },
            )
            return True

        if action == "warmup_service":
            service_id = str(payload.get("service_id", "") or "")
            executor = self._service_executors.get(service_id)
            if executor is None and service_id in self._service_workers:
                executor = self._rebuild_service_executor(service_id)
            if executor is None:
                self._emit_response(request_id, ok=False, error="service executor missing")
                return True
            fanout = max(1, int(payload.get("fanout", 1) or 1))
            submitted = self._submit_background_calls(executor, payload, fanout=fanout, kind="service", label="warmup")
            self._emit_response(request_id, ok=True, submitted=submitted)
            return True

        if action == "preload_service":
            service_id = str(payload.get("service_id", "") or "")
            executor = self._service_executors.get(service_id)
            if executor is None and service_id in self._service_workers:
                executor = self._rebuild_service_executor(service_id)
            if executor is None:
                self._emit_response(request_id, ok=False, error="service executor missing")
                return True
            fanout = max(1, int(payload.get("fanout", 1) or 1))
            submitted = self._submit_background_calls(executor, payload, fanout=fanout, kind="service", label="preload")
            self._emit_response(request_id, ok=True, submitted=submitted)
            return True

        if action == "submit_runtime_task":
            future = self._submit_runtime_future(payload)
            self._track_inflight(
                future,
                {
                    "kind": "runtime",
                    "runtime_key": str(payload.get("runtime_key", "") or ""),
                    "task_id": str(payload.get("task_id", "") or ""),
                    "attempt": int(payload.get("attempt", 0) or 0),
                    "payload": dict(payload),
                    "recoveries": 0,
                },
            )
            self._emit_response(request_id, ok=True)
            return True

        if action == "warmup_runtime":
            executor = self._ensure_task_executor()
            fanout = max(1, int(payload.get("fanout", 1) or 1))
            submitted = self._submit_background_calls(executor, payload, fanout=fanout, kind="runtime", label="warmup")
            self._emit_response(request_id, ok=True, submitted=submitted)
            return True

        if action == "submit_pool_task":
            pool_id = str(payload.get("pool_id", "") or "")
            if pool_id not in self._pool_workers:
                self._emit_response(request_id, ok=False, error="task pool missing")
                return True
            future = self._submit_pool_future(pool_id, payload)
            self._track_inflight(
                future,
                {
                    "kind": "pool",
                    "pool_id": pool_id,
                    "task_id": str(payload.get("task_id", "") or ""),
                    "attempt": int(payload.get("attempt", 0) or 0),
                    "start_at": time.monotonic(),
                    "payload": dict(payload),
                    "recoveries": 0,
                },
            )
            self._emit_response(request_id, ok=True)
            return True

        if action == "warmup_pool":
            pool_id = str(payload.get("pool_id", "") or "")
            executor = self._pool_executors.get(pool_id)
            if executor is None and pool_id in self._pool_workers:
                executor = self._rebuild_pool_executor(pool_id)
            if executor is None:
                self._emit_response(request_id, ok=False, error="task pool missing")
                return True
            fanout = max(1, int(payload.get("fanout", 1) or 1))
            submitted = self._submit_background_calls(executor, payload, fanout=fanout, kind="pool", label="warmup")
            self._emit_response(request_id, ok=True, submitted=submitted)
            return True

        if action == "preload_pool":
            pool_id = str(payload.get("pool_id", "") or "")
            executor = self._pool_executors.get(pool_id)
            if executor is None and pool_id in self._pool_workers:
                executor = self._rebuild_pool_executor(pool_id)
            if executor is None:
                self._emit_response(request_id, ok=False, error="task pool missing")
                return True
            fanout = max(1, int(payload.get("fanout", 1) or 1))
            submitted = self._submit_background_calls(executor, payload, fanout=fanout, kind="pool", label="preload")
            self._emit_response(request_id, ok=True, submitted=submitted)
            return True

        self._emit_response(request_id, ok=False, error=f"unknown action: {action}")
        return True

    def poll_once(self) -> None:
        now = time.monotonic()
        self._drain_completed_futures()
        self._expire_service_timeouts(now=now)

    def _drain_completed_futures(self) -> None:
        while True:
            try:
                future = self._completed_futures.get_nowait()
            except queue.Empty:
                break
            meta = self._inflight.pop(future, None)
            if meta is None:
                continue
            try:
                status_text, result, err_type, err_message, timings = unpack_subprocess_result(future.result())
            except Exception as exc:
                status_text, result, err_type, err_message, timings = self._recover_or_fail_future(exc, meta)
                if status_text == "__RETRIED__":
                    continue
            kind = str(meta.get("kind", "") or "")
            if kind == "pool":
                self._emit_event(
                    {
                        "kind": "pool_task_done",
                        "pool_id": str(meta.get("pool_id", "") or ""),
                        "task_id": str(meta.get("task_id", "") or ""),
                        "attempt": int(meta.get("attempt", 0) or 0),
                        "status_text": status_text,
                        "result": result,
                        "err_type": err_type,
                        "err_message": err_message,
                        "timings": timings,
                    }
                )
                continue
            if kind == "runtime":
                self._emit_event(
                    {
                        "kind": "runtime_task_done",
                        "runtime_key": str(meta.get("runtime_key", "") or ""),
                        "task_id": str(meta.get("task_id", "") or ""),
                        "attempt": int(meta.get("attempt", 0) or 0),
                        "status_text": status_text,
                        "result": result,
                        "err_type": err_type,
                        "err_message": err_message,
                    }
                )
                continue
            if kind == "service":
                self._emit_response(
                    str(meta.get("request_id", "") or ""),
                    ok=True,
                    status_text=status_text,
                    result=result,
                    err_type=err_type,
                    err_message=err_message,
                    timings=timings,
                )

    def _recover_or_fail_future(
        self,
        exc: BaseException,
        meta: Dict[str, Any],
    ) -> tuple[str, Any, str, str, Dict[str, Any]]:
        kind = str(meta.get("kind", "") or "")
        recoveries = int(meta.get("recoveries", 0) or 0)
        if is_recoverable_pool_error(exc) and recoveries < 1:
            retry_payload = dict(meta.get("payload") or {})
            retry_future = None
            try:
                if kind == "service":
                    retry_future = self._submit_service_future(str(meta.get("service_id", "") or ""), retry_payload)
                elif kind == "pool":
                    retry_future = self._submit_pool_future(str(meta.get("pool_id", "") or ""), retry_payload)
                elif kind == "runtime":
                    retry_future = self._submit_runtime_future(retry_payload)
            except Exception:
                retry_future = None
            if retry_future is not None:
                if kind == "pool":
                    self._emit_event(
                        {
                            "kind": "pool_executor_rebuilt",
                            "pool_id": str(meta.get("pool_id", "") or ""),
                            "recoveries": recoveries + 1,
                        }
                    )
                elif kind == "service":
                    self._emit_event(
                        {
                            "kind": "service_executor_rebuilt",
                            "service_id": str(meta.get("service_id", "") or ""),
                            "recoveries": recoveries + 1,
                        }
                    )
                retry_meta = dict(meta)
                retry_meta["recoveries"] = recoveries + 1
                self._track_inflight(retry_future, retry_meta)
                return "__RETRIED__", None, "", "", {}
        return "FAILED_INFRA", None, "InfraException", repr(exc), {}

    def _expire_service_timeouts(self, *, now: float) -> None:
        timed_out = []
        for future, meta in list(self._inflight.items()):
            if str(meta.get("kind", "") or "") != "service":
                continue
            start_at = float(meta.get("start_at", now) or now)
            timeout_sec = float(meta.get("timeout_sec", 60.0) or 60.0)
            if now - start_at > timeout_sec:
                timed_out.append((future, meta))
        for future, meta in timed_out:
            self._inflight.pop(future, None)
            service_id = str(meta.get("service_id", "") or "")
            if service_id:
                executor = self._service_executors.pop(service_id, None)
                if executor is not None:
                    self._shutdown_executor(executor, wait=False)
            else:
                try:
                    future.cancel()
                except Exception:
                    pass
            self._emit_response(
                str(meta.get("request_id", "") or ""),
                ok=False,
                timeout=True,
                error="invoke timeout",
            )
