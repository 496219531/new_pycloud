from __future__ import annotations

"""Dedicated local executor host process for user-code execution."""

from collections import deque
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FutureTimeout
import multiprocessing as mp
import queue
import threading
import time
from typing import Any, Deque, Dict, Optional


def _executor_host_main(request_q, event_q) -> None:
    service_executors: Dict[str, ProcessPoolExecutor] = {}
    runtime_executors: Dict[str, ProcessPoolExecutor] = {}
    inflight: Dict[object, Dict[str, Any]] = {}
    shutdown_request_id = ""

    def _send_response(request_id: str, **payload: object) -> None:
        event_q.put({"kind": "response", "request_id": request_id, **payload})

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

    def _ensure_mp_context():
        try:
            return mp.get_context("spawn")
        except ValueError:
            return None

    def _submit_callable(executor: ProcessPoolExecutor, args: Dict[str, Any]):
        from pycloud_parallel.controlplane.state import _execute_payload_in_subprocess

        return executor.submit(
            _execute_payload_in_subprocess,
            args["artifact_path"],
            args["entry_module"],
            args["package_format"],
            args["dependency_path"],
            args["object_dir"],
            args["export_mode"],
            args["export_methods"],
            args["export_decorator"],
            args["method_name"],
            args["entry_callable"],
            args["payload"],
        )

    def _handle_request(message: Dict[str, Any]) -> bool:
        request_id = str(message.get("request_id", "") or "")
        action = str(message.get("action", "") or "")
        payload = dict(message.get("payload") or {})
        if action == "shutdown":
            nonlocal shutdown_request_id
            shutdown_request_id = request_id
            return False

        try:
            if action == "create_service":
                service_id = str(payload.get("service_id", "") or "")
                worker_count = max(1, int(payload.get("worker_count", 1) or 1))
                existing = service_executors.pop(service_id, None)
                _shutdown_executor(existing, wait=True)
                service_executors[service_id] = ProcessPoolExecutor(
                    max_workers=worker_count,
                    mp_context=_ensure_mp_context(),
                )
                _send_response(request_id, ok=True)
                return True

            if action == "stop_service":
                service_id = str(payload.get("service_id", "") or "")
                executor = service_executors.pop(service_id, None)
                _shutdown_executor(executor, wait=True)
                _send_response(request_id, ok=True)
                return True

            if action == "call_service":
                service_id = str(payload.get("service_id", "") or "")
                executor = service_executors.get(service_id)
                if executor is None:
                    _send_response(request_id, ok=False, error="service executor missing")
                    return True
                future = _submit_callable(executor, payload)
                try:
                    status_text, result, err_type, err_message = future.result(timeout=max(0.1, float(payload.get("timeout_sec", 60.0) or 60.0)))
                except FutureTimeout:
                    _send_response(request_id, ok=False, timeout=True, error="invoke timeout")
                    return True
                except Exception as exc:
                    _send_response(request_id, ok=False, error=repr(exc))
                    return True
                _send_response(
                    request_id,
                    ok=True,
                    status_text=status_text,
                    result=result,
                    err_type=err_type,
                    err_message=err_message,
                )
                return True

            if action == "start_runtime_slot":
                runtime_key = str(payload.get("runtime_key", "") or "")
                existing = runtime_executors.get(runtime_key)
                if existing is None:
                    runtime_executors[runtime_key] = ProcessPoolExecutor(
                        max_workers=1,
                        mp_context=_ensure_mp_context(),
                    )
                _send_response(request_id, ok=True)
                return True

            if action == "stop_runtime_slot":
                runtime_key = str(payload.get("runtime_key", "") or "")
                executor = runtime_executors.pop(runtime_key, None)
                _shutdown_executor(executor, wait=True)
                _send_response(request_id, ok=True)
                return True

            if action == "submit_runtime_task":
                runtime_key = str(payload.get("runtime_key", "") or "")
                executor = runtime_executors.get(runtime_key)
                if executor is None:
                    _send_response(request_id, ok=False, error="runtime slot missing")
                    return True
                future = _submit_callable(executor, payload)
                inflight[future] = {
                    "runtime_key": runtime_key,
                    "task_id": str(payload.get("task_id", "") or ""),
                    "attempt": int(payload.get("attempt", 0) or 0),
                }
                _send_response(request_id, ok=True)
                return True

            _send_response(request_id, ok=False, error=f"unknown action: {action}")
            return True
        except Exception as exc:
            _send_response(request_id, ok=False, error=repr(exc))
            return True

    running = True
    while running:
        try:
            message = request_q.get(timeout=0.05)
        except queue.Empty:
            message = None
        if isinstance(message, dict):
            running = _handle_request(message)

        completed = [future for future in list(inflight.keys()) if future.done()]
        for future in completed:
            meta = inflight.pop(future, {})
            try:
                status_text, result, err_type, err_message = future.result()
            except Exception as exc:
                status_text = "FAILED_INFRA"
                result = None
                err_type = "InfraException"
                err_message = repr(exc)
            event_q.put(
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

    for executor in list(service_executors.values()):
        _shutdown_executor(executor, wait=True)
    for executor in list(runtime_executors.values()):
        _shutdown_executor(executor, wait=True)
    if shutdown_request_id:
        _send_response(shutdown_request_id, ok=True)


class ExecutorHostClient:
    def __init__(self) -> None:
        self._ctx = mp.get_context("spawn")
        self._request_q = self._ctx.Queue()
        self._event_q = self._ctx.Queue()
        self._responses: Dict[str, Dict[str, Any]] = {}
        self._async_events: Deque[Dict[str, Any]] = deque()
        self._cv = threading.Condition()
        self._seq = 0
        self._closed = False
        self._reader_stop = threading.Event()
        self._process = self._ctx.Process(
            target=_executor_host_main,
            args=(self._request_q, self._event_q),
            daemon=False,
        )
        self._process.start()
        self._reader = threading.Thread(target=self._reader_loop, name="executor-host-reader", daemon=True)
        self._reader.start()

    def _reader_loop(self) -> None:
        while not self._reader_stop.is_set():
            try:
                item = self._event_q.get(timeout=0.1)
            except queue.Empty:
                continue
            if not isinstance(item, dict):
                continue
            kind = str(item.get("kind", "") or "")
            with self._cv:
                if kind == "response":
                    request_id = str(item.get("request_id", "") or "")
                    self._responses[request_id] = item
                    self._cv.notify_all()
                else:
                    self._async_events.append(item)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._request("shutdown", timeout_sec=30.0)
        except Exception:
            pass
        self._reader_stop.set()
        if self._reader.is_alive():
            self._reader.join(timeout=1.0)
        if self._process.is_alive():
            self._process.join(timeout=5.0)
        if self._process.is_alive():
            self._process.terminate()
            self._process.join(timeout=2.0)
        try:
            self._request_q.close()
            self._request_q.join_thread()
        except Exception:
            pass
        try:
            self._event_q.close()
            self._event_q.join_thread()
        except Exception:
            pass

    def drain_events(self) -> list[Dict[str, Any]]:
        with self._cv:
            items = list(self._async_events)
            self._async_events.clear()
            return items

    def _request(self, action: str, *, payload: Optional[Dict[str, Any]] = None, timeout_sec: float = 10.0) -> Dict[str, Any]:
        if self._closed:
            raise RuntimeError("executor host is closed")
        with self._cv:
            self._seq += 1
            request_id = f"req-{self._seq}"
        self._request_q.put({"request_id": request_id, "action": action, "payload": dict(payload or {})})
        deadline = time.monotonic() + max(0.1, float(timeout_sec))
        with self._cv:
            while request_id not in self._responses:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"executor host request timed out: {action}")
                self._cv.wait(timeout=min(0.1, remaining))
            return self._responses.pop(request_id)

    def create_service(self, *, service_id: str, worker_count: int) -> None:
        resp = self._request(
            "create_service",
            payload={"service_id": service_id, "worker_count": int(worker_count)},
        )
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "create_service failed")))

    def stop_service(self, *, service_id: str) -> None:
        resp = self._request("stop_service", payload={"service_id": service_id})
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "stop_service failed")))

    def call_service(self, *, service_id: str, timeout_sec: float, execute_spec: Dict[str, Any]) -> Dict[str, Any]:
        resp = self._request(
            "call_service",
            payload={"service_id": service_id, "timeout_sec": float(timeout_sec), **dict(execute_spec)},
            timeout_sec=max(1.0, float(timeout_sec) + 2.0),
        )
        if not resp.get("ok", False):
            return resp
        return resp

    def start_runtime_slot(self, *, runtime_key: str) -> None:
        resp = self._request("start_runtime_slot", payload={"runtime_key": runtime_key})
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "start_runtime_slot failed")))

    def stop_runtime_slot(self, *, runtime_key: str) -> None:
        resp = self._request("stop_runtime_slot", payload={"runtime_key": runtime_key})
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "stop_runtime_slot failed")))

    def submit_runtime_task(
        self,
        *,
        runtime_key: str,
        task_id: str,
        attempt: int,
        execute_spec: Dict[str, Any],
    ) -> None:
        resp = self._request(
            "submit_runtime_task",
            payload={
                "runtime_key": runtime_key,
                "task_id": task_id,
                "attempt": int(attempt),
                **dict(execute_spec),
            },
        )
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "submit_runtime_task failed")))
