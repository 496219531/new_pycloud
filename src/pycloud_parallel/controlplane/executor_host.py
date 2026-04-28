from __future__ import annotations

"""Dedicated local executor host process for user-code execution."""

from collections import deque
import multiprocessing as mp
import os
import signal
import threading
import time
from typing import Any, Deque, Dict, Optional

from pycloud_parallel.controlplane.executor_core import ExecutorCore


def _simple_queue_get_if_ready(simple_queue, *, timeout: float = 0.0):
    reader = getattr(simple_queue, "_reader", None)
    if reader is not None:
        try:
            if not reader.poll(max(0.0, float(timeout))):
                return None
            return simple_queue.get()
        except (EOFError, OSError):
            return None
    try:
        if simple_queue.empty():
            return None
        return simple_queue.get()
    except (EOFError, OSError):
        return None


def _executor_host_main(request_q, event_q, task_worker_capacity: int) -> None:
    os.environ["PYCLOUD_EXECUTOR_PARENT_KIND"] = "executor_host"

    def _emit(item: Dict[str, Any]) -> None:
        event_q.put(dict(item))

    core = ExecutorCore(
        task_worker_capacity=max(1, int(task_worker_capacity or 1)),
        emit_response=_emit,
        emit_event=_emit,
    )
    running = True
    while running:
        message = _simple_queue_get_if_ready(request_q, timeout=0.01)
        if isinstance(message, dict):
            running = core.handle_request(
                str(message.get("request_id", "") or ""),
                str(message.get("action", "") or ""),
                dict(message.get("payload") or {}),
            )
        core.poll_once()
        if message is None:
            time.sleep(0.001)
    core.close()


class ExecutorHostClient:
    def __init__(self, *, task_worker_capacity: int = 1) -> None:
        self._ctx = mp.get_context("spawn")
        self._request_q = self._ctx.SimpleQueue()
        self._event_q = self._ctx.SimpleQueue()
        self._responses: Dict[str, Dict[str, Any]] = {}
        self._stream_events: Dict[str, Deque[Dict[str, Any]]] = {}
        self._expired_requests: set[str] = set()
        self._async_events: Deque[Dict[str, Any]] = deque()
        self._worker_pids: set[int] = set()
        self._cv = threading.Condition()
        self._seq = 0
        self._closed = False
        self._reader_stop = threading.Event()
        self._process = self._ctx.Process(
            target=_executor_host_main,
            args=(self._request_q, self._event_q, max(1, int(task_worker_capacity or 1))),
            daemon=False,
        )
        self._process.start()
        self._reader = threading.Thread(target=self._reader_loop, name="executor-host-reader", daemon=True)
        self._reader.start()

    def is_alive(self) -> bool:
        return (not self._closed) and self._process.is_alive()

    def _reader_loop(self) -> None:
        while not self._reader_stop.is_set():
            item = _simple_queue_get_if_ready(self._event_q, timeout=0.05)
            if item is None:
                continue
            if not isinstance(item, dict):
                continue
            kind = str(item.get("kind", "") or "")
            with self._cv:
                if kind == "executor_worker_pids":
                    for pid in item.get("worker_pids", ()) or ():
                        try:
                            normalized_pid = int(pid)
                        except (TypeError, ValueError):
                            continue
                        if normalized_pid > 0:
                            self._worker_pids.add(normalized_pid)
                    self._async_events.append(item)
                    continue
                if kind == "response":
                    request_id = str(item.get("request_id", "") or "")
                    if request_id in self._expired_requests:
                        self._expired_requests.discard(request_id)
                        continue
                    self._responses[request_id] = item
                    self._cv.notify_all()
                elif kind == "service_stream_item":
                    request_id = str(item.get("request_id", "") or "")
                    if request_id in self._expired_requests:
                        self._expired_requests.discard(request_id)
                        continue
                    self._stream_events.setdefault(request_id, deque()).append(item)
                    self._cv.notify_all()
                else:
                    self._async_events.append(item)

    def close(self, *, shutdown_timeout_sec: float = 2.0) -> None:
        if self._closed:
            return
        if self._process.is_alive():
            try:
                self._request("shutdown", timeout_sec=max(0.1, float(shutdown_timeout_sec or 0.1)))
            except Exception:
                pass
        self._closed = True
        self._reader_stop.set()
        if self._reader.is_alive():
            self._reader.join(timeout=1.0)
        if self._process.is_alive():
            self._process.join(timeout=5.0)
        if self._process.is_alive():
            self._process.terminate()
            self._process.join(timeout=2.0)
        if self._process.is_alive() and hasattr(self._process, "kill"):
            self._process.kill()
            self._process.join(timeout=1.0)
        self._terminate_tracked_workers()
        try:
            self._request_q.close()
        except Exception:
            pass
        try:
            self._event_q.close()
        except Exception:
            pass

    def _terminate_tracked_workers(self) -> None:
        with self._cv:
            worker_pids = list(self._worker_pids)
            self._worker_pids.clear()
        current_pid = os.getpid()
        for pid in worker_pids:
            if pid <= 0 or pid == current_pid:
                continue
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                continue
        if worker_pids:
            time.sleep(0.05)
        sigkill = getattr(signal, "SIGKILL", signal.SIGTERM)
        for pid in worker_pids:
            if pid <= 0 or pid == current_pid:
                continue
            try:
                os.kill(pid, sigkill)
            except Exception:
                continue

    def drain_events(self) -> list[Dict[str, Any]]:
        with self._cv:
            items = list(self._async_events)
            self._async_events.clear()
            return items

    def poll_events(self) -> list[Dict[str, Any]]:
        return self.drain_events()

    def _request(self, action: str, *, payload: Optional[Dict[str, Any]] = None, timeout_sec: float = 10.0) -> Dict[str, Any]:
        if self._closed and action != "shutdown":
            raise RuntimeError("executor host is closed")
        request_id = self._send_request(action, payload=payload)
        return self._wait_response(request_id, action=action, timeout_sec=timeout_sec)

    def _send_request(self, action: str, *, payload: Optional[Dict[str, Any]] = None) -> str:
        if self._closed and action != "shutdown":
            raise RuntimeError("executor host is closed")
        with self._cv:
            self._seq += 1
            request_id = f"req-{self._seq}"
        self._request_q.put({"request_id": request_id, "action": action, "payload": dict(payload or {})})
        return request_id

    def _wait_response(self, request_id: str, *, action: str, timeout_sec: float) -> Dict[str, Any]:
        deadline = time.monotonic() + max(0.1, float(timeout_sec))
        with self._cv:
            while request_id not in self._responses:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._responses.pop(request_id, None)
                    self._expired_requests.add(request_id)
                    raise TimeoutError(f"executor host request timed out: {action}")
                if not self._process.is_alive():
                    raise RuntimeError("executor host process died")
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

    def create_task_pool(self, *, pool_id: str, worker_count: int) -> None:
        resp = self._request(
            "create_task_pool",
            payload={"pool_id": pool_id, "worker_count": int(worker_count)},
        )
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "create_task_pool failed")))

    def stop_task_pool(self, *, pool_id: str) -> None:
        resp = self._request("stop_task_pool", payload={"pool_id": pool_id})
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "stop_task_pool failed")))

    def _request_action(
        self,
        action: str,
        *,
        payload: Dict[str, Any],
        timeout_sec: float = 30.0,
        raise_on_error: bool = True,
    ) -> Dict[str, Any]:
        resp = self._request(action, payload=payload, timeout_sec=timeout_sec)
        if raise_on_error and not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", f"{action} failed")))
        return resp

    def prepare_artifact(self, *, artifact_spec: Dict[str, Any], timeout_sec: float = 30.0) -> Dict[str, Any]:
        return self._request_action(
            "prepare_artifact",
            payload=dict(artifact_spec),
            timeout_sec=max(1.0, float(timeout_sec or 1.0)),
            raise_on_error=False,
        )

    def _submit_task(
        self,
        action: str,
        *,
        identity: Dict[str, Any],
        task_id: str,
        attempt: int,
        execute_spec: Dict[str, Any],
    ) -> None:
        self._request_action(
            action,
            payload={
                **dict(identity),
                "task_id": task_id,
                "attempt": int(attempt),
                **dict(execute_spec),
            },
        )

    def _warmup(self, action: str, *, identity: Dict[str, Any], fanout: int, execute_spec: Dict[str, Any]) -> int:
        resp = self._request_action(
            action,
            payload={**dict(identity), "fanout": int(fanout), **dict(execute_spec)},
            timeout_sec=max(1.0, float(fanout) + 5.0),
        )
        return int(resp.get("submitted", 0) or 0)

    def call_service(self, *, service_id: str, timeout_sec: float, execute_spec: Dict[str, Any]) -> Dict[str, Any]:
        return self._request_action(
            "call_service",
            payload={"service_id": service_id, "timeout_sec": float(timeout_sec), **dict(execute_spec)},
            timeout_sec=max(1.0, float(timeout_sec) + 2.0),
            raise_on_error=False,
        )

    def call_service_stream(self, *, service_id: str, timeout_sec: float, execute_spec: Dict[str, Any]):
        request_id = self._send_request(
            "call_service_stream",
            payload={"service_id": service_id, "timeout_sec": float(timeout_sec), **dict(execute_spec)},
        )
        deadline = time.monotonic() + max(1.0, float(timeout_sec) + 2.0)

        def _iter():
            try:
                while True:
                    with self._cv:
                        while request_id not in self._responses and not self._stream_events.get(request_id):
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                self._responses.pop(request_id, None)
                                self._stream_events.pop(request_id, None)
                                self._expired_requests.add(request_id)
                                raise TimeoutError("executor host request timed out: call_service_stream")
                            if not self._process.is_alive():
                                raise RuntimeError("executor host process died")
                            self._cv.wait(timeout=min(0.1, remaining))
                        queue = self._stream_events.get(request_id)
                        if queue:
                            item = queue.popleft()
                            if not queue:
                                self._stream_events.pop(request_id, None)
                        else:
                            item = None
                        response = self._responses.pop(request_id, None)
                    if item is not None:
                        yield item
                        if response is None:
                            continue
                    if response is None:
                        continue
                    if not response.get("ok", False):
                        raise RuntimeError(str(response.get("error", "call_service_stream failed")))
                    done_event = dict(response)
                    done_event["kind"] = "service_stream_done"
                    yield done_event
                    return
            finally:
                with self._cv:
                    self._stream_events.pop(request_id, None)
                    self._responses.pop(request_id, None)

        return _iter()

    def warmup_service(self, *, service_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._warmup("warmup_service", identity={"service_id": service_id}, fanout=fanout, execute_spec=execute_spec)

    def preload_service(self, *, service_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._warmup("preload_service", identity={"service_id": service_id}, fanout=fanout, execute_spec=execute_spec)

    def submit_runtime_task(
        self,
        *,
        runtime_key: str,
        task_id: str,
        attempt: int,
        execute_spec: Dict[str, Any],
    ) -> None:
        self._submit_task(
            "submit_runtime_task",
            identity={"runtime_key": runtime_key},
            task_id=task_id,
            attempt=attempt,
            execute_spec=execute_spec,
        )

    def warmup_runtime(self, *, runtime_key: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._warmup("warmup_runtime", identity={"runtime_key": runtime_key}, fanout=fanout, execute_spec=execute_spec)

    def submit_pool_task(
        self,
        *,
        pool_id: str,
        task_id: str,
        attempt: int,
        execute_spec: Dict[str, Any],
    ) -> None:
        self._submit_task(
            "submit_pool_task",
            identity={"pool_id": pool_id},
            task_id=task_id,
            attempt=attempt,
            execute_spec=execute_spec,
        )

    def warmup_pool(self, *, pool_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._warmup("warmup_pool", identity={"pool_id": pool_id}, fanout=fanout, execute_spec=execute_spec)

    def preload_pool(self, *, pool_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._warmup("preload_pool", identity={"pool_id": pool_id}, fanout=fanout, execute_spec=execute_spec)
