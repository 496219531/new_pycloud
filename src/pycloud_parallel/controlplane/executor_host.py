from __future__ import annotations

"""Dedicated local executor host process for user-code execution."""

from collections import deque
import contextlib
import subprocess
import traceback
import multiprocessing as mp
import os
import signal
import sys
import threading
import time
from typing import Any, Deque, Dict, Optional, Tuple

from pycloud_parallel.controlplane.executor_core import ExecutorCore


DEFAULT_EXECUTOR_OPERATION_TIMEOUT_SEC = 600.0


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


def _pid_alive(pid: int) -> bool:
    normalized = int(pid or 0)
    if normalized <= 0:
        return False
    if os.name == "nt":
        try:
            proc = subprocess.run(
                ["tasklist", "/FI", f"PID eq {normalized}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=2.0,
                check=False,
            )
            return str(normalized) in str(proc.stdout or "")
        except Exception:
            return True
    try:
        os.kill(normalized, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _executor_host_main(request_q, event_q, task_worker_capacity: int) -> None:
    os.environ["PYCLOUD_EXECUTOR_PARENT_KIND"] = "executor_host"

    def _emit(item: Dict[str, Any]) -> None:
        event_q.put(dict(item))

    core = ExecutorCore(
        task_worker_capacity=max(1, int(task_worker_capacity or 1)),
        emit_response=_emit,
        emit_event=_emit,
    )
    try:
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
    except BaseException as exc:
        with contextlib.suppress(Exception):
            event_q.put(
                {
                    "kind": "executor_host_crash",
                    "error_type": type(exc).__name__,
                    "error": repr(exc),
                    "traceback": traceback.format_exc(limit=40),
                }
            )
        raise
    finally:
        core.close()


def _windows_spawn_entrypoint_hint() -> str:
    if os.name != "nt":
        return ""
    main = sys.modules.get("__main__")
    main_file = str(getattr(main, "__file__", "") or "")
    if not main_file or main_file.startswith("<") or main_file.endswith("\\<stdin>") or main_file.endswith("/<stdin>"):
        return "Windows spawn cannot re-import the current entrypoint; run from a .py file with if __name__ == '__main__' guard"
    return "on Windows, ensure the caller script wraps pycloud/deploy startup in if __name__ == '__main__'"


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
        self._worker_pid_sets: Dict[str, set[int]] = {}
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
                    pid_set: set[int] = set()
                    for pid in item.get("worker_pids", ()) or ():
                        try:
                            normalized_pid = int(pid)
                        except (TypeError, ValueError):
                            continue
                        if normalized_pid > 0:
                            pid_set.add(normalized_pid)
                    scope = str(item.get("scope", "") or "")
                    key = str(item.get("key", "") or "")
                    worker_key = f"{scope}:{key}"
                    previous = self._worker_pid_sets.get(worker_key, set())
                    self._worker_pids.difference_update(previous)
                    self._worker_pid_sets[worker_key] = pid_set
                    self._worker_pids.update(pid_set)
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
        tracked_host_pid = int(getattr(self._process, "pid", 0) or 0)
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
        if os.name == "nt" and self._process.is_alive():
            self._kill_process_tree(tracked_host_pid)
            self._process.join(timeout=2.0)
        if self._process.is_alive():
            self._process.terminate()
            self._process.join(timeout=2.0)
        if self._process.is_alive() and hasattr(self._process, "kill"):
            self._process.kill()
            self._process.join(timeout=1.0)
        self._terminate_tracked_workers()
        if self._process.is_alive():
            self._kill_process_tree(tracked_host_pid)
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
            self._worker_pid_sets.clear()
        current_pid = os.getpid()
        if os.name == "nt":
            for pid in worker_pids:
                if pid <= 0 or pid == current_pid:
                    continue
                self._kill_process_tree(pid)
            return
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

    def _terminate_worker_pids(self, worker_pids: list[int]) -> None:
        current_pid = os.getpid()
        normalized = [int(pid) for pid in (worker_pids or []) if int(pid or 0) > 0 and int(pid or 0) != current_pid]
        if not normalized:
            return
        if os.name == "nt":
            for pid in normalized:
                self._kill_process_tree(pid)
            return
        for pid in normalized:
            with contextlib.suppress(Exception):
                os.kill(pid, signal.SIGTERM)
        time.sleep(0.05)
        sigkill = getattr(signal, "SIGKILL", signal.SIGTERM)
        for pid in normalized:
            with contextlib.suppress(Exception):
                os.kill(pid, sigkill)

    def _kill_process_tree(self, pid: int) -> None:
        if pid <= 0 or pid == os.getpid():
            return
        if os.name == "nt":
            with contextlib.suppress(Exception):
                subprocess.run(
                    ["taskkill", "/PID", str(int(pid)), "/T", "/F"],
                    capture_output=True,
                    text=True,
                    timeout=5.0,
                    check=False,
                )
            return
        with contextlib.suppress(Exception):
            os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))

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
        request_payload = dict(payload or {})
        request_id = self._send_request(action, payload=request_payload)
        return self._wait_response(request_id, action=action, timeout_sec=timeout_sec, payload=request_payload)

    def _send_request(self, action: str, *, payload: Optional[Dict[str, Any]] = None) -> str:
        if self._closed and action != "shutdown":
            raise RuntimeError("executor host is closed")
        with self._cv:
            self._seq += 1
            request_id = f"req-{self._seq}"
        self._request_q.put({"request_id": request_id, "action": action, "payload": dict(payload or {})})
        return request_id

    def _wait_response(self, request_id: str, *, action: str, timeout_sec: float, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        deadline = time.monotonic() + max(0.1, float(timeout_sec))
        with self._cv:
            while request_id not in self._responses:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._responses.pop(request_id, None)
                    self._expired_requests.add(request_id)
                    request_payload = dict(payload or {})
                    artifact_payload = request_payload if action == "prepare_artifact" else {}
                    logger.warning(
                        "executor host request timed out action=%s request_id=%s timeout_sec=%.3f "
                        "scope=%s key=%s artifact_path=%s entry_module=%s package_format=%s dependency_policy_mode=%s",
                        action,
                        request_id,
                        max(0.1, float(timeout_sec)),
                        str(artifact_payload.get("prepare_scope", "") or ""),
                        str(artifact_payload.get("prepare_key", "") or ""),
                        str(artifact_payload.get("artifact_path", "") or ""),
                        str(artifact_payload.get("entry_module", "") or ""),
                        str(artifact_payload.get("package_format", "") or ""),
                        str(artifact_payload.get("dependency_policy_mode", "") or ""),
                    )
                    raise TimeoutError(f"executor host request timed out: {action}")
                if not self._process.is_alive():
                    raise RuntimeError(
                        self._format_process_died_message(action)
                    )
                self._cv.wait(timeout=min(0.1, remaining))
            return self._responses.pop(request_id)

    def _format_process_died_message(self, action: str) -> str:
        pid = int(getattr(self._process, "pid", 0) or 0)
        exitcode = getattr(self._process, "exitcode", None)
        parts = [
            f"executor host process died action={action}",
            f"pid={pid}",
            f"exitcode={exitcode}",
        ]
        hint = _windows_spawn_entrypoint_hint()
        if hint:
            parts.append(f"hint={hint}")
        return " ".join(parts)

    def create_service(self, *, service_id: str, worker_count: int) -> None:
        resp = self._request(
            "create_service",
            payload={"service_id": service_id, "worker_count": int(worker_count)},
            timeout_sec=DEFAULT_EXECUTOR_OPERATION_TIMEOUT_SEC,
        )
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "create_service failed")))

    def stop_service(self, *, service_id: str, reason: str = "") -> None:
        worker_key = f"service:{str(service_id or '').strip()}"
        with self._cv:
            service_worker_pids = list(self._worker_pid_sets.pop(worker_key, set()))
            self._worker_pids.difference_update(service_worker_pids)
        resp = self._request("stop_service", payload={"service_id": service_id, "reason": str(reason or "")})
        self._terminate_worker_pids(service_worker_pids)
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "stop_service failed")))

    def service_worker_liveness(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        with self._cv:
            for worker_key, pid_set in self._worker_pid_sets.items():
                if not worker_key.startswith("service:"):
                    continue
                service_id = worker_key.split(":", 1)[1]
                alive = 0
                for pid in list(pid_set):
                    if _pid_alive(int(pid)):
                        alive += 1
                out[service_id] = alive
        return out

    def resource_worker_liveness(self) -> Dict[Tuple[str, str], int]:
        out: Dict[Tuple[str, str], int] = {}
        with self._cv:
            for worker_key, pid_set in self._worker_pid_sets.items():
                if ":" not in worker_key:
                    continue
                scope, resource_id = worker_key.split(":", 1)
                resource_kind = "service" if scope == "service" else "task_pool" if scope == "pool" else ""
                if not resource_kind:
                    continue
                alive = 0
                for pid in list(pid_set):
                    if _pid_alive(int(pid)):
                        alive += 1
                out[(resource_kind, resource_id)] = alive
        return out

    def create_task_pool(self, *, pool_id: str, worker_count: int) -> None:
        resp = self._request(
            "create_task_pool",
            payload={"pool_id": pool_id, "worker_count": int(worker_count)},
            timeout_sec=DEFAULT_EXECUTOR_OPERATION_TIMEOUT_SEC,
        )
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "create_task_pool failed")))

    def stop_task_pool(self, *, pool_id: str, reason: str = "") -> None:
        worker_key = f"pool:{str(pool_id or '').strip()}"
        with self._cv:
            pool_worker_pids = list(self._worker_pid_sets.pop(worker_key, set()))
            self._worker_pids.difference_update(pool_worker_pids)
        resp = self._request("stop_task_pool", payload={"pool_id": pool_id, "reason": str(reason or "")})
        self._terminate_worker_pids(pool_worker_pids)
        if not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", "stop_task_pool failed")))

    def _request_action(
        self,
        action: str,
        *,
        payload: Dict[str, Any],
        timeout_sec: float = DEFAULT_EXECUTOR_OPERATION_TIMEOUT_SEC,
        raise_on_error: bool = True,
    ) -> Dict[str, Any]:
        resp = self._request(action, payload=payload, timeout_sec=timeout_sec)
        if raise_on_error and not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", f"{action} failed")))
        return resp

    def prepare_artifact(self, *, artifact_spec: Dict[str, Any], timeout_sec: float = DEFAULT_EXECUTOR_OPERATION_TIMEOUT_SEC) -> Dict[str, Any]:
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
            timeout_sec=DEFAULT_EXECUTOR_OPERATION_TIMEOUT_SEC,
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
                                raise RuntimeError(self._format_process_died_message("call_service_stream"))
                            self._cv.wait(timeout=min(0.1, remaining))
                        queue = self._stream_events.get(request_id)
                        if queue:
                            item = queue.popleft()
                            if not queue:
                                self._stream_events.pop(request_id, None)
                            response = None
                        else:
                            item = None
                            response = self._responses.pop(request_id, None)
                    if item is not None:
                        yield item
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
