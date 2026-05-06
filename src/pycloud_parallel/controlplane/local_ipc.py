from __future__ import annotations

"""Local service IPC registry and transport."""

import base64
import contextlib
import hashlib
import json
import os
import pickle
from pathlib import Path
import signal
import tempfile
import threading
import time
import uuid
from multiprocessing.connection import Client, Listener
from typing import Any, Dict, Optional

from pycloud_parallel.data.ref import DataRef, maybe_data_ref
from pycloud_parallel.controlplane.config import OBJECT_SEGMENT_MAX_BYTES, get_local_service_payload_policy
from pycloud_parallel.controlplane.node.results import (
    _commit_result_file,
    _commit_result_segment,
    _resolve_single_data_ref,
)
from pycloud_parallel.controlplane.payload_transport import estimate_payload_inline_size, prepare_outbound_payload
from pycloud_parallel.controlplane.serialization import (
    INTERNAL_PICKLE_NATIVE_V1,
    make_validated_inline_transport_carrier,
    validate_inline_payload_size,
)


_REGISTRY_VERSION = 1
LOCAL_IPC_BACKLOG = 1024
LOCAL_IPC_CONNECT_RETRY_INTERVAL_SEC = 0.02
PYCLOUD_LOCAL_IPC_AUTH = "PYCLOUD_LOCAL_IPC_AUTH"


def _local_ipc_auth_enabled() -> bool:
    raw = str(os.environ.get(PYCLOUD_LOCAL_IPC_AUTH, "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _local_object_dir(meta: Dict[str, object]) -> str:
    object_dir = str(meta.get("object_dir", "") or "").strip()
    if not object_dir:
        raise RuntimeError("local service IPC metadata has no object_dir")
    return object_dir


def _store_local_payload_blob(
    blob: bytes,
    *,
    meta: Dict[str, object],
    fmt: str,
    materialize_as: str,
) -> DataRef:
    object_dir = _local_object_dir(meta)
    if len(blob) <= max(0, int(OBJECT_SEGMENT_MAX_BYTES)):
        artifact = _commit_result_segment(blob, object_dir=object_dir, fmt=fmt, materialize_as=materialize_as)
    else:
        root = Path(object_dir).resolve()
        root.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix="pycloud-local-payload-", suffix=".bin", dir=str(root))
        try:
            with os.fdopen(fd, "wb") as fp:
                fp.write(blob)
            artifact = _commit_result_file(
                Path(tmp_name),
                object_dir=object_dir,
                fmt=fmt,
                size_bytes=len(blob),
                materialize_as=materialize_as,
            )
        finally:
            Path(tmp_name).unlink(missing_ok=True)
    return DataRef(
        ref_id=artifact.object_id,
        storage_id=artifact.object_id,
        logical_type="",
        format=artifact.format,
        size_bytes=artifact.size_bytes,
        materialize_as=artifact.materialize_as,
        locator_kind="node_local",
        locator_token="",
        consume_on_read=False,
        node_id=str(meta.get("node_id", "") or ""),
        node_instance_id=str(meta.get("node_instance_id", "") or ""),
    )


def _put_local_payload_data(
    value: Any,
    *,
    meta: Dict[str, object],
    format: str = "",
    default_serialization_mode: str = "",
) -> DataRef:
    existing = maybe_data_ref(value)
    if existing is not None:
        return existing
    from pycloud_parallel.execution.support import _serialize_data_for_object_ref

    source = _serialize_data_for_object_ref(
        value,
        format=format,
        default_serialization_mode=default_serialization_mode,
    )
    if getattr(source, "is_file", False):
        path = Path(str(source.file_path)).expanduser().resolve()
        artifact = _commit_result_file(
            path,
            object_dir=_local_object_dir(meta),
            fmt=source.format,
            size_bytes=int(path.stat().st_size),
            materialize_as=source.materialize_as,
        )
        return DataRef(
            ref_id=artifact.object_id,
            storage_id=artifact.object_id,
            logical_type="",
            format=artifact.format,
            size_bytes=artifact.size_bytes,
            materialize_as=artifact.materialize_as,
            locator_kind="node_local",
            locator_token="",
            consume_on_read=False,
            node_id=str(meta.get("node_id", "") or ""),
            node_instance_id=str(meta.get("node_instance_id", "") or ""),
        )
    return _store_local_payload_blob(
        source.blob,
        meta=meta,
        fmt=source.format,
        materialize_as=source.materialize_as,
    )


def _estimate_local_inline_size(value: Any) -> int:
    if isinstance(value, os.PathLike):
        return len(str(Path(value).expanduser().resolve()).encode("utf-8")) + 16
    if isinstance(value, str):
        try:
            path = Path(value).expanduser()
            if path.exists() and path.is_file():
                return len(str(path.resolve()).encode("utf-8")) + 16
        except OSError:
            pass
        return len(value.encode("utf-8")) + 16
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    try:
        return estimate_payload_inline_size(value)
    except Exception:
        return len(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))


def _normalize_local_path_value(value: Any) -> Any:
    if isinstance(value, os.PathLike):
        return Path(value).expanduser().resolve()
    if isinstance(value, str):
        try:
            path = Path(value).expanduser()
            if path.exists() and path.is_file():
                return str(path.resolve())
        except OSError:
            return value
    return value


def _normalize_local_payload_paths(value: Any) -> Any:
    normalized = _normalize_local_path_value(value)
    if normalized is not value:
        return normalized
    if isinstance(value, dict):
        return {key: _normalize_local_payload_paths(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_local_payload_paths(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_local_payload_paths(item) for item in value)
    return value


def _prepare_local_payload(payload: Dict[str, object], *, meta: Dict[str, object], serialization_mode: str = "") -> Dict[str, object]:
    return prepare_outbound_payload(
        _normalize_local_payload_paths(payload),
        put_data=lambda value, *, format="": _put_local_payload_data(
            value,
            meta=meta,
            format=format,
            default_serialization_mode=serialization_mode,
        ),
        estimate_inline_size=_estimate_local_inline_size,
        policy=get_local_service_payload_policy(),
    )


def _make_local_pickle_payload_transport(
    payload: Dict[str, object],
    *,
    meta: Dict[str, object],
    serialization_mode: str = "",
) -> Dict[str, object]:
    policy = get_local_service_payload_policy()
    normalized_payload = dict(_normalize_local_payload_paths(payload or {}))
    raw_payload = pickle.dumps(normalized_payload, protocol=pickle.HIGHEST_PROTOCOL)
    if len(raw_payload) > max(1, int(policy.inline_payload_soft_limit_bytes)):
        prepared_payload = _prepare_local_payload(payload, meta=meta, serialization_mode=serialization_mode)
        raw_payload = pickle.dumps(prepared_payload, protocol=pickle.HIGHEST_PROTOCOL)
    size = validate_inline_payload_size(
        len(raw_payload),
        limit_bytes=policy.inline_payload_hard_limit_bytes,
        context="local IPC service payload",
    )
    return make_validated_inline_transport_carrier(
        codec=INTERNAL_PICKLE_NATIVE_V1,
        payload=raw_payload,
        content_size=size,
        payload_mode="service_call",
        context="service_owner",
    )


def _registry_dir() -> Path:
    root = os.environ.get("PYCLOUD_LOCAL_IPC_DIR", "")
    path = Path(root).expanduser() if root else Path(tempfile.gettempdir()) / "pycloud_parallel" / "local_services"
    path.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        path.chmod(0o700)
    return path


def _service_key(service_name: str) -> str:
    normalized = str(service_name or "").strip()
    if not normalized:
        raise ValueError("service_name is required")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def local_service_metadata_path(service_name: str) -> Path:
    return _registry_dir() / f"{_service_key(service_name)}.json"


def _local_service_address(service_name: str) -> tuple[str, str]:
    key = _service_key(service_name)
    if os.name == "nt":
        return rf"\\.\pipe\pycloud-parallel-{key[:32]}", "AF_PIPE"
    socket_root = Path("/tmp") if Path("/tmp").exists() else Path(tempfile.gettempdir())
    return str(socket_root / f"pycloud-{key[:24]}.sock"), "AF_UNIX"


def _read_metadata(service_name: str) -> Dict[str, object]:
    path = local_service_metadata_path(service_name)
    if not path.exists():
        raise FileNotFoundError(str(path))
    return json.loads(path.read_text(encoding="utf-8"))


def _write_metadata(service_name: str, payload: Dict[str, object]) -> None:
    path = local_service_metadata_path(service_name)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    with contextlib.suppress(OSError):
        tmp.chmod(0o600)
    os.replace(str(tmp), str(path))
    with contextlib.suppress(OSError):
        path.chmod(0o600)


def _remove_metadata(service_name: str) -> None:
    local_service_metadata_path(service_name).unlink(missing_ok=True)


def _metadata_token(service_name: str) -> str:
    with contextlib.suppress(Exception):
        return str(_read_metadata(service_name).get("ipc_token", "") or "")
    return ""


def _discard_metadata_if_current(service_name: str, meta: Dict[str, object]) -> None:
    expected_token = str(meta.get("ipc_token", "") or "")
    if expected_token and _metadata_token(service_name) != expected_token:
        return
    _remove_metadata(service_name)
    if str(meta.get("family", "") or "") == "AF_UNIX":
        with contextlib.suppress(OSError):
            Path(str(meta.get("address", "") or "")).unlink(missing_ok=True)


def _pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import subprocess

        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {int(pid)}"],
            capture_output=True,
            text=True,
            check=False,
        )
        return str(int(pid)) in str(result.stdout or "")
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def _terminate_pid(pid: int, *, force: bool = False) -> None:
    if pid <= 0 or not _pid_running(pid):
        return
    if os.name == "nt":
        import subprocess

        cmd = ["taskkill", "/PID", str(int(pid))]
        if force:
            cmd.insert(1, "/F")
        subprocess.run(cmd, check=False, capture_output=True, text=True)
        return
    sig = signal.SIGKILL if force else signal.SIGTERM
    with contextlib.suppress(ProcessLookupError):
        os.kill(int(pid), sig)


def iter_local_service_metadata() -> list[Dict[str, object]]:
    rows: list[Dict[str, object]] = []
    for path in sorted(_registry_dir().glob("*.json")):
        try:
            meta = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
        if not isinstance(meta, dict):
            meta = {}
        meta["_path"] = str(path)
        service_name = str(meta.get("service_name", "") or "").strip()
        if not service_name:
            meta["service_name"] = path.stem
        rows.append(meta)
    return rows


def inspect_local_services(*, timeout_sec: float = 0.5) -> list[Dict[str, object]]:
    rows: list[Dict[str, object]] = []
    for meta in iter_local_service_metadata():
        service_name = str(meta.get("service_name", "") or "").strip()
        pid = int(meta.get("pid", 0) or 0)
        pid_running = _pid_running(pid)
        ping_ok = False
        error = ""
        if service_name:
            try:
                resp = _call_once(meta, {"action": "ping"}, timeout_sec=timeout_sec)
                ping_ok = bool(resp.get("ok", False))
            except Exception as exc:
                error = str(exc) or repr(exc)
        row = dict(meta)
        row["pid"] = pid
        row["pid_running"] = pid_running
        row["alive"] = bool(pid_running and ping_ok)
        row["ping_ok"] = ping_ok
        row["error"] = error
        rows.append(row)
    return rows


def cleanup_stale_local_services(*, timeout_sec: float = 0.5) -> list[Dict[str, object]]:
    removed: list[Dict[str, object]] = []
    for row in inspect_local_services(timeout_sec=timeout_sec):
        if bool(row.get("alive", False)):
            continue
        service_name = str(row.get("service_name", "") or "").strip()
        if service_name:
            _discard_metadata_if_current(service_name, row)
        removed.append(dict(row))
    return removed


def stop_local_service(
    service_name: str,
    *,
    timeout_sec: float = 3.0,
    force: bool = False,
    cleanup: bool = True,
) -> Dict[str, object]:
    meta = _read_metadata(service_name)
    pid = int(meta.get("pid", 0) or 0)
    current_pid = int(os.getpid())
    stopped_by = ""
    error = ""
    with contextlib.suppress(Exception):
        resp = _call_once(meta, {"action": "shutdown"}, timeout_sec=min(1.0, max(0.1, timeout_sec)))
        if bool(resp.get("ok", False)):
            stopped_by = "ipc"
    deadline = time.monotonic() + max(0.1, float(timeout_sec))
    while (
        pid > 0
        and pid != current_pid
        and _pid_running(pid)
        and time.monotonic() < deadline
    ):
        time.sleep(0.05)
    if pid > 0 and pid != current_pid and _pid_running(pid):
        try:
            _terminate_pid(pid, force=False)
            stopped_by = stopped_by or "sigterm"
        except Exception as exc:
            error = str(exc) or repr(exc)
    deadline = time.monotonic() + max(0.1, float(timeout_sec))
    while (
        pid > 0
        and pid != current_pid
        and _pid_running(pid)
        and time.monotonic() < deadline
    ):
        time.sleep(0.05)
    if force and pid > 0 and pid != current_pid and _pid_running(pid):
        try:
            _terminate_pid(pid, force=True)
            stopped_by = "sigkill"
        except Exception as exc:
            error = str(exc) or repr(exc)
    if cleanup:
        _discard_metadata_if_current(service_name, meta)
    pid_still_running = pid > 0 and pid != current_pid and _pid_running(pid)
    return {
        "service_name": service_name,
        "pid": pid,
        "stopped": not pid_still_running,
        "stopped_by": stopped_by,
        "error": error,
    }


def _authkey_from_metadata(meta: Dict[str, object]) -> Optional[bytes]:
    raw = str(meta.get("authkey", "") or "").strip()
    if not raw:
        return None
    return base64.b64decode(raw.encode("ascii"))


def _connect_local_service(meta: Dict[str, object]):
    address = str(meta.get("address", "") or "")
    family = str(meta.get("family", "") or "")
    if not address or not family:
        raise RuntimeError("local service metadata is incomplete")
    return Client(address, family=family, authkey=_authkey_from_metadata(meta))


def _local_service_stale_reason(meta: Dict[str, object]) -> str:
    pid = int(meta.get("pid", 0) or 0)
    if pid > 0 and not _pid_running(pid):
        return f"registered process pid={pid} is not running"
    family = str(meta.get("family", "") or "")
    address = str(meta.get("address", "") or "")
    if family == "AF_UNIX" and address and not Path(address).exists():
        return f"registered IPC socket does not exist: {address}"
    if not family or not address:
        return "local service metadata is incomplete"
    return ""


def _call_once(meta: Dict[str, object], request: Dict[str, object], *, timeout_sec: float = 5.0) -> Dict[str, object]:
    conn = _connect_local_service(meta)
    try:
        conn.send(dict(request or {}))
        if not conn.poll(max(0.1, float(timeout_sec or 5.0))):
            raise TimeoutError("local service IPC request timed out")
        response = conn.recv()
    finally:
        conn.close()
    if not isinstance(response, dict):
        raise RuntimeError(f"invalid local service IPC response: {type(response).__name__}")
    return response


def _stream_once(meta: Dict[str, object], request: Dict[str, object], *, timeout_sec: float = 5.0):
    address = str(meta.get("address", "") or "")
    family = str(meta.get("family", "") or "")
    if not address or not family:
        raise RuntimeError("local service metadata is incomplete")
    conn = Client(address, family=family, authkey=_authkey_from_metadata(meta))
    try:
        conn.send(dict(request or {}))
        while True:
            if not conn.poll(max(0.1, float(timeout_sec or 5.0))):
                raise TimeoutError("local service IPC stream timed out")
            event = conn.recv()
            if not isinstance(event, dict):
                raise RuntimeError(f"invalid local service IPC stream event: {type(event).__name__}")
            yield event
            if str(event.get("event", "") or "") == "done":
                return
    finally:
        conn.close()


def _is_metadata_alive(service_name: str, *, timeout_sec: float = 1.0) -> bool:
    try:
        meta = _read_metadata(service_name)
        resp = _call_once(meta, {"action": "ping"}, timeout_sec=timeout_sec)
        return bool(resp.get("ok", False))
    except Exception:
        return False


class LocalServiceIpcServer:
    def __init__(self, *, node: Any, service_name: str) -> None:
        self.node = node
        self.service_name = str(service_name or "").strip()
        self.address, self.family = _local_service_address(self.service_name)
        self.authkey = os.urandom(32) if _local_ipc_auth_enabled() else None
        self.ipc_token = uuid.uuid4().hex
        self._listener: Optional[Listener] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if _is_metadata_alive(self.service_name):
            meta = {}
            with contextlib.suppress(Exception):
                meta = _read_metadata(self.service_name)
            pid = int(meta.get("pid", 0) or 0) if isinstance(meta, dict) else 0
            raise RuntimeError(
                f"local service_name already exists: {self.service_name}\n"
                f"pid={pid or '-'}\n"
                f"stop with: pycloudctl stop-local-service {self.service_name}"
            )
        _remove_metadata(self.service_name)
        if self.family == "AF_UNIX":
            Path(self.address).unlink(missing_ok=True)
        self._listener = Listener(
            self.address,
            family=self.family,
            backlog=LOCAL_IPC_BACKLOG,
            authkey=self.authkey,
        )
        _write_metadata(
            self.service_name,
            {
                "version": _REGISTRY_VERSION,
                "service_name": self.service_name,
                "pid": os.getpid(),
                "address": self.address,
                "family": self.family,
                "authkey": base64.b64encode(self.authkey).decode("ascii") if self.authkey is not None else "",
                "ipc_token": self.ipc_token,
                "node_id": str(getattr(self.node, "node_id", "") or ""),
                "node_instance_id": str(getattr(self.node, "node_instance_id", "") or ""),
                "object_dir": str(getattr(self.node, "object_dir", "") or ""),
            },
        )
        self._thread = threading.Thread(
            target=self._serve,
            name=f"local-service-ipc-{self.service_name}",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        listener = self._listener
        if listener is not None:
            with contextlib.suppress(Exception):
                listener.close()
        owns_metadata = _metadata_token(self.service_name) == self.ipc_token
        if owns_metadata:
            _remove_metadata(self.service_name)
        if owns_metadata and self.family == "AF_UNIX":
            Path(self.address).unlink(missing_ok=True)
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1.0)

    def _serve(self) -> None:
        listener = self._listener
        if listener is None:
            return
        while not self._stop.is_set():
            try:
                conn = listener.accept()
            except (OSError, EOFError):
                if self._stop.is_set():
                    return
                continue
            threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()

    def _handle_conn(self, conn: Any) -> None:
        try:
            while True:
                request = conn.recv()
                if not isinstance(request, dict):
                    raise ValueError("request must be a dict")
                if str(request.get("action", "") or "").strip() == "stream_call":
                    self._handle_stream_request(conn, request)
                    return
                response = self._handle_request(request)
                conn.send(response)
        except Exception as exc:
            with contextlib.suppress(Exception):
                conn.send({"ok": False, "error": str(exc) or repr(exc), "error_type": exc.__class__.__name__})
        finally:
            with contextlib.suppress(Exception):
                conn.close()

    def _handle_request(self, request: Dict[str, object]) -> Dict[str, object]:
        action = str(request.get("action", "") or "").strip()
        if action == "ping":
            return {"ok": True, "pid": os.getpid(), "service_name": self.service_name}
        if action == "shutdown":
            threading.Thread(target=self.node.close, name=f"local-service-shutdown-{self.service_name}", daemon=True).start()
            return {"ok": True, "pid": os.getpid(), "service_name": self.service_name, "stopping": True}
        if action == "list_methods":
            methods = [
                {"method": str(name), "qualified_name": str(name), "doc": ""}
                for name in getattr(self.node, "methods", [])
            ]
            return {"ok": True, "methods": methods}
        if action == "get_status":
            service_id = str(getattr(self.node, "_local_service_id", "") or "")
            session = getattr(self.node, "_services", {}).get(service_id) if service_id else None
            worker_count = max(1, int(getattr(session, "worker_count", 0) or getattr(self.node, "service_default_worker_count", 1) or 1))
            policy_id = str(getattr(session, "policy_id", "") or "trusted_internal")
            return {
                "ok": True,
                "service_name": self.service_name,
                "route_count": 1,
                "routes": [
                    {
                        "service_name": self.service_name,
                        "node_id": str(getattr(self.node, "node_id", "") or ""),
                        "node_instance_id": str(getattr(self.node, "node_instance_id", "") or ""),
                        "control_addr": "local",
                        "worker_count": worker_count,
                        "alive_workers": worker_count,
                        "policy_id": policy_id,
                    }
                ],
            }
        if action == "call":
            payload = request.get("payload_transport")
            if payload is None:
                payload = dict(request.get("payload", {}) or {})
            _node_key, body = self.node.call_balanced(
                str(request.get("method", "") or ""),
                payload,
                timeout_sec=max(0.1, float(request.get("timeout_sec", 60.0) or 60.0)),
                serialization_mode=str(request.get("serialization_mode", "") or ""),
            )
            return {"ok": True, "response": body}
        if action == "fetch_result_data":
            return {"ok": True, "data": self.node.fetch_result_data(request.get("value"))}
        raise ValueError(f"unsupported local service IPC action: {action}")

    def _handle_stream_request(self, conn: Any, request: Dict[str, object]) -> None:
        item_count = 0
        try:
            payload = request.get("payload_transport")
            if payload is None:
                payload = dict(request.get("payload", {}) or {})
            for item in self.node.stream_call(
                str(request.get("method", "") or ""),
                payload,
                timeout_sec=max(0.1, float(request.get("timeout_sec", 60.0) or 60.0)),
                serialization_mode=str(request.get("serialization_mode", "") or ""),
            ):
                conn.send({"event": "item", "index": item_count, "data": item})
                item_count += 1
            conn.send({"event": "done", "ok": True, "item_count": item_count})
        except Exception as exc:
            conn.send(
                {
                    "event": "done",
                    "ok": False,
                    "item_count": item_count,
                    "error_type": exc.__class__.__name__,
                    "error": str(exc) or repr(exc),
                }
            )


class LocalServiceClient:
    def __init__(self, *, service_name: str, timeout_sec: float = 10.0) -> None:
        self.service_name = str(service_name or "").strip()
        self.timeout_sec = max(0.1, float(timeout_sec or 10.0))
        self._conn_lock = threading.Lock()
        self._thread_conns: dict[int, tuple[str, Any]] = {}
        deadline = time.monotonic() + self.timeout_sec
        last_exc: Optional[BaseException] = None
        while True:
            try:
                candidate_meta = _read_metadata(self.service_name)
            except FileNotFoundError as exc:
                raise RuntimeError(
                    f"local service IPC unavailable for service_name={self.service_name!r}; "
                    "local service registry entry was not found"
                ) from exc

            stale_reason = _local_service_stale_reason(candidate_meta)
            if stale_reason:
                _discard_metadata_if_current(self.service_name, candidate_meta)
                raise RuntimeError(
                    f"local service IPC unavailable for service_name={self.service_name!r}; "
                    f"{stale_reason}"
                )

            try:
                response = _call_once(candidate_meta, {"action": "ping"}, timeout_sec=min(1.0, self.timeout_sec))
                if not bool(response.get("ok", False)):
                    raise RuntimeError(str(response.get("error", "local service IPC ping failed")))
                self._meta = candidate_meta
                break
            except Exception as exc:
                last_exc = exc
                with contextlib.suppress(Exception):
                    _discard_metadata_if_current(self.service_name, candidate_meta)
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"local service IPC unavailable for service_name={self.service_name!r}; "
                        "local service registry entry exists but the service is not accepting connections"
                    ) from last_exc
                time.sleep(0.05)

    def _close_thread_conn(self, thread_id: int) -> None:
        with self._conn_lock:
            entry = self._thread_conns.pop(thread_id, None)
        if entry is not None:
            with contextlib.suppress(Exception):
                entry[1].close()

    def _get_thread_conn(self) -> Any:
        thread_id = threading.get_ident()
        current_token = str(self._meta.get("ipc_token", "") or "")
        with self._conn_lock:
            entry = self._thread_conns.get(thread_id)
            if entry is not None and entry[0] == current_token:
                return entry[1]
        self._close_thread_conn(thread_id)
        conn = _connect_local_service(self._meta)
        with self._conn_lock:
            self._thread_conns[thread_id] = (current_token, conn)
        return conn

    def _request_via_connection(self, conn: Any, request: Dict[str, object], *, timeout_sec: float) -> Dict[str, object]:
        conn.send(dict(request or {}))
        if not conn.poll(max(0.1, float(timeout_sec or self.timeout_sec))):
            raise TimeoutError("local service IPC request timed out")
        response = conn.recv()
        if not isinstance(response, dict):
            raise RuntimeError(f"invalid local service IPC response: {type(response).__name__}")
        return response

    def _request(self, action: str, **kwargs: object) -> Dict[str, object]:
        request = {"action": action, **kwargs}
        deadline = time.monotonic() + self.timeout_sec
        first_exc: Optional[BaseException] = None
        while True:
            try:
                response = self._request_via_connection(self._get_thread_conn(), request, timeout_sec=self.timeout_sec)
                break
            except Exception as exc:
                if first_exc is None:
                    first_exc = exc
                self._close_thread_conn(threading.get_ident())
                previous_meta = dict(self._meta)
                try:
                    refreshed_meta = _read_metadata(self.service_name)
                except Exception:
                    _discard_metadata_if_current(self.service_name, previous_meta)
                    raise RuntimeError(
                        f"local service IPC unavailable for service_name={self.service_name!r}; "
                        "the local service process is not running"
                    ) from first_exc
                self._meta = refreshed_meta
                if str(refreshed_meta.get("ipc_token", "") or "") == str(previous_meta.get("ipc_token", "") or ""):
                    if time.monotonic() < deadline:
                        time.sleep(LOCAL_IPC_CONNECT_RETRY_INTERVAL_SEC)
                        continue
                    _discard_metadata_if_current(self.service_name, previous_meta)
                    raise RuntimeError(
                        f"local service IPC unavailable for service_name={self.service_name!r}; "
                        "removed stale local service registry entry after repeated connection failures"
                    ) from first_exc
                if time.monotonic() >= deadline:
                    _discard_metadata_if_current(self.service_name, self._meta)
                    raise RuntimeError(
                        f"local service IPC unavailable for service_name={self.service_name!r}; "
                        "the replacement local service process is not accepting connections"
                    ) from first_exc
        if not bool(response.get("ok", False)):
            raise RuntimeError(str(response.get("error", "local service IPC request failed")))
        return response

    def close(self) -> None:
        with self._conn_lock:
            thread_ids = list(self._thread_conns.keys())
        for thread_id in thread_ids:
            self._close_thread_conn(thread_id)

    def list_methods(self, *, service_name: str = "", include_docs: bool = False) -> list[Dict[str, object]]:
        del service_name, include_docs
        return list(self._request("list_methods").get("methods", []) or [])

    def get_status(self, *, service_name: str = "") -> Dict[str, object]:
        del service_name
        return self._request("get_status")

    def call(
        self,
        *,
        service_name: str = "",
        method: str,
        payload: Dict[str, object],
        timeout_sec: float = 60.0,
        serialization_mode: str = "",
        **kwargs: object,
    ) -> Dict[str, object]:
        del service_name, kwargs
        payload_transport = _make_local_pickle_payload_transport(
            payload,
            meta=self._meta,
            serialization_mode=serialization_mode,
        )
        response = self._request(
            "call",
            method=method,
            payload_transport=payload_transport,
            timeout_sec=max(0.1, float(timeout_sec or self.timeout_sec)),
            serialization_mode=serialization_mode,
        )
        body = response.get("response")
        if not isinstance(body, dict):
            raise RuntimeError("local service IPC returned invalid call response")
        if not bool(body.get("ok", False)):
            raise RuntimeError(str(body.get("error", "local service call failed")))
        return body

    def stream_call(
        self,
        *,
        service_name: str = "",
        method: str,
        payload: Dict[str, object],
        timeout_sec: float = 60.0,
        serialization_mode: str = "",
        **kwargs: object,
    ):
        del service_name, kwargs
        payload_transport = _make_local_pickle_payload_transport(
            payload,
            meta=self._meta,
            serialization_mode=serialization_mode,
        )
        request = {
            "action": "stream_call",
            "method": method,
            "payload_transport": payload_transport,
            "timeout_sec": max(0.1, float(timeout_sec or self.timeout_sec)),
            "serialization_mode": serialization_mode,
        }
        try:
            yield from _stream_once(self._meta, request, timeout_sec=max(self.timeout_sec, float(timeout_sec or 0.0)))
        except Exception as first_exc:
            previous_meta = dict(self._meta)
            try:
                refreshed_meta = _read_metadata(self.service_name)
            except Exception:
                _discard_metadata_if_current(self.service_name, previous_meta)
                raise RuntimeError(
                    f"local service IPC unavailable for service_name={self.service_name!r}; "
                    "the local service process is not running"
                ) from first_exc
            self._meta = refreshed_meta
            if str(refreshed_meta.get("ipc_token", "") or "") == str(previous_meta.get("ipc_token", "") or ""):
                _discard_metadata_if_current(self.service_name, previous_meta)
                raise RuntimeError(
                    f"local service IPC unavailable for service_name={self.service_name!r}; "
                    "removed stale local service registry entry"
                ) from first_exc
            try:
                yield from _stream_once(self._meta, request, timeout_sec=max(self.timeout_sec, float(timeout_sec or 0.0)))
            except Exception as second_exc:
                _discard_metadata_if_current(self.service_name, self._meta)
                raise RuntimeError(
                    f"local service IPC unavailable for service_name={self.service_name!r}; "
                    "the replacement local service process is not accepting connections"
                ) from second_exc

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        if target_path:
            raise ValueError("local service IPC fetch_result_data does not support target_path")
        ref = maybe_data_ref(response_or_data)
        if ref is None and isinstance(response_or_data, dict):
            ref = maybe_data_ref(response_or_data.get("data"))
            nested = response_or_data.get("data")
            if ref is None and isinstance(nested, dict):
                ref = maybe_data_ref(nested.get("data"))
        if ref is None:
            if isinstance(response_or_data, dict) and "data" in response_or_data:
                return response_or_data["data"]
            return response_or_data
        object_dir = str(self._meta.get("object_dir", "") or "").strip()
        if not object_dir:
            raise RuntimeError(
                f"local service IPC metadata for service_name={self.service_name!r} has no object_dir; "
                "cannot materialize local DataRef directly"
            )
        return _resolve_single_data_ref(ref, object_dir=object_dir)

    def download_result_to_file(self, response_or_data: object, *, target_path: str):
        data = self.fetch_result_data(response_or_data)
        path = Path(target_path)
        if isinstance(data, (bytes, bytearray, memoryview)):
            path.write_bytes(bytes(data))
        else:
            path.write_text(str(data), encoding="utf-8")
        return path


def start_local_service_ipc(*, node: Any, service_name: str) -> LocalServiceIpcServer:
    server = LocalServiceIpcServer(node=node, service_name=service_name)
    server.start()
    return server


__all__ = [
    "LocalServiceClient",
    "LocalServiceIpcServer",
    "cleanup_stale_local_services",
    "inspect_local_services",
    "local_service_metadata_path",
    "start_local_service_ipc",
    "stop_local_service",
]
