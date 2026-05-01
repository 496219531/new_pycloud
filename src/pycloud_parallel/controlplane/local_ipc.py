from __future__ import annotations

"""Local service IPC registry and transport."""

import base64
import contextlib
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import uuid
from multiprocessing.connection import Client, Listener
from typing import Any, Dict, Optional


_REGISTRY_VERSION = 1


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


def _authkey_from_metadata(meta: Dict[str, object]) -> bytes:
    return base64.b64decode(str(meta.get("authkey", "") or "").encode("ascii"))


def _call_once(meta: Dict[str, object], request: Dict[str, object], *, timeout_sec: float = 5.0) -> Dict[str, object]:
    address = str(meta.get("address", "") or "")
    family = str(meta.get("family", "") or "")
    if not address or not family:
        raise RuntimeError("local service metadata is incomplete")
    conn = Client(address, family=family, authkey=_authkey_from_metadata(meta))
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
        self.authkey = os.urandom(32)
        self.ipc_token = uuid.uuid4().hex
        self._listener: Optional[Listener] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if _is_metadata_alive(self.service_name):
            raise RuntimeError(f"local service_name already exists: {self.service_name}")
        _remove_metadata(self.service_name)
        if self.family == "AF_UNIX":
            Path(self.address).unlink(missing_ok=True)
        self._listener = Listener(self.address, family=self.family, authkey=self.authkey)
        _write_metadata(
            self.service_name,
            {
                "version": _REGISTRY_VERSION,
                "service_name": self.service_name,
                "pid": os.getpid(),
                "address": self.address,
                "family": self.family,
                "authkey": base64.b64encode(self.authkey).decode("ascii"),
                "ipc_token": self.ipc_token,
                "node_id": str(getattr(self.node, "node_id", "") or ""),
                "node_instance_id": str(getattr(self.node, "node_instance_id", "") or ""),
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
            request = conn.recv()
            if not isinstance(request, dict):
                raise ValueError("request must be a dict")
            if str(request.get("action", "") or "").strip() == "stream_call":
                self._handle_stream_request(conn, request)
                return
            response = self._handle_request(request)
        except Exception as exc:
            response = {"ok": False, "error": str(exc) or repr(exc), "error_type": exc.__class__.__name__}
        try:
            conn.send(response)
        finally:
            with contextlib.suppress(Exception):
                conn.close()

    def _handle_request(self, request: Dict[str, object]) -> Dict[str, object]:
        action = str(request.get("action", "") or "").strip()
        if action == "ping":
            return {"ok": True, "pid": os.getpid(), "service_name": self.service_name}
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
            _node_key, body = self.node.call_balanced(
                str(request.get("method", "") or ""),
                dict(request.get("payload", {}) or {}),
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
            for item in self.node.stream_call(
                str(request.get("method", "") or ""),
                dict(request.get("payload", {}) or {}),
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
        deadline = time.monotonic() + self.timeout_sec
        last_exc: Optional[BaseException] = None
        while True:
            try:
                self._meta = _read_metadata(self.service_name)
                break
            except FileNotFoundError as exc:
                last_exc = exc
                if time.monotonic() >= deadline:
                    raise last_exc
                time.sleep(0.05)

    def _request(self, action: str, **kwargs: object) -> Dict[str, object]:
        try:
            response = _call_once(self._meta, {"action": action, **kwargs}, timeout_sec=self.timeout_sec)
        except Exception:
            self._meta = _read_metadata(self.service_name)
            response = _call_once(self._meta, {"action": action, **kwargs}, timeout_sec=self.timeout_sec)
        if not bool(response.get("ok", False)):
            raise RuntimeError(str(response.get("error", "local service IPC request failed")))
        return response

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
        response = self._request(
            "call",
            method=method,
            payload=dict(payload or {}),
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
        request = {
            "action": "stream_call",
            "method": method,
            "payload": dict(payload or {}),
            "timeout_sec": max(0.1, float(timeout_sec or self.timeout_sec)),
            "serialization_mode": serialization_mode,
        }
        try:
            yield from _stream_once(self._meta, request, timeout_sec=max(self.timeout_sec, float(timeout_sec or 0.0)))
        except Exception:
            self._meta = _read_metadata(self.service_name)
            yield from _stream_once(self._meta, request, timeout_sec=max(self.timeout_sec, float(timeout_sec or 0.0)))

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        if target_path:
            raise ValueError("local service IPC fetch_result_data does not support target_path")
        return self._request("fetch_result_data", value=response_or_data).get("data")

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
    "local_service_metadata_path",
    "start_local_service_ipc",
]
