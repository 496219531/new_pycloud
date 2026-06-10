from __future__ import annotations

"""HTTP implementation for the core NodeControl TaskPool and Service APIs."""

import base64
import contextlib
import json
import logging
import os
import secrets
import subprocess
import sys
import tempfile
import threading
import time
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple, Union
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen

from google.protobuf import json_format

from pycloud_parallel.controlplane.artifact import (
    ArtifactDeps,
    _coerce_artifact_deps,
    _default_entry_module_for_package,
    _normalize_dependency_policy_mode,
    _resolve_package_format,
)
from pycloud_parallel.controlplane.client_transport import _materialize_downloaded_result, _normalize_http_response_body
from pycloud_parallel.controlplane.client_transport_runtime import (
    RuntimeTransportRequest,
    pack_binary_sidecar,
    runtime_http_request,
    runtime_http_request_for_binary_sidecar_response,
    unpack_binary_sidecar,
)
from pycloud_parallel.controlplane.config import (
    OBJECT_CHUNK_SIZE_BYTES,
    get_node_control_http_body_limit_bytes,
    get_payload_policy,
)
from pycloud_parallel.controlplane.http_client import target_to_base_url
from pycloud_parallel.controlplane.http_gateway import StreamingHttpResponse
from pycloud_parallel.controlplane.node.models import ServiceSession, TaskPoolState
from pycloud_parallel.controlplane.node_object_http import (
    HttpNodeObjectClient,
    NodeObjectHttpApp,
)
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.controlplane.payload_transport import decode_payload_from_transport, decode_result_from_transport, encode_payload_for_transport
from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient, ServiceSessionClient
from pycloud_parallel.controlplane.serialization import (
    decode_transport_payload_bytes,
    detect_transport_mode,
    dict_to_struct,
    encode_transport_payload_bytes,
    log_payload_flow,
    serialize_arrow_compatible,
    serialize_inline_payload,
    struct_to_python,
    summarize_payload_flow_value,
)
from pycloud_parallel.controlplane.serialization_mode import resolve_effective_serialization_mode
from pycloud_parallel.controlplane.state_time import utc_now
from pycloud_parallel.data.ref import DataRef, maybe_data_ref
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


logger = logging.getLogger(__name__)
MAX_NODE_CONTROL_HTTP_BODY_BYTES = get_node_control_http_body_limit_bytes()


def _is_client_disconnect_error(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)):
        return True
    text = repr(exc).lower()
    return (
        "broken pipe" in text
        or "connectionabortederror" in text
        or "connectionreseterror" in text
        or "winerror 10053" in text
        or "winerror 10054" in text
    )


def _restart_current_process_delayed(delay_sec: float = 1.0) -> None:
    def _restart() -> None:
        time.sleep(max(0.1, float(delay_sec)))
        args = [sys.executable, *sys.argv]
        try:
            if os.name == "nt":
                creationflags = int(getattr(subprocess, "CREATE_NEW_CONSOLE", 0) or 0)
                subprocess.Popen(args, cwd=os.getcwd(), env=os.environ.copy(), close_fds=False, creationflags=creationflags)
            else:
                subprocess.Popen(args, cwd=os.getcwd(), env=os.environ.copy(), close_fds=True)
        finally:
            os._exit(0)

    threading.Thread(target=_restart, name="nodecontrol-delayed-restart", daemon=True).start()
def _split_host_port(bind: str) -> Tuple[str, int]:
    if ":" not in bind:
        raise ValueError("bind must be host:port")
    host, port = bind.rsplit(":", 1)
    return host.strip(), int(port)


def _json_bytes(data: Dict[str, object]) -> bytes:
    return json.dumps(serialize_arrow_compatible(data), ensure_ascii=False).encode("utf-8")


def _read_json(body: bytes) -> Dict[str, object]:
    try:
        parsed = json.loads(body.decode("utf-8") if body else "{}")
    except Exception as exc:
        raise ValueError("invalid json body") from exc
    if not isinstance(parsed, dict):
        raise ValueError("json body must be object")
    return parsed

def _transport_payload_meta(value: pb2.TransportPayload) -> Dict[str, object]:
    return {
        "codec": str(value.codec or ""),
        "version": int(value.version or 0),
        "payload_size": len(bytes(value.payload or b"")),
    }


def _message_to_dict(message) -> Dict[str, object]:
    try:
        return json_format.MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=False,
        )
    except TypeError:
        return json_format.MessageToDict(
            message,
            preserving_proto_field_name=True,
        )


def _parse_message(message_cls, payload: object):
    msg = message_cls()
    json_format.ParseDict(dict(payload or {}), msg, ignore_unknown_fields=True)
    return msg


def _decode_initial_globals(payload: Dict[str, object], *, context: str) -> Dict[str, object]:
    raw_values = payload.get("initial_globals")
    if not isinstance(raw_values, dict) or not raw_values:
        return {}
    serialization_mode = detect_transport_mode(
        raw_values,
        default=str(payload.get("initial_globals_mode", "") or "legacy_v1"),
    )
    values = decode_payload_from_transport(
        raw_values,
        policy=get_payload_policy("managed_globals"),
        mode=serialization_mode,
        context=context,
    )
    return values if isinstance(values, dict) else {}


def _task_pool_status_to_dict(info: Dict[str, object]) -> Dict[str, object]:
    return {
        "pool_id": str(info.get("pool_id", "")),
        "owner_client_id": str(info.get("owner_client_id", "")),
        "pool_name": str(info.get("pool_name", "")),
        "code_version": str(info.get("code_version", "")),
        "worker_count": int(info.get("worker_count", 0) or 0),
        "alive_workers": int(info.get("alive_workers", 0) or 0),
        "heartbeat_timeout_sec": int(info.get("heartbeat_timeout_sec", 0) or 0),
        "status": str(info.get("status", "")),
        "resource_health": str(info.get("resource_health", "") or ""),
        "degraded": bool(info.get("degraded", False)),
        "task_count": int(info.get("task_count", 0) or 0),
        "received_count": int(info.get("received_count", info.get("task_count", 0)) or 0),
        "returned_count": int(info.get("returned_count", 0) or 0),
        "inflight": int(info.get("inflight", 0) or 0),
        "created_at": info["created_at"].isoformat(),
        "last_heartbeat_at": info["last_heartbeat_at"].isoformat(),
        "lease_expire_at": info["lease_expire_at"].isoformat(),
        "stop_reason": str(info.get("stop_reason", info.get("failure_reason", "")) or ""),
        "failure_reason": str(info.get("failure_reason", "") or ""),
        "failure_at": info["failure_at"].isoformat() if info.get("failure_at") is not None else "",
    }


def _service_status_to_dict(info: Dict[str, object]) -> Dict[str, object]:
    return {
        "service_id": str(info.get("service_id", "")),
        "owner_client_id": str(info.get("owner_client_id", "")),
        "service_name": str(info.get("service_name", "")),
        "policy_id": str(info.get("policy_id", "") or "default_safe"),
        "code_version": str(info.get("code_version", "")),
        "status": int(info.get("status", pb2.SERVICE_STATUS_UNSPECIFIED)),
        "worker_count": int(info.get("worker_count", 0) or 0),
        "alive_workers": int(info.get("alive_workers", 0) or 0),
        "in_flight": int(info.get("in_flight", 0) or 0),
        "queued": int(info.get("queued", 0) or 0),
        "created_at": info["created_at"].isoformat(),
        "last_heartbeat_at": info["last_heartbeat_at"].isoformat(),
        "lease_expire_at": info["lease_expire_at"].isoformat(),
        "http_base_url": str(info.get("http_base_url", "")),
    }


def _created_task_pool_response(pool: TaskPoolState) -> Dict[str, object]:
    return {
        "ok": True,
        "pool_id": pool.pool_id,
        "code_version": pool.code_version,
        "worker_count": pool.worker_count,
        "heartbeat_timeout_sec": pool.heartbeat_timeout_sec,
        "owner_client_id": pool.owner_client_id,
        "pool_token": pool.pool_token,
    }


def _created_service_response(session: ServiceSession) -> Dict[str, object]:
    return {
        "ok": True,
        "service_id": session.service_id,
        "code_version": session.code_version,
        "status": int(session.status),
        "worker_count": session.worker_count,
        "heartbeat_timeout_sec": session.heartbeat_timeout_sec,
        "owner_client_id": session.owner_client_id,
        "service_token": session.service_token,
        "http_base_url": session.http_base_url,
        "policy_id": str(session.policy_id or "").strip().lower() or "default_safe",
    }


class NodeControlHttpApp:
    def __init__(
        self,
        state: NodeControlState,
        *,
        on_service_routes_changed=None,
        max_body_bytes: int = MAX_NODE_CONTROL_HTTP_BODY_BYTES,
        api_token: str = "",
    ) -> None:
        self.state = state
        self.on_service_routes_changed = on_service_routes_changed
        self.max_body_bytes = get_node_control_http_body_limit_bytes(max_body_bytes)
        self.object_app = NodeObjectHttpApp(state, max_body_bytes=self.max_body_bytes)
        self.api_token = str(api_token or "").strip()

    def _notify(self) -> None:
        if self.on_service_routes_changed is None:
            return
        self.on_service_routes_changed()

    def _configured_api_token(self) -> str:
        return self.api_token

    def _admin_token_path(self) -> Path:
        return self.state.artifact_dir / "admin_token"

    def _configured_admin_token(self) -> str:
        path = self._admin_token_path()
        try:
            return path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return ""

    def _write_admin_token(self, token: str) -> None:
        normalized = str(token or "").strip()
        if not normalized:
            raise ValueError("admin_token must not be empty")
        path = self._admin_token_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
        tmp.write_text(normalized + "\n", encoding="utf-8")
        try:
            os.replace(str(tmp), str(path))
        finally:
            if tmp.exists():
                with contextlib.suppress(OSError):
                    tmp.unlink()

    @staticmethod
    def _provided_api_token(headers) -> str:
        if not headers:
            return ""
        auth = str(headers.get("Authorization", "") or headers.get("authorization", "") or "").strip()
        if auth.lower().startswith("bearer "):
            return auth[7:].strip()
        return str(
            headers.get("X-PyCloud-Api-Token", "")
            or headers.get("x-pycloud-api-token", "")
            or ""
        ).strip()

    def _require_create_api_token(self, headers) -> Optional[Tuple[int, Dict[str, str], bytes]]:
        expected = self._configured_api_token()
        if not expected:
            return None
        provided = self._provided_api_token(headers)
        if not provided:
            return self._err(401, "owner api token is required to create service/taskpool resources")
        if not secrets.compare_digest(provided, expected):
            return self._err(403, "invalid owner api token for resource creation")
        return None

    def _require_admin_token(self, headers) -> Optional[Tuple[int, Dict[str, str], bytes]]:
        expected = self._configured_admin_token()
        if not expected:
            return None
        provided = self._provided_api_token(headers)
        if not provided:
            return self._err(401, "admin token is required")
        if not secrets.compare_digest(provided, expected):
            return self._err(403, "invalid admin token")
        return None

    @staticmethod
    def _is_resource_create_path(path: str) -> bool:
        parts = [unquote(x) for x in urlparse(path).path.split("/") if x]
        return parts == ["taskpools"] or parts == ["services"]

    @staticmethod
    def _is_admin_path(path: str) -> bool:
        parts = [unquote(x) for x in urlparse(path).path.split("/") if x]
        return bool(parts and parts[0] == "admin")

    def handle_get(self, path: str) -> Union[Tuple[int, Dict[str, str], bytes], StreamingHttpResponse]:
        parsed = urlparse(path)
        parts = [unquote(x) for x in parsed.path.split("/") if x]
        if parts and parts[0] == "objects":
            return self.object_app.handle_get(path)
        try:
            if parts == ["node", "status"]:
                snapshot = self.state.registrar_snapshot(include_stopped=True)
                return self._ok(
                    {
                        "ok": True,
                        "node": {
                            "node_id": str(getattr(self.state, "node_id", "") or ""),
                            "node_instance_id": str(getattr(self.state, "node_instance_id", "") or ""),
                            "execution_fenced": bool(snapshot.get("execution_fenced", False)),
                            "accept_service_deploy": bool(snapshot.get("accept_service_deploy", True)),
                            "execution_fenced_reason": str(snapshot.get("execution_fenced_reason", "") or ""),
                            "execution_fenced_at": str(snapshot.get("execution_fenced_at", "") or ""),
                            "metrics": dict(snapshot.get("metrics") or {}),
                            "active_runtimes": list(snapshot.get("active_runtimes") or []),
                            "service_count": len(list(snapshot.get("service_reports") or [])),
                            "task_pool_count": len(list(snapshot.get("task_pool_reports") or [])),
                            "service_worker_capacity": int(snapshot.get("service_worker_capacity", 0) or 0),
                            "service_worker_used": int(snapshot.get("service_worker_used", 0) or 0),
                            "task_pool_worker_capacity": int(snapshot.get("task_pool_worker_capacity", 0) or 0),
                            "task_pool_worker_used": int(snapshot.get("task_pool_worker_used", 0) or 0),
                        },
                    }
                )
            if len(parts) == 2 and parts[0] == "taskpools" and parts[1]:
                info = self.state.task_pool_status_info(parts[1])
                return self._ok({"ok": True, "pool": _task_pool_status_to_dict(info)})
            if len(parts) == 3 and parts[0] == "services" and parts[2] == "methods":
                include_docs = "include_docs=true" in str(parsed.query).lower()
                methods = self.state.list_service_methods(parts[1])
                if not include_docs:
                    methods = [dict(item, doc="") for item in methods]
                return self._ok({"ok": True, "service_id": parts[1], "methods": methods})
            if len(parts) == 3 and parts[0] == "services" and parts[2] == "status":
                return self._ok({"ok": True, "service": _service_status_to_dict(self.state.service_status_info(parts[1]))})
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        return self._err(404, "not found")

    def handle_post(self, path: str, body: bytes, headers=None) -> Tuple[int, Dict[str, str], bytes]:
        parsed = urlparse(path)
        parts = [unquote(x) for x in parsed.path.split("/") if x]
        if parts and parts[0] == "objects":
            return self.object_app.handle_post(path, headers or {}, body)
        try:
            if self._is_admin_path(path) and parts != ["admin", "token"]:
                auth_error = self._require_admin_token(headers or {})
                if auth_error is not None:
                    return auth_error
            if parts == ["taskpools"] or parts == ["services"]:
                auth_error = self._require_create_api_token(headers or {})
                if auth_error is not None:
                    return auth_error
            if parts == ["admin", "token"]:
                payload = _read_json(body)
                return self._set_admin_token(payload)
            if parts == ["admin", "upgrade"]:
                payload = _read_json(body)
                return self._upgrade_node(payload)
            if parts == ["admin", "restart"]:
                payload = _read_json(body)
                return self._restart_node(payload)
            if len(parts) == 3 and parts[0] == "taskpools" and parts[2] == "submit-bytes":
                return self._submit_pool_tasks_bytes(parts[1], body)
            if len(parts) == 3 and parts[0] == "taskpools" and parts[2] == "results-bytes":
                payload = _read_json(body)
                return self._pull_pool_results_bytes(parts[1], payload)
            if parts == ["runtime-globals-bytes"]:
                return self._update_runtime_globals_bytes(body)
            if len(parts) == 3 and parts[0] == "services" and parts[2] == "globals-bytes":
                return self._update_service_globals_bytes(parts[1], body)
            payload = _read_json(body)
            if parts == ["taskpools"]:
                return self._create_task_pool(payload)
            if len(parts) == 3 and parts[0] == "taskpools" and parts[2] == "heartbeat":
                return self._heartbeat_task_pool(parts[1], payload)
            if len(parts) == 3 and parts[0] == "taskpools" and parts[2] == "submit":
                return self._submit_pool_tasks(parts[1], payload)
            if len(parts) == 3 and parts[0] == "taskpools" and parts[2] == "results":
                return self._pull_pool_results(parts[1], payload)
            if len(parts) == 3 and parts[0] == "taskpools" and parts[2] == "cancel":
                return self._cancel_pool_job(parts[1], payload)
            if parts == ["runtime-globals"]:
                return self._update_runtime_globals(payload)
            if parts == ["services"]:
                return self._create_service(payload)
            if len(parts) == 4 and parts[0] == "services" and parts[2] == "call":
                return self._call_service(parts[1], parts[3], payload)
            if len(parts) == 3 and parts[0] == "services" and parts[2] == "globals":
                return self._update_service_globals(parts[1], payload)
            if len(parts) == 3 and parts[0] == "services" and parts[2] == "heartbeat":
                return self._heartbeat_service(parts[1], payload)
        except ValueError as exc:
            return self._err(400, str(exc))
        return self._err(404, "not found")

    def _upgrade_node(self, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        wheel_b64 = str(payload.get("wheel_b64", "") or "").strip()
        wheel_name = str(payload.get("wheel_name", "") or "pycloud_parallel_upgrade.whl").strip()
        if not wheel_b64:
            return self._err(400, "wheel_b64 is required")
        if "/" in wheel_name or "\\" in wheel_name or not wheel_name.endswith(".whl"):
            return self._err(400, "wheel_name must be a .whl filename")
        try:
            wheel_bytes = base64.b64decode(wheel_b64.encode("ascii"), validate=True)
        except Exception:
            return self._err(400, "invalid wheel_b64")
        restart = bool(payload.get("restart", False))
        pip_args = payload.get("pip_args") or []
        if not isinstance(pip_args, list):
            return self._err(400, "pip_args must be a list")
        safe_pip_args = [str(arg) for arg in pip_args if str(arg).strip()]
        with tempfile.TemporaryDirectory(prefix="pycloud-upgrade-") as tmpdir:
            wheel_path = Path(tmpdir) / wheel_name
            wheel_path.write_bytes(wheel_bytes)
            cmd = [sys.executable, "-m", "pip", "install", "--upgrade", str(wheel_path), *safe_pip_args]
            started_at = time.time()
            completed = subprocess.run(
                cmd,
                cwd=str(Path.cwd()),
                capture_output=True,
                text=True,
                timeout=max(30.0, float(payload.get("timeout_sec", 300.0) or 300.0)),
            )
        data = {
            "ok": completed.returncode == 0,
            "returncode": int(completed.returncode),
            "cmd": cmd,
            "duration_sec": round(time.time() - started_at, 3),
            "stdout": (completed.stdout or "")[-4000:],
            "stderr": (completed.stderr or "")[-4000:],
            "restart_requested": restart,
            "restart_scheduled": bool(restart and completed.returncode == 0),
        }
        if completed.returncode != 0:
            data["error"] = "pip install failed"
            return 500, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes(data)
        if restart:
            _restart_current_process_delayed()
        return self._ok(data)

    def _restart_node(self, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        delay_sec = max(0.1, float(payload.get("delay_sec", 1.0) or 1.0))
        _restart_current_process_delayed(delay_sec=delay_sec)
        return self._ok({"ok": True, "restart_scheduled": True, "delay_sec": delay_sec})

    def _set_admin_token(self, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        new_token = str(payload.get("admin_token", "") or "").strip()
        old_token = str(payload.get("old_admin_token", "") or "").strip()
        if not new_token:
            return self._err(400, "admin_token is required")
        current = self._configured_admin_token()
        if current:
            if not old_token:
                return self._err(401, "old_admin_token is required to update admin token")
            if not secrets.compare_digest(old_token, current):
                return self._err(403, "invalid old_admin_token")
        self._write_admin_token(new_token)
        return self._ok({"ok": True, "admin_token_configured": True, "updated": bool(current)})

    def handle_delete(self, path: str, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        parts = [unquote(x) for x in urlparse(path).path.split("/") if x]
        try:
            payload = _read_json(body)
            if len(parts) == 2 and parts[0] == "taskpools":
                self.state.close_task_pool(
                    owner_client_id=str(payload.get("owner_client_id", "") or ""),
                    pool_id=parts[1],
                    pool_token=str(payload.get("pool_token", "") or ""),
                    reason=str(payload.get("reason", "") or ""),
                )
                self._notify()
                return self._ok({"ok": True, "accepted": True})
            if len(parts) == 2 and parts[0] == "services":
                session = self.state.end_service(
                    owner_client_id=str(payload.get("owner_client_id", "") or ""),
                    service_id=parts[1],
                    service_token=str(payload.get("service_token", "") or ""),
                    reason=str(payload.get("reason", "") or ""),
                )
                self._notify()
                return self._ok({"ok": True, "accepted": True, "status": int(session.status)})
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except ValueError as exc:
            return self._err(400, str(exc))
        return self._err(404, "not found")

    def _create_task_pool(self, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        expected_node_instance_id = str(payload.get("expected_node_instance_id", "") or "").strip()
        local_node_instance_id = str(getattr(self.state, "node_instance_id", "") or "").strip()
        if expected_node_instance_id and local_node_instance_id and expected_node_instance_id != local_node_instance_id:
            return self._err(
                409,
                "node control_addr instance mismatch; "
                f"expected_node_instance_id={expected_node_instance_id} "
                f"actual_node_instance_id={local_node_instance_id} "
                f"node_id={getattr(self.state, 'node_id', '')}",
            )
        meta = _parse_message(pb2.CreateTaskPoolMeta, payload.get("meta", {}))
        blob = base64.b64decode(str(payload.get("code_b64", "") or "").encode("utf-8"))
        try:
            pool = self.state.create_task_pool(
                owner_client_id=meta.owner_client_id,
                pool_name=meta.pool_name,
                sha256=meta.sha256,
                runtime=meta.runtime,
                entry_module=meta.entry_module,
                entry_callable=meta.entry_callable,
                package_format=meta.package_format,
                dependency_policy_mode=meta.dependency_policy_mode,
                dependency_allowlist=list(meta.dependency_allowlist),
                managed_global_names=list(meta.managed_global_names),
                initial_globals=_decode_initial_globals(payload, context="taskpool_session"),
                worker_count=meta.worker_count,
                heartbeat_timeout_sec=meta.heartbeat_timeout_sec,
                idle_ttl_sec=meta.idle_ttl_sec,
                chunks=[blob],
            )
        except Exception as exc:
            return self._err(400, f"{exc.__class__.__name__}: {exc}")
        self._notify()
        return self._ok(_created_task_pool_response(pool))

    def _create_service(self, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        expected_node_instance_id = str(payload.get("expected_node_instance_id", "") or "").strip()
        local_node_instance_id = str(getattr(self.state, "node_instance_id", "") or "").strip()
        if expected_node_instance_id and local_node_instance_id and expected_node_instance_id != local_node_instance_id:
            return self._err(
                409,
                "node control_addr instance mismatch; "
                f"expected_node_instance_id={expected_node_instance_id} "
                f"actual_node_instance_id={local_node_instance_id} "
                f"node_id={getattr(self.state, 'node_id', '')}",
            )
        meta = _parse_message(pb2.CreateServiceMeta, payload.get("meta", {}))
        blob = base64.b64decode(str(payload.get("code_b64", "") or "").encode("utf-8"))
        try:
            session = self.state.create_service(
                owner_client_id=meta.owner_client_id,
                service_name=meta.service_name,
                sha256=meta.sha256,
                runtime=meta.runtime,
                entry_module=meta.entry_module,
                entry_callable=meta.entry_callable,
                package_format=meta.package_format,
                export_mode=meta.export_spec.mode,
                export_methods=list(meta.export_spec.methods),
                export_decorator=meta.export_spec.decorator,
                dependency_policy_mode=meta.dependency_policy_mode,
                dependency_allowlist=list(meta.dependency_allowlist),
                managed_global_names=list(meta.managed_global_names),
                initial_globals=_decode_initial_globals(payload, context="service_owner"),
                policy_id=str(meta.policy_id or "").strip().lower() or "default_safe",
                worker_count=meta.worker_count,
                heartbeat_timeout_sec=meta.heartbeat_timeout_sec,
                idle_ttl_sec=meta.idle_ttl_sec,
                expose_http=meta.expose_http,
                chunks=[blob],
            )
        except ValueError as exc:
            return self._err(400, str(exc))
        except Exception as exc:
            return self._err(500, repr(exc))
        self._notify()
        return self._ok(_created_service_response(session))

    def _submit_pool_tasks(self, pool_id: str, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        tasks = [_parse_message(pb2.TaskSubmitItem, item) for item in (payload.get("tasks") or [])]
        try:
            accepted, rejected = self.state.submit_pool_tasks(
                pool_id=pool_id,
                pool_token=str(payload.get("pool_token", "") or ""),
                tasks=tasks,
                job_id=str(payload.get("job_id", "") or ""),
            )
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except ValueError as exc:
            return self._err(400, str(exc))
        except RuntimeError as exc:
            return self._runtime_err(exc)
        return self._ok(
            {
                "ok": True,
                "accepted": [_message_to_dict(item) for item in accepted],
                "rejected": [_message_to_dict(item) for item in rejected],
                "node_credit": 0,
            }
        )

    def _submit_pool_tasks_bytes(self, pool_id: str, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        try:
            payload, raw = unpack_binary_sidecar(body)
            tasks = []
            offset = 0
            for item in list(payload.get("tasks") or ()):
                if not isinstance(item, dict):
                    raise ValueError("task meta entries must be objects")
                transport_meta = item.get("transport_payload")
                if isinstance(transport_meta, dict) and str(transport_meta.get("codec", "") or "").strip():
                    size = max(0, int(transport_meta.get("payload_size", 0) or 0))
                    chunk = raw[offset : offset + size]
                    if len(chunk) != size:
                        raise ValueError("binary task payload is truncated")
                    offset += size
                    tasks.append(
                        pb2.TaskSubmitItem(
                            task_id=str(item.get("task_id", "") or ""),
                            timeout_hint_sec=int(item.get("timeout_hint_sec", 0) or 0),
                            priority=int(item.get("priority", 0) or 0),
                            runtime_key=str(item.get("runtime_key", "") or ""),
                            transport_payload=pb2.TransportPayload(
                                codec=str(transport_meta.get("codec", "") or ""),
                                version=int(transport_meta.get("version", 0) or 0),
                                payload=chunk,
                            ),
                        )
                    )
                else:
                    tasks.append(_parse_message(pb2.TaskSubmitItem, item.get("message", item)))
            if offset != len(raw):
                raise ValueError("binary task payload has trailing bytes")
            accepted, rejected = self.state.submit_pool_tasks(
                pool_id=pool_id,
                pool_token=str(payload.get("pool_token", "") or ""),
                tasks=tasks,
                job_id=str(payload.get("job_id", "") or ""),
            )
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except ValueError as exc:
            return self._err(400, str(exc))
        except RuntimeError as exc:
            return self._runtime_err(exc)
        return self._ok(
            {
                "ok": True,
                "accepted": [_message_to_dict(item) for item in accepted],
                "rejected": [_message_to_dict(item) for item in rejected],
                "node_credit": 0,
            }
        )

    def _pull_pool_results(self, pool_id: str, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        try:
            results, next_cursor = self.state.pull_pool_results(
                pool_id=pool_id,
                pool_token=str(payload.get("pool_token", "") or ""),
                limit=int(payload.get("limit", 100) or 100),
                wait_ms=int(payload.get("wait_ms", 0) or 0),
                cursor=str(payload.get("cursor", "") or ""),
            )
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except RuntimeError as exc:
            return self._runtime_err(exc)
        return self._ok({"ok": True, "results": [_message_to_dict(item) for item in results], "next_cursor": next_cursor})

    def _pull_pool_results_bytes(self, pool_id: str, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        try:
            results, next_cursor = self.state.pull_pool_results(
                pool_id=pool_id,
                pool_token=str(payload.get("pool_token", "") or ""),
                limit=int(payload.get("limit", 100) or 100),
                wait_ms=int(payload.get("wait_ms", 0) or 0),
                cursor=str(payload.get("cursor", "") or ""),
            )
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except RuntimeError as exc:
            return self._runtime_err(exc)
        meta_results = []
        chunks = []
        for item in results:
            if item.HasField("transport_result") and str(item.transport_result.codec or "").strip():
                row: Dict[str, object] = {
                    "task_id": str(item.task_id or ""),
                    "status": int(item.status),
                    "attempt": int(item.attempt or 0),
                    "job_id": str(item.job_id or ""),
                    "transport_result": _transport_payload_meta(item.transport_result),
                }
                if item.HasField("started_at"):
                    row["started_at"] = _message_to_dict(item.started_at)
                if item.HasField("finished_at"):
                    row["finished_at"] = _message_to_dict(item.finished_at)
                if item.HasField("error"):
                    row["error"] = _message_to_dict(item.error)
                meta_results.append(row)
                chunks.append(bytes(item.transport_result.payload or b""))
            else:
                meta_results.append(_message_to_dict(item))
        raw = pack_binary_sidecar({"ok": True, "results": meta_results, "next_cursor": next_cursor}, chunks)
        return 200, {"Content-Type": "application/octet-stream"}, raw

    def _heartbeat_task_pool(self, pool_id: str, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        try:
            pool = self.state.heartbeat_task_pool(
                owner_client_id=str(payload.get("owner_client_id", "") or ""),
                pool_id=pool_id,
                pool_token=str(payload.get("pool_token", "") or ""),
            )
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except RuntimeError as exc:
            return self._err(409, str(exc))
        return self._ok({"ok": True, "accepted": True, "next_heartbeat_in_sec": max(1, pool.heartbeat_timeout_sec // 2)})

    def _cancel_pool_job(self, pool_id: str, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        try:
            queued_cancelled, running_marked, already_done, not_found = self.state.cancel_pool_job(
                pool_id=pool_id,
                pool_token=str(payload.get("pool_token", "") or ""),
                job_id=str(payload.get("job_id", "") or ""),
                reason=str(payload.get("reason", "") or ""),
            )
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except ValueError as exc:
            return self._err(400, str(exc))
        return self._ok(
            {
                "ok": True,
                "queued_cancelled": queued_cancelled,
                "running_marked": running_marked,
                "already_done": already_done,
                "not_found": not_found,
            }
        )

    def _update_runtime_globals(self, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        client_id = str(payload.get("client_id", "") or "")
        code_version = str(payload.get("code_version", "") or "")
        runtime_key = str(payload.get("runtime_key", "") or "")
        code_token = str(payload.get("code_token", "") or "")
        if not client_id or not code_version or not code_token:
            return self._err(400, "client_id, code_version and code_token are required")
        try:
            transport_values = payload.get("transport_values")
            if isinstance(transport_values, dict) and str(transport_values.get("codec", "") or "").strip():
                transport = _parse_message(pb2.TransportPayload, transport_values)
                serialization_mode = str(transport.codec or "").strip().lower()
                decoded_values = decode_transport_payload_bytes(
                    transport.codec,
                    transport.version,
                    transport.payload,
                    context="taskpool_session",
                )
            else:
                values = _parse_message(pb2.UpdateRuntimeGlobalsRequest, {"values": payload.get("values", {})}).values
                raw_values = struct_to_python(values)
                serialization_mode = detect_transport_mode(raw_values, default="legacy_v1")
                decoded_values = decode_payload_from_transport(
                    raw_values,
                    policy=get_payload_policy("managed_globals"),
                    mode=serialization_mode,
                    context="taskpool_session",
                )
            globals_digest, updated_names = self.state.update_runtime_globals(
                client_id=client_id,
                code_version=code_version,
                runtime_key=runtime_key,
                code_token=code_token,
                values=decoded_values if isinstance(decoded_values, dict) else {},
                serialization_mode=serialization_mode,
            )
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except ValueError as exc:
            return self._err(400, str(exc))
        return self._ok(
            {
                "ok": True,
                "code_version": code_version,
                "runtime_key": runtime_key or code_version,
                "globals_digest": globals_digest,
                "updated_names": list(updated_names or ()),
            }
        )

    def _update_runtime_globals_bytes(self, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        try:
            payload, raw = unpack_binary_sidecar(body)
            client_id = str(payload.get("client_id", "") or "")
            code_version = str(payload.get("code_version", "") or "")
            runtime_key = str(payload.get("runtime_key", "") or "")
            code_token = str(payload.get("code_token", "") or "")
            transport_meta = payload.get("transport_values")
            if not client_id or not code_version or not code_token:
                return self._err(400, "client_id, code_version and code_token are required")
            if not isinstance(transport_meta, dict) or not str(transport_meta.get("codec", "") or "").strip():
                raise ValueError("transport_values metadata is required")
            expected_size = max(0, int(transport_meta.get("payload_size", 0) or 0))
            if len(raw) != expected_size:
                raise ValueError("binary globals payload size mismatch")
            serialization_mode = str(transport_meta.get("codec", "") or "").strip().lower()
            decoded_values = decode_transport_payload_bytes(
                serialization_mode,
                int(transport_meta.get("version", 0) or 0),
                raw,
                context="taskpool_session",
            )
            globals_digest, updated_names = self.state.update_runtime_globals(
                client_id=client_id,
                code_version=code_version,
                runtime_key=runtime_key,
                code_token=code_token,
                values=decoded_values if isinstance(decoded_values, dict) else {},
                serialization_mode=serialization_mode,
            )
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except ValueError as exc:
            return self._err(400, str(exc))
        return self._ok(
            {
                "ok": True,
                "code_version": code_version,
                "runtime_key": runtime_key or code_version,
                "globals_digest": globals_digest,
                "updated_names": list(updated_names or ()),
            }
        )

    def _call_service(self, service_id: str, method: str, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        raw_payload = payload.get("payload", {})
        serialization_mode = detect_transport_mode(raw_payload, default=str(payload.get("serialization_mode", "") or "legacy_v1"))
        code, body = self.state.call_service(
            service_id=service_id,
            method=method,
            payload=raw_payload if isinstance(raw_payload, dict) else {},
            service_token=str(payload.get("service_token", "") or ""),
            timeout_sec=max(0.1, float(payload.get("timeout_sec", 60.0) or 60.0)),
            serialization_mode=serialization_mode,
            use_transport_result=False,
        )
        out = dict(body)
        if out.get("ok", False) and "data" in out:
            out["data"] = encode_payload_for_transport(
                out.get("data"),
                policy=get_payload_policy("result"),
                context="service_result",
                mode=serialization_mode,
            )
        return code, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes(out)

    def _update_service_globals(self, service_id: str, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        raw_values = payload.get("values", {})
        serialization_mode = detect_transport_mode(raw_values, default=str(payload.get("serialization_mode", "") or "legacy_v1"))
        values = decode_payload_from_transport(
            raw_values,
            policy=get_payload_policy("managed_globals"),
            mode=serialization_mode,
            context="service_owner",
        )
        try:
            digest, updated_names = self.state.update_service_globals(
                owner_client_id=str(payload.get("owner_client_id", "") or ""),
                service_id=service_id,
                service_token=str(payload.get("service_token", "") or ""),
                values=values if isinstance(values, dict) else {},
                serialization_mode=serialization_mode,
            )
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except ValueError as exc:
            return self._err(400, str(exc))
        return self._ok({"ok": True, "service_id": service_id, "globals_digest": digest, "updated_names": updated_names})

    def _update_service_globals_bytes(self, service_id: str, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        try:
            payload, raw = unpack_binary_sidecar(body)
            owner_client_id = str(payload.get("owner_client_id", "") or "")
            service_token = str(payload.get("service_token", "") or "")
            transport_meta = payload.get("transport_values")
            if not owner_client_id or not service_token:
                return self._err(400, "owner_client_id and service_token are required")
            if not isinstance(transport_meta, dict) or not str(transport_meta.get("codec", "") or "").strip():
                raise ValueError("transport_values metadata is required")
            expected_size = max(0, int(transport_meta.get("payload_size", 0) or 0))
            if len(raw) != expected_size:
                raise ValueError("binary service globals payload size mismatch")
            serialization_mode = str(transport_meta.get("codec", "") or "").strip().lower()
            decoded_values = decode_transport_payload_bytes(
                serialization_mode,
                int(transport_meta.get("version", 0) or 0),
                raw,
                context="service_owner",
            )
            digest, updated_names = self.state.update_service_globals(
                owner_client_id=owner_client_id,
                service_id=service_id,
                service_token=service_token,
                values=decoded_values if isinstance(decoded_values, dict) else {},
                serialization_mode=serialization_mode,
            )
        except KeyError as exc:
            return self._err(404, str(exc))
        except PermissionError as exc:
            return self._err(401, str(exc))
        except ValueError as exc:
            return self._err(400, str(exc))
        return self._ok({"ok": True, "service_id": service_id, "globals_digest": digest, "updated_names": updated_names})

    def _heartbeat_service(self, service_id: str, payload: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        try:
            session = self.state.heartbeat_service(
                owner_client_id=str(payload.get("owner_client_id", "") or ""),
                service_id=service_id,
                service_token=str(payload.get("service_token", "") or ""),
            )
        except KeyError:
            return self._err(404, "service not found")
        except PermissionError as exc:
            return self._err(401, str(exc))
        except RuntimeError as exc:
            return self._err(409, str(exc))
        self._notify()
        return self._ok({"ok": True, "accepted": True, "status": int(session.status), "next_heartbeat_in_sec": max(1, session.heartbeat_timeout_sec // 2)})

    def _ok(self, data: Dict[str, object]) -> Tuple[int, Dict[str, str], bytes]:
        return 200, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes(data)

    def _err(self, status_code: int, message: str) -> Tuple[int, Dict[str, str], bytes]:
        return int(status_code), {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": str(message)})

    def _runtime_err(self, exc: RuntimeError) -> Tuple[int, Dict[str, str], bytes]:
        message = str(exc)
        lower = message.lower()
        if any(marker in lower for marker in ("not running", "not found", "is stopped", "stopped")):
            return self._err(409, message)
        return self._err(500, f"{exc.__class__.__name__}: {message}")


class NodeControlHttpServer:
    def __init__(
        self,
        *,
        bind: str,
        state: NodeControlState,
        on_service_routes_changed=None,
        max_body_bytes: int = MAX_NODE_CONTROL_HTTP_BODY_BYTES,
        api_token: str = "",
    ) -> None:
        self.bind = bind
        self.app = NodeControlHttpApp(
            state,
            on_service_routes_changed=on_service_routes_changed,
            max_body_bytes=max_body_bytes,
            api_token=api_token,
        )
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self.base_url = ""

    def start(self) -> None:
        host, port = _split_host_port(self.bind)
        app = self.app

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                try:
                    result = app.handle_get(self.path)
                    if isinstance(result, StreamingHttpResponse):
                        self._send_stream(result)
                    else:
                        self._send(*result)
                except Exception as exc:
                    self._send_unexpected_error("GET", exc)

            def do_POST(self):  # noqa: N802
                try:
                    if urlparse(self.path).path.rstrip("/") == "/objects/upload":
                        self._handle_object_upload_stream()
                        return
                    self._handle_with_body(app.handle_post, pass_headers=True)
                except Exception as exc:
                    self._send_unexpected_error("POST", exc)

            def do_DELETE(self):  # noqa: N802
                try:
                    self._handle_with_body(app.handle_delete)
                except Exception as exc:
                    self._send_unexpected_error("DELETE", exc)

            def _handle_with_body(self, handler, *, pass_headers: bool = False) -> None:
                length = int(self.headers.get("Content-Length", "0") or 0)
                if length > app.max_body_bytes:
                    self._send(413, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": "payload too large"}))
                    return
                if pass_headers and app._is_resource_create_path(self.path):
                    auth_error = app._require_create_api_token(self.headers)
                    if auth_error is not None:
                        self._send(*auth_error)
                        return
                if pass_headers:
                    self._send(*handler(self.path, self.rfile.read(max(0, length)), self.headers))
                else:
                    self._send(*handler(self.path, self.rfile.read(max(0, length))))

            def _handle_object_upload_stream(self) -> None:
                length = int(self.headers.get("Content-Length", "0") or 0)
                if length > app.object_app.max_body_bytes:
                    self._send(
                        413,
                        {"Content-Type": "application/json; charset=utf-8"},
                        _json_bytes(
                            {
                                "ok": False,
                                "error": (
                                    f"object upload payload too large: size_bytes={length} "
                                    f"limit_bytes={app.object_app.max_body_bytes}"
                                ),
                            }
                        ),
                    )
                    return
                self._send(*app.object_app.handle_post_stream(self.path, self.headers, self.rfile, content_length=length))

            def _send(self, status_code: int, headers: Dict[str, str], raw: bytes) -> None:
                self.send_response(int(status_code))
                for key, value in dict(headers or {}).items():
                    self.send_header(str(key), str(value))
                self.send_header("Content-Length", str(len(raw or b"")))
                self.end_headers()
                if raw:
                    try:
                        self.wfile.write(raw)
                    except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError, OSError) as exc:
                        if _is_client_disconnect_error(exc):
                            logger.warning(
                                "NodeControl HTTP client disconnected before response write method=%s path=%s "
                                "node_id=%s node_instance_id=%s status_code=%s bytes=%s err=%r",
                                self.command,
                                self.path,
                                app.state.node_id,
                                app.state.node_instance_id,
                                int(status_code),
                                len(raw or b""),
                                exc,
                            )
                            return
                        raise

            def _send_stream(self, response: StreamingHttpResponse) -> None:
                self.send_response(int(response.status_code or 200))
                self.send_header("Content-Type", str(response.content_type or "application/octet-stream"))
                for key, value in dict(response.extra_headers or {}).items():
                    if str(key).lower() == "content-type":
                        continue
                    self.send_header(str(key), str(value))
                if int(response.content_length or 0) > 0:
                    self.send_header("Content-Length", str(int(response.content_length)))
                self.end_headers()
                for chunk in response.body_iter:
                    if chunk:
                        self.wfile.write(bytes(chunk))

            def _send_unexpected_error(self, method: str, exc: BaseException) -> None:
                logger.exception(
                    "NodeControl HTTP handler failed method=%s path=%s node_id=%s node_instance_id=%s",
                    method,
                    self.path,
                    getattr(app.state, "node_id", ""),
                    getattr(app.state, "node_instance_id", ""),
                )
                body = _json_bytes(
                    {
                        "ok": False,
                        "error": f"NodeControl internal error: {exc.__class__.__name__}: {exc}",
                    }
                )
                with contextlib.suppress(Exception):
                    self._send(500, {"Content-Type": "application/json; charset=utf-8"}, body)

            def log_message(self, _format, *args):  # noqa: A002
                return

        self._server = ThreadingHTTPServer((host, int(port)), _Handler)
        actual_port = self._server.server_address[1]
        public_host = "127.0.0.1" if host in {"", "0.0.0.0", "::"} else host
        self.base_url = f"http://{public_host}:{actual_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, name="nodecontrol-http", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def wait_for_termination(self) -> None:
        if self._thread is not None:
            self._thread.join()


class HttpNodeControlClient:
    def __init__(self, target: str, *, timeout_sec: float = 10.0, api_token: str = "") -> None:
        self.base_url = target_to_base_url(target)
        self.target = self.base_url
        self.control_addr = self.base_url
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.api_token = str(api_token or "").strip()
        self._object_client: Optional[HttpNodeObjectClient] = None

    def _api_headers(self, api_token: str = "") -> Dict[str, str]:
        token = str(api_token or self.api_token or "").strip()
        if not token:
            return {}
        return {"Authorization": f"Bearer {token}"}

    def close(self) -> None:
        if self._object_client is not None:
            self._object_client.close()
        return None

    def __enter__(self) -> "HttpNodeControlClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def node_status(self) -> Dict[str, object]:
        data = self._json("GET", "/node/status", None)
        node = data.get("node")
        return dict(node or {}) if isinstance(node, dict) else {}

    def _objects(self) -> HttpNodeObjectClient:
        if self._object_client is None:
            self._object_client = HttpNodeObjectClient(self.base_url, timeout_sec=self.timeout_sec)
        return self._object_client

    def upload_object_from_bytes(self, **kwargs):
        return self._objects().upload_object_from_bytes(**kwargs)

    def upload_object_from_file(self, **kwargs):
        return self._objects().upload_object_from_file(**kwargs)

    def upgrade_from_wheel_bytes(
        self,
        *,
        wheel_name: str,
        wheel_bytes: bytes,
        restart: bool = False,
        pip_args: Optional[Sequence[str]] = None,
        timeout_sec: float = 300.0,
        api_token: str = "",
    ) -> Dict[str, object]:
        payload = {
            "wheel_name": str(wheel_name or "pycloud_parallel_upgrade.whl"),
            "wheel_b64": base64.b64encode(bytes(wheel_bytes)).decode("ascii"),
            "restart": bool(restart),
            "pip_args": [str(arg) for arg in (pip_args or ()) if str(arg).strip()],
            "timeout_sec": max(30.0, float(timeout_sec or 300.0)),
        }
        return self._json(
            "POST",
            "/admin/upgrade",
            payload,
            timeout_sec=max(self.timeout_sec, float(timeout_sec or 300.0) + 5.0),
            headers=self._api_headers(api_token),
        )

    def upgrade_from_wheel_file(
        self,
        *,
        wheel_path: str,
        restart: bool = False,
        pip_args: Optional[Sequence[str]] = None,
        timeout_sec: float = 300.0,
        api_token: str = "",
    ) -> Dict[str, object]:
        path = Path(wheel_path).expanduser().resolve()
        return self.upgrade_from_wheel_bytes(
            wheel_name=path.name,
            wheel_bytes=path.read_bytes(),
            restart=restart,
            pip_args=pip_args,
            timeout_sec=timeout_sec,
            api_token=api_token,
        )

    def restart_node(self, *, delay_sec: float = 1.0, api_token: str = "") -> Dict[str, object]:
        return self._json(
            "POST",
            "/admin/restart",
            {"delay_sec": max(0.1, float(delay_sec or 1.0))},
            headers=self._api_headers(api_token),
        )

    def set_admin_token(self, *, admin_token: str, old_admin_token: str = "") -> Dict[str, object]:
        return self._json(
            "POST",
            "/admin/token",
            {
                "admin_token": str(admin_token or "").strip(),
                "old_admin_token": str(old_admin_token or "").strip(),
            },
        )

    def download_object_bytes(self, **kwargs) -> bytes:
        return self._objects().download_object_bytes(**kwargs)

    def download_object_to_file(self, **kwargs):
        return self._objects().download_object_to_file(**kwargs)

    def get_object_meta(self, **kwargs):
        return self._objects().get_object_meta(**kwargs)

    def has_object(self, **kwargs) -> bool:
        return self._objects().has_object(**kwargs)

    def pin_object(self, **kwargs) -> bool:
        return self._objects().pin_object(**kwargs)

    def release_object(self, **kwargs) -> bool:
        return self._objects().release_object(**kwargs)

    def release_object_ref(self, **kwargs) -> bool:
        return self._objects().release_object_ref(**kwargs)

    def download_result_to_file(self, result_ref: DataRef | object, *, target_path: str) -> Path:
        data_ref = maybe_data_ref(result_ref)
        if data_ref is None:
            raise TypeError("result_ref must be a DataRef-compatible value")
        path = self.download_object_to_file(object_id=data_ref.object_id, target_path=target_path)
        self._release_data_ref_if_consumed(data_ref)
        return path

    def fetch_result_ref_data(self, result_ref: DataRef | object, *, target_path: str = ""):
        data_ref = maybe_data_ref(result_ref)
        if data_ref is None:
            raise TypeError("result_ref must be a DataRef-compatible value")
        total_started_at = time.perf_counter()
        log_payload_flow(
            "result_ref_fetch",
            format=data_ref.format,
            materialize_as=data_ref.materialize_as,
            target_path=(target_path or "<temp>"),
            summary=summarize_payload_flow_value(data_ref),
        )
        if target_path:
            download_started_at = time.perf_counter()
            path = self.download_result_to_file(data_ref, target_path=target_path)
            log_payload_flow(
                "result_ref_fetch_done",
                client_result_ref_download_ms=(time.perf_counter() - download_started_at) * 1000.0,
                client_result_materialize_ms=0.0,
                client_result_total_ms=(time.perf_counter() - total_started_at) * 1000.0,
                target_path=str(path),
            )
            return path
        suffix = Path(f"result{('.' + data_ref.format) if data_ref.format else ''}")
        tmp = tempfile.NamedTemporaryFile(prefix="pycloud-result-", suffix=suffix.suffix, delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        try:
            download_started_at = time.perf_counter()
            self.download_result_to_file(data_ref, target_path=str(tmp_path))
            download_ms = (time.perf_counter() - download_started_at) * 1000.0
            materialize_started_at = time.perf_counter()
            result = _materialize_downloaded_result(tmp_path, result_ref=data_ref)
            materialize_ms = (time.perf_counter() - materialize_started_at) * 1000.0
            log_payload_flow(
                "result_ref_fetch_done",
                client_result_ref_download_ms=download_ms,
                client_result_materialize_ms=materialize_ms,
                client_result_total_ms=(time.perf_counter() - total_started_at) * 1000.0,
                target_path=str(tmp_path),
            )
            return result
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    def fetch_result_data(self, task_result: pb2.TaskResult, *, target_path: str = ""):
        if task_result.HasField("transport_result") and str(task_result.transport_result.codec or "").strip():
            data = decode_transport_payload_bytes(
                str(task_result.transport_result.codec or ""),
                int(task_result.transport_result.version or 0),
                task_result.transport_result.payload,
                context="taskpool_session",
            )
        else:
            raw = struct_to_python(task_result.result)
            data = decode_result_from_transport(
                raw,
                mode=detect_transport_mode(raw, default="legacy_v1"),
                context="taskpool_session",
            )
        if maybe_data_ref(data) is None:
            return data
        return self.fetch_result_ref_data(data, target_path=target_path)

    def _release_data_ref_if_consumed(self, ref: DataRef) -> None:
        if not bool(getattr(ref, "consume_on_read", False)):
            return
        try:
            self.release_object_ref(object_id=ref.object_id, ref_id=str(ref.ref_id or ""))
        except Exception:
            return None

    def create_task_pool_from_bytes(
        self,
        *,
        owner_client_id: str,
        pool_name: str = "",
        blob: bytes,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        deps: Optional[ArtifactDeps] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        initial_globals: Optional[Dict[str, object]] = None,
        worker_count: int = 1,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        api_token: str = "",
        expected_node_instance_id: str = "",
    ) -> NativeTaskPoolClient:
        import hashlib

        effective_format = _resolve_package_format(package_format, default="py")
        effective_module = _default_entry_module_for_package(
            package_format=effective_format,
            entry_module=entry_module,
            fallback_stem="task_pool_artifact",
        )
        resolved_deps = _coerce_artifact_deps(deps)
        effective_managed_global_names = [str(name) for name in (managed_global_names or ()) if str(name).strip()]
        if initial_globals:
            known_names = set(effective_managed_global_names)
            for name in initial_globals:
                normalized_name = str(name).strip()
                if normalized_name and normalized_name not in known_names:
                    effective_managed_global_names.append(normalized_name)
                    known_names.add(normalized_name)
        meta = pb2.CreateTaskPoolMeta(
            owner_client_id=owner_client_id,
            pool_name=pool_name,
            sha256=f"sha256:{hashlib.sha256(bytes(blob)).hexdigest()}",
            runtime=runtime,
            entry_module=effective_module,
            entry_callable=entry_callable or "run",
            worker_count=max(1, int(worker_count)),
            heartbeat_timeout_sec=max(1, int(heartbeat_timeout_sec)),
            idle_ttl_sec=max(0, int(idle_ttl_sec)),
            package_format=effective_format,
            dependency_allowlist=list(resolved_deps.dependency_allowlist),
            managed_global_names=effective_managed_global_names,
            dependency_policy_mode=_normalize_dependency_policy_mode(
                resolved_deps.mode,
                dependency_allowlist=resolved_deps.dependency_allowlist,
            ),
        )
        payload = {"meta": _message_to_dict(meta), "code_b64": base64.b64encode(bytes(blob)).decode("ascii")}
        if str(expected_node_instance_id or "").strip():
            payload["expected_node_instance_id"] = str(expected_node_instance_id or "").strip()
        if initial_globals:
            payload["initial_globals"] = encode_payload_for_transport(
                dict(initial_globals),
                policy=get_payload_policy("managed_globals"),
                context="taskpool_session",
                mode="structured_v1",
            )
        data = self._json(
            "POST",
            "/taskpools",
            payload,
            headers=self._api_headers(api_token),
        )
        now = utc_now()
        return NativeTaskPoolClient(
            _client=self,
            owner_client_id=owner_client_id,
            pool_id=str(data.get("pool_id", "")),
            pool_token=str(data.get("pool_token", "")),
            code_version=str(data.get("code_version", "")),
            worker_count=int(data.get("worker_count", 0) or 0),
            heartbeat_timeout_sec=int(data.get("heartbeat_timeout_sec", 0) or 0),
            pool_name=str(pool_name or ""),
            idle_ttl_sec=max(0, int(idle_ttl_sec or 0)),
            created_at=now,
            last_heartbeat_at=now,
            lease_expire_at=now + timedelta(seconds=max(1, int(data.get("heartbeat_timeout_sec", 0) or 0))),
        )

    def create_service_from_bytes(
        self,
        *,
        owner_client_id: str,
        service_name: str,
        blob: bytes,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        deps: Optional[ArtifactDeps] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        initial_globals: Optional[Dict[str, object]] = None,
        policy_id: str = "",
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        api_token: str = "",
        expected_node_instance_id: str = "",
    ) -> ServiceSessionClient:
        import hashlib

        effective_format = _resolve_package_format(package_format, default="py")
        effective_module = _default_entry_module_for_package(
            package_format=effective_format,
            entry_module=entry_module,
            fallback_stem="service_artifact",
        )
        resolved_deps = _coerce_artifact_deps(deps)
        effective_managed_global_names = [str(name) for name in (managed_global_names or ()) if str(name).strip()]
        if initial_globals:
            known_names = set(effective_managed_global_names)
            for name in initial_globals:
                normalized_name = str(name).strip()
                if normalized_name and normalized_name not in known_names:
                    effective_managed_global_names.append(normalized_name)
                    known_names.add(normalized_name)
        meta = pb2.CreateServiceMeta(
            owner_client_id=owner_client_id,
            service_name=service_name,
            sha256=f"sha256:{hashlib.sha256(bytes(blob)).hexdigest()}",
            runtime=runtime,
            entry_module=effective_module,
            entry_callable=entry_callable or "run",
            worker_count=max(1, int(worker_count)),
            heartbeat_timeout_sec=max(1, int(heartbeat_timeout_sec)),
            idle_ttl_sec=max(0, int(idle_ttl_sec)),
            expose_http=bool(expose_http),
            package_format=effective_format,
            export_spec=pb2.ModuleExportSpec(mode=str(export_mode or ""), methods=[str(x) for x in (export_methods or [])], decorator="pycloud_export"),
            dependency_allowlist=list(resolved_deps.dependency_allowlist),
            managed_global_names=effective_managed_global_names,
            dependency_policy_mode=_normalize_dependency_policy_mode(
                resolved_deps.mode,
                dependency_allowlist=resolved_deps.dependency_allowlist,
            ),
            policy_id=str(policy_id or "").strip().lower() or "default_safe",
        )
        payload = {"meta": _message_to_dict(meta), "code_b64": base64.b64encode(bytes(blob)).decode("ascii")}
        if str(expected_node_instance_id or "").strip():
            payload["expected_node_instance_id"] = str(expected_node_instance_id or "").strip()
        if initial_globals:
            payload["initial_globals"] = encode_payload_for_transport(
                dict(initial_globals),
                policy=get_payload_policy("managed_globals"),
                context="service_owner",
                mode="structured_v1",
            )
        data = self._json(
            "POST",
            "/services",
            payload,
            headers=self._api_headers(api_token),
        )
        now = utc_now()
        return ServiceSessionClient(
            _client=self,
            owner_client_id=owner_client_id,
            service_id=str(data.get("service_id", "")),
            service_token=str(data.get("service_token", "")),
            code_version=str(data.get("code_version", "")),
            http_base_url=str(data.get("http_base_url", "")),
            heartbeat_timeout_sec=int(data.get("heartbeat_timeout_sec", 0) or 0),
            worker_count=int(data.get("worker_count", 0) or 0),
            status=int(data.get("status", 0) or 0),
            service_name=str(service_name or ""),
            idle_ttl_sec=max(0, int(idle_ttl_sec or 0)),
            created_at=now,
            last_heartbeat_at=now,
            lease_expire_at=now + timedelta(seconds=max(1, int(data.get("heartbeat_timeout_sec", 0) or 0))),
        )

    def submit_pool_tasks(self, *, pool_id: str, pool_token: str, tasks: Sequence[pb2.TaskSubmitItem], job_id: str = "") -> pb2.SubmitTasksResponse:
        if any(item.HasField("transport_payload") and str(item.transport_payload.codec or "").strip() for item in tasks):
            meta_tasks = []
            chunks = []
            for item in tasks:
                if item.HasField("transport_payload") and str(item.transport_payload.codec or "").strip():
                    meta_tasks.append(
                        {
                            "task_id": str(item.task_id or ""),
                            "timeout_hint_sec": int(item.timeout_hint_sec or 0),
                            "priority": int(item.priority or 0),
                            "runtime_key": str(item.runtime_key or ""),
                            "transport_payload": _transport_payload_meta(item.transport_payload),
                        }
                    )
                    chunks.append(bytes(item.transport_payload.payload or b""))
                else:
                    meta_tasks.append({"message": _message_to_dict(item)})
            data = self._binary_json(
                "POST",
                f"/taskpools/{quote(str(pool_id), safe='')}/submit-bytes",
                {"pool_token": pool_token, "tasks": meta_tasks, "job_id": job_id},
                chunks,
            )
            return _parse_message(pb2.SubmitTasksResponse, data)
        data = self._json(
            "POST",
            f"/taskpools/{quote(str(pool_id), safe='')}/submit",
            {"pool_token": pool_token, "tasks": [_message_to_dict(item) for item in tasks], "job_id": job_id},
        )
        return _parse_message(pb2.SubmitTasksResponse, data)

    def pull_pool_results(self, *, pool_id: str, pool_token: str, limit: int = 100, wait_ms: int = 0, cursor: str = "") -> pb2.PullResultsResponse:
        meta, raw = self._json_to_binary_sidecar(
            "POST",
            f"/taskpools/{quote(str(pool_id), safe='')}/results-bytes",
            {"pool_token": pool_token, "limit": max(1, int(limit or 100)), "wait_ms": max(0, int(wait_ms or 0)), "cursor": cursor},
        )
        if not bool(meta.get("ok", False)):
            raise RuntimeError(str(meta.get("error", "pull pool results failed")))
        resp = pb2.PullResultsResponse(ok=True, next_cursor=str(meta.get("next_cursor", "") or ""))
        offset = 0
        for entry in list(meta.get("results") or ()):
            if not isinstance(entry, dict):
                continue
            item_payload = dict(entry)
            transport_meta = item_payload.pop("transport_result", None)
            item = _parse_message(pb2.TaskResult, item_payload)
            if isinstance(transport_meta, dict) and str(transport_meta.get("codec", "") or "").strip():
                size = max(0, int(transport_meta.get("payload_size", 0) or 0))
                chunk = raw[offset : offset + size]
                if len(chunk) != size:
                    raise RuntimeError("binary task result payload is truncated")
                offset += size
                item.transport_result.codec = str(transport_meta.get("codec", "") or "")
                item.transport_result.version = int(transport_meta.get("version", 0) or 0)
                item.transport_result.payload = chunk
            resp.results.append(item)
        if offset != len(raw):
            raise RuntimeError("binary task result payload has trailing bytes")
        return resp

    def close_task_pool(self, *, owner_client_id: str, pool_id: str, pool_token: str, reason: str = "") -> pb2.CloseTaskPoolResponse:
        data = self._json("DELETE", f"/taskpools/{quote(str(pool_id), safe='')}", {"owner_client_id": owner_client_id, "pool_token": pool_token, "reason": reason})
        return _parse_message(pb2.CloseTaskPoolResponse, data)

    def heartbeat_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
        seq: int = 0,
        timeout_sec: Optional[float] = None,
    ) -> pb2.HeartbeatTaskPoolResponse:
        data = self._json(
            "POST",
            f"/taskpools/{quote(str(pool_id), safe='')}/heartbeat",
            {"owner_client_id": owner_client_id, "pool_token": pool_token, "seq": int(seq)},
            timeout_sec=timeout_sec,
        )
        return _parse_message(pb2.HeartbeatTaskPoolResponse, data)

    def cancel_pool_job(self, *, pool_id: str, pool_token: str, job_id: str, reason: str = "") -> pb2.CancelJobResponse:
        data = self._json("POST", f"/taskpools/{quote(str(pool_id), safe='')}/cancel", {"pool_token": pool_token, "job_id": job_id, "reason": reason})
        return _parse_message(pb2.CancelJobResponse, data)

    def update_runtime_globals_encoded(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str,
        code_token: str,
        prepared_keys: Sequence[str],
        values: Optional[object] = None,
        transport_values: Optional[pb2.TransportPayload] = None,
    ) -> pb2.UpdateRuntimeGlobalsResponse:
        del prepared_keys
        payload: Dict[str, object] = {
            "client_id": str(client_id or "").strip(),
            "code_version": str(code_version or "").strip(),
            "runtime_key": str(runtime_key or "").strip(),
            "code_token": str(code_token or "").strip(),
        }
        if transport_values is not None and str(getattr(transport_values, "codec", "") or "").strip():
            payload["transport_values"] = _transport_payload_meta(transport_values)
            data = self._binary_json("POST", "/runtime-globals-bytes", payload, [bytes(transport_values.payload or b"")])
            return _parse_message(pb2.UpdateRuntimeGlobalsResponse, data)
        elif values is not None:
            payload["values"] = _message_to_dict(values)
        else:
            payload["values"] = {}
        data = self._json("POST", "/runtime-globals", payload)
        return _parse_message(pb2.UpdateRuntimeGlobalsResponse, data)

    def update_runtime_globals_prepared(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str,
        code_token: str,
        prepared_values: Dict[str, object],
        serialization_mode: str = "",
        effective_policy=None,
    ) -> pb2.UpdateRuntimeGlobalsResponse:
        limit_bytes = int(getattr(effective_policy, "inline_payload_hard_limit_bytes", 0) or 0) if effective_policy is not None else 0
        effective_mode = resolve_effective_serialization_mode(request_mode=serialization_mode, context="taskpool_session")
        transport_values = encode_transport_payload_bytes(
            prepared_values or {},
            mode=effective_mode,
            context="taskpool_session",
            limit_bytes=limit_bytes,
        )
        return self.update_runtime_globals_encoded(
            client_id=client_id,
            code_version=code_version,
            runtime_key=runtime_key,
            code_token=code_token,
            prepared_keys=sorted(str(key) for key in prepared_values.keys()),
            transport_values=transport_values,
        )

    def get_task_pool_status(self, *, pool_id: str, pool_token: str) -> pb2.TaskPoolStatusInfo:
        del pool_token
        data = self._json("GET", f"/taskpools/{quote(str(pool_id), safe='')}", None)
        return _parse_message(pb2.TaskPoolStatusInfo, data.get("pool", {}))

    def list_service_methods(self, *, service_id: str, include_docs: bool = False) -> Sequence[pb2.ServiceMethodInfo]:
        data = self._json("GET", f"/services/{quote(str(service_id), safe='')}/methods?include_docs={'true' if include_docs else 'false'}", None)
        return [_parse_message(pb2.ServiceMethodInfo, item) for item in (data.get("methods") or [])]

    def call_service(
        self,
        *,
        service_id: str,
        method: str,
        payload: Dict[str, object],
        timeout_sec: float = 60.0,
        service_token: str = "",
        serialization_mode: str = "",
        effective_policy=None,
    ) -> pb2.CallServiceResponse:
        effective_mode = resolve_effective_serialization_mode(request_mode=serialization_mode, context="service_owner")
        encoded_payload, _, _ = serialize_inline_payload(payload or {}, context="service call payload", mode=effective_mode)
        data = self._json(
            "POST",
            f"/services/{quote(str(service_id), safe='')}/call/{quote(str(method), safe='')}",
            {"service_token": service_token, "timeout_sec": max(0.1, float(timeout_sec)), "serialization_mode": effective_mode, "payload": encoded_payload},
            timeout_sec=max(self.timeout_sec, max(0.1, float(timeout_sec)) + 1.0),
        )
        if not data.get("ok", False):
            raise RuntimeError(str(data.get("error", "call service failed")))
        return pb2.CallServiceResponse(
            ok=True,
            service_id=service_id,
            method=method,
            data=dict_to_struct(data.get("data", {}), mode=effective_mode),
        )

    def update_service_globals(self, *, owner_client_id: str, service_id: str, service_token: str, values: Dict[str, object], serialization_mode: str = "", effective_policy=None):
        effective_mode = resolve_effective_serialization_mode(request_mode=serialization_mode, context="service_owner")
        encoded_values = encode_payload_for_transport(values or {}, policy=get_payload_policy("managed_globals"), context="service_owner", mode=effective_mode)
        data = self._json(
            "POST",
            f"/services/{quote(str(service_id), safe='')}/globals",
            {"owner_client_id": owner_client_id, "service_token": service_token, "serialization_mode": effective_mode, "values": encoded_values},
        )
        return _parse_message(pb2.UpdateServiceGlobalsResponse, data)

    def update_service_globals_encoded(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        prepared_keys: Sequence[str],
        values: Optional[object] = None,
        transport_values: Optional[pb2.TransportPayload] = None,
    ) -> pb2.UpdateServiceGlobalsResponse:
        del prepared_keys
        payload: Dict[str, object] = {
            "owner_client_id": str(owner_client_id or "").strip(),
            "service_token": str(service_token or "").strip(),
        }
        if transport_values is not None and str(getattr(transport_values, "codec", "") or "").strip():
            payload["transport_values"] = _transport_payload_meta(transport_values)
            data = self._binary_json(
                "POST",
                f"/services/{quote(str(service_id), safe='')}/globals-bytes",
                payload,
                [bytes(transport_values.payload or b"")],
            )
            return _parse_message(pb2.UpdateServiceGlobalsResponse, data)
        elif values is not None:
            raw_values = struct_to_python(values)
            effective_mode = detect_transport_mode(raw_values, default="legacy_v1")
            payload["serialization_mode"] = effective_mode
            payload["values"] = raw_values
        else:
            payload["serialization_mode"] = "legacy_v1"
            payload["values"] = {}
        data = self._json(
            "POST",
            f"/services/{quote(str(service_id), safe='')}/globals",
            payload,
        )
        return _parse_message(pb2.UpdateServiceGlobalsResponse, data)

    def update_service_globals_prepared(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        prepared_values: Dict[str, object],
        serialization_mode: str = "",
        effective_policy=None,
    ) -> pb2.UpdateServiceGlobalsResponse:
        limit_bytes = int(getattr(effective_policy, "inline_payload_hard_limit_bytes", 0) or 0) if effective_policy is not None else 0
        effective_mode = resolve_effective_serialization_mode(request_mode=serialization_mode, context="service_owner")
        transport_values = encode_transport_payload_bytes(
            prepared_values or {},
            mode=effective_mode,
            context="service_owner",
            limit_bytes=limit_bytes,
        )
        return self.update_service_globals_encoded(
            owner_client_id=owner_client_id,
            service_id=service_id,
            service_token=service_token,
            prepared_keys=sorted(str(key) for key in prepared_values.keys()),
            transport_values=transport_values,
        )

    def heartbeat_service(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        seq: int = 0,
        timeout_sec: Optional[float] = None,
    ) -> pb2.HeartbeatServiceResponse:
        data = self._json(
            "POST",
            f"/services/{quote(str(service_id), safe='')}/heartbeat",
            {"owner_client_id": owner_client_id, "service_token": service_token, "seq": int(seq)},
            timeout_sec=timeout_sec,
        )
        return _parse_message(pb2.HeartbeatServiceResponse, data)

    def end_service(self, *, owner_client_id: str, service_id: str, service_token: str, reason: str = "") -> pb2.EndServiceResponse:
        data = self._json("DELETE", f"/services/{quote(str(service_id), safe='')}", {"owner_client_id": owner_client_id, "service_token": service_token, "reason": reason})
        return _parse_message(pb2.EndServiceResponse, data)

    def get_service_status(self, *, service_id: str) -> pb2.ServiceStatusInfo:
        data = self._json("GET", f"/services/{quote(str(service_id), safe='')}/status", None)
        return _parse_message(pb2.ServiceStatusInfo, data.get("service", {}))

    def _binary_json(
        self,
        method: str,
        path: str,
        meta: Dict[str, object],
        chunks: Sequence[bytes],
        *,
        timeout_sec: Optional[float] = None,
    ) -> Dict[str, object]:
        return runtime_http_request(
            base_url=self.base_url,
            control_addr=self.base_url,
            request=RuntimeTransportRequest(
                path=path,
                mode="binary_sidecar",
                payload=meta,
                chunks=chunks,
                timeout_sec=float(timeout_sec if timeout_sec is not None else self.timeout_sec),
                method=method.upper(),
                headers={"Accept": "application/json"},
            ),
        )

    def _json_to_binary_sidecar(
        self,
        method: str,
        path: str,
        payload: Dict[str, object],
        *,
        timeout_sec: Optional[float] = None,
    ) -> Tuple[Dict[str, object], bytes]:
        return runtime_http_request_for_binary_sidecar_response(
            base_url=self.base_url,
            control_addr=self.base_url,
            request=RuntimeTransportRequest(
                path=path,
                mode="json",
                payload=payload,
                timeout_sec=float(timeout_sec if timeout_sec is not None else self.timeout_sec),
                method=method.upper(),
                headers={"Accept": "application/octet-stream"},
            ),
        )

    def _json(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, object]],
        *,
        timeout_sec: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, object]:
        return runtime_http_request(
            base_url=self.base_url,
            control_addr=self.base_url,
            request=RuntimeTransportRequest(
                path=path,
                mode="json",
                payload=payload,
                timeout_sec=float(timeout_sec if timeout_sec is not None else self.timeout_sec),
                method=method.upper(),
                headers=dict(headers or {}),
            ),
        )


__all__ = ["HttpNodeControlClient", "NodeControlHttpApp", "NodeControlHttpServer"]
