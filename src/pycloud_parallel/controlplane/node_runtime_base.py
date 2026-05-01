from __future__ import annotations

"""Shared runtime shell for node-like components."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import importlib
import inspect
import json
import os
import signal
import sys
import threading
import uuid
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

from pycloud_parallel.controlplane.http_gateway import ExtraGetHandler, MethodsHandler, ServiceHttpGateway, StreamingHttpResponse
from pycloud_parallel.controlplane.netutil import format_host_port, split_host_port
from pycloud_parallel.controlplane.node_capability import NodeCapability, detect_local_node_capability
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _invoke_python_callable(
    fn: Callable[..., object],
    payload: dict,
    *,
    params: Optional[Sequence[inspect.Parameter]] = None,
) -> object:
    if payload is None:
        payload = {}
    if params is None:
        try:
            params = list(inspect.signature(fn).parameters.values())
        except Exception:
            params = []
    if not params:
        return fn()
    if isinstance(payload, dict) and ("args" in payload or "kwargs" in payload):
        other_keys = set(payload.keys()) - {"args", "kwargs"}
        if not other_keys:
            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})
            if args is None:
                args = []
            elif not isinstance(args, (list, tuple)):
                args = list(args) if args else []
            if kwargs is None or not isinstance(kwargs, dict):
                kwargs = {}
            return fn(*args, **kwargs)
    if isinstance(payload, dict):
        return fn(**payload)
    return fn(payload)


@dataclass
class StaticServiceMount:
    service_id: str
    service_name: str
    invoke_handler: Callable[[str, dict, str, float, str, bool, bool], Union[Tuple[int, Dict[str, object]], StreamingHttpResponse]]
    methods_handler: Callable[[bool], Tuple[int, Dict[str, object]]]
    status_handler: Optional[Callable[[], Tuple[int, Dict[str, object]]]] = None
    extra_get_handler: Optional[Callable[[List[str], Dict[str, List[str]]], Optional[Tuple[object, ...]]]] = None
    worker_count: int = 1
    http_base_url: str = ""
    policy_id: str = "default_safe"
    module: Optional[ModuleType] = None
    managed_global_names: Tuple[str, ...] = ()


class NodeRuntimeBase:
    """Small shared shell for HTTP gateway, registration, and static services."""

    accept_service_deploy = False

    def __init__(
        self,
        *,
        node_id: str,
        service_http_bind: str = "",
        service_http_base_url: str = "",
        accept_service_deploy: bool = False,
    ) -> None:
        self.node_id = str(node_id or "").strip()
        self.service_http_bind = str(service_http_bind or "").strip()
        self.service_http_base_url = str(service_http_base_url or "").strip()
        self.accept_service_deploy = bool(accept_service_deploy)
        self._service_http_gateway: Optional[ServiceHttpGateway] = None
        self._startup_services: Dict[str, StaticServiceMount] = {}
        self._pending_startup_globals: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._infocenter_registrar = None
        self._closed = threading.Event()
        self._interrupt_shutdown_requested = False
        self._interrupt_signal_handlers: Dict[object, object] = {}
        self._python_version = f"py{sys.version_info.major}.{sys.version_info.minor}"
        self._service_worker_capacity_override = 0
        self._task_pool_worker_capacity_override = 0
        self.globals_digests: Dict[str, str] = {}

    def mount_startup_service(
        self,
        *,
        service_name: str,
        invoke_handler: Callable[[str, dict, str, float, str, bool, bool], Union[Tuple[int, Dict[str, object]], StreamingHttpResponse]],
        methods_handler: Callable[[bool], Tuple[int, Dict[str, object]]],
        status_handler: Optional[Callable[[], Tuple[int, Dict[str, object]]]] = None,
        extra_get_handler: Optional[Callable[[List[str], Dict[str, List[str]]], Optional[Tuple[object, ...]]]] = None,
        service_id: str = "",
        worker_count: int = 1,
        policy_id: str = "",
        module: Optional[ModuleType] = None,
        managed_global_names: Sequence[str] = (),
    ) -> StaticServiceMount:
        normalized_service_id = str(service_id or "").strip() or uuid.uuid4().hex
        mount = StaticServiceMount(
            service_id=normalized_service_id,
            service_name=str(service_name or f"service-{normalized_service_id[:8]}").strip(),
            invoke_handler=invoke_handler,
            methods_handler=methods_handler,
            status_handler=status_handler,
            extra_get_handler=extra_get_handler,
            worker_count=max(1, int(worker_count or 1)),
            policy_id=str(policy_id or "").strip().lower() or "default_safe",
            module=module,
            managed_global_names=tuple(str(name).strip() for name in (managed_global_names or ()) if str(name).strip()),
        )
        mount.http_base_url = f"{self.service_http_base_url}/svc/{mount.service_id}" if self.service_http_base_url else ""
        self._startup_services[normalized_service_id] = mount
        self._apply_pending_startup_globals(mount)
        return mount

    @staticmethod
    def _startup_globals_digest(values: Dict[str, Any]) -> str:
        digest_payload = json.dumps(dict(values or {}), ensure_ascii=False, sort_keys=True, default=str)
        return "sha256:" + hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()

    def _apply_pending_startup_globals(self, mount: StaticServiceMount) -> None:
        applied_keys: List[Tuple[str, str]] = []
        for key, values in list(self._pending_startup_globals.items()):
            pending_service_id, pending_service_name = key
            if pending_service_id and pending_service_id != mount.service_id:
                continue
            if pending_service_name and pending_service_name != mount.service_name:
                continue
            self._apply_startup_service_globals(mount, values)
            applied_keys.append(key)
        for key in applied_keys:
            self._pending_startup_globals.pop(key, None)

    def _matching_startup_services(
        self,
        *,
        service_id: str = "",
        service_name: str = "",
    ) -> List[StaticServiceMount]:
        normalized_service_id = str(service_id or "").strip()
        normalized_service_name = str(service_name or "").strip()
        return [
            mount
            for mount in self._startup_services.values()
            if (not normalized_service_id or mount.service_id == normalized_service_id)
            and (not normalized_service_name or mount.service_name == normalized_service_name)
        ]

    def _apply_startup_service_globals(self, mount: StaticServiceMount, values: Dict[str, Any]) -> str:
        if mount.module is None:
            raise ValueError("startup service module globals are unavailable")
        allowed_names = tuple(str(name).strip() for name in mount.managed_global_names if str(name).strip())
        normalized_values = dict(values or {})
        if allowed_names:
            unknown = sorted(str(name) for name in normalized_values if str(name) not in set(allowed_names))
            if unknown:
                raise ValueError(f"managed globals not declared for startup service: {unknown}")
        globals_digest = self._startup_globals_digest(normalized_values)
        apply_hook = getattr(mount.module, "apply_managed_globals", None)
        fallback_values: Optional[Dict[str, Any]]
        if apply_hook is None:
            fallback_values = normalized_values
        else:
            if not callable(apply_hook):
                raise ValueError("apply_managed_globals must be callable when defined")
            hook_result = apply_hook(
                dict(normalized_values),
                service_id=mount.service_id,
                service_name=mount.service_name,
                entry_module=str(getattr(mount.module, "__name__", "") or ""),
                session_kind="startup_service",
                globals_digest=globals_digest,
            )
            if hook_result is None:
                fallback_values = None
            elif isinstance(hook_result, dict):
                fallback_values = dict(hook_result)
            else:
                raise RuntimeError("apply_managed_globals must return None or dict")
        if fallback_values:
            module_globals = getattr(mount.module, "__dict__", None)
            if not isinstance(module_globals, dict):
                raise RuntimeError("startup service module globals are unavailable")
            for name, value in fallback_values.items():
                normalized_name = str(name or "").strip()
                if normalized_name:
                    module_globals[normalized_name] = value
        self.globals_digests[mount.service_id] = globals_digest
        return globals_digest

    def update_startup_service_globals(
        self,
        values: Dict[str, Any],
        *,
        service_id: str = "",
        service_name: str = "",
    ) -> str:
        if not isinstance(values, dict):
            raise RuntimeError("update_globals values must be a dict")
        mounts = self._matching_startup_services(service_id=service_id, service_name=service_name)
        if not mounts:
            normalized_service_id = str(service_id or "").strip()
            normalized_service_name = str(service_name or "").strip()
            if not normalized_service_id and not normalized_service_name:
                raise KeyError("startup service not found")
            pending_values = dict(values or {})
            self._pending_startup_globals[(normalized_service_id, normalized_service_name)] = pending_values
            digest = self._startup_globals_digest(pending_values)
            self.globals_digests[normalized_service_id or normalized_service_name] = digest
            return digest
        last_digest = ""
        for mount in mounts:
            last_digest = self._apply_startup_service_globals(mount, values)
        return last_digest

    def apply_managed_globals(
        self,
        values: Dict[str, Any],
        *,
        service_id: str = "",
        service_name: str = "",
    ) -> str:
        return self.update_startup_service_globals(values, service_id=service_id, service_name=service_name)

    def mount_python_module_service(
        self,
        *,
        service_name: str,
        entry_module: str,
        export_methods: Optional[Sequence[str]] = None,
        service_id: str = "",
        worker_count: int = 1,
        policy_id: str = "",
        managed_global_names: Sequence[str] = (),
    ) -> StaticServiceMount:
        module_name = str(entry_module or "").strip()
        if not module_name:
            raise ValueError("entry_module is required")
        normalized_service_id = str(service_id or "").strip() or uuid.uuid4().hex
        module = importlib.import_module(module_name)
        method_names = [str(name).strip() for name in (export_methods or ()) if str(name).strip()]
        if not method_names:
            method_names = [
                name
                for name, value in vars(module).items()
                if not name.startswith("_") and callable(value)
            ]
        methods = {name: getattr(module, name) for name in method_names if callable(getattr(module, name, None))}
        if not methods:
            raise ValueError(f"no callable service methods found in module {module_name!r}")
        method_params: Dict[str, Sequence[inspect.Parameter]] = {}
        method_param_names: Dict[str, set[str]] = {}
        method_docs: List[Dict[str, object]] = []
        for name, fn in sorted(methods.items()):
            try:
                params = list(inspect.signature(fn).parameters.values())
            except Exception:
                params = []
            method_params[name] = params
            method_param_names[name] = {param.name for param in params}
            method_docs.append(
                {
                    "method": name,
                    "qualified_name": f"{module_name}.{name}",
                    "doc": inspect.getdoc(fn),
                }
            )

        def _invoke(
            method: str,
            payload: dict,
            token: str,
            timeout_sec: float,
            serialization_mode: str,
            use_transport_result: bool,
            stream_response: bool,
        ):
            method_name = str(method or "").strip()
            fn = methods.get(method_name)
            if fn is None:
                return 404, {"ok": False, "error": f"method not found: {method}"}
            effective_payload = dict(payload or {})
            param_names = method_param_names.get(method_name, set())
            context_values = {
                "_service_id": normalized_service_id,
                "_service_token": token,
                "_timeout_sec": timeout_sec,
                "_serialization_mode": serialization_mode,
                "_use_transport_result": use_transport_result,
                "_stream_response": stream_response,
            }
            for name, value in context_values.items():
                if name in param_names and name not in effective_payload:
                    effective_payload[name] = value
            data = _invoke_python_callable(fn, effective_payload, params=method_params.get(method_name))
            if isinstance(data, dict) and data.get("__pycloud_raw_response__", False):
                raw = dict(data)
                raw.pop("__pycloud_raw_response__", None)
                status_code = int(raw.pop("__pycloud_status_code__", 200) or 200)
                return status_code, raw
            return 200, {"ok": True, "method": method_name, "data": {} if data is None else data}

        def _methods(include_docs: bool):
            if include_docs:
                return 200, {"ok": True, "methods": [dict(row) for row in method_docs]}
            return 200, {"ok": True, "methods": [{**row, "doc": ""} for row in method_docs]}

        status_fn = getattr(module, "service_status", None)
        status_params: Sequence[inspect.Parameter] = []
        status_param_names: set[str] = set()
        if callable(status_fn):
            try:
                status_params = list(inspect.signature(status_fn).parameters.values())
            except Exception:
                status_params = []
            status_param_names = {param.name for param in status_params}

        def _status():
            if not callable(status_fn):
                return None
            status_payload = {}
            if "_service_id" in status_param_names:
                status_payload["_service_id"] = normalized_service_id
            data = _invoke_python_callable(status_fn, status_payload, params=status_params)
            if isinstance(data, dict) and data.get("__pycloud_raw_response__", False):
                raw = dict(data)
                raw.pop("__pycloud_raw_response__", None)
                status_code = int(raw.pop("__pycloud_status_code__", 200) or 200)
                return status_code, raw
            return 200, {"ok": True, "service": {} if data is None else data}

        extra_get_fn = getattr(module, "extra_get", None)
        extra_get_param_names: set[str] = set()
        if callable(extra_get_fn):
            try:
                extra_get_param_names = set(inspect.signature(extra_get_fn).parameters.keys())
            except Exception:
                extra_get_param_names = set()

        def _extra_get(path_parts: List[str], query: Dict[str, List[str]]):
            if not callable(extra_get_fn):
                return None
            kwargs = {"path_parts": path_parts, "query": query}
            if "_service_id" in extra_get_param_names:
                kwargs["_service_id"] = normalized_service_id
            return extra_get_fn(**kwargs)

        return self.mount_startup_service(
            service_name=service_name,
            invoke_handler=_invoke,
            methods_handler=_methods,
            status_handler=_status if callable(status_fn) else None,
            extra_get_handler=_extra_get if callable(extra_get_fn) else None,
            service_id=normalized_service_id,
            worker_count=worker_count,
            policy_id=policy_id,
            module=module,
            managed_global_names=managed_global_names,
        )

    def start_mounted_service_gateway(self) -> None:
        self.start_service_gateway(
            invoke_handler=self._invoke_mounted_startup_service,
            status_handler=self._status_mounted_startup_service,
            methods_handler=self._methods_mounted_startup_service,
            extra_get_handler=self._extra_get_mounted_startup_service,
        )
        for mount in self._startup_services.values():
            mount.http_base_url = f"{self.service_http_base_url}/svc/{mount.service_id}" if self.service_http_base_url else ""

    def start_service_gateway(
        self,
        *,
        invoke_handler: Callable[[str, str, dict, str, float, str, bool, bool], Union[Tuple[int, Dict[str, object]], StreamingHttpResponse]],
        status_handler: Callable[[str], Tuple[int, Dict[str, object]]],
        methods_handler: Optional[MethodsHandler] = None,
        extra_get_handler: Optional[ExtraGetHandler] = None,
    ) -> None:
        if not self.service_http_bind or self._service_http_gateway is not None:
            return
        self._service_http_gateway = ServiceHttpGateway(
            bind=self.service_http_bind,
            invoke_handler=invoke_handler,
            status_handler=status_handler,
            methods_handler=methods_handler,
            extra_get_handler=extra_get_handler,
        )
        self._service_http_gateway.start()
        if not self.service_http_base_url:
            self.service_http_base_url = self._service_http_gateway.base_url

    def stop_service_gateway(self) -> None:
        if self._service_http_gateway is not None:
            self._service_http_gateway.stop()

    def close(self) -> None:
        self._restore_interrupt_shutdown_handlers()
        self._closed.set()
        if self._infocenter_registrar is not None:
            self._infocenter_registrar.close()
            self._infocenter_registrar = None
        self.stop_service_gateway()

    def join(self, timeout: Optional[float] = None) -> None:
        if timeout is not None:
            self._closed.wait(max(0.0, float(timeout)))
            return
        while not self._closed.wait(3600.0):
            pass

    def install_interrupt_shutdown_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            sig = getattr(signal, sig_name, None)
            if sig is None or sig in self._interrupt_signal_handlers:
                continue
            try:
                previous = signal.getsignal(sig)
                signal.signal(sig, self._handle_interrupt_signal)
            except (OSError, ValueError):
                continue
            self._interrupt_signal_handlers[sig] = previous

    def _restore_interrupt_shutdown_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        handlers = dict(self._interrupt_signal_handlers)
        self._interrupt_signal_handlers.clear()
        for sig, previous in handlers.items():
            try:
                if signal.getsignal(sig) == self._handle_interrupt_signal:
                    signal.signal(sig, previous)
            except (OSError, ValueError):
                pass

    def _handle_interrupt_signal(self, signum, frame) -> None:
        del frame
        if self._interrupt_shutdown_requested:
            os._exit(130)
        self._interrupt_shutdown_requested = True
        self._closed.set()
        threading.Thread(target=self.close, name=f"{self.node_id}-signal-close", daemon=True).start()
        if signum == getattr(signal, "SIGTERM", None):
            raise SystemExit(128 + int(signum or 0))
        raise KeyboardInterrupt

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def infocenter_registered(self) -> bool:
        return self._infocenter_registrar is not None

    def advertise_http_addr(self) -> str:
        parsed = urlparse(str(self.service_http_base_url or ""))
        if parsed.hostname and parsed.port:
            return format_host_port(parsed.hostname, int(parsed.port))
        if self.service_http_bind:
            host, port = split_host_port(self.service_http_bind)
            return format_host_port(host, int(port))
        return ""

    def advertise_control_addr(self) -> str:
        return ""

    def start_infocenter_registration(
        self,
        *,
        infocenter_target: str,
        control_addr: str = "",
        capacity: int = 0,
        queue_capacity: int = 0,
        tags: Optional[Sequence[str]] = None,
        version: str = "",
        metadata: Optional[Dict[str, str]] = None,
        heartbeat_sec: int = 10,
        rpc_timeout_sec: float = 5.0,
    ) -> None:
        target = str(infocenter_target or "").strip()
        if not target:
            return
        if self._infocenter_registrar is not None:
            self._infocenter_registrar.request_sync()
            return
        if self.service_http_bind and not self.service_http_base_url:
            self.start_mounted_service_gateway()
        from pycloud_parallel.controlplane.registrar import NodeInfoCenterRegistrar

        effective_capacity = max(1, int(capacity or self.service_worker_capacity or 1))
        effective_metadata = {
            "accept_service_deploy": "false",
            "startup_service": "true",
            **dict(metadata or {}),
        }
        registrar = NodeInfoCenterRegistrar(
            infocenter_addr=target,
            node_id=self.node_id,
            control_addr=str(control_addr or "").strip(),
            state=self,
            capacity=effective_capacity,
            queue_capacity=max(1, int(queue_capacity or effective_capacity)),
            tags=list(tags or ["startup-service"]),
            version=version,
            metadata=effective_metadata,
            fallback_heartbeat_sec=max(1, int(heartbeat_sec or 1)),
            rpc_timeout_sec=max(0.5, float(rpc_timeout_sec or 0.5)),
        )
        self._infocenter_registrar = registrar
        registrar.start()

    def service_timing_metadata(self) -> Dict[str, str]:
        return {}

    def task_pool_reports(self) -> Dict[str, object]:
        return {}

    def active_runtime_keys(self, *, limit: int = 10) -> List[str]:
        del limit
        return [f"py{sys.version_info.major}", self._python_version]

    @property
    def service_worker_capacity(self) -> int:
        startup_capacity = sum(max(1, int(mount.worker_count or 1)) for mount in self._startup_services.values())
        return startup_capacity or max(0, int(self._service_worker_capacity_override or 0))

    @service_worker_capacity.setter
    def service_worker_capacity(self, value: int) -> None:
        self._service_worker_capacity_override = max(0, int(value or 0))

    def service_worker_used(self) -> int:
        return self.service_worker_capacity

    @property
    def task_pool_worker_capacity(self) -> int:
        return max(0, int(self._task_pool_worker_capacity_override or 0))

    @task_pool_worker_capacity.setter
    def task_pool_worker_capacity(self, value: int) -> None:
        self._task_pool_worker_capacity_override = max(0, int(value or 0))

    def task_pool_worker_used(self) -> int:
        return 0

    @property
    def python_version(self) -> str:
        return self._python_version

    def node_capability(self) -> NodeCapability:
        return detect_local_node_capability()

    def metrics(self) -> Dict[str, int]:
        capacity = self.service_worker_capacity
        return {"queued": 0, "inflight": 0, "running": 0, "credit": capacity}

    def startup_service_report_payloads(self) -> List[Dict[str, object]]:
        lease_expire_at = datetime.now(timezone.utc).isoformat()
        return [
            {
                "service_name": mount.service_name,
                "service_id": mount.service_id,
                "status": int(pb2.SERVICE_STATUS_RUNNING),
                "worker_count": int(mount.worker_count),
                "alive_workers": int(mount.worker_count),
                "in_flight": 0,
                "lease_expire_at": lease_expire_at,
                "http_base_url": mount.http_base_url,
                "policy_id": mount.policy_id,
            }
            for mount in self._startup_services.values()
        ]

    def service_report_payloads(self, *, include_stopped: bool = False) -> List[Dict[str, object]]:
        del include_stopped
        return self.startup_service_report_payloads()

    def _mounted_service(self, service_id: str) -> Optional[StaticServiceMount]:
        return self._startup_services.get(str(service_id or "").strip())

    def _invoke_mounted_startup_service(
        self,
        service_id: str,
        method: str,
        payload: dict,
        service_token: str,
        timeout_sec: float,
        serialization_mode: str = "",
        use_transport_result: bool = False,
        stream_response: bool = False,
    ) -> Union[Tuple[int, Dict[str, object]], StreamingHttpResponse]:
        mount = self._mounted_service(service_id)
        if mount is None:
            return 404, {"ok": False, "error": "service not found"}
        return mount.invoke_handler(
            method,
            payload,
            service_token,
            timeout_sec,
            serialization_mode,
            use_transport_result,
            stream_response,
        )

    def _status_mounted_startup_service(self, service_id: str) -> Tuple[int, Dict[str, object]]:
        mount = self._mounted_service(service_id)
        if mount is None:
            return 404, {"ok": False, "error": "service not found"}
        if mount.status_handler is not None:
            return mount.status_handler()
        return 200, {
            "ok": True,
            "service": {
                "service_id": mount.service_id,
                "service_name": mount.service_name,
                "status": int(pb2.SERVICE_STATUS_RUNNING),
                "status_text": pb2.ServiceStatus.Name(pb2.SERVICE_STATUS_RUNNING),
                "http_base_url": mount.http_base_url,
            },
        }

    def _methods_mounted_startup_service(self, service_id: str, include_docs: bool) -> Tuple[int, Dict[str, object]]:
        mount = self._mounted_service(service_id)
        if mount is None:
            return 404, {"ok": False, "error": "service not found"}
        code, body = mount.methods_handler(include_docs)
        if isinstance(body, dict) and "service_id" not in body:
            body = {"service_id": mount.service_id, **body}
        return code, body

    def _extra_get_mounted_startup_service(
        self,
        service_id: str,
        path_parts: List[str],
        query: Dict[str, List[str]],
    ) -> Optional[Tuple[object, ...]]:
        mount = self._mounted_service(service_id)
        if mount is None or mount.extra_get_handler is None:
            return None
        return mount.extra_get_handler(path_parts, query)

    def create_service(self, *args, **kwargs):
        del args, kwargs
        raise RuntimeError("this node only supports startup-mounted services; dynamic service deployment is disabled")


__all__ = ["NodeRuntimeBase", "StaticServiceMount"]
