from __future__ import annotations

"""Authoritative V1 service execution implementation."""

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
import asyncio
import contextlib
import io
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from pycloud_parallel.controlplane.artifact import (
    Artifact,
    ArtifactDeps,
    _coerce_artifact_deps,
    _default_artifact_filename,
    _default_entry_module_for_func,
    _default_entry_module_for_module,
    _infer_entry_module_from_artifact_path,
    _normalize_artifact_input,
    _normalize_entry_callable_arg,
    _normalize_entry_module_arg,
    _prepare_artifact,
    _resolve_package_format,
)
from pycloud_parallel.controlplane.config import OBJECT_CHUNK_SIZE_BYTES
from pycloud_parallel.controlplane.infocenter_client import (
    InfoCenterNode,
    InfoCenterServiceRoute,
    NodeCircuitState,
    _build_unique_node_id_map,
    _node_instance_key_from_node,
    _node_instance_key_from_route,
    _route_sort_key,
)
from pycloud_parallel.data.ref import DataRef
from pycloud_parallel.controlplane.replica_client import ServiceSessionClient
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle
from pycloud_parallel.controlplane.runtime_spec import matches_python_runtime, normalize_python_runtime_spec
from pycloud_parallel.execution.base import ServiceExecutionSession
from pycloud_parallel.execution.call_proxy import _BroadcastProxy, _CallProxy
from pycloud_parallel.execution.support import (
    _DEFAULT_EXPORT_DECORATOR,
    _RetryableReadyError,
    _SERVICE_SESSION_LOCKED_PATHS,
    _SERVICE_SESSION_LOCK_GUARD,
    _SERVICE_SESSION_SCHEMA_VERSION,
    _artifact_code_version,
    _default_service_session_cache_dir,
    _emit_owner_notice,
    _ensure_private_dir,
    _filter_nodes_by_runtime,
    _get_local_ip,
    _prepare_code_blob,
    _prepare_managed_globals_values_for_upload,
    _put_data_via_clients,
    _resolve_public_target_arg,
    _resolve_high_level_service_data,
    _resolve_high_level_service_results,
    _retry_infocenter_request,
    _sanitize_session_cache_part,
    _serialize_arrow_compatible,
    _source_func_from_entry_callable_arg,
    _source_module_from_entry_module_arg,
    _summarize_discovered_nodes,
    _timestamp_to_datetime,
    _write_private_json,
)
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.runtime.compat import runtime_mismatch_message_for_nodes

def _infocenter_client(*args, **kwargs):
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

    return InfoCenterClient(*args, **kwargs)


def _node_control_client(*args, **kwargs):
    from pycloud_parallel.controlplane.node_control_client import NodeControlClient

    return NodeControlClient(*args, **kwargs)


@dataclass
class _ServiceSessionFileLock:
    path: Path
    _fp: Optional[io.BufferedRandom] = field(default=None, init=False, repr=False)

    def acquire(self) -> "_ServiceSessionFileLock":
        normalized = str(self.path.resolve())
        with _SERVICE_SESSION_LOCK_GUARD:
            if normalized in _SERVICE_SESSION_LOCKED_PATHS:
                raise RuntimeError(f"local deploy session already holds cache lock: {self.path}")
            _SERVICE_SESSION_LOCKED_PATHS.add(normalized)
        try:
            _ensure_private_dir(self.path.parent)
            fp = open(self.path, "a+b")
        except Exception:
            with _SERVICE_SESSION_LOCK_GUARD:
                _SERVICE_SESSION_LOCKED_PATHS.discard(normalized)
            raise
        try:
            try:
                os.chmod(self.path, 0o600)
            except OSError:
                pass
            if os.name == "nt":
                import msvcrt

                fp.seek(0)
                msvcrt.locking(fp.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception as exc:
            fp.close()
            with _SERVICE_SESSION_LOCK_GUARD:
                _SERVICE_SESSION_LOCKED_PATHS.discard(normalized)
            raise RuntimeError(
                f"another local deploy process already owns service session cache lock: {self.path}"
            ) from exc
        self._fp = fp
        return self

    def write_json(self, payload: Dict[str, object]) -> None:
        if self._fp is None:
            raise RuntimeError("service session lock is not acquired")
        data = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        self._fp.seek(0)
        self._fp.truncate()
        self._fp.write(data)
        self._fp.flush()
        try:
            os.fsync(self._fp.fileno())
        except OSError:
            pass

    def clear(self) -> None:
        if self._fp is None:
            return
        self._fp.seek(0)
        self._fp.truncate()
        self._fp.flush()
        try:
            os.fsync(self._fp.fileno())
        except OSError:
            pass

    def close(self) -> None:
        if self._fp is None:
            return
        normalized = str(self.path.resolve())
        try:
            if os.name == "nt":
                import msvcrt

                self._fp.seek(0)
                msvcrt.locking(self._fp.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fp.fileno(), fcntl.LOCK_UN)
        finally:
            try:
                self._fp.close()
            finally:
                self._fp = None
                with _SERVICE_SESSION_LOCK_GUARD:
                    _SERVICE_SESSION_LOCKED_PATHS.discard(normalized)


def _service_session_cache_file(
    *,
    owner_client_id: str,
    service_name: str,
    cache_dir: str = "",
) -> Path:
    base_dir = Path(cache_dir).expanduser() if str(cache_dir).strip() else _default_service_session_cache_dir()
    return (
        base_dir
        / _sanitize_session_cache_part(owner_client_id)
        / f"{_sanitize_session_cache_part(service_name)}.json"
    )


def _load_service_session_cache(
    *,
    owner_client_id: str,
    service_name: str,
    cache_dir: str = "",
) -> Optional[Dict[str, object]]:
    path = _service_session_cache_file(
        owner_client_id=owner_client_id,
        service_name=service_name,
        cache_dir=cache_dir,
    )
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("schema_version", 0) or 0) != _SERVICE_SESSION_SCHEMA_VERSION:
        return None
    if payload.get("owner_client_id") != owner_client_id or payload.get("service_name") != service_name:
        return None
    nodes = payload.get("nodes")
    if not isinstance(nodes, dict):
        return None
    return payload


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


_SERVICE_READY_GRACE_SEC = max(0.0, _env_float("PYCLOUD_SERVICE_READY_GRACE_SEC", 5.0))
_SERVICE_READY_RETRY_INTERVAL_SEC = max(0.05, _env_float("PYCLOUD_SERVICE_READY_RETRY_INTERVAL_SEC", 0.25))
_SERVICE_SESSION_LOCK_RETRY_SEC = max(0.0, _env_float("PYCLOUD_SERVICE_SESSION_LOCK_RETRY_SEC", 3.0))


def _ready_retry_timeout(timeout_sec: float, *, grace_sec: float) -> float:
    effective_timeout = max(0.0, float(timeout_sec or 0.0))
    effective_grace = max(0.0, float(grace_sec or 0.0))
    if effective_timeout <= 0.0:
        return effective_grace
    if effective_grace <= 0.0:
        return 0.0
    return min(effective_timeout, effective_grace)


def _retry_ready_state(
    fn: Callable[[], Any],
    *,
    timeout_sec: float,
    grace_sec: float,
    target: str,
    action: str,
    retry_interval_sec: float = _SERVICE_READY_RETRY_INTERVAL_SEC,
) -> Any:
    wait_timeout = _ready_retry_timeout(timeout_sec, grace_sec=grace_sec)
    if wait_timeout <= 0.0:
        return fn()
    deadline = time.monotonic() + wait_timeout
    last_exc: Optional[_RetryableReadyError] = None
    while True:
        try:
            return fn()
        except _RetryableReadyError as exc:
            last_exc = exc
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"{action} not ready via {target} after {wait_timeout:.1f}s: {exc}"
                ) from exc
            time.sleep(min(max(0.05, float(retry_interval_sec or 0.25)), max(0.05, deadline - time.monotonic())))


def _acquire_service_session_lock_with_retry(
    path: Path,
    *,
    timeout_sec: float,
    action: str,
) -> _ServiceSessionFileLock:
    wait_timeout = _ready_retry_timeout(timeout_sec, grace_sec=_SERVICE_SESSION_LOCK_RETRY_SEC)
    deadline = time.monotonic() + wait_timeout
    last_exc: Optional[RuntimeError] = None
    while True:
        try:
            return _ServiceSessionFileLock(path).acquire()
        except RuntimeError as exc:
            last_exc = exc
            if wait_timeout <= 0.0 or time.monotonic() >= deadline:
                raise RuntimeError(f"{action}: {exc}") from exc
            time.sleep(min(_SERVICE_READY_RETRY_INTERVAL_SEC, max(0.05, deadline - time.monotonic())))


class _ConnectedService:
    """Unified product-facing connected service object for discovery/gateway transports."""

    def __init__(
        self,
        *,
        transport_client: Any,
        service_name: str,
        transport: str,
        timeout_sec: float,
        validate_on_init: bool = True,
    ) -> None:
        self._transport_client = transport_client
        self.service_name = str(service_name or "").strip()
        self.transport = str(transport or "").strip().lower() or "discovery"
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.target = str(
            getattr(transport_client, "target", "") or getattr(transport_client, "infocenter_target", "") or ""
        ).strip()
        self._route_cache = getattr(transport_client, "_route_cache", None)
        self._client_mod: Any = None
        if self.transport == "discovery":
            from pycloud_parallel.controlplane import discovery_client as discovery_client_mod

            self._client_mod = discovery_client_mod.client_mod
        elif self.transport == "gateway":
            from pycloud_parallel.controlplane import gateway_client as gateway_client_mod

            self._client_mod = gateway_client_mod.client_mod
        self._discovered_methods: Optional[List[str]] = None
        self._last_status: Optional[Dict[str, object]] = None
        if not self.service_name:
            raise ValueError("service_name is required")
        if validate_on_init:
            self._validate_service_ready()

    def close(self) -> None:
        close = getattr(self._transport_client, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "_ConnectedService":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _validate_service_ready(self) -> Dict[str, object]:
        def _probe() -> Dict[str, object]:
            if self.transport == "discovery":
                try:
                    refresh = getattr(self._transport_client, "refresh_routes", None)
                    if callable(refresh):
                        refresh(service_name=self.service_name, force=True)
                    status = self._transport_client.get_status(service_name=self.service_name)
                except Exception as exc:
                    raise _RetryableReadyError(
                        f"failed to query {self.transport} status for service_name={self.service_name!r}: {exc}"
                    ) from exc
            else:
                try:
                    status = self._transport_client.get_status(service_name=self.service_name)
                except Exception as exc:
                    raise _RetryableReadyError(
                        f"failed to query {self.transport} status for service_name={self.service_name!r}: {exc}"
                    ) from exc
            if not isinstance(status, dict):
                raise RuntimeError(
                    f"invalid {self.transport} status for service_name={self.service_name!r}: {status!r}"
                )
            self._last_status = status
            route_count = int(status.get("route_count", 0) or 0)
            if route_count <= 0:
                raise _RetryableReadyError(
                    f"no available route for service_name={self.service_name!r} via {self.transport}"
                )
            return status

        return _retry_ready_state(
            _probe,
            timeout_sec=self.timeout_sec,
            grace_sec=_SERVICE_READY_GRACE_SEC,
            target=self.target or self.transport,
            action=f"service connect {self.service_name!r}",
        )

    def _ensure_methods_discovered(self) -> None:
        if self._discovered_methods is not None:
            return

        def _probe() -> List[str]:
            try:
                methods = self.list_methods(include_docs=True)
            except Exception as exc:
                self._validate_service_ready()
                raise _RetryableReadyError(
                    f"failed to list methods for service_name={self.service_name!r} via {self.transport}: {exc}"
                ) from exc
            discovered = [str(item.get("method", "")).strip() for item in methods if str(item.get("method", "")).strip()]
            if not discovered:
                self._validate_service_ready()
                raise _RetryableReadyError(
                    f"service_name={self.service_name!r} has active {self.transport} routes but no exported methods"
                )
            return discovered

        self._discovered_methods = _retry_ready_state(
            _probe,
            timeout_sec=self.timeout_sec,
            grace_sec=_SERVICE_READY_GRACE_SEC,
            target=self.target or self.transport,
            action=f"service method discovery {self.service_name!r}",
        )

    def refresh_methods(self) -> List[str]:
        self._discovered_methods = None
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def list_methods(self, *, include_docs: bool = False) -> List[Dict[str, object]]:
        if self.transport == "discovery":
            route_cache = self._route_cache
            strategy = "predicted_busy"
            tried: Set[str] = set()
            try:
                if route_cache is not None:
                    route = route_cache.select_route(self.service_name, strategy=strategy)
                else:
                    routes = self._discoverable_routes(force_refresh=True)
                    if not routes:
                        raise RuntimeError(f"no available route for service_name={self.service_name!r}")
                    route = sorted(routes, key=lambda item: _route_sort_key(item, strategy=strategy))[0]
                tried.add(str(getattr(route, "service_id", "") or ""))
                methods = self._list_methods_via_route(route, include_docs=include_docs)
                if route_cache is not None:
                    with contextlib.suppress(Exception):
                        route_cache.mark_success(route)
            except Exception as exc:
                if route_cache is not None and "route" in locals():
                    with contextlib.suppress(Exception):
                        route_cache.mark_failure(route, str(exc))
                    with contextlib.suppress(Exception):
                        route_cache.refresh(self.service_name, force=True)
                    retry_route = route_cache.select_route(
                        self.service_name,
                        exclude_service_ids=tried,
                        strategy=strategy,
                    )
                else:
                    retry_candidates = [
                        item
                        for item in self._discoverable_routes(force_refresh=True)
                        if str(getattr(item, "service_id", "") or "") not in tried
                    ]
                    if not retry_candidates:
                        raise
                    retry_route = sorted(
                        retry_candidates,
                        key=lambda item: _route_sort_key(item, strategy=strategy),
                    )[0]
                methods = self._list_methods_via_route(retry_route, include_docs=include_docs)
                if route_cache is not None:
                    with contextlib.suppress(Exception):
                        route_cache.mark_success(retry_route)
        else:
            methods = self._transport_client.list_methods(
                service_name=self.service_name,
                include_docs=include_docs,
            )
        return list(methods)

    def _list_methods_via_route(self, route: object, *, include_docs: bool) -> List[Dict[str, object]]:
        if not str(getattr(route, "control_addr", "") or "").strip():
            return self._client_mod._list_route_methods_http(route, include_docs=include_docs, timeout_sec=self.timeout_sec)
        with _node_control_client(route.control_addr, timeout_sec=self.timeout_sec) as client:
            methods = client.list_service_methods(service_id=getattr(route, "service_id", ""), include_docs=include_docs)
        return [
            {
                "method": item.method,
                "qualified_name": item.qualified_name,
                "doc": item.doc,
            }
            for item in methods
        ]

    @property
    def methods(self) -> List[str]:
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def status(self) -> Dict[str, object]:
        if self.transport == "discovery":
            try:
                status = self._transport_client.get_status(service_name=self.service_name)
            except Exception:
                status = {}
            if isinstance(status, dict) and int(status.get("route_count", 0) or 0) > 0:
                self._last_status = status
                return status
            routes = self._discoverable_routes()
            status = {
                "ok": True,
                "service_name": self.service_name,
                "route_count": len(routes),
                "routes": [self._client_mod._serialize_route(route) for route in routes],
            }
        else:
            status = self._transport_client.get_status(service_name=self.service_name)
        if isinstance(status, dict):
            self._last_status = status
        return status

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        return self._transport_client.fetch_result_data(response_or_data, target_path=target_path)

    def download_result_to_file(self, response_or_data: object, *, target_path: str):
        return self._transport_client.download_result_to_file(response_or_data, target_path=target_path)

    def _prepare_discovery_route_payload(self, route: object, payload: Dict[str, object]) -> Dict[str, object]:
        from pycloud_parallel.controlplane.remote_payload import prepare_remote_call_payload

        control_addr = str(getattr(route, "control_addr", "") or "").strip()
        if not control_addr:
            return dict(payload or {})
        with _node_control_client(control_addr, timeout_sec=self.timeout_sec) as route_client:
            return prepare_remote_call_payload(
                [route_client],
                payload,
                object_threshold_bytes=self._client_mod.INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
            )

    def _discoverable_routes(self, *, force_refresh: bool = False) -> List[InfoCenterServiceRoute]:
        if self.transport != "discovery":
            return []
        route_cache = self._route_cache
        routes: List[InfoCenterServiceRoute] = []
        if route_cache is not None:
            if force_refresh:
                with contextlib.suppress(Exception):
                    route_cache.refresh(self.service_name, force=True)
            with contextlib.suppress(Exception):
                routes = list(route_cache.get_routes(self.service_name))
        routes = [route for route in routes if str(getattr(route, "service_name", "") or "").strip() == self.service_name]
        if routes:
            return routes
        return self._discover_routes_from_nodes()

    def _discover_routes_from_nodes(self) -> List[InfoCenterServiceRoute]:
        try:
            with _infocenter_client(self.target, timeout_sec=self.timeout_sec) as infocenter:
                nodes = list(
                    infocenter.list_nodes(
                        healthy_only=True,
                        tags=None,
                        limit=500,
                    )
                )
        except Exception:
            return []
        routes: List[InfoCenterServiceRoute] = []
        now = datetime.now(timezone.utc)
        lease_expire_at = now + timedelta(seconds=max(1.0, float(self.timeout_sec)))
        for node in nodes:
            services = tuple(getattr(node, "services", ()) or ())
            for svc in services:
                if str(getattr(svc, "service_name", "") or "").strip() != self.service_name:
                    continue
                http_base_url = str(getattr(svc, "http_base_url", "") or "").strip()
                control_addr = str(getattr(node, "control_addr", "") or "").strip()
                if not http_base_url or not control_addr:
                    continue
                status = int(getattr(svc, "status", 0) or 0)
                if status not in {pb2.SERVICE_STATUS_RUNNING, pb2.SERVICE_STATUS_STARTING}:
                    continue
                worker_count = max(1, int(getattr(svc, "worker_count", 0) or 1))
                alive_workers = max(1, int(getattr(svc, "alive_workers", 0) or worker_count))
                in_flight = max(0, int(getattr(svc, "in_flight", 0) or 0))
                routes.append(
                    InfoCenterServiceRoute(
                        service_name=self.service_name,
                        service_id=str(getattr(svc, "service_id", "") or ""),
                        status=status,
                        node_instance_id=str(getattr(node, "node_instance_id", "") or getattr(node, "node_id", "") or ""),
                        node_id=str(getattr(node, "node_id", "") or ""),
                        control_addr=control_addr,
                        node_healthy=bool(getattr(node, "healthy", True)),
                        worker_count=worker_count,
                        alive_workers=alive_workers,
                        in_flight=in_flight,
                        lease_expire_at=lease_expire_at,
                        http_base_url=http_base_url,
                        reported_in_flight=in_flight,
                        received_count=0,
                        returned_count=0,
                        ema_child_invoke_ms=0.0,
                        ema_samples=0,
                        predicted_busy=float(in_flight) / float(alive_workers),
                    )
                )
        routes.sort(key=lambda route: _route_sort_key(route, strategy="predicted_busy"))
        return routes

    def call_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        del refresh_status, max_attempts
        if self.transport == "discovery":
            route_cache = self._route_cache
            strategy_name = "predicted_busy" if strategy == "least_inflight" else strategy
            if route_cache is not None:
                route = route_cache.select_route(self.service_name, strategy=strategy_name)
            else:
                routes = self._discoverable_routes(force_refresh=True)
                if not routes:
                    raise RuntimeError(f"no available route for service_name={self.service_name!r}")
                route = sorted(routes, key=lambda item: _route_sort_key(item, strategy=strategy_name))[0]
            tried = {str(getattr(route, "service_id", "") or "")}
            token = getattr(self._transport_client, "service_token", "")

            def _call_route(selected_route: object) -> Tuple[str, Dict[str, object]]:
                prepared_payload = self._prepare_discovery_route_payload(selected_route, payload)
                resp = self._client_mod._call_route_http(
                    selected_route,
                    method=method,
                    payload=prepared_payload,
                    timeout_sec=max(0.1, float(timeout_sec)),
                    service_token=token,
                )
                attach_locator = getattr(self._transport_client, "_attach_controlplane_locator", None)
                if callable(attach_locator):
                    resp = attach_locator(resp, route=selected_route)
                return self._client_mod._node_instance_key_from_route(selected_route), resp

            try:
                node_id, response = _call_route(route)
                if route_cache is not None:
                    with contextlib.suppress(Exception):
                        route_cache.mark_success(route)
                return node_id, response
            except self._client_mod.DiscoveryCallError as exc:
                if not self._client_mod._is_route_failure(exc):
                    raise RuntimeError(str(exc)) from exc
                if route_cache is not None:
                    with contextlib.suppress(Exception):
                        route_cache.mark_failure(route, str(exc))
                    with contextlib.suppress(Exception):
                        route_cache.refresh(self.service_name, force=True)
                    retry_route = route_cache.select_route(
                        self.service_name,
                        exclude_service_ids=tried,
                        strategy=strategy_name,
                    )
                else:
                    retry_candidates = [
                        item for item in self._discoverable_routes(force_refresh=True)
                        if str(getattr(item, "service_id", "") or "") not in tried
                    ]
                    if not retry_candidates:
                        raise RuntimeError(str(exc)) from exc
                    retry_route = sorted(
                        retry_candidates,
                        key=lambda item: _route_sort_key(item, strategy=strategy_name),
                    )[0]
                try:
                    node_id, response = _call_route(retry_route)
                    if route_cache is not None:
                        with contextlib.suppress(Exception):
                            route_cache.mark_success(retry_route)
                    return node_id, response
                except self._client_mod.DiscoveryCallError as retry_exc:
                    if self._client_mod._is_route_failure(retry_exc) and route_cache is not None:
                        with contextlib.suppress(Exception):
                            route_cache.mark_failure(retry_route, str(retry_exc))
                    raise RuntimeError(str(retry_exc)) from retry_exc
        response = self._transport_client.call(
            service_name=self.service_name,
            method=method,
            payload=payload,
            timeout_sec=timeout_sec,
        )
        return self.transport, response

    async def acall_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.call_balanced(
                method,
                payload,
                timeout_sec=timeout_sec,
                strategy=strategy,
                refresh_status=refresh_status,
                max_attempts=max_attempts,
            ),
        )

    async def acall_all(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ):
        del method, payload, timeout_sec, max_concurrency
        raise NotImplementedError(f"{self.transport} connected service does not support broadcast")

    async def call(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = await self.acall_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = self.call_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    async def call_all(self, method: str, **kwargs) -> List[Tuple[Optional[str], Optional[object], Optional[Exception]]]:
        results = await self.acall_all(method, kwargs)
        return _resolve_high_level_service_results(self, results=results)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        if self._discovered_methods is None:
            self._ensure_methods_discovered()
        if self._discovered_methods is not None and name not in self._discovered_methods:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{name}'. Available methods: {self._discovered_methods}"
            )
        proxy_strategy = "predicted_busy" if self.transport == "discovery" else "gateway"
        return _CallProxy(
            method=name,
            group=self,
            timeout_sec=self.timeout_sec,
            strategy=proxy_strategy,
            refresh_status=False,
        )

    def __repr__(self) -> str:
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<ConnectedService "
            f"service={self.service_name!r} "
            f"transport={self.transport} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


@dataclass
class Service(ServiceExecutionSession):
    """A deployed service group spread across multiple NodeControl nodes."""

    owner_client_id: str
    service_name: str
    sessions: Dict[str, ServiceSessionClient]
    nodes: Dict[str, InfoCenterNode]
    failed: bool = False
    failures: Dict[str, str] = field(default_factory=dict)
    globals_digests: Dict[str, str] = field(default_factory=dict)
    breaker_enabled: bool = True
    breaker_failure_threshold: int = 3
    breaker_cooldown_sec: float = 5.0
    breaker_max_cooldown_sec: float = 120.0
    _clients: Dict[str, NodeControlClient] = field(default_factory=dict, repr=False)
    _session_cache_file: Optional[Path] = field(default=None, repr=False)
    _session_cache_lock: Optional[_ServiceSessionFileLock] = field(default=None, repr=False)
    _delete_session_cache_on_close: bool = field(default=False, repr=False)
    _artifact_code_version: str = field(default="", repr=False)
    _route_index: int = field(default=0, repr=False)
    _route_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _breaker_states: Dict[str, NodeCircuitState] = field(default_factory=dict, repr=False)
    _discovered_methods: Optional[List[str]] = field(default=None, repr=False)
    _closed: bool = field(default=False, repr=False)

    def _replica_handles(self) -> Dict[str, ExecutionReplicaHandle]:
        return self.sessions

    @classmethod
    def deploy(
        cls,
        *,
        target: str = "",
        **kwargs: Any,
    ) -> "Service":
        """Product-facing deploy action for V1 service sessions.

        Default path: ``Service.deploy(target=\"127.0.0.1:50051\", source=my_module, ...)``.
        Advanced path: ``Service.deploy(artifact=Artifact(...), ...)``.
        """
        effective_target = _resolve_public_target_arg(
            target=target,
            kwargs=kwargs,
            action_name="Service.deploy()",
        )
        return cls.deploy_from_infocenter(infocenter_target=effective_target, **kwargs)

    @classmethod
    def connect(
        cls,
        *,
        target: str,
        service_name: str,
        timeout_sec: float = 10.0,
        service_token: str = "",
        transport: str = "discovery",
        validate_on_init: bool = False,
    ):
        """Product-facing connect action for an already deployed service."""
        normalized_transport = str(transport or "discovery").strip().lower() or "discovery"
        if normalized_transport == "gateway":
            from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

            return _ConnectedService(
                transport_client=GatewayServiceClient(
                    target,
                    timeout_sec=timeout_sec,
                    service_token=service_token,
                ),
                service_name=service_name,
                transport=normalized_transport,
                timeout_sec=timeout_sec,
                validate_on_init=validate_on_init,
            )
        if normalized_transport == "discovery":
            from pycloud_parallel.controlplane.discovery_client import DiscoveryServiceClient

            return _ConnectedService(
                transport_client=DiscoveryServiceClient(
                    target,
                    timeout_sec=timeout_sec,
                    service_token=service_token,
                ),
                service_name=service_name,
                transport=normalized_transport,
                timeout_sec=timeout_sec,
                validate_on_init=validate_on_init,
            )
        raise ValueError("transport must be one of: discovery, gateway")

    @classmethod
    def deploy_from_infocenter(
        cls,
        *,
        infocenter_target: str,
        source: Any = None,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        artifact: Optional[Any] = None,
        deps: Optional[Any] = None,
        func: Optional[Callable] = None,
        artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
        blob: Optional[bytes] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: Any = "run",
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 5.0,
        breaker_max_cooldown_sec: float = 120.0,
    ) -> "Service":
        """从 InfoCenter 发现节点并部署服务。

        Args:
            infocenter_target: InfoCenter 地址
            source: 默认产品化代码输入；可传 callable / module / path / bytes
            owner_client_id: 所有者客户端 ID
            service_name: 服务名称
            artifact: 高级 Artifact 声明对象
            func: 函数对象（自动打包依赖，优先级最高）
            artifact_path: 单个文件、单个文件夹或文件/文件夹路径列表
            blob: 直接提供代码内容
            runtime: 运行时版本
            entry_module: 入口模块名，或可导入的真实模块对象
            entry_callable: 入口函数名，或真实函数对象
            package_format: 包格式 ("py", "zip", "tar.gz")
            export_mode: 导出模式 ("decorator", "explicit", "all", "single")
            export_methods: 显式导出的方法列表
            worker_count: 工作进程数
            heartbeat_timeout_sec: 心跳超时
            idle_ttl_sec: 空闲 TTL
            expose_http: 是否暴露 HTTP
            chunk_size: 上传分片大小
            healthy_only: 是否只使用健康节点
            tags: 节点标签过滤
            node_ids: 显式指定要部署到哪些节点
            node_count: 需要挑选的节点数量；未指定时默认使用 min_success_nodes
            node_limit: 节点数量限制
            allow_partial: 是否允许部分失败
            min_success_nodes: 最小成功节点数
            timeout_sec: 超时时间
            ensure_unique_service_name: 是否确保服务名唯一
            reuse_existing_same_code: 同 owner + 同代码时是否直接复用已存在服务
            replace_existing_if_code_changed: 同 owner + 同服务名但代码变化时是否替换（默认自动替换）
            session_cache_dir: 本地 service session token 缓存目录
            breaker_enabled: 是否启用熔断器
            breaker_failure_threshold: 熔断失败阈值
            breaker_cooldown_sec: 熔断冷却时间
            breaker_max_cooldown_sec: 熔断最大冷却时间

        Returns:
            Service: 部署的服务组
        """
        normalized_artifact = _normalize_artifact_input(
            consumer_kind="service",
            source=source,
            artifact=artifact,
            deps=deps,
            func=func,
            artifact_path=artifact_path,
            blob=blob,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            export_mode=export_mode,
            export_methods=export_methods,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=managed_global_names,
        )
        prepared_artifact = _prepare_artifact(
            normalized_artifact,
            consumer_kind="service",
        )
        effective_blob = prepared_artifact.blob
        effective_filename = prepared_artifact.filename
        runtime = prepared_artifact.runtime
        effective_entry_module = prepared_artifact.entry_module
        entry_callable = prepared_artifact.entry_callable
        effective_package_format = prepared_artifact.package_format
        export_mode = prepared_artifact.export_mode
        export_methods = list(prepared_artifact.export_methods)
        dependency_allowlist = list(prepared_artifact.dependency_allowlist)
        managed_global_names = list(prepared_artifact.managed_global_names)

        # 生成默认的 owner_client_id 和 service_name
        local_ip = _get_local_ip()

        # 如果 owner_client_id 为空，使用本机 IP
        effective_owner_client_id = owner_client_id
        if not effective_owner_client_id:
            effective_owner_client_id = f"client-{local_ip}"

        # 先确定 entry_module（用于生成 service_name）
        effective_entry_module = effective_entry_module or _infer_entry_module_from_artifact_path(artifact_path)
        if not effective_entry_module:
            if effective_filename:
                # 优先使用推导出的 artifact 文件名
                if effective_filename.endswith(".py"):
                    effective_entry_module = Path(effective_filename).stem

        # 如果 service_name 为空，使用 entry_module + 本机 IP + 时间戳（精确到秒）
        # 添加时间戳确保唯一性，避免服务名冲突
        effective_service_name = service_name
        if not effective_service_name:
            # 生成时间戳（精确到秒）
            timestamp = time.strftime("%Y%m%d%H%M%S")  # 格式: 20250330120000

            if effective_entry_module:
                effective_service_name = f"{effective_entry_module}-{local_ip}-{timestamp}"
            else:
                effective_service_name = f"service-{local_ip}-{timestamp}"

        # 现在才进行校验
        if not effective_owner_client_id:
            raise ValueError("owner_client_id is required")
        if not effective_service_name:
            raise ValueError("service_name is required")

        effective_code_version = prepared_artifact.code_version
        session_cache_file = _service_session_cache_file(
            owner_client_id=effective_owner_client_id,
            service_name=effective_service_name,
            cache_dir=session_cache_dir,
        )
        session_cache_lock: Optional[_ServiceSessionFileLock] = None

        requested_node_ids = [str(node_id).strip() for node_id in (node_ids or []) if str(node_id).strip()]
        requested_node_instance_ids = [str(node_id).strip() for node_id in (node_instance_ids or []) if str(node_id).strip()]
        desired_node_count = max(0, int(node_count or 0))
        required_success_nodes = max(1, int(min_success_nodes))
        discovery_limit = max(
            1,
            int(node_limit),
            len(requested_node_ids),
            len(requested_node_instance_ids),
            desired_node_count or required_success_nodes,
        )

        _emit_owner_notice(
            "deploy start "
            f"service_name={effective_service_name} owner={effective_owner_client_id} "
            f"target={infocenter_target} runtime={runtime} "
            f"requested_node_ids={requested_node_ids or 'auto'} "
            f"requested_node_instance_ids={requested_node_instance_ids or 'auto'} "
            f"min_success_nodes={required_success_nodes}"
        )

        def _discover_from_infocenter() -> Tuple[Sequence[InfoCenterServiceRoute], Sequence[InfoCenterNode]]:
            with _infocenter_client(infocenter_target, timeout_sec=timeout_sec) as infocenter:
                existing_routes: Sequence[InfoCenterServiceRoute] = ()
                if ensure_unique_service_name:
                    existing_routes = infocenter.list_service_routes(
                        service_name=effective_service_name,
                        healthy_only=True,
                        limit=max(100, discovery_limit * 10),
                    )
                discovered_nodes = infocenter.list_nodes(
                    healthy_only=healthy_only,
                    tags=tags,
                    limit=discovery_limit,
                )
                return existing_routes, discovered_nodes

        normalized_runtime = normalize_python_runtime_spec(runtime)

        def _select_nodes_from_discovery(
            existing_routes: Sequence[InfoCenterServiceRoute],
            discovered_nodes: Sequence[InfoCenterNode],
        ) -> Sequence[InfoCenterNode]:
            if not discovered_nodes:
                raise _RetryableReadyError(
                    f"no available nodes from InfoCenter: target={infocenter_target} "
                    f"healthy_only={healthy_only} tags={list(tags or ())}"
                )

            discovered_instance_map = {_node_instance_key_from_node(node): node for node in discovered_nodes}
            if requested_node_instance_ids:
                missing_node_instance_ids = [
                    node_id for node_id in requested_node_instance_ids if node_id not in discovered_instance_map
                ]
                if missing_node_instance_ids:
                    raise _RetryableReadyError(
                        f"requested node_instance_ids not found in current discovery scope: {missing_node_instance_ids}"
                    )
                selected_nodes = [discovered_instance_map[node_id] for node_id in requested_node_instance_ids]
                if normalized_runtime:
                    incompatible = [
                        node
                        for node in selected_nodes
                        if str(node.python_version or "").strip()
                        and not matches_python_runtime(node.python_version, normalized_runtime)
                    ]
                    if incompatible:
                        raise RuntimeError(
                            runtime_mismatch_message_for_nodes(
                                requested_runtime=normalized_runtime,
                                nodes=incompatible,
                                scope="requested_node_instance_ids",
                            )
                        )
                return selected_nodes

            if requested_node_ids:
                discovered_node_map = _build_unique_node_id_map(discovered_nodes, requested_ids=requested_node_ids)
                missing_node_ids = [node_id for node_id in requested_node_ids if node_id not in discovered_node_map]
                if missing_node_ids:
                    raise _RetryableReadyError(
                        f"requested node_ids not found in current discovery scope: {missing_node_ids}"
                    )
                selected_nodes = [discovered_node_map[node_id] for node_id in requested_node_ids]
                if normalized_runtime:
                    incompatible = [
                        node
                        for node in selected_nodes
                        if str(node.python_version or "").strip()
                        and not matches_python_runtime(node.python_version, normalized_runtime)
                    ]
                    if incompatible:
                        raise RuntimeError(
                            runtime_mismatch_message_for_nodes(
                                requested_runtime=normalized_runtime,
                                nodes=incompatible,
                                scope="requested_node_ids",
                            )
                        )
                return selected_nodes

            candidate_nodes = [
                node
                for node in discovered_nodes
                if node.healthy and node.schedulable and not node.drain
            ]
            if normalized_runtime:
                candidate_nodes = _filter_nodes_by_runtime(candidate_nodes, runtime=normalized_runtime)
            if not candidate_nodes:
                if normalized_runtime:
                    raise RuntimeError(
                        runtime_mismatch_message_for_nodes(
                            requested_runtime=normalized_runtime,
                            nodes=discovered_nodes,
                            scope="nodes",
                        )
                    )
                raise _RetryableReadyError(
                    f"no schedulable nodes from InfoCenter; target={infocenter_target}; "
                    f"candidates={_summarize_discovered_nodes(discovered_nodes)}"
                )
            candidate_nodes.sort(
                key=lambda node: (
                    -int(node.service_worker_available),
                    -int(node.capacity),
                    int(node.queued),
                    node.node_id,
                )
            )
            effective_node_count = max(1, desired_node_count or required_success_nodes)
            selected_nodes = candidate_nodes[:effective_node_count]
            if len(selected_nodes) < required_success_nodes:
                raise _RetryableReadyError(
                    "not enough schedulable nodes from InfoCenter: "
                    f"selected={len(selected_nodes)} required={required_success_nodes}"
                )
            return selected_nodes

        def _discover_and_select_nodes() -> Tuple[Sequence[InfoCenterServiceRoute], Sequence[InfoCenterNode], Sequence[InfoCenterNode]]:
            existing_routes, discovered_nodes = _discover_from_infocenter()
            selected_nodes = _select_nodes_from_discovery(existing_routes, discovered_nodes)
            return existing_routes, discovered_nodes, selected_nodes

        discovery_wait_timeout = _ready_retry_timeout(timeout_sec, grace_sec=_SERVICE_READY_GRACE_SEC)
        try:
            if discovery_wait_timeout > 0.0:
                selection_result = _retry_infocenter_request(
                    _discover_and_select_nodes,
                    timeout_sec=discovery_wait_timeout,
                    target=infocenter_target,
                    action="service deployment discovery",
                    retry_interval_sec=_SERVICE_READY_RETRY_INTERVAL_SEC,
                )
                if isinstance(selection_result, tuple) and len(selection_result) == 3:
                    existing_routes, discovered_nodes, selected_nodes = selection_result
                elif isinstance(selection_result, tuple) and len(selection_result) == 2:
                    existing_routes, discovered_nodes = selection_result
                    selected_nodes = _select_nodes_from_discovery(existing_routes, discovered_nodes)
                else:
                    raise RuntimeError(f"unexpected service discovery result: {selection_result!r}")
            else:
                existing_routes, discovered_nodes, selected_nodes = _discover_and_select_nodes()
        except RuntimeError as exc:
            message = str(exc)
            if "no available nodes" in message:
                _emit_owner_notice(
                    "deploy failed: no available nodes "
                    f"target={infocenter_target} healthy_only={healthy_only} tags={list(tags or ())}"
                )
            elif "no schedulable nodes" in message or "not enough schedulable nodes" in message:
                _emit_owner_notice(
                    "deploy failed: no schedulable nodes "
                    f"target={infocenter_target} candidates=retry_exhausted"
                )
            raise

        discovered_instance_map = {_node_instance_key_from_node(node): node for node in discovered_nodes}

        if ensure_unique_service_name:
            active_routes = cls._select_active_routes(existing_routes)
            if active_routes:
                existing_infos = cls._inspect_existing_routes(active_routes=active_routes, timeout_sec=timeout_sec)
                existing_infos = [
                    (route, info)
                    for route, info in existing_infos
                    if cls._is_active_service_status(getattr(info, "status", route.status))
                ]
                if not existing_infos:
                    _emit_owner_notice(
                        f"ignore stale existing routes service_name={effective_service_name}; redeploying fresh replicas"
                    )
                else:
                    existing_owners = {info.owner_client_id for _, info in existing_infos}
                    existing_versions = {info.code_version for _, info in existing_infos}
                    if len(existing_owners) != 1 or len(existing_versions) != 1:
                        raise RuntimeError(
                            f"service_name already exists but active routes are inconsistent: {effective_service_name}"
                        )

                    existing_owner = next(iter(existing_owners))
                    existing_code_version = next(iter(existing_versions))
                    if existing_owner != effective_owner_client_id:
                        raise RuntimeError(
                            f"service_name already exists and belongs to another owner: "
                            f"service_name={effective_service_name}; owner={existing_owner}"
                        )

                    cached_session = _load_service_session_cache(
                        owner_client_id=effective_owner_client_id,
                        service_name=effective_service_name,
                        cache_dir=session_cache_dir,
                    )

                    if existing_code_version == effective_code_version:
                        if not reuse_existing_same_code:
                            raise RuntimeError(
                                f"service_name already exists with same code_version: {effective_service_name}; "
                                "set reuse_existing_same_code=True to reuse"
                            )
                        if cached_session is None or cached_session.get("artifact_code_version") != effective_code_version:
                            raise RuntimeError(
                                f"service_name already exists with same code_version but no reusable local token cache was found: "
                                f"{effective_service_name}"
                            )
                        try:
                            session_cache_lock = _acquire_service_session_lock_with_retry(
                                session_cache_file,
                                timeout_sec=timeout_sec,
                                action=(
                                    "another local deploy process is already active for "
                                    f"owner_client_id={effective_owner_client_id!r} service_name={effective_service_name!r}"
                                ),
                            )
                        except RuntimeError as exc:
                            raise RuntimeError(str(exc)) from exc
                        try:
                            group = cls._reuse_existing_group(
                                owner_client_id=effective_owner_client_id,
                                service_name=effective_service_name,
                                artifact_code_version=effective_code_version,
                                cache_payload=cached_session,
                                active_routes=existing_infos,
                                discovered_node_map=discovered_instance_map,
                                timeout_sec=timeout_sec,
                                breaker_enabled=breaker_enabled,
                                breaker_failure_threshold=breaker_failure_threshold,
                                breaker_cooldown_sec=breaker_cooldown_sec,
                                breaker_max_cooldown_sec=breaker_max_cooldown_sec,
                                session_cache_file=session_cache_file,
                                session_cache_lock=session_cache_lock,
                            )
                        except RuntimeError as exc:
                            if "service is stopped" not in str(exc):
                                raise
                            with contextlib.suppress(Exception):
                                session_cache_file.unlink()
                            _emit_owner_notice(
                                f"reuse existing service skipped because cached route stopped: {effective_service_name}; redeploying"
                            )
                        else:
                            _emit_owner_notice(
                                f"reuse existing service service_name={effective_service_name} nodes={list(group.sessions.keys())}"
                            )
                            return group

                    else:
                        raise RuntimeError(
                            f"service_name already exists with different code_version and is still running: "
                            f"{effective_service_name}; existing={existing_code_version}; incoming={effective_code_version}; "
                            "stop the active service first, then redeploy with the same service_name"
                        )

        try:
            try:
                session_cache_lock = _acquire_service_session_lock_with_retry(
                    session_cache_file,
                    timeout_sec=timeout_sec,
                    action=(
                        "another local deploy process is already active for "
                        f"owner_client_id={effective_owner_client_id!r} service_name={effective_service_name!r}"
                    ),
                )
            except RuntimeError as exc:
                raise RuntimeError(str(exc)) from exc
            sessions: Dict[str, ServiceSessionClient] = {}
            clients: Dict[str, NodeControlClient] = {}
            nodes: Dict[str, InfoCenterNode] = {}
            failures: Dict[str, str] = {}

            for node in selected_nodes:
                client = _node_control_client(node.control_addr, timeout_sec=timeout_sec)
                node_worker_count = max(1, int(worker_count or 1))
                if int(getattr(node, "service_worker_available", 0) or 0) > 0:
                    node_worker_count = max(1, min(node_worker_count, int(getattr(node, "service_worker_available", 0) or 0)))
                try:
                    session = client.create_service_from_bytes(
                        owner_client_id=effective_owner_client_id,
                        service_name=effective_service_name,
                        blob=effective_blob,
                        runtime=runtime,
                        entry_module=effective_entry_module,
                        entry_callable=entry_callable,
                        package_format=effective_package_format,
                        export_mode=export_mode,
                        export_methods=export_methods,
                        deps=prepared_artifact.dependency_policy,
                        managed_global_names=managed_global_names,
                        worker_count=node_worker_count,
                        heartbeat_timeout_sec=heartbeat_timeout_sec,
                        idle_ttl_sec=idle_ttl_sec,
                        expose_http=expose_http,
                        chunk_size=chunk_size,
                    )
                except Exception as exc:
                    failures[_node_instance_key_from_node(node)] = repr(exc)
                    client.close()
                    if not allow_partial:
                        cls._cleanup_created_services(sessions=sessions, clients=clients, reason="rollback deploy")
                        raise RuntimeError(
                            f"deploy failed on node={node.node_id}/{_node_instance_key_from_node(node)}: {exc}"
                        ) from exc
                    continue

                node_key = _node_instance_key_from_node(node)
                session.node_instance_id = node_key
                session.node_id = str(node.node_id or "")
                sessions[node_key] = session
                clients[node_key] = client
                nodes[node_key] = node

            if len(sessions) < required_success_nodes:
                cls._cleanup_created_services(sessions=sessions, clients=clients, reason="insufficient success nodes")
                _emit_owner_notice(
                    "deploy failed: insufficient success nodes "
                    f"service_name={effective_service_name} success={len(sessions)} "
                    f"required={required_success_nodes} failures={failures}"
                )
                raise RuntimeError(
                    f"deploy success nodes={len(sessions)} < min_success_nodes={required_success_nodes}; "
                    f"failures={failures}"
                )

            group = cls(
                owner_client_id=effective_owner_client_id,
                service_name=effective_service_name,
                sessions=sessions,
                nodes=nodes,
                failures=failures,
                breaker_enabled=bool(breaker_enabled),
                breaker_failure_threshold=max(1, int(breaker_failure_threshold)),
                breaker_cooldown_sec=max(0.1, float(breaker_cooldown_sec)),
                breaker_max_cooldown_sec=max(0.1, float(breaker_max_cooldown_sec)),
                _clients=clients,
                _session_cache_file=session_cache_file,
                _session_cache_lock=session_cache_lock,
                _artifact_code_version=effective_code_version,
            )
            group._persist_session_cache()
            group._start_keepalive()
            deployed_nodes = list(sessions.keys())
            if failures:
                _emit_owner_notice(
                    "deploy success with partial failures "
                    f"service_name={effective_service_name} nodes={deployed_nodes} failures={failures}"
                )
            else:
                _emit_owner_notice(
                    f"deploy success service_name={effective_service_name} nodes={deployed_nodes}"
                )
            return group
        except Exception:
            if session_cache_lock is not None:
                session_cache_lock.close()
            raise

    @classmethod
    def deploy_from_func(
        cls,
        *,
        infocenter_target: str,
        func: Callable,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        deps: Optional[Any] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: str = "run",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 5.0,
        breaker_max_cooldown_sec: float = 120.0,
    ) -> "Service":
        return cls.deploy_from_infocenter(
            infocenter_target=infocenter_target,
            owner_client_id=owner_client_id,
            service_name=service_name,
            deps=deps,
            func=func,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            export_mode=export_mode,
            export_methods=export_methods,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=managed_global_names,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            expose_http=expose_http,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            allow_partial=allow_partial,
            min_success_nodes=min_success_nodes,
            timeout_sec=timeout_sec,
            ensure_unique_service_name=ensure_unique_service_name,
            reuse_existing_same_code=reuse_existing_same_code,
            replace_existing_if_code_changed=replace_existing_if_code_changed,
            session_cache_dir=session_cache_dir,
            breaker_enabled=breaker_enabled,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown_sec=breaker_cooldown_sec,
            breaker_max_cooldown_sec=breaker_max_cooldown_sec,
        )

    @classmethod
    def deploy_from_file(
        cls,
        *,
        infocenter_target: str,
        artifact_path: str,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        deps: Optional[Any] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 5.0,
        breaker_max_cooldown_sec: float = 120.0,
    ) -> "Service":
        return cls.deploy_from_infocenter(
            infocenter_target=infocenter_target,
            owner_client_id=owner_client_id,
            service_name=service_name,
            deps=deps,
            artifact_path=artifact_path,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            export_mode=export_mode,
            export_methods=export_methods,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=managed_global_names,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            expose_http=expose_http,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            allow_partial=allow_partial,
            min_success_nodes=min_success_nodes,
            timeout_sec=timeout_sec,
            ensure_unique_service_name=ensure_unique_service_name,
            reuse_existing_same_code=reuse_existing_same_code,
            replace_existing_if_code_changed=replace_existing_if_code_changed,
            session_cache_dir=session_cache_dir,
            breaker_enabled=breaker_enabled,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown_sec=breaker_cooldown_sec,
            breaker_max_cooldown_sec=breaker_max_cooldown_sec,
        )

    @classmethod
    def deploy_from_bytes(
        cls,
        *,
        infocenter_target: str,
        blob: bytes,
        entry_module: Any = "",
        entry_callable: str = "run",
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        deps: Optional[Any] = None,
        runtime: str = "py3",
        package_format: str = "py",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 5.0,
        breaker_max_cooldown_sec: float = 120.0,
    ) -> "Service":
        return cls.deploy_from_infocenter(
            infocenter_target=infocenter_target,
            owner_client_id=owner_client_id,
            service_name=service_name,
            deps=deps,
            blob=blob,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            export_mode=export_mode,
            export_methods=export_methods,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=managed_global_names,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            expose_http=expose_http,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            allow_partial=allow_partial,
            min_success_nodes=min_success_nodes,
            timeout_sec=timeout_sec,
            ensure_unique_service_name=ensure_unique_service_name,
            reuse_existing_same_code=reuse_existing_same_code,
            replace_existing_if_code_changed=replace_existing_if_code_changed,
            session_cache_dir=session_cache_dir,
            breaker_enabled=breaker_enabled,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown_sec=breaker_cooldown_sec,
            breaker_max_cooldown_sec=breaker_max_cooldown_sec,
        )

    @staticmethod
    def _is_active_service_status(status: int) -> bool:
        return int(status or 0) in (
            pb2.SERVICE_STATUS_STARTING,
            pb2.SERVICE_STATUS_RUNNING,
            pb2.SERVICE_STATUS_DRAINING,
        )

    @staticmethod
    def _select_active_routes(routes: Sequence[InfoCenterServiceRoute]) -> List[InfoCenterServiceRoute]:
        return [
            route
            for route in routes
            if Service._is_active_service_status(route.status)
        ]

    @classmethod
    def _inspect_existing_routes(
        cls,
        *,
        active_routes: Sequence[InfoCenterServiceRoute],
        timeout_sec: float,
    ) -> List[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]]:
        out: List[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]] = []
        failures: Dict[str, str] = {}
        for route in active_routes:
            client = _node_control_client(route.control_addr, timeout_sec=timeout_sec)
            try:
                info = client.get_service_status(service_id=route.service_id)
                out.append((route, info))
            except Exception as exc:
                failures[_node_instance_key_from_route(route)] = repr(exc)
            finally:
                client.close()
        if failures:
            raise RuntimeError(f"failed to inspect existing active service routes: {failures}")
        return out

    @classmethod
    def _reuse_existing_group(
        cls,
        *,
        owner_client_id: str,
        service_name: str,
        artifact_code_version: str,
        cache_payload: Dict[str, object],
        active_routes: Sequence[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]],
        discovered_node_map: Dict[str, InfoCenterNode],
        timeout_sec: float,
        breaker_enabled: bool,
        breaker_failure_threshold: int,
        breaker_cooldown_sec: float,
        breaker_max_cooldown_sec: float,
        session_cache_file: Path,
        session_cache_lock: _ServiceSessionFileLock,
    ) -> "Service":
        cache_nodes = cache_payload.get("nodes")
        if not isinstance(cache_nodes, dict):
            raise RuntimeError("invalid local service session cache: nodes missing")

        sessions: Dict[str, ServiceSessionClient] = {}
        clients: Dict[str, NodeControlClient] = {}
        nodes: Dict[str, InfoCenterNode] = {}

        try:
            for route, info in active_routes:
                route_key = _node_instance_key_from_route(route)
                node = discovered_node_map.get(route_key)
                if node is None:
                    raise RuntimeError(
                        f"existing service route is outside current discovery scope: node_instance_id={route_key}"
                    )

                cached_node = cache_nodes.get(route_key)
                if not isinstance(cached_node, dict):
                    raise RuntimeError(
                        f"local service session cache missing node entry for reuse: node_instance_id={route_key}"
                    )

                cached_service_id = str(cached_node.get("service_id", "")).strip()
                cached_token = str(cached_node.get("service_token", "")).strip()
                if cached_service_id != route.service_id:
                    raise RuntimeError(
                        f"local service session cache is stale for node_instance_id={route_key}: "
                        f"cached_service_id={cached_service_id} route_service_id={route.service_id}"
                    )
                if not cached_token:
                    raise RuntimeError(f"local service session cache missing token for node_instance_id={route_key}")

                client = _node_control_client(route.control_addr, timeout_sec=timeout_sec)
                try:
                    hb = client.heartbeat_service(
                        owner_client_id=owner_client_id,
                        service_id=route.service_id,
                        service_token=cached_token,
                        seq=0,
                    )
                except Exception:
                    client.close()
                    raise

                sessions[route_key] = ServiceSessionClient(
                    _client=client,
                    owner_client_id=owner_client_id,
                    service_id=route.service_id,
                    service_token=cached_token,
                    code_version=str(info.code_version or ""),
                    http_base_url=str(cached_node.get("http_base_url", "") or info.http_base_url or route.http_base_url),
                    heartbeat_timeout_sec=max(
                        1,
                        int(
                            cached_node.get("heartbeat_timeout_sec", 0)
                            or (max(1, int(hb.next_heartbeat_in_sec or 0)) * 2)
                            or 30
                        ),
                    ),
                    worker_count=max(1, int(cached_node.get("worker_count", 0) or info.worker_count or route.worker_count or 1)),
                    status=hb.status or info.status,
                    service_name=str(info.service_name or route.service_name or ""),
                    node_instance_id=str(route_key or ""),
                    node_id=str(node.node_id or route.node_id or ""),
                    created_at=_timestamp_to_datetime(info.created_at),
                    last_heartbeat_at=_timestamp_to_datetime(info.last_heartbeat_at),
                    lease_expire_at=_timestamp_to_datetime(info.lease_expire_at),
                )
                clients[route_key] = client
                nodes[route_key] = node
        except Exception:
            for client in clients.values():
                try:
                    client.close()
                except Exception:
                    pass
            try:
                session_cache_lock.close()
            except Exception:
                pass
            raise

        group = cls(
            owner_client_id=owner_client_id,
            service_name=service_name,
            sessions=sessions,
            nodes=nodes,
            failures={},
            breaker_enabled=bool(breaker_enabled),
            breaker_failure_threshold=max(1, int(breaker_failure_threshold)),
            breaker_cooldown_sec=max(0.1, float(breaker_cooldown_sec)),
            breaker_max_cooldown_sec=max(0.1, float(breaker_max_cooldown_sec)),
            _clients=clients,
            _session_cache_file=session_cache_file,
            _session_cache_lock=session_cache_lock,
            _artifact_code_version=artifact_code_version,
        )
        group._persist_session_cache()
        group._start_keepalive()
        return group

    @classmethod
    def _end_existing_group(
        cls,
        *,
        owner_client_id: str,
        cache_payload: Dict[str, object],
        active_routes: Sequence[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]],
        timeout_sec: float,
        reason: str,
    ) -> None:
        cache_nodes = cache_payload.get("nodes")
        if not isinstance(cache_nodes, dict):
            raise RuntimeError("invalid local service session cache: nodes missing")

        failures: Dict[str, str] = {}
        for route, _info in active_routes:
            route_key = _node_instance_key_from_route(route)
            cached_node = cache_nodes.get(route_key)
            if not isinstance(cached_node, dict):
                failures[route_key] = "missing cached node entry"
                continue
            cached_service_id = str(cached_node.get("service_id", "")).strip()
            cached_token = str(cached_node.get("service_token", "")).strip()
            if cached_service_id != route.service_id or not cached_token:
                failures[route_key] = "stale or missing cached token"
                continue

            client = _node_control_client(route.control_addr, timeout_sec=timeout_sec)
            try:
                client.end_service(
                    owner_client_id=owner_client_id,
                    service_id=route.service_id,
                    service_token=cached_token,
                    reason=reason,
                )
            except Exception as exc:
                failures[route_key] = repr(exc)
            finally:
                client.close()

        if failures:
            raise RuntimeError(f"failed to end existing active service before replace: {failures}")

    @staticmethod
    def _cleanup_created_services(
        *,
        sessions: Dict[str, ServiceSessionClient],
        clients: Dict[str, NodeControlClient],
        reason: str,
    ) -> None:
        for session in sessions.values():
            try:
                session.end(reason)
            except Exception:
                pass
        for client in clients.values():
            try:
                client.close()
            except Exception:
                pass

    def _persist_session_cache(self) -> None:
        if self._session_cache_file is None or not self.sessions:
            return
        payload: Dict[str, object] = {
            "schema_version": _SERVICE_SESSION_SCHEMA_VERSION,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "owner_client_id": self.owner_client_id,
            "service_name": self.service_name,
            "artifact_code_version": self._artifact_code_version,
            "nodes": {},
        }
        nodes_payload: Dict[str, object] = {}
        for node_key, session in sorted(self.sessions.items()):
            node = self.nodes.get(node_key)
            control_addr = ""
            if node is not None:
                control_addr = node.control_addr
            elif node_key in self._clients:
                control_addr = self._clients[node_key].target
            nodes_payload[node_key] = {
                "node_id": str(node.node_id if node is not None else ""),
                "control_addr": control_addr,
                "service_id": session.service_id,
                "service_token": session.service_token,
                "http_base_url": session.http_base_url,
                "heartbeat_timeout_sec": int(session.heartbeat_timeout_sec),
                "worker_count": int(session.worker_count),
            }
        payload["nodes"] = nodes_payload
        if self._session_cache_lock is not None:
            self._session_cache_lock.write_json(payload)
        else:
            _write_private_json(self._session_cache_file, payload)

    def _clear_session_cache(self) -> None:
        if self._session_cache_file is None:
            return
        if self._session_cache_lock is not None:
            self._session_cache_lock.clear()
            self._delete_session_cache_on_close = True
            return
        try:
            self._session_cache_file.unlink()
        except FileNotFoundError:
            pass

    def __post_init__(self) -> None:
        self._init_execution_session_state()
        if self.breaker_max_cooldown_sec < self.breaker_cooldown_sec:
            self.breaker_max_cooldown_sec = self.breaker_cooldown_sec
        for node_id in self.sessions:
            self._breaker_states.setdefault(node_id, NodeCircuitState())

    def _breaker_state_locked(self, node_id: str) -> NodeCircuitState:
        state = self._breaker_states.get(node_id)
        if state is None:
            state = NodeCircuitState()
            self._breaker_states[node_id] = state
        return state

    def _breaker_cooldown_locked(self, state: NodeCircuitState) -> float:
        exp = max(0, state.open_count - 1)
        cooldown = self.breaker_cooldown_sec * (2.0**exp)
        return min(self.breaker_max_cooldown_sec, cooldown)

    def _breaker_mark_success(self, node_id: str) -> None:
        if not self.breaker_enabled:
            return
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            state.state = "closed"
            state.consecutive_failures = 0
            state.open_until_monotonic = 0.0
            state.open_count = 0
            state.probe_in_flight = False
            state.last_error = ""

    def _breaker_mark_failure(self, node_id: str, exc: Exception) -> None:
        if not self.breaker_enabled:
            return
        now = time.monotonic()
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            state.last_error = repr(exc)
            if state.state == "half_open":
                state.consecutive_failures = max(state.consecutive_failures, self.breaker_failure_threshold)
            elif state.state == "closed":
                state.consecutive_failures += 1
            state.probe_in_flight = False

            if state.consecutive_failures < self.breaker_failure_threshold:
                return

            state.state = "open"
            state.open_count += 1
            state.open_until_monotonic = now + self._breaker_cooldown_locked(state)

    def _breaker_candidate_state(self, node_id: str) -> Tuple[str, bool]:
        if not self.breaker_enabled:
            return "closed", True
        now = time.monotonic()
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            if state.state == "open":
                if now >= state.open_until_monotonic:
                    state.state = "half_open"
                    state.probe_in_flight = False
                else:
                    return state.state, False
            if state.state == "half_open" and state.probe_in_flight:
                return state.state, False
            return state.state, True

    def _breaker_before_invoke(self, node_id: str) -> bool:
        if not self.breaker_enabled:
            return True
        now = time.monotonic()
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            if state.state == "open":
                if now < state.open_until_monotonic:
                    return False
                state.state = "half_open"
                state.probe_in_flight = False
            if state.state == "half_open":
                if state.probe_in_flight:
                    return False
                state.probe_in_flight = True
            return True

    def breaker_snapshot(self) -> Dict[str, Dict[str, object]]:
        now = time.monotonic()
        out: Dict[str, Dict[str, object]] = {}
        with self._route_lock:
            for node_id, state in self._breaker_states.items():
                remain = max(0.0, state.open_until_monotonic - now) if state.state == "open" else 0.0
                out[node_id] = {
                    "state": state.state,
                    "consecutive_failures": state.consecutive_failures,
                    "open_count": state.open_count,
                    "cooldown_remaining_sec": round(remain, 3),
                    "probe_in_flight": state.probe_in_flight,
                    "last_error": state.last_error,
                }
        return out

    def put_object_from_file(
        self,
        file_path: str,
        *,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> DataRef:
        refs = [
            client.upload_object_from_file(
                file_path=file_path,
                format=format,
                chunk_size=chunk_size,
            )
            for client in self._clients.values()
        ]
        if not refs:
            raise RuntimeError("no node clients available for object upload")
        object_ids = {ref.object_id for ref in refs}
        formats = {ref.format for ref in refs}
        if len(object_ids) != 1 or len(formats) != 1:
            raise RuntimeError(f"inconsistent object upload across nodes: {refs}")
        return refs[0]

    def put_object_from_bytes(
        self,
        blob: bytes,
        *,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> DataRef:
        refs = [
            client.upload_object_from_bytes(
                blob=blob,
                format=format,
                chunk_size=chunk_size,
            )
            for client in self._clients.values()
        ]
        if not refs:
            raise RuntimeError("no node clients available for object upload")
        object_ids = {ref.object_id for ref in refs}
        formats = {ref.format for ref in refs}
        if len(object_ids) != 1 or len(formats) != 1:
            raise RuntimeError(f"inconsistent object upload across nodes: {refs}")
        return refs[0]

    def put_data(
        self,
        data: Any,
        *,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> DataRef:
        return _put_data_via_clients(
            list(self._clients.values()),
            data,
            format=format,
            chunk_size=chunk_size,
        )

    def put_dataframe(self, dataframe: Any, *, chunk_size: int = OBJECT_CHUNK_SIZE_BYTES) -> DataRef:
        return self.put_data(dataframe, format="parquet", chunk_size=chunk_size)

    def put_ndarray(self, array: Any, *, chunk_size: int = OBJECT_CHUNK_SIZE_BYTES) -> DataRef:
        return self.put_data(array, format="npy", chunk_size=chunk_size)

    def put_json(self, value: Any, *, chunk_size: int = OBJECT_CHUNK_SIZE_BYTES) -> DataRef:
        return self.put_data(value, format="json", chunk_size=chunk_size)

    def update_globals(self, values: Dict[str, object]) -> str:
        with self._route_lock:
            sessions_snapshot = list(self.sessions.items())
            clients_snapshot = dict(self._clients)
        active_clients = [clients_snapshot[node_id] for node_id, _ in sessions_snapshot if node_id in clients_snapshot]
        prepared_values = _prepare_managed_globals_values_for_upload(active_clients, values)
        digests: Dict[str, str] = {}
        failed_nodes: Dict[str, str] = {}
        for node_id, session in sessions_snapshot:
            if getattr(session, "failed", False):
                failed_nodes[node_id] = str(getattr(session, "last_error", "") or "session failed")
                continue
            try:
                resp = session.update_globals_prepared(prepared_values)
                digests[node_id] = resp.globals_digest
            except Exception as exc:
                failed_nodes[node_id] = repr(exc)

        for node_id, message in failed_nodes.items():
            with self._route_lock:
                self.failures[node_id] = message
                self.sessions.pop(node_id, None)
                self.nodes.pop(node_id, None)
                client = self._clients.pop(node_id, None)
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

        if not digests:
            raise RuntimeError(f"update_globals failed on all nodes: {failed_nodes}")
        self.globals_digests = dict(digests)
        unique = {digest for digest in digests.values() if str(digest).strip()}
        return next(iter(unique), "") if len(unique) == 1 else next(iter(digests.values()))

    def __enter__(self) -> "Service":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(end_services=False)

    def node_ids(self) -> Sequence[str]:
        return [self.nodes[key].node_id if key in self.nodes else key for key in self.sessions.keys()]

    def node_instance_ids(self) -> Sequence[str]:
        return list(self.sessions.keys())

    def _resolve_node_key(self, node_ref: str) -> str:
        normalized = str(node_ref or "").strip()
        if not normalized:
            raise KeyError("node reference is required")
        if normalized in self.sessions:
            return normalized
        matched = [node_key for node_key, node in self.nodes.items() if str(node.node_id or "").strip() == normalized]
        if len(matched) == 1:
            return matched[0]
        if len(matched) > 1:
            raise KeyError(
                f"ambiguous node_id: {normalized}; multiple live node instances match. Please use node_instance_id instead."
            )
        raise KeyError(f"unknown node reference: {normalized}")

    def _start_keepalive(self, interval_sec: Optional[float] = None) -> None:
        super()._start_keepalive(interval_sec=interval_sec)

    def join(
        self,
        *,
        poll_interval_sec: float = 1.0,
        end_services_on_interrupt: bool = True,
        end_reason: str = "owner interrupted",
    ) -> None:
        wait_sec = max(0.1, float(poll_interval_sec))
        try:
            while True:
                with self._hb_lock:
                    thread = self._hb_thread
                if thread is None or not thread.is_alive():
                    self._sync_failures_from_replicas()
                    if self.failures:
                        _emit_owner_notice(
                            f"owner keepalive stopped service_name={self.service_name} failures={self.failures}"
                        )
                    return
                time.sleep(wait_sec)
        except KeyboardInterrupt:
            if end_services_on_interrupt:
                self.end(reason=end_reason)
            else:
                self._stop_keepalive()

    def _stop_keepalive(self) -> None:
        super()._stop_keepalive()

    def status_map(self) -> Dict[str, pb2.ServiceStatusInfo]:
        out: Dict[str, pb2.ServiceStatusInfo] = {}
        for node_key, session in self.sessions.items():
            out[node_key] = session.get_status()
        return out

    def call_on_node(
        self,
        node_id: str,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
    ) -> Dict[str, object]:
        node_key = self._resolve_node_key(node_id)
        session = self.sessions.get(node_key)
        if session is None:
            raise KeyError(f"unknown node reference: {node_id}")
        return session.call(method, payload, timeout_sec=timeout_sec)

    def _select_node(self, *, strategy: str, refresh_status: bool, exclude: Optional[Set[str]] = None) -> str:
        excluded = exclude or set()
        all_candidates = [nid for nid in sorted(self.sessions.keys()) if nid not in excluded]
        candidates = []
        state_rank: Dict[str, int] = {}
        for node_id in all_candidates:
            breaker_state, allowed = self._breaker_candidate_state(node_id)
            if not allowed:
                continue
            # Prefer closed nodes over half-open probe nodes.
            state_rank[node_id] = 0 if breaker_state == "closed" else 1
            candidates.append(node_id)
        if not candidates:
            raise RuntimeError("no available service node (all candidates may be open-circuit)")

        if strategy == "round_robin":
            ranked_candidates = sorted(candidates, key=lambda node_id: (state_rank.get(node_id, 0), node_id))
            with self._route_lock:
                idx = self._route_index % len(ranked_candidates)
                self._route_index += 1
            return ranked_candidates[idx]

        if strategy != "least_inflight":
            raise ValueError("strategy must be one of: least_inflight, round_robin")

        best_node_id = ""
        best_key: Optional[Tuple[int, int, int, str]] = None
        for node_id in candidates:
            session = self.sessions[node_id]
            info: Optional[pb2.ServiceStatusInfo] = None
            if refresh_status:
                try:
                    info = session.get_status()
                except Exception:
                    continue
                if info.status != pb2.SERVICE_STATUS_RUNNING:
                    continue
            in_flight = int(info.in_flight if info is not None else 0)
            alive_workers = int(info.alive_workers if info is not None else session.worker_count)
            key = (state_rank.get(node_id, 0), in_flight, -alive_workers, node_id)
            if best_key is None or key < best_key:
                best_key = key
                best_node_id = node_id

        if best_node_id:
            return best_node_id

        with self._route_lock:
            idx = self._route_index % len(candidates)
            self._route_index += 1
        return candidates[idx]

    def call_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        if not self.sessions:
            raise RuntimeError("no active service sessions")

        # 序列化 Arrow 兼容对象
        serialized_payload = _serialize_arrow_compatible(payload)

        tries = max(1, int(max_attempts or len(self.sessions)))
        excluded: Set[str] = set()
        last_error: Optional[Exception] = None

        for _ in range(tries):
            node_id = self._select_node(strategy=strategy, refresh_status=refresh_status, exclude=excluded)
            excluded.add(node_id)
            if not self._breaker_before_invoke(node_id):
                continue
            try:
                resp = self.sessions[node_id].call(method, serialized_payload, timeout_sec=timeout_sec)
                self._breaker_mark_success(node_id)
                return node_id, resp
            except Exception as exc:
                last_error = exc
                self._breaker_mark_failure(node_id, exc)

        raise RuntimeError(f"call failed on all candidate nodes: {last_error}")

    async def acall_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        """异步版本的 call_balanced。

        使用 asyncio 在线程池中执行同步 HTTP 调用，不阻塞事件循环。

        Args:
            method: 服务方法名
            payload: 调用参数
            timeout_sec: 超时时间
            strategy: 节点选择策略（"least_inflight" 或 "round_robin"）
            refresh_status: 是否在选择节点前刷新状态
            max_attempts: 最大尝试次数
        Returns:
            Tuple[str, Dict[str, object]]: (节点 ID, 响应结果)

        Raises:
            RuntimeError: 所有节点都调用失败时
        """
        if not self.sessions:
            raise RuntimeError("no active service sessions")

        tries = max(1, int(max_attempts or len(self.sessions)))
        excluded: Set[str] = set()
        last_error: Optional[Exception] = None

        loop = asyncio.get_running_loop()
        serialized_payload = _serialize_arrow_compatible(payload)
        for _ in range(tries):
            node_id = self._select_node(strategy=strategy, refresh_status=refresh_status, exclude=excluded)
            excluded.add(node_id)
            if not self._breaker_before_invoke(node_id):
                continue
            try:
                # 在线程池中执行同步调用，不阻塞事件循环
                resp = await loop.run_in_executor(
                    None,
                    lambda nid=node_id: self.sessions[nid].call(method, serialized_payload, timeout_sec=timeout_sec),
                )
                self._breaker_mark_success(node_id)
                return node_id, resp
            except Exception as exc:
                last_error = exc
                self._breaker_mark_failure(node_id, exc)

        raise RuntimeError(f"call failed on all candidate nodes: {last_error}")

    async def acall_all(
        self,
        method: str,
        payloads: Union[List[Dict[str, object]], Dict[str, object]],
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        """并发调用所有节点。

        将 payload 同时发送到所有可用节点，返回所有结果。

        Args:
            method: 服务方法名
            payloads: 可以是单个 payload（发送给所有节点）或 payload 列表（与节点一一对应）
            timeout_sec: 单次调用超时时间
            max_concurrency: 最大并发数

        Returns:
            List[Tuple[节点ID, 响应, 异常]]：所有节点的结果列表
        """
        if not self.sessions:
            raise RuntimeError("no active service sessions")

        nodes = list(self.sessions.keys())
        # 如果是单个 payload，复制给所有节点
        if isinstance(payloads, dict):
            shared_payload = _serialize_arrow_compatible(payloads)
            payloads = [dict(shared_payload) for _ in nodes]
        elif isinstance(payloads, list):
            if len(payloads) != len(nodes):
                raise ValueError(f"payload list length ({len(payloads)}) must match node count ({len(nodes)})")
            payloads = [_serialize_arrow_compatible(payload) for payload in payloads]

        loop = asyncio.get_running_loop()
        semaphore = asyncio.Semaphore(max_concurrency)

        async def _call_single(node_id: str, payload: Dict[str, object]) -> Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]:
            async with semaphore:
                if not self._breaker_before_invoke(node_id):
                    return node_id, None, RuntimeError("circuit breaker open")
                try:
                    resp = await loop.run_in_executor(
                        None,
                        lambda nid=node_id: self.sessions[nid].call(method, payload, timeout_sec=timeout_sec),
                    )
                    self._breaker_mark_success(node_id)
                    return node_id, resp, None
                except Exception as exc:
                    self._breaker_mark_failure(node_id, exc)
                    return node_id, None, exc

        tasks = [_call_single(node_id, payload) for node_id, payload in zip(nodes, payloads)]
        return await asyncio.gather(*tasks)

    def end(self, reason: str = "group end") -> Dict[str, Optional[pb2.EndServiceResponse]]:
        self._stop_keepalive()
        out: Dict[str, Optional[pb2.EndServiceResponse]] = {}
        for node_id, session in self.sessions.items():
            try:
                out[node_id] = session.end(reason)
            except Exception:
                out[node_id] = None
        if out and all(
            resp is not None and resp.ok and resp.accepted and resp.status == pb2.SERVICE_STATUS_STOPPED
            for resp in out.values()
        ):
            self._clear_session_cache()
        return out

    def close(self, *, end_services: bool = False, reason: str = "group close") -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_keepalive()
        if end_services:
            self.end(reason=reason)
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                pass
        self._clients.clear()
        if self._session_cache_lock is not None:
            self._session_cache_lock.close()
            self._session_cache_lock = None
        if self._delete_session_cache_on_close and self._session_cache_file is not None:
            try:
                self._session_cache_file.unlink()
            except FileNotFoundError:
                pass
            self._delete_session_cache_on_close = False

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        if self._discovered_methods is None:
            self._ensure_methods_discovered()
        if self._discovered_methods is not None and name not in self._discovered_methods:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{name}'. "
                f"Available methods: {self._discovered_methods}"
            )
        return _CallProxy(
            method=name,
            group=self,
            timeout_sec=60.0,
            strategy="least_inflight",
            refresh_status=True,
        )

    def _ensure_methods_discovered(self) -> None:
        if self._discovered_methods is not None:
            return
        if self.sessions:
            first_session = next(iter(self.sessions.values()))
            try:
                methods = first_session.list_methods(include_docs=True)
                self._discovered_methods = [m.method for m in methods]
                return
            except Exception:
                pass
        self._discovered_methods = []

    def list_methods(self) -> List[str]:
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    @property
    def methods(self) -> List[str]:
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    async def call(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = await self.acall_balanced(method, kwargs)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = self.call_balanced(method, kwargs)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    async def call_all(self, method: str, **kwargs) -> List[Tuple[Optional[str], Optional[object], Optional[Exception]]]:
        results = await self.acall_all(method, kwargs)
        return _resolve_high_level_service_results(self, results=results)

    def __repr__(self) -> str:
        node_ids = list(self.sessions.keys()) if self.sessions else []
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<Service "
            f"service={self.service_name!r} "
            f"nodes={len(node_ids)} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


__all__ = [
    "_ServiceSessionFileLock",
    "_service_session_cache_file",
    "_load_service_session_cache",
    "Service",
]
