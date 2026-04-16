from __future__ import annotations

"""Discovery-based service caller client and module-style facade."""

import contextlib
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, Optional, Sequence, Set

from pycloud_parallel.controlplane.client_transport import (
    DiscoveryCallError,
    _call_route_http,
    _is_route_failure,
    _list_route_methods_http,
    _serialize_route,
)
from pycloud_parallel.controlplane.data_ref import maybe_data_ref, with_data_ref_locator
from pycloud_parallel.controlplane.data_registry import DataRegistryClient, resolve_data_ref
from pycloud_parallel.controlplane.discovery_route_cache import _DiscoveryRouteCache
from pycloud_parallel.controlplane.infocenter_client import _node_instance_key_from_route
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.remote_payload import prepare_remote_call_payload
from pycloud_parallel.controlplane.replica_client import _extract_result_ref
from pycloud_parallel.controlplane.serialization import INLINE_PAYLOAD_SOFT_LIMIT_BYTES
from pycloud_parallel.execution.call_proxy import _CallProxy
from pycloud_parallel.execution.support import _resolve_high_level_service_data

client_mod = SimpleNamespace(
    _DiscoveryRouteCache=_DiscoveryRouteCache,
    _serialize_route=_serialize_route,
    _extract_result_ref=_extract_result_ref,
    NodeControlClient=NodeControlClient,
    _prepare_remote_call_payload=prepare_remote_call_payload,
    INLINE_PAYLOAD_SOFT_LIMIT_BYTES=INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    _call_route_http=_call_route_http,
    DiscoveryCallError=DiscoveryCallError,
    _is_route_failure=_is_route_failure,
    _list_route_methods_http=_list_route_methods_http,
    _node_instance_key_from_route=_node_instance_key_from_route,
    _resolve_high_level_service_data=_resolve_high_level_service_data,
)


class DiscoveryServiceClient:
    """Client-side service discovery caller."""

    def __init__(
        self,
        infocenter_target: str,
        *,
        timeout_sec: float = 10.0,
        service_token: str = "",
        refresh_interval_sec: float = 3.0,
        failure_threshold: int = 3,
        open_sec: float = 5.0,
        route_limit: int = 500,
    ) -> None:
        self.infocenter_target = str(infocenter_target or "").strip()
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.service_token = str(service_token or "").strip()
        self._route_cache = client_mod._DiscoveryRouteCache(
            infocenter_target=self.infocenter_target,
            timeout_sec=self.timeout_sec,
            refresh_interval_sec=refresh_interval_sec,
            failure_threshold=failure_threshold,
            open_sec=open_sec,
            route_limit=route_limit,
        )
        self._route_cache.start()

    def close(self) -> None:
        self._route_cache.stop()

    def __enter__(self) -> "DiscoveryServiceClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def refresh_routes(self, *, service_name: str, force: bool = False) -> Sequence[object]:
        return list(self._route_cache.refresh(service_name, force=force))

    def list_routes(self, *, service_name: str) -> Sequence[object]:
        return list(self._route_cache.get_routes(service_name))

    def get_status(self, *, service_name: str) -> Dict[str, object]:
        info = self._route_cache.snapshot_info(service_name)
        routes = info["routes"]
        return {
            "ok": True,
            "service_name": str(info["service_name"]),
            "refreshed_at": info["refreshed_at"],
            "route_count": int(info["route_count"]),
            "routes": [client_mod._serialize_route(route) for route in routes],
        }

    def download_result_to_file(self, response_or_data: object, *, target_path: str) -> Path:
        ref = client_mod._extract_result_ref(response_or_data)
        if ref is None:
            raise ValueError("service result is inline data; no download needed")
        self._touch_data_ref(ref)
        resolved = resolve_data_ref(ref, target=self.infocenter_target, timeout_sec=self.timeout_sec)
        try:
            with client_mod.NodeControlClient(resolved.control_addr, timeout_sec=self.timeout_sec) as client:
                return client.download_result_to_file(ref, target_path=target_path)
        finally:
            self._release_data_ref_if_consumed(ref)

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        ref = client_mod._extract_result_ref(response_or_data)
        if ref is None:
            if isinstance(response_or_data, dict) and "data" in response_or_data:
                return response_or_data["data"]
            return response_or_data
        self._touch_data_ref(ref)
        resolved = resolve_data_ref(ref, target=self.infocenter_target, timeout_sec=self.timeout_sec)
        try:
            with client_mod.NodeControlClient(resolved.control_addr, timeout_sec=self.timeout_sec) as client:
                return client.fetch_result_ref_data(ref, target_path=target_path)
        finally:
            self._release_data_ref_if_consumed(ref)

    def list_methods(
        self,
        *,
        service_name: str,
        include_docs: bool = False,
        strategy: str = "predicted_busy",
    ) -> Sequence[Dict[str, object]]:
        tried: Set[str] = set()
        try:
            route = self._route_cache.select_route(service_name, strategy=strategy)
            tried.add(route.service_id)
            methods = self._list_methods_via_route(route, include_docs=include_docs)
            self._route_cache.mark_success(route)
            return methods
        except Exception as exc:
            if tried:
                self._route_cache.mark_failure(route, str(exc))
            self._route_cache.refresh(service_name, force=True)
            retry_route = self._route_cache.select_route(service_name, exclude_service_ids=tried, strategy=strategy)
            methods = self._list_methods_via_route(retry_route, include_docs=include_docs)
            self._route_cache.mark_success(retry_route)
            return methods

    def call(
        self,
        *,
        service_name: str,
        method: str,
        payload: Optional[Dict[str, object]] = None,
        timeout_sec: float = 60.0,
        service_token: Optional[str] = None,
        strategy: str = "predicted_busy",
    ) -> Dict[str, object]:
        name = str(service_name or "").strip()
        method_name = str(method or "").strip()
        if not name:
            raise ValueError("service_name is required")
        if not method_name:
            raise ValueError("method is required")
        token = self.service_token if service_token is None else str(service_token or "").strip()
        tried: Set[str] = set()
        route = self._route_cache.select_route(name, strategy=strategy)
        tried.add(route.service_id)
        routes_snapshot = list(self._route_cache.get_routes(name))
        clients: List[object] = []
        try:
            for item in routes_snapshot:
                control_addr = str(getattr(item, "control_addr", "") or "").strip()
                if not control_addr:
                    continue
                clients.append(client_mod.NodeControlClient(control_addr, timeout_sec=self.timeout_sec))
            prepared_payload = (
                client_mod._prepare_remote_call_payload(
                    clients,
                    payload or {},
                    object_threshold_bytes=client_mod.INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
                )
                if clients
                else (payload or {})
            )
        finally:
            for client in clients:
                with contextlib.suppress(Exception):
                    client.close()
        try:
            resp = client_mod._call_route_http(
                route,
                method=method_name,
                payload=prepared_payload,
                timeout_sec=max(0.1, float(timeout_sec)),
                service_token=token,
            )
            self._route_cache.mark_success(route)
            return self._attach_controlplane_locator(resp, route=route)
        except client_mod.DiscoveryCallError as exc:
            if not client_mod._is_route_failure(exc):
                raise RuntimeError(str(exc)) from exc
            self._route_cache.mark_failure(route, str(exc))
            self._route_cache.refresh(name, force=True)
            retry_route = self._route_cache.select_route(name, exclude_service_ids=tried, strategy=strategy)
            try:
                resp = client_mod._call_route_http(
                    retry_route,
                    method=method_name,
                    payload=prepared_payload,
                    timeout_sec=max(0.1, float(timeout_sec)),
                    service_token=token,
                )
                self._route_cache.mark_success(retry_route)
                return self._attach_controlplane_locator(resp, route=retry_route)
            except client_mod.DiscoveryCallError as retry_exc:
                if client_mod._is_route_failure(retry_exc):
                    self._route_cache.mark_failure(retry_route, str(retry_exc))
                raise RuntimeError(str(retry_exc)) from retry_exc

    def _list_methods_via_route(self, route: object, *, include_docs: bool) -> List[Dict[str, object]]:
        if not str(getattr(route, "control_addr", "") or "").strip():
            return client_mod._list_route_methods_http(route, include_docs=include_docs, timeout_sec=self.timeout_sec)
        with client_mod.NodeControlClient(route.control_addr, timeout_sec=self.timeout_sec) as client:
            methods = client.list_service_methods(service_id=route.service_id, include_docs=include_docs)
        return [
            {
                "method": item.method,
                "qualified_name": item.qualified_name,
                "doc": item.doc,
            }
            for item in methods
        ]

    def _attach_controlplane_locator(self, response: Dict[str, object], *, route: object) -> Dict[str, object]:
        if not isinstance(response, dict) or "data" not in response:
            return response
        updated = with_data_ref_locator(
            response.get("data"),
            locator_kind="controlplane",
            locator_token=self.infocenter_target,
            node_id=str(getattr(route, "node_id", "") or ""),
            node_instance_id=str(getattr(route, "node_instance_id", "") or ""),
        )
        if updated is response.get("data"):
            return response
        ref = maybe_data_ref(updated)
        if ref is not None:
            try:
                DataRegistryClient(self.infocenter_target, timeout_sec=self.timeout_sec).register(
                    updated,
                    node_id=str(getattr(route, "node_id", "") or ""),
                    node_instance_id=str(getattr(route, "node_instance_id", "") or ""),
                    control_addr=str(getattr(route, "control_addr", "") or ""),
                    locator_kind="node_control",
                    locator_token=str(getattr(route, "control_addr", "") or ""),
                )
            except Exception:
                pass
        body = dict(response)
        body["data"] = updated
        return body

    def _touch_data_ref(self, ref: object) -> None:
        data_ref = maybe_data_ref(ref)
        if data_ref is None or str(data_ref.locator_kind or "").strip().lower() != "controlplane":
            return
        target = str(data_ref.locator_token or self.infocenter_target or "").strip()
        if not target:
            return
        try:
            DataRegistryClient(target, timeout_sec=self.timeout_sec).touch(data_ref.ref_id)
        except Exception:
            pass

    def _release_data_ref_if_consumed(self, ref: object) -> None:
        data_ref = maybe_data_ref(ref)
        if data_ref is None or not bool(data_ref.consume_on_read):
            return
        target = str(data_ref.locator_token or self.infocenter_target or "").strip()
        if not target:
            return
        try:
            DataRegistryClient(target, timeout_sec=self.timeout_sec).release(data_ref.ref_id)
        except Exception:
            pass


class DiscoveryCallerFacade(DiscoveryServiceClient):
    """Module-like caller built on InfoCenter discovery + direct instance calls."""

    def __init__(
        self,
        infocenter_target: str,
        *,
        service_name: str,
        timeout_sec: float = 10.0,
        service_token: str = "",
        refresh_interval_sec: float = 3.0,
        failure_threshold: int = 3,
        open_sec: float = 5.0,
        route_limit: int = 500,
        validate_on_init: bool = True,
    ) -> None:
        super().__init__(
            infocenter_target,
            timeout_sec=timeout_sec,
            service_token=service_token,
            refresh_interval_sec=refresh_interval_sec,
            failure_threshold=failure_threshold,
            open_sec=open_sec,
            route_limit=route_limit,
        )
        self.service_name = str(service_name or "").strip()
        if not self.service_name:
            raise ValueError("service_name is required")
        self._discovered_methods: Optional[List[str]] = None
        self._last_status: Optional[Dict[str, object]] = None
        if validate_on_init:
            self._validate_service_ready()

    def _validate_service_ready(self) -> Dict[str, object]:
        try:
            self.refresh_routes(service_name=self.service_name, force=True)
            status = self.get_status(service_name=self.service_name)
        except Exception as exc:
            raise RuntimeError(
                f"failed to query discovery status for service_name={self.service_name!r} via {self.infocenter_target}: {exc}"
            ) from exc
        if not isinstance(status, dict):
            raise RuntimeError(
                f"invalid discovery status for service_name={self.service_name!r} via {self.infocenter_target}: {status!r}"
            )
        self._last_status = status
        route_count = int(status.get("route_count", 0) or 0)
        if route_count <= 0:
            raise RuntimeError(
                f"no available route for service_name={self.service_name!r} via infocenter {self.infocenter_target}"
            )
        return status

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
            timeout_sec=self.timeout_sec,
            strategy="predicted_busy",
            refresh_status=False,
        )

    def _ensure_methods_discovered(self) -> None:
        if self._discovered_methods is not None:
            return
        try:
            methods = self.list_methods(include_docs=True)
        except Exception as exc:
            self._validate_service_ready()
            raise RuntimeError(
                f"failed to list methods for service_name={self.service_name!r} via discovery {self.infocenter_target}: {exc}"
            ) from exc
        discovered = [str(item.get("method", "")).strip() for item in methods if str(item.get("method", "")).strip()]
        if not discovered:
            self._validate_service_ready()
            raise RuntimeError(
                f"service_name={self.service_name!r} has active discovery routes via {self.infocenter_target} but no exported methods"
            )
        self._discovered_methods = discovered

    def refresh_methods(self) -> List[str]:
        self._discovered_methods = None
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def list_methods(self, *, include_docs: bool = False, strategy: str = "predicted_busy") -> List[Dict[str, object]]:  # type: ignore[override]
        return list(
            super().list_methods(
                service_name=self.service_name,
                include_docs=include_docs,
                strategy=strategy,
            )
        )

    @property
    def methods(self) -> List[str]:
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def status(self) -> Dict[str, object]:
        status = self.get_status(service_name=self.service_name)
        if isinstance(status, dict):
            self._last_status = status
        return status

    def call_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
        max_attempts: int = 0,
    ):
        del refresh_status, max_attempts
        route = self._route_cache.select_route(self.service_name, strategy=strategy)
        tried = {route.service_id}
        token = self.service_token

        def _prepare_route_payload(selected_route) -> Dict[str, object]:
            control_addr = str(getattr(selected_route, "control_addr", "") or "").strip()
            if not control_addr:
                return dict(payload or {})
            with client_mod.NodeControlClient(control_addr, timeout_sec=self.timeout_sec) as route_client:
                return client_mod._prepare_remote_call_payload(
                    [route_client],
                    payload,
                    object_threshold_bytes=client_mod.INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
                )

        try:
            prepared_payload = _prepare_route_payload(route)
            resp = client_mod._call_route_http(
                route,
                method=method,
                payload=prepared_payload,
                timeout_sec=max(0.1, float(timeout_sec)),
                service_token=token,
            )
            self._route_cache.mark_success(route)
            return client_mod._node_instance_key_from_route(route), resp
        except client_mod.DiscoveryCallError as exc:
            if not client_mod._is_route_failure(exc):
                raise RuntimeError(str(exc)) from exc
            self._route_cache.mark_failure(route, str(exc))
            self._route_cache.refresh(self.service_name, force=True)
            retry_route = self._route_cache.select_route(self.service_name, exclude_service_ids=tried, strategy=strategy)
            try:
                retry_payload = _prepare_route_payload(retry_route)
                resp = client_mod._call_route_http(
                    retry_route,
                    method=method,
                    payload=retry_payload,
                    timeout_sec=max(0.1, float(timeout_sec)),
                    service_token=token,
                )
                self._route_cache.mark_success(retry_route)
                return client_mod._node_instance_key_from_route(retry_route), resp
            except client_mod.DiscoveryCallError as retry_exc:
                if client_mod._is_route_failure(retry_exc):
                    self._route_cache.mark_failure(retry_route, str(retry_exc))
                raise RuntimeError(str(retry_exc)) from retry_exc

    async def acall_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
        max_attempts: int = 0,
    ):
        import asyncio

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

    async def call(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = await self.acall_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return client_mod._resolve_high_level_service_data(self, node_id=node_id, response=resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = self.call_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return client_mod._resolve_high_level_service_data(self, node_id=node_id, response=resp)

    async def acall_all(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ):
        del method, payload, timeout_sec, max_concurrency
        raise NotImplementedError("discovery caller facade does not support broadcast; use direct discovery for single-route calls")

    def __repr__(self) -> str:
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<DiscoveryCallerFacade "
            f"service={self.service_name!r} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


__all__ = ["DiscoveryServiceClient", "DiscoveryCallerFacade"]
