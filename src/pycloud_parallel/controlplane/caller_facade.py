from __future__ import annotations

"""Caller-side facade proxies extracted from controlplane client."""

import asyncio
from typing import Dict, List, Optional, Tuple


from pycloud_parallel.controlplane import client as client_mod


class _CallProxy:
    """服务方法调用代理。"""

    def __init__(
        self,
        method: str,
        group,
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status

    def __repr__(self) -> str:
        return f"<CallProxy method={self._method!r}>"

    @property
    def method(self) -> str:
        return self._method

    async def __call__(self, *args, **kwargs) -> Dict[str, object]:
        payload = {}
        if args:
            payload["args"] = list(args)
        if args and kwargs:
            payload["kwargs"] = kwargs
        final_payload = payload if args else kwargs
        serialized_payload = client_mod._serialize_arrow_compatible(final_payload)
        node_id, resp = await self._group.acall_balanced(
            self._method,
            serialized_payload,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )
        return client_mod._resolve_high_level_service_data(self._group, node_id=node_id, response=resp)

    def __await__(self):
        return self().__await__()

    @property
    def sync(self) -> "_SyncCallProxy":
        return _SyncCallProxy(
            method=self._method,
            group=self._group,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )

    @property
    def broadcast(self) -> "_BroadcastProxy":
        return _BroadcastProxy(
            method=self._method,
            group=self._group,
            timeout_sec=self._timeout_sec,
        )

    def with_options(
        self,
        *,
        timeout_sec: Optional[float] = None,
        strategy: Optional[str] = None,
        refresh_status: Optional[bool] = None,
    ) -> "_CallProxy":
        return _CallProxy(
            method=self._method,
            group=self._group,
            timeout_sec=timeout_sec if timeout_sec is not None else self._timeout_sec,
            strategy=strategy if strategy is not None else self._strategy,
            refresh_status=refresh_status if refresh_status is not None else self._refresh_status,
        )


class _SyncCallProxy:
    """同步调用代理。"""

    def __init__(
        self,
        method: str,
        group,
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status

    def __repr__(self) -> str:
        return f"<SyncCallProxy method={self._method!r}>"

    def __call__(self, *args, **kwargs) -> Dict[str, object]:
        payload = {}
        if args:
            payload["args"] = list(args)
        if args and kwargs:
            payload["kwargs"] = kwargs
        final_payload = payload if args else kwargs
        serialized_payload = client_mod._serialize_arrow_compatible(final_payload)
        node_id, resp = self._group.call_balanced(
            self._method,
            serialized_payload,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )
        return client_mod._resolve_high_level_service_data(self._group, node_id=node_id, response=resp)


class _BroadcastProxy:
    """广播调用代理，调用所有节点。"""

    def __init__(
        self,
        method: str,
        group,
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._max_concurrency = max_concurrency

    def __repr__(self) -> str:
        return f"<BroadcastProxy method={self._method!r}>"

    async def __call__(self, **kwargs) -> List[Tuple[Optional[str], Optional[object], Optional[Exception]]]:
        results = await self._group.acall_all(
            self._method,
            kwargs,
            timeout_sec=self._timeout_sec,
            max_concurrency=self._max_concurrency,
        )
        return client_mod._resolve_high_level_service_results(self._group, results=results)

    def __await__(self):
        return self().__await__()


class DeployedService(client_mod.ServiceGroup):
    _discovered_methods: Optional[List[str]] = None

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
        return client_mod._resolve_high_level_service_data(self, node_id=node_id, response=resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = self.call_balanced(method, kwargs)
        return client_mod._resolve_high_level_service_data(self, node_id=node_id, response=resp)

    async def call_all(self, method: str, **kwargs) -> List[Tuple[Optional[str], Optional[object], Optional[Exception]]]:
        results = await self.acall_all(method, kwargs)
        return client_mod._resolve_high_level_service_results(self, results=results)

    def __repr__(self) -> str:
        node_ids = list(self.sessions.keys()) if self.sessions else []
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<DeployedService "
            f"service={self.service_name!r} "
            f"nodes={len(node_ids)} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


class GatewayConnect(client_mod.GatewayServiceClient):
    """Module-like caller on top of ControlPlane Gateway."""

    def __init__(
        self,
        target: str,
        *,
        service_name: str,
        timeout_sec: float = 10.0,
        service_token: str = "",
        validate_on_init: bool = True,
    ) -> None:
        super().__init__(target, timeout_sec=timeout_sec, service_token=service_token)
        self.service_name = str(service_name or "").strip()
        if not self.service_name:
            raise ValueError("service_name is required")
        self._discovered_methods: Optional[List[str]] = None
        self._last_status: Optional[Dict[str, object]] = None
        if validate_on_init:
            self._validate_service_ready()

    def _validate_service_ready(self) -> Dict[str, object]:
        try:
            status = self.get_status(service_name=self.service_name)
        except Exception as exc:
            raise RuntimeError(
                f"failed to query gateway status for service_name={self.service_name!r} via {self.target}: {exc}"
            ) from exc
        if not isinstance(status, dict):
            raise RuntimeError(
                f"invalid gateway status for service_name={self.service_name!r} via {self.target}: {status!r}"
            )
        self._last_status = status
        route_count = int(status.get("route_count", 0) or 0)
        if route_count <= 0:
            raise RuntimeError(
                f"no available route for service_name={self.service_name!r} via gateway {self.target}"
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
            strategy="gateway",
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
                f"failed to list methods for service_name={self.service_name!r} via gateway {self.target}: {exc}"
            ) from exc
        discovered = [str(item.get("method", "")).strip() for item in methods if str(item.get("method", "")).strip()]
        if not discovered:
            self._validate_service_ready()
            raise RuntimeError(
                f"service_name={self.service_name!r} has active gateway routes via {self.target} but no exported methods"
            )
        self._discovered_methods = discovered

    def refresh_methods(self) -> List[str]:
        self._discovered_methods = None
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def list_methods(self, *, include_docs: bool = False) -> List[Dict[str, object]]:  # type: ignore[override]
        return list(super().list_methods(service_name=self.service_name, include_docs=include_docs))

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
        strategy: str = "gateway",
        refresh_status: bool = False,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        del strategy, refresh_status, max_attempts
        resp = super().call(
            service_name=self.service_name,
            method=method,
            payload=payload,
            timeout_sec=timeout_sec,
        )
        return "gateway", resp

    async def acall_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "gateway",
        refresh_status: bool = False,
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
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        del method, payload, timeout_sec, max_concurrency
        raise NotImplementedError("GatewayConnect does not support broadcast; use Gateway for single-route calls")

    def __repr__(self) -> str:
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<GatewayConnect "
            f"service={self.service_name!r} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


class DirectConnect(client_mod.DiscoveryServiceClient):
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
    ) -> Tuple[str, Dict[str, object]]:
        del refresh_status, max_attempts
        route = self._route_cache.select_route(self.service_name, strategy=strategy)
        tried = {route.service_id}
        token = self.service_token

        def _prepare_route_payload(selected_route: InfoCenterServiceRoute) -> Dict[str, object]:
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
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        del method, payload, timeout_sec, max_concurrency
        raise NotImplementedError("DirectConnect does not support broadcast; use direct discovery for single-route calls")

    def __repr__(self) -> str:
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<DirectConnect "
            f"service={self.service_name!r} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


__all__ = [
    "_CallProxy",
    "_SyncCallProxy",
    "_BroadcastProxy",
    "DeployedService",
    "GatewayConnect",
    "DirectConnect",
]
