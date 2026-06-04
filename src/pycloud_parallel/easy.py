from __future__ import annotations

"""Convenience helpers for starting and using pycloud-parallel."""

import os
from typing import Any, Iterable, Optional, Sequence


DEFAULT_TARGET = "127.0.0.1:50051"
DEFAULT_NODE_BIND = "0.0.0.0:18061"
DEFAULT_SERVICE_HTTP_BIND = "0.0.0.0:18080"


def _target_from_addr(addr: Optional[str]) -> str:
    return str(addr or DEFAULT_TARGET).strip()


def _bind_from_port(port: Optional[int], *, default: str) -> str:
    if port is None:
        return default
    return f"0.0.0.0:{int(port)}"


def _module_name(module: Any) -> str:
    return str(getattr(module, "__name__", "") or module).strip()


def _normalize_paths(paths: Optional[Iterable[Any]]) -> Sequence[str]:
    return tuple(str(path).strip() for path in (paths or ()) if str(path).strip())


def serve_controlplane(
    port: int = 50051,
    *,
    host: str = "0.0.0.0",
    bind: str = "",
    gateway_refresh_interval_sec: float = 3.0,
    gateway_failure_threshold: int = 3,
    gateway_open_sec: float = 5.0,
) -> None:
    """Run a ControlPlane/InfoCenter HTTP server forever."""
    from pycloud_parallel.controlplane.server import build_controlplane_server, _wait_until_stopped

    effective_bind = str(bind or "").strip() or f"{host}:{int(port)}"
    server = build_controlplane_server(
        effective_bind,
        gateway_refresh_interval_sec=gateway_refresh_interval_sec,
        gateway_failure_threshold=gateway_failure_threshold,
        gateway_open_sec=gateway_open_sec,
    )
    _wait_until_stopped(server, on_stop=lambda: None)


def serve_node(
    target: str = DEFAULT_TARGET,
    *,
    port: Optional[int] = None,
    bind: str = DEFAULT_NODE_BIND,
    node_id: str = "node-local-01",
    worker_capacity: int = 32,
    queue_capacity: int = 4000,
    max_workers: int = 64,
    artifact_dir: str = "",
    service_http_bind: str = DEFAULT_SERVICE_HTTP_BIND,
    service_http_base_url: str = "",
    control_base_url: str = "",
    service_default_worker_count: int = 10,
    service_heartbeat_timeout_sec: int = 30,
    node_tags: str | Sequence[str] = "compute",
    node_version: str = "v1",
    api_token: str = "",
) -> None:
    """Run a NodeControl worker and register it to ControlPlane forever."""
    from pycloud_parallel.controlplane.netutil import resolve_public_host, split_host_port
    from pycloud_parallel.controlplane.registrar import NodeInfoCenterRegistrar
    from pycloud_parallel.controlplane.server import build_nodecontrol_server, _wait_until_stopped

    effective_target = _target_from_addr(target)
    effective_bind = _bind_from_port(port, default=bind)
    control_host, control_port = split_host_port(effective_bind)
    effective_control_base_url = str(control_base_url or "").strip()
    if not effective_control_base_url:
        effective_control_base_url = f"http://{resolve_public_host(control_host, remote_hint=effective_target)}:{int(control_port)}"

    service_host, service_port = split_host_port(service_http_bind)
    effective_service_http_base_url = str(service_http_base_url or "").strip()
    if not effective_service_http_base_url:
        effective_service_http_base_url = f"http://{resolve_public_host(service_host, remote_hint=effective_target)}:{int(service_port)}"

    registrar_holder: dict[str, Optional[NodeInfoCenterRegistrar]] = {"value": None}

    def _sync_routes_now() -> None:
        registrar = registrar_holder["value"]
        if registrar is not None:
            registrar.request_sync()

    server, state = build_nodecontrol_server(
        effective_bind,
        node_id=node_id,
        artifact_dir=artifact_dir,
        worker_capacity=worker_capacity,
        queue_capacity=queue_capacity,
        max_workers=max_workers,
        service_http_bind=service_http_bind,
        service_http_base_url=effective_service_http_base_url,
        control_base_url=effective_control_base_url,
        service_default_worker_count=service_default_worker_count,
        service_default_heartbeat_timeout_sec=service_heartbeat_timeout_sec,
        on_service_routes_changed=_sync_routes_now,
        api_token=str(api_token or os.getenv("PYCLOUD_API_TOKEN", "") or ""),
    )
    tags = [str(item).strip() for item in node_tags] if not isinstance(node_tags, str) else [
        item.strip() for item in node_tags.split(",")
    ]
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=effective_target,
        node_id=node_id,
        control_addr=effective_control_base_url,
        state=state,
        capacity=worker_capacity,
        queue_capacity=queue_capacity,
        tags=[item for item in tags if item],
        version=node_version,
        metadata={"role": "compute-node"},
        exit_on_fence=True,
    )
    registrar_holder["value"] = registrar

    def _on_start() -> None:
        registrar.start()

    def _on_stop() -> None:
        registrar.close()
        state.close()

    _wait_until_stopped(server, on_start=_on_start, on_stop=_on_stop)


def serve_module(
    module: Any,
    target: str = DEFAULT_TARGET,
    *,
    port: Optional[int] = None,
    service_name: str = "",
    worker_count: int = 1,
    policy_id: str = "",
    serialization_mode: str = "",
) -> None:
    """Startup-mount a local module as a service and block forever."""
    from pycloud_parallel import Service
    from pycloud_parallel.controlplane.policy_profile import get_default_policy_id_for_binding

    bind = None if port is None else f"0.0.0.0:{int(port)}"
    node = Service.startup(
        source=module,
        target=_target_from_addr(target),
        service_name=service_name or _module_name(module),
        bind=bind,
        worker_count=worker_count or 1,
        export_methods=None,
        policy_id=policy_id or (
            get_default_policy_id_for_binding("service_internal")
            if str(serialization_mode or "").strip()
            else ""
        ),
        replace_existing=True,
    )
    node.join()


def deploy_module_service(
    module: Any,
    target: str = DEFAULT_TARGET,
    source_paths: Optional[Iterable[Any]] = None,
    *,
    service_name: str = "",
    worker_count: int = 5,
    node_ids: Optional[Sequence[str]] = None,
    initial_globals: Optional[dict[str, object]] = None,
    managed_global_names: Optional[Sequence[str]] = None,
    tags: Optional[Sequence[str]] = None,
    join: bool = True,
    **kwargs: Any,
):
    """Deploy a module service with export-all semantics."""
    from pycloud_parallel import Service
    from pycloud_parallel.artifact import Artifact, ArtifactExports
    from pycloud_parallel.controlplane.netutil import detect_local_ip

    effective_service_name = str(service_name or "").strip()
    if not effective_service_name:
        effective_service_name = f"{_module_name(module)}_{detect_local_ip(remote_hint=_target_from_addr(target))}"
    paths = _normalize_paths(source_paths)
    artifact = None
    source = module
    if paths:
        artifact = Artifact.from_paths(
            paths,
            entry_module=_module_name(module),
            exports=ArtifactExports.export_all(),
        )
        source = None
    group = Service.deploy(
        target=_target_from_addr(target),
        source=source,
        artifact=artifact,
        service_name=effective_service_name,
        worker_count=worker_count or 1,
        node_ids=node_ids,
        initial_globals=initial_globals,
        managed_global_names=managed_global_names,
        tags=list(tags or ("compute",)),
        **kwargs,
    )
    if join:
        group.join()
    return group


def serve_function(func: Any, target: str = DEFAULT_TARGET, *, worker_count: int = 1, join: bool = True, **kwargs: Any):
    """Deploy a single function as a service."""
    from pycloud_parallel import Service
    from pycloud_parallel.artifact import Artifact, ArtifactExports

    service_name = str(getattr(func, "__name__", "") or "function_service")
    group = Service.deploy(
        target=_target_from_addr(target),
        artifact=Artifact.from_function(func, exports=ArtifactExports.single(service_name)),
        service_name=service_name,
        worker_count=worker_count or 1,
        allow_partial=True,
        min_success_nodes=1,
        tags=["compute"],
        **kwargs,
    )
    if join:
        group.join()
    return group


def submit_tasks(
    target: str,
    payloads: Sequence[dict[str, object]],
    *,
    task_method: str = "",
    close: bool = True,
    **kwargs: Any,
):
    """Submit task payloads and return the submit response."""
    from pycloud_parallel import TaskPool

    pool = TaskPool.open(target=_target_from_addr(target))
    try:
        return pool.submit_payloads(list(payloads), task_method=task_method, **kwargs)
    finally:
        if close:
            pool.close()


def run_tasks(
    target: str,
    payloads: Sequence[dict[str, object]],
    *,
    task_method: str = "",
    timeout_sec: float = 30.0,
    wait_ms: int = 500,
    progress: Any = False,
    **kwargs: Any,
):
    """Submit task payloads and wait for returned data."""
    from pycloud_parallel import TaskPool

    items = list(payloads)
    pool = TaskPool.open(target=_target_from_addr(target))
    try:
        pool.submit_payloads(items, task_method=task_method, **kwargs)
        return pool.wait_for_data(expected_count=len(items), timeout_sec=timeout_sec, wait_ms=wait_ms, progress=progress)
    finally:
        pool.close()


def call_service_method(target: str, service_name: str, method_name: str, *args: Any, print_result: bool = False, **kwargs: Any):
    """Call a remote service method synchronously."""
    from pycloud_parallel import Service

    client = Service.connect(target=_target_from_addr(target), service_name=service_name)
    try:
        result = getattr(client, method_name).sync(*args, **kwargs)
        if print_result:
            print(result)
        return result
    finally:
        client.close()


def unsupported_upgrade_system(*_args: Any, **_kwargs: Any) -> None:
    raise NotImplementedError(
        "pycloud-parallel uses artifact-based deploy/update instead of the old "
        "UpdateSys file sync path. Use Service.deploy(..., source=module, "
        "resource_paths=[...]) or Service.deploy(..., artifact=Artifact.from_paths(...))."
    )


def upgrade_system(*args: Any, **kwargs: Any) -> None:
    return unsupported_upgrade_system(*args, **kwargs)


def _run_udf(info_addr: str, port: Optional[int] = None) -> None:
    return serve_node(info_addr, port=port)


def run_worker_forever(info_pub_addr: str = DEFAULT_TARGET, port: Optional[int] = None, *args: Any, **kwargs: Any) -> None:
    del args
    return serve_node(info_pub_addr, port=port, **kwargs)


def run_module_forever(
    info_pub_addr: str,
    port: Optional[int] = None,
    module: Any = None,
    service_name: str = "",
    worker_num: Optional[int] = None,
    *args: Any,
    **kwargs: Any,
) -> None:
    if module is None:
        if not args:
            raise TypeError("run_module_forever() missing required argument: 'module'")
        module = args[0]
        args = args[1:]
    if args:
        raise TypeError(f"run_module_forever() got unexpected positional arguments: {args!r}")
    worker_count = kwargs.pop("worker_count", worker_num if worker_num is not None else 1)
    return serve_module(
        module,
        target=info_pub_addr,
        port=port,
        service_name=service_name,
        worker_count=int(worker_count or 1),
        **kwargs,
    )


def run_info_center(
    req_port: int,
    pub_port: Optional[int] = None,
    flask_port: int = 8038,
    *,
    bind_host: str = "0.0.0.0",
    **kwargs: Any,
) -> None:
    del pub_port, flask_port
    host = str(kwargs.pop("host", bind_host) or "0.0.0.0")
    return serve_controlplane(int(req_port), host=host, **kwargs)


def register_service_singleton(
    module: Any,
    addr: str,
    source_list: Optional[Iterable[Any]],
    service_name: Optional[str] = None,
    node_num: int = 5,
    node_pattern_list: Optional[Sequence[str]] = None,
    module_data: Optional[Any] = None,
):
    return deploy_module_service(
        module,
        target=addr,
        source_paths=source_list,
        service_name=service_name or "",
        worker_count=node_num,
        node_ids=node_pattern_list,
        initial_globals=module_data if isinstance(module_data, dict) else None,
        join=True,
    )


def run_func_server_without_return(func: Any, addr: str, worker_num: Optional[int] = None):
    return serve_function(func, target=addr, worker_count=worker_num or 1, join=True)


def send_tasks_to_server(addr: str, task_list: Sequence[dict[str, object]], join: bool = True, check_interval: int = 10):
    if join:
        return run_tasks(addr, list(task_list), timeout_sec=max(check_interval, 1))
    return submit_tasks(addr, list(task_list))


def send_tasks_to_server_without_return(
    addr: str,
    task_list: Sequence[dict[str, object]],
    join: bool = True,
    check_interval: int = 10,
):
    return send_tasks_to_server(addr, task_list, join=join, check_interval=check_interval)


def call_func_remote(addr: str, service_name: str, func_name: str, *args: Any, **kwargs: Any):
    return call_service_method(addr, service_name, func_name, *args, print_result=True, **kwargs)


__all__ = [
    "DEFAULT_NODE_BIND",
    "DEFAULT_SERVICE_HTTP_BIND",
    "DEFAULT_TARGET",
    "call_func_remote",
    "call_service_method",
    "deploy_module_service",
    "register_service_singleton",
    "run_func_server_without_return",
    "run_info_center",
    "run_module_forever",
    "run_tasks",
    "run_worker_forever",
    "send_tasks_to_server",
    "send_tasks_to_server_without_return",
    "serve_controlplane",
    "serve_function",
    "serve_module",
    "serve_node",
    "submit_tasks",
    "unsupported_upgrade_system",
    "upgrade_system",
]
