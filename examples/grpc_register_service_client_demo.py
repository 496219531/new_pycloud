from pycloud_parallel.controlplane.client import ServiceGroup
import time

def main():
    # 你的业务代码（也可以用 artifact_path 指向本地 .py 文件）
    # 如果服务依赖节点未预装的包，可显式填 dependency_allowlist。
    dependency_allowlist = []
    blob = (
        b"def pycloud_export(fn):\n"
        b"    fn.__pycloud_export__ = True\n"
        b"    return fn\n\n"
        b"@pycloud_export\n"
        b"def square(x):\n"
        b"    return {'x': x, 'y': x * x}\n"
    )

    suffix = int(time.time())
    group = ServiceGroup.deploy_from_infocenter(
        infocenter_target="127.0.0.1:50051",
        owner_client_id=f"client-owner-{suffix}",
        service_name=f"square-service",
        blob=blob,
        filename="square_service.py",
        runtime="py3",
        entry_module="square_service",
        entry_callable="square",
        export_mode="decorator",
        export_decorator="pycloud_export",
        dependency_allowlist=dependency_allowlist,
        worker_count=4,
        heartbeat_timeout_sec=30,
        healthy_only=True,
        tags=["compute"],
        min_success_nodes=1,
        allow_partial=True,
        ensure_unique_service_name=True,
    )
    joined = False

    print("=" * 60)
    print("  Service Owner Long-Running Demo")
    print("=" * 60)
    print(f"service_name: {group.service_name}")
    print(f"owner_client_id: {group.owner_client_id}")
    print("部署节点:")
    for node_id, session in group.sessions.items():
        print(f"  - {node_id}: {session.http_base_url}")

    node_id, resp = group.call_balanced("square", {"x": 7}, timeout_sec=10)
    print(f"预热调用成功 node={node_id} data={resp['data']}")
    print("服务已进入常驻模式，按 Ctrl+C 结束并自动回收服务。")

    try:
        group.join(end_services_on_interrupt=True, end_reason="owner ctrl+c")
        joined = True
    finally:
        group.close(
            end_services=not joined,
            reason="grpc_register_service_client_demo cleanup",
        )

if __name__ == "__main__":
    main()
