from pathlib import Path
import sys

REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

import time
from pycloud_parallel import Service

def main():
    # 你的业务代码（也可以用 artifact_path 指向本地 .py 文件）
    # 如果服务依赖节点未预装的包，可显式填 dependency_allowlist。
    dependency_allowlist = []
    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def square(x):\n"
        b"    return {'x': x, 'y': x * x}\n"
    )

    suffix = int(time.time())
    group = Service.deploy(
        infocenter_target="127.0.0.1:50051",
        owner_client_id=f"client-owner-{suffix}",
        service_name=f"square-service-{suffix}",
        blob=blob,
        runtime="py3",
        entry_module="square_service",
        entry_callable="square",
        export_mode="decorator",
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
    print("若需常驻，可手动调用 group.join(...); 此 demo 默认执行后自动收尾。")

    try:
        group.close(end_services=True, reason="grpc_register_service_client_demo cleanup")
        joined = True
    finally:
        group.close(
            end_services=not joined,
            reason="grpc_register_service_client_demo cleanup",
        )

if __name__ == "__main__":
    main()
