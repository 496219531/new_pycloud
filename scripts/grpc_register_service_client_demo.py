from pycloud_parallel.controlplane.client import MultiNodeServiceGroup

def main():
    # 你的业务代码（也可以用 artifact_path 指向本地 .py 文件）
    blob = (
        b"def pycloud_export(fn):\n"
        b"    fn.__pycloud_export__ = True\n"
        b"    return fn\n\n"
        b"@pycloud_export\n"
        b"def square(payload):\n"
        b"    x = int(payload.get('x', 0))\n"
        b"    return {'x': x, 'y': x * x}\n"
    )

    group = MultiNodeServiceGroup.deploy_from_infocenter(
        infocenter_target="127.0.0.1:50051",
        owner_client_id="client-owner-001",
        service_name="square-service",          # 同名已存在会直接抛错
        blob=blob,
        filename="square_service.py",
        runtime="py3.11",
        entry_module="square_service",
        entry_callable="square",
        export_mode="decorator",
        export_decorator="pycloud_export",
        worker_count=4,
        heartbeat_timeout_sec=30,
        healthy_only=True,
        tags=["compute"],                       # 可按节点标签筛选
        min_success_nodes=1,
        allow_partial=True,
        ensure_unique_service_name=True,        # 默认就是 True
    )

    print("注册成功，部署节点：", list(group.sessions.keys()))
    group.start_keepalive()  # 持续心跳，服务常驻

    try:
        for i in range(1000):
            node_id, resp = group.call_balanced("square", {"x": 7}, timeout_sec=10)
            node_id, resp = group.call_balanced("square", {"x": 15}, timeout_sec=10)
            print(i,"调用节点:", node_id, "结果:", resp["data"])
        import time
        time.sleep(1000)
    finally:
        group.end("owner主动结束")
        group.close(end_services=False)

if __name__ == "__main__":
    main()
