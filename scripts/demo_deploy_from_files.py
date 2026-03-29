#!/usr/bin/env python3
"""
PyCloud 部署服务示例：从多个文件/文件夹部署

演示如何将多个文件和文件夹自动打包成 zip 部署到多个节点
"""
from pycloud_parallel.controlplane.client import MultiNodeServiceGroup


def main():
    print("=" * 60)
    print("  PyCloud 部署服务示例：文件/文件夹列表")
    print("=" * 60)
    print()

    # 方式 1: 使用 artifact_paths 部署多个文件/文件夹
    print("-" * 60)
    print("  方式 1: 使用 artifact_paths 列表")
    print("-" * 60)
    print()

    # 假设你有以下文件结构：
    # my_service/
    #   ├── main.py          # 主入口
    #   ├── utils.py         # 工具函数
    #   └── config.py        # 配置

    # artifact_paths 支持混合文件和文件夹
    group = MultiNodeServiceGroup.deploy_from_infocenter(
        infocenter_target="127.0.0.1:50051",
        owner_client_id="client-files-001",
        service_name="multi-file-service",
        artifact_paths=[
            "/path/to/main.py",          # 单个文件
            "/path/to/utils.py",         # 单个文件
            "/path/to/config.py",         # 单个文件
            # "/path/to/my_service/",      # 整个文件夹
        ],
        entry_module="main",             # 入口模块名（不含 .py）
        entry_callable="main_func",      # 入口函数名
        export_mode="decorator",         # 装饰器模式
        export_decorator="pycloud_export",
        worker_count=4,
        heartbeat_timeout_sec=30,
        healthy_only=True,
        tags=["compute"],
        min_success_nodes=1,
        allow_partial=True,
    )

    print(f"✓ 服务部署成功！节点: {list(group.sessions.keys())}")
    print(f"  HTTP 端点:")
    for node_id, session in group.sessions.items():
        print(f"    - {node_id}: {session.http_base_url}")
    print()

    group.start_keepalive()

    try:
        # 调用服务
        node_id, resp = group.call_balanced("process", {"data": "test"}, timeout_sec=10)
        print(f"调用结果: {resp['data']}")
    finally:
        group.close(end_services=False)


def example_with_zip():
    """方式 2: 也可以手动打包成 zip 后部署"""
    print("-" * 60)
    print("  方式 2: 手动打包 zip")
    print("-" * 60)
    print()

    import zipfile
    import tempfile
    from pathlib import Path

    # 创建 zip 包
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name

    with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        # 添加文件到 zip
        zf.write("/path/to/main.py", "main.py")
        zf.write("/path/to/utils.py", "utils.py")
        zf.write("/path/to/config.py", "config.py")

    group = MultiNodeServiceGroup.deploy_from_infocenter(
        infocenter_target="127.0.0.1:50051",
        owner_client_id="client-zip-001",
        service_name="zip-service",
        artifact_path=tmp_path,           # 使用 zip 文件
        filename="service.zip",            # 文件名
        package_format="zip",              # 显式指定格式
        entry_module="main",
        export_mode="explicit",
        export_methods=["process", "compute"],
        worker_count=4,
    )

    print(f"✓ 服务部署成功！节点: {list(group.sessions.keys())}")
    print()

    # 清理临时 zip
    Path(tmp_path).unlink(missing_ok=True)

    group.close()


def example_with_blob():
    """方式 3: 直接提供代码 bytes"""
    print("-" * 60)
    print("  方式 3: 直接提供代码 (blob)")
    print("-" * 60)
    print()

    code = b'''
def pycloud_export(fn):
    fn.__pycloud_export__ = True
    return fn

@pycloud_export
def process(payload):
    data = payload.get("data", "")
    return {"result": data.upper()}
'''

    group = MultiNodeServiceGroup.deploy_from_infocenter(
        infocenter_target="127.0.0.1:50051",
        owner_client_id="client-blob-001",
        service_name="blob-service",
        blob=code,
        filename="inline.py",
        runtime="py3",
        entry_module="inline",
        export_mode="decorator",
        export_decorator="pycloud_export",
        worker_count=2,
    )

    print(f"✓ 服务部署成功！节点: {list(group.sessions.keys())}")
    print()

    group.close()


# ============================================================================
# 完整示例：一个实际的多文件服务
# ============================================================================

def example_complete_service():
    """完整示例：部署一个真实的多文件服务"""

    # 假设你有以下目录结构：
    # my_compute_service/
    #   ├── __init__.py
    #   ├── main.py
    #   ├── algorithms/
    #   │   ├── __init__.py
    #   │   ├── sort.py
    #   │   └── search.py
    #   └── utils/
    #       ├── __init__.py
    #       └── helpers.py

    group = MultiNodeServiceGroup.deploy_from_infocenter(
        infocenter_target="127.0.0.1:50051",
        owner_client_id="client-compute-001",
        service_name="compute-service",

        # artifact_paths 支持混合：
        artifact_paths=[
            # 方式 A: 添加整个文件夹
            "/path/to/my_compute_service/",

            # 方式 B: 选择性添加文件
            # "/path/to/my_compute_service/main.py",
            # "/path/to/my_compute_service/algorithms/",
            # "/path/to/my_compute_service/utils/",
        ],

        # 入口配置
        entry_module="main",           # main.py
        entry_callable="main_func",    # def main_func(payload): ...

        # 导出配置
        export_mode="decorator",       # 使用 @pycloud_export 装饰器
        export_decorator="pycloud_export",

        # 运行时配置
        runtime="py3.11",
        worker_count=4,                # 每个节点 4 个 worker
        heartbeat_timeout_sec=30,
        idle_ttl_sec=300,              # 5 分钟无任务自动停止

        # 部署配置
        expose_http=True,              # 暴露 HTTP 接口
        healthy_only=True,             # 只部署到健康节点
        tags=["compute", "cpu-intensive"],  # 节点标签

        # 高可用配置
        min_success_nodes=1,           # 至少成功 1 个节点
        allow_partial=True,            # 允许部分失败
        node_limit=10,                 # 最多部署到 10 个节点

        # 熔断器配置
        breaker_enabled=True,
        breaker_failure_threshold=3,
        breaker_cooldown_sec=15.0,
    )

    print(f"✓ 服务部署成功！")
    print(f"  服务名: {group.service_name}")
    print(f"  部署节点: {list(group.sessions.keys())}")
    print(f"  所有者: {group.owner_client_id}")
    print()

    # 启动心跳
    group.start_keepalive()

    # 使用服务
    try:
        for i in range(10):
            node_id, resp = group.call_balanced(
                "sort",                        # 调用方法
                {"data": list(range(10))},    # 参数
                timeout_sec=10,
            )
            print(f"调用 {node_id}: {resp['data']}")
    finally:
        group.end("demo完成")
        group.close(end_services=False)


if __name__ == "__main__":
    print("""
    PyCloud 部署示例
    ================

    选择部署方式：
    1. artifact_paths (文件/文件夹列表) - 推荐
    2. 手动打包 zip
    3. 直接提供代码 (blob)

    请修改代码中的路径后运行。
    """)

    # 示例 1：文件列表（推荐）
    main()

    # 示例 2：手动 zip
    # example_with_zip()

    # 示例 3：直接代码
    # example_with_blob()

    # 示例 4：完整服务
    # example_complete_service()

    print("请取消注释其中一个函数来运行对应示例")
