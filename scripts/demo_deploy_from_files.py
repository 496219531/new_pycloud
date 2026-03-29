#!/usr/bin/env python3
"""
PyCloud 部署服务示例：从多个文件/文件夹部署

演示如何将多个文件和文件夹自动打包成 zip 部署到多个节点

使用方式：
    1. 确保 InfoCenter 和 NodeControl 已启动
       或使用 --start-services 自动启动
    2. 运行脚本
"""
from pycloud_parallel.controlplane.client import MultiNodeServiceGroup


def demo_service_code():
    """生成示例服务代码文件"""
    return {
        "compute_service/__init__.py": '"""计算服务模块"""',

        "compute_service/main.py": '''
# 定义导出装饰器
def pycloud_export(fn):
    fn.__pycloud_export__ = True
    return fn

# 内联算法实现，避免跨模块导入
def quick_sort(arr):
    """快速排序实现"""
    if len(arr) <= 1:
        return arr
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    return quick_sort(left) + middle + quick_sort(right)

def binary_search(arr, target):
    """二分查找实现"""
    left, right = 0, len(arr) - 1
    while left <= right:
        mid = (left + right) // 2
        if arr[mid] == target:
            return mid
        elif arr[mid] < target:
            left = mid + 1
        else:
            right = mid - 1
    return -1

@pycloud_export
def quick_sort(payload):
    """快速排序"""
    data = payload.get("data", [])
    return {"result": quick_sort(data), "algorithm": "quick_sort"}

@pycloud_export
def binary_search(payload):
    """二分查找"""
    data = payload.get("data", [])
    target = payload.get("target", 0)
    idx = binary_search(data, target)
    return {"result": idx, "target": target, "found": idx >= 0}

@pycloud_export
def process(payload):
    """通用处理函数"""
    action = payload.get("action", "")
    data = payload.get("data", [])
    if action == "sort":
        return {"result": sorted(data)}
    elif action == "reverse":
        return {"result": list(reversed(data))}
    else:
        return {"error": f"Unknown action: {action}"}
''',

        "compute_service/utils.py": '''
"""工具函数模块"""

def validate_data(data):
    """验证数据"""
    if not isinstance(data, list):
        raise ValueError("data must be a list")
    return data

def format_result(result, meta=None):
    """格式化结果"""
    output = {"data": result}
    if meta:
        output["meta"] = meta
    return output
''',

        "compute_service/algorithms/__init__.py": '"""算法模块"""',

        "compute_service/algorithms/sort.py": '''
"""排序算法"""

def quick_sort(arr):
    """快速排序实现"""
    if len(arr) <= 1:
        return arr
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    return quick_sort(left) + middle + quick_sort(right)

def merge_sort(arr):
    """归并排序实现"""
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    return merge(left, right)

def merge(left, right):
    """归并两个已排序数组"""
    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result
''',

        "compute_service/algorithms/search.py": '''
"""搜索算法"""

def binary_search(arr, target):
    """二分查找"""
    left, right = 0, len(arr) - 1
    while left <= right:
        mid = (left + right) // 2
        if arr[mid] == target:
            return mid
        elif arr[mid] < target:
            left = mid + 1
        else:
            right = mid - 1
    return -1

def linear_search(arr, target):
    """线性查找"""
    for i, val in enumerate(arr):
        if val == target:
            return i
    return -1
'''
    }


def create_temp_service_files():
    """创建临时示例服务文件"""
    import tempfile
    from pathlib import Path

    # 创建临时目录
    temp_dir = Path(tempfile.mkdtemp(prefix="pycloud_demo_"))

    # 生成代码文件
    files = demo_service_code()
    for file_path, content in files.items():
        full_path = temp_dir / file_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(content, encoding="utf-8")

    return temp_dir


def check_and_start_services():
    """检查并启动 PyCloud 服务"""
    import subprocess
    import time
    from pathlib import Path

    script_dir = Path(__file__).parent
    start_script = script_dir / "start_services.sh"

    if not start_script.exists():
        print("⚠️  未找到启动脚本")
        print(f"   请确保 {start_script} 存在")
        return False

    print("检查 PyCloud 服务状态...")

    # 检查是否有进程在运行
    try:
        result = subprocess.run(
            ["pgrep", "-f", "pycloud_parallel.controlplane.server"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print("✓ PyCloud 服务已运行")
            return True
    except Exception:
        pass

    # 启动服务
    print("正在启动 PyCloud 服务...")
    try:
        proc = subprocess.Popen(
            [str(start_script), "start"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # 等待启动
        time.sleep(3)

        # 检查是否成功启动
        result = subprocess.run(
            ["pgrep", "-f", "pycloud_parallel.controlplane.server"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print("✓ PyCloud 服务启动成功")
            return True
        else:
            print("✗ 服务启动失败")
            stdout, stderr = proc.communicate()
            if stderr:
                print(f"  错误: {stderr}")
            return False

    except Exception as exc:
        print(f"✗ 启动服务失败: {exc}")
        return False


def cleanup_existing_services():
    """清理已存在的服务"""
    from pycloud_parallel.controlplane.client import InfoCenterClient

    print("检查并清理已存在的服务...")
    try:
        with InfoCenterClient('127.0.0.1:50051') as client:
            routes = list(client.list_service_routes(
                service_name="compute-service",
                healthy_only=False,
                limit=10
            ))
            if routes:
                print(f"  找到 {len(routes)} 个已存在的服务实例")
                # 自动结束这些服务
                from pycloud_parallel.controlplane.client import NodeControlClient
                for route in routes:
                    try:
                        nc = NodeControlClient(route.control_addr, timeout_sec=5)
                        nc.end_service(
                            owner_client_id="demo-compute-001",
                            service_id=route.service_id,
                            reason="demo cleanup"
                        )
                        print(f"  ✓ 清理 {route.node_id}")
                    except Exception as exc:
                        print(f"  ✗ 清理 {route.node_id} 失败: {exc}")
                print()
                time.sleep(1)  # 等待清理完成
    except Exception as exc:
        print(f"  跳过清理: {exc}")
        print()


def main():
    """主函数：部署服务"""
    import tempfile
    import time
    import shutil
    from pathlib import Path

    print("=" * 60)
    print("  PyCloud 部署服务示例：多文件项目")
    print("=" * 60)
    print()

    # 检查并启动服务
    check_and_start_services()
    print()

    # 清理已存在的服务
    print("-" * 60)
    print("  清理已存在的服务...")
    print("-" * 60)
    try:
        from pycloud_parallel.controlplane.client import InfoCenterClient, NodeControlClient

        with InfoCenterClient('127.0.0.1:50051', timeout_sec=5) as client:
            routes = list(client.list_service_routes(
                service_name="compute-service",
                healthy_only=False,
                limit=10
            ))
            if routes:
                print(f"  找到 {len(routes)} 个已存在的服务实例，正在清理...")
                for route in routes:
                    try:
                        nc = NodeControlClient(route.control_addr, timeout_sec=5)
                        nc.end_service(
                            owner_client_id="demo-compute-001",
                            service_id=route.service_id,
                            reason="demo cleanup"
                        )
                        print(f"    ✓ {route.node_id}")
                    except Exception as exc:
                        print(f"    ✗ {route.node_id}: {exc}")
                time.sleep(1)
    except Exception as exc:
        print(f"  跳过清理: {exc}")
    print()

    # 方式 1: 使用 artifact_paths 部署
    print("-" * 60)
    print("  方式 1: 使用 artifact_paths（推荐）")
    print("-" * 60)
    print()

    # 创建临时服务文件
    temp_dir = create_temp_service_files()
    service_dir = temp_dir / "compute_service"

    print(f"✓ 创建临时服务文件: {service_dir}")
    print(f"  文件结构:")
    for file in sorted(service_dir.rglob("*")):
        if file.is_file():
            rel_path = file.relative_to(service_dir)
            print(f"    - {rel_path}")
    print()

    group = None
    try:
        # 部署服务
        print("-" * 60)
        print("  部署服务...")
        print("-" * 60)
        print()

        try:
            group = MultiNodeServiceGroup.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="demo-compute-001",
                service_name="compute-service",

                # 使用 artifact_paths 部署多个文件/文件夹
                artifact_paths=[
                    str(service_dir / "main.py"),
                ],

                # 入口配置
                entry_module="main",                # 对应 main.py（去掉 .py）
                entry_callable="process",            # 默认函数

                # 导出配置
                export_mode="decorator",
                export_decorator="pycloud_export",

                # 运行时配置
                runtime="py3.11",
                worker_count=4,
                heartbeat_timeout_sec=30,

                # 部署配置
                expose_http=True,
                healthy_only=True,
                tags=["compute"],  # 只使用 "compute" 标签
                min_success_nodes=1,
                allow_partial=True,
            )
        except RuntimeError as exc:
            if "no available nodes from InfoCenter" in str(exc):
                print("✗ 部署失败：没有可用的节点")
                print()
                print("  请先启动 PyCloud 服务：")
                print("    ./scripts/start_services.sh start")
                print()
                print("  或在运行此脚本时使用：")
                print("    python scripts/demo_deploy_from_files.py --start-services")
                return
            raise

        print(f"✓ 服务部署成功！")
        print(f"  服务名: {group.service_name}")
        print(f"  部署节点: {list(group.sessions.keys())}")
        print(f"  所有者: {group.owner_client_id}")
        print()

        # 启动心跳
        group.start_keepalive()

        # 测试调用
        print("-" * 60)
        print("  测试调用")
        print("-" * 60)
        print()

        test_cases = [
            {
                "method": "quick_sort",
                "payload": {"data": [5, 2, 8, 1, 9]},
                "desc": "快速排序"
            },
            {
                "method": "binary_search",
                "payload": {"data": [1, 3, 5, 7, 9], "target": 5},
                "desc": "二分查找"
            },
            {
                "method": "process",
                "payload": {"action": "sort", "data": [3, 1, 2]},
                "desc": "通用处理"
            },
        ]

        for test in test_cases:
            try:
                node_id, resp = group.call_balanced(
                    test["method"],
                    test["payload"],
                    timeout_sec=10
                )
                print(f"✓ {test['desc']}")
                print(f"  方法: {test['method']}")
                print(f"  节点: {node_id}")
                print(f"  结果: {resp.get('data')}")
                print()
            except Exception as exc:
                print(f"✗ {test['desc']}: {exc}")
                print()

        print("=" * 60)
        print("  示例完成")
        print("=" * 60)

    finally:
        # 清理
        try:
            shutil.rmtree(temp_dir)
            print(f"\n✓ 清理临时文件: {temp_dir}")
        except Exception as exc:
            print(f"\n✗ 清理失败: {exc}")

        if group is not None:
            group.close(end_services=False)


if __name__ == "__main__":
    import sys

    print("""
    PyCloud 部署示例
    ================
    """)

    # 检查命令行参数
    if "--start-services" in sys.argv:
        print("参数: --start-services")
        print("自动启动 PyCloud 服务...")
        print()
        check_and_start_services()
        print()

    # 运行主函数
    main()
