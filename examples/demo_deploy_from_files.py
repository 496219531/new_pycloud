#!/usr/bin/env python3
"""
PyCloud 部署服务示例：从多个文件/文件夹部署

演示如何将多个文件和文件夹自动打包成 zip 部署到多个节点

使用方式：
    1. 确保 InfoCenter 和 NodeControl 已启动
    2. 运行脚本
"""
from pycloud_parallel.controlplane.client import ServiceGroup


def demo_service_code():
    """生成示例服务代码文件"""
    return {
        "compute_service/__init__.py": '"""计算服务模块"""',

        "compute_service/main.py": '''
# 定义导出装饰器
def pycloud_export(fn):
    fn.__pycloud_export__ = True
    return fn

from .algorithms import sort, search

@pycloud_export
def quick_sort(payload):
    """快速排序"""
    data = payload.get("data", [])
    return {"result": sort.quick_sort(data), "algorithm": "quick_sort"}

@pycloud_export
def binary_search(payload):
    """二分查找"""
    data = payload.get("data", [])
    target = payload.get("target", 0)
    idx = search.binary_search(data, target)
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

    repo_root = Path(__file__).resolve().parents[1]
    start_script = repo_root / "scripts" / "start_services.sh"

    if not start_script.exists():
        raise FileNotFoundError(f"start script not found: {start_script}")

    print("检查 PyCloud 服务状态...")

    # 检查是否有进程在运行
    result = subprocess.run(
        ["pgrep", "-f", "pycloud_parallel.controlplane.server"],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print("✓ PyCloud 服务已运行")
        return True

    # 启动服务
    print("正在启动 PyCloud 服务...")
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

    stdout, stderr = proc.communicate()
    detail = (stderr or stdout or "").strip()
    raise RuntimeError(f"failed to start PyCloud services: {detail or 'process did not come up'}")


def main():
    """主函数：部署服务"""
    import time
    import shutil

    print("=" * 60)
    print("  PyCloud 部署服务示例：多文件项目")
    print("=" * 60)
    print()

    # 检查并启动服务
    check_and_start_services()
    print()

    service_suffix = int(time.time())
    service_name = f"compute-service-{service_suffix}"
    owner_client_id = f"demo-compute-{service_suffix}"

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
    joined = False
    try:
        # 部署服务
        print("-" * 60)
        print("  部署服务...")
        print("-" * 60)
        print()

        group = ServiceGroup.deploy_from_infocenter(
            infocenter_target="127.0.0.1:50051",
            owner_client_id=owner_client_id,
            service_name=service_name,

            # 使用 artifact_paths 部署多个文件/文件夹
            artifact_paths=[
                str(service_dir),
            ],

            # 入口配置
            entry_module="compute_service.main",  # zip 内路径：compute_service/main.py
            entry_callable="process",             # 默认函数

            # 导出配置
            export_mode="decorator",
            export_decorator="pycloud_export",

            # 运行时配置
            runtime="py3",
            worker_count=4,
            heartbeat_timeout_sec=30,

            # 部署配置
            expose_http=True,
            healthy_only=True,
            tags=["compute"],  # 只使用 "compute" 标签
            min_success_nodes=1,
            allow_partial=True,
        )

        print(f"✓ 服务部署成功！")
        print(f"  服务名: {group.service_name}")
        print(f"  部署节点: {list(group.sessions.keys())}")
        print(f"  所有者: {group.owner_client_id}")
        print()

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

        print("=" * 60)
        print("  示例完成")
        print("=" * 60)
        print("  服务进入长驻模式，按 Ctrl+C 自动回收")
        print("=" * 60)
        print()
        group.join(
            end_services_on_interrupt=True,
            end_reason="owner ctrl+c",
        )
        joined = True

    finally:
        # 清理
        shutil.rmtree(temp_dir)
        print(f"\n✓ 清理临时文件: {temp_dir}")

        if group is not None:
            group.close(
                end_services=not joined,
                reason="demo_deploy_from_files cleanup",
            )


if __name__ == "__main__":
    main()
