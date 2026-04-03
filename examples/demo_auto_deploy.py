#!/usr/bin/env python3
"""
本地源码自动打包部署演示

展示 DeployedService 和 TaskSubmitter 如何直接接收函数对象，
并自动打包本地源码依赖。
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pycloud_parallel import DeployedService, TaskSubmitter


def demo_deployed_service_with_function():
    """演示 1: DeployedService 自动部署函数"""
    print("=" * 70)
    print("  演示 1: DeployedService 自动部署函数")
    print("=" * 70)
    print()

    # 定义服务函数
    def process_data(x):
        """数据处理服务"""
        import math
        import json
        result = math.sqrt(x)
        return {"result": result, "status": "ok"}

    try:
        print("[1] 部署服务（自动打包本地源码依赖）...")
        print("-" * 70)

        group = DeployedService.deploy_from_infocenter(
            infocenter_target="127.0.0.1:50051",
            func=process_data,  # ← 直接传函数对象
            runtime="py3",
            worker_count=2,
            tags=["compute"],
            min_success_nodes=1,
        )

        print(f"✓ 服务部署成功")
        print(f"  服务名: {group.service_name}")
        print(f"  节点: {list(group.sessions.keys())}")
        print()

        import time
        time.sleep(3)

        print("[2] 调用服务...")
        print("-" * 70)

        result = group.process_data.sync(x=16)
        print(f"✓ 调用结果: {result}")
        print()

    finally:
        print("[3] 清理服务")
        print("-" * 70)
        if 'group' in locals():
            group.close(end_services=True)
            print("✓ 服务已停止")
        print()


def demo_task_submitter_with_function():
    """演示 2: TaskSubmitter 自动部署函数"""
    print("=" * 70)
    print("  演示 2: TaskSubmitter 自动部署函数")
    print("=" * 70)
    print()

    # 定义任务函数
    def square(x):
        """计算平方"""
        return x ** 2

    try:
        print("[1] 创建任务客户端（自动打包本地源码依赖）...")
        print("-" * 70)

        submitter = TaskSubmitter.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            func=square,  # ← 直接传函数对象
            runtime="py3",
            tags=["compute"],
        )

        print(f"✓ 任务客户端创建成功")
        print(f"  client_id: {submitter.client_id}")
        print(f"  job_id: {submitter.job_id}")
        print(f"  code_version: {submitter.code_version}")
        print(f"  节点: {list(submitter.nodes.keys())}")
        print()

        print("[2] 提交任务...")
        print("-" * 70)

        # 提交单个任务
        result = submitter.square(x=5)
        print(f"✓ 任务结果: {result}")
        print()

        print("[3] 批量提交任务...")
        print("-" * 70)

        # 批量提交
        results = submitter.square.submit(x=[1, 2, 3, 4, 5])
        print(f"✓ 已提交 {len(results)} 个任务")

        # 等待结果
        completed = submitter.wait_for_results(expected_count=5)
        print(f"✓ 完成 {len(completed)} 个任务:")
        for result in completed:
            if result.status == "TASK_STATUS_SUCCEEDED":
                print(f"  {result.task_id}: {result.result}")
        print()

    finally:
        print("[4] 清理")
        print("-" * 70)
        if 'submitter' in locals():
            submitter.close()
            print("✓ 客户端已关闭")
        print()


def demo_comparison():
    """演示 3: 对比传统方式和新的自动方式"""
    print("=" * 70)
    print("  演示 3: 传统方式 vs 自动方式对比")
    print("=" * 70)
    print()

    # 定义函数
    def my_function(data):
        """示例函数"""
        import numpy as np
        return np.sum(data)

    print("【传统方式】手动打包代码:")
    print("-" * 70)
    print("""
# 1. 需要先将函数保存到文件
# 2. 或者手动创建 blob
blob = b'''
def my_function(data):
    import numpy as np
    return np.sum(data)
'''

# 3. 然后部署
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    runtime="py3",
    entry_module="my_module",
    dependency_allowlist=["./third_party/my_local_pkg"],  # 可选
)
""")
    print()

    print("【自动方式】直接传函数对象:")
    print("-" * 70)
    print("""
# 1. 直接传函数对象
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    func=my_function,  # ← 一行搞定！
    runtime="py3",
    # 如果远端节点缺第三方包，可再显式传 dependency_allowlist
)

# 自动完成：
# ✓ 收集本地源码依赖
# ✓ 打包函数所在模块 / package
# ✓ 推断 entry_module 和 entry_callable
# ✓ 上传到 PyCloud
# 第三方包如果远端缺失，仍建议显式传 dependency_allowlist
""")
    print()


def main():
    print()
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 18 + "本地源码自动打包部署演示" + " " * 26 + "║")
    print("╚" + "=" * 68 + "╝")
    print()

    # 演示对比
    demo_comparison()

    # 演示 DeployedService
    # demo_deployed_service_with_function()

    # 演示 TaskSubmitter
    # demo_task_submitter_with_function()

    print("=" * 70)
    print("  所有演示完成!")
    print("=" * 70)
    print()
    print("✅ 新功能:")
    print("  1. ✅ DeployedService.deploy_from_infocenter(func=...)")
    print("  2. ✅ TaskSubmitter.from_infocenter(func=...)")
    print("  3. ✅ 自动打包本地源码依赖")
    print("  4. ✅ 自动推断 entry_module 和 entry_callable")
    print()
    print("📋 使用方式:")
    print("  - 之前：需要手动打包代码或创建 blob")
    print("  - 现在：直接传函数对象，自动处理本地源码打包")
    print()
    print("💡 提示:")
    print("  - 函数会被自动打包成 tar.gz")
    print("  - 本地源码依赖和 package 资源会自动包含")
    print("  - 如果远端缺第三方包，可显式传 dependency_allowlist")
    print()


if __name__ == "__main__":
    main()
