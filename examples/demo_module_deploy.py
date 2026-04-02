#!/usr/bin/env python3
"""
模块对象部署演示

展示 DeployedService 和 TaskSubmitter 的模块对象自动部署功能。
当前会自动打包模块对应的本地 package 树与资源文件。
"""

import sys
import os
import tempfile
import shutil

# 添加项目路径

from pycloud_parallel import DeployedService, TaskSubmitter


def demo_create_test_module():
    """创建测试模块"""
    # 创建临时目录
    temp_dir = tempfile.mkdtemp(prefix="pycloud_module_test_")
    sys.path.insert(0, temp_dir)
    created = False

    try:
        # 创建测试模块
        module_file = os.path.join(temp_dir, "my_processor.py")
        with open(module_file, "w") as f:
            f.write("""
\"\"\"数据处理模块\"\"\"

import json
import math


def process_data(data):
    \"\"\"处理数据\"\"\"
    result = {
        "sum": sum(data),
        "mean": sum(data) / len(data),
        "count": len(data),
    }
    return json.dumps(result)


def square(x):
    \"\"\"计算平方\"\"\"
    return x ** 2


def cube(x):
    \"\"\"计算立方\"\"\"
    return x ** 3


class Processor:
    \"\"\"处理器类\"\"\"

    def __init__(self, factor=1):
        self.factor = factor

    def process(self, data):
        return data * self.factor
""")

        # 导入模块
        import my_processor
        created = True
        return my_processor, temp_dir
    finally:
        if not created:
            sys.path.remove(temp_dir)
            shutil.rmtree(temp_dir)


def demo_task_submitter_with_module():
    """演示: TaskSubmitter 部署模块"""
    print("=" * 70)
    print("  演示: TaskSubmitter 部署模块")
    print("=" * 70)
    print()

    # 创建测试模块
    print("[1] 创建测试模块...")
    print("-" * 70)
    my_processor, temp_dir = demo_create_test_module()
    print(f"✓ 模块创建成功: {my_processor.__name__}")
    print(f"  模块文件: {my_processor.__file__}")
    print(f"  导出的函数: process_data, square, cube")
    print()

    try:
        print("[2] 部署模块（自动打包本地 package）...")
        print("-" * 70)

        submitter = TaskSubmitter.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            module=my_processor,  # ← 直接传模块对象！
            runtime="py3",
            tags=["compute"],
        )

        print(f"✓ 任务客户端创建成功")
        print(f"  client_id: {submitter.client_id}")
        print(f"  job_id: {submitter.job_id}")
        print(f"  code_version: {submitter.code_version}")
        print(f"  entry_module: my_processor (自动推断)")
        print()

        print("[3] 调用模块函数...")
        print("-" * 70)

        # 调用 process_data
        result = submitter.process_data(data=[1, 2, 3, 4, 5])
        print(f"✓ process_data([1,2,3,4,5]): {result}")
        print()

        # 调用 square
        result = submitter.square(x=5)
        print(f"✓ square(5): {result}")
        print()

        # 调用 cube
        result = submitter.cube(x=3)
        print(f"✓ cube(3): {result}")
        print()

    finally:
        # 清理
        sys.path.remove(temp_dir)
        shutil.rmtree(temp_dir)

        if 'submitter' in locals():
            print("[4] 清理")
            print("-" * 70)
            submitter.close()
            print("✓ 客户端已关闭")
        print()


def demo_comparison():
    """演示: 对比三种部署方式"""
    print("=" * 70)
    print("  演示: 三种部署方式对比")
    print("=" * 70)
    print()

    print("【方式 1】传统方式 - 从文件部署:")
    print("-" * 70)
    print("""
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="./my_module.py",
    runtime="py3",
)
""")
    print()

    print("【方式 2】新方式 - 从函数部署:")
    print("-" * 70)
    print("""
def square(x):
    return x ** 2

submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    func=square,  # ← 传函数对象
    runtime="py3",
)
""")
    print()

    print("【方式 3】最新方式 - 从模块部署:")
    print("-" * 70)
    print("""
import my_module

submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    module=my_module,  # ← 传模块对象
    runtime="py3",
)

# ��以调用模块中的任何函数
result1 = submitter.square(x=5)
result2 = submitter.cube(x=3)
result3 = submitter.process_data(data=[1,2,3])
""")
    print()


def main():
    print()
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 25 + "模块对象部署演示" + " " * 27 + "║")
    print("╚" + "=" * 68 + "╝")
    print()

    # 对比三种方式
    demo_comparison()

    # 实际部署模块（需要服务运行）
    # demo_task_submitter_with_module()

    print("=" * 70)
    print("  演示完成!")
    print("=" * 70)
    print()
    print("✅ 新功能:")
    print("  1. ✅ DeployedService.deploy_from_infocenter(module=...)")
    print("  2. ✅ TaskSubmitter.from_infocenter(module=...)")
    print("  3. ✅ 自动打包整个本地模块 / package")
    print("  4. ✅ 可以调用模块中的任何导出函数")
    print()
    print("📋 优势:")
    print("  - 一次性部署整个模块")
    print("  - 可以调用模块中的多个函数")
    print("  - 自动带上本地源码依赖和资源文件")
    print("  - entry_module 自动推断为模块名")
    print()
    print("💡 使用场景:")
    print("  - 模块包含多个相关函数")
    print("  - 需要共享代码和依赖")
    print("  - 模块级别的代码组织")
    print("  - 第三方包缺失时，再显式传 dependency_allowlist")
    print()


if __name__ == "__main__":
    main()
