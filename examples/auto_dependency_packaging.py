#!/usr/bin/env python3
"""
本地源码自动打包演示

展示如何使用 DependencyAnalyzer 和 DependencyPackager
分析函数/模块的本地源码依赖并打包。
"""

import sys
import os

# 添加项目路径（优先使用仓库内 src）
REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if REPO_SRC not in sys.path:
    sys.path.insert(0, REPO_SRC)

from pycloud_parallel.controlplane.dependency import (
    DependencyAnalyzer,
    DependencyPackager,
    auto_deploy_function,
)


def demo_simple_function():
    """演示 1: 简单函数的依赖分析"""
    print("=" * 70)
    print("  演示 1: 简单函数的依赖分析")
    print("=" * 70)
    print()

    def process_data(x):
        """一个简单的数据处理函数"""
        import math
        import json
        result = math.sqrt(x)
        return json.dumps({"result": result})

    analyzer = DependencyAnalyzer()
    deps = analyzer.analyze_function(process_data)

    print(f"函数: {deps['function_name']}")
    print(f"所在模块: {deps['module']}")
    print(f"源文件: {deps['source_file']}")
    print()

    print("导入的模块:")
    for imp in deps['imports']:
        print(f"  - {imp['type']}: {imp['module']}")
    print()

    print("标准库模块:")
    print(f"  {deps['stdlib_modules']}")
    print()

    print("第三方库模块:")
    print(f"  {deps['third_party_modules']}")
    print()

    print("本地模块:")
    for mod in deps['local_modules']:
        print(f"  - {mod['name']}: {mod['file']}")
    print()


def demo_function_with_local_deps():
    """演示 2: 带本地依赖的函数"""
    print("=" * 70)
    print("  演示 2: 带本地依赖的函数")
    print("=" * 70)
    print()

    # 创建一个本地模块
    import tempfile
    import shutil

    temp_dir = tempfile.mkdtemp(prefix="pycloud_demo_")

    try:
        # 创建本地工具模块
        utils_module = os.path.join(temp_dir, "my_utils.py")
        with open(utils_module, "w") as f:
            f.write("""
def helper_function(x):
    '''辅助函数'''
    return x * 2

class HelperClass:
    '''辅助类'''
    def __init__(self, value):
        self.value = value

    def process(self):
        return self.value + 10
""")

        # 创建主函数模块
        main_module = os.path.join(temp_dir, "main_module.py")
        with open(main_module, "w") as f:
            f.write("""
import numpy as np
from my_utils import helper_function, HelperClass

def process_data(data):
    '''使用本地依赖的主函数'''
    result = helper_function(data)
    helper = HelperClass(result)
    return helper.process() + np.sum([data])
""")

        # 添加到 Python 路径
        sys.path.insert(0, temp_dir)

        # 导入并分析
        from main_module import process_data

        analyzer = DependencyAnalyzer()
        deps = analyzer.analyze_function(process_data)

        print(f"函数: {deps['function_name']}")
        print(f"源文件: {deps['source_file']}")
        print()

        print("导入的模块:")
        for imp in deps['imports']:
            print(f"  - {imp['type']}: {imp.get('module', '')} -> {imp.get('name', '')}")
        print()

        print("标准库模块:")
        print(f"  {deps['stdlib_modules']}")
        print()

        print("第三方库模块:")
        print(f"  {deps['third_party_modules']}")
        print()

        print("本地模块 (需要打包):")
        for mod in deps['local_modules']:
            print(f"  ✓ {mod['name']}: {mod['file']}")
        print()

    finally:
        # 清���
        sys.path.remove(temp_dir)
        shutil.rmtree(temp_dir)


def demo_auto_packaging():
    """演示 3: 自动打包"""
    print("=" * 70)
    print("  演示 3: 自动打包函数和本地源码依赖")
    print("=" * 70)
    print()

    # 创建测试函数
    def my_processor(data):
        """数据处理函数"""
        import json
        import math
        from collections import defaultdict

        result = {
            "sum": sum(data),
            "mean": sum(data) / len(data),
            "count": len(data),
        }

        return json.dumps(result)

    packager = DependencyPackager()

    print("正在打包函数和依赖...")
    package_path = packager.package_function(
        my_processor,
        output_file="/tmp/demo_auto_package.tar.gz",
        include_tests=False,
    )

    print(f"✓ 打包完成: {package_path}")
    print()

    # 查看包内容
    import tarfile
    print("包内容:")
    with tarfile.open(package_path, "r:gz") as tar:
        for member in tar.getmembers():
            print(f"  - {member.name} ({member.size} bytes)")
    print()

    # 清理
    os.remove(package_path)
    print("✓ 清理完成")
    print()


def demo_complex_function():
    """演示 4: 复杂函数的依赖分析"""
    print("=" * 70)
    print("  演示 4: 复杂函数的依赖分析")
    print("=" * 70)
    print()

    def complex_processor(df):
        """包含多种依赖的复杂函数"""
        import os
        import sys
        import json
        import numpy as np
        import pandas as pd
        from collections import defaultdict, Counter
        from typing import List, Dict

        # 使用多种库
        result = {
            "shape": df.shape,
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        }

        return result

    analyzer = DependencyAnalyzer()
    deps = analyzer.analyze_function(complex_processor)

    print(f"函数: {deps['function_name']}")
    print()

    print("导入的模块 ({len(deps['imports'])} 个):")
    for imp in deps['imports']:
        module = imp.get('module', '')
        name = imp.get('name', '')
        if name:
            print(f"  - from {module} import {name}")
        else:
            print(f"  - import {module}")
    print()

    print("分类结果:")
    print(f"  标准库 ({len(deps['stdlib_modules'])}): {', '.join(deps['stdlib_modules'][:5])}...")
    print(f"  第三方库 ({len(deps['third_party_modules'])}): {', '.join(deps['third_party_modules'])}")
    print(f"  本地模块 ({len(deps['local_modules'])}): {len(deps['local_modules'])} 个")
    print()


def demo_module_analysis():
    """演示 5: 模块级依赖分析"""
    print("=" * 70)
    print("  演示 5: 模块级依赖分析")
    print("=" * 70)
    print()

    # 分析标准库模块
    analyzer = DependencyAnalyzer()

    print("分析标准库模块 (json):")
    deps = analyzer.analyze_module("json")
    if not deps.get("error"):
        print(f"  模块文件: {deps['file']}")
        print(f"  导入数: {len(deps['imports'])}")
        print(f"  标准库依赖: {len(deps['stdlib_modules'])}")
    print()

    # 分析第三方库
    print("分析第三方库模块 (numpy):")
    deps = analyzer.analyze_module("numpy")
    if not deps.get("error"):
        print(f"  模块文件: {deps.get('file', 'N/A')}")
        print(f"  第三方库: {len(deps['third_party_modules'])}")
    print()


def main():
    print()
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 13 + "本地源码自动打包演示" + " " * 31 + "║")
    print("╚" + "=" * 68 + "╝")
    print()

    demo_simple_function()
    demo_function_with_local_deps()
    demo_auto_packaging()
    demo_complex_function()
    demo_module_analysis()

    print("=" * 70)
    print("  所有演示完成!")
    print("=" * 70)
    print()
    print("✅ 本地源码自动打包功能:")
    print("  1. ✅ 分析函数的 import 语句")
    print("  2. ✅ 区分标准库、第三方库、本地模块")
    print("  3. ✅ 自动查找本地依赖文件")
    print("  4. ✅ 打包成 tar.gz 文件")
    print("  5. ✅ 支持 __init__.py 等相关文件")
    print()
    print("📋 下一步:")
    print("  - 集成到 TaskPool / JobQueue 的自动打包入口")
    print("  - 添加 auto_deploy_function() 便捷 API")
    print("  - 优化依赖查找算法（处理复杂情况）")
    print()


if __name__ == "__main__":
    main()
