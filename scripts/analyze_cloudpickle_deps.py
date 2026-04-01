#!/usr/bin/env python3
"""
分析 Cloudpickle 的依赖发现逻辑

研究 cloudpickle 如何序列化函数和模块，以实现我们的自动依赖检测。
"""

import sys
import inspect
import dis
from pathlib import Path

print("=" * 70)
print("  Cloudpickle 依赖分析研究")
print("=" * 70)
print()

# ==============================================================================
# 1. Cloudpickle 序列化时捕获的信息
# ==============================================================================

print("1. Cloudpickle 序列化的内容")
print("-" * 70)
print()

import cloudpickle

def example_function(x):
    import numpy as np  # ← 内部导入
    import pandas as pd
    return np.sum(x) + pd.Series([1, 2, 3]).sum()

# 序列化
pickle_bytes = cloudpickle.dumps(example_function)

print(f"函数: {example_function.__name__}")
print(f"序列化大小: {len(pickle_bytes)} bytes")
print()

# 反序列化查看内容（部分）
import pickle
import io

# 尝试分析 pickle 内容
class PickleAnalyzer:
    """分析 pickle 内容的工具"""
    def __init__(self):
        self.modules = set()
        self.functions = set()
        self.classes = set()
        self.code_objects = []

    def analyze(self, pickle_bytes):
        """分析 pickle 字节流"""
        import pickletools
        stream = io.BytesIO(pickle_bytes)

        try:
            # 使用 pickletools 分析
            pickletools.dis(pickle_bytes)
        except Exception as e:
            print(f"⚠️ pickletools 分析失败: {e}")

        # 尝试反序列化并记录
        try:
            obj = pickle.loads(pickle_bytes)
            print(f"✓ 成功反序列化: {type(obj)}")
            self._inspect_object(obj)
        except Exception as e:
            print(f"✗ 反序列化失败: {e}")

    def _inspect_object(self, obj, depth=0):
        """递归检查对象"""
        indent = "  " * depth

        if inspect.isfunction(obj):
            print(f"{indent}📦 函数: {obj.__name__}")
            print(f"{indent}   模块: {obj.__module__}")
            print(f"{indent}   文件: {inspect.getfile(obj)}")

            # 检查代码对象
            code = obj.__code__
            print(f"{indent}   代码对象:")
            print(f"{indent}     - co_consts: {len(code.co_consts)} 个常量")
            print(f"{indent}     - co_names: {code.co_names}")
            print(f"{indent}     - co_varnames: {code.co_varnames}")

            # 检查全局变量引用
            print(f"{indent}   全局引用:")
            for name in code.co_names:
                if name in obj.__globals__:
                    ref = obj.__globals__[name]
                    print(f"{indent}     - {name}: {type(ref).__name__}")

        elif inspect.ismethod(obj):
            print(f"{indent}📦 方法: {obj.__name__}")
            self._inspect_object(obj.__func__, depth + 1)

        elif inspect.isclass(obj):
            print(f"{indent}📦 类: {obj.__name__}")
            print(f"{indent}   模块: {obj.__module__}")

analyzer = PickleAnalyzer()
analyzer.analyze(pickle_bytes)

print()

# ==============================================================================
# 2. 字节码分析：找出导入的模块
# ==============================================================================

print("2. 字节码分析：找出 IMPORT 指令")
print("-" * 70)
print()

def analyze_imports_from_bytecode(func):
    """通过字节码分析函数的导入"""
    code = func.__code__

    print(f"函数: {func.__name__}")
    print(f"文件: {inspect.getsourcefile(func)}")
    print()

    # 反汇编字节码
    instructions = list(dis.get_instructions(code))

    # 找出所有 IMPORT 相关指令
    imports = []
    for instr in instructions:
        if instr.opname.startswith("IMPORT_"):
            imports.append({
                "opcode": instr.opname,
                "arg": instr.arg,
                "argval": instr.argval,
                "lineno": instr.starts_line,
            })

    if imports:
        print("导入指令:")
        for imp in imports:
            print(f"  {imp}")
    else:
        print("  无直接导入指令")

    print()

    # 分析 co_names（导入的名称）
    print(f"co_names (可能导入的模块/对象): {code.co_names}")
    print()

    # 分析 co_consts（常量，包括代码对象）
    print("co_consts (嵌套的代码对象):")
    for i, const in enumerate(code.co_consts):
        if hasattr(const, 'co_code'):  # 是代码对象
            print(f"  [{i}] 代码对象:")
            print(f"      co_names: {const.co_names}")
            print(f"      co_filename: {const.co_filename}")

    return imports

imports = analyze_imports_from_bytecode(example_function)

# ==============================================================================
# 3. 源代码分析：找出 import 语句
# ==============================================================================

print("3. 源代码分析：找出 import 语句")
print("-" * 70)
print()

import ast
import re

def extract_imports_from_source(func):
    """从函数源码提取 import 语句"""
    try:
        source = inspect.getsource(func)
    except OSError:
        print("⚠️ 无法获取源代码")
        return []

    print(f"函数源码 ({func.__name__}):")
    print(source)
    print()

    # 方式 1: AST 解析
    try:
        tree = ast.parse(source)

        imports_ast = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports_ast.append({
                        "type": "import",
                        "module": alias.name,
                        "asname": alias.asname,
                    })
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    imports_ast.append({
                        "type": "from...import",
                        "module": module,
                        "name": alias.name,
                        "asname": alias.asname,
                    })

        if imports_ast:
            print("AST 分析结果:")
            for imp in imports_ast:
                print(f"  {imp}")
        else:
            print("  无 import 语句")

    except SyntaxError as e:
        print(f"⚠️ AST 解析失败: {e}")

    print()

    # 方式 2: 正则表达式（备用）
    import_lines = []
    for line in source.split('\n'):
        line = line.strip()
        if line.startswith(('import ', 'from ')):
            import_lines.append(line)

    if import_lines:
        print("正则匹配结果:")
        for line in import_lines:
            print(f"  {line}")

    return imports_ast

imports_ast = extract_imports_from_source(example_function)

# ==============================================================================
# 4. 完整的依赖分析
# ==============================================================================

print("4. 完整依赖分析")
print("-" * 70)
print()

def analyze_full_dependencies(func):
    """完整分析函数的所有依赖"""
    result = {
        "function": func.__name__,
        "file": inspect.getfile(func),
        "module": func.__module__,
        "imports": [],
        "local_files": [],
        "stdlib_modules": [],
        "third_party_modules": [],
    }

    # 获取源码
    try:
        source = inspect.getsource(func)
    except OSError:
        source = ""

    # AST 分析导入
    if source:
        try:
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        result["imports"].append({
                            "type": "import",
                            "module": alias.name,
                        })
                elif isinstance(node, ast.ImportFrom):
                    result["imports"].append({
                        "type": "from...import",
                        "module": node.module or "",
                    })
        except SyntaxError:
            pass

    # 分类导入
    for imp in result["imports"]:
        module_name = imp["module"]

        # 检查是否是标准库
        if module_name in sys.stdlib_module_names:
            result["stdlib_modules"].append(module_name)
        # 检查是否是已安装的第三方库
        else:
            try:
                mod = __import__(module_name)
                mod_file = getattr(mod, '__file__', '')
                if 'site-packages' in mod_file or 'dist-packages' in mod_file:
                    result["third_party_modules"].append(module_name)
                else:
                    result["local_files"].append(mod_file)
            except ImportError:
                result["local_files"].append(module_name)

    return result

deps = analyze_full_dependencies(example_function)

print("依赖分析结果:")
print(f"  函数: {deps['function']}")
print(f"  文件: {deps['file']}")
print(f"  模块: {deps['module']}")
print(f"  导入: {deps['imports']}")
print(f"  标准库: {deps['stdlib_modules']}")
print(f"  第三方库: {deps['third_party_modules']}")
print(f"  本地文件: {deps['local_files']}")

print()

# ==============================================================================
# 5. 实际案例：复杂函数
# ==============================================================================

print("5. 复杂案例分析")
print("-" * 70)
print()

def complex_function(data):
    """包含多种导入的复杂函数"""
    import os
    import sys
    import json
    import numpy as np
    from pandas import DataFrame
    from collections import defaultdict

    result = {
        "sum": np.sum(data),
        "df": DataFrame(data),
        "counts": defaultdict(int),
    }

    return result

deps_complex = analyze_full_dependencies(complex_function)

print("复杂函数依赖:")
print(f"  标准库: {deps_complex['stdlib_modules']}")
print(f"  第三方库: {deps_complex['third_party_modules']}")
print()

# ==============================================================================
# 6. 总结：自动依赖检测的关键点
# ==============================================================================

print("6. 自动依赖检测的关键点")
print("-" * 70)
print()

print("""
✅ Cloudpickle 自动捕获的内容：
  1. 函数的代码对象（字节码）
  2. 函数的全局变量字典（__globals__）
  3. 函数的闭包变量（__closure__）
  4. 函数的默认参数（__defaults__）
  5. 代码对象中引用的所有模块和函数

✅ 我们可以借鉴的方法：
  1. 字节码分析：找出 IMPORT_NAME 指令
  2. 源码 AST 分析：找出 import 语句
  3. 全局变量遍历：找出引用的模块
  4. 递归分析：分析嵌套函数和类

✅ 自动打包策略：
  1. 获取函数所在模块的文件路径
  2. 分析函数的所有导入
  3. 区分标准库、第三方库、本地模块
  4. 打包函数所在文件 + 本地依赖文件
  5. 排除标准库和第三方库（假设目标环境已安装）

📋 实现步骤：
  1. extract_function_dependencies(func) → 分析依赖
  2. find_module_files(module_name) → 查找文件路径
  3. build_dependency_package(func) → 创建 tar.gz
  4. upload_with_dependencies(func) → 上传到 PyCloud
""")

print()
print("=" * 70)
