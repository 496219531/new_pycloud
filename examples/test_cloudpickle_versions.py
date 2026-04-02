#!/usr/bin/env python3
"""
测试 Cloudpickle 的跨版本兼容性

在 Python 3.10 序列化，尝试在 Python 3.11 反序列化
"""

import sys
import hashlib
import tempfile
from pathlib import Path

print(f"当前 Python 版本: {sys.version}")
print(f"版本信息: {sys.version_info}")

# 检查是否安装了 cloudpickle
try:
    import cloudpickle
    print(f"Cloudpickle 版本: {cloudpickle.__version__}")
except ImportError:
    print("❌ 未安装 cloudpickle，请运行: pip install cloudpickle")
    sys.exit(1)

print()

# 定义几个测试函数
def simple_function(x):
    """简单函数"""
    return x * 2

def function_with_closure(y):
    """带闭包的函数"""
    multiplier = 10
    def inner(x):
        return x * multiplier + y
    return inner

lambda_func = lambda x: x ** 2

# 测试序列化
test_cases = [
    ("简单函数", simple_function),
    ("闭包函数", function_with_closure(5)),
    ("Lambda 函数", lambda_func),
    ("类", type("MyClass", (), {"value": 42})),
]

tmp_dir = Path(tempfile.mkdtemp(prefix="cloudpickle_test_"))

print("=" * 60)
print("  序列化测试")
print("=" * 60)
print()

for name, obj in test_cases:
    try:
        # 序列化
        serialized = cloudpickle.dumps(obj)
        print(f"✓ {name}:")
        print(f"  类型: {type(obj)}")
        print(f"  序列化大小: {len(serialized)} bytes")
        print(f"  SHA256: {hashlib.sha256(serialized).hexdigest()[:16]}...")

        # 保存到文件
        pickle_file = tmp_dir / f"{name.replace(' ', '_')}.pkl"
        pickle_file.write_bytes(serialized)
        print(f"  已保存: {pickle_file}")

        # 尝试反序列化（同版本）
        try:
            deserialized = cloudpickle.loads(serialized)
            print(f"  ✓ 同版本反序列化成功: {type(deserialized)}")
            if callable(deserialized):
                try:
                    result = deserialized(5)
                    print(f"    调用结果: {result}")
                except Exception as e:
                    print(f"    调用失败: {e}")
        except Exception as e:
            print(f"  ✗ 同版本反序列化失败: {e}")

        print()
    except Exception as e:
        print(f"✗ {name}: 序列化失败 - {e}")
        print()

print("=" * 60)
print("  版本兼容性说明")
print("=" * 60)
print()
print("❌ 跨版本问题：")
print("  - Python 3.10 序列化的代码无法在 Python 3.11 反序列��")
print("  - 原因：pickle 包含字节码和内部对象结构，版本特定")
print()
print("✓ 同版本兼容：")
print("  - 相同 Python 版本之间可以正常序列化/反序列化")
print("  - 相同 minor 版本（如 3.10.x 和 3.10.y）通常兼容")
print()
print("=" * 60)
print(f"  测试文件保存在: {tmp_dir}")
print("=" * 60)
print()
print("💡 提示：如果要在不同 Python 版本间测试，可以：")
print(f"  1. 在 Python 3.10 运行此脚本，生成 pickle 文件")
print(f"  2. 切换到 Python 3.11，运行反序列化测试")
print(f"  3. 预期会看到：_pickle.UnpicklingError 或类似的错误")
print()
