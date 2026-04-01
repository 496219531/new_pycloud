# 代码分发方案完整对比

## 📊 三种方案总览

| 方案 | 手动打包 | Cloudpickle | 自动依赖检测 |
|------|---------|-------------|-------------|
| **实现状态** | ✅ 已实现 | ❌ 未实现 | ✅ 已实现 |
| **API 简洁性** | ⚠️ 需要打包步骤 | ✅ 一行代码 | ✅ 一行代码 |
| **跨版本兼容** | ✅ 完全兼容 | ❌ 不兼容 | ✅ 完全兼容 |
| **闭包支持** | ❌ 不支持 | ✅ 完全支持 | ❌ 不支持 |
| **Lambda 支持** | ❌ 不支持 | ✅ 完全支持 | ❌ 不支持 |
| **依赖检测** | ❌ 手动 | ✅ 自动 | ✅ 自动 |
| **可调试性** | ✅ 可查看源码 | ❌ 二进制格式 | ✅ 可查看源码 |
| **可缓存性** | ✅ 代码级缓存 | ⚠️ 闭包级缓存 | ✅ 代码级缓存 |

---

## 1️⃣ 手动打包（当前实现）

### 使用方式

```python
from pycloud_parallel import TaskSubmitter

# 方式 1: 从文件部署
submitter = TaskSubmitter.deploy_from_code(
    infocenter_target="127.0.0.1:50051",
    code_path="./my_module.py",
    runtime="py3.11",
)

# 方式 2: 从 blob 部署
blob = b"""
def process(x):
    return x * 2
"""
submitter = TaskSubmitter.deploy_from_blob(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    runtime="py3.11",
)
```

### 适用场景

✅ **适合：**
- 生产环境（跨版本 Python）
- 需要调试的场景
- 大型项目/模块部署
- 需要依赖管理

❌ **不适合：**
- 快速原型开发
- 交互式计算（Jupyter）
- 需要闭包的场景

---

## 2️⃣ Cloudpickle（未实现，不推荐）

### 使用方式（假设）

```python
from pycloud_parallel import TaskSubmitter

def process(x):
    return x * 2

submitter = TaskSubmitter.deploy_from_function(
    infocenter_target="127.0.0.1:50051",
    func=process,  # ← 直接传函数对象
    runtime="py3.11",
)
```

### 致命问题

❌ **跨版本不兼容**

```python
# Python 3.10 序列化
import cloudpickle
pickle_bytes = cloudpickle.dumps(my_function)

# Python 3.11 反序列化
# ❌ _pickle.UnpicklingError
# 原因：字节码格式不同
```

**版本兼容矩阵：**
| 序列化版本 | 反序列化版本 | 结果 |
|-----------|-------------|------|
| 3.10.x | 3.10.y | ✅ 兼容 |
| 3.10.x | 3.11.x | ❌ **不兼容** |
| 3.11.x | 3.10.x | ❌ **不兼容** |

### 结论

**不推荐实现 Cloudpickle 方案**，因为：
1. 跨版本不兼容是致命缺陷
2. 生产环境通常有多个 Python 版本
3. 自动依赖检测方案提供了相同的便利性

---

## 3️⃣ 自动依赖检测（推荐，已实现）

### 使用方式

```python
from pycloud_parallel import auto_deploy_function

# 定义函数
def process_data(df):
    from my_utils import helper
    import numpy as np
    return helper(df, np.mean)

# 一键部署（自动检测依赖并打包）
submitter = auto_deploy_function(
    func=process_data,
    infocenter_target="127.0.0.1:50051",
    runtime="py3.11",
)

# 使用
result = submitter.submit(df)
```

### 工作原理

```
┌─────────────────────────────────────────┐
│  1. 分析函数所在模块的源码               │
│     - 读取整个 .py 文件                 │
│     - AST 解析提取 import 语句          │
└──────────────┬──────────────────────────┘
               │
               ▼
┌──────────────��──────────────────────────┐
│  2. 分类导入的模块                      │
│     - 标准库 (os, sys, json)            │
│     - 第三方库 (numpy, pandas)          │
│     - 本地模块 (my_utils)               │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  3. 收集需要打包的文件                  │
│     - 函数所在文件                      │
│     - 本地依赖文件                      │
│     - __init__.py 等相关文件            │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  4. 创建 tar.gz 包                      │
│     - 保持目录结构                      │
│     - 排除标准库和第三方库              │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  5. 上传到 PyCloud                      │
│     - 计算 SHA256                       │
│     - 调用现有的 UploadCode 接口        │
└─────────────────────────────────────────┘
```

### 优势

✅ **结合了手动打包和 Cloudpickle 的优点：**

| 特性 | 手动打包 | 自动依赖检测 |
|------|---------|-------------|
| 操作步骤 | 3-5 步 | **1 步** |
| 跨版本兼容 | ✅ | ✅ |
| 依赖检测 | ❌ 手动 | ✅ **自动** |
| 可调试性 | ✅ | ✅ |
| 依赖遗漏 | 可能 | **不会** |

### 实际效果

```python
# 示例：带本地依赖的函数

# my_module.py
from my_utils import helper_function

def process(data):
    return helper_function(data)

# my_utils.py
def helper_function(data):
    return data * 2

# 一键部署
submitter = auto_deploy_function(func=process, ...)

# 自动打包：
# ✅ my_module.py
# ✅ my_utils.py
# ✅ __init__.py
# ❌ numpy, pandas (假设目标环境已安装)
```

---

## 🎯 推荐方案总结

### 短期（当前）

✅ **保留手动打包方案**
- 生产环境稳定可靠
- 完全的跨版本兼容
- 可调试性强

### 中期（正在进行）

✅ **推广自动依赖检测**
```python
# 统一 API，自动选择最优方案
from pycloud_parallel import auto_deploy_function

submitter = auto_deploy_function(
    func=process_data,
    infocenter_target="127.0.0.1:50051",
    runtime="py3.11",
)
```

**优势：**
- 1 行代码完成部署
- 自动检测依赖
- 跨版本兼容
- 可调试

### 长期（未来规划）

❌ **不推荐 Cloudpickle**
- 跨版本不兼容是致命缺陷
- 生产环境风险太高
- 自动依赖检测提供了相同的便利性

✅ **可能的方向：**
- 混合方案：闭包用 cloudpickle，普通函数用自动检测
- 智能选择：根据环境自动选择最合适的方案
- 依赖版本检测：确保依赖版本匹配

---

## 📈 迁移路径

### 阶段 1: 当前（已完成）

```python
# 手动打包
submitter = TaskSubmitter.deploy_from_code(
    code_path="./my_module.py",
    runtime="py3.11",
)
```

### 阶段 2: 添加自动依赖检测（进行中）

```python
# 自动检测依赖
submitter = auto_deploy_function(
    func=process_data,
    runtime="py3.11",
)
```

### 阶段 3: 智能选择（未来）

```python
# 装饰器风格
from pycloud_parallel import remote

@remote(runtime="py3.11")
def process_data(df):
    return df.groupby("key").sum()

# 自动检测环境并选择最优方案
result = process_data.remote(df)
```

---

## 🎓 经验总结

### Cloudpickle 的教训

1. **跨版本兼容性很重要**
   - 生产环境很难保证所有节点版本一致
   - 版本升级会导致序列化代码全部失效

2. **可调试性很重要**
   - 二进制格式无法查看实际执行的代码
   - 排错困难，安全审计困难

3. **依赖透明很重要**
   - Cloudpickle 不清楚实际依赖了什么
   - 隐式依赖导致运行时错误

### 自动依赖检测的优势

1. **结合了两种方案的优点**
   - Cloudpickle 的便利性
   - 手动打包的可靠性和可调试性

2. **智能的依赖管理**
   - 自动区分标准库、第三方库、本地模块
   - 只打包必要的文件

3. **用户友好**
   - 1 行代码完成部署
   - 不需要了解打包细节

---

## 📚 相关文档

- [Cloudpickle vs 文件上传对比](./CLOUDPICKLE_VS_FILE_UPLOAD.md)
- [自动依赖检测系统设计](./AUTO_DEPENDENCY_DETECTION.md)
- [实现代码](../src/pycloud_parallel/controlplane/dependency.py)
- [演示脚本](../scripts/demo_auto_dependency.py)

---

## ✅ 结论

**推荐使用自动依赖检测方案**，原因：

1. ✅ **便利性**：1 行代码完成部署
2. ✅ **兼容性**：跨版本 Python 完全兼容
3. ✅ **可靠性**：自动检测依赖，不会遗漏
4. ✅ **可调试性**：源码可见，易于调试
5. ✅ **可扩展性**：可以添加更多智能功能

**不推荐 Cloudpickle**，原因：

1. ❌ **跨版本不兼容**：致命缺陷
2. ❌ **生产环境风险**：版本升级会导致全部失效
3. ❌ **可调试性差**：二进制格式难以调试
4. ❌ **依赖不透明**：不清楚实际依赖了什么

**保留手动打包方案**，用于：

1. ✅ 需要精确控制的场景
2. ✅ 复杂的项目结构
3. ✅ 需要包含额外资源的场景
