# 本地源码自动打包部署功能

## 🎯 新功能

**DeployedService** 和 **TaskSubmitter** 现在支持直接传递函数对象，
自动打包本地源码依赖并部署。

---

## 📝 使用方式

### 1. DeployedService（服务模式）

#### 传统方式（仍然支持）

```python
from pycloud_parallel import DeployedService

# 方式 1: 从文件部署
group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="./my_service.py",
    runtime="py3.11",
)

# 方式 2: 从 blob 部署
blob = b"""
def process_data(x):
    return x * 2
"""
group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    runtime="py3.11",
    entry_module="my_service",
)
```

#### 新方式（推荐）

```python
from pycloud_parallel import DeployedService

# 定义服务函数
def process_data(x):
    """数据处理服务"""
    import numpy as np
    return np.sum(x)

# 直接传函数对象，自动打包本地源码依赖
group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    func=process_data,  # ← 直接传函数！
    runtime="py3.11",
    worker_count=2,
    tags=["compute"],
)

# 调用服务
result = group.process_data.sync(x=[1, 2, 3, 4, 5])
print(result)  # {'result': 15}
```

### 2. TaskSubmitter（任务模式）

#### 传统方式（仍然支持）

```python
from pycloud_parallel import TaskSubmitter

# 方式 1: 从文件部署
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="./my_task.py",
    runtime="py3.11",
)

# 方式 2: 从 blob 部署
blob = b"""
def square(x):
    return x ** 2
"""
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    runtime="py3.11",
    entry_module="my_task",
)
```

#### 新方式（推荐）

```python
from pycloud_parallel import TaskSubmitter

# 定义任务函数
def square(x):
    """计算平方"""
    return x ** 2

# 直接传函数对象，自动打包本地源码依赖
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    func=square,  # ← 直接传函数！
    runtime="py3.11",
    tags=["compute"],
)

# 提交任务
result = submitter.square(x=5)
print(result)  # {'value': 25, 'square': 625}

# 批量提交
results = submitter.square.submit(x=[1, 2, 3, 4, 5])
completed = submitter.wait_for_results(expected_count=5)
```

---

## 🔄 自动处理的内容

### 1. 本地源码依赖分析

```python
def complex_function(data):
    # 这些 import 会被自动检测
    import os
    import json
    import numpy as np
    from my_utils import helper

    return helper(data, np.mean)
```

**当前结果：**
- ✅ 标准库：`os`, `json` → 不打包
- ✅ 第三方库：`numpy` → 不自动打包；远端缺失时建议显式传 `dependency_allowlist`
- ✅ 本地模块：`my_utils` → **自动打包**

### 2. 自动打包

```python
# 自动创建 tar.gz 包，包含：
# - 函数所在模块 / package
# - 本地源码依赖
# - __init__.py
# - package 内资源文件
```

### 3. 自动推断

```python
def my_process(x):
    return x * 2

# 自动推断：
# - entry_module = 基于源码文件推断的模块路径
# - entry_callable = "my_process" (函数名)
```

---

## 💡 最佳实践

### 1. 函数定义

```python
# ✅ 推荐：模块级函数
def process_data(df):
    import pandas as pd
    return df.groupby("key").sum()

# ⚠️ 可以：嵌套函数（但可能无法捕获闭包）
def make_processor(factor):
    def process(x):
        return x * factor
    return process

# ❌ 不推荐：lambda（无法获取源码）
func = lambda x: x * 2
```

### 2. 依赖管理

```python
# ✅ 推荐：明确的 import
def process(data):
    import numpy as np
    return np.sum(data)

# ⚠️ 可以：使用全局导入
import numpy as np

def process(data):
    return np.sum(data)
```

### 3. 本地依赖

```python
# 假设项目结构：
# my_project/
# ├── main.py
# └── utils/
#     ├── __init__.py
#     └── helpers.py

# main.py
from utils import helper_function

def process(data):
    return helper_function(data)

# 部署时会自动打包：
# - main.py
# - utils/__init__.py
# - utils/helpers.py
```

---

## 🚀 完整示例

### 示例 1: 数据处理服务

```python
from pycloud_parallel import DeployedService

def data_processor(df):
    """数据处理服务"""
    import pandas as pd
    import numpy as np

    # 数据清洗
    df = df.dropna()

    # 数据聚合
    result = df.groupby("category").agg({
        "value": ["sum", "mean", "count"]
    })

    return result.to_dict()

# 部署服务
group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    func=data_processor,
    runtime="py3.11",
    worker_count=4,
    tags=["compute"],
)

# 调用服务
import pandas as pd
df = pd.DataFrame({
    "category": ["A", "B", "A", "B"],
    "value": [1, 2, 3, 4],
})

result = group.data_processor.sync(df)
print(result)
```

### 示例 2: 批量任务处理

```python
from pycloud_parallel import TaskSubmitter

def process_item(item):
    """处理单个项目"""
    import time
    import hashlib

    time.sleep(0.1)  # 模拟处理
    return {
        "item": item,
        "hash": hashlib.md5(str(item).encode()).hexdigest(),
    }

# 创建任务客户端
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    func=process_item,
    runtime="py3.11",
    tags=["compute"],
)

# 批量提交任务
items = list(range(100))
results = submitter.process_item.submit(item=items)

# 等待所有任务完成
completed = submitter.wait_for_results(expected_count=100)

print(f"完成 {len(completed)} 个任务")
for result in completed[:5]:  # 打印前5个结果
    print(f"  {result.result}")
```

---

## 📊 对比总结

| 特性 | 传统方式 | 新方式 |
|------|---------|--------|
| **操作步骤** | 3-5 步 | 1 步 |
| **代码打包** | 手动 | 自动 |
| **依赖检测** | 手动 | 自动 |
| **文件管理** | 需要维护 .py 文件 | 无需额外文件 |
| **调试友好** | ✅ | ✅ |
| **版本兼容** | ✅ | ✅ |

---

## 🔧 实现细节

### 1. 依赖检测算法

```python
# 1. 读取函数所在模块的源码
source = inspect.getsource(module)

# 2. AST 解析提取 import
tree = ast.parse(source)
imports = []
for node in ast.walk(tree):
    if isinstance(node, ast.Import):
        imports.append(alias.name)
    elif isinstance(node, ast.ImportFrom):
        imports.append(node.module)

# 3. 分类：标准库 / 第三方 / 本地
stdlib_modules = set(sys.stdlib_module_names)
for imp in imports:
    if imp in stdlib_modules:
        # 标准库，不打包
    elif is_local_module(imp):
        # 本地模块，打包
    else:
        # 第三方库，不打包
```

### 2. 打包流程

```python
# 1. 分析函数依赖
deps = analyzer.analyze_function(func)

# 2. 收集文件
files = [func.__code__.co_filename]
for mod in deps['local_modules']:
    files.append(mod['file'])
    files.extend(find_related_files(mod['file']))

# 3. 创建 tar.gz
with tarfile.open(output_path, "w:gz") as tar:
    for file in files:
        tar.add(file, arcname=relative_path)
```

### 3. 参数处理

```python
# 优先级：func > blob > artifact_path
if func is not None:
    # 自动打包函数
    blob, filename = _prepare_code_blob(func=func)
elif blob is not None:
    # 使用提供的 blob
    pass
elif artifact_path:
    # 从文件读取
    pass
```

---

## ✅ 优势

1. **极简 API**：一行代码完成部署
2. **自动本地源码打包**：不需要手动整理本地模块文件
3. **跨版本兼容**：源码级别，完全兼容
4. **调试友好**：可以看到实际执行的代码
5. **向后兼容**：传统方式仍然支持

---

## 🎓 总结

新的本地源码自动打包功能让 PyCloud 的使用变得更加简单：

**之前：**
```python
# 1. 写代码到文件
# 2. 手动上传文件
# 3. 指定各种参数
```

**现在：**
```python
# 直接传函数，一步搞定！
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    func=my_function,
    runtime="py3.11",
)
```

这就是 PyCloud 的"零摩擦"部署体验！
