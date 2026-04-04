# 模块对象部署功能

## 🎯 新功能

现在支持直接传递**模块对象**，自动打包整个本地模块 / package 树。
第三方依赖如果远端缺失，仍建议显式传 `dependency_allowlist`。

---

## 📝 使用方式

### 1. 从模块部署（新功能）

```python
import my_module
from pycloud_parallel import TaskSubmitter

# 直接传模块对象
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    module=my_module,  # ← 传模块对象！
    runtime="py3.11",
)

# 可以调用模块中的任何导出函数
result1 = submitter.square(x=5)
result2 = submitter.cube(x=3)
result3 = submitter.process_data(data=[1, 2, 3])
```

### 2. 完整示例

假设有模块 `my_processor.py`：

```python
"""数据处理模块"""

import json
import math


def process_data(data):
    """处理数据"""
    result = {
        "sum": sum(data),
        "mean": sum(data) / len(data),
        "count": len(data),
    }
    return json.dumps(result)


def square(x):
    """计算平方"""
    return x ** 2


def cube(x):
    """计算立方"""
    return x ** 3


class Processor:
    """处理器类"""

    def __init__(self, factor=1):
        self.factor = factor

    def process(self, data):
        return data * self.factor
```

部署和使用：

```python
import my_processor
from pycloud_parallel import TaskSubmitter

# 部署整个模块
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    module=my_processor,
    runtime="py3.11",
)

# 调用不同的函数
result1 = submitter.process_data(data=[1, 2, 3, 4, 5])
print(result1)  # {"sum": 15, "mean": 3.0, "count": 5}

result2 = submitter.square(x=5)
print(result2)  # {"value": 25}

result3 = submitter.cube(x=3)
print(result3)  # {"value": 27}
```

### 3. 共享静态数据文件（当前可行）

如果有一份**共享的大数据文件**需要随模块一起部署，当前推荐做法是：

1. 把数据文件放进模块 / package 目录树内部
2. 通过 `module=...` 方式部署整个模块
3. 在代码里通过 `__file__` 的相对路径访问数据文件

推荐目录结构：

```text
my_job/
  __init__.py
  main.py
  resources/
    lookup.parquet
```

`main.py`：

```python
from pathlib import Path

DATA_PATH = Path(__file__).resolve().parent / "resources" / "lookup.parquet"


def run(key: str):
    # 这里继续按本地相对路径习惯读取
    data_bytes = DATA_PATH.read_bytes()
    return {"key": key, "size": len(data_bytes)}
```

部署：

```python
import my_job.main
from pycloud_parallel import DeployedService

group = DeployedService.deploy_from_module(
    infocenter_target="127.0.0.1:50051",
    module=my_job.main,
    runtime="py3.11",
)
```

这个模式当前可以走通，因为 `module` 自动打包会把模块 / package 树里的**资源文件一起带上**，不只打包 `.py` 源码。相关回归测试见 [test_dependency_packager.py](../tests/test_dependency_packager.py#L33)。

**边界：**

- 适合“共享、静态、相对稳定”的数据文件
- 数据文件必须放在模块 / package 树内
- 推荐使用 `Path(__file__).resolve().parent / ...` 这种相对路径写法
- 如果你传的是**单个 `.py` 文件模块**，不会自动把旁边的兄弟数据文件带上
- 如果数据已经大到“每次部署都重新上传很慢”，后续应考虑独立数据引用 / 对象存储方案

---

## 🔄 三种部署方式对比

### 方式 1: 从文件部署（传统）

```python
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="./my_module.py",
    runtime="py3.11",
)
```

**特点：**
- ✅ 需要文件存在
- ✅ 适合已存在的代码文件
- ❌ 需要手动管理文件

### 方式 2: 从函数部署（新功能）

```python
def square(x):
    return x ** 2

submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    func=square,
    runtime="py3.11",
)
```

**特点：**
- ✅ 直接传函数对象
- ✅ 自动打包依赖
- ✅ 适合单个函数
- ❌ 只能调用一个函数

### 方式 3: 从模块部署（最新）

```python
import my_module

submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    module=my_module,
    runtime="py3.11",
)
```

**特点：**
- ✅ 直接传模块对象
- ✅ 自动打包整个本地模块 / package
- ✅ 可以调用多个函数
- ✅ 自动带上本地源码依赖和资源文件
- ✅ 适合模块化代码组织

---

## 💡 优先级

`from_infocenter` 方法的参数优先级：

```
module > func > blob > artifact_path
```

```python
# 优先级示例
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    module=module,      # 优先级 1（最高）
    func=func,          # 优先级 2
    blob=blob,          # 优先级 3
    artifact_path=path, # 优先级 4（最低）
    runtime="py3.11",
)
```

**注意：** 只会使用优先级最高的参数，其他参数会被忽略。

---

## 🎓 使用场景

### 场景 1: 模块包含多个相关函数

```python
# math_operations.py
def add(a, b):
    return a + b

def subtract(a, b):
    return a - b

def multiply(a, b):
    return a * b

def divide(a, b):
    return a / b
```

```python
import math_operations

submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    module=math_operations,
    runtime="py3.11",
)

# 可以调用任何函数
result1 = submitter.add(a=10, b=5)
result2 = submitter.multiply(a=3, b=7)
```

### 场景 2: 需要共享代码和依赖

```python
# data_processor.py
import pandas as pd
import numpy as np

# 共享的辅助函数
def _clean_data(df):
    return df.dropna()

def _normalize(df):
    return (df - df.mean()) / df.std()

# 导出的处理函数
def process_csv(data):
    df = pd.DataFrame(data)
    df = _clean_data(df)
    return df.to_dict()

def process_json(data):
    df = pd.DataFrame(data)
    df = _clean_data(df)
    df = _normalize(df)
    return df.to_dict()
```

```python
import data_processor

submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    module=data_processor,
    runtime="py3.11",
)

# 两个函数共享依赖和辅助函数
result1 = submitter.process_csv(data=[...])
result2 = submitter.process_json(data=[...])
```

### 场景 3: 模块级别的代码组织

```python
# ml_pipeline.py
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import joblib

class MLPipeline:
    def __init__(self):
        self.scaler = StandardScaler()
        self.model = RandomForestClassifier()

    def train(self, X, y):
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled, y)
        return {"status": "trained"}

    def predict(self, X):
        X_scaled = self.scaler.transform(X)
        return self.model.predict(X_scaled)

def load_pipeline(path):
    return joblib.load(path)

def save_pipeline(pipeline, path):
    joblib.dump(pipeline, path)
```

```python
import ml_pipeline

submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    module=ml_pipeline,
    runtime="py3.11",
)

# 可以使用类和函数
result = submitter.MLPipeline.train(X=..., y=...)
```

---

## 🔧 自动处理的内容

### 1. 模块打包

```python
# 自动打包：
# - my_module.py（主文件）
# - 依赖的其他本地模块
# - __init__.py 文件
# - 相关的资源文件
```

### 2. 依赖检测

```python
# my_module.py
import os              # 标准库 → 不打包
import pandas as pd    # 第三方库 → 不打包
from my_utils import helper  # 本地模块 → 打包
```

### 3. 自动推断

```python
import my_module

submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    module=my_module,
    runtime="py3.11",
)

# 自动推断：
# - entry_module = "my_module"
# - entry_callable = "run"（默认）
# - package_format = "tar.gz"
```

---

## 📊 对比总结

| 特性 | 文件部署 | 函数部署 | 模块部署 |
|------|---------|---------|---------|
| **参数类型** | 文件路径 | 函数对象 | 模块对象 |
| **代码管理** | 需要文件 | 无需文件 | 无需文件 |
| **函数数量** | 多个 | 单个 | 多个 |
| **依赖检测** | ❌ | ✅ | ✅ |
| **共享代码** | ✅ | ❌ | ✅ |
| **适合场景** | 已有代码 | 单个函数 | 模块化代码 |

---

## ✅ 优势

1. **极简 API**
   ```python
   # 一行代码部署整个模块
   submitter = TaskSubmitter.from_infocenter(module=MyModule, ...)
   ```

2. **多函数支持**
   ```python
   # 可以调用模块中的任何函数
   submitter.function1(...)
   submitter.function2(...)
   submitter.function3(...)
   ```

3. **代码共享**
   ```python
   # 模块内的辅助函数自动共享
   def _helper(x):
       return x * 2

   def func1(x):
       return _helper(x)

   def func2(x):
       return _helper(x) + 1
   ```

4. **依赖管理**
   ```python
   # 模块级别的导入自动处理
   import pandas as pd
   import numpy as np
   # 这些依赖会被自动检测
   ```

---

## 🎓 总结

模块对象部署提供了最灵活的代码组织方式：

**最适合：**
- ✅ 包含多个相关函数的模块
- ✅ 需要共享代码和依赖
- ✅ 模块化的代码组织
- ✅ 复杂的业务逻辑

**使用体验：**
```python
# 之前：需要分别部署每个函数
submitter1 = TaskSubmitter.from_infocenter(func=func1, ...)
submitter2 = TaskSubmitter.from_infocenter(func=func2, ...)

# 现在：一次性部署整个模块
submitter = TaskSubmitter.from_infocenter(module=MyModule, ...)
result1 = submitter.func1(...)
result2 = submitter.func2(...)
```

这就是 PyCloud 的模块级部署体验！
