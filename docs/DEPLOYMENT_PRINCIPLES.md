# 三种部署方式的底层实现原理详解

## 🎯 核心发现

**你的理解完全正确！** 三种��署方式的底层实现**最终都是一样的**：
- **方式 1（文件）**: 读取文件 → tar.gz → 上传
- **方式 2（函数）**: 分析依赖 → 打包文件 → tar.gz → 上传
- **方式 3（模块）**: 分析依赖 → 打包文件 → tar.gz → 上传

**关键区别**：方式 2 和 3 只是**自动帮你做了"分析依赖 + 打包文件"这两个步骤**。

---

## 📐 完整流程对比

### 方式 1: 文件部署（手动）

```python
submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="./my_module.py",
    runtime="py3.11",
)
```

**底层流程：**

```
┌─────────────────────────────────────────┐
│  1. 用户指定 artifact_path              │
│     artifact_path = "./my_module.py"    │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  2. _prepare_code_blob()               │
│     - 检查 artifact_path               │
│     - 如果是文件：直接读取             │
│     - 如果是目录：打包成 tar.gz         │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  3. 获取 blob                          │
│     blob = my_module.py 的字节内容      │
│     或                                 │
│     blob = 打包后的 tar.gz 字节内容     │
└──────────────┬──────────────────────────┘
               │
               ▼
┌──────────────���──────────────────────────┐
│  4. 上传到 NodeControl                 │
│     client.upload_code_from_bytes(     │
│         blob=blob,                     │
│         filename="my_module.py",       │
│         ...                             │
│     )                                  │
└─────────────────────────────────────────┘
```

**关键点：**
- ✅ 用户需要**手动准备**文件
- ✅ 用户需要**手动包含**所有依赖文件
- ❌ **不会自动收集本地源码依赖**
- ❌ **不会自动打包本地模块**

---

### 方式 2: 函数部署（本地源码自动打包）

```python
def square(x):
    return x ** 2

submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    func=square,
    runtime="py3.11",
)
```

**底层流程：**

```
┌─────────────────────────────────────────┐
│  1. 用户指定 func                       │
│     func = square (函数对象)            │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  2. _auto_package_function()           │
│     - 创建 DependencyPackager          │
│     - 调用 packager.package_function() │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  3. DependencyAnalyzer.analyze_function()│
│     - 获取函数所在模块的源码           │
│     - AST 解析提取 import 语句         │
│     - 分类：标准库/第三方/本地         │
│     - 查找本地模块文件路径             │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  4. DependencyPackager.package_function()│
│     - 收集文件：                       │
│       ① 函数所在文件                   │
│       ② 本地依赖文件                   │
│       ③ __init__.py 等相关文件         │
│     - 创建 tar.gz 包                   │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  5. 读取 tar.gz 字节内容               │
│     blob = tar.gz 的字节内容           │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  6. 上传到 NodeControl                 │
│     client.upload_code_from_bytes(     │
│         blob=blob,                     │
│         filename="...tar.gz",         │
│         ...                             │
│     )                                  │
└─────────────────────────────────────────┘
```

**关键步骤详解：**

#### 步骤 3: 依赖分析

```python
# analyze_function(square) 返回：
{
    "function_name": "square",
    "module": "__main__",
    "source_file": "/path/to/script.py",
    "imports": [
        {"type": "import", "module": "os"},
        {"type": "import", "module": "sys"},
        # ... 所有 import
    ],
    "stdlib_modules": ["os", "sys"],  # → 不打包
    "third_party_modules": [],         # → 不打包
    "local_modules": [],               # → 需要打包
}
```

#### 步骤 4: 文件收集

```python
files_to_package = [
    "/path/to/script.py",  # 函数所在文件
]

# 如果有本地依赖
# files_to_package.extend([
#     "/path/to/utils.py",
#     "/path/to/__init__.py",
# ])
```

#### 步骤 5: 创建 tar.gz

```python
tar.gz 内容：
├── script.py         # 包含 square 函数的文件
└── (依赖文件)
```

**关键点：**
- ✅ **自动收集本地源码依赖**（通过 import 分析）
- ✅ **自动打包文件**（收集相关文件）
- ✅ **自动创建 tar.gz**
- ✅ 用户只需传函数对象

---

### 方式 3: 模块部署（本地源码自动打包）

```python
import my_module

submitter = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    module=my_module,
    runtime="py3.11",
)
```

**底层流程：**

```
┌─────────────────────────────────────────┐
│  1. 用户指定 module                     │
│     module = my_module (模块对象)       │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  2. _prepare_code_blob()               │
│     - 创建 DependencyPackager          │
│     - 调用 packager.package_module()   │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  3. DependencyAnalyzer.analyze_module() │
│     - 获取模块文件路径                 │
│     - 读取模块源码                     │
│     - AST 解析提取 import 语句         │
│     - 分类：标准库/第三方/本地         │
│     - 查找本地模块文件路径             │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  4. DependencyPackager.package_module() │
│     - 收集文件：                       │
│       ① 模块主文件                     │
│       ② 本地依赖文件                   │
│       ③ __init__.py 等相关文件         │
│     - 创建 tar.gz 包                   │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  5. 读取 tar.gz 字节内容               │
│     blob = tar.gz 的字节内容           │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  6. 上传到 NodeControl                 │
│     client.upload_code_from_bytes(     │
│         blob=blob,                     │
│         filename="my_module.tar.gz",  │
│         ...                             │
│     )                                  │
└─────────────────────────────────────────┘
```

**关键步骤详解：**

#### 步骤 3: 模块依赖分析

```python
# analyze_module("my_module") 返回：
{
    "module_name": "my_module",
    "file": "/path/to/my_module.py",
    "imports": [
        {"type": "import", "module": "pandas"},
        {"type": "from...import", "module": "my_utils"},
        # ... 所有 import
    ],
    "stdlib_modules": [],
    "third_party_modules": ["pandas"],  # → 不打包
    "local_modules": [                    # → 需要打包
        {"name": "my_utils", "file": "/path/to/my_utils.py"}
    ],
}
```

#### 步骤 4: 文件收集

```python
files_to_package = [
    "/path/to/my_module.py",  # 模块主文件
    "/path/to/my_utils.py",   # 本地依赖
    "/path/to/__init__.py",   # 相关文件
]
```

#### 步骤 5: 创建 tar.gz

```python
tar.gz 内容：
├── my_module.py     # 模块主文件
├── my_utils.py      # 依赖文件
└── __init__.py      # 包初始化文件
```

**关键点：**
- ✅ **自动打包本地模块 / package 树**
- ✅ **自动打包文件**（收集所有相关文件）
- ✅ **自动创建 tar.gz**
- ✅ 用户只需传模块对象

---

## 🔍 三种方式的本质区别

### 本质相同点

```
┌─────────────────────────────────────────┐
│         最终都是上传一个 tar.gz         │
│                                         │
│  上传内容：                             │
│  - tar.gz 包含 Python 源代码文件       │
│  - 服务端解压后动态导入                │
│  - 调用指定的函数                      │
└─────────────────────────────────────────┘
```

### 本质不同点

| 方面 | 方式 1（文件） | 方式 2（函数） | 方式 3（模块） |
|------|--------------|---------------|---------------|
| **用户输入** | 文件路径 | 函数对象 | 模块对象 |
| **依赖检测** | ❌ 手动 | ✅ 自动 | ✅ 自动 |
| **文件打包** | ❌ 手动 | ✅ 自动 | ✅ 自动 |
| **打包粒度** | 手动控制 | 函数所在文件 | 整个模块 |
| **tar.gz 创建** | 用户创建 | 自动创建 | 自动创建 |

---

## 💡 代码实现细节

### 关键函数调用链

#### 方式 2（函数）的调用链：

```python
_prepare_code_blob(func=square)
  └── _auto_package_function(square)
        └── DependencyPackager.package_function(square)
              ├── DependencyAnalyzer.analyze_function(square)
              │     ├── _get_module_source(square)
              │     ├── _extract_imports_from_source(source)
              │     ├── _find_module_file(module_name)
              │     └── _is_local_module(module_file)
              ├── 收集文件
              └── _create_tar_package(files, "square.tar.gz")
```

#### 方式 3（模块）的调用链：

```python
_prepare_code_blob(module=my_module)
  └── DependencyPackager.package_module("my_module")
        ├── DependencyAnalyzer.analyze_module("my_module")
        │     ├── importlib.import_module("my_module")
        │     ├── 读取模块文件
        │     ├── _extract_imports_from_source(source)
        │     ├── _find_module_file(module_name)
        │     └── _is_local_module(module_file)
        ├── 收集文件
        └── _create_tar_package(files, "my_module.tar.gz")
```

**对比：**
- 方式 2: `analyze_function()` - 分析函数所在模块
- 方式 3: `analyze_module()` - 分析指定模块
- **其他步骤完全相同！**

---

## 🎯 关键发现

### 1. 最终产物相同

```python
# 三种方式最终都是：
blob = tar.gz 文件的字节内容

# 上传到服务端
client.upload_code_from_bytes(
    blob=blob,           # ← 都是 tar.gz
    filename="...",
    package_format="tar.gz",  # ← 都是 tar.gz
    ...
)
```

### 2. 服务端处理相同

```python
# 服务端（NodeControl）接收后：

# 1. 保存 tar.gz
artifact_path = f"{artifact_dir}/{sha256}.tar.gz"

# 2. 解压到临时目录
extract_archive(artifact_path, out_dir=tmp_dir)

# 3. 动态导入模块
import importlib.util
spec = importlib.util.spec_from_file_location(
    "_pycloud_user_xxx",
    f"{tmp_dir}/my_module.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

# 4. 调用函数
result = module.entry_callable(*args, **kwargs)
```

**无论哪种方式，服务端处理完全一样！**

### 3. 区别仅在客户端

```
┌─────────────────────────────────────────┐
│          客户端（预处理阶段）            │
│                                         │
│  方式 1: 用户手动准备 tar.gz            │
│  方式 2: 自动分析 → 打包 → tar.gz        │
│  方式 3: 自动分析 → 打包 → tar.gz        │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│          服务端（运行阶段）              │
│                                         │
│  三种方式完全一样：                     │
│  - 接收 tar.gz                          │
│  - 解压                                 │
│  - 动态导入                             │
│  - 调用函数                             │
└─────────────────────────────────────────┘
```

---

## 📊 实际例子对比

### 假设有这样的代码结构：

```
project/
├── main.py           # 主脚本
├── my_module.py      # 业务模块
└── utils/
    ├── __init__.py
    └── helpers.py     # 辅助函数
```

**my_module.py:**
```python
import pandas as pd
from utils.helpers import clean_data

def process(data):
    df = pd.DataFrame(data)
    df = clean_data(df)
    return df.to_dict()
```

### 方式 1（文件）：

```python
# 用户需要：
# 1. 手动打包这些文件：
#    - my_module.py
#    - utils/__init__.py
#    - utils/helpers.py
# 2. 创建 tar.gz
# 3. 上传

submitter = TaskSubmitter.from_infocenter(
    artifact_path="./my_module.tar.gz",  # 手动创建
    runtime="py3.11",
)
```

**问题：**
- ❌ 需要知道 `utils/helpers.py` 是依赖
- ❌ 需要手动打包所有文件
- ❌ 容易遗漏依赖

### 方式 2（函数）：

```python
from my_module import process

submitter = TaskSubmitter.from_infocenter(
    func=process,  # ← 传函数
    runtime="py3.11",
)

# 自动完成：
# 1. 分析 process 函数
# 2. 发现依赖：utils.helpers
# 3. 打包：
#    - my_module.py
#    - utils/__init__.py
#    - utils/helpers.py
# 4. 创建 tar.gz
# 5. 上传
```

**优点：**
- ✅ 自动收集本地源码依赖
- ✅ 自动打包文件
- ❌ 只能调用 `process` 函数

### 方式 3（模块）：

```python
import my_module

submitter = TaskSubmitter.from_infocenter(
    module=my_module,  # ← 传模块
    runtime="py3.11",
)

# 自动完成：
# 1. 分析 my_module 模块
# 2. 发现依赖：utils.helpers
# 3. 打包：
#    - my_module.py
#    - utils/__init__.py
#    - utils/helpers.py
# 4. 创建 tar.gz
# 5. 上传

# 可以调用模块中的任何函数：
result1 = submitter.process(...)
result2 = submitter.another_function(...)
```

**优点：**
- ✅ 自动收集本地源码依赖
- ✅ 自动打包文件
- ✅ 可以调用多个函数

---

## 🎓 总结

### 你的理解完全正确！

**三种方式的底层实现最终都是：**
1. **创建 tar.gz 包**（包含 Python 源代码文件）
2. **上传到服务端**
3. **服务端解压并动态导入**

**唯一的区别在于"谁来做"：**
- 方式 1: **用户手动**准备 tar.gz
- 方式 2: **系统自动**分析函数 → 打包 → tar.gz
- 方式 3: **系统自动**分析模块 → 打包 → tar.gz

### 方式 2 和 3 的核心价值

**自动化依赖检测和打包：**
- ✅ 不需要手动分析依赖
- ✅ 不需要手动打包文件
- ✅ 对常见本地源码依赖更省心，但不是完美静态分析
- ✅ 开发体验更好

**底层实现：**
- 使用 AST 解析源码
- 提取 import 语句
- 区分标准库/第三方/本地模块
- 只打包本地模块
- 最终创建 tar.gz 上传

这就是为什么方式 2 和 3 "看起来更简单"，但底层其实和方式 1 完全一样！🎯
