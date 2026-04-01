# 自动依赖检测系统设计

## 🎯 核心思想

**参考 cloudpickle 的依赖发现逻辑，自动分析函数/模块的 import 语句，智能打包所需的文件。**

### 问题
- ❌ 当前需要手动打包代码文件
- ❌ 需要知道哪些本地模块需要一起上传
- ❌ 容易遗漏依赖文件

### 解决方案
- ✅ 自动分析函数/模块的所有 import
- ✅ 区分标准库、第三方库、本地模块
- ✅ 只打包本地模块（标准库和第三方库假设目标环境已安装）
- ✅ 自动查找 `__init__.py` 等相��文件

---

## 📐 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                     用户 API                             │
│  auto_deploy_function(func, runtime="py3.11")          │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│              DependencyPackager                         │
│  1. 调用 DependencyAnalyzer 分析依赖                   │
│  2. 收集需要打包的文件                                  │
│  3. 创建 tar.gz 包                                      │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────���───────────┐
│              DependencyAnalyzer                         │
│  1. 读取模块源码                                        │
│  2. AST 解析提取 import                                │
│  3. 分类：标准库 / 第三方 / 本地                        │
│  4. 查找本地模块文件路径                                │
└─────────────────────────────────────────────────────────┘
```

---

## 🔍 依赖检测算法

### 1. 源码分析（AST）

```python
# 输入：函数所在模块的源码
source = """
import numpy as np
from pandas import DataFrame
from my_utils import helper_function
from collections import defaultdict

def process_data(data):
    result = helper_function(data)
    return DataFrame(result)
"""

# AST 解析
tree = ast.parse(source)

# 提取所有 import
imports = []
for node in ast.walk(tree):
    if isinstance(node, ast.Import):
        for alias in node.names:
            imports.append(alias.name)  # ['numpy', 'pandas']
    elif isinstance(node, ast.ImportFrom):
        imports.append(node.module)  # ['my_utils', 'collections']

# 结果
imports = [
    {'type': 'import', 'module': 'numpy'},
    {'type': 'from...import', 'module': 'pandas'},
    {'type': 'from...import', 'module': 'my_utils'},
    {'type': 'from...import', 'module': 'collections'},
]
```

### 2. 模块分类

```python
def classify_module(module_name):
    """将模块分类为标准库/第三方/本地"""
    # 1. 检查是否是标准库
    if module_name in sys.stdlib_module_names:
        return "stdlib"

    # 2. 查找模块文件
    spec = importlib.util.find_spec(module_name)
    if not spec or not spec.origin:
        return "unknown"

    module_file = Path(spec.origin)

    # 3. 检查是否在 site-packages
    if 'site-packages' in module_file.parts:
        return "third_party"

    # 4. 检查是否在标准库路径
    if any(stdlib in module_file.parents for stdlib in stdlib_paths):
        return "stdlib"

    # 5. 其他都是本地模块
    return "local"
```

### 3. 文件查找

```python
def find_local_module_files(module_name):
    """查找本地模块的所有相关文件"""
    spec = importlib.util.find_spec(module_name)
    if not spec:
        return []

    files = [spec.origin]  # 主文件

    # 查找 __init__.py
    module_dir = Path(spec.origin).parent
    init_file = module_dir / "__init__.py"
    if init_file.exists():
        files.append(str(init_file))

    # 递归查找父目录的 __init__.py
    parent = module_dir
    while parent != Path.cwd():
        parent_init = parent / "__init__.py"
        if parent_init.exists():
            files.append(str(parent_init))
            parent = parent.parent
        else:
            break

    return files
```

---

## 💻 API 设计

### 方式 1: 自动部署函数（最简单）

```python
from pycloud_parallel import auto_deploy_function

# 定义函数
def process_data(df):
    import numpy as np
    import pandas as pd
    return df.groupby("key").sum()

# 一键部署（自动检测依赖并打包）
submitter = auto_deploy_function(
    func=process_data,
    infocenter_target="127.0.0.1:50051",
    runtime="py3.11",
)

# 使用
result = submitter.submit(df)
```

### 方式 2: 显式打包

```python
from pycloud_parallel.controlplane import DependencyPackager

# 定义函数
def my_function(data):
    from my_utils import helper
    return helper(data)

# 打包
packager = DependencyPackager()
package_path = packager.package_function(
    func=my_function,
    output_file="/tmp/my_package.tar.gz",
    include_tests=False,
)

# 上传
from pycloud_parallel import TaskSubmitter
submitter = TaskSubmitter.deploy_from_blob(
    infocenter_target="127.0.0.1:50051",
    blob=open(package_path, "rb").read(),
    runtime="py3.11",
)
```

### 方式 3: 只分析依赖

```python
from pycloud_parallel.controlplane import DependencyAnalyzer

# 分析函数
analyzer = DependencyAnalyzer()
deps = analyzer.analyze_function(my_function)

print(f"标准库: {deps['stdlib_modules']}")
print(f"第三方库: {deps['third_party_modules']}")
print(f"本地模块: {deps['local_modules']}")
```

---

## 📦 打包策略

### 打包内容

```
my_package.tar.gz
├── my_module.py              # 主模块
├── __init__.py               # 包初始化文件
├── utils/
│   ├── __init__.py
│   └── helpers.py            # 本地依赖
└── config/
    ├── __init__.py
    └── settings.py           # 本地依赖
```

### 打包规则

| 类型 | 是否打包 | 原因 |
|------|---------|------|
| 标准库 (os, sys, json) | ❌ | 目标环境已有 |
| 第三方库 (numpy, pandas) | ❌ | 假设目标环境已安装 |
| 本地模块 (my_utils) | ✅ | 必须打包 |
| `__init__.py` | ✅ | Python 包结构需要 |
| 测试文件 (test_*.py) | 可选 | 默认不打包 |

---

## 🔄 工作流程

### 完整流程

```python
# 1. 用户定义函数
def process_data(df):
    from my_utils import clean_data
    import numpy as np
    return clean_data(df, np.mean)

# 2. 自动分析依赖
analyzer = DependencyAnalyzer()
deps = analyzer.analyze_function(process_data)
# 结果：
# - 标准库: []
# - 第三方库: ['numpy']
# - 本地模块: [{'name': 'my_utils', 'file': '/path/to/my_utils.py'}]

# 3. 收集文件
files = [
    '/path/to/my_module.py',      # 函数所在文件
    '/path/to/my_utils.py',       # 本地依赖
    '/path/to/__init__.py',       # 包初始化文件
]

# 4. 创建 tar.gz
packager = DependencyPackager()
package_path = packager.package_function(process_data)

# 5. 计算哈希
sha256 = hashlib.sha256(open(package_path, 'rb').read()).hexdigest()

# 6. 上传到 PyCloud
submitter = TaskSubmitter.deploy_from_blob(
    infocenter_target="127.0.0.1:50051",
    blob=open(package_path, 'rb').read(),
    runtime="py3.11",
    entry_module="my_module",
    entry_callable="process_data",
)

# 7. 提交任务
result = submitter.submit(df)
```

---

## ⚙️ 实现细节

### DependencyAnalyzer

```python
class DependencyAnalyzer:
    """依赖分析器"""

    def analyze_function(self, func: Callable) -> Dict[str, Any]:
        """分析函数的所有依赖"""
        # 1. 获取函数所在模块的源码
        source = self._get_module_source(func)

        # 2. AST 解析提取 import
        imports = self._extract_imports_from_source(source)

        # 3. 分类每个导入
        for imp in imports:
            module_name = imp["module"]

            if self._is_stdlib(module_name):
                result["stdlib_modules"].append(module_name)
            elif self._is_local_module(module_name):
                module_file = self._find_module_file(module_name)
                result["local_modules"].append({
                    "name": module_name,
                    "file": module_file,
                })
            else:
                result["third_party_modules"].append(module_name)

        return result
```

### DependencyPackager

```python
class DependencyPackager:
    """依赖打包器"""

    def package_function(self, func: Callable, **kwargs) -> str:
        """打包函数及其所有依赖"""
        # 1. 分析依赖
        deps = self.analyzer.analyze_function(func)

        # 2. 收集文件
        files = [deps["source_file"]]
        for mod in deps["local_modules"]:
            files.append(mod["file"])
            # 查找相关文件
            files.extend(self._find_related_files(mod["file"]))

        # 3. 创建 tar.gz
        tar_path = self._create_tar_package(files, **kwargs)

        return tar_path
```

---

## 🎨 使用示例

### 示例 1: 简单函数

```python
def square(x):
    return x ** 2

submitter = auto_deploy_function(
    func=square,
    infocenter_target="127.0.0.1:50051",
    runtime="py3.11",
)
```

**打包内容:**
- ✅ 当前脚本文件
- ❌ 无其他依赖

### 示例 2: 带本地依赖

```python
# my_module.py
from my_utils import helper_function

def process(data):
    return helper_function(data)

# my_utils.py
def helper_function(data):
    return data * 2

submitter = auto_deploy_function(func=process, ...)
```

**打包内容:**
- ✅ `my_module.py`
- ✅ `my_utils.py`
- ✅ `__init__.py` (如果存在)

### 示例 3: 复杂依赖

```python
def complex_processor(df):
    import numpy as np
    import pandas as pd
    from my_local_pkg import clean, transform
    from collections import defaultdict

    result = clean(df)
    result = transform(result)
    return result
```

**依赖分析:**
- 标准库: `collections` → ❌ 不打包
- 第三方库: `numpy`, `pandas` → ❌ 不打包
- 本地模块: `my_local_pkg` → ✅ 打包

**打包内容:**
- ✅ 当前文件
- ✅ `my_local_pkg/` (整个包)

---

## 🚀 优势

### vs 手动打包

| 特性 | 手动打包 | 自动依赖检测 |
|------|---------|-------------|
| 操作步骤 | 3-5 步 | 1 步 |
| 依赖遗漏 | 可能 | 不会 |
| 维护成本 | 高 | 低 |
| 用户体验 | 差 | 优秀 |

### vs Cloudpickle

| 特性 | Cloudpickle | 自动依赖检测 |
|------|-------------|-------------|
| 跨版本兼容 | ❌ | ✅ |
| 闭包支持 | ✅ | ⚠️ |
| 可调试性 | ❌ | ✅ |
| 依赖透明 | ❌ | ✅ |

---

## 🔮 未来扩展

### 1. 智能依赖推断

```python
# 分析函数调用，推断隐式依赖
def process_data(df):
    return df.groupby("key").sum()  # 隐式依赖 pandas

# 自动检测：需要 pandas
```

### 2. 依赖版本检测

```python
# 检测本地环境依赖版本
import numpy
print(numpy.__version__)  # 1.24.0

# 检查目标环境版本是否匹配
# 如果不匹配，给出警告
```

### 3. 可选依赖打包

```python
# 打包第三方库（如果目标环境可能没有）
submitter = auto_deploy_function(
    func=process_data,
    include_third_party=["numpy", "pandas"],  # 强制打包
)
```

### 4. 依赖缓存优化

```python
# 相同依赖的函数共享缓存
func1 = lambda x: x * 2
func2 = lambda x: x ** 2

# 都没有本地依赖，使用同一个基础镜像
```

---

## 📝 总结

### ✅ 已实现

- [x] AST 解析提取 import
- [x] 模块分类（标准库/第三方/本地）
- [x] 本地模块文件查找
- [x] 自动打包 tar.gz
- [x] `__init__.py` 等相关文件处理

### 🔄 进行中

- [ ] 集成到 `TaskSubmitter`
- [ ] 添加 `auto_deploy_function()` API
- [ ] 优化依赖查找算法

### 🎯 待规划

- [ ] 闭包支持（结合 cloudpickle）
- [ ] 依赖版本检测
- [ ] 智能依赖推断
- [ ] 依赖缓存优化

---

## 📚 参考资料

- [cloudpickle 源码](https://github.com/cloudpipe/cloudpickle)
- [Python AST 文档](https://docs.python.org/3/library/ast.html)
- [importlib 文档](https://docs.python.org/3/library/importlib.html)
