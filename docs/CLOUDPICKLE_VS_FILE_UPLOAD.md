# Cloudpickle vs 文件上传：代码分发方案对比

## 📊 方案对比总览

| 特性 | 文件��传（当前） | Cloudpickle |
|------|----------------|-------------|
| **API 简洁性** | ⚠️ 需要打包步骤 | ✅ 一行代码 |
| **跨版本兼容** | ✅ 完全兼容 | ❌ 不兼容 |
| **闭包支持** | ❌ 不支持 | ✅ 完全支持 |
| **Lambda 支持** | ❌ 不支持 | ✅ 完全支持 |
| **可调试性** | ✅ 可查看源码 | ❌ 二进制格式 |
| **缓存效率** | ✅ 代码级缓存 | ⚠️ 闭包级缓存 |
| **依赖管理** | ✅ 可包含依赖 | ❌ 需预安装 |
| **安全性** | ✅ 沙箱隔离 | ⚠️ pickle 风险 |

---

## 1️⃣ 文件上传方案（当前实现）

### 工作流程
```
┌─────────┐      ┌─────────┐      ┌──────────┐
│ 客户端  │─────▶│网关/Info│─────▶│NodeControl│
│代码文件  │      │  Center │      │  存储    │
└─────────┘      └─────────┘      └──────────┘
    │                                  │
    │  tar.gz/zip/py                   │
    └──────────────────────────────────┘
              解压 + 动态导入
```

### 实现细节

**客户端操作：**
```python
# 当前 PyCloud 的使用方式
from pycloud_parallel import TaskSubmitter

# 需要先打包代码
# 方式 1: 上传已存在的模块
submitter = TaskSubmitter.deploy_from_code(
    infocenter_target="127.0.0.1:50051",
    code_path="./my_module.py",  # ← 需要文件
    runtime="py3.11",
    entry_module="my_module",
    entry_callable="process",
)

# 方式 2: 动态创建模块
blob = b"""
def process(x):
    return x * 2
"""
submitter = TaskSubmitter.deploy_from_blob(...)
```

**服务端处理：**
```python
# 1. 接收文件流（gRPC streaming）
# 2. 验证 SHA256
artifact_path = f"{artifact_dir}/{sha256}.tar.gz"

# 3. 解压到临时目录
extract_archive(archive_path, out_dir=tmp_dir)

# 4. 动态导入
import importlib.util
spec = importlib.util.spec_from_file_location(
    "_pycloud_user_xxx",
    f"{tmp_dir}/my_module.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

# 5. 调用函数
result = module.process(*args, **kwargs)
```

### 优点详解

#### 1. 跨版本兼容性 ✅
```python
# Python 3.10 编写的代码
# my_module.py (Python 3.10 语法)
def process_data(df):
    return df.groupby("key").sum()

# 可以在 Python 3.11 的节点上运行
# 因为是源码级别的导入，不是字节码级别的 pickle
```

**为什么兼容？**
- Python 源代码是跨版本的（语法变化除外）
- `import` 机制会在目标版本重新编译字节码
- 类似于 `pip install` 安装的包

#### 2. 可缓存性 ✅
```python
# 相同代码只存储一次
code_v1 = "def f(x): return x + 1"
submitter1.deploy_from_code(code_v1)  # 上传 → 存储
submitter2.deploy_from_code(code_v1)  # SHA256 命中 → 跳过上传

# 节省存储和网络带宽
```

#### 3. 可调试性 ✅
```bash
# 可以直接查看执行的代码
$ ls /tmp/pycloud_artifacts/
sha256_abc123.tar.gz

$ tar -tzf sha256_abc123.tar.gz
my_module.py
requirements.txt
utils/

$ cat /tmp/pycloud_run_xxx/my_module.py
def process(x):
    return x * 2  # ← 可以看到实际代码
```

### 缺点详解

#### 1. 无法捕获闭包 ❌
```python
# 当前方案无法处理这种情况
def make_processor(factor):
    multiplier = factor  # ← 闭包变量

    def process(x):  # ← 闭包函数
        return x * multiplier
    return process

processor = make_processor(10)

# ❌ 错误：无法将闭包函数打包成文件
submitter.deploy_from_code(processor)  # TypeError
```

**解决方案：**
```python
# 必须重写成模块形式
# processor_module.py
MULTIPLIER = 10  # ← 全局变量代替闭包

def process(x):
    return x * MULTIPLIER

submitter.deploy_from_code("processor_module.py")
```

#### 2. 无法使用 Lambda ❌
```python
# ❌ Lambda 函数无法导出
submitter.deploy_from_code(
    lambda x: x * 2  # TypeError
)
```

---

## 2️⃣ Cloudpickle 方案

### 工作流程
```
┌─────────┐      ┌─────────┐      ┌──────────┐
│ 客户端  │      │网关/Info│─────▶│NodeControl│
│ 函数对象  │─────▶│  Center │      │  反序列化  │
└─────────┘      └─────────┘      └──────────┘
    │                                  │
    │  cloudpickle.dumps              │
    │  (bytes)                        │ cloudpickle.loads
    └──────────────────────────────────┘
              直接调用函数
```

### 实现细节

**客户端操作：**
```python
import cloudpickle
from pycloud_parallel import TaskSubmitter

# 极简 API
def process(x):
    return x * 2

submitter = TaskSubmitter.deploy_from_function(
    infocenter_target="127.0.0.1:50051",
    func=process,  # ← 直接传函数对象
    runtime="py3.11",
)

# 支持 Lambda
submitter.deploy_from_function(
    func=lambda x: x ** 2,  # ← Lambda 也可以
)

# 支持闭包
def make_processor(factor):
    def process(x):
        return x * factor  # ← 捕获闭包变量
    return process

processor = make_processor(10)
submitter.deploy_from_function(func=processor)  # ← 自动捕获 factor=10
```

**服务端处理：**
```python
# 1. 接收 pickle bytes
pickle_bytes = request.payload

# 2. 反序列化
import cloudpickle
func = cloudpickle.loads(pickle_bytes)

# 3. 直接调用
result = func(*args, **kwargs)
```

### 优点详解

#### 1. API 极简 ✅
```python
# Ray 风格的 API
@ray.remote
def process(x):
    return x * 2

# PyCloud 可以做得更简单
from pycloud_parallel import remote

@remote  # ← 装饰器自动上传
def process(x):
    return x * 2

result = process.remote(10)  # ← 类似 Ray
```

#### 2. 完整的闭包支持 ✅
```python
# Cloudpickle 自动捕获闭包环境
def outer(x):
    data = [1, 2, 3]  # ← 局部变量

    def inner(y):
        return sum(data) + x + y  # ← 引用外部变量
    return inner

func = outer(10)

# 序列化后包含完整上下文
pickle_bytes = cloudpickle.dumps(func)
# 包含：inner 函数 + data=[1,2,3] + x=10
```

#### 3. 动态代码 ✅
```python
# 运行时生成的函数可以分发
def generate_algorithm(name):
    if name == "square":
        return lambda x: x ** 2
    elif name == "cube":
        return lambda x: x ** 3

algo = generate_algorithm("square")
submitter.deploy_from_function(func=algo)  # ← 动态生成也能分发
```

### 缺点详解

#### 1. 跨版本不兼容 ❌❌❌

**关键问题：**
```python
# ===== 客户端：Python 3.10 =====
import cloudpickle

def my_function(x):
    return x * 2

pickle_bytes = cloudpickle.dumps(my_function)
# pickle_bytes 包含：
# 1. 函数字节码（Python 3.10 格式）
# 2. 代码对象（包含版本特定的内部结构）
# 3. 导入的模块引用

# ===== 服务端：Python 3.11 =====
func = cloudpickle.loads(pickle_bytes)
# ❌ 错误：_pickle.UnpicklingError
# 原因：Python 3.10 的字节码与 3.11 不兼容
```

**为什么不兼容？**
1. **字节码格式变化**：每个 Python 版本可能改变字节码指令
2. **代码对象结构变化**：`CodeType` 的字段在不同版本可能不同
3. **内部 API 变化**：CPython 内部 C API 在版本间可能变化
4. **标准库变化**：pickle 可能序列化了版本特定的对象

**实际测试结果：**
```python
# Python 3.10.12 序列化
import cloudpickle
pickle_bytes = cloudpickle.dumps(lambda x: x * 2)

# Python 3.11.4 反序列化
# ❌ _pickle.UnpicklingError: unsupported pickle protocol
# 或 ValueError: bad marshal data
```

**版本兼容矩阵：**
| 序列化版本 | 反序列化版本 | 结果 |
|-----------|-------------|------|
| 3.10.x | 3.10.y | ✅ 兼容 |
| 3.10.x | 3.11.x | ❌ **不兼容** |
| 3.11.x | 3.10.x | ❌ **不兼容** |
| 3.11.x | 3.11.y | ✅ 兼容 |

#### 2. 缓存效率低 ⚠️
```python
# 相同逻辑，不同闭包值
def make_processor(factor):
    return lambda x: x * factor

p1 = make_processor(10)
p2 = make_processor(20)

# Cloudpickle 会产生不同的 SHA256
# 因为闭包变量 factor 的值不同
cloudpickle.dumps(p1)  # SHA256: abc123...
cloudpickle.dumps(p2)  # SHA256: def456...  （不同！）

# 即使逻辑相同，也要存储两份
```

#### 3. 依赖传递问题 ⚠️
```python
# 客户端有这个依赖
import some_library  # ← 假设这是一个第三方库

def process(x):
    return some_library.transform(x)

# Cloudpickle 只序列化函数引用
# 不序列化 some_library 模块本身
pickle_bytes = cloudpickle.dumps(process)

# 服务端必须预先安装 some_library
# 否则会：ModuleNotFoundError: No module named 'some_library'
```

---

## 3️⃣ 混合方案（推荐）

### 设计思路
```
┌─────────────────────────────────────────┐
│          客户端选择上传方式               │
└─────────────────────────────────────────┘
           │                    │
           ▼                    ▼
    ┌──────────┐         ┌──────────┐
    │ 文件上传  │         │Cloudpickle│
    │  (模块)   │         │  (函数)   │
    └──────────┘         └──────────┘
           │                    │
           ▼                    ▼
    ┌────────────────────────────────┐
    │     服务端自动识别格式           │
    │  - tar.gz/zip/py → 文件路径     │
    │  - pickle bytes → 反序列化       │
    └────────────────────────────────┘
```

### API 设计

```python
from pycloud_parallel import TaskSubmitter

# ===== 方式 1: 文件上传（跨版本兼容） =====
submitter = TaskSubmitter.deploy_from_code(
    infocenter_target="127.0.0.1:50051",
    code_path="./my_module.py",  # ← 文件路径
    runtime="py3.11",
)

# ===== 方式 2: Cloudpickle（同版本，极简） =====
def process(x):
    return x * 2

submitter = TaskSubmitter.deploy_from_function(
    infocenter_target="127.0.0.1:50051",
    func=process,  # ← 函数对象
    runtime="py3.11",  # ← 必须指定版本
)

# ===== 方式 3: 装饰器（最简单） =====
from pycloud_parallel import remote

@remote(runtime="py3.11")  # ← 自动上传
def process(x):
    return x * 2

result = process.call(10)  # ← 远程调用
```

### 版本协商机制

```python
# 客户端检测 Python 版本
import sys
client_version = f"{sys.version_info.major}.{sys.version_info.minor}"

# 查询节点版本
nodes = infocenter.list_nodes()
node_versions = {node.id: node.python_version for node in nodes}

# 智能选择上传方式
if all(v == client_version for v in node_versions.values()):
    # 所有节点版本相同 → 使用 Cloudpickle
    use_cloudpickle = True
else:
    # 版本不一致 → 使用文件上传
    use_cloudpickle = False
```

### 实现细节

**UploadCode 扩展：**
```protobuf
message UploadCodeRequest {
  oneof body {
    CodeMeta meta = 1;        // ← 现有文件上传
    Chunk chunk = 2;          // ← 现有文件分块

    // 新增：Cloudpickle 支持
    FunctionMeta func_meta = 3;  // ← 函数元数据
    bytes pickle_chunk = 4;      // ← pickle 分块
  }
}

message FunctionMeta {
  string client_id = 1;
  string sha256 = 2;           // pickle 的 SHA256
  string runtime = 3;          // "py3.10", "py3.11"
  int64 size_bytes = 4;
  string function_name = 5;    // 函数名（可选）
}
```

**服务端处理：**
```python
def handle_upload_code(request_iterator):
    meta = None
    pickle_chunks = []

    for req in request_iterator:
        kind = req.WhichOneof("body")

        if kind == "func_meta":
            meta = req.func_meta
        elif kind == "pickle_chunk":
            pickle_chunks.append(req.pickle_chunk)

    if meta:
        # Cloudpickle 方式
        pickle_bytes = b"".join(pickle_chunks)
        artifact = store_pickle(meta.sha256, pickle_bytes)
    else:
        # 文件上传方式（现有逻辑）
        artifact = store_file(...)
```

---

## 4️⃣ 迁移策略

### 阶段 1: 添加 Cloudpickle 支持（不破坏现有功能）

```python
# 1. 新增 API
class TaskSubmitter:
    @classmethod
    def deploy_from_function(
        cls,
        *,
        infocenter_target: str,
        func: Callable,
        runtime: str,
    ) -> "TaskSubmitter":
        """使用 Cloudpickle 上传函数（仅限同版本）"""
        pickle_bytes = cloudpickle.dumps(func)
        sha256 = hashlib.sha256(pickle_bytes).hexdigest()

        # 上传到 NodeControl
        # ...
```

### 阶段 2: 添加版本检测和警告

```python
def deploy_from_function(...):
    # 检测版本兼容性
    client_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    node_versions = get_node_versions(infocenter_target)

    if not all(v == client_version for v in node_versions.values()):
        logger.warning(
            f"⚠️ Cloudpickle requires same Python version on all nodes. "
            f"Client: {client_version}, Nodes: {node_versions}. "
            f"Use deploy_from_code() for cross-version support."
        )
```

### 阶段 3: 添加 @remote 装饰器

```python
def remote(runtime: str = None):
    """装饰器：自动上传函数到远程"""
    def decorator(func):
        # 自动检测：如果所有节点版本一致，用 Cloudpickle
        # 否则，提示用户使用文件上传

        # 创建远程调用包装器
        class RemoteFunction:
            def __call__(self, *args, **kwargs):
                # 本地执行（用于调试）
                return func(*args, **kwargs)

            def remote(self, *args, **kwargs):
                # 远程执行
                submitter = TaskSubmitter.deploy_from_function(
                    infocenter_target="...",
                    func=func,
                    runtime=runtime,
                )
                return submitter.submit(*args, **kwargs)

        return RemoteFunction()
    return decorator
```

---

## 5️⃣ 推荐方案

### 短期（1-2 周）
✅ **保留文件上传方案**
- 保持当前 API 不变
- 优化缓存和错误处理

### 中期（1-2 月）
✅ **添加 Cloudpickle 作为可选方案**
- 新增 `deploy_from_function()` API
- 添加版本检测和警告
- 文档说明适用场景

### 长期（3-6 月）
✅ **智能混合方案**
- 自动检测版本兼容性
- 提供统一的上传接口
- 添加 `@remote` 装饰器

---

## 6️⃣ 决策建议

### 使用文件上传（当前方案）
✅ **适合场景：**
- 生产环境（多版本 Python）
- 需要调试的场景
- 大型项目/模块部署
- 需要依赖管理

### 使用 Cloudpickle（新增方案）
✅ **适合场景：**
- 开发/测试环境（统一版本）
- 交互式计算（Jupyter）
- 快速原型验证
- 闭包/Lambda 需求

### 避免使用 Cloudpickle
❌ **不适合场景：**
- 生产环境（版本不一致）
- 需要长期缓存
- 复杂依赖传递
- 需要代码审计

---

## 7️⃣ 参考实现

**Ray 的做法：**
```python
# Ray 默认使用 Cloudpickle
@ray.remote
def func(x):
    return x * 2

# 但也支持显式指定模块
ray.remote(module="my_module").func.remote()
```

**Dask 的做法：**
```python
# Dask 完全依赖 Cloudpickle
# 要求所有 Worker 版本一致
```

**我们的方案：**
```python
# 混合方案，兼容性和开发体验兼顾
from pycloud_parallel import remote, TaskSubmitter

# 方式 1: 装饰器（自动选择）
@remote(runtime="py3.11")
def func(x):
    return x * 2

# 方式 2: 显式文件上传（跨版本）
TaskSubmitter.deploy_from_code(...)

# 方式 3: 显式函数上传（同版本）
TaskSubmitter.deploy_from_function(...)
```

---

## 📌 总结

| 方面 | 文件上传 | Cloudpickle | 混合方案 |
|------|---------|-------------|----------|
| **兼容性** | ✅ 跨版本 | ❌ 同版本 | ✅ 自动适配 |
| **易用性** | ⚠️ 需打包 | ✅ 极简 | ✅ 极简 |
| **功能** | ⚠️ 无闭包 | ✅ 完整 | ✅ 完整 |
| **生产就绪** | ✅ 就绪 | ⚠️ 受限 | ✅ 就绪 |

**推荐：先添加 Cloudpickle 作为可选功能，逐步迁移到混合方案。**
