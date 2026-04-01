# 位置参数支持

## 概述

PyCloud 现在支持完整的 Python 参数传递语义，包括：
- **位置参数** (positional arguments)
- **命名参数** (keyword arguments)
- **混合使用** (mixed positional and keyword arguments)

## 设计原理

### 传输格式

客户端和服务器之间使用统一的 payload 格式，自动识别：

```python
# 格式 1: 只有位置参数
payload = {
    "args": [1, 2, 3]
}
# → fn(1, 2, 3)

# 格式 2: 只有命名参数
payload = {
    "kwargs": {"x": 1, "y": 2}
}
# → fn(x=1, y=2)

# 格式 3: 混合参数
payload = {
    "args": [1, 2],
    "kwargs": {"z": 3}
}
# → fn(1, 2, z=3)

# 格式 4: HTTP 风格（不包含 args/kwargs 键）
payload = {
    "x": 1,
    "y": 2
}
# → fn(x=1, y=2)
```

**识别规则**：
- 如果 `payload` 包含 `args` 或 `kwargs` 键 → 使用新格式（支持位置参数）
- 否则 → HTTP 风格，整个 payload 作为 kwargs

### 服务端处理

服务端的 `_invoke_user_callable` 函数会自动检测并解析参数：

```python
# state.py:350
def _invoke_user_callable(fn, payload: dict):
    # 判断标准：payload 包含 'args' 或 'kwargs' 键
    if isinstance(payload, dict) and ("args" in payload or "kwargs" in payload):
        args = payload.get("args", [])      # 如果没有 args，返回空列表
        kwargs = payload.get("kwargs", {})  # 如果没有 kwargs，返回空字典
        return fn(*args, **kwargs)  # 解包调用

    # HTTP 风格：整个 payload 作为 kwargs
    return fn(**payload)
```

**兼容逻辑**：
1. 如果 `payload` 包含 `args` 或 `kwargs` 键 → 新格式，支持位置参数
2. 否则 → HTTP 风格，整个 payload 直接作为 kwargs

这样既支持新的位置参数，又完全兼容 HTTP 调用！

### 客户端转换

客户端代理自动将 Python 函数调用转换为 payload：

```python
# client.py:3562 (_CallProxy.__call__)
async def __call__(self, *args, **kwargs):
    payload = {}
    if args:
        payload["args"] = list(args)
    if kwargs:
        payload["kwargs"] = kwargs

    # 如果两者都有，使用新格式；否则保持向后兼容
    final_payload = payload if payload else kwargs
    ...
```

## 使用方式

### Service Session 模式

```python
from pycloud_parallel import DeployedService

# 服务端代码：最自然的 Python 函数
blob = b"""
def pycloud_export(fn):
    fn.__pycloud_export__ = True
    return fn

@pycloud_export
def square(x):
    return x * x

@pycloud_export
def add(a, b):
    return a + b

@pycloud_export
def compute(a, b, c=0, d=0):
    return a + b + c + d
"""

# 部署服务
group = DeployedService.deploy_from_infocenter(...)

# 客户端调用：支持多种方式

# 1. 位置参数
result = await group.square(7)        # 49

# 2. 命名参数
result = await group.square(x=7)      # 49

# 3. 多位置参数
result = await group.add(10, 20)      # 30

# 4. 多命名参数
result = await group.add(a=10, b=20)  # 30

# 5. 混合使用
result = await group.compute(1, 2, c=3, d=4)  # 10

# 6. 同步调用
result = group.square.sync(7)

# 7. 异步并发
results = await asyncio.gather(
    group.square(1),
    group.square(2),
    group.add(10, 20),
)
```

### Task 模式

```python
from pycloud_parallel import TaskSubmitter

# 任务代码
blob = b"""
def run(payload):
    if isinstance(payload, dict):
        if 'args' in payload or 'kwargs' in payload:
            args = payload.get('args', [])
            kwargs = payload.get('kwargs', {})
            return compute(*args, **kwargs)
    return compute(0, 0)

def compute(x, y=1):
    return x * y
"""

# 创建任务客户端
task = TaskSubmitter.from_infocenter(blob=blob, ...)

# 提交任务

# 1. 位置参数
results = task.run(7)           # 7

# 2. 命名参数
results = task.run(x=5, y=3)    # 15

# 3. 混合使用
results = task.run(10, y=2)     # 20
```

### Gateway 调用

```python
from pycloud_parallel import GatewayConnect

# 创建 Gateway 连接
client = GatewayConnect(
    "127.0.0.1:50051",
    service_name="my-service"
)

# 支持 Service Session 模式的所有调用方式
result = client.square.sync(7)
result = await client.square(7)
result = await client.add(10, 20)
```

## 向后兼容

旧代码完全兼容，无需修改：

```python
# 旧代码（仍然有效）
result = await group.square(x=7)

# 新代码（更简洁）
result = await group.square(7)
```

## 服务端代码编写

服务端代码**不需要处理 payload 字典**，直接写普通 Python 函数：

```python
# ✅ 正确：自然的 Python 函数
def square(x):
    return x * x

def add(a, b):
    return a + b

def greet(name, message="hello"):
    return f"{message}, {name}!"

def summarize(*values):
    return sum(values)

# ❌ 错误：不需要自己解析 payload
def square(payload):
    x = payload.get('x')  # 不需要这样！
    return x * x
```

框架会自动：
1. 接收 `{"args": [...], "kwargs": {...}}` 格式
2. 解包为 `*args, **kwargs`
3. 调用你的函数 `func(*args, **kwargs)`

## 示例脚本

- `scripts/demo_positional_args.py` - Service Session 模式位置参数演示
- `scripts/demo_task_positional_args.py` - Task 模式位置参数演示
- `scripts/demo_http_compat.py` - HTTP 风格兼容性演示

## 实现细节

### 修改的文件

1. **src/pycloud_parallel/controlplane/state.py**
   - `_invoke_user_callable` 函数：支持 `args/kwargs` 解包

2. **src/pycloud_parallel/controlplane/client.py**
   - `_CallProxy.__call__`：异步调用代理，支持位置参数
   - `_SyncCallProxy.__call__`：同步调用代理，支持位置参数
   - `_TaskCallProxy.__call__`：任务提交代理，支持位置参数

### 测试

确保你的服务端函数签名正确：

```python
# 测试框架是否正确解包参数
def test(*args, **kwargs):
    return {
        "args": args,
        "kwargs": kwargs,
        "args_count": len(args),
        "kwargs_count": len(kwargs),
    }
```

调用：
```python
result = await group.test(1, 2, 3, a=4, b=5)
# 返回: {"args": (1, 2, 3), "kwargs": {"a": 4, "b": 5}, ...}
```

## 限制

1. **服务端函数必须可调用**：使用 `@pycloud_export` 或在 `export_methods` 中声明
2. **参数类型必须可序列化**：通过 JSON/gRPC 传输
3. **不支持 `**kwargs` 捕获所有参数**：因为框架已经处理了参数解包

## HTTP 风格兼容性

PyCloud 完全兼容 HTTP API 的调用风格。当你使用 `GatewayServiceClient` 或直接调用 HTTP 端点时：

```python
from pycloud_parallel.controlplane.client import GatewayServiceClient

# HTTP 风格：直接传字典
with GatewayServiceClient("127.0.0.1:50051") as client:
    result = client.call(
        service_name="my-service",
        method="add",
        payload={"a": 10, "b": 20},  # HTTP 风格
    )
    # → 服务端调用 add(a=10, b=20)
```

**兼容逻辑**：
- 框架检测到 `payload` 不包含 `args` 或 `kwargs` 键
- 自动将整个 payload 作为 kwargs 传递
- 完全兼容标准 HTTP API 调用

### 混合使用示例

```python
# 方式 1: 模块化调用（位置参数）
result = await group.add(10, 20)
# payload: {"args": [10, 20]}

# 方式 2: 模块化调用（命名参数）
result = await group.add(a=10, b=20)
# payload: {"kwargs": {"a": 10, "b": 20}}

# 方式 3: HTTP 调用（字典风格）
result = client.call(..., payload={"a": 10, "b": 20})
# payload: {"a": 10, "b": 20} → 当作 kwargs

# 所有方式都能正确调用服务端的 add(a, b) 函数！
```

## 总结

现在 PyCloud 完全支持 Python 的参数传递语义：
- ✅ 位置参数
- ✅ 命名参数
- ✅ 默认参数值
- ✅ 可变参数 (`*args`)
- ✅ 混合使用
- ✅ 同步/异步调用
- ✅ HTTP 风格兼容
- ✅ 向后兼容旧代码

服务端代码可以像写本地函数一样自然！

