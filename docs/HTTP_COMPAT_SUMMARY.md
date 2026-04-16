# HTTP 风格兼容性说明

## 问题

如何在支持位置参数的同时，保持与 HTTP API 调用风格的兼容性？

## 解决方案

采用智能识别策略：根据 payload 的键来判断使用哪种格式。

## 识别逻辑

```python
# node/execution.py:_invoke_user_callable
if isinstance(payload, dict) and ("args" in payload or "kwargs" in payload):
    # 新格式：支持位置参数
    args = payload.get("args", [])
    kwargs = payload.get("kwargs", {})
    return fn(*args, **kwargs)
else:
    # HTTP 风格：整个 payload 作为 kwargs
    return fn(**payload)
```

## 支持的格式

### 1. 新格式（支持位置参数）

```python
# 只有位置参数
{"args": [1, 2, 3]}
# → fn(1, 2, 3)

# 只有命名参数
{"kwargs": {"x": 1, "y": 2}}
# → fn(x=1, y=2)

# 混合参数
{"args": [1], "kwargs": {"y": 2}}
# → fn(1, y=2)
```

### 2. HTTP 风格（直接字典）

```python
# 不包含 args/kwargs 键
{"x": 1, "y": 2}
# → fn(x=1, y=2)

# 嵌套字典
{"user": {"name": "Alice"}, "action": "login"}
# → fn(user={"name": "Alice"}, action="login")
```

## 使用场景

### 场景 1: 模块化调用（自动使用新格式）

```python
from pycloud_parallel import Service

group = Service.deploy_from_infocenter(...)

# 位置参数 → {"args": [10, 20]}
result = await group.add(10, 20)

# 命名参数 → {"kwargs": {"a": 10, "b": 20}}
result = await group.add(a=10, b=20)

# 混合参数 → {"args": [10], "kwargs": {"b": 20}}
result = await group.add(10, b=20)
```

### 场景 2: HTTP 调用（自动使用 HTTP 风格）

```python
from pycloud_parallel.controlplane.client import GatewayServiceClient

# HTTP 客户端 → payload 直接传递
with GatewayServiceClient("127.0.0.1:50051") as client:
    result = client.call(
        service_name="my-service",
        method="add",
        payload={"a": 10, "b": 20},  # HTTP 风格
    )
```

### 场景 3: REST API 调用（完全兼容）

```bash
# 标准 REST API 调用
curl -X POST http://127.0.0.1:50051/svc/my-service/call/add \
  -H "Content-Type: application/json" \
  -d '{"a": 10, "b": 20}'

# 服务器会自动将 payload 作为 kwargs 传递
```

## 兼容性矩阵

| 调用方式 | Payload 格式 | 服务端接收 |
|---------|-------------|-----------|
| `group.add(10, 20)` | `{"args": [10, 20]}` | `add(10, 20)` |
| `group.add(a=10, b=20)` | `{"kwargs": {"a": 10, "b": 20}}` | `add(a=10, b=20)` |
| `group.add(10, b=20)` | `{"args": [10], "kwargs": {"b": 20}}` | `add(10, b=20)` |
| `client.call(..., payload={"a": 10, "b": 20})` | `{"a": 10, "b": 20}` | `add(a=10, b=20)` |
| `curl ... -d '{"a": 10, "b": 20}'` | `{"a": 10, "b": 20}` | `add(a=10, b=20)` |

## 关键优势

1. **自动识别**：框架根据 payload 结构自动选择解析方式
2. **完全兼容**：HTTP API 调用无需修改
3. **零学习成本**：开发者无需关心底层格式
4. **逐步迁移**：可以逐步采用新的位置参数特性

## 实现细节

### 判断条件

```python
# 新格式的判断条件
has_args_or_kwargs = "args" in payload or "kwargs" in payload

# 为什么这样判断？
# - 新格式明确包含 args 或 kwargs 键
# - HTTP 风格的 payload 通常包含业务键（如 user_id, name 等）
# - 避免误判：只有显式指定才使用新格式
```

### 边界情况处理

```python
# 空字典
{} → fn()

# 只有 args，kwargs 为空
{"args": []} → fn()

# 只有 kwargs，args 为空
{"kwargs": {}} → fn()

# 业务字典（HTTP 风格）
{"x": 1, "y": 2} → fn(x=1, y=2)

# 如果业务数据真的需要叫 "args" 或 "kwargs"？
# 极少数情况，建议使用其他键名
```

## 测试验证

运行演示脚本测试所有场景：

```bash
# 测试位置参数
python examples/demo_positional_args.py

# 测试 HTTP 兼容性
python examples/demo_http_compat.py
```

## 总结

通过智能识别策略，PyCloud 实现了：
- ✅ 支持位置参数（新格式）
- ✅ 完全兼容 HTTP 风格
- ✅ 自动判断，无需手动指定
- ✅ 零学习成本

开发者可以自由选择调用方式，框架自动处理！
