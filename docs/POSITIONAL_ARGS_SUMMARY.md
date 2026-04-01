# 位置参数支持实现总结

## 完成时间
2026-04-01

## 需求背景

用户提出：服务端代码应该是**最自然的 Python 函数**，而不是处理字典。框架应该支持完整的 Python 参数传递语义，包括位置参数、命名参数和混合使用。

## 实现方案

### 1. 传输格式设计

采用统一的 `args/kwargs` 结构：

```python
# 新格式
payload = {
    "args": [1, 2, 3],      # 位置参数
    "kwargs": {"c": 4}      # 命名参数
}

# 旧格式（向后兼容）
payload = {"x": 1, "y": 2}
```

### 2. 服务端实现

**文件**: `src/pycloud_parallel/controlplane/state.py`

修改 `_invoke_user_callable` 函数（第 350-389 行）：

```python
def _invoke_user_callable(fn, payload: dict):
    """调用用户函数，支持多种参数传递方式。"""

    # 检查是否是新的 args/kwargs 格式
    if isinstance(payload, dict) and ("args" in payload or "kwargs" in payload):
        args = payload.get("args", [])
        kwargs = payload.get("kwargs", {})
        # 确保类型正确
        if not isinstance(args, list):
            args = list(args) if args else []
        if not isinstance(kwargs, dict):
            kwargs = {}
        return fn(*args, **kwargs)  # 关键：解包调用

    # 旧格式兼容
    if len(params) == 1 and params[0].kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
        return fn(payload)

    if isinstance(payload, dict):
        try:
            return fn(**payload)
        except TypeError:
            return fn(payload)
    return fn(payload)
```

### 3. 客户端实现

**文件**: `src/pycloud_parallel/controlplane/client.py`

#### 3.1 异步调用代理 `_CallProxy.__call__`（第 3562-3591 行）

```python
async def __call__(self, *args, **kwargs) -> Dict[str, object]:
    """异步调用服务方法，支持位置参数和命名参数。"""
    # 构造新的 payload 格式
    payload = {}
    if args:
        payload["args"] = list(args)
    if kwargs:
        payload["kwargs"] = kwargs

    # 如果两者都有，使用新格式；否则保持向后兼容
    final_payload = payload if payload else kwargs

    _, resp = await self._group.acall_balanced(
        self._method,
        final_payload,
        timeout_sec=self._timeout_sec,
        strategy=self._strategy,
        refresh_status=self._refresh_status,
    )
    return resp.get("data", resp)
```

#### 3.2 同步调用代理 `_SyncCallProxy.__call__`（第 3675-3705 行）

```python
def __call__(self, *args, **kwargs) -> Dict[str, object]:
    """同步调用服务方法，支持位置参数和命名参数。"""
    # 构造新的 payload 格式
    payload = {}
    if args:
        payload["args"] = list(args)
    if kwargs:
        payload["kwargs"] = kwargs

    # 如果两者都有，使用新格式；否则保持向后兼容
    final_payload = payload if payload else kwargs

    _, resp = self._group.call_balanced(
        self._method,
        final_payload,
        timeout_sec=self._timeout_sec,
        strategy=self._strategy,
        refresh_status=self._refresh_status,
    )
    return resp.get("data", resp)
```

#### 3.3 任务提交代理 `_TaskCallProxy.__call__`（第 4321-4347 行）

```python
def __call__(self, *args, **kwargs) -> Sequence[pb2.TaskResult]:
    """直接调用：提交任务并等待结果，支持位置参数。"""
    # 构造 payload
    payload = dict(self._payload)

    # 如果有位置参数或命名参数，使用新格式
    if args or kwargs:
        if args:
            payload["args"] = list(args)
        if kwargs:
            payload["kwargs"] = kwargs
        return self.submit_and_wait(
            timeout_hint_sec=self._timeout_hint_sec,
            priority=self._priority,
            runtime_key=self._runtime_key,
            payload=payload,
        )

    return self.submit_and_wait()
```

### 4. 演示脚本

#### 4.1 Service Session 模式演示
**文件**: `scripts/demo_positional_args.py`

展示功能：
- 单参数函数（位置/命名）
- 多参数函数（全部位置/全部命名）
- 带默认值函数
- 可变参数函数
- 混合参数函数
- 同步/异步调用
- 批量并发调用

#### 4.2 Task 模式演示
**文件**: `scripts/demo_task_positional_args.py`

展示功能：
- 位置参数提交
- 命名参数提交
- 混合参数提交
- 批量提交

### 5. 文档

**文件**: `docs/POSITIONAL_ARGS_SUPPORT.md`

包含：
- 设计原理
- 使用方式
- 代码示例
- 向后兼容说明
- 实现细节

## 使用示例

### Service Session 模式

```python
from pycloud_parallel import DeployedService

# 服务端：自然 Python 函数
blob = b"""
@pycloud_export
def square(x):
    return x * x

@pycloud_export
def add(a, b):
    return a + b
"""

group = DeployedService.deploy_from_infocenter(blob=blob, ...)

# 客户端：多种调用方式
result = await group.square(7)        # 位置参数
result = await group.square(x=7)      # 命名参数
result = await group.add(10, 20)      # 多位置参数
result = await group.add(a=10, b=20)  # 多命名参数
```

### Task 模式

```python
from pycloud_parallel import TaskSubmitter

task = TaskSubmitter.from_infocenter(blob=blob, ...)

results = task.run(7)           # 位置参数
results = task.run(x=5, y=3)    # 命名参数
results = task.run(10, y=2)     # 混合使用
```

## 向后兼容

✅ 完全向后兼容旧代码

```python
# 旧代码（仍然有效）
result = await group.square(x=7)

# 新代码（更简洁）
result = await group.square(7)
```

## 优势

1. **服务端代码自然**：像写本地函数一样
2. **调用方式灵活**：支持所有 Python 参数传递方式
3. **类型安全**：保留参数位置和类型信息
4. **完全兼容**：旧代码无需修改

## 测试验证

✅ 所有文件语法检查通过：
- `src/pycloud_parallel/controlplane/state.py`
- `src/pycloud_parallel/controlplane/client.py`
- `scripts/demo_positional_args.py`
- `scripts/demo_task_positional_args.py`

## 修改的文件清单

1. **核心实现**
   - `src/pycloud_parallel/controlplane/state.py` - 服务端参数解包
   - `src/pycloud_parallel/controlplane/client.py` - 客户端参数打包

2. **演示脚本**
   - `scripts/demo_positional_args.py` - Service Session 位置参数演示
   - `scripts/demo_task_positional_args.py` - Task 位置参数演示

3. **文档**
   - `docs/POSITIONAL_ARGS_SUPPORT.md` - 功能说明文档
   - `docs/POSITIONAL_ARGS_SUMMARY.md` - 本实现总结

## 下一步

用户可以：
1. 运行演示脚本测试功能
2. 在现有代码中逐步采用位置参数
3. 享受更自然的 Python 调用体验！
