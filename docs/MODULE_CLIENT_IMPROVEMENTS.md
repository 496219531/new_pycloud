# PyCloud 模块化客户端改进总结

## 改进内容

### 1. TaskModuleClient（新增）

**问题：** `TaskBatchClient` 使用繁琐，需要手动构造 payload

**改进前：**
```python
from pycloud_parallel.controlplane.client import TaskBatchClient

batch = TaskBatchClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task.py",
)

# 提交任务
result = batch.submit_payloads([{"value": 7}])

# 等待结果
results = batch.wait_for_results(expected_count=1)
```

**改进后：**
```python
from pycloud_parallel import TaskModuleClient

task = TaskModuleClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task.py",
)

# 像调用函数一样提交任务并等待结果
results = task.run(value=7)
```

### 2. 顶层导入（新增）

**改进前：**
```python
from pycloud_parallel.controlplane.client import (
    ServiceModuleGroup,
    TaskModuleClient,
    GatewayModuleClient,
)
```

**改进后：**
```python
from pycloud_parallel import (
    ServiceModuleGroup,
    TaskModuleClient,
    GatewayModuleClient,
)
```

## 新增类

### TaskModuleClient

任务模式的模块化客户端，提供类似 Python 函数调用的方式来提交任务。

**特点：**
- ✅ 简化 API：`task.run(value=7)` 而不是 `submit_payloads([{"value": 7}])`
- ✅ 自动处理 payload 序列化
- ✅ 灵活：支持提交后等待或异步获取
- ✅ 兼容性：基于 TaskBatchClient，保留所有功能

**使用方式：**
```python
from pycloud_parallel import TaskModuleClient

task = TaskModuleClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task.py",
)

# 方式 1: 提交并等待
results = task.run(value=7)

# 方式 2: 只提交
resp = task.run.submit(value=7)
results = task.wait_for_results(expected_count=1)

# 方式 3: 批量提交
results = task.submit_payloads([{"value": i} for i in range(10)])
```

### _TaskCallProxy

任务调用代理，提供类似函数的调用方式。

**特点：**
- `.submit()` - 只提交任务
- `()` - 提交任务并等待结果

## 顶层导出

更新了 `pycloud_parallel/__init__.py`，导出以下模块化客户端：

```python
from pycloud_parallel import (
    # 本地并行
    foreach,
    parallel_for,
    # 分布式模块化客户端
    ServiceModuleGroup,
    TaskModuleClient,
    GatewayModuleClient,
)
```

## 文件变更

### 修改的文件

1. **[src/pycloud_parallel/controlplane/client.py](src/pycloud_parallel/controlplane/client.py)**
   - 新增 `TaskModuleClient` 类
   - 新增 `_TaskCallProxy` 类

2. **[src/pycloud_parallel/controlplane/__init__.py](src/pycloud_parallel/controlplane/__init__.py)**
   - 导出 `TaskModuleClient`

3. **[src/pycloud_parallel/__init__.py](src/pycloud_parallel/__init__.py)**
   - 导出 `ServiceModuleGroup`
   - 导出 `TaskModuleClient`
   - 导出 `GatewayModuleClient`

### 新增的文件

4. **[examples/demo_task_module_client.py](../examples/demo_task_module_client.py)**
   - TaskModuleClient 演示脚本

5. **[examples/demo_top_level_import.py](../examples/demo_top_level_import.py)**
   - 顶层导入演示脚本

6. **[docs/TASK_MODULE_CLIENT.md](docs/TASK_MODULE_CLIENT.md)**
   - TaskModuleClient 使用指南

7. **[docs/QUICK_START.md](docs/QUICK_START.md)**
   - 快速入门指南

## 三种模块化客户端对比

| 客户端 | 模式 | 用途 | 生命周期 |
|--------|------|------|----------|
| `ServiceModuleGroup` | Service Session | 长运行 RPC 服务 | 拥有者管理 |
| `TaskModuleClient` | Task | 短期任务处理 | 拥有者管理 |
| `GatewayModuleClient` | Gateway | 按服务名调用 | 无需管理 |

## 使用示例对比

### Service Session 模式

```python
from pycloud_parallel import ServiceModuleGroup

group = ServiceModuleGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="service.py",
)

result = await group.square(x=7)
group.close(end_services=True)
```

### Task 模式

```python
from pycloud_parallel import TaskModuleClient

task = TaskModuleClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task.py",
)

results = task.run(value=7)
task.close()
```

### Gateway 调用

```python
from pycloud_parallel import GatewayModuleClient

client = GatewayModuleClient(
    "127.0.0.1:50051",
    service_name="square-service",
)

result = client.square.sync(x=7)
```

## 运行演示

```bash
# 本地并行
python -c "from pycloud_parallel import foreach; print(foreach(lambda x: x*x, range(10)))"

# Service Session
python examples/demo_gateway_complete.py

# Task 模式
python examples/demo_task_module_client.py

# Gateway 调用
python examples/demo_gateway_client.py

# 顶层导入
python examples/demo_top_level_import.py
```

## 向后兼容性

✅ **完全向后兼容**：
- 所有旧的导入方式仍然可用
- 从子模块导入仍然有效
- 现有代码无需修改

```python
# 仍然可以工作
from pycloud_parallel.controlplane import ServiceModuleGroup
from pycloud_parallel.controlplane.client import TaskBatchClient
```

## 总结

| 改进点 | 效果 |
|--------|------|
| 新增 TaskModuleClient | Task 模式使用像函数调用一样简单 |
| 顶层导入 | 导入语句更简洁 |
| 统一 API | 三种模式使用方式一致 |
| 完整文档 | 降低学习成本 |

现在 PyCloud 的三种分布式模式（Service Session、Task、Gateway）都有一致的、简洁的模块化调用方式！
