# TaskSubmitter

它的目标是：

1. 比 `TaskBatchClient` 更少样板代码
2. 保留任务模式的 `job_id / runtime_key / cancel / wait_for_results`
3. 提供更接近“调用函数”的体验

## 1. 与 TaskBatchClient 的关系

关系很简单：

1. `TaskSubmitter` 是对 `TaskBatchClient` 的薄封装
2. 底层选点、上传代码、任务流、结果等待仍然由 `TaskBatchClient` 承担

## 2. 创建方式

```python
from pycloud_parallel import TaskSubmitter

blob = (
    b"def run(value=0, **_kwargs):\n"
    b"    value = int(value)\n"
    b"    return {'value': value, 'square': value * value}\n"
)

with TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    runtime="py3",
    entry_module="task_demo",
    preferred_runtime_key="demo-runtime",
) as task:
    print(task.client_id)
    print(task.job_id)
    print(task.code_version)
    print(task.node_ids)
```

## 3. 调用方式

### 3.1 直接调用并等待

```python
results = task.run(value=7, runtime_key="demo-runtime")
```

这等价于：

1. 提交一个任务
2. 等待这次提交对应的结果返回

### 3.2 只提交，不等待

```python
resp = task.run.submit(value=7, runtime_key="demo-runtime")
```

然后：

```python
results = task.wait_for_results(
    expected_count=len(resp.accepted),
    timeout_sec=10.0,
)
```

### 3.3 批量 payload

```python
resp = task.submit_payloads(
    [{"value": 1}, {"value": 2}],
    job_id="job-demo",
    runtime_key="demo-runtime",
)
results = task.wait_for_results(job_id="job-demo", expected_count=2)
```

## 4. `runtime_key` 用法

建议把同一类热任务显式放到同一个 `runtime_key`：

```python
results = task.run(value=7, runtime_key="factor-alpha")
resp = task.run.submit(value=8, runtime_key="factor-alpha")
```

作用：

1. 任务更容易被回打到热 node
2. 节点内更容易复用 runtime slot
3. 减少代码切换和冷启动

`runtime_key` 只影响热点复用，不负责 Python 版本筛选。

如果你需要约束节点 Python 版本，请在创建客户端时传 `runtime`：

```python
with TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    runtime=">=py3.11",
    entry_module="task_demo",
) as task:
    ...
```

## 5. 常用能力

```python
# 等结果
results = task.wait_for_results(expected_count=1, timeout_sec=10.0)

# 拉结果
resp = task.pull_results(limit=20, wait_ms=500)

# 取消整批任务
task.cancel_job(job_id="job-demo", reason="debug stop")

# 查节点指标
metrics = task.get_metrics()
```

## 6. 适用场景

适合：

1. 一个任务模块主要暴露一个入口函数
2. 希望少写 `TaskSubmitItem` 和 payload 包装代码
3. 想保留任务模式的多节点选点、热点路由和取消能力

不适合：

1. 你需要完全手工控制每个 `TaskSubmitItem`
2. 你要混合不同 job 的复杂批量路由

这些情况下直接用 `TaskBatchClient` 更合适。
