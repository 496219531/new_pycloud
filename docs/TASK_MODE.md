# 任务模式

## 1. 当前定位

当前任务侧已经收敛为两层：

1. `JobQueue Mode`
   - 大任务排队与单活调度层
   - 大任务排到后，再展开成 subtasks
2. `TaskPool Mode`
   - 子任务执行层
   - 通过原生 `TaskPoolSession` 创建专属 pool 执行 subtasks

`Gateway` 不参与任务模式。

## 2. 当前推荐入口

### 2.1 `TaskPoolSession`

适合：

1. 直接申请一组专属 worker
2. 立即执行一批 subtasks
3. 执行结束后回收 pool

最小示例：

```python
from pycloud_parallel import TaskPoolSession

with TaskPoolSession.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    job_id="demo-job",
    blob=blob,
    runtime="py3",
    entry_module="task_demo",
    entry_callable="run",
    worker_count=2,
    node_count=1,
) as pool:
    resp = pool.submit_payloads([{"value": 7}, {"value": 8}])
    results = pool.wait_for_data(expected_count=len(resp.accepted), timeout_sec=10.0)
    print(results)

    mapped = pool.map([9, 10, 11], timeout_sec=10.0)
    print(mapped)
```

### 2.2 `JobQueueClient`

适合：

1. 大任务先排队
2. 同一时刻只允许一个大任务进入运行态
3. job 排到后，再自动创建 `TaskPoolSession`

最小示例：

```python
from pycloud_parallel import JobQueueClient

client = JobQueueClient("127.0.0.1:50051")
client.submit_job_from_bytes(
    blob=driver_blob,
    driver_entry_module="job_driver_demo",
    task_entry_module="task_demo",
    task_entry_callable="run",
    pool_worker_count=2,
    pool_node_count=2,
)
```

函数对象写法：

```python
client.submit_job_from_func(
    func=build_subtasks,
    task_func=run_subtask,
    pool_worker_count=2,
    pool_node_count=2,
)
```

模块对象写法：

```python
client.submit_job_from_module(
    module=job_driver_module,
    task_module=task_module,
    task_entry_callable="run",
    pool_worker_count=2,
    pool_node_count=2,
)
```

等待 job 终态：

```python
final = client.wait_for_terminal(job_id, timeout_sec=30.0)
print(final["job"]["status"])
```

## 3. TaskPoolSession 能力

当前原生 `TaskPoolSession` 已支持：

1. 创建 pool
2. pool heartbeat 保活
3. 提交任务
4. 拉取结果
5. 取消 job
6. 查询 pool 状态
7. 关闭 pool

## 4. 结果语义

当前任务结果支持：

1. 小结果 inline 返回
2. 大结果 / 文件结果 / `DataFrame` / `ndarray`
   - 落到 node 本地 `objects/`
   - 返回 `ResultRef`

高层接口：

1. `TaskPoolSession.wait_for_data(...)`
2. `JobQueueClient` 查询 job 结果

会自动按当前高层约定返回可直接消费的数据结构。

## 5. 依赖补装

当前仍保持保守策略：

1. 默认严格校验
2. 只有显式传 `dependency_allowlist` 才允许节点补装
3. 安装目录位于节点 `code_cache/codes/<sha>/deps`

## 6. 兼容入口

`DedicatedTaskServiceSession`

1. 是兼容专属池实现
2. 底层复用 `ServiceGroup`
3. 适合过渡期使用

## 7. 已移除

以下共享任务池能力已经移除：

1. 旧共享任务池入口
2. 旧共享任务池流式入口
3. 旧共享任务结果拉取与取消链路
