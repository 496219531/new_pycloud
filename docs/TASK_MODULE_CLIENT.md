# 任务客户端说明

旧的共享任务池模式已经移除。

当前任务侧推荐入口：

1. `TaskPoolSession`
   - 原生专属任务池会话
   - 适合直接创建一组专属 worker 执行 subtasks
2. `JobQueueClient`
   - 大任务排队入口
   - 大任务排到后，再自动创建 `TaskPoolSession`
3. `DedicatedTaskServiceSession`
   - 兼容专属池实现
   - 底层复用 `ServiceGroup`

最小示例：

```python
from pycloud_parallel import TaskPoolSession

with TaskPoolSession.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    job_id="demo-job",
    blob=blob,
    entry_module="task_demo",
    entry_callable="run",
    worker_count=2,
) as pool:
    resp = pool.submit_payloads([{"value": 7}])
    results = pool.wait_for_data(expected_count=len(resp.accepted), timeout_sec=10.0)
    print(results)
```

说明：

1. `TaskPoolSession` 当前只暴露一个任务入口，也就是 `entry_callable`
2. 如果手动传 `task_method=...`，它必须和这个入口名一致
3. `runtime_key` 保留为 runtime 逻辑隔离键，但不再表示独立 runtime-slot

排队执行示例：

```python
from pycloud_parallel import JobQueueClient

client = JobQueueClient("127.0.0.1:50052", client_id="job-demo")
client.submit_job_from_bytes(
    blob=job_blob,
    entry_module="job_demo",
    job_payload={"value": 10, "count": 6},
)
```

约定：

1. `JobQueueClient` 固定要求 job module 导出 `run / task_generator / handle_result / finalize`
2. queue / pool / 并发窗口等调度细节由 `job-orchestrator` 负责，不再从 client helper 暴露

如果你直接持有模块对象：

```python
client.submit_job_from_module(
    module=job_module,
    job_payload={"value": 10, "count": 6},
)
```

这里推荐直接提交模块对象；`submit_job_from_func(...)` 已移除，避免把函数对象临时拼模块带来的不稳定依赖。

等待终态：

```python
final = client.wait_for_terminal(job_id, timeout_sec=30.0)
print(final["job"]["status"])
```

相关资料：

- [QUICK_START.md](QUICK_START.md)
- [TASK_MODE.md](TASK_MODE.md)
- [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
