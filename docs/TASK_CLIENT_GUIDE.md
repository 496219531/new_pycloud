# 任务客户端说明

旧的共享任务池模式已经移除。

当前任务侧推荐入口：

1. `TaskPool`
   - 原生专属任务池会话
   - 适合直接创建一组专属 worker 执行 subtasks
2. `JobQueue`
   - 大任务排队入口
   - 大任务排到后，再自动创建 `TaskPool`

最小示例：

```python
from pycloud_parallel import TaskPool

import my_task_module

with TaskPool.open(
    target="127.0.0.1:50051",
    job_id="demo-job",
    source=my_task_module,
    worker_count=2,
) as pool:
    resp = pool.submit_payloads([{"value": 7}])
    results = pool.wait_for_data(expected_count=len(resp.accepted), timeout_sec=10.0)
    print(results)
```

说明：

1. `TaskPool` 当前只暴露一个任务入口，也就是 `entry_func / entry_callable`
2. 如果手动传 `task_method=...`，它必须和这个入口名一致
3. `runtime_key` 保留为 runtime 逻辑隔离键，但不再表示独立 runtime-slot

排队执行示例：

```python
from pycloud_parallel import JobQueue

import my_job_module

client = JobQueue.connect("127.0.0.1:50051", client_id="job-demo")
client.submit(
    source=my_job_module,
    job_payload={"value": 10, "count": 6},
)
```

约定：

1. `JobQueue` 的 target 应指向 `InfoCenter` 或内嵌 `InfoCenter` 的 `controlplane`
2. `JobQueue` 会先发现 `job-orchestrator` route，再直连它自己的 HTTP 数据面
3. job module 约定 6 个 hook 位：
4. `run(payload...)`
   - 必选，子任务入口
5. `task_generator(...)`
   - 必选
   - 返回 `list[dict]` 或 payload 迭代器
6. `update_globals(...)`
   - 可选
   - 只负责在 job-orch 端生成共享数据 `dict`
7. `handle_result(task_id, result, state=..., ...)` / `handle_data(...)`
   - 可选，增量更新聚合状态
8. `finalize(state=..., ...)`
   - 可选，输出最终 `final_result`
9. `apply_managed_globals(values, **context)`
   - 可选
   - 在 worker/node 端运行
   - 决定共享数据怎么作用到入口模块 A 或依赖模块 B
   - `None` -> 不再默认 raw assign
   - `dict` -> 再把这个 dict 写回入口模块 A 的 globals
10. queue / pool / 并发窗口等调度细节由 `job-orchestrator` 负责，不再从 client helper 暴露

说明：

1. `job_payload` 是可选 `dict`
2. `submit(source=my_job_module, ...)` 会自动发现并绑定 `task_generator`
3. `handle_result` / `handle_data` / `finalize` / `update_globals` 都是可选，发现到才会写进 payload
4. `apply_managed_globals` 不通过 payload 传，worker 固定按约定名在入口模块 A 中查找
5. 你也可以显式传 `update_globals=...`，支持 `dict`、callable 名称字符串，或 callable 对象

如果你直接持有模块对象，当前推荐直接走：

```python
client.submit(
    source=job_module,
    job_payload={"value": 10, "count": 6},
)
```

这里推荐直接提交模块对象；`submit_job_from_func(...)` 已移除，避免把函数对象临时拼模块带来的不稳定依赖。

兼容 helper：

1. `submit_job_from_module(...)`
2. `submit_job_from_bytes(...)`

仍然可用，但不再是文档主入口。

模块对象自动打包当前有两个关键约束：

1. 依赖分析基于“已加载 module object + 真实 `__file__`”
2. 自动打包只收 `.py / .pyd / .so`
3. `.csv / .json / README / docs` 等非 Python 文件不会自动带上
4. 如果 job 依赖非 Python 资源，请预先自己构建 `zip / tar.gz / whl`，再通过 `submit(source=archive_path)` 或 `submit_job_from_bytes(...)` 提交

如果你想本地检查自动打包产物：

```bash
python scripts/debug_package_module.py calc_asset_ratio_job_module
```

等待终态：

```python
final = client.wait_for_terminal(job_id, timeout_sec=30.0)
print(final["job"]["status"])
```

相关资料：

- [QUICK_START.md](QUICK_START.md)
- [TASK_MODE.md](TASK_MODE.md)
- [SERVICE_GUIDE.md](SERVICE_GUIDE.md)
