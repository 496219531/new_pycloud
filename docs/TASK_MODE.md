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

补充：

1. `TaskPoolSession` 当前是单入口 task pool
2. 整个 module / package 会一起上传，但真正对外暴露的任务入口只有 `entry_callable`
3. `runtime_key` 仍然保留，但它表示 runtime 侧的逻辑隔离键，不再对应独立的 runtime-slot 调度器

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

说明：

1. `pool.methods` 当前只会返回一个方法名，也就是 `entry_callable`
2. `submit_payloads(..., task_method=...)` 可以显式传方法名，但只能等于这个单一入口
3. 如果传了别的方法名，现在会直接抛 `AttributeError`，不再静默回退到默认入口

### 2.2 `JobQueueClient`

适合：

1. 大任务先排队
2. 同一时刻只允许一个大任务进入运行态
3. 默认经 `gateway -> 唯一 job-orchestrator -> TaskPoolSession`
4. job 排到后，再自动创建 `TaskPoolSession`

最小示例：

```python
from pycloud_parallel import JobQueueClient

client = JobQueueClient("127.0.0.1:50052", client_id="job-demo")
client.submit_job_from_bytes(
    blob=job_blob,
    entry_module="job_demo",
    job_payload={"value": 10, "count": 6},
)
```

这里的 `target` 建议指向 `gateway`；`job-orchestrator` 会通过 `infocenter` 暴露成唯一 service route。

约定：

1. `JobQueueClient` 固定要求 job module 导出 `run / task_generator / handle_result / finalize`
2. queue / pool / 并发窗口等调度细节由 `job-orchestrator` 决定，不再从 client helper 暴露

模块对象写法：

```python
client.submit_job_from_module(
    module=job_module,
    job_payload={"value": 10, "count": 6},
)
```

推荐优先使用 `submit_job_from_module(...)`。
`JobQueueClient` 不再提供 `submit_job_from_func(...)`，避免把嵌套函数 / 闭包 / 局部依赖打包成不稳定的隐式模块。

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
8. `is_alive()`
9. `failed / failures`
10. `iter_results(...) / collect_results(...)`
11. `iter_data(...) / collect_data(...)`
12. `iter_items(...) / collect_items(...)`

补充说明：

1. `TaskPoolSession` 不强调 `join()` 这类 owner 常驻语义
2. 更推荐的使用方式是：
   - 用 `submit_payloads(...)` 持续发任务
   - 用 `wait_for_results(...)` / `wait_for_data(...)` 持续收结果
   - 收完后主动 `close()`
3. 当前 keepalive 已按 node 独立降级：
   - 单个 node pool 心跳失败时，会记录到 `failures`
   - 只要还有别的 active node pool，session 仍可继续使用
   - 所有 active node pool 都失效时，session 才会进入 `failed=True`
4. `runtime_key` 的作用主要是：
   - 作为 runtime 侧 managed globals 的作用域键
   - 作为节点活跃 runtime 统计的聚合键
   - 不表示“为该 key 单独常驻一个 runtime-slot”

如果你想边到边处理结果，而不是等一批结果都回来再统一处理：

```python
for item in pool.iter_results(max_count=10, timeout_sec=10.0):
    print(item.task_id, item.status)
```

如果你想直接拿已经物化好的结果数据：

```python
for task_id, data in pool.iter_data(max_count=10, timeout_sec=10.0):
    print(task_id, data)
```

说明：

1. 默认 `raise_on_error=False`
2. 失败结果会返回 `(task_id, None)`
3. 如果你希望遇到失败立即抛异常，可显式传 `raise_on_error=True`

如果你只是想提交单个任务并拿到这次提交的 `task_id`：

```python
task_id = pool.run(value=7)
print(task_id)
```

如果你想保留原来“提交并直接拿结果”的语义，用：

```python
result = pool.run.sync(value=7)
print(result)
```

注意：

1. `run.sync(...)` 启动前要求当前 session 没有历史未收结果
2. `imap_unordered(...)` 也要求当前 session 是干净的
3. 如果你之前已经异步提交过任务，需要先把结果接收干净，再进入这两种独占模式

如果你不想自己迭代，也可以直接收集：

```python
results = pool.collect_results(max_count=10, timeout_sec=10.0)
items = pool.collect_data(max_count=10, timeout_sec=10.0)
```

如果你既想保留流式处理，又不希望单条失败直接打断整批，可以用：

```python
for item in pool.iter_items(timeout_sec=10.0):
    if item.ok:
        print(item.task_id, item.data)
    else:
        print(item.task_id, item.error_type, item.error_message)
```

说明：

1. `max_count=None` 是默认语义
2. 表示“这次等待当前已提交但尚未返回的结果全部回完，或直到超时”
3. 如果传整数 `N`，表示“本次最多接收 `N` 条结果后就结束这次阻塞”
4. `iter_data(...) / collect_data(...)` 遇到失败结果会抛异常
5. `iter_items(...) / collect_items(...)` 会把成功/失败都显式返回给你

如果你想边准备数据、边 submit、边接收结果，推荐直接用：

```python
for task_id, data in pool.unordered(
    payloads,
    max_in_flight=32,
    receive_batch=4,
    result_timeout_sec=30.0,
):
    print(task_id, data)
```

如果你希望边收结果边执行收尾逻辑，也可以直接：

```python
def handle(task_id, result):
    print(task_id, result)

processed = pool.consume_unordered(
    payloads,
    handle=handle,
    max_in_flight=32,
    receive_batch=4,
    result_timeout_sec=30.0,
)
print(processed)
```

语义：

1. `max_in_flight`
   - 最多同时保留多少个已提交但未返回的任务
2. `receive_batch`
   - 每轮最多接收多少条结果
3. 结果一到就立即 yield，不需要等整批任务全部结束
4. `unordered(...)` / `imap_unordered(...)` / `consume_unordered(...)` 运行期间会独占当前 session，不允许并发混用其他 submit/取数接口

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
4. owner 侧也支持 `update_globals(...)`，复用 `ServiceGroup` 的 managed globals 更新链路

## 7. 已移除

以下共享任务池能力已经移除：

1. 旧共享任务池入口
2. 旧共享任务池流式入口
3. 旧共享任务结果拉取与取消链路
