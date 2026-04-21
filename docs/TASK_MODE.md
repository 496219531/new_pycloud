# 任务模式

## 1. 当前定位

当前任务侧已经收敛为两层：

1. `JobQueue Mode`
   - 大任务排队与单活调度层
   - 大任务排到后，再展开成 subtasks
2. `TaskPool Mode`
   - 子任务执行层
   - 通过 `TaskPool` 创建专属 pool 执行 subtasks

`Gateway` 不参与任务模式。

补充：

1. `TaskPool` 当前是单入口 task pool
2. 模块对象自动打包当前只会收 `.py / .pyd / .so` 文件闭包，不会把整个仓库或 package 目录树原样上传
3. `runtime_key` 仍然保留，但它表示 runtime 侧的逻辑隔离键，不再对应独立的 runtime-slot 调度器

## 2. 当前推荐入口

### 2.1 `TaskPool`

适合：

1. 直接申请一组专属 worker
2. 立即执行一批 subtasks
3. 执行结束后回收 pool

最小示例：

```python
from pycloud_parallel import TaskPool

import my_task_module

with TaskPool.open(
    target="127.0.0.1:50051",
    job_id="demo-job",
    source=my_task_module,
    runtime="py3",
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

1. `pool.methods` 当前只会返回一个方法名，也就是任务入口名
2. `entry_func` 是更直接的 callable 入口口径；`entry_callable` 是字符串名口径
3. `submit_payloads(..., task_method=...)` 可以显式传方法名，但只能等于这个单一入口
4. 如果传了别的方法名，现在会直接抛 `AttributeError`，不再静默回退到默认入口

### 2.2 `JobQueue`

适合：

1. 大任务先排队
2. 同一时刻只允许一个大任务进入运行态
3. `JobQueue` 会先向 `InfoCenter / controlplane` 查询 `job-orchestrator` route，再直连它自己的 HTTP 数据面
4. job 排到后，再自动创建 `TaskPool`

这里更适合把 `JobQueue` 理解成“排队与单活编排入口”，而不是单纯的客户端 helper。

最小示例：

```python
from pycloud_parallel import JobQueue

import my_job_module

client = JobQueue.connect("127.0.0.1:50051", client_id="job-demo")
client.submit(
    source=my_job_module,
    job_payload={"value": 10, "count": 6},
)
```

这里的 `target` 建议指向 `InfoCenter` 或带内嵌 `InfoCenter` 的 `controlplane`；`JobQueue` 会先发现唯一的 `job-orchestrator` route，再直连它。

约定：

1. `JobQueue` 的 job module 固定约定 5 个函数位：
2. `run(payload...)`
   - 必选
   - 子任务入口
3. `task_generator(...)`
   - 必选
   - 返回 payload 迭代器或 `list[dict]`
4. `update_globals(...)`
   - 可选
   - 只负责在 job-orch 端生成共享数据 `dict`
5. `handle_result(index, result, state=..., ...)` / `handle_data(...)`
   - 可选
   - 每个结果到达时增量更新状态
6. `finalize(state=..., ...)`
   - 可选
   - 产出最终 `final_result`
7. `apply_managed_globals(values, **context)`
   - 可选
   - 在 worker/node 端运行
   - 决定共享数据怎么作用到 runtime
   - `None` -> 不再默认 raw assign
   - `dict` -> 再把这个 dict 写回入口模块 A 的 globals
8. queue / pool / 并发窗口等调度细节由 `job-orchestrator` 决定，不再从 client helper 暴露
9. 节点差异靠 `tags` / healthy / runtime 过滤表达；一旦 session 建成，`effective_policy` 只由中央 profile + session context 冻结，不再和 node capability 做交集

说明：

1. `job_payload` 是可选 `dict`
2. `submit(source=my_job_module, ...)` 会自动发现并绑定 `task_generator`
3. `handle_result` / `handle_data` / `finalize` / `update_globals` 都是可选，发现到才会写进 payload
4. `apply_managed_globals` 不通过 payload 传，worker 固定按约定名在入口模块 A 中查找
5. 你也可以显式传 `update_globals=...`，支持 `dict`、callable 名称字符串，或 callable 对象
6. `JobQueue` 自己固定使用 `structured_v1 + default_safe`；如果你在 `submit(...)` 里传 `serialization_mode / policy_id`，它们会解释为未来 `TaskPool` 的执行策略

模块对象写法：

```python
client.submit(
    source=job_module,
    job_payload={"value": 10, "count": 6},
)
```

推荐优先使用 `submit(source=job_module, ...)`。
`JobQueue` 不再提供 `submit_job_from_func(...)`，避免把嵌套函数 / 闭包 / 局部依赖打包成不稳定的隐式模块。

补充边界：

1. `submit(source=module)` 当前按“已加载 module object + 真实文件”收集本地依赖
2. 自动打包只收 `.py / .pyd / .so`
3. 非 Python 资源文件不会自动进入包
4. 如果 service/taskpool/job 依赖 `.csv` 等静态资源，默认不会自动打包；可以显式传 `resource_paths=[...]`
5. 对 `JobQueue.submit(source=module, ...)`，如果 worker/taskpool 也需要这些资源，再额外传 `task_resource_paths=[...]`
6. 如果不想逐个列文件，也可以自行构建归档后再上传

兼容 helper：

1. `submit_job_from_module(...)`
2. `submit_job_from_bytes(...)`

仍然可用，但不再是文档主入口。

等待 job 终态：

```python
final = client.wait_for_terminal(job_id, timeout_sec=30.0)
print(final["job"]["status"])
```

## 3. TaskPool 能力

当前原生 `TaskPool` 已支持：

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

1. `TaskPool` 不强调 `join()` 这类 owner 常驻语义
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

## 4. Task Serialization Mode

`TaskPool` 当前也已经接入统一 transport codec pipeline。

也就是说：

1. task payload submit
2. task result decode
3. `put_data()` 生成的 `DataRef`

三者已经按同一 `serialization_mode` 语义工作。

当前支持：

1. `legacy_v1`
   - 默认兼容模式
2. `structured_v1`
   - 结构化显式 codec
3. `pickle_stable_v1`
   - 受信环境高保真 Python codec

当前选择优先级：

1. 单次提交显式 `serialization_mode=...`
2. `TaskPool` session 自己的 `serialization_mode`
3. system mode / env
4. 最终回退 `legacy_v1`

说明：

1. `TaskPool.serialization_mode` 是当前 session 的主边界
2. `submit_payloads(...)` 可以临时 override，但不会污染 session 默认值
3. `put_data()/put_json()/put_ndarray()/put_dataframe()` 不显式传 mode 时，会继承当前 `TaskPool` session mode
4. transport decode 不再靠 env 猜 mode；没有 envelope 时只按 `legacy_v1` 兜底

显式示例：

```python
with TaskPool.open(
    target="127.0.0.1:50051",
    source=my_task_module,
    serialization_mode="structured_v1",
) as pool:
    pool.submit_payloads([{"value": 1, "blob": b"abc"}])
```

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
6. 当前 `TaskPool` 的公开批量入口已经共享统一 scheduler 选点核心
   - 先决定“下一条该发给谁”
   - 再由 `TaskPool` 自己做 `max_in_flight / refill / pull_results`
   - 也就是统一“选点”，不强行统一“流控循环”
7. 当前默认 profile 是 `TASKPOOL_DEFAULT`
   - 如果以后需要显式切策略，方向会是：
     - `TASKPOOL_DEFAULT`
     - `TASKPOOL_THROUGHPUT`
   - 但公开接口名字本身不会再扩成新的产品概念

例如，如果你更关心吞吐而不是尽量压低本地 inflight，可以显式传：

```python
results = pool.map(
    values,
    strategy="taskpool_throughput",
)
```

如果你想边准备数据、边 submit、边接收结果，推荐直接用公开统一接口：

```python
for index, data in pool.unordered(
    payloads,
    max_in_flight=32,
):
    print(index, data)
```

如果你需要低层流控参数，比如 `receive_batch / result_timeout_sec / wait_ms`，请显式使用：

```python
for index, data in pool.imap_unordered(
    payloads,
    max_in_flight=32,
    receive_batch=4,
    result_timeout_sec=30.0,
):
    print(index, data)
```

如果你希望边收结果边执行收尾逻辑，也可以直接：

```python
def handle(index, result):
    print(index, result)

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
2. `unordered(...)` / `aunordered(...)`
   - 返回 `(index, result_or_none)`
   - 不再接受低层流控参数
3. `imap_unordered(...)`
   - 返回 `(index, result_or_none)`
   - 但仍保留 `receive_batch / result_timeout_sec / wait_ms` 这类低层流控能力
   - 当前 submit/requeue/infra-failure 也已经接入统一 failover 状态机
   - 局部失效节点会被禁用，后续 payload 会继续退化到健康节点
   - 公开主路径不再建立在旧 `_iter_batch_items()` 的 chunk/barrier helper 之上
4. 结果一到就立即 yield，不需要等整批任务全部结束
5. `unordered(...)` / `aunordered(...)` / `imap_unordered(...)` / `consume_unordered(...)` 运行期间会独占当前 session，不允许并发混用其他 submit/取数接口

## 4. 结果语义

当前任务结果支持：

1. 小结果 inline 返回
2. 大结果 / 文件结果 / `DataFrame` / `ndarray`
   - 落到 node 本地 `objects/`
   - 返回 `DataRef`

高层接口：

1. `TaskPool.wait_for_data(...)`
2. `JobQueue` 查询 job 结果

会自动按当前高层约定返回可直接消费的数据结构。

## 5. 依赖补装

当前仍保持保守策略：

1. 默认严格校验
2. 只有显式传 `dependency_allowlist` 才允许节点补装
3. 安装目录位于节点 `code_cache/codes/<sha>/deps`

## 6. 兼容入口

兼容专属池实现已经进入收尾迁移阶段。

最终目标保持：

1. `TaskPool` 是批量任务执行会话，不再单独宣称为“唯一执行内核”
2. `Service` 与 `TaskPool` 共享 `ExecutorHost + ExecutionSession` 底座，但保留两类兄弟会话类型
3. `JobQueue` 仍建立在 `TaskPool` 之上

## 7. 已移除

以下共享任务池能力已经移除：

1. 旧共享任务池入口
2. 旧共享任务池流式入口
3. 旧共享任务结果拉取与取消链路
