# TaskPool 调试链路

这份文档总结 `TaskPoolSession` 的任务从发出到结果返回的关键函数。

适合排查：

1. task pool 创建失败
2. 任务提交成功但没执行
3. 结果拉不到
4. 结果类型不对
5. pool token / node pool 轮转问题
6. `task_method` 校验失败
7. managed globals warmup / worker pid 日志

## 1. 常见入口

当前更常见的入口是：

1. `TaskPoolSession`
   - 原生 task pool 会话
2. `NativeTaskPoolClient`
   - 单节点 pool 的低层 gRPC client

当前语义先记住两点：

1. 原生 `TaskPoolSession` 是单入口模式
   - `methods == [entry_callable]`
   - 不支持像 Service 那样在一个 pool 里导出多个方法再路由
2. `runtime_key` 只是 runtime 逻辑隔离键
   - 不再对应单独的 runtime-slot 调度链路

从高层视角看，典型调用顺序是：

1. 创建 pool
2. `submit_payloads(...)` 或 `map(...)`
3. `wait_for_results(...)` 或 `wait_for_data(...)`

关键位置：

1. `src/pycloud_parallel/controlplane/client.py`
2. `TaskPoolSession` 在 `2087` 左右
3. `NativeTaskPoolClient` 在 `1680` 左右

## 2. 创建 TaskPool

创建 pool 的高层链路一般是：

1. `NodeControlClient.create_task_pool_from_bytes()`
2. gRPC `CreateTaskPool`
3. `NodeControlState.create_task_pool()`

创建时做的事：

1. `put_code(...)` 上传并缓存代码
2. `_ensure_artifact_ready(...)`
3. 分配 `pool_id`
4. 生成 `pool_token`
5. 在 executor host 里创建 task pool worker 容器
6. 注册 `TaskPoolState`

关键位置：

1. `client.py` 中 `create_task_pool_from_bytes()` 在 `3560` 左右
2. `state.py` 中 `create_task_pool()` 在 `2788` 左右

如果 pool 根本起不来，优先看这里。

## 3. 高层提交任务

高层 caller 常走：

1. `TaskPoolSession.submit_payloads()`
2. `TaskPoolSession.submit_values()`
3. `TaskPoolSession.map()`

其中 `submit_payloads()` 做的事：

1. 为每个 payload 生成 `task_id`
2. `serialize_inline_payload(payload, context="task pool payload")`
3. 先校验 `task_method` 是否等于当前唯一入口方法
4. `_select_pool_node()` 选择一个 node pool
5. 按节点分组
6. 调每个底层 `NativeTaskPoolClient.submit_tasks(...)`

关键位置：

1. `TaskPoolSession.submit_payloads()` 在 `2155` 左右
2. `_select_pool_node()` 在 `2147` 左右

这里最适合排查：

1. payload 在提交前是否已被正确序列化
2. 任务是否按预期轮转到不同节点
3. task_id 是否重复
4. `task_method` 是否写成了不是 `entry_callable` 的别名

## 4. gRPC 提交到 NodeControl

低层提交流程是：

1. `NativeTaskPoolClient.submit_tasks(...)`
2. gRPC `SubmitPoolTasks`
3. `NodeControlState.submit_pool_tasks()`

`submit_pool_tasks()` 做的事：

1. 校验 `pool_id`
2. 校验 `pool_token`
3. 校验 pool 是否 `RUNNING`
4. 校验 `code_version`
5. 把 `item.payload` 用 `struct_to_dict(...)` 反序列化
6. 创建 `TaskState`
7. `executor_host.submit_pool_task(...)`
8. 给 caller 返回 `TaskAccepted` / `TaskRejected`

注意：

1. pool 任务最终还是按 `artifact.entry_callable` 执行
2. 也就是说，gRPC 提交层没有额外 method 路由
3. `task_method` 的意义主要是高层 API 早失败校验，而不是在节点侧二次分发

关键位置：

1. `client.py` 中 `submit_pool_tasks()` 在 `3620` 左右
2. `state.py` 中 `submit_pool_tasks()` 在 `2854` 左右

常见故障：

1. `pool_token mismatch`
2. `task pool not running`
3. `duplicate task_id`
4. `code artifact missing`

## 5. 节点内部执行

任务提交进 pool 后，会进入 executor host，再进入真正的用户函数执行链路。

核心执行链路可以理解成：

1. `executor_host.submit_pool_task(...)`
2. 生成 `execute_spec`
3. `_execute_payload_in_subprocess(...)`
4. `_resolve_object_refs_in_payload(...)`
5. `_invoke_user_callable(...)`
6. `_normalize_user_return(...)`

这部分和 service call 的“用户函数执行链”基本是同一套。

所以如果你看到：

1. 任务提交成功但函数没跑
2. payload 进函数后长得不对
3. 返回结果被包装成了别的类型

直接去看：

1. `state.py` 中 `_execute_payload_in_subprocess()`
2. `state.py` 中 `_invoke_user_callable()`

如果问题出在 managed globals 更新后“第一次调用变慢”或“warmup 看起来没生效”，还可以看：

1. `update_service_globals()` / `update_runtime_globals()`
2. `executor_host.warmup_service()` / `warmup_pool()` / `warmup_runtime()`
3. warmup 日志里的 `worker_pids`

## 6. 结果写回 NodeState

任务执行完成后，结果最终回到：

1. `NodeControlState.report_result()`

这里负责：

1. 更新任务状态
2. 成功时 `task.result = struct_to_dict(request.result)`
3. 失败时记录 `error_type` / `error_message`
4. 调 `_publish_result_locked(task)` 把结果推到 result hook

关键位置：

1. `state.py` 中 `report_result()` 在 `3417` 左右

如果“任务明明执行完成，但 caller 拉不到结果”，先看这里有没有 publish。

## 7. 结果拉取

高层 caller 拉结果一般走：

1. `TaskPoolSession.wait_for_results()`
2. 内部轮询每个 `NativeTaskPoolClient.pull_results(...)`
3. `NodeControlState.pull_pool_results()`
4. `_pool_result_hook.pull(...)`

高层返回 data 则是：

1. `TaskPoolSession.wait_for_data()`
2. `_resolve_task_results_data(...)`

关键位置：

1. `TaskPoolSession.wait_for_results()` 在 `2194` 左右
2. `TaskPoolSession.wait_for_data()` 在 `2220` 左右
3. `NativeTaskPoolClient.pull_results()` 在 `1702` 左右
4. `NodeControlState.pull_pool_results()` 在 `2921` 左右

当前 `TaskPoolSession.wait_for_results()` 的特点：

1. 轮询所有 node pool
2. 用 `seen` 去重 task_id
3. 满足 `expected_count` 就提前返回

所以如果结果数量不对，先看：

1. `expected_count` 是否合理
2. 某个 pool 是否没被轮询到
3. 某个任务是否根本没 publish 进 pool result hook

## 8. `map()` 的最短链路

如果你走的是：

```python
results = pool.map(values, timeout_sec=...)
```

关键函数顺序就是：

1. `TaskPoolSession.map()`
2. `TaskPoolSession.submit_values()`
3. `TaskPoolSession.submit_payloads()`
4. `NativeTaskPoolClient.submit_tasks()`
5. `NodeControlState.submit_pool_tasks()`
6. executor host 执行
7. `NodeControlState.report_result()`
8. `TaskPoolSession.wait_for_data()`

这是最适合调试的一条主线。

## 9. 建议的断点顺序

排查一次 TaskPool，建议按这个顺序下断点：

1. `TaskPoolSession.submit_payloads()`
2. `NativeTaskPoolClient.submit_tasks()`
3. `NodeControlState.submit_pool_tasks()`
4. `_execute_payload_in_subprocess()`
5. `_invoke_user_callable()`
6. `NodeControlState.report_result()`
7. `TaskPoolSession.wait_for_results()`
8. `NodeControlState.pull_pool_results()`

这样很快就能判断问题是在：

1. payload 提交前
2. pool token / state 校验
3. 用户函数执行
4. 结果 publish
5. caller 轮询聚合

## 10. 和普通 Task 流的区别

TaskPool 和普通 `SubmitTasks/PullResults` 的主要差异在于：

1. client_id 不再是普通 caller id，而是 `pool_id`
2. `submit_pool_tasks()` 会把任务塞进 `_pool_tasks`
3. `pull_pool_results()` 走 `_pool_result_hook`
4. 高层 `TaskPoolSession` 自己做多 pool 轮询和去重
5. 普通 task 的 `runtime_key` 仍然存在，但它只是共享 runtime executor 的逻辑 key，不是 slot 资源对象

所以排查时不要把它和普通 `Task Mode` 的 `_tasks/_result_hook` 完全混在一起看。
