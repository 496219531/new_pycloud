# 任务模式

## 1. 当前接口

任务模式保留在 `NodeControl gRPC` 上，核心接口是：

1. `UploadCode`
2. `SubmitTasks`
3. `PullResults`
4. `CancelTasks`
5. `CancelJob`
6. `GetMetrics`

边界上需要特别注意：

1. 任务模式不经过 `Gateway`
2. `controlplane` 只提供节点事实，不代理任务提交
3. task client 先查 `InfoCenter`，再自己直连目标 `NodeControl`

## 2. 标识语义

### 2.1 `task_id`

1. 标识单个任务
2. 仍然是幂等和去重的主键

### 2.2 `job_id`

1. 标识一批任务
2. 可跨多个 `SubmitTasks` 请求复用
3. 不是心跳 session
4. 主要用于批量取消和调试观察

## 3. 生命周期

### 3.1 提交流程

1. task client 先从 `InfoCenter` 查询节点事实
2. 客户端自行选择目标 node
3. 上传代码，得到 `code_version`
4. `SubmitTasks(client_id, code_version, tasks, job_id)`
5. 节点放入共享任务队列
6. 本地多进程执行
7. 通过 `PullResults` 拉取结果

也就是说：

1. `InfoCenter` 不做 task proxy
2. `Gateway` 也不做 task proxy
3. 高频任务链路始终保持为 `client -> NodeControl gRPC`

### 3.2 取消流程

1. 单任务取消：`CancelTasks`
2. 批次取消：`CancelJob`

`CancelJob` 的当前语义：

1. `QUEUED` 任务直接取消
2. `RUNNING` 任务只标记 `cancel_requested`
3. 终态任务保持不变

返回计数：

1. `queued_cancelled`
2. `running_marked`
3. `already_done`
4. `not_found`

其中 `not_found=1` 表示没有找到这个 `client_id + job_id` 对应的任何任务。

## 4. 状态语义

当前任务状态保持为：

1. `QUEUED`
2. `RUNNING`
3. `SUCCEEDED`
4. `FAILED_USER`
5. `FAILED_INFRA`
6. `CANCELLED`

解释：

1. `FAILED_USER`
   - 用户代码抛错
   - 输入问题
   - 不自动重试
2. `FAILED_INFRA`
   - 进程崩溃
   - 心跳超时
   - 系统故障
   - 可按节点策略重试

## 5. 结果保留

当前结果保存在内存里：

1. 结果按 `client_id` 分队列
2. 每个客户端结果数有上限
3. 节点重启后结果会丢失

这是一种有意保持轻量的设计，不引入额外存储依赖。

## 6. 与服务模式的区别

服务模式：

1. owner client 创建服务并 `join()` 长驻
2. caller 通过 `service_name` 调 `ControlPlane Gateway`
3. Gateway 再转到某个 `NodeControl` 上的 `service_id` 实例

任务模式：

1. 没有服务心跳
2. 没有 Gateway
3. 直接面向 `NodeControl gRPC`
4. `job_id` 只是任务分组键，不是 session
## 7. 示例脚本

可直接运行：

```bash
python scripts/grpc_task_client_demo.py
```

如果使用 Python helper，推荐走：

```python
from pycloud_parallel.controlplane.client import TaskBatchClient

with TaskBatchClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    client_id="task-client-demo",
    job_id="job-demo",
    blob=blob,
    filename="task_demo.py",
    entry_module="task_demo",
) as batch:
    batch.submit_payloads([{"value": 1}, {"value": 2}], job_id="job-demo")
    results = batch.wait_for_results(job_id="job-demo", expected_count=2, timeout_sec=5.0, wait_ms=500)
```
