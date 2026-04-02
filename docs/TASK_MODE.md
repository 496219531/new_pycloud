# 任务模式

## 1. 当前定位

任务模式当前保持这几个边界：

1. 任务不经过 `Gateway`
2. `InfoCenter` 只提供节点事实与热点提示
3. 高频任务链路直接走 `client -> NodeControl gRPC`
4. 结果当前只存在节点内存里

## 2. 当前接口

### 2.1 低层 gRPC

`NodeControlService` 相关任务接口：

1. `UploadCode`
2. `TaskStream`
3. `SubmitTasks`
4. `PullResults`
5. `CancelTasks`
6. `CancelJob`
7. `GetMetrics`

其中：

1. `TaskStream` 是当前推荐的任务入口
2. `SubmitTasks / PullResults` 仍保留，方便兼容和调试

### 2.2 Python helper

当前推荐优先级：

1. `TaskSubmitter` / `TaskModuleClient`
2. `TaskBatchClient`
3. `NodeControlClient.open_task_stream(...)`

上传侧现在支持可选参数：

1. `dependency_allowlist`
2. 用于显式声明“节点允许补装哪些依赖”

## 3. 标识语义

### 3.1 `task_id`

1. 单个任务唯一标识
2. 也是节点侧去重键

### 3.2 `job_id`

1. 一批任务的分组键
2. 可以跨多次提交复用
3. 主要用于等待结果、调试和 `CancelJob`
4. 不是 session，不需要 heartbeat

### 3.3 `runtime_key`

1. 表示这批任务希望复用同一热 runtime
2. 不传时通常退化为 `code_version`
3. 是任务模式热点路由和 slot 复用的关键键

### 3.4 `runtime`

`runtime` 不是 `runtime_key`。

区别：

1. `runtime`
   - Python 版本约束
   - 用于选点和节点侧校验
2. `runtime_key`
   - 热点粘性键
   - 用于 runtime slot 复用和热路由

常见写法：

1. `runtime="py3"`
2. `runtime="py3.11"`
3. `runtime=">=py3.11"`

## 4. 节点内执行模型

当前 `NodeControlState` 里的任务执行已经不是“所有代码共享一个简单进程池”，而是：

1. 按 `runtime_key` 维护 runtime slot
2. 每个 slot 自己排队
3. 每个 slot 复用一个单进程 worker
4. 节点只保留前 `K` 个活跃 slot
5. 冷 slot 等待激活
6. slot 空闲超过 `idle TTL` 自动回收

这样做的目标是：

1. 尽量少切代码
2. 尽量让热代码持续热
3. 同时避免把单个 node 撑爆

## 5. 热点路由

任务模式当前支持轻量热点路由：

1. 节点心跳会把 `active_runtimes` 同步到 `InfoCenter`
2. `InfoCenterClient.select_task_nodes(...)` 支持 `preferred_runtime_key`
3. `TaskBatchClient.from_infocenter(...)` 默认会把热点偏好带入选点
4. 客户端本地还会继续维护 runtime 粘性提示，尽量把同 runtime 回打到热 node

这是一种“轻量热点提示”机制，不是强一致调度锁。

## 6. 当前生命周期

### 6.1 推荐流程

1. task client 从 `InfoCenter` 查 node
2. 上传代码，得到 `code_version`
3. 与目标节点建立 `TaskStream`
4. 提交任务
5. 接收结果
6. 必要时 `CancelJob`

### 6.2 取消语义

`CancelJob` 当前语义：

1. `QUEUED` 任务：直接取消并发布结果
2. `RUNNING` 任务：打取消标记，等 worker 结束时转终态
3. 已终态任务：保持不变

返回统计：

1. `queued_cancelled`
2. `running_marked`
3. `already_done`
4. `not_found`

## 7. 结果语义

当前结果：

1. 按 `client_id` 保存在节点内存队列
2. 每个客户端结果队列有上限
3. 节点重启后结果丢失
4. 当前没有内建结果持久化

## 7.1 参数序列化边界

任务模式当前只额外支持这 3 种 Python 类型：

1. `pandas.DataFrame`
2. `pandas.Series`
3. `numpy.ndarray`

行为约定：

1. `client -> NodeControl gRPC` 会对这 3 种类型做显式包装
2. node 内执行用户函数前会自动还原回 `DataFrame / Series / ndarray`
3. 用户函数返回值里如果包含这 3 种类型，也会再包装后回传
4. `numpy.ndarray` 只支持简单 `dtype`
5. 其他复杂 Python 对象不支持，直接报错

报错原则：

1. 尽量在提交前或编码阶段就失败
2. 错误信息会尽量带字段路径
3. 例如会提示 `payload.bundle.bad has unsupported type ...`

建议：

1. 任务参数尽量保持为基础 JSON 结构
2. 只有确实需要时，再传 `DataFrame / Series / ndarray`
3. 更复杂对象由业务侧自己转普通结构或外部落地

## 8. 推荐用法

### 8.1 `TaskSubmitter`

```python
from pycloud_parallel import TaskSubmitter

with TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task_demo.py",
    runtime="py3",
    entry_module="task_demo",
    preferred_runtime_key="demo-runtime",
) as task:
    results = task.run(value=7, runtime_key="demo-runtime")
    print(results)

    resp = task.run.submit(value=8, runtime_key="demo-runtime")
    more = task.wait_for_results(expected_count=len(resp.accepted), timeout_sec=10.0)
```

### 8.2 `TaskBatchClient`

```python
from pycloud_parallel.controlplane.client import TaskBatchClient

with TaskBatchClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task_demo.py",
    runtime="py3",
    entry_module="task_demo",
    preferred_runtime_key="demo-runtime",
) as batch:
    batch.submit_payloads(
        [{"value": 1}, {"value": 2}],
        job_id="job-demo",
        runtime_key="demo-runtime",
    )
    results = batch.wait_for_results(job_id="job-demo", expected_count=2, timeout_sec=10.0)
```

### 8.3 低层任务流

```python
from pycloud_parallel.controlplane.client import NodeControlClient

with NodeControlClient("127.0.0.1:50061") as client:
    upload = client.upload_code_from_bytes(
        client_id="demo-client",
        filename="task_demo.py",
        blob=blob,
        runtime="py3",
        entry_module="task_demo",
    )
    with client.open_task_stream(
        client_id="demo-client",
        code_version=upload.code_version,
    ) as stream:
        stream.submit_tasks([...], job_id="job-demo")
        results = stream.pull_results(limit=10, wait_ms=500)
```

### 8.4 缺依赖时的补装

默认行为：

1. 上传校验时如果发现 `ModuleNotFoundError`
2. 直接返回错误
3. 不会自动执行 `pip install`

如果你确认允许节点补装，可以显式传：

```python
with TaskBatchClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="./task_src",
    runtime="py3",
    entry_module="task_src.main",
    dependency_allowlist=[
        "./third_party/my_local_pkg",
        "/abs/path/to/pkg.whl",
        "orjson==3.10.18",
    ],
) as batch:
    batch.submit_payloads([{"value": 1}])
```

当前约束：

1. 节点不会把 `import yaml` 自动猜成 `PyYAML`
2. 白名单会整批安装到当前 `code_version` 绑定的隔离目录
3. 同一个 `code_version` 如果换一套白名单，会被拒绝

## 9. 与服务模式的区别

服务模式：

1. owner 需要 keepalive
2. caller 通常走 `Gateway`
3. 面向 `service_name + method`

任务模式：

1. 不走 `Gateway`
2. 不需要 owner keepalive
3. 面向 `job_id / task_id / runtime_key`
4. 更关注热代码复用与流式提交

## 10. Python 版本筛选

如果你使用 `TaskSubmitter.from_infocenter(...)` 或 `TaskBatchClient.from_infocenter(...)`：

1. 客户端会先读取 `InfoCenter` 返回的节点 `python_version`
2. 再按 `runtime` 做筛选
3. 节点侧上传代码时还会再次校验

因此：

1. `runtime="py3"` 适合大多数通用任务
2. 精确版本只在你确实依赖该版本特性时使用
