# Payload 路径调试说明

这份文档专门回答一个问题：

1. 一次请求里的 payload 到底走了哪条路
2. 是 inline 传输、ObjectRef 上传、ResultRef 返回，还是文件 materialize
3. 调试时应该怎么打开日志

当前已经接入一套专门的 debug logger：

1. logger 名称：`pycloud_parallel.payload_flow`
2. 级别：`DEBUG`

它的目标不是打印整包 payload，而是打印“路径摘要”。

## 1. 为什么需要这套日志

只看业务代码时，很多时候很难直观看出来：

1. `DataFrame` / `Series` 这次到底是 inline 了，还是走了 bundle 对象上传
2. 参数里的 `ObjectRef` 是否在节点侧被成功解引用
3. 返回结果是 inline dict，还是 `ResultRef`
4. 用户函数最终收到的是：
   - `fn(*args, **kwargs)`
   - `fn(**payload)`
   - `fn(payload)`

`pycloud_parallel.payload_flow` 就是为了回答这些问题。

## 2. 当前会打印哪些关键事件

已经接入的事件包括：

1. `inline_payload_encode`
   - payload 准备走 inline 序列化
2. `inline_result_encode`
   - 结果准备走 inline 返回
3. `object_ref_upload_prepare`
   - 调 `put_data()` / `put_dataframe()` / `put_ndarray()` 前，先判断对象类型
4. `object_ref_upload`
   - 确认这次要走对象上传
5. `object_ref_resolve`
   - 节点侧开始解引用 `ObjectRef`
6. `object_ref_resolved`
   - 节点侧成功把 `ObjectRef` 物化成真实对象
7. `user_invoke`
   - 用户函数调用前，记录最终参数模式
8. `result_ref_store`
   - 返回值太大或类型特殊，准备落成对象结果
9. `inline_result_ready`
   - 返回值已经确认会走 inline
10. `result_ref_fetch`
    - caller 侧开始下载 `ResultRef`
11. `result_materialize`
    - caller 侧把结果文件 materialize 成 pandas / numpy / bytes / path
12. `taskpool_create_grpc`
    - caller 侧发起 `CreateTaskPool`
13. `taskpool_create_grpc_result`
    - caller 侧收到 `CreateTaskPool` 返回
14. `taskpool_submit_grpc`
    - caller 侧发起 `SubmitPoolTasks`
15. `taskpool_submit_grpc_result`
    - caller 侧收到 `SubmitPoolTasks` 返回
16. `taskpool_submit_rpc`
    - NodeControl gRPC 服务端收到 `SubmitPoolTasks`
17. `taskpool_submit_rpc_result`
    - NodeControl gRPC 服务端返回 `SubmitPoolTasks`
18. `taskpool_submit_state`
    - NodeState 开始把 pool task 写入内部状态
19. `taskpool_submit_state_result`
    - NodeState 完成 pool task 入队
20. `taskpool_pull_results_grpc`
    - caller 侧发起 `PullPoolResults`
21. `taskpool_pull_results_grpc_result`
    - caller 侧收到 `PullPoolResults` 返回
22. `taskpool_pull_results_rpc`
    - NodeControl gRPC 服务端收到 `PullPoolResults`
23. `taskpool_pull_results_rpc_result`
    - NodeControl gRPC 服务端返回 `PullPoolResults`
24. `taskpool_pull_results_state`
    - NodeState 从 pool result hook 拉取结果
25. `task_result_report`
    - 任务执行完成后写回 NodeState

## 3. 每条日志长什么样

日志是标准 `logger.debug(...)`，格式类似：

```text
event=inline_payload_encode context=service call payload size_bytes=312 summary=DataFrame(shape=(10, 3), index=DatetimeIndex, columns=Index)
event=object_ref_upload path_type=dataframe format=dfbundle summary=DataFrame(shape=(5000, 12), index=MultiIndex, columns=Index)
event=object_ref_resolve materialize_as=dataframe summary=ObjectRef(format=dfbundle, size_bytes=123456, materialize_as=dataframe)
event=user_invoke mode=args_kwargs args_summary=list(len=3) kwargs_summary=dict(len=0, keys=[])
event=result_ref_store path_type=dataframe summary=DataFrame(shape=(80000, 20), index=RangeIndex, columns=Index)
event=result_ref_fetch format=dfbundle materialize_as=dataframe target_path=<temp> summary=ResultRef(format=dfbundle, size_bytes=456789, materialize_as=dataframe, node_id=node-1)
event=result_materialize materialize_as=dataframe format=dfbundle path=/tmp/pycloud-result-xxx.zip
event=taskpool_submit_grpc pool_id=pool-1 task_count=10 job_id=pool-job-1
event=taskpool_submit_state_result pool_id=pool-1 accepted=10 rejected=0
event=task_result_report task_id=pool-job-1-task-0001 status=SUCCEEDED result_summary=dict(len=1, keys=['value'])
event=taskpool_pull_results_grpc_result pool_id=pool-1 result_count=10 next_cursor=10
```

重点字段：

1. `event`
   - 当前走到哪一步
2. `context`
   - 这次 inline 编码属于哪类上下文
3. `summary`
   - 对象摘要，不是完整 payload
4. `materialize_as`
   - ObjectRef / ResultRef 最终按什么方式还原
5. `path_type`
   - 这次对象上传或结果落盘属于什么大类
6. `mode`
   - 用户函数最终是按哪种参数方式被调用

## 4. 怎么打开这套日志

### 4.1 调试本地 Python caller

如果你在 Python 脚本里调试 caller，最简单的是：

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logging.getLogger("pycloud_parallel.payload_flow").setLevel(logging.DEBUG)
```

这样：

1. 普通日志还是 `INFO`
2. 只有 `pycloud_parallel.payload_flow` 会输出 `DEBUG`

如果你希望连普通 HTTP 请求 debug 也一起打开，可以再加：

```python
logging.getLogger("pycloud_parallel.controlplane.client").setLevel(logging.DEBUG)
```

### 4.2 调试服务端 / Gateway / NodeControl

如果你是直接启动控制面进程，可以先用：

```bash
python -m pycloud_parallel.controlplane.server \
  --role cont \
  --bind 0.0.0.0:50051 \
  --log-level DEBUG
```

这会打开根 logger 的 `DEBUG`，所有模块都会更详细。

如果你不想把所有日志都放大，推荐在启动代码里单独设置：

```python
import logging

logging.getLogger("pycloud_parallel.payload_flow").setLevel(logging.DEBUG)
```

### 4.3 当前仓库的重启脚本

当前 [scripts/restart_services.sh](/Users/hkk/Documents/new_pycloud/scripts/restart_services.sh) 默认还是：

1. `--log-level INFO`

如果你要临时看 payload 路径，可以把那几行改成：

1. `--log-level DEBUG`

或者以后再单独加 payload logger 的环境注入。

### 4.4 task / taskpool 和 HTTP 的区别

服务调用常见入口是：

1. `GatewayConnect`
2. `GatewayServiceClient`
3. `DirectConnect`

它们会经过 HTTP transport，所以你除了 payload path 事件，还能结合普通 HTTP debug 看。

但 `TaskPoolSession` / `NativeTaskPoolClient` 走的是 gRPC，不会经过 `_http_json_request()`。

因此 task/taskpool 调试时，更应该关注：

1. `taskpool_*_grpc`
2. `taskpool_*_rpc`
3. `taskpool_*_state`
4. `task_result_report`

这几组事件。

## 5. 怎么判断 payload 走的是哪条路

### 5.1 请求参数走 inline

你会看到：

1. `event=inline_payload_encode`
2. 后面如果进入 service call，会继续看到 `event=user_invoke`

这表示：

1. payload 直接编码进 JSON / protobuf Struct
2. 没有走 `ObjectRef`

### 5.2 请求参数走对象上传

你会先看到：

1. `event=object_ref_upload_prepare`
2. `event=object_ref_upload`

之后在节点执行前，还会看到：

1. `event=object_ref_resolve`
2. `event=object_ref_resolved`

这表示：

1. caller 侧没有 inline 大对象
2. 实际传的是 `ObjectRef`
3. 节点侧执行前再物化成真实对象

### 5.3 返回结果走 inline

你会看到：

1. `event=inline_result_encode`
2. `event=inline_result_ready`

这表示：

1. 结果直接塞进响应体
2. caller 侧不会再下载 `ResultRef`

### 5.4 返回结果走 ResultRef

你会看到：

1. `event=result_ref_store`
2. caller 侧 `event=result_ref_fetch`
3. caller 侧 `event=result_materialize`

这表示：

1. 结果先落成对象文件
2. 返回的是 `ResultRef`
3. caller 再按 `materialize_as` 下载并恢复

补充：

1. 如果返回结果是 `DataFrame / Series / ndarray`，框架会先尝试 inline；超出 `PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES` 才会走本地落盘与 `ResultRef`。
2. Windows 上在高并发场景里，一旦进入落盘路径，这一步更容易受到杀毒软件、索引器或文件句柄竞争影响。
3. 如果你观察到 `service_timing` 里 `error_type=PermissionError` 且 `executor_ms` 很高，优先怀疑结果落盘竞争。
4. 这时可以优先：
   - 调大 `PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES`
   - 使用 `imap_unordered(...)`
   - 限制 `max_in_flight`

## 6. `user_invoke` 的三种模式

`event=user_invoke` 里最关键的是 `mode`。

### 6.1 `mode=args_kwargs`

表示 payload 是：

```python
{"args": [...], "kwargs": {...}}
```

最终调用：

```python
fn(*args, **kwargs)
```

### 6.2 `mode=http_kwargs`

表示 payload 是普通 HTTP 风格对象：

```python
{"x": 1, "y": 2}
```

最终调用：

```python
fn(**payload)
```

### 6.3 `mode=direct_payload`

表示 payload 不是 dict，直接原样传入：

```python
fn(payload)
```

## 7. pandas 调试时怎么读这些日志

如果你在查 `DataFrame` / `Series`，重点看 `summary` 和 `format`。

例如：

```text
summary=DataFrame(shape=(100, 5), index=MultiIndex, columns=Index)
summary=Series(len=200, index=DatetimeIndex, name='nav')
```

这能快速告诉你：

1. index 有没有保留下来
2. 是不是 `DatetimeIndex`
3. 是不是 `MultiIndex`
4. columns 是不是普通 `Index`

如果看到：

1. `event=inline_payload_encode`
2. `summary=DataFrame(...)`

说明这次 pandas 对象走的是 inline schema。

如果看到：

1. `event=object_ref_upload`
2. `path_type=dataframe`

说明这次走的是对象 bundle 上传。

当前 pandas 对象路径的统一语义是：

1. inline 路径
   - 直接编码成可传输结构
2. object/result 路径
   - `DataFrame` 使用 `dfbundle`
   - `Series` 使用 `seriesbundle`
   - bundle 内部包含：
     - `data.parquet`
     - `meta.json`

如果你在查 taskpool，推荐看这一组组合：

1. `taskpool_submit_grpc`
2. `taskpool_submit_rpc`
3. `taskpool_submit_state`
4. `user_invoke`
5. `task_result_report`
6. `taskpool_pull_results_state`
7. `taskpool_pull_results_grpc_result`

## 8. 推荐排查顺序

排查一次 payload 路径时，推荐按下面的判断顺序看日志：

1. 有没有 `inline_payload_encode`
2. 有没有 `object_ref_upload`
3. 节点侧有没有 `object_ref_resolve`
4. `user_invoke.mode` 是什么
5. 返回侧有没有 `inline_result_ready`
6. 有没有 `result_ref_store`
7. caller 侧有没有 `result_ref_fetch`
8. `result_materialize` 最终还原成了什么

排查一次 taskpool，则建议顺着：

1. `taskpool_create_grpc`
2. `taskpool_submit_grpc`
3. `taskpool_submit_rpc`
4. `taskpool_submit_state`
5. `user_invoke`
6. `task_result_report`
7. `taskpool_pull_results_state`
8. `taskpool_pull_results_grpc_result`

## 9. 当前没有接入的地方

这套日志目前重点覆盖了 payload 路径本身，还没有把每个中间 HTTP/gRPC hop 都变成 payload_flow 事件。

比如：

1. `GatewayHttpApp.handle_post()` 本身没有单独打 payload_flow 事件
2. `NodeControlService.CallService()` 目前还是普通 service log

如果后面需要更强的全链路追踪，可以继续补：

1. request id / trace id
2. gateway 收到请求时的 payload_flow 事件
3. gRPC 收到请求时的 payload_flow 事件

## 10. 建议搭配阅读

如果你在具体排查链路，建议配合：

1. [HTTP_SERVICE_DEBUG_FLOW.md](/Users/hkk/Documents/new_pycloud/docs/HTTP_SERVICE_DEBUG_FLOW.md)
2. [TASKPOOL_DEBUG_FLOW.md](/Users/hkk/Documents/new_pycloud/docs/TASKPOOL_DEBUG_FLOW.md)

前两份文档告诉你“函数链路”，这份文档告诉你“日志怎么看”。
