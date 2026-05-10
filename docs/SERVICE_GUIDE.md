# Service

`Service` 当前更适合被理解为“owner 侧部署内部常驻服务会话”的入口。

它的定位是：

1. 内部 RPC / 内部服务会话层
2. 稳定可寻址的常驻服务实例
3. 不是标准 ASGI/WSGI Web 服务运行时

如果你需要真正对外的轻网络服务，建议独立使用 `FastAPI/Flask + uvicorn/gunicorn`，再在业务层调用这里的服务会话。

它面向 owner 侧，职责是：

1. 部署服务
2. 持有 `service_token`
3. 自动 keepalive
4. 需要时 `join()` 长驻
5. 正常退出时 `EndService`
6. 可选声明 `deps=ArtifactDeps.allow_install(...)`

`Service` 的边界：

1. `Service` 是 public、discovery-aware 的 remote service
2. 运行时协议是 `call / stream`
3. 它不会改成 taskpool 的 `submit / results` 模型
4. 虽然它和 `TaskPool` 共享一些底座，但不共享 runtime protocol
5. startup service 可以复用 service route/call/status 模型，但不等于中心统一 deploy 的多副本 service

## 1. 基本用法

```python
from pycloud_parallel import Service

import my_service_module

group = Service.deploy(
    target="127.0.0.1:50051",
    owner_client_id="demo-owner",
    service_name="square-service",
    source=my_service_module,
    runtime="py3",
    worker_count=1,
    node_count=1,
)

print(group.square.sync(x=7))
```

如果服务代码依赖节点未预装的包：

```python
from pycloud_parallel.artifact import ArtifactDeps

group = Service.deploy(
    target="127.0.0.1:50051",
    owner_client_id="demo-owner",
    service_name="viewer-service",
    source="./viewer_pkg",
    runtime="py3",
    deps=ArtifactDeps.allow_install([
        "./third_party/my_local_pkg",
        "orjson==3.10.18",
    ]),
    node_count=1,
)
```

## 2. 长驻与退出

`deploy(...)` 成功后会自动开始 keepalive。

当前 owner 侧还会默认向 `stderr` 输出几类提示：

1. 部署开始
2. 部署成功
3. 部分节点部署失败
4. 无可用 node / 无可调度 node
5. keepalive 失败导致 owner 退出 `join()`

节点侧现在还会记录服务调用 timing：

1. logger：`pycloud_parallel.service_timing`
2. 聚合指标会附带在 node heartbeat metadata 中
3. InfoCenter `/ops` 服务实例表会展示：
   - `calls`
   - `errors`
   - `avg_total_ms`
   - `avg_child_decode_ms`
   - `avg_child_invoke_ms`
   - `avg_child_encode_ms`

这些 timing 的当前实现边界如下：

1. `avg_total_ms`
   - 一次 node 侧服务调用从进入 `_invoke_service_call(...)` 到准备返回响应为止的累计平均墙钟时间
2. `child_decode / child_invoke / child_encode`
   - 发生在 executor 子进程
   - `child_decode`：artifact/router 加载、managed globals 应用、payload/DataRef 解引用、方法查找
   - `child_invoke`：真正执行用户函数
   - `child_encode`：结果标准化，以及必要时转成 `StoredResultArtifact`
3. `avg_*`
   - 当前都是 service session 生命周期内的累计平均值，不是最近 N 次滑窗

owner 长驻推荐：

```python
joined = False
try:
    group.join(end_services_on_interrupt=True, end_reason="owner ctrl+c")
    joined = True
finally:
    group.close(end_services=not joined)
```

多节点标识说明：

1. `node_id`
   - 主要用于展示
   - 允许重复
2. `node_instance_id`
   - 是 service group / task pool / InfoCenter 内部使用的唯一键
   - 当你需要精确指定某一个同名节点实例时，应优先使用 `node_instance_id`
   - 动态补偿会按它记录失败副本；同一个 `node_id` 重启后如果生成新的 `node_instance_id`，会重新进入候选
   - 失效实例不能复用旧 `node_instance_id` 恢复执行状态；应清理 service/taskpool/executor/worker 后以新实例重新注册

要点：

1. keepalive 只在 owner 侧部署路径自动开启
2. `join()` 用于把 owner 进程挂住
3. `Ctrl+C` 是正常退出路径
4. 如果所有已部署 session 的 keepalive 连续失败，`join()` 会退出，并在 `stderr` 打印失败节点与原因
5. 如果部署目标数未满足，owner keepalive 会定期尝试动态补偿；失败的旧实例不会占用目标副本数
6. 如果需要动态扩容，应由同一个部署端提高 `node_count` 后重启/恢复 deploy session；快速重启会接回本 owner 已经部署的同 code version 服务，再由 keepalive 补齐新增节点

更多边界说明见：

- [SERVICE_TASKPOOL_BOUNDARY.md](SERVICE_TASKPOOL_BOUNDARY.md)

## 3. 调用体验

### 3.1 异步调用

```python
result = await group.square(x=7)
```

### 3.2 同步调用

```python
result = group.square.sync(x=7)
```

### 3.3 广播调用

```python
results = await group.square.broadcast(x=7)
```

### 3.4 通用接口

```python
result = await group.call("square", x=7)
result = group.call_sync("square", x=7)
```

### 3.4.1 默认选路口径

`Service` 当前默认已经统一到同一套 scheduler 主心智：

1. owner `Service`
2. `Service.connect(..., route="discovery")`
3. `Service.connect(..., route="gateway")`

默认都按 `predicted_busy / service_default` 这套口径选路。

如果你想显式切到更偏延迟优先的策略，可以传：

```python
result = group.call_sync("square", x=7, strategy="service_latency_first")
```

可选策略：

1. `predicted_busy`
2. `service_default`
3. `service_latency_first`
4. `least_inflight`
5. `round_robin`

### 3.5 轻量批量 RPC

connected `Service` 现在支持轻量批量 RPC 辅助能力：

```python
svc = Service.connect(
    target="127.0.0.1:50051",
    service_name="square-service",
    route="gateway",
)

results = svc.square.map([1, 2, 3], arg_name="x")
print(results)

for index, result in svc.square.unordered([{"x": 1}, {"x": 2}, {"x": 3}], max_in_flight=3):
    print(index, result)

# async 场景可选使用 amap(...) / aunordered(...)；
# 当前更推荐把它们理解成进阶能力，而不是 service 主调用路径
# results = await svc.square.amap([1, 2, 3], arg_name="x")
# async for index, result in svc.square.aunordered([{"x": 1}, {"x": 2}], max_in_flight=2):
#     ...

# 需要完整错误信息时，使用 iter_items / collect_items
# for item in svc.square.iter_items([{"x": 1}, {"x": 2}], max_in_flight=2):
#     print(item.index, item.ok, item.result, item.error_message)
```

说明：

1. `map(...)`
   - 并发发多个 RPC
   - 返回顺序与输入顺序一致
   - 某一项失败时该位置返回 `None`
   - 异步场景可选使用 `amap(...)`
2. `unordered(...)`
   - 谁先返回谁先 yield
   - yield 形状固定为 `(index, result_or_none)`
   - 同步消费用 `unordered(...)`，异步消费可选 `aunordered(...)`
3. `iter_items(...) / collect_items(...)`
   - 返回完整 `ExecutionItem`
   - 适合错误排查、重试和审计
4. 这是 **RPC 批量调用辅助能力**
5. 它不是 `TaskPool` 的任务模型，不会引入 `task_id / job_id / result cursor`

## 4. 当前导出模型

服务已经不是单入口函数模型，而是模块导出模型：

1. 传入 `source=` 模块、包或代码目录
2. 使用 `export` 标记对外方法
3. 调用时按 `method` 路由

推荐默认：

1. `Service.deploy(target=..., source=my_module)`
2. 在模块里使用 `export`

服务方法当前默认按 kwargs 调用：

1. HTTP / Gateway / Python 模块客户端传的 JSON body 会展开成关键字参数
2. 推荐服务函数写成 `def square(x=0, **_kwargs): ...`
3. 如果你需要位置参数，使用 `{"args": [...], "kwargs": {...}}` 约定

### 4.1 参数序列化边界

服务模式当前只额外支持这 3 种 Python 类型：

1. `pandas.DataFrame`
2. `pandas.Series`
3. `numpy.ndarray`

并且分两层看：

1. 小对象默认走 inline 传输
2. HTTP inline 数据面本质仍然是 `JSON/Struct`
3. 框架会把这 3 种类型自动转成可传输结构再发送
4. node 侧调用用户函数前再还原回 `DataFrame / Series / ndarray`
5. 大 `DataFrame / Series / ndarray` 会自动转 `DataRef`
6. 对于 `DataFrame / Series` 的对象路径，数据主体走 bundle：
   - `data.parquet`
   - `meta.json`
7. 这个 bundle 会保留常见 `index/columns` 语义，例如：
   - `int columns`
   - `DatetimeIndex`
   - `MultiIndex`
8. 其他复杂 Python 对象不支持，直接报错

额外限制：

1. `numpy.ndarray` 只支持简单 `dtype`
2. 更复杂的对象数组、业务自定义类实例等，不做自动兼容
3. 报错会尽量带字段路径，方便定位是哪一段 payload 不被支持

### 4.2 大结果与 `DataRef`

服务返回值当前也支持“大结果自动转引用”：

1. 小结果
   - 直接 inline 返回
2. 大结果 / 文件结果 / `DataFrame` / `ndarray`
   - 落到 node 本地 `objects/`
   - 返回 `DataRef`

高层 API：

1. `group.square.sync(...)`
2. `group.call_sync(...)`

会自动把 `DataRef` 下载并还原。

如果你明确知道返回值很大，建议服务函数返回：

1. 小摘要
2. 文件引用 / 对象引用

## 5. 常用部署参数

```python
group = Service.deploy(
    target="127.0.0.1:50051",
    owner_client_id="demo-owner",
    service_name="square-service",
    source="./service_dir",
    runtime="py3",
    worker_count=2,
    node_count=2,
    reuse_existing_same_code=True,
    replace_existing_if_code_changed=True,
)
```

语义：

1. 同 `owner_client_id + service_name + code_version` 且仍在同一 owner 控制域内时可复用
2. 同名但代码变化时，如果旧服务仍在运行，会直接拒绝
3. 要更新同名服务，先结束旧服务，再重新部署
4. 客户端会本地缓存 `service_id/service_token`，便于 owner 进程重启后恢复自己的部署会话
5. 同一台机器上，同一个 `owner_client_id + service_name` 只允许一个活跃 deployservice 持有该本地 session cache 锁
6. 如果 node 断开后以新的 `node_instance_id` 重连，它必须重新接受 owner 部署，不能用旧 service 进程冒充仍在同一发布批次内
7. startup service 由启动它的进程自行管理；即使 `service_name/code_version` 相同，也不能被动态 `Service.deploy(...)` owner 复用、接管或作为扩容副本加入，因为它不在该 owner 的版本管控、回滚、keepalive 与 close 闭环内
8. 如果 startup service 传入 `target` 并注册到 `InfoCenter`，它会参与 `service_name` 排他检查：动态服务已经占用同一个 `service_name` 时，startup service 必须拒绝启动
9. 反过来，已注册 startup service 存在时，动态 deploy 也必须拒绝，不能因为 code version 一致而合并为一个服务组
10. 如果 `Service.startup(target="")` 或不传 `target`，它是 startup 专属的未注册模式：不注册 `InfoCenter`，不参与全局 `service_name` 排他，可以在不同端口启动多个同名 startup service；这种实例不会被 `InfoCenter` / Gateway 自动发现，外部进程需要直连对应本地 service HTTP 地址，本进程内仍使用 `startup.foo.sync(...)` 直调本地 executor
11. `Service.startup(...)` 主推 `source=已 import 的 module`。startup 是本机部署语义，不是远程 deploy；module source 默认走本地 module mount，不做远程代码 upload，也不把 module 重新打包成远端 artifact。运行配置建议通过 `managed_global_names` + `update_globals(...)` 注入，不推荐用 `cwd` / `os.environ` 这类进程全局状态承载单个服务配置。
12. 空 `target` 不表示通用 local 模式。`Service.deploy(...)`、`Service.connect(...)`、`TaskPool.open(...)` 仍然必须显式传入 `target`
13. `Service.startup(...).foo.sync(...)` 在 startup 的非 local 模式和 local 模式下都是当前 startup node 对自己挂载服务的本地调用门面：本进程内 proxy 直接调用 `StartupServiceNode.call_service(...)`，进入本地 executor 队列和 worker，不经过 Discovery、Gateway 或 service HTTP；它只是调用便利，不表示 startup service 加入动态服务组
14. `target="local"` 是显式本地 IPC 模式：`Service.startup(target="local", ...)` 和 `Service.deploy(target="local", ...)` 在底层基本一致，都会创建本进程持有的 `StartupServiceNode`、返回本地 proxy、按 `service_name` 写入本机 IPC registry；同名 local service 已存活时启动会失败，`Service.connect(target="local", service_name=...)` 通过该 registry 连接到对应本地服务
15. local 模式保留与远端 service 基本一致的用户侧调用形态：`service.foo.sync(...)`、`await service.foo(...)`、`service.foo.stream(...)`、`service.foo.broadcast(...)`、`Service.connect(...)` 的方法代理语义不变；local/connected service 的 broadcast 按单节点处理，即执行一次并返回单元素结果列表；需要在本地 IPC 与远端 ControlPlane/InfoCenter 之间切换时，通常只改 `target`，业务调用代码基本无感
16. `Service.connect(...)` 不论 local 还是 remote，都只是调用端 client，不暴露 `update_globals(...)`；`update_globals(...)` 是 owner/control 能力，只能在 `Service.startup(...)` 或 `Service.deploy(...)` 返回的 owner handle 上使用
17. `TaskPool.open(target="local", ...)` 创建 opener 私有的本地 task pool，任务直接提交到当前进程持有的 NodeControl runtime；`unordered(...)`、`aunordered(...)`、`iter_items(...)` 等高层 wrapper 复用同一套 session 逻辑；TaskPool 没有 connect 语义，其他进程不能接入这个 pool
18. local TaskPool 是单机私有 pool；如果本地 worker/pool 失效，语义是快速失败并由 opener 决定是否重建，不做跨节点 accepted-task replay，也不假装可以切换到其他节点
19. Windows named pipe、spawn 模式和 Ctrl+C 清理属于 local runtime 的平台体验项，需要在 Windows 实机压测；非 Windows 单测只覆盖本机 IPC registry、普通调用、stream、DataRef、managed globals 和 close 主路径
20. 动态扩容应走同一个 owner 的 deploy session：扩大 `node_count` 并重启/恢复部署端，由缓存的 `service_id/service_token` 接回旧副本，再补齐新副本

## 5.1 依赖补装语义

当前策略：

1. 默认严格校验，缺依赖直接失败
2. 只有显式传 `deps=ArtifactDeps.allow_install(...)` 才允许节点补装
3. 节点把依赖安装到当前 `code_version` 的隔离目录
4. 运行时调用服务方法时，也会把该依赖目录加入 `sys.path`
5. 同一个 `code_version` 不允许混用不同依赖策略

## 5.2 managed globals

服务模式现在支持声明可动态更新的全局变量：

```python
group = Service.deploy(
    target="127.0.0.1:50051",
    service_name="square-service",
    source=blob,
    package_format="py",
    initial_globals={"STATE": "v1"},
)

group.update_globals({"STATE": "v2"})
```

规则：

1. 只有 owner 持有 `service_token`，所以只有 owner 能更新
2. `initial_globals={...}` 是创建期同步注入：node 先写入 globals，再把 service replica 放进可见状态；如果未显式传 `managed_global_names`，会从 `initial_globals` 的 key 自动补齐声明名
3. `update_globals(...)` 是创建后的 owner 控制命令，语义是异步热更新，不阻塞 service 对外可见；如果调用方依赖某个 global 一定已存在，应在创建时传 `initial_globals`
4. 当前版本是按 `service_id` 定义的
5. 同一套代码的不同服务实例可以有不同 globals

## 5.3 节点目录布局

当前 node 目录布局：

```text
artifact_dir/
  codes/
    <code_sha>/
      artifact.py | pkg/
      deps/
      scopes/
        service/<scope_hash>/
      meta.json
  objects/
    <object_sha>.<fmt>
    meta/<object_sha>.json
```

说明：

1. `codes/<code_sha>/`
   - 一套代码作用域目录
2. `scopes/service/...`
   - 这套代码下的服务级 managed globals
3. `objects/`
   - 大对象与大结果缓存

## 5.4 GC

服务模式文件回收当前推荐走离线命令：

```bash
pycloudctl gc --scope codes --older-than-hours 168 --dry-run
pycloudctl gc --scope all --older-than-hours 168
```

当前规则：

1. `codes`
   - 超过阈值删除整个 `codes/<code_sha>/`
2. `objects`
   - 被当前 globals 引用对象保留
   - 其余对象按 `last_at` 超时删除
3. `all`
   - 先删 `codes`
   - 再删 `objects`

## 6. 节点选择

如果不显式传 `node_ids`，部署时会：

1. 从 `InfoCenter` 查询节点
2. 过滤 `healthy=false`
3. 过滤 `schedulable=false`
4. 过滤 `drain=true`
5. 过滤 `accept_service_deploy=false`
6. 如果指定了 `runtime`，先按节点 `python_version` 过滤
7. 按 `service_worker_available` 选节点
8. 部署后如果新 node 加入或旧 node 重启为新实例，owner 会在 keepalive 后台尝试补齐目标副本数
9. 创建失败或 host 失败的 service 会在 InfoCenter `/ops` 的 `failure_reason` 中显示原因

这里的“补齐”是重新部署，不是信任 node 上残留的旧 service 状态。重连节点必须以新的 `node_instance_id` 进入 owner 的发布控制域后，才算当前部署的一部分。

补充：

1. `drain` 节点不接新 service 调用，但 owner 命令仍应能到达，例如 `update_globals`、`close`、`shutdown`。
2. `cordon` 节点不接新部署；已有 RUNNING 服务是否继续路由由 `drain` 决定。
3. 排他性部署和版本冲突检查要看 drain/cordon 节点上的已有服务，不能只看当前可路由服务。

## 7. 与轻量 caller 的区别

`Service`：

1. 是 owner
2. 会上传代码
3. 会创建服务
4. 会 keepalive
5. 可以 `end()` 服务

轻量 caller：

1. 只是 caller
2. 不上传代码
3. 不持有 token
4. 不管理服务生命周期

## 8. 何时用更底层 controlplane caller

如果你只是想调已有服务，优先用 V1 的 `Service` owner/caller 组合。

只有在这些场景才更适合从 `pycloud_parallel.controlplane` 使用更底层客户端：

1. 调试具体实例
2. 旁路 Gateway
3. 客户端本地自己维护 route cache
