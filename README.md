# pycloud-parallel

`pycloud-parallel` 当前定位：

1. 主功能：多节点/跨集群任务与服务调度（ControlPlane + NodeControl）
2. 辅功能：本地多进程并行 API（轻量兜底）

更细一点的边界建议这样理解：

1. `Task Mode`
   - 面向子任务执行层
   - 更适合 CPU 密集型子任务、批量任务分发、长耗时数据处理
2. `JobQueue Mode`
   - 面向大任务排队与单活调度
   - 大任务排到后，再展开成 subtasks 交给执行层
3. `Service Mode`
   - 面向可寻址、常驻的函数服务实例
   - 更适合内部 RPC、轻量状态服务、稳定路由的函数调用
   - 当前本质仍是“函数执行服务”，不是标准 ASGI/WSGI 网络服务运行时
4. `External Web Layer`
   - 如果需要真正的轻网络服务，建议独立使用 `FastAPI/Flask + uvicorn/gunicorn`
   - 该层负责 HTTP API、鉴权、参数校验、编排与聚合
  - 重计算下沉到 `JobQueue Mode + TaskPool`，内部函数调用下沉到 `Service Mode`

## 安装

默认安装即包含集群控制面所需依赖：

```bash
pip install pycloud-parallel
```

## 当前架构

当前默认部署形态：

1. `ControlPlane = InfoCenter + Gateway`，对外 `HTTP + JSON`
2. `NodeControl`，对外 `gRPC`
3. 服务实例数据面，节点内 `HTTP + JSON`

可以把这三层关系理解成：

1. `uvicorn/gunicorn`
   - 对外轻网络入口层
2. `Service Mode`
   - 内部常驻函数服务层
3. `JobQueue Mode`
   - 大任务排队与单活调度层
4. `TaskPool / Task Mode`
   - 子任务执行层（唯一执行内核）

## Payload 序列化边界

当前运行时数据流只额外支持很窄的一组 Python 类型：

1. `pandas.DataFrame`
2. `pandas.Series`
3. `numpy.ndarray`

其中：

1. `HTTP` 调服务时，底层仍然是 `JSON`
2. 框架会把上面 3 种类型自动转成简单 JSON 结构
3. 复杂对象不会自动兼容，直接报错
4. `gRPC` 任务/服务控制面会对这 3 种类型做显式包装与还原
5. `numpy.ndarray` 只接受简单 `dtype`：数值 / bool / 字符串

如果业务参数里有更复杂的 Python 对象，当前建议用户自己先转成普通 JSON 结构，或落到外部存储后只传引用。

## 大文件与缓存

当前 node 侧缓存目录已经按代码与对象分开：

```text
artifact_dir/
  codes/
    <storage_key>/
      artifact.py | pkg/
      deps/
      scopes/
        service/<scope_hash>/
        runtime/<scope_hash>/
      meta.json
  code_index/
    <entry_module>__<entry_callable>__<short_code>.meta.json
    <entry_module>__<entry_callable>__<short_code> -> ../codes/<storage_key>
  objects/
    <sha_prefix>/<object_sha>.<fmt>
    meta/<object_sha>.json
    segments/
    materialized/
```

结果说明：

1. `codes/<storage_key>/`
   - 一套代码的实际缓存目录
   - 目录名是稳定存储键，不直接暴露长 `code_version`
   - 包含代码本体、补装依赖、managed globals scope 状态与 `meta.json`
2. `code_index/`
   - 人类可读索引目录
   - 每个条目都用 `entry_module + entry_callable + 短 code 标识` 命名
   - 索引名本体是一个可直接打开的链接，指向真实 `codes/<storage_key>/`
   - 同名 `.meta.json` 会记录 `code_version`、真实目录、artifact path 等辅助信息
3. `objects/`
   - 大对象与大结果缓存
   - `meta/<object_sha>.json` 里记录 `created_at`、`last_at` 与存储后端
   - 较大的结果可能会复用 `segments/` 做分段落盘

如果你想找某份缓存代码，优先看索引而不是直接进 `codes/`：

```bash
pycloudctl cache-list
pycloudctl cache-list --match calc_asset_ratio
open code_cache/code_index/<entry_module>__<entry_callable>__<short_code>
```

### 结果返回机制

TaskPool / Service 两条执行链路的结果返回当前是自动分流的：

1. 小结果
   - 直接 inline 回传
2. 大结果 / 文件结果 / `DataFrame` / `ndarray`
   - 落到 node 本地 `objects/`
   - 返回 `DataRef`

高层 Python API 会自动帮你下载并还原 `DataRef` 指向的大结果。

如果你明确知道结果会很大，建议业务侧主动返回“小摘要 + 对象引用”，不要依赖超大 inline 返回。

### GC 约定

当前推荐使用离线命令做 GC，而不是把 GC 挂进常驻服务进程：

```bash
pycloudctl cache-list --match demo
pycloudctl gc --dry-run
pycloudctl gc --scope codes --older-than-hours 168
pycloudctl gc --scope objects --older-than-hours 168
pycloudctl gc --scope all --older-than-hours 168
```

说明：

1. 如果 `runtime-root` 下检测到本地受管 `controlplane/node` 进程仍在运行，`gc` 默认会拒绝执行破坏性删除
2. 这种情况下先停进程，再跑 `gc`
3. `--dry-run` 允许在线查看候选项
4. 只有明确知道风险时才用 `--force`

当前规则：

1. `codes`
   - 按 `codes/<storage_key>/meta.json` 的 `last_at`
   - 超过阈值就删整个 code scope
2. `objects`
   - 被“当前 globals 版本”引用的对象保留
   - 其他对象按 `last_at` 超时删除
3. `all`
   - 先回收 `codes`
   - 再基于剩余 code scope 扫描当前 globals 引用
   - 最后回收 `objects`

职责划分：

1. `InfoCenter`
   - 节点注册与心跳
   - 节点与服务路由事实查询
   - 暴露节点 `python_version`
   - 轻量运维页面 `/ops`
2. `Gateway`
   - 对外服务调用入口
   - `service_name -> route` 缓存与失败切换
3. `NodeControl`
   - 代码上传
   - task pool 执行
   - 服务模式生命周期管理

## 当前协议边界

### 服务模式

1. 对外推荐入口：`ControlPlane Gateway HTTP + JSON`
2. 节点管理面：`NodeControl gRPC`
3. 节点内服务执行：`POST /svc/{service_id}/call/{method}`

### 任务模式

1. 高频任务链路仍然走 `NodeControl gRPC`
2. `InfoCenter` 只提供节点事实，不代理任务
3. `Gateway` 不代理任务

## 顶层 Python API

V1 顶层公开面只保留：

```python
from pycloud_parallel import (
    Service,
    TaskPool,
    JobQueue,
    DataRef,
    export,
)
```

语义：

1. `Service`
   - owner 侧部署并持有服务
2. `TaskPool`
   - 唯一执行内核，对应专属任务池会话
3. `JobQueue`
   - 大任务排队客户端
4. `DataRef`
   - 唯一公开的大对象引用类型
5. `export`
   - 模块 / package 部署时的导出装饰器

本地并行入口不再从顶层包导出，请改用：

```python
from pycloud_parallel.local import configure, foreach, parallel_for
```

如果你需要更底层的控制面类，请从 `pycloud_parallel.controlplane` 导入。

## 快速开始

### 1. 启动控制面和节点

安装后的全局命令（推荐）：

```bash
pycloudctl start
pycloudctl start-controlplane
pycloudctl start-infocenter
pycloudctl start-gateway --infocenter-addr 127.0.0.1:50051
pycloudctl start-job-orchestrator --infocenter-addr 127.0.0.1:50051
pycloudctl start-node --node-id node-1 --infocenter-addr 127.0.0.1:50051
pycloudctl start-node --node-id node-1 --node-port 50061 --service-http-port 18081 --infocenter-addr 127.0.0.1:50051
pycloudctl status
pycloudctl doctor
pycloudctl stop-node node-1
```

`start` 支持指定运行目录、端口和 worker 容量，只是这些是全局参数，要写在 `start` 前面：

```bash
pycloudctl \
  --runtime-root /tmp/pycloud-dev \
  --controlplane-port 51051 \
  --job-orchestrator-port 51053 \
  --node1-port 51061 \
  --node1-http-port 18181 \
  --node2-port 51062 \
  --node2-http-port 18182 \
  --node-worker-capacity 4 \
  start
```

不要写成：

```bash
pycloudctl start --runtime-root /tmp/pycloud-dev
```

更完整的命令说明、目录规则、日志位置、GC 和示例见：

- [docs/PYCLOUDCTL_USAGE.md](docs/PYCLOUDCTL_USAGE.md)

`pycloudctl start` 现在会默认把独立 `job-orchestrator` 也一起拉起，方便直接走 `gateway -> job-orchestrator -> TaskPool` 这条任务链路。

如果你要单独起 `infocenter`、`gateway(http)`、`job-orchestrator`、`nodecontrol` 或独立 `controlplane`，现在也可以直接用上面的 `pycloudctl start-*` 子命令；更底层的 `pycloud-control` 示例见这份文档里的“单独起各角色”一节。

如果升级后怀疑旧服务没停掉，先看：

```bash
pycloudctl doctor
pycloudctl stop --scan-ports
```

macOS / Linux:

```bash
./scripts/start_services.sh start
./scripts/start_services.sh status
```

Windows `cmd`:

```bat
scripts\start_services.bat start
scripts\start_services.bat status
```

默认端口：

1. `ControlPlane`: `<auto-detected-local-ip>:50051`
2. `NodeControl node-1`: `<auto-detected-local-ip>:50061`
3. `NodeControl node-2`: `<auto-detected-local-ip>:50062`
4. `node-1 service HTTP`: `<auto-detected-local-ip>:18081`
5. `node-2 service HTTP`: `<auto-detected-local-ip>:18082`

默认情况下，`pycloudctl start` 不再把这些地址固定成 `127.0.0.1`，而是自动探测本机可达 IP。
如果你就是想强制只监听回环地址，请显式传：

```bash
pycloudctl \
  --controlplane-host 127.0.0.1 \
  --node1-host 127.0.0.1 \
  --node1-http-host 127.0.0.1 \
  --node2-host 127.0.0.1 \
  --node2-http-host 127.0.0.1 \
  start
```

现在也可以直接用更短的写法：

```bash
pycloudctl --local start
```

### 2. 服务模式

```python
from pycloud_parallel import Service

blob = (
import my_service_module

group = Service.deploy(
    infocenter_target="127.0.0.1:50051",
    service_name="square-service",
    source=my_service_module,
    runtime="py3",
    node_count=1,
)

print(group.square.sync(x=7))
# group.join() 适合 owner 长驻
# 重新部署同名服务且代码变化时，需先结束旧服务
# 同一台机器上，同一个 owner_client_id + service_name 只允许一个活跃 deployservice
```

默认推荐直接传模块对象 `source=my_service_module`。
如果你的代码依赖节点上未预装的包，或你需要更细的打包/导出控制，再使用高级 `Artifact(...)` 或显式白名单：

```python
group = Service.deploy(
    infocenter_target="127.0.0.1:50051",
    service_name="dep-service",
    artifact_path="./service_src",
    runtime="py3",
    entry_module="viewer",
    dependency_allowlist=[
        "./third_party/my_local_pkg",
        "orjson==3.10.18",
    ],
)
```

如果你直接传真实模块对象：

```python
import my_service_module
from pycloud_parallel import Service

group = Service.deploy(
    infocenter_target="127.0.0.1:50051",
    source=my_service_module,
    runtime="py3",
)
```

当前自动打包边界：

1. 自动打包只收 `.py / .pyd / .so`
2. 依赖分析直接基于“已加载 module object + 真实文件”
3. 非 Python 资源文件不会自动带上

如果你必须一起带 `.csv / .json` 等资源：

1. 预先自行构建 `zip / tar.gz / whl`
2. 再通过 `artifact_path=<archive file>` 或 `blob=...` 上传

完整说明见 [MODULE_DEPLOY_GUIDE.md](docs/MODULE_DEPLOY_GUIDE.md)。

### 3. 任务模式

```python
from pycloud_parallel import TaskPool

import my_task_module

with TaskPool.open(
    infocenter_target="127.0.0.1:50051",
    job_id="demo-job",
    source=my_task_module,
    runtime="py3",
) as pool:
    resp = pool.submit_payloads([{"value": 7}])
    results = pool.wait_for_data(expected_count=len(resp.accepted), timeout_sec=10.0)
    print(results)

    task_id = pool.run(value=11)
    print(task_id)

    result = pool.run.sync(value=12)
    print(result)

    items = pool.collect_data(max_count=1, timeout_sec=10.0)
    print(items)

    for task_id, data in pool.imap_unordered(
        [{"value": 20}, {"value": 21}],
        max_in_flight=2,
        receive_batch=1,
        result_timeout_sec=10.0,
    ):
        print(task_id, data)
```

说明：

1. `TaskPool` 当前只暴露一个任务入口，也就是 `entry_func / entry_callable`
2. `pool.methods` 会返回这个单一方法名
3. 如果你手动传 `task_method=...`，它现在会做严格校验；方法名不匹配会直接报错，不再静默回退

说明：

1. 如果子任务返回 `DataFrame / Series / ndarray`，框架会先尝试 inline 返回；只有超过 `PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES` 才会落到 node 本地对象目录并返回 `ResultRef`。
2. 如果你希望更大的 `DataFrame / Series / ndarray` 继续走 inline，可以调大 `PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES`。
3. 在 Windows 上一次性并发提交很多这类大结果任务时，一旦开始走结果落盘，文件系统更容易出现瞬时 `PermissionError(13)`。
4. 这类场景更推荐 `imap_unordered(...)` 或显式限制并发，例如把 `max_in_flight` 控制在 `8~32`，而不是一次性同时打满几十个任务。

如果你希望先排队，再由调度器自动创建专属 pool，使用 `JobQueue`。

常见高层入口：

1. `submit(source=...)`
2. `get_job_status(...)`
3. `wait_for_terminal(...)`

job module 约定：

1. `run(payload...)`
   - 必选，子任务入口
2. `task_generator(...)`
   - 必选
   - 返回 `list[dict]` 或 payload 迭代器
3. `update_globals(...)`
   - 可选
   - 只负责在 job-orch 端生成共享数据 `dict`
4. `handle_result(task_id, result, state=..., ...)` / `handle_data(...)`
   - 可选，增量聚合结果
5. `finalize(state=..., ...)`
   - 可选，生成最终 `final_result`
6. `apply_managed_globals(values, **context)`
   - 可选
   - 在 node/worker 端运行
   - 负责决定共享数据怎么作用到 runtime
   - 返回 `None` 时不做默认 raw assign
   - 返回 `dict` 时再把这个 dict 写回入口模块 A 的 globals

说明：

1. `job_payload` 是可选 `dict`
2. `submit(source=my_job_module, ...)` 会自动发现并绑定 `task_generator`
3. `handle_result` / `handle_data` / `finalize` / `update_globals` 都是可选，发现到才会写进 payload
4. `apply_managed_globals` 不需要通过 payload 传，worker 固定按约定名在入口模块 A 里查找
5. 你也可以显式传 `update_globals=...`，支持 `dict`、callable 名称字符串，或 callable 对象
6. 直接传模块对象时，会自动打包该模块及其本地 Python 依赖
7. 如果 job 依赖非 Python 资源文件，请预先自行构建归档后再上传

本地调试自动打包结果：

```bash
python scripts/debug_package_module.py calc_asset_ratio_job_module
```

### 4. Gateway 调用

```python
from pycloud_parallel import GatewayConnect

client = GatewayConnect("127.0.0.1:50051", service_name="square-service")
print(client.square.sync(x=9))
```

### 5. 本地并行（辅助能力）

```python
from pycloud_parallel import foreach, parallel_for

print(foreach(lambda x: x * x, [1, 2, 3], max_workers=2))
print(parallel_for(range(5), lambda i: i + 1, max_workers=2))
```

## 服务模式说明

服务模式已经收敛为“模块 + 多函数导出”：

1. 上传支持 `py / tar.gz / zip / whl`
2. 注册时传 `entry_module + export_spec`
3. 导出模式支持：`decorator / explicit / all / single`
4. 推荐默认：`decorator + pycloud_export`
5. 对外按 `service_name + method` 调用
6. owner 权限依赖 `service_token`

当前推荐调用路径：

1. owner：`Service.deploy(...)`
2. caller：`GatewayConnect(...)`
3. 调试直连：`DirectConnect(...)`

## 任务模式说明

任务模式当前已经收敛为：

1. `TaskPool`
   - 原生专属任务池会话
   - pool 自己保活、提交、拉结果、取消和关闭
2. `JobQueueMode`
   - 大任务排队与单活调度
   - job 排到后，再自动创建 `TaskPool`

## Python Runtime 约束

`runtime` 当前表示 Python 版本约束，不是任意标签。

支持的写法：

1. `py3`
2. `py3.11`
3. `>=py3.11`
4. `<=py3.11`
5. `>py3.11`
6. `<py3.11`

当前行为：

1. `InfoCenter` 会暴露节点 `python_version`
2. 服务部署和任务选点会先按 `runtime` 过滤节点
3. 节点侧在上传代码和创建服务时会再做一次本地校验

建议：

1. 通用示例默认使用 `runtime="py3"`
2. 只有明确依赖某个次版本时，再使用精确版本或比较表达式

## 依赖补装策略

当前策略刻意保持保守：

1. 默认不自动安装任何缺失模块
2. 只有调用方显式传 `dependency_allowlist` 才允许节点补装
3. 节点不会猜测 `import 名 -> pip 包名`
4. 白名单会安装到 `code_cache/codes/<sha>/deps`
5. 同一个 `code_version` 如果使用不同的 `dependency_allowlist`，会直接拒绝，避免缓存语义混乱

## 运维接口

当前 `ControlPlane` 端口同时提供：

1. `POST /nodes/register`
2. `POST /nodes/heartbeat`
3. `GET /nodes`
4. `GET /services/routes`
5. `GET /ops`
6. `POST /svc/{service_name}/call/{method}`
7. `GET /svc/{service_name}/methods`
8. `GET /svc/{service_name}/status`

节点运维：

1. `POST /ops/nodes/{node_id}/cordon`
2. `POST /ops/nodes/{node_id}/uncordon`
3. `POST /ops/nodes/{node_id}/drain`
4. `POST /ops/nodes/{node_id}/undrain`

## 推荐阅读顺序

1. [快速开始](docs/QUICK_START.md)
2. [架构总览](docs/ARCHITECTURE_OVERVIEW.md)
3. [任务模式](docs/TASK_MODE.md)
4. [Service](docs/SERVICE_MODULE_GROUP.md)
5. [Gateway 客户端指南](docs/GATEWAY_CLIENT_GUIDE.md)
6. [InfoCenter HTTP](docs/INFOCENTER_HTTP.md)
7. [Runtime 参数说明](docs/RUNTIME_PARAMETER_ANALYSIS.md)
