# pycloud-parallel

`pycloud-parallel` 目前包含两层能力：

1. 本地多进程并行 API。
2. 轻量控制面，用于跨节点部署和调用 Python 服务。

## 当前定位

### 本地并行 API

本地运行时已经收敛为纯本地多进程：

1. 只做单机 `ProcessPoolExecutor` / 多进程执行。
2. 不再承担跨节点调度。
3. 不依赖本地 gateway、集群控制面或复杂配置文件。

跨节点能力统一走 `src/pycloud_parallel/controlplane`。

### 控制面协议边界

当前协议已经简化为两条线：

1. `ControlPlane` 默认是一体化进程：`InfoCenter + Gateway`
   - 默认对外只起一个 HTTP 端口
   - `InfoCenter` 负责注册/发现/运维事实
   - `Gateway` 负责稳定服务入口、路由缓存、失败切换
2. `NodeControl` 使用 `gRPC`
   - 上传代码
   - 任务执行
   - 服务会话管理

展开后，对外协议边界是：

1. `InfoCenter` 子接口使用 `HTTP + JSON`
   - 节点注册
   - 节点心跳
   - 节点列表查询
   - 服务路由查询
   - 轻量运维页面
2. `Gateway` 子接口使用 `HTTP + JSON`
   - `POST /svc/{service_name}/call/{method}`
   - `GET /svc/{service_name}/methods`
   - `GET /svc/{service_name}/status`
3. `NodeControl` 使用 `gRPC`
   - 上传代码
   - 任务执行
   - 服务会话管理
4. 节点内部服务数据面使用 `HTTP + JSON`
   - `POST /svc/{service_id}/call/{method}`
   - `GET /svc/{service_id}/status`

也就是说：

1. `InfoCenter` 不再暴露 gRPC service。
2. `WorkerInternalService` 已移除，不再保留内部 gRPC 壳子。
3. 高频任务链路仍保留 `NodeControl gRPC`。
4. 外部调用方优先连 `ControlPlane Gateway`，而不是自己直接拼某个 `NodeControl` 的内部 HTTP 地址。

## 控制面组件

### ControlPlane

职责：

1. 默认由 `InfoCenter + Gateway` 组成。
2. 维护节点注册表。
3. 聚合节点上报的服务路由。
4. 提供简单的节点运维能力：`cordon / uncordon / drain / undrain`。
5. 提供一个极简 Web 页面 `/ops`。
6. 对外暴露稳定服务入口 `/svc/{service_name}/...`。

当前 HTTP 路径：

1. `POST /nodes/register`
2. `POST /nodes/heartbeat`
3. `GET /nodes`
4. `GET /services/routes`
5. `GET /ops`
6. `POST /ops/nodes/{node_id}/cordon`
7. `POST /ops/nodes/{node_id}/uncordon`
8. `POST /ops/nodes/{node_id}/drain`
9. `POST /ops/nodes/{node_id}/undrain`
10. `POST /svc/{service_name}/call/{method}`
11. `GET /svc/{service_name}/methods`
12. `GET /svc/{service_name}/status`

### NodeControl

职责：

1. 接收上传的代码或工程包。
2. 维护代码版本和本地缓存。
3. 执行任务模式请求。
4. 管理服务会话生命周期。
5. 暴露服务 HTTP 数据面：`/svc/{service_id}/...`

当前服务调用支持：

1. 节点内部 HTTP：`POST /svc/{service_id}/call/{method}`
2. 节点内部兼容：gRPC `CallService`
3. 对外推荐入口：先走 `ControlPlane Gateway` 的 `POST /svc/{service_name}/call/{method}`

## 服务会话模型

当前默认是“模块 + 多函数导出”模型：

1. 上传支持 `py / tar.gz / zip / whl`。
2. 注册时指定 `entry_module + export_spec`。
3. 导出规则支持：
   - `decorator`
   - `explicit`
   - `all`
   - `single`
4. 默认推荐：`decorator + pycloud_export`
5. 调用时按 `method` 路由，不再局限单一 `run()`。

## 上传与缓存

当前上传链路：

1. 客户端把目录或文件列表打包为 `tar.gz` / `zip`。
2. 通过 `NodeControl gRPC` 分块流式上传。
3. `NodeControl` 边收边写临时文件，不整包进内存。
4. 校验 `sha256` 后落地为 `code_version=sha256:<digest>`。
5. 归档包会解压到独立目录，例如 `<digest>_pkg`。

当前实现里：

1. 包缓存键是 `sha256`，目标是去重和稳定，不追求人工可读。
2. 同名包重复部署时，导入前会清理相关模块缓存，避免旧包路径污染。
3. 服务管理权限由 `service_token` 控制，不靠 `owner_client_id` 单独鉴权。

## deploy_from_infocenter 当前语义

`ServiceGroup.deploy_from_infocenter(...)` / `ServiceModuleGroup.deploy_from_infocenter(...)` 的默认行为已经简化：

1. 先从 InfoCenter 查询节点。
2. 过滤掉：
   - `unhealthy`
   - `schedulable=false`
   - `drain=true`
3. 按 `service_worker_available` 从高到低排序。
4. 默认只选择“需要的节点数”，不是默认把所有节点都部署一遍。

关键参数：

1. `node_ids`
   - 显式指定部署到哪些节点。
2. `node_count`
   - 指定要挑选多少个节点。
3. `min_success_nodes`
   - 默认也会影响选取数量。
4. `allow_partial`
   - 是否允许部分节点部署失败。

服务名相关语义：

1. 活跃 `service_name` 视为全局唯一。
2. 服务端不再对 `owner_client_id + service_name` 做兼容路由。
3. 多租户或多实例命名由客户端自行处理。
4. 对外发现主键是 `service_name`，`service_id` 主要用于实例级管理。

客户端复用策略：

1. 同 `owner_client_id + service_name + code_version` 时，默认复用已有服务。
2. 同名但代码变更时，默认拒绝覆盖。
3. 显式 `replace_existing_if_code_changed=True` 才会先结束旧服务再重建。
4. Python 客户端会把 `service_id/service_token` 本地落盘，支持客户端重启后继续心跳和结束服务。

## 任务模式

任务模式继续走 `NodeControl gRPC`，当前核心接口是：

1. `UploadCode`
2. `SubmitTasks`
3. `PullResults`
4. `CancelTasks`
5. `CancelJob`
6. `GetMetrics`

任务语义：

1. `task_id` 标识单个任务
2. `job_id` 标识一批任务
3. `job_id` 不是 session，不需要 heartbeat
4. 结果当前保存在节点内存中，不做持久化
5. task client 可先从 `InfoCenter.list_nodes()/select_task_nodes()` 取事实，再自行选 node 直连 `NodeControl`

Python 客户端 `NodeControlClient` 已提供对应 helper：

1. `upload_code_from_file(...)`
2. `upload_code_from_bytes(...)`
3. `submit_tasks(...)`
4. `pull_results(...)`
5. `cancel_tasks(...)`
6. `cancel_job(...)`
7. `get_metrics()`

如果想少写样板代码，也可以直接使用 `TaskBatchClient.from_infocenter(...)`：

1. 从 `InfoCenter` 选 node
2. 复用 `NodeControlClient` 上传代码
3. 绑定 `client_id + job_id + code_version`
4. 提供 `submit_payloads / wait_for_results(job_id=...) / cancel_job`

## Gateway Python Helper

如果你是 Python 调用方，推荐直接使用 `GatewayServiceClient`：

```python
from pycloud_parallel.controlplane.client import GatewayServiceClient

with GatewayServiceClient("127.0.0.1:50051", timeout_sec=10.0) as client:
    methods = client.list_methods(service_name="square-service")
    resp = client.call(
        service_name="square-service",
        method="square",
        payload={"x": 7},
        timeout_sec=10.0,
    )
```

适合：

1. 你想显式看到 `methods/status/raw response`
2. 你希望保持最薄的一层 HTTP helper
3. 你不需要 module-like 调用体验

如果你希望调用体验更像本地模块，也可以直接使用 `GatewayModuleClient`：

```python
from pycloud_parallel.controlplane.client import GatewayModuleClient

client = GatewayModuleClient("127.0.0.1:50051", service_name="square-service")

result1 = client.square.sync(x=7)
# 或
# result2 = await client.square(x=7)
```

适合：

1. 你已经知道服务方法名
2. 你更喜欢 `client.square.sync(...)` 这种调用方式
3. 你只是 caller，不是 owner

## Discovery Python Helper

如果你不想经过 Gateway，而是想像 Eureka client 一样：

1. 先查 `InfoCenter`
2. 客户端本地维护 route cache
3. 直接调用某个实例

可以使用 `DiscoveryServiceClient`：

```python
from pycloud_parallel.controlplane.client import DiscoveryServiceClient

with DiscoveryServiceClient("127.0.0.1:50051", timeout_sec=10.0) as client:
    resp = client.call(
        service_name="square-service",
        method="square",
        payload={"x": 7},
        timeout_sec=10.0,
    )
```

如果希望调用体验更像本地模块，可以使用 `DiscoveryModuleClient`：

```python
from pycloud_parallel.controlplane.client import DiscoveryModuleClient

client = DiscoveryModuleClient("127.0.0.1:50051", service_name="square-service")
result = client.square.sync(x=7)
```

适合：

1. 内部 Python 调用方
2. 希望自己掌控客户端侧服务发现
3. 不想把调用统一交给 Gateway

## 客户端分类

当前建议按职责来选客户端：

1. `InfoCenterClient`
   - 查节点
   - 查服务 route
   - 任务选点
   - 适合“看事实，不直接发业务调用”
2. `GatewayServiceClient`
   - 走 `controlplane Gateway`
   - 薄 HTTP helper
   - 适合显式按 `service_name + method` 调用
3. `GatewayModuleClient`
   - 走 `controlplane Gateway`
   - module-like caller
   - 适合 `client.foo.sync(...)`
4. `DiscoveryServiceClient`
   - 不经过 Gateway
   - 客户端自己查 `InfoCenter` + 本地 route cache + 直连实例
   - 适合 Eureka 风格调用
5. `DiscoveryModuleClient`
   - 不经过 Gateway
   - module-like caller
   - 适合内部 Python 调用方自己掌控发现和选路
6. `ServiceModuleGroup`
   - owner / deploy 侧
   - 负责部署、复用、长驻、结束服务
   - 不是普通 caller
7. `NodeControlClient`
   - 任务模式和底层管理面 gRPC
   - 更底层
8. `TaskBatchClient`
   - 任务模式 helper
   - 适合批量任务提交和拉结果

## 本地启动

### 启动一体化 ControlPlane

```bash
python -m pycloud_parallel.controlplane.server --role controlplane --bind 0.0.0.0:50051
```

### 启动 NodeControl

```bash
python -m pycloud_parallel.controlplane.server \
  --role nodecontrol \
  --bind 0.0.0.0:50061 \
  --node-id node-local-01 \
  --infocenter-addr 127.0.0.1:50051 \
  --advertise-addr 127.0.0.1:50061
```

### 可选：拆分成独立 InfoCenter 与 Gateway

```bash
python -m pycloud_parallel.controlplane.server --role infocenter --bind 0.0.0.0:50051
python -m pycloud_parallel.controlplane.server --role gateway --bind 0.0.0.0:50052 --infocenter-addr 127.0.0.1:50051
```

### 使用脚本启动本地演示环境

```bash
./scripts/start_services.sh start
./scripts/start_services.sh status
./scripts/start_services.sh stop
```

`status` 当前会输出：

1. `controlplane/node-1/node-2` 进程状态。
2. `Loaded Services By Node`，即每个节点当前加载的服务名列表。

## 示例脚本

当前可直接运行的脚本：

```bash
python scripts/demo_simple_deploy.py
python scripts/demo_deploy_from_files.py
python scripts/demo_service_module_group.py
python scripts/demo_gateway_client.py
python scripts/demo_gateway_module_client.py
python scripts/grpc_register_service_client_demo.py
python scripts/grpc_async_demo.py
python scripts/grpc_existing_service_client_demo.py
python scripts/grpc_task_client_demo.py
```

这些脚本当前都不依赖命令行参数解析，默认配置直接写在 `main()` 里。

推荐按角色理解：

1. owner / deploy
   - `scripts/grpc_register_service_client_demo.py`
   - `scripts/demo_service_module_group.py`
   - `scripts/demo_deploy_from_files.py`
2. caller
   - `scripts/demo_gateway_client.py`
   - `scripts/demo_gateway_module_client.py`
   - `scripts/grpc_existing_service_client_demo.py`
3. task
   - `scripts/grpc_task_client_demo.py`

owner 类脚本当前统一推荐：

1. 先部署服务
2. 部署返回后 keepalive 已自动启动
3. 做一两个预热调用
4. 调 `group.join(...)` 长驻
5. 用 `Ctrl+C` 正常结束并回收服务

## gRPC Stub 生成

```bash
python scripts/gen_grpc_stubs.py
```

或：

```bash
bash scripts/gen_grpc_stubs.sh
```

## 文档索引

1. `docs/ARCHITECTURE_OVERVIEW.md`
2. `docs/TASK_MODE.md`
3. `docs/SERVICE_MODULE_GROUP.md`
4. `docs/DEPLOY_DEFAULT_VALUES.md`
5. `docs/DEPLOY_FINAL_SUMMARY.md`
6. `docs/INFOCENTER_HTTP.md`
