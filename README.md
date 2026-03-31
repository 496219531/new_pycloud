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

1. `InfoCenter` 使用 `HTTP + JSON`
   - 节点注册
   - 节点心跳
   - 节点列表查询
   - 服务路由查询
   - 轻量运维页面
2. `NodeControl` 使用 `gRPC`
   - 上传代码
   - 任务执行
   - 服务会话管理

也就是说：

1. `InfoCenter` 不再暴露 gRPC service。
2. `WorkerInternalService` 已移除，不再保留内部 gRPC 壳子。
3. 高频任务链路仍保留 `NodeControl gRPC`。

## 控制面组件

### InfoCenter

职责：

1. 维护节点注册表。
2. 聚合节点上报的服务路由。
3. 提供简单的节点运维能力：`cordon / uncordon / drain / undrain`。
4. 提供一个极简 Web 页面 `/ops`。

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

### NodeControl

职责：

1. 接收上传的代码或工程包。
2. 维护代码版本和本地缓存。
3. 执行任务模式请求。
4. 管理服务会话生命周期。
5. 暴露服务 HTTP 数据面：`/svc/{service_id}/...`

当前服务调用支持：

1. gRPC `CallService`
2. HTTP `POST /svc/{service_id}/call/{method}`

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

`MultiNodeServiceGroup.deploy_from_infocenter(...)` / `ModuleLikeServiceGroup.deploy_from_infocenter(...)` 的默认行为已经简化：

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

客户端复用策略：

1. 同 `owner_client_id + service_name + code_version` 时，默认复用已有服务。
2. 同名但代码变更时，默认拒绝覆盖。
3. 显式 `replace_existing_if_code_changed=True` 才会先结束旧服务再重建。
4. Python 客户端会把 `service_id/service_token` 本地落盘，支持客户端重启后继续心跳和结束服务。

## 本地启动

### 启动 InfoCenter

```bash
python -m pycloud_parallel.controlplane.server --role infocenter --bind 0.0.0.0:50051
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

### 使用脚本启动本地演示环境

```bash
./scripts/start_services.sh start
./scripts/start_services.sh status
./scripts/start_services.sh stop
```

`status` 当前会输出：

1. `infocenter/node-1/node-2` 进程状态。
2. `Loaded Services By Node`，即每个节点当前加载的服务名列表。

## 示例脚本

当前可直接运行的脚本：

```bash
python scripts/demo_simple_deploy.py
python scripts/demo_deploy_from_files.py
python scripts/demo_module_like_client.py
python scripts/grpc_register_service_client_demo.py
python scripts/grpc_async_demo.py
python scripts/grpc_existing_service_client_demo.py
```

这些脚本当前都不依赖命令行参数解析，默认配置直接写在 `main()` 里。

## gRPC Stub 生成

```bash
python scripts/gen_grpc_stubs.py
```

或：

```bash
bash scripts/gen_grpc_stubs.sh
```

## 文档索引

1. `ARCHITECTURE_V1.md`
2. `API_CONTRACT_V1.md`
3. `GRPC_CONTRACT_V1.md`
4. `SERVICE_SESSION_PROTOCOL_V1.md`
5. `docs/MODULE_LIKE_SERVICE.md`
6. `docs/DEPLOY_DEFAULT_VALUES.md`
7. `docs/INFOCENTER_HTTP.md`
