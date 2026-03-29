# PyCloud 服务会话通信机制（V1）

## 1. 目标

1. 客户端上传代码后由 NodeControl 动态启动服务进程组（例如 10 个）。
2. 通过客户端心跳维护租约，实现“断开自动回收”和“持续心跳常驻”。
3. 服务注册成功后，其他客户端可通过 HTTP 网关地址调用该服务。

## 2. 角色

1. `OwnerClient`：发起服务注册的客户端（拥有续租和结束权限）。
2. `NodeControl`：服务生命周期管理者（启动、续租、回收、HTTP转发）。
3. `CallerClient`：非 owner 客户端，只消费 HTTP 服务能力。

## 3. gRPC 控制面方法

1. `CreateService(stream CreateServiceRequest) -> CreateServiceResponse`
   - 上传代码与服务元数据，返回 `service_id`。
2. `HeartbeatService(HeartbeatServiceRequest) -> HeartbeatServiceResponse`
   - owner 定时续租，刷新 `lease_expire_at`。
3. `EndService(EndServiceRequest) -> EndServiceResponse`
   - owner 主动结束服务，立即进入回收。
4. `GetServiceStatus(GetServiceStatusRequest) -> GetServiceStatusResponse`
   - 查询服务状态、进程数、租约信息。
5. `InfoCenter.ListServiceRoutes(ListServiceRoutesRequest) -> ListServiceRoutesResponse`
   - 按 `service_name` 查询已部署服务路由（节点地址、状态、在途数）。

## 3.1 注册中心同步（NodeControl -> InfoCenter）

1. NodeControl 启动后会向 InfoCenter `RegisterNode`，声明自己是可用计算节点。
2. NodeControl 心跳 `HeartbeatNode` 会持续携带本机已部署服务清单（`services`）。
3. 每条服务路由包含：
   - `service_name` / `service_id`
   - `status`
   - `alive_workers` / `in_flight`
   - `http_base_url`
4. 客户端注册服务前先查 `ListServiceRoutes(service_name=xxx)`：
   - 若已有同名活跃服务（STARTING/RUNNING/DRAINING），直接报错并终止注册。
   - 若只是调用方客户端，则直接按已存在路由调用 HTTP 数据面。

## 4. 服务状态机

1. `STARTING`
   - NodeControl 接收代码、启动进程组、预热入口函数。
2. `RUNNING`
   - 服务可被 HTTP 调用，且 owner 心跳正常。
3. `DRAINING`
   - 收到 `EndService` 或租约超时，停止接收新请求，等待在途完成。
4. `STOPPED`
   - 进程退出，代码上下文回收，路由移除。

## 5. 心跳与回收

1. 建议间隔：`heartbeat_interval_sec=10`
2. 超时阈值：`heartbeat_timeout_sec=30`（由 CreateService 指定或默认）
3. 判定逻辑：
   - `now > lease_expire_at` -> 进入 `DRAINING` -> `STOPPED`
4. 主动结束：
   - owner 调用 `EndService`，不依赖超时判定。

## 6. 进程管理策略

1. NodeControl 按 `worker_count` 启动进程组（默认 10）。
2. 进程异常退出时可按策略拉起替补（维持目标数量）。
3. `DRAINING` 状态不再拉新进程，只做优雅退出。

## 7. HTTP 调用约定（服务数据面）

1. NodeControl 对外暴露：`http_base_url`（例如 `http://node:18080/svc/{service_id}`）。
2. 调用入口建议：
   - `POST /svc/{service_id}/invoke`
3. 鉴权建议：
   - `Authorization: Bearer <service_token>` 或 `X-Service-Token`。
4. 返回结构：
   - 成功：`{"ok": true, "data": ...}`
   - 失败：`{"ok": false, "error": "..."}`

## 8. 安全与隔离建议

1. `WorkerInternalService` 仅本机可达（Unix socket 或 localhost）。
2. 服务代码执行目录按 `service_id` 隔离并最小权限运行。
3. 回收时清理：
   - 进程组
   - 临时代码目录
   - 路由与 token

## 9. 当前实现状态

1. Proto 契约已提供（见 `proto/pycloud_v1.proto`）并已落地到 gRPC 服务实现。
2. NodeControl 已实现服务会话生命周期：
   - `CreateService` 创建服务并启动进程池
   - `HeartbeatService` 续租
   - `EndService` 显式回收
   - 超时扫描自动回收（`lease_expire_at`）
3. NodeControl 已内置 HTTP 网关：
   - `POST /svc/{service_id}/invoke`
   - `GET /svc/{service_id}/status`
4. 任务模式与服务会话模式均使用本机多进程执行（`spawn`），兼容 Windows。
