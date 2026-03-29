# PyCloud gRPC 接口文档（V1）

> 说明：客户端与 NodeControl 的通信改为 gRPC。  
> 对应 proto 文件：`proto/pycloud_v1.proto`

## 1. 总体约定

1. 传输协议：gRPC（HTTP/2 + Protobuf）。
2. 包名：`pycloud.v1`
3. 服务拆分：
   - `InfoCenterService`
   - `NodeControlService`
   - `WorkerInternalService`（仅本机内部调试/兼容；生产默认由 NodeControl 内置进程池执行）
4. 认证建议：
   - 外部接口（InfoCenter/NodeControl）使用 Bearer Token（metadata）。
   - 内部接口（WorkerInternal）限制为 Unix Socket 或 localhost。

## 2. 服务与方法

## 2.1 InfoCenterService

1. `RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse)`
   - 节点注册与重注册。
   - 支持携带本节点已部署服务路由（`services`）。
2. `HeartbeatNode(HeartbeatNodeRequest) returns (HeartbeatNodeResponse)`
   - 节点心跳续约。
   - 支持上报实时服务路由状态（`services`）。
3. `ListNodes(ListNodesRequest) returns (ListNodesResponse)`
   - 客户端查询可用节点列表与负载。
4. `ListServiceRoutes(ListServiceRoutesRequest) returns (ListServiceRoutesResponse)`
   - 按 `service_name` 查询已部署服务所在节点与路由状态。

## 2.2 NodeControlService

1. `UploadCode(stream UploadCodeRequest) returns (UploadCodeResponse)`
   - 客户端流式上传代码包（首帧 metadata，后续 chunk）。
   - 元数据中的 `entry_callable` 是执行入口（默认 `run`）。
2. `SubmitTasks(SubmitTasksRequest) returns (SubmitTasksResponse)`
   - 批量提交任务，返回 accepted/rejected。
3. `PullResults(PullResultsRequest) returns (PullResultsResponse)`
   - 结果长轮询（`wait_ms`）。
4. `CancelTasks(CancelTasksRequest) returns (CancelTasksResponse)`
   - 取消任务。
5. `GetMetrics(GetMetricsRequest) returns (GetMetricsResponse)`
   - 节点运行指标。
6. `CreateService(stream CreateServiceRequest) returns (CreateServiceResponse)`
   - 注册服务会话：上传代码 + 启动进程组 + 返回 `service_id` 与 `http_base_url`。
7. `HeartbeatService(HeartbeatServiceRequest) returns (HeartbeatServiceResponse)`
   - owner 客户端续租服务会话。
8. `EndService(EndServiceRequest) returns (EndServiceResponse)`
   - owner 主动结束服务并触发回收。
9. `GetServiceStatus(GetServiceStatusRequest) returns (GetServiceStatusResponse)`
   - 查询服务会话状态与租约信息。
10. 说明
   - 客户端不负责创建 worker，NodeControl 会在本机动态管理多进程 worker。

## 2.3 WorkerInternalService

1. `PollTask(PollTaskRequest) returns (PollTaskResponse)`
   - worker 拉取任务。
2. `HeartbeatTask(HeartbeatTaskRequest) returns (HeartbeatTaskResponse)`
   - worker 上报任务心跳/进度。
3. `ReportResult(ReportResultRequest) returns (ReportResultResponse)`
   - worker 上报执行结果。

## 3. 关键字段规范

1. `task_id`
   - 全局唯一，幂等键，重复提交不得重复执行。
2. `code_version`
   - 由代码上传返回，任务必须引用该版本。
3. `attempt`
   - 从 1 开始，基础设施失败最多重试到 3。
4. `execution_mode`
   - `PERSISTENT` 或 `EPHEMERAL`。
5. `payload` / `result`
   - 使用 `google.protobuf.Struct`，保持业务字段灵活。
6. `service_id`
   - 服务会话唯一 ID，用于续租、结束和状态查询。
7. `owner_client_id`
   - 服务会话所有者，用于权限与回收判定。
8. `services`（RegisterNode/HeartbeatNode）
   - NodeControl 上报本机部署服务清单与状态（用于服务发现）。
9. `service_name`
   - 作为服务注册名，客户端创建服务前应先用 `ListServiceRoutes` 校验同名冲突。

## 4. 错误与重试语义

1. 有返回（`SUCCEEDED` 或 `FAILED_USER`）
   - 终态，不重试。
2. 无返回（心跳超时、失联、网络中断）
   - 标记 `FAILED_INFRA`，最多重试 3 次。
3. 心跳参数（建议）
   - 心跳间隔 30 秒
   - 失联阈值 90 秒
   - 扫描周期 10 秒

## 5. 分发与背压语义

1. 客户端先从 `ListNodes` 获取节点。
2. 初始每节点投递 10 个任务。
3. 后续按 `credit = queue_capacity - (queued + inflight)` 优先投递。
4. 当节点返回 `NO_CREDIT`，客户端切换其他节点，避免积压。

## 6. 状态码与错误码

1. gRPC status：
   - `OK`
   - `INVALID_ARGUMENT`
   - `UNAUTHENTICATED`
   - `NOT_FOUND`
   - `ALREADY_EXISTS`
   - `RESOURCE_EXHAUSTED`
   - `UNAVAILABLE`
   - `INTERNAL`
2. 业务错误码在响应内使用 `ErrorCode` 枚举表达（见 proto）。

## 7. 服务会话说明

1. 详细机制见 `SERVICE_SESSION_PROTOCOL_V1.md`。
2. 当前版本中，服务会话接口已实现，NodeControl 提供完整生命周期管理（创建、续租、主动结束、超时回收）。
