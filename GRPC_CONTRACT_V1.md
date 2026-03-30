# PyCloud gRPC 契约文档（V1）

> 当前生效基线：`proto/pycloud_v1.proto` 与 `src/pycloud_parallel/controlplane/*` 实现。  
> 本文是实现导向摘要，不替代 proto。

## 1. 服务划分

1. `InfoCenterService`
2. `NodeControlService`
3. `WorkerInternalService`

## 2. InfoCenterService

1. `RegisterNode`
   - 节点注册，支持附带服务路由 `services`。
2. `HeartbeatNode`
   - 节点续租，支持持续上报服务路由状态。
3. `ListNodes`
   - 查询节点健康与负载。
4. `ListServiceRoutes`
   - 按 `service_name` 查询服务路由。

## 3. NodeControlService

### 3.1 任务模式

1. `UploadCode(stream UploadCodeRequest)`
2. `SubmitTasks`
3. `PullResults`
4. `CancelTasks`
5. `GetMetrics`

### 3.2 服务会话模式

1. `CreateService(stream CreateServiceRequest)`
2. `ListServiceMethods`
3. `CallService`
4. `HeartbeatService`
5. `EndService`
6. `GetServiceStatus`

## 4. 上传与导出模型

### 4.1 `UploadCodeMeta` / `CreateServiceMeta` 关键字段

1. `entry_module`
2. `entry_callable`（兼容字段）
3. `package_format`：`py | tar.gz | zip | whl`
4. `export_spec`：`ModuleExportSpec`

### 4.2 `ModuleExportSpec`

1. `mode`：`decorator | explicit | all | single`
2. `methods`：当 `explicit/single` 时使用
3. `decorator`：装饰器标记名（默认 `pycloud_export`）

## 5. 方法调用模型

1. 客户端先 `ListServiceMethods(service_id)` 获取可调用方法。
2. 再 `CallService(service_id, method, payload, timeout_sec, service_token)`。
3. 成功返回 `data`；失败返回 `task_error/error`。

## 6. WorkerInternalService（本机内部）

1. `PollTask`
2. `HeartbeatTask`
3. `ReportResult`

> 该服务用于本机 worker 协同，生产场景一般不直接给业务客户端调用。

## 7. 错误语义（实现约定）

1. 业务执行异常：`FAILED_USER`
2. 基础设施异常：`FAILED_INFRA`（任务模式可重试至 `max_retries`）
3. 服务方法不存在：`NOT_FOUND`
4. 服务 token 错误：`PERMISSION_DENIED`

## 8. 当前实现补充

1. 上传是“流式分块 + NodeControl 边收边写临时文件”。
2. 校验 `sha256` 后落地为 `code_version=sha256:<digest>`。
3. 包格式为归档时会解压到独立目录并按 `entry_module` 导入。
4. 服务会话调用支持 gRPC 与 HTTP 双通道（见 `SERVICE_SESSION_PROTOCOL_V1.md`）。
