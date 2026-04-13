# PyCloud gRPC 契约文档（V1）

> 当前实现里，gRPC 只保留 `NodeControlService`。  
> `InfoCenterService` 和 `WorkerInternalService` 都已经从 proto 中移除。

## 1. 当前 gRPC 服务

只有一个：

1. `NodeControlService`

定义来源：`proto/pycloud_v1.proto`

## 2. NodeControlService 方法

### 2.1 代码与对象管理

1. `UploadCode(stream UploadCodeRequest)`
2. `UploadObject(stream UploadObjectRequest)`
3. `DownloadObject(DownloadObjectRequest)`
4. `UpdateRuntimeGlobals(UpdateRuntimeGlobalsRequest)`
5. `GetMetrics(GetMetricsRequest)`

### 2.2 TaskPool 模式

1. `CreateTaskPool(stream CreateTaskPoolRequest)`
2. `HeartbeatTaskPool(HeartbeatTaskPoolRequest)`
3. `SubmitPoolTasks(SubmitPoolTasksRequest)`
4. `PullPoolResults(PullPoolResultsRequest)`
5. `CancelPoolJob(CancelPoolJobRequest)`
6. `GetTaskPoolStatus(GetTaskPoolStatusRequest)`
7. `CloseTaskPool(CloseTaskPoolRequest)`

### 2.3 服务会话模式

1. `CreateService(stream CreateServiceRequest)`
2. `ListServiceMethods(ListServiceMethodsRequest)`
3. `CallService(CallServiceRequest)`
4. `UpdateServiceGlobals(UpdateServiceGlobalsRequest)`
5. `HeartbeatService(HeartbeatServiceRequest)`
6. `EndService(EndServiceRequest)`
7. `GetServiceStatus(GetServiceStatusRequest)`

## 3. 上传与工程包

`UploadCodeMeta` / `CreateServiceMeta` 的关键字段：

1. `sha256`
2. `runtime`
3. `entry_module`
4. `entry_callable`
5. `package_format`
6. `export_spec`

支持的包格式：

1. `py`
2. `tar.gz`
3. `zip`
4. `whl`

## 4. 方法导出模型

`ModuleExportSpec`：

1. `mode`
   - `decorator`
   - `explicit`
   - `all`
   - `single`
2. `methods`
3. `decorator`

当前推荐：

1. `mode="decorator"`
2. `decorator="pycloud_export"`

## 5. 服务会话权限

### 5.1 创建

`CreateService` 返回：

1. `service_id`
2. `code_version`
3. `status`
4. `worker_count`
5. `heartbeat_timeout_sec`
6. `owner_client_id`
7. `service_token`
8. `http_base_url`

### 5.2 后续管理

以下接口要求使用 `service_token`：

1. `HeartbeatService`
2. `EndService`
3. `CallService` 可选携带 `service_token`

当前真正的管理权限依赖 `service_token`，不是仅靠 `owner_client_id`。

## 6. 当前语义补充

### 6.1 `service_name`

1. 活跃 `service_name` 视为全局唯一。
2. 服务端不再兼容 `owner_client_id + service_name` 的同名区分。

### 6.2 客户端复用

Python 客户端当前支持：

1. 同 `owner_client_id + service_name + code_version` 时复用已有活跃服务。
2. 同名但代码变化时默认拒绝。
3. 只有 `replace_existing_if_code_changed=True` 才会替换。

### 6.3 节点部署选择

`deploy_from_infocenter(...)` 当前不会默认铺满所有节点，而是：

1. 按 InfoCenter 返回的节点状态过滤。
2. 按 `service_worker_available` 选前 N 个节点。
3. N 由 `node_ids` / `node_count` / `min_success_nodes` 决定。

### 6.4 节点实例唯一键

虽然 `InfoCenterService` 已经从 gRPC service 里移除，但当前 proto 里的节点相关消息仍然统一带上了 `node_instance_id`：

1. `RegisterNodeRequest`
2. `RegisterNodeResponse`
3. `HeartbeatNodeRequest`
4. `NodeInfo`
5. `ServiceRouteInfo`

约定：

1. `node_id` 主要用于逻辑展示，允许重复
2. `node_instance_id` 才是节点实例唯一键
3. HTTP `/services/routes` 与 `/nodes` 返回的也是这套字段

## 7. 已移除的旧项

以下 gRPC service 已不再存在：

1. `InfoCenterService`
2. `WorkerInternalService`

以下共享任务模式 RPC 已从当前 `NodeControlService` 契约移除：

1. `TaskStream`
2. `SubmitTasks`
3. `PullResults`
4. `CancelTasks`
5. `CancelJob`

如果旧文档或旧代码还在引用它们，应以当前 proto 为准。

## 8. 参考

1. `proto/pycloud_v1.proto`
2. `API_CONTRACT_V1.md`
3. `SERVICE_SESSION_PROTOCOL_V1.md`
