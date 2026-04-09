# PyCloud 架构说明（V1）

## 1. 当前范围

当前仓库已经分成两块：

1. `pycloud_parallel/local_runtime`
   - 只负责单机本地多进程并行。
2. `pycloud_parallel/controlplane`
   - 负责跨节点部署、服务发现、任务与服务会话。

本地运行时不再承担跨集群职责。

## 2. 当前设计原则

这版实现刻意偏“简单而稳定”：

1. 协议边界尽量少。
2. 组件职责尽量直白。
3. 节点选择尽量可预期。
4. 不做复杂调度器和自动 reconcile。
5. 宁可粗暴，也避免难以调试的隐式行为。

## 3. 组件图

### 3.1 控制面

1. `InfoCenter`
   - `HTTP + JSON`
   - 节点注册/心跳
   - 路由查询
   - 简单运维接口和页面
   - 暴露节点 `python_version`
2. `NodeControl`
   - `gRPC`
   - 代码上传
   - 任务执行
   - 服务会话管理
3. `Service HTTP Gateway`
   - 节点本地 HTTP 数据面
   - `POST /svc/{service_id}/call/{method}`
   - `GET /svc/{service_id}/status`

### 3.2 本地运行时

1. `Runtime`
2. `ProcessPoolRunner`
3. 本地 executor

不再保留本地 gateway、cluster adapter、跨节点调度壳子。

## 4. 协议边界

### 4.1 InfoCenter

InfoCenter 当前只保留 HTTP：

1. `POST /nodes/register`
2. `POST /nodes/heartbeat`
3. `GET /nodes`
4. `GET /services/routes`
5. `GET /ops`
6. 节点运维切换接口

### 4.2 NodeControl

NodeControl 当前保留 gRPC：

1. `UploadCode`
2. `TaskStream`
3. `SubmitTasks`
4. `PullResults`
5. `CancelTasks`
6. `CancelJob`
7. `GetMetrics`
8. `CreateService`
9. `ListServiceMethods`
10. `CallService`
11. `HeartbeatService`
12. `EndService`
13. `GetServiceStatus`

### 4.3 已移除

1. `InfoCenterService` gRPC service 已移除。
2. `WorkerInternalService` 已移除。
3. Worker 内部不再走一层 gRPC 壳子。

## 5. 两种执行模型

### 5.1 任务模式

1. 客户端上传代码。
2. 获得 `code_version`。
3. 从 `InfoCenter` 选任务节点。
4. 向目标 `NodeControl` 建立 `TaskStream`。
5. 提交任务到 NodeControl。
6. NodeControl 用 runtime slot + 本机进程执行。
7. 客户端拉取结果。

这条链路适合高频任务提交，因此仍保留 gRPC。

### 5.2 服务会话模式

1. 客户端上传工程包。
2. NodeControl 按 `entry_module + export_spec` 发现可调用方法。
3. 建立 `method -> callable` 路由。
4. 返回 `service_id + service_token`。
5. owner 通过心跳保活。
6. 其他调用方可通过 gRPC 或 HTTP 调服务方法。

两种模式都支持 `runtime` 作为 Python 版本约束：

1. `py3`
2. `py3.11`
3. `>=py3.11`

客户端会先按 `InfoCenter` 提供的 `python_version` 过滤，节点侧再做二次校验。

## 6. 上传与导入

### 6.1 上传

1. 客户端把目录或文件列表打包成 `tar.gz` / `zip`。
2. 通过 gRPC 流式上传到 NodeControl。
3. NodeControl 边收边写临时文件。
4. 上传完成后校验 `sha256`。

### 6.2 落地

1. `py` 文件直接保存。
2. `tar.gz / zip / whl` 会解压到独立目录。
3. 代码版本统一命名为 `sha256:<digest>`。

### 6.3 导入污染防护

对于归档包：

1. 导入前会清理 `entry_module` 及其父包缓存。
2. 这样重复部署同名包时，不会命中旧 `sys.modules` 路径。

## 7. 服务导出模型

支持四种导出模式：

1. `decorator`
2. `explicit`
3. `all`
4. `single`

默认推荐：

1. `export_mode="decorator"`
2. `export_decorator="pycloud_export"`

原因：

1. 更安全。
2. 更容易控制对外 API 面。
3. 不容易误暴露辅助函数。

## 8. 节点选择与部署策略

当前客户端部署策略尽量简单：

1. 从 InfoCenter 拉节点列表。
2. 过滤不健康节点。
3. 过滤 `schedulable=false`。
4. 过滤 `drain=true`。
5. 如果指定了 `runtime`，先按节点 `python_version` 过滤。
6. 按 `service_worker_available` 倒序排序。
7. 取 `node_ids` 或 `node_count` / `min_success_nodes` 决定的前 N 个节点。

默认不是全量铺节点。

## 9. 服务唯一性与权限

### 9.1 命名

1. 活跃 `service_name` 视为全局唯一。
2. 服务端不再用 `owner_client_id + service_name` 做二次区分。
3. 如果需要多租户命名，交给客户端自己编码到 `service_name`。

### 9.2 管理权限

1. `owner_client_id` 主要用于标识 owner。
2. 真正的服务管理权限依赖 `service_token`。
3. `HeartbeatService` 和 `EndService` 都要求 `service_token`。

## 10. 运维模型

运维入口放在 InfoCenter：

1. 统一查看节点状态。
2. 统一查看路由聚合结果。
3. 统一做节点 `cordon/drain`。
4. 现在按 `node_instance_id` 精确区分同名节点实例。

节点状态由谁真正执行：

1. 运维动作先写入 InfoCenter 状态。
2. 客户端选点时遵守这些状态。
3. 当前不做复杂的自动迁移与重平衡。

节点标识补充：

1. `node_id`
   - 展示名 / 逻辑名
   - 允许重复
2. `node_instance_id`
   - InfoCenter 内部主键
   - `/ops`、路由聚合、节点状态跟踪按它定位

## 11. 当前不做的事

1. 不做复杂调度器。
2. 不做自动故障迁移闭环。
3. 不做统一调用鉴权网关。
4. 不做 node/cluster 间复杂协商。

这些都可以以后再加，但不作为当前基础架构的一部分。
