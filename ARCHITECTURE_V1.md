# PyCloud 架构说明（V1）

## 1. 架构目标

1. 并行任务执行（任务模式）。
2. 多方法长驻服务（服务会话模式）。
3. 控制面统一使用 gRPC；服务数据面支持 HTTP/gRPC 调用。

## 2. 组件

1. `InfoCenter`
   - 节点注册与健康维护。
   - 服务路由聚合与查询。
2. `NodeControl`
   - 代码接收与版本化。
   - 任务队列与本机进程池执行。
   - 服务会话生命周期管理。
   - HTTP Gateway 暴露服务调用入口。
3. `Worker Process Pool`
   - 使用多进程执行用户代码（spawn）。

## 3. 两种执行模型

### 3.1 任务模式

1. 上传代码得到 `code_version`。
2. `SubmitTasks` 提交批量任务。
3. NodeControl 分发到本机进程池。
4. `PullResults` 拉取结果。

### 3.2 服务会话模式

1. `CreateService` 上传工程包并启动服务进程池。
2. 加载 `entry_module`，按 `export_spec` 构建方法路由。
3. `ListServiceMethods` 暴露可调用方法。
4. `CallService` / `POST /svc/{id}/call/{method}` 执行方法。
5. `HeartbeatService` 续租，`EndService` 主动结束。

## 4. 上传链路（当前实现）

1. gRPC chunk 到达 NodeControl。
2. NodeControl 边收边写临时文件（不整包驻留内存）。
3. 完成后校验 `sha256`。
4. 依据 `package_format`：
   - `py`：直接保存。
   - `tar.gz/zip/whl`：保存归档并解压到独立目录。
5. 生成 `code_version=sha256:<digest>`。

## 5. 方法导出与安全

1. 默认建议 `decorator` 白名单导出。
2. `explicit` 适合严格控制 API 面。
3. `all` 风险较高，通常仅用于内部调试。
4. 方法名不允许以下划线开头，防止私有函数误暴露。

## 6. 可靠性机制

1. 任务模式：
   - `FAILED_USER` 终态不重试。
   - `FAILED_INFRA` 可按 `max_retries` 重试。
2. 服务模式：
   - owner 心跳超时自动回收。
   - 支持主动结束。
3. 节点与服务路由通过 InfoCenter 心跳维护。

## 7. 调度与路由

1. 节点发现：`ListNodes` / `ListServiceRoutes`。
2. 多节点服务组（client）：
   - `least_inflight` 或 `round_robin`。
   - 内置断路器（open/half-open/closed）。

## 8. 协议文档索引

1. `proto/pycloud_v1.proto`
2. `GRPC_CONTRACT_V1.md`
3. `SERVICE_SESSION_PROTOCOL_V1.md`
4. `API_CONTRACT_V1.md`（REST 草案）
