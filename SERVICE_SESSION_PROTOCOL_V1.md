# PyCloud 服务会话协议（V1）

## 1. 目标

服务会话用于“上传一份工程包，长期暴露多个可调用方法”，并通过 owner 心跳控制生命周期。

## 2. 角色

1. `OwnerClient`：创建/续租/结束服务。
2. `NodeControl`：管理服务进程池、路由与回收。
3. `CallerClient`：通过 gRPC 或 HTTP 调用已注册服务。

## 3. gRPC 控制流

1. `CreateService(stream CreateServiceRequest)`
2. `ListServiceMethods(ListServiceMethodsRequest)`
3. `CallService(CallServiceRequest)`
4. `HeartbeatService(HeartbeatServiceRequest)`
5. `EndService(EndServiceRequest)`
6. `GetServiceStatus(GetServiceStatusRequest)`

## 4. 上传形态与导出规则

### 4.1 上传形态

1. 支持 `py / tar.gz / zip / whl`。
2. 客户端以 gRPC chunk 流上传，NodeControl 边收边写临时文件。
3. `sha256` 校验通过后生成 `code_version`。

### 4.2 导出规则（`export_spec`）

1. `decorator`：按装饰器白名单导出（默认推荐）。
2. `explicit`：按 `methods` 显式列表导出。
3. `all`：导出模块所有公开可调用对象。
4. `single`：只导出一个方法（兼容旧入口风格）。

## 5. 方法路由

1. 服务启动时建立 `method -> callable` 路由表。
2. `ListServiceMethods` 返回可调用方法列表。
3. `CallService` 与 HTTP 都按 `method` 分发。

## 6. HTTP 数据面

1. `POST /svc/{service_id}/call/{method}?timeout_sec=...`
2. `GET /svc/{service_id}/status`
3. 支持 `X-Service-Token` 或 `Authorization: Bearer ...`

成功响应示例：

```json
{"ok": true, "method": "square", "data": {"value": 3, "square": 9}}
```

失败响应示例：

```json
{"ok": false, "method": "square", "error_type": "UserError", "error": "..."}
```

## 7. 生命周期

1. 创建后进入 `RUNNING`。
2. owner 周期性 `HeartbeatService` 续租。
3. 超时或主动 `EndService` 后进入回收并 `STOPPED`。
4. NodeControl 心跳上报服务路由到 InfoCenter，供 `ListServiceRoutes` 查询。

## 8. 客户端建议流程

1. `CreateService`
2. `ListServiceMethods`
3. 按需 `CallService`
4. 开启 keepalive（owner）
5. 完成后 `EndService`

## 9. 与任务模式关系

1. 服务会话与任务模式可并存。
2. 两者共享 NodeControl 与代码版本管理。
3. 任务模式是“task_id 驱动”，服务会话是“method 驱动”。
