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
4. 对于同名包的重复部署，导入前会清理父包模块缓存，避免导入到旧版本路径。

### 4.2 导出规则（`export_spec`）

1. `decorator`：按装饰器白名单导出（默认推荐）。
2. `explicit`：按 `methods` 显式列表导出。
3. `all`：导出模块所有公开可调用对象。
4. `single`：只导出一个方法（兼容旧入口风格）。

## 5. 方法路由

1. 服务启动时建立 `method -> callable` 路由表。
2. `ListServiceMethods` 返回可调用方法列表。
3. `CallService` 与 HTTP 都按 `method` 分发。
4. `CallService` 可携带 `service_token`；HTTP 可通过 `X-Service-Token` 或 `Authorization: Bearer ...` 传递。

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
2. `CreateService` 返回 `service_token`，owner 需要持久化该 token。
3. owner 周期性 `HeartbeatService(owner_client_id, service_id, service_token)` 续租。
4. 主动结束时调用 `EndService(owner_client_id, service_id, service_token)`。
5. 超时或主动 `EndService` 后进入回收并 `STOPPED`。
6. NodeControl 心跳上报服务路由到 InfoCenter，供 `ListServiceRoutes` 查询。

## 8. 命名约束

1. `service_name` 在活跃服务范围内应视为全局唯一。
2. 服务发现按 `service_name` 聚合，不按 `owner_client_id` 做二次路由区分。
3. 如果需要多租户隔离命名，应由客户端自行生成唯一名字。

## 9. 客户端重启复用

1. 客户端本地应缓存：
   - `owner_client_id`
   - `service_name`
   - `artifact_code_version`
   - 每个节点的 `service_id + service_token`
2. 同一个客户端重启后，如果远端活跃服务与本地缓存满足：
   - 同 `owner_client_id`
   - 同 `service_name`
   - 同 `artifact_code_version`
   则可直接复用，不需要重复上传部署包。
3. 如果同名服务存在但 `artifact_code_version` 不同，默认拒绝覆盖；客户端需要显式选择“replace”语义。
4. 当前 Python 客户端默认提供：
   - `reuse_existing_same_code=True`
   - `replace_existing_if_code_changed=False`

## 10. 客户端建议流程

1. `CreateService`
2. 落盘保存 `service_token`
3. `ListServiceMethods`
4. 按需 `CallService`
5. 开启 keepalive（owner）
6. 完成后 `EndService`

## 11. 与任务模式关系

1. 服务会话与任务模式可并存。
2. 两者共享 NodeControl 与代码版本管理。
3. 任务模式是“task_id 驱动”，服务会话是“method 驱动”。
