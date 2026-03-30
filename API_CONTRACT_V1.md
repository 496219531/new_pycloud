# PyCloud API 文档（V1）

> 说明：本仓库当前生效基线是 gRPC（`proto/pycloud_v1.proto`）。  
> 本文保留 REST 视角，用于网关映射与外部系统对接说明。

## 1. 状态说明

1. gRPC：当前主协议（已实现）。
2. HTTP：当前仅服务会话数据面与状态查询是稳定实现。
3. 其余 REST 路径保留为草案，不作为当前强约束。

## 2. 当前已实现 HTTP 路径

### 2.1 调用服务方法

`POST /svc/{service_id}/call/{method}?timeout_sec=60`

Headers（可选）：

1. `X-Service-Token: <token>`
2. `Authorization: Bearer <token>`

请求体（JSON）：

```json
{"value": 3}
```

成功响应：

```json
{"ok": true, "method": "square", "data": {"value": 3, "square": 9}}
```

失败响应：

```json
{"ok": false, "method": "square", "error_type": "UserError", "error": "..."}
```

### 2.2 查询服务状态

`GET /svc/{service_id}/status`

成功响应示例：

```json
{
  "ok": true,
  "service": {
    "service_id": "...",
    "owner_client_id": "...",
    "service_name": "...",
    "status": 2,
    "worker_count": 4,
    "alive_workers": 4,
    "in_flight": 0,
    "http_base_url": "http://127.0.0.1:18080/svc/...",
    "methods": ["square", "cube"]
  }
}
```

## 3. gRPC 到 HTTP 的映射建议

1. `CallService` <-> `POST /svc/{service_id}/call/{method}`
2. `GetServiceStatus` <-> `GET /svc/{service_id}/status`
3. `ListServiceMethods`：建议保留 gRPC 为主；若要补 HTTP 可扩展为 `GET /svc/{service_id}/methods`
4. `HeartbeatService / EndService` 当前只定义 gRPC 管理面，不建议通过 HTTP 暴露。

## 4. 上传与导出语义（与 gRPC 对齐）

1. 包格式：`py / tar.gz / zip / whl`
2. 导出规则：`decorator / explicit / all / single`
3. 推荐：`decorator + pycloud_export`

## 5. 服务管理权限与重启复用

1. `CreateService` 返回 `service_token`。
2. 管理面接口 `HeartbeatService / EndService` 需要 `owner_client_id + service_id + service_token`。
3. 数据面 `CallService` / HTTP `POST /svc/{service_id}/call/{method}` 可选择携带 `service_token`。
4. Python 客户端会把 `service_id/service_token` 本地落盘，用于客户端重启后继续续租或主动结束。
5. `MultiNodeServiceGroup.deploy_from_infocenter(...)` 当前默认策略：
   - 同 `owner_client_id + service_name + code_version` 时直接复用已有活跃服务
   - 同名但代码版本不同默认拒绝
   - 显式 `replace_existing_if_code_changed=True` 才允许先结束旧服务再重建

## 6. 文档参考

1. gRPC 详细契约：`GRPC_CONTRACT_V1.md`
2. 服务会话细节：`SERVICE_SESSION_PROTOCOL_V1.md`
3. 架构层说明：`ARCHITECTURE_V1.md`

## 7. 运维观察（脚本）

配套脚本：

```bash
./scripts/start_services.sh status
```

输出包含：

1. `infocenter/node-*` 进程状态
2. `Loaded Services By Node`（每个节点当前加载的服务名）
