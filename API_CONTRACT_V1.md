# PyCloud HTTP / JSON 契约（V1）

> 当前实现里，`HTTP + JSON` 覆盖三部分：
> 1. `InfoCenter` 控制面
> 2. `Gateway` 服务入口
> 3. `NodeControl` 管理面与节点服务数据面

## 1. InfoCenter HTTP API

### 1.1 `POST /nodes/register`

用途：节点首次注册。

请求体示例：

```json
{
  "node_instance_id": "node-1-a1b2c3d4e5f6",
  "node_id": "node-1",
  "control_addr": "127.0.0.1:50061",
  "capacity": 4,
  "queue_capacity": 1000,
  "tags": ["compute"],
  "version": "v1",
  "metadata": {"role": "compute-node"},
  "python_version": "py3.13",
  "service_worker_capacity": 4,
  "service_worker_used": 0,
  "active_runtimes": [],
  "services": []
}
```

响应示例：

```json
{
  "ok": true,
  "heartbeat_interval_sec": 5,
  "node": {
    "node_instance_id": "node-1-a1b2c3d4e5f6",
    "node_id": "node-1",
    "control_addr": "127.0.0.1:50061",
    "healthy": true,
    "schedulable": true,
    "drain": false,
    "python_version": "py3.13",
    "service_worker_available": 4,
    "loaded_services": []
  }
}
```

### 1.2 `POST /nodes/heartbeat`

用途：节点续租并上报当前服务路由。

请求体示例：

```json
{
  "node_instance_id": "node-1-a1b2c3d4e5f6",
  "node_id": "node-1",
  "healthy": true,
  "metrics": {
    "queued": 0,
    "inflight": 0,
    "running": 0,
    "credit": 1000,
    "cpu_percent": 0.0,
    "mem_percent": 0.0
  },
  "python_version": "py3.13",
  "service_worker_capacity": 4,
  "service_worker_used": 1,
  "active_runtimes": ["runtime-hot-a"],
  "services": [
    {
      "service_name": "square-service",
      "service_id": "svc-001",
      "status": 2,
      "status_text": "SERVICE_STATUS_RUNNING",
      "worker_count": 1,
      "alive_workers": 1,
      "in_flight": 0,
      "resource_health": "running",
      "readiness": "ready",
      "create_stage": "ready",
      "stop_reason": "",
      "http_base_url": "http://127.0.0.1:18081/svc/svc-001"
    }
  ]
}
```

响应示例：

```json
{
  "ok": true,
  "accepted": true,
  "next_heartbeat_in_sec": 5
}
```

### 1.3 `GET /nodes`

查询参数：

1. `healthy_only=true|false`
2. `tags=compute,gpu`
3. `limit=100`

响应示例：

```json
{
  "ok": true,
  "nodes": [
    {
      "node_instance_id": "node-1-a1b2c3d4e5f6",
      "node_id": "node-1",
      "control_addr": "127.0.0.1:50061",
      "healthy": true,
      "schedulable": true,
      "drain": false,
      "capacity": 4,
      "queue_capacity": 1000,
      "python_version": "py3.13",
      "active_runtimes": ["runtime-hot-a"],
      "service_worker_capacity": 4,
      "service_worker_used": 1,
      "service_worker_available": 3,
      "loaded_services": ["square-service"]
    }
  ]
}
```

### 1.4 `GET /services/routes`

查询参数：

1. `service_name=<name>`
2. `healthy_only=true|false`
3. `limit=500`

响应示例：

```json
{
  "ok": true,
  "routes": [
    {
      "service_name": "square-service",
      "service_id": "svc-001",
      "status": 2,
      "node_instance_id": "node-1-a1b2c3d4e5f6",
      "node_id": "node-1",
      "control_addr": "127.0.0.1:50061",
      "node_healthy": true,
      "worker_count": 1,
      "alive_workers": 1,
      "in_flight": 0,
      "lease_expire_at": "2026-03-31T00:00:00+00:00",
      "http_base_url": "http://127.0.0.1:18081/svc/svc-001"
    }
  ]
}
```

### 1.5 运维接口

1. `GET /ops`
2. `POST /ops/nodes/{node_instance_id}/cordon`
3. `POST /ops/nodes/{node_instance_id}/uncordon`
4. `POST /ops/nodes/{node_instance_id}/drain`
5. `POST /ops/nodes/{node_instance_id}/undrain`
6. `POST /ops/nodes/{node_instance_id}/mark-lost`

这些接口当前是轻量运维开关，不做复杂的自动迁移。

节点标识约定：

1. `node_id`
   - 逻辑展示名
   - 允许重复
2. `node_instance_id`
   - InfoCenter 内部真正的节点主键
   - `/ops` 运维动作按它定位

`/ops` 会把同名 service/taskpool 的当前实例和历史诊断记录合并展示。当前状态优先来自健康运行实例；已停止实例的 `stop_reason` / `failure_at` 只进入 `failure_reason`，不应把当前运行实例染成失败态。

## 2. 服务 HTTP 数据面

### 2.1 `POST /svc/{service_id}/call/{method}`

查询参数：

1. `timeout_sec`

Header 可选：

1. `X-Service-Token: <token>`
2. `Authorization: Bearer <token>`

请求体示例：

```json
{"x": 7}
```

当前调用约定：

1. 普通 JSON object 会按 kwargs 展开
2. 推荐服务函数写成 `def square(x=0, **_kwargs): ...`
3. 如果要传位置参数，可用：

```json
{"args": [7], "kwargs": {"scale": 2}}
```

成功响应：

```json
{
  "ok": true,
  "method": "square",
  "data": {"x": 7, "y": 49}
}
```

失败响应：

```json
{
  "ok": false,
  "method": "square",
  "error_type": "ValueError",
  "error": "bad input"
}
```

### 2.2 `GET /svc/{service_id}/status`

成功响应示例：

```json
{
  "ok": true,
  "service": {
    "service_id": "svc-001",
    "owner_client_id": "demo-owner",
    "service_name": "square-service",
    "status": 2,
    "worker_count": 1,
    "alive_workers": 1,
    "in_flight": 0,
    "http_base_url": "http://127.0.0.1:18081/svc/svc-001",
    "methods": ["square"]
  }
}
```

## 3. 当前协议定位

1. InfoCenter：HTTP 为当前正式协议。
2. NodeControl 管理面：HTTP 为当前正式协议。
3. 服务调用：Gateway / discovery 是路由方式，底层都走 HTTP。

## 4. NodeControl 管理面

以下能力当前由 NodeControl HTTP 管理面提供：

1. `CreateService`
2. `ListServiceMethods`
3. `HeartbeatService`
4. `EndService`
5. `GetServiceStatus`

这也包括：

1. 上传代码时的 `dependency_allowlist`
2. 创建服务时的 `dependency_allowlist`

它们属于 NodeControl 管理面，不是外部 service gateway 数据面。

## 5. 参考文档

1. `SERVICE_SESSION_PROTOCOL_V1.md`
2. `ARCHITECTURE_V1.md`
