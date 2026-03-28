# PyCloud 字段级接口文档（V1）

> 对应架构文档：`ARCHITECTURE_V1.md`  
> 版本：`v1`（内网，轻量模式，单 InfoCenter 实例）
> 状态：该文档为 REST 版本草案；当前生效基线为 gRPC 版本 `GRPC_CONTRACT_V1.md` 与 `proto/pycloud_v1.proto`。

## 1. 通用约定

1. 协议：HTTP/JSON（文件上传使用 `multipart/form-data`）。
2. 路径前缀：`/v1/...`
3. 时间格式：UTC ISO-8601（例如 `2026-03-28T09:30:00Z`）。
4. 认证（建议）：`Authorization: Bearer <token>`。
5. 关键 ID：
   - `node_id`：节点唯一标识（建议机器唯一 + 环境前缀）
   - `task_id`：任务唯一标识（全局唯一，推荐 UUID）
   - `code_version`：代码版本（`sha256` 或版本号）
   - `attempt`：重试次数（从 1 开始）
6. 结果语义：
   - 有返回（成功或业务报错）为终态，不重试。
   - 无返回（失联/超时/网络中断）判 `FAILED_INFRA`，最多重试 3 次。

## 2. 枚举定义

### 2.1 任务状态 `task_status`

- `QUEUED`
- `RUNNING`
- `SUCCEEDED`
- `FAILED_USER`
- `FAILED_INFRA`
- `CANCELLED`

### 2.2 执行模式 `execution_mode`

- `persistent`
- `ephemeral`

### 2.3 统一错误码 `error_code`

- `INVALID_REQUEST`
- `UNAUTHORIZED`
- `UNKNOWN_NODE`
- `UNKNOWN_CODE_VERSION`
- `DUPLICATE_TASK`
- `NO_CREDIT`
- `QUEUE_FULL`
- `TASK_NOT_FOUND`
- `NODE_DRAINING`
- `INTERNAL_ERROR`

## 3. InfoCenter API

## 3.1 `POST /v1/nodes/register`

节点首次注册或重注册。

请求体：

```json
{
  "node_id": "node-sh-01",
  "control_url": "http://10.0.0.21:8081",
  "capacity": 32,
  "queue_capacity": 4000,
  "tags": ["cpu", "shanghai-a"],
  "version": "1.0.0",
  "metadata": {
    "host": "10.0.0.21",
    "region": "cn-sh"
  }
}
```

响应体：

```json
{
  "ok": true,
  "node_id": "node-sh-01",
  "lease_ttl_sec": 90,
  "heartbeat_interval_sec": 30,
  "server_time": "2026-03-28T09:30:00Z"
}
```

## 3.2 `POST /v1/nodes/heartbeat`

节点心跳续约。

请求体：

```json
{
  "node_id": "node-sh-01",
  "timestamp": "2026-03-28T09:30:30Z",
  "metrics": {
    "queued": 120,
    "inflight": 300,
    "running": 280,
    "credit": 3580,
    "cpu_percent": 73.2,
    "mem_percent": 61.5
  },
  "healthy": true,
  "reason": ""
}
```

响应体：

```json
{
  "ok": true,
  "accepted": true,
  "next_heartbeat_in_sec": 30,
  "drain": false
}
```

## 3.3 `GET /v1/nodes`

客户端查询可用节点。

查询参数：

- `healthy`：`true/false`（默认 `true`）
- `tag`：可重复传入，如 `tag=cpu&tag=shanghai-a`
- `limit`：默认 `100`

响应体：

```json
{
  "ok": true,
  "nodes": [
    {
      "node_id": "node-sh-01",
      "control_url": "http://10.0.0.21:8081",
      "healthy": true,
      "last_seen_at": "2026-03-28T09:30:30Z",
      "capacity": 32,
      "queue_capacity": 4000,
      "queued": 120,
      "inflight": 300,
      "credit": 3580,
      "tags": ["cpu", "shanghai-a"]
    }
  ]
}
```

## 4. NodeControl（客户端可调用）API

## 4.1 `POST /v1/code/upload`

上传业务执行代码（先发代码，再发任务）。

请求：`multipart/form-data`

- `file`：代码包（wheel/zip）
- `sha256`：文件哈希
- `runtime`：例如 `py3.11`
- `entry_module`：入口模块（可选）
- `entry_callable`：入口函数（可选）

响应体：

```json
{
  "ok": true,
  "code_version": "sha256:ab12cd34...",
  "cached": false,
  "size_bytes": 1048576,
  "created_at": "2026-03-28T09:32:10Z"
}
```

## 4.2 `POST /v1/tasks/submit`

批量提交任务（流式派发建议小批量多次调用）。

请求体：

```json
{
  "client_id": "client-a",
  "code_version": "sha256:ab12cd34...",
  "execution_mode": "persistent",
  "tasks": [
    {
      "task_id": "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac001",
      "payload": {
        "x": 1,
        "y": 2
      },
      "timeout_hint_sec": 600,
      "priority": 1
    },
    {
      "task_id": "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac002",
      "payload": {
        "x": 3,
        "y": 4
      },
      "timeout_hint_sec": 600,
      "priority": 1
    }
  ]
}
```

响应体：

```json
{
  "ok": true,
  "accepted": [
    {
      "task_id": "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac001",
      "status": "QUEUED"
    }
  ],
  "rejected": [
    {
      "task_id": "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac002",
      "error_code": "NO_CREDIT",
      "message": "node queue/inflight is full"
    }
  ],
  "node_credit": 3579
}
```

## 4.3 `GET /v1/tasks/result`

拉取结果（建议长轮询，避免忙轮询）。

查询参数：

- `client_id`：必填
- `limit`：默认 `100`，最大 `1000`
- `wait_ms`：默认 `0`，建议 `1000-5000`
- `cursor`：可选，增量拉取游标

响应体：

```json
{
  "ok": true,
  "results": [
    {
      "task_id": "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac001",
      "status": "SUCCEEDED",
      "attempt": 1,
      "started_at": "2026-03-28T09:32:20Z",
      "finished_at": "2026-03-28T09:32:21Z",
      "result": {
        "sum": 3
      },
      "error": null
    },
    {
      "task_id": "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac003",
      "status": "FAILED_USER",
      "attempt": 1,
      "started_at": "2026-03-28T09:32:22Z",
      "finished_at": "2026-03-28T09:32:23Z",
      "result": null,
      "error": {
        "type": "ValueError",
        "message": "bad input"
      }
    }
  ],
  "next_cursor": "1711618343000-12"
}
```

## 4.4 `POST /v1/tasks/cancel`

取消尚未完成的任务。

请求体：

```json
{
  "client_id": "client-a",
  "task_ids": [
    "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac001",
    "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac004"
  ],
  "reason": "manual stop"
}
```

响应体：

```json
{
  "ok": true,
  "cancelled": [
    "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac004"
  ],
  "not_found": [],
  "already_done": [
    "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac001"
  ]
}
```

## 4.5 `GET /v1/metrics`

节点运行指标查询。

响应体：

```json
{
  "ok": true,
  "node_id": "node-sh-01",
  "queued": 120,
  "inflight": 300,
  "running": 280,
  "credit": 3580,
  "queue_capacity": 4000,
  "worker_capacity": 32,
  "cpu_percent": 73.2,
  "mem_percent": 61.5,
  "uptime_sec": 86400
}
```

## 5. NodeControl <-> Worker 内部 API（本机）

> 这一组接口仅限本机调用，可走 Unix Socket 或 localhost HTTP。

## 5.1 `POST /v1/worker/poll`

worker 拉任务。

请求体：

```json
{
  "worker_id": "node-sh-01-wp-07",
  "max_tasks": 1
}
```

响应体（有任务）：

```json
{
  "ok": true,
  "task": {
    "task_id": "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac001",
    "code_version": "sha256:ab12cd34...",
    "attempt": 1,
    "execution_mode": "persistent",
    "payload": {
      "x": 1,
      "y": 2
    }
  }
}
```

响应体（无任务）：

```json
{
  "ok": true,
  "task": null
}
```

## 5.2 `POST /v1/worker/heartbeat`

worker 上报任务心跳或进度。

请求体：

```json
{
  "worker_id": "node-sh-01-wp-07",
  "task_id": "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac001",
  "attempt": 1,
  "timestamp": "2026-03-28T09:33:00Z",
  "child_alive": true,
  "progress": {
    "done": 500,
    "total": 2000
  }
}
```

响应体：

```json
{
  "ok": true,
  "accepted": true,
  "cancel_requested": false
}
```

## 5.3 `POST /v1/worker/result`

worker 上报执行结果（成功或业务错误都算有返回）。

请求体：

```json
{
  "worker_id": "node-sh-01-wp-07",
  "task_id": "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac001",
  "attempt": 1,
  "status": "SUCCEEDED",
  "result": {
    "sum": 3
  },
  "error": null,
  "started_at": "2026-03-28T09:32:20Z",
  "finished_at": "2026-03-28T09:32:21Z"
}
```

失败示例（业务异常）：

```json
{
  "worker_id": "node-sh-01-wp-08",
  "task_id": "f0e5d5aa-2b1d-4d25-9c7f-18b9bafac003",
  "attempt": 1,
  "status": "FAILED_USER",
  "result": null,
  "error": {
    "type": "ValueError",
    "message": "bad input"
  },
  "started_at": "2026-03-28T09:32:22Z",
  "finished_at": "2026-03-28T09:32:23Z"
}
```

响应体：

```json
{
  "ok": true,
  "accepted": true
}
```

## 6. 失败判定与重试规则（字段级）

1. 业务失败：`/v1/worker/result` 上报 `status=FAILED_USER`。
   - 状态终止，不重试。
2. 基础设施失败：
   - 条件：连续 `90s` 未收到对应任务心跳或结果。
   - 状态记为 `FAILED_INFRA`，可重试。
3. 重试字段：
   - `attempt` 每次重试 +1，最大 `3`。
   - 超过 `3` 次后终态 `FAILED_INFRA`。
4. 晚到数据丢弃：
   - 若上报结果的 `attempt` 小于当前活跃 `attempt`，返回 `accepted=false`。

## 7. 统一错误响应格式

HTTP 非 2xx 时，返回：

```json
{
  "ok": false,
  "error_code": "INVALID_REQUEST",
  "message": "field `task_id` is required",
  "request_id": "req-20260328-0001"
}
```

## 8. 建议的 HTTP 状态码

1. `200`：成功（包括部分成功，详见 body）。
2. `400`：参数错误。
3. `401`：认证失败。
4. `404`：节点/任务/代码版本不存在。
5. `409`：重复任务冲突（`DUPLICATE_TASK`）。
6. `429`：节点无可用 credit（`NO_CREDIT`/`QUEUE_FULL`）。
7. `503`：节点排空或暂不可用（`NODE_DRAINING`）。
8. `500`：服务内部错误。
