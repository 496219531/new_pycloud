# InfoCenter HTTP

## 1. 当前定位

`InfoCenter` 当前是轻量 `HTTP + JSON` 控制面。

默认部署时，它和 `Gateway` 合并为一个 `controlplane` 进程，共用一个端口。

职责：

1. 节点注册与心跳
2. 节点事实查询
3. 服务路由查询
4. 轻量运维页面 `/ops`
5. 为任务模式提供热点提示字段

它本身不代理任务执行。

## 2. 主要接口

### 2.1 `POST /nodes/register`

首次注册节点。

关键请求字段：

1. `node_id`
2. `control_addr`
3. `capacity`
4. `queue_capacity`
5. `tags`
6. `version`
7. `metadata`
8. `services`
9. `active_runtimes`
10. `service_worker_capacity`
11. `service_worker_used`

### 2.2 `POST /nodes/heartbeat`

节点续租与事实刷新。

关键请求字段：

1. `node_id`
2. `healthy`
3. `metrics`
4. `services`
5. `active_runtimes`
6. `service_worker_capacity`
7. `service_worker_used`

### 2.3 `GET /nodes`

查询节点列表。

查询参数：

1. `healthy_only`
2. `tags`
3. `limit`

返回节点当前常用字段：

1. `healthy`
2. `schedulable`
3. `drain`
4. `credit`
5. `queued`
6. `inflight`
7. `loaded_services`
8. `active_runtimes`
9. `active_runtime_count`
10. `service_worker_capacity`
11. `service_worker_used`
12. `service_worker_available`

### 2.4 `GET /services/routes`

查询服务路由。

查询参数：

1. `service_name`
2. `healthy_only`
3. `limit`

返回关键字段：

1. `service_name`
2. `service_id`
3. `node_id`
4. `control_addr`
5. `node_healthy`
6. `worker_count`
7. `alive_workers`
8. `in_flight`
9. `http_base_url`

### 2.5 `GET /ops`

简单 Web 运维页。

默认地址：

```text
http://127.0.0.1:50051/ops
```

当前页面显示：

1. 节点健康状态
2. `schedulable / drain`
3. `service_worker` 容量与占用
4. 当前已加载的服务名
5. 当前 `active_runtimes`

### 2.6 运维动作

```text
POST /ops/nodes/{node_id}/cordon
POST /ops/nodes/{node_id}/uncordon
POST /ops/nodes/{node_id}/drain
POST /ops/nodes/{node_id}/undrain
```

## 3. Python 客户端

### 3.1 查看节点

```python
from pycloud_parallel.controlplane.client import InfoCenterClient

with InfoCenterClient("127.0.0.1:50051", timeout_sec=5.0) as client:
    nodes = client.list_nodes(healthy_only=False, tags=["compute"], limit=100)
    for node in nodes:
        print(node.node_id, node.credit, node.active_runtimes)
```

### 3.2 查询服务路由

```python
with InfoCenterClient("127.0.0.1:50051", timeout_sec=5.0) as client:
    routes = client.list_service_routes(service_name="square-service", healthy_only=True, limit=100)
    for route in routes:
        print(route.node_id, route.service_id)
```

### 3.3 任务选点

```python
with InfoCenterClient("127.0.0.1:50051", timeout_sec=5.0) as client:
    nodes = client.select_task_nodes(
        healthy_only=True,
        tags=["compute"],
        node_count=2,
        preferred_runtime_key="demo-runtime",
    )
```

排序思路：

1. 命中 `preferred_runtime_key` 的热 node 优先
2. 再按 `credit`
3. 再按 `queued / inflight`

## 4. 命令行与 curl

### 4.1 查看节点

```bash
curl 'http://127.0.0.1:50051/nodes?healthy_only=false&limit=100' | jq
```

### 4.2 查看服务路由

```bash
curl 'http://127.0.0.1:50051/services/routes?service_name=square-service&healthy_only=false' | jq
```

### 4.3 运维操作

```bash
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1/cordon
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1/uncordon
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1/drain
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1/undrain
```

## 5. 与 Gateway 的关系

同一个 `controlplane` 端口还会挂载 Gateway 路径：

1. `POST /svc/{service_name}/call/{method}`
2. `GET /svc/{service_name}/methods`
3. `GET /svc/{service_name}/status`

也就是说：

1. `/nodes`、`/services/routes`、`/ops` 是 InfoCenter
2. `/svc/...` 是 Gateway
3. 默认同端口共存
