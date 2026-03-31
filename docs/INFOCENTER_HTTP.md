# InfoCenter HTTP 说明

## 1. 当前定位

`InfoCenter` 现在是一个轻量的 `HTTP + JSON` 控制面服务。

它负责：

1. 节点注册
2. 节点心跳
3. 服务路由聚合
4. 简单运维开关
5. 一个极简 Web 页面

它不再提供 gRPC service。

## 2. 当前接口

### 2.1 `POST /nodes/register`

节点首次注册。

请求体关键字段：

1. `node_id`
2. `control_addr`
3. `capacity`
4. `queue_capacity`
5. `tags`
6. `version`
7. `metadata`
8. `service_worker_capacity`
9. `service_worker_used`
10. `services`

返回：

1. `ok`
2. `heartbeat_interval_sec`
3. 当前节点快照

### 2.2 `POST /nodes/heartbeat`

节点心跳续租。

请求体关键字段：

1. `node_id`
2. `healthy`
3. `metrics`
4. `service_worker_capacity`
5. `service_worker_used`
6. `services`

返回：

1. `ok`
2. `accepted`
3. `next_heartbeat_in_sec`

### 2.3 `GET /nodes`

查询节点列表。

支持查询参数：

1. `healthy_only`
2. `tags`
3. `limit`

返回的节点信息里，当前比较重要的字段有：

1. `healthy`
2. `schedulable`
3. `drain`
4. `service_worker_capacity`
5. `service_worker_used`
6. `service_worker_available`
7. `loaded_services`

### 2.4 `GET /services/routes`

查询服务路由。

支持查询参数：

1. `service_name`
2. `healthy_only`
3. `limit`

返回每条路由的核心字段：

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

返回一个简单 HTML 页面，用来查看：

1. 节点是否健康
2. 是否可调度
3. 是否处于 drain
4. service worker 容量与使用量
5. 当前节点已加载的服务名

### 2.6 节点运维接口

1. `POST /ops/nodes/{node_id}/cordon`
2. `POST /ops/nodes/{node_id}/uncordon`
3. `POST /ops/nodes/{node_id}/drain`
4. `POST /ops/nodes/{node_id}/undrain`

当前这些接口只改 InfoCenter 里的调度状态，不做复杂自动迁移。

## 3. 与 NodeControl 的关系

当前关系很清晰：

1. `InfoCenter` 只做注册、心跳、查询和运维状态。
2. `NodeControl` 才是真正执行任务和服务的地方。
3. `NodeControl` 通过 registrar 把节点状态和服务路由上报给 InfoCenter。

## 4. 路由可见性的时间窗口

这是当前实现里一个很重要的现实细节：

1. 服务在 NodeControl 里创建成功后，不是立刻同步出现在 InfoCenter。
2. 需要等节点下一次 heartbeat，把新的 `services` 列表带上去。
3. 所以“刚部署完立刻查不到路由”是可能发生的。

当前默认心跳周期已经压短到 5 秒，降低这种等待感。

## 5. 当前节点选择依赖哪些字段

客户端在 `deploy_from_infocenter(...)` 里选节点时，会关注：

1. `healthy`
2. `schedulable`
3. `drain`
4. `service_worker_available`

也就是说：

1. 节点不健康，不选。
2. 被 `cordon`，不选。
3. 被 `drain`，不选。
4. 剩余 service worker 多的节点优先。

## 6. 简单排障

### 6.1 查节点状态

```bash
curl 'http://127.0.0.1:50051/nodes?healthy_only=false&limit=100'
```

### 6.2 查服务路由

```bash
curl 'http://127.0.0.1:50051/services/routes?service_name=square-service&healthy_only=false&limit=100'
```

### 6.3 看运维页面

浏览器打开：

```text
http://127.0.0.1:50051/ops
```

### 6.4 用脚本查看

```bash
./scripts/start_services.sh status
```

它会展示：

1. 进程状态
2. `Loaded Services By Node`

## 7. 参考

1. [README.md](/Users/hkk/Documents/new_pycloud/README.md)
2. [ARCHITECTURE_V1.md](/Users/hkk/Documents/new_pycloud/ARCHITECTURE_V1.md)
3. [API_CONTRACT_V1.md](/Users/hkk/Documents/new_pycloud/API_CONTRACT_V1.md)
4. [infocenter_http.py](/Users/hkk/Documents/new_pycloud/src/pycloud_parallel/controlplane/infocenter_http.py)
