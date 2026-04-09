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
6. 暴露节点 `python_version`

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
9. `python_version`
10. `active_runtimes`
11. `service_worker_capacity`
12. `service_worker_used`

### 2.2 `POST /nodes/heartbeat`

节点续租与事实刷新。

关键请求字段：

1. `node_id`
2. `healthy`
3. `metrics`
4. `services`
5. `python_version`
6. `active_runtimes`
7. `service_worker_capacity`
8. `service_worker_used`

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
8. `services`
9. `python_version`
10. `active_runtimes`
11. `active_runtime_count`
12. `service_worker_capacity`
13. `service_worker_used`
14. `service_worker_available`

其中 `services` 会展开每个服务实例的：

1. `service_name`
2. `service_id`
3. `status`
4. `status_text`
5. `worker_count`
6. `alive_workers`
7. `in_flight`
8. `lease_expire_at`
9. `http_base_url`

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

当前 `/ops` 页面除了节点健康、服务实例、worker 数外，还会展示聚合 timing 指标：

1. `calls`
2. `errors`
3. `last_total_ms`
4. `last_setup_ms`
5. `last_build_execute_spec_ms`
6. `last_executor_ms`
7. `last_finalize_ms`
8. `last_child_decode_ms`
9. `last_child_invoke_ms`
10. `last_child_encode_ms`
11. `avg_total_ms`
12. `avg_setup_ms`
13. `avg_build_execute_spec_ms`
14. `avg_executor_ms`
15. `avg_finalize_ms`
16. `avg_child_decode_ms`
17. `avg_child_invoke_ms`
18. `avg_child_encode_ms`
19. `max_total_ms`
20. `last_invoke_ms`

这些指标来自 node 侧服务调用 timing 聚合，并随 heartbeat 同步到 InfoCenter。

计时边界说明：

1. `last_total_ms` / `avg_total_ms`
   - node 侧一次服务调用的总墙钟时间
   - 从 `NodeControlState._invoke_service_call(...)` 进入开始计时
   - 到 node 侧准备好返回 JSON/HTTP body 为止
   - 不包含浏览器/客户端到 node 的网络传输耗时，也不包含最终 socket 写回后的客户端接收耗时
2. `last_setup_ms` / `avg_setup_ms`
   - 父进程前置阶段
   - 包含：方法名校验、service/session 查找、token/status 校验、artifact 查找、`touch_code_last_at(...)`、`_ensure_executor_host_alive_locked()`、`session.in_flight += 1`
   - 不包含真正把请求发给 executor host 的等待时间
3. `last_build_execute_spec_ms` / `avg_build_execute_spec_ms`
   - 父进程构造执行描述阶段
   - 主要就是 `_build_execute_spec(...)`
   - 包含 payload 预处理、managed globals scope/digest 带入、ObjectRef/执行描述包装等
4. `last_executor_ms` / `avg_executor_ms`
   - executor host 往返阶段
   - 从 `_build_execute_spec(...)` 完成、开始调用 `_executor_host.call_service(...)` 起算
   - 到 executor host 返回结果结束
   - 当前它包含：父子进程 IPC、executor 侧排队/等待、用户函数执行、executor 返回结果
   - 所以它仍然不是纯用户函数 CPU 时间
5. `last_finalize_ms` / `avg_finalize_ms`
   - 父进程收尾阶段
   - 成功时主要包含：`StoredResultArtifact -> ResultRef` 转换，以及最终返回体组装
   - 失败时主要包含：错误返回体组装
   - 对 timeout / executor 提前报错这类路径，当前通常记为 `0.0`
6. `last_child_decode_ms` / `avg_child_decode_ms`
   - 子进程内部前半段
   - 包含：artifact/router 加载、`managed globals` 应用、payload 里的 `ObjectRef` 解引用、方法查找
7. `last_child_invoke_ms` / `avg_child_invoke_ms`
   - 子进程里真正执行 `_invoke_user_callable(...)` 的时间
   - 这是当前最接近“用户函数本体耗时”的指标
8. `last_child_encode_ms` / `avg_child_encode_ms`
   - 子进程结果收尾阶段
   - 包含 `_normalize_user_return(...)`，也就是结果标准化、必要时落成 `StoredResultArtifact`
9. `max_total_ms`
   - 当前 service session 生命周期内观测到的最大 `total_ms`
10. `calls` / `errors`
   - 当前 service session 生命周期内的累计调用次数和累计错误次数
11. 这些 `avg_*` 指标是“自该 service session 启动以来的累计平均值”，不是最近 N 次的滑动平均
12. `last_invoke_ms`
   - 为兼容旧字段保留
   - 当前等价于 `last_child_invoke_ms`

当前页面显示：

1. 节点健康状态
2. `schedulable / drain`
3. `python_version`
4. `service_worker` 容量与占用
5. 每个节点当前服务数量
6. 每个服务的 `alive_workers / worker_count / in_flight`
7. 单独的服务实例明细表
8. 当前 `active_runtimes`

节点唯一键说明：

1. `node_id`
   - 现在主要是展示名/逻辑名
   - 可以重复
2. `node_instance_id`
   - 是 InfoCenter 内部真正的节点主键
   - `/ops` 运维动作（`cordon / drain / mark-lost`）现在都按它定位
3. 所以当两个 node 使用相同 `node_id` 时：
   - `/ops` 不会再互相覆盖
   - 页面会同时显示相同的 `node_id` 和各自不同的 `instance_id`

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
        print(node.node_id, node.python_version, node.credit, node.active_runtimes)
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
        runtime=">=py3.11",
        preferred_runtime_key="demo-runtime",
    )
```

排序思路：

1. 命中 `preferred_runtime_key` 的热 node 优先
2. 再按 `credit`
3. 再按 `queued / inflight`

`runtime` 过滤规则：

1. `py3`
2. `py3.11`
3. `>=py3.11`
4. `<=py3.11`

注意：

1. 精确 `py3.11` 只匹配 Python 3.11
2. 只有显式写 `>=py3.11` 才表示“3.11 及以上”

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
