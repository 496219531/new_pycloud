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

注册时传入的 `tags` 会保留为 `legacy_node_tags`，用于兼容旧启动参数。它不是中心管理标签。

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
15. `managed_tags`
16. `capability_tags`
17. `legacy_node_tags`
18. `tags`

`tags` 是兼容字段，表示最终筛选标签：

```text
tags = managed_tags + capability_tags + legacy_node_tags
```

其中：

1. `managed_tags` 是管理员在 controlplane 侧维护的人工标签，按 `control_addr`/endpoint 归一后的 `profile_key` 持久化到 `profiles.json`
2. `capability_tags` 是 InfoCenter 根据当前注册/心跳事实自动生成的建议标签，每次刷新都会重算，不写入 `profiles.json`
3. `legacy_node_tags` 是 node 注册时传入的旧 `tags`，短期继续参与最终 `tags`，保证旧 CLI/启动参数兼容

第一版 `profile_key` 使用 endpoint，例如 `http://127.0.0.1:50061` 与 `127.0.0.1:50061` 会归一到同一个 profile。endpoint 改变时，managed profile 不会自动迁移，需要管理员重新设置或后续迁移工具处理。

边界：

1. 推荐 client 只使用 `tags=[...]` 做简单筛选，不要求调用方区分 tag 来源
2. `profile_key` 是实现细节，不建议业务代码直接依赖
3. `capability_tags` 只是事实/建议标签，不是权限、limit 或 policy authority
4. 复杂 node 生命周期管理、资源调度、机器分组、拓扑、配额、自动迁移不在本项目内继续扩展；需要这些能力时，应接入外部成熟工具，再由外部工具调用 `/ops`/API 设置简单 managed tags

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
10. `stop_reason`

其中 `stop_reason` 只在服务已停止或创建失败时有值，用于在 `/ops` 上显示失败原因。

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

当 `healthy_only=true` 时，路由列表只返回健康节点上的 `RUNNING` 服务。已停止或创建失败的服务仍可能出现在 `/nodes` 与 `/ops` 中，用于诊断，但不会进入健康路由。

### 2.5 `GET /ops`

简单 Web 运维页。

默认地址：

```text
http://127.0.0.1:50051/ops
```

当前 `/ops` 页面除了节点健康、服务实例、worker 数外，还会展示聚合 timing 指标：

1. `calls`
2. `errors`
3. `avg_total_ms`
4. `avg_child_decode_ms`
5. `avg_child_invoke_ms`
6. `avg_child_encode_ms`
7. `failure_reason`

这些指标来自 node 侧服务调用 timing 聚合，并随 heartbeat 同步到 InfoCenter。
`failure_reason` 用于显示某条 service/taskpool 的创建失败、executor host 重建失败、owner heartbeat 超时等原因。

如果注册了独立 `job-orchestrator`，`/ops` 页面还会额外显示 `Job Queue` 区块：

1. `current_job_id`
2. `current_status`
3. `waiting`
4. `running`
5. `terminal`
6. `job_count`

同时还会显示 `Recent Jobs` 区块，便于快速查看最近几个 job 的：

1. `job_id`
2. `status`
3. `submitted_at`
4. `finished_at`
5. `final_result`
6. `error`

其中 `job_id` 会直接链接到对应 `job-orchestrator` 的 job 详情 JSON。

详情页会按区块展示 `payload / checkpoint / final_result / results`，其中 `results` 支持：

1. 按 `task_id` / `status` 前端过滤
2. 成功 / 失败结果折叠展开
3. 失败行高亮

`/ops` 页面上的 `Waiting Jobs` 区块还支持对非运行态 job 做上移 / 下移调序。

### 2.6 `POST /data/register`

注册一个 `DataRef` 逻辑句柄。

主要用途：

1. gateway 大对象 relay
2. job staging / delayed resolve
3. result handle 生命周期管理

关键请求字段：

1. `ref`
2. `ttl_sec`
3. `node_id`
4. `node_instance_id`
5. `control_addr`
6. `locator_kind`
7. `locator_token`
8. `replicas`

其中 `replicas` 是控制面 registry 的附加副本元数据。

如果 InfoCenter 配置了 token，`/data/...` 接口需要携带 `X-Infocenter-Token` 或 bearer token。

### 2.7 `GET /data/resolve/{ref_id}`

解析某个 `DataRef` 条目。

返回关键字段：

1. `storage_id`
2. `format`
3. `materialize_as`
4. `ttl_sec`

对外返回是 public 视图，不返回真实 node `control_addr` 或 `replicas`。真实落点只在 registry 内部保存，供 data-plane 下载时解析。

### 2.8 `GET /data/refs/{ref_id}/download`

下载已注册的结果 `DataRef`。

当前语义：

1. caller 只请求 InfoCenter / controlplane 的 `/data/...` 入口
2. data-plane 通过 registry 解析真实 node 副本
3. data-plane 内部请求 node object HTTP download
4. data-plane 边从 node 读，边转发给 caller
5. 响应体是原始 bytes，不在服务端自动 materialize 成 dataframe/json/ndarray

响应 headers 会尽量保留：

1. `X-Pycloud-Ref-Id`
2. `X-Pycloud-Object-Id`
3. `X-Pycloud-Object-Format`
4. `X-Pycloud-Object-Size-Bytes`

边界：

1. 这是结果下载 data-plane，不是上传入口
2. 不暴露真实 node 地址给外部响应体
3. 不把整个对象读入 controlplane 内存
4. client 侧如需 dataframe/ndarray 等还原，应继续使用 SDK materialize helper
5. 如果 InfoCenter 配置了 token，下载请求同样需要认证

### 2.9 `POST /data/touch`

延长某个 `DataRef` 的 TTL。

请求字段：

1. `ref_id`

### 2.10 `POST /data/release`

释放某个 `DataRef` 逻辑句柄。

当前语义：

1. 先释放 registry 条目
2. 对 `consume_on_read` 引用 best-effort 通知 node 释放 pin
3. 真正物理删除仍可能延后到 GC

### 2.11 `GET /data/refs`

列出当前 registry 中的 `DataRef` 条目。

查询参数：

1. `limit`
2. `node_id`
3. `node_instance_id`

计时边界说明：

1. `avg_total_ms`
   - node 侧一次服务调用从进入 `_invoke_service_call(...)` 到准备返回响应为止的累计平均墙钟时间
2. `avg_child_decode_ms`
   - 子进程内部前半段的累计平均耗时
   - 包含：artifact/router 加载、managed globals 应用、payload 里的 `DataRef` 解引用、方法查找
3. `avg_child_invoke_ms`
   - 子进程里真正执行用户函数的累计平均耗时
   - 这是当前最接近“用户函数本体耗时”的指标
4. `avg_child_encode_ms`
   - 子进程结果收尾阶段的累计平均耗时
   - 包含结果标准化，以及必要时落成 `StoredResultArtifact`
5. `calls` / `errors`
   - 当前 service session 生命周期内的累计调用次数和累计错误次数
6. 这些 `avg_*` 指标是“自该 service session 启动以来的累计平均值”，不是最近 N 次的滑动平均

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
   - service/taskpool 动态补偿也按它记录失败副本
3. 所以当两个 node 使用相同 `node_id` 时：
   - `/ops` 不会再互相覆盖
   - 页面会同时显示相同的 `node_id` 和各自不同的 `instance_id`
4. 如果某个 node 重启后修复环境问题：
   - 旧的失败记录仍绑定旧 `node_instance_id`
   - 新进程会获得新的 `node_instance_id`
   - 即使 `node_id` 相同，新实例也可以重新进入 service/taskpool 补偿候选

实例 fencing 约定：

1. 不再引入单独的 `epoch` 概念；`node_instance_id` 本身就是实例生命周期身份。
2. `node_id` 可以持久化和复用，`node_instance_id` 不能在失效后复用。
3. 当 InfoCenter 判定某个实例 heartbeat timeout、被 `mark-lost`、或收到其它失效信号时，会 fence 该 `node_instance_id`。
4. fenced 实例后续 register/heartbeat 会收到 `reset_required=true` / `new_instance_required=true`，node 侧必须清理执行状态并用新的 `node_instance_id` 重新注册。
5. 清理范围包括 service session、taskpool session、executor backend / executor host、worker 进程、service_token / pool_token 对应的执行状态。
6. code cache / object cache 可以保留；它们不是执行租约身份的一部分。
7. 新实例第一次注册时应上报空执行状态，再由 owner / deploy 流程重新下发 service 或 taskpool。

`unhealthy` / `drain` / `cordon` 的区别：

1. `unhealthy`：实例执行状态不可信，需要 fencing 和重置；它不应继续作为 service route、deploy candidate 或 task target。
2. `drain`：不接新业务调用和新 task 分配，但仍接受 owner 控制命令，例如 `update_globals`、`close`、`shutdown`、heartbeat。
3. `cordon`：不接新部署；已有服务是否继续对外服务取决于是否同时 `drain`。
4. service 部署候选应过滤 `unhealthy`、`drain`、`cordon`、`accept_service_deploy=false`。
5. service 调用路由应过滤 `unhealthy` 和 `drain`，但不应仅因 `cordon` 过滤已有 RUNNING 服务。
6. owner 控制命令不应过滤 `drain` / `cordon`；否则旧版本服务可能被隐藏后无法退出或更新。
7. 排他性独占 / 版本冲突检查只应忽略已 fenced 的 unhealthy 实例；drain/cordon 节点上的 STARTING/RUNNING/DRAINING 服务仍要参与冲突判断，避免版本混乱。

### 2.11 运维动作

```text
POST /ops/nodes/{node_instance_id}/cordon
POST /ops/nodes/{node_instance_id}/uncordon
POST /ops/nodes/{node_instance_id}/drain
POST /ops/nodes/{node_instance_id}/undrain
POST /ops/nodes/{node_instance_id}/enable
POST /ops/nodes/{node_instance_id}/disable
POST /ops/nodes/{node_instance_id}/managed-tags
POST /ops/nodes/{node_instance_id}/notes
POST /ops/nodes/{node_instance_id}/mark-lost
```

说明：路径名仍是 `/ops/nodes/...`，但参数语义是 `node_instance_id`。页面上的操作按钮会自动使用对应实例 id；手写 curl 时不要只填可重复的 `node_id`。

`cordon/uncordon/enable/disable/drain/undrain/managed-tags/notes` 会落到 endpoint profile，因此 node 重启后只要 `control_addr` 不变，人工标签、enabled/drain、notes 都会恢复。`mark-lost` 仍是当前 instance 的故障标记，不写入 endpoint profile。

`managed-tags` 只接受简单字符串标签。它不是资源规格、权限声明、limit 配置或 machine inventory。

## 3. Python 客户端

### 3.1 查看节点

```python
from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

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

当前任务选点已经接入统一 scheduler 候选/评分框架：

1. 先做硬过滤
   - `healthy`
   - `schedulable`
   - `drain`
   - `control_addr`
   - `runtime` 兼容
2. 再走 `JOBQUEUE_DEFAULT` profile 评分
   - `predicted_busy`
   - `node_inflight`
   - `alive_workers`
   - `worker_capacity`
   - `credit`
3. 同分候选再做 round-robin 打散

`preferred_runtime_key` 仍然会作为候选 metadata 进入选择过程，但不再是单独一套排序器。

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
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1-xxxxxxxxxxxx/cordon
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1-xxxxxxxxxxxx/uncordon
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1-xxxxxxxxxxxx/drain
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1-xxxxxxxxxxxx/undrain
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1-xxxxxxxxxxxx/mark-lost
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
