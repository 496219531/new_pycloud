# 模块化客户端说明

当前推荐保留的模块化入口有 5 个：

1. `Service`
2. `TaskPool`
3. `JobQueue`
4. `DataRef`
5. `export`

说明：

1. 旧共享任务池模式已经移除
2. 任务执行现在优先走原生 `TaskPool`
3. 大任务排队入口优先走 `JobQueue`
4. 启动时固定服务使用 `Service.startup(...)`，运行期动态部署使用 `Service.deploy(...)`
5. `Service / TaskPool / JobQueue` 当前共用统一 scheduler 核心来回答“这次该选谁”
6. `TaskPool` 的批量 refill / `Service` 的 RPC 发送循环仍然各自保留，不强行混成一套

分工：

| 类 | 模式 | 用途 |
|---|---|---|
| `Service` | Service | `deploy(...)` 动态部署、`connect(...)` 连接已有服务、`startup(...)` 启动时挂载固定 module |
| `TaskPool` | TaskPool | 创建原生专属任务池并执行 subtasks |
| `JobQueue` | JobQueue | 提交大任务、排队、单活调度 |
| `DataRef` | Data | 大对象 / 大结果 / 文件引用 |
| `export` | Artifact | 模块 / package 导出装饰器 |

推荐资料：

- [QUICK_START.md](QUICK_START.md)
- [TASK_CLIENT_GUIDE.md](TASK_CLIENT_GUIDE.md)
- [SERVICE_GUIDE.md](SERVICE_GUIDE.md)
- [SERVICE_GATEWAY_GUIDE.md](SERVICE_GATEWAY_GUIDE.md)

## 序列化模式

当前公开面已经定义 3 个 serialization mode：

1. `legacy_v1`
   - 当前老兼容模式
   - 继续基于 Arrow-compatible / Struct-safe dict
2. `structured_v1`
   - 显式 versioned 结构化 codec
   - bytes 通过结构化 sentinel 表达
3. `pickle_stable_v1`
   - 受信环境高保真 Python codec
   - 外层仍是 pickle
   - 但 `DataFrame / Series / ndarray` 先转稳定 schema 再 pickle
   - 其中 ndarray 的原始数据直接保留为 raw bytes，不再先 base64 文本化

边界说明：

1. `structured_v1` 不是 pickle
2. `pickle_stable_v1` 也不是“任意 Python 对象全支持”的通用 pickle 模式
3. 当前明确支持：
   - pandas `DataFrame / Series / Index`
   - numpy `ndarray`（非 `dtype=object`）
   - 结构化标量 / 容器
4. `ndarray dtype=object` 明确不支持

分层原则：

1. `legacy_v1 / structured_v1 / pickle_stable_v1` 首先是对象 codec 层
2. JSON / Struct / protobuf bytes / object upload blob 属于 transport 容器层
3. `pickle_stable_v1` 不会为了 JSON/Struct 预先把 schema 里的 raw bytes 文本化
4. 如果当前 transport 是 JSON/Struct-only，base64 或拒绝都由 transport 适配层决定
5. 因此：
   - codec 层表达对象本身
   - transport 层表达“这个对象怎么进当前容器”

当前 NodeControl/protobuf 主链已经有两条并行 transport 通道：

1. 旧 `Struct` 通道
   - 继续兼容 `legacy_v1`
2. 新 `TransportPayload(codec, version, payload)` 通道
   - `pickle_stable_v1` 优先走这条 bytes 通道
   - 旧字段仍保留以保证兼容

当前 HTTP 主链也有两条并行 transport 通道：

1. 旧 JSON 通道
   - `Content-Type: application/json`
   - 继续兼容 `legacy_v1`
2. 新 bytes 通道
   - `Content-Type: application/x-pycloud-transport`
   - `X-Pycloud-Codec`
   - `X-Pycloud-Transport-Version`
   - `pickle_stable_v1` 优先走这条 bytes 通道

这些 mode 当前已经统一作用于：

1. `put_data() / put_dataframe() / put_ndarray() / put_json()`
2. `Service.connect(...).method(...)` 的主调用链
3. `TaskPool.submit_payloads(...)` 与 task result decode
4. service HTTP request / response
5. `DataRef` 对象上传与物化

公开入口上的默认值优先级：

1. 单次调用显式 `serialization_mode=...`
2. 当前 session 的 `serialization_mode`
3. 当前 system mode / env
4. 默认回退 `legacy_v1`

权限边界：

1. `Service.connect(...)`
2. `Service.deploy(...)`
3. `TaskPool.open(...)`
4. `JobQueue.connect(...)`
5. `put_data() / put_dataframe() / put_ndarray() / put_json()`

这些边界负责选择 mode；内部 transport/helper 只消费和传递 mode，不再私自重选默认值。

另外：

1. 非 legacy transport body 必须显式带 codec/version envelope
2. decode 端优先按 envelope 解码
3. 没有 envelope 时只按 `legacy_v1` 兜底，不再按全局 env 猜 mode
4. 接收端会按当前边界上下文重新校验 declared mode，不是发送端声明什么就无条件接受什么
5. `gateway_public` 默认硬性禁止 `pickle_stable_v1`

## 会话级 Effective Policy

当前 `Service / TaskPool / JobQueue` 的公开面已经开始统一走三层模型：

1. `Policy Profile`
   - 中心统一策略模板
2. `Tags / Health / Runtime Filtering`
   - 决定哪些 node 参与当前 session
3. `Effective Policy`
   - 会话创建时计算并冻结的实际执行策略

对用户侧最重要的影响是：

1. 客户端可以表达 `serialization_mode` 偏好，但不能直接挑 `policy profile`
2. node 不会再各自凭本机 env 选默认 mode 或 payload limit
3. 会话建成以后，`serialization_mode`、payload limits、protobuf bytes lane、HTTP bytes lane 都按冻结后的 `effective_policy` 走

`policy_id` 现在属于控制面/部署层输入，而不是普通客户端输入。
普通用户在主路径上只会看到最终冻结的 `effective_policy`，不会被引导去直接选择 profile。

`Service.connect(...)` 会通过 service route/status metadata 继承 deploy 时绑定的 profile，再按 connect 上下文冻结 `effective_policy`。
如果同一个 service 的 routes 暴露出不一致的 `policy_id`，connect 会直接失败，而不是私自猜默认值。

当前内置 profile：

1. `default_safe`
2. `trusted_internal`
3. `pickle_internal_heavy`

默认行为：

1. `gateway_public` 即使后端节点支持 pickle，只要 profile 不允许，也会统一拒绝
2. payload 准备链会优先遵守 session 的 effective payload limit，必要时转 `DataRef`
3. `Service.connect(transport="gateway")` 默认绑定 `default_safe`，默认 mode 是 `legacy_v1`
4. `Service.connect(transport="discovery")` / `Service.deploy(...)` 默认绑定 `trusted_internal`，默认 mode 是 `pickle_stable_v1`
5. `TaskPool.open(...)` 默认绑定 `trusted_internal`，默认 mode 是 `pickle_stable_v1`
6. 重数据 task 场景建议显式切到 `pickle_internal_heavy`
7. `JobQueue.connect()` 默认绑定 `jobqueue_controlplane_transport`，对应 profile=`default_safe`，默认 mode=`structured_v1`
8. 节点差异由 `tags`、`healthy_only` 和 runtime 过滤表达，不再参与 effective policy 协商

carrier 选择也已经改成同一个原则：

1. 主判断来自 `effective_policy.use_transport_payload_bytes`
2. HTTP 主判断来自 `effective_policy.use_http_bytes_transport`
3. 只有当前调用没有 effective policy 时，才 fallback 到 mode helper

所以现在不要再把 “`pickle_stable_v1` 一定走 bytes” 当成固定规则；
真正生效的是当前 session / route snapshot 解析出来的 effective policy。

对 `JobQueue` 要再补一句：

1. `JobQueue` 自己的 transport/session 默认绑定 `jobqueue_controlplane_transport -> default_safe`
2. 这个 binding 的默认 mode 仍然是 `structured_v1`
3. `job-orchestrator` 是系统内置 startup service，不作为用户 module deploy 入口暴露
4. 用户在 `JobQueue.submit(...)` 里传的 `task_serialization_mode`，解释为未来 `TaskPool` 的执行策略；`policy_id/taskpool_policy_id` 不再允许由 submit 传入
5. `job-orchestrator` 运行期维护单共享 `TaskPool`（串行 job）；同 artifact/codeversion 优先软切 mode 复用，软切失败再回退重建
6. 因此 job-orchestrator 的调用面和后续 task 执行面的策略边界是分开的
