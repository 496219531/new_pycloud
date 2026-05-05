# 架构总览

## 1. 当前边界

当前实现已经收敛为四层：

1. `External Web Layer`
   - 真正对外的轻网络入口层
   - 推荐独立使用 `FastAPI/Flask + uvicorn/gunicorn`
2. `Service Mode`
   - 常驻服务会话层
3. `JobQueue Mode`
   - 大任务排队与单活调度层
   - `JobQueue` 默认先查 `InfoCenter` 找到唯一 `job-orchestrator` route，再直连它的 HTTP 数据面
4. `TaskPool Mode`
   - 子任务执行层
   - 面向批量任务执行的核心产品对象

一句话概括：

1. `Service Mode = 常驻服务会话层`
2. `JobQueue Mode = 大任务排队与单活调度层`
3. `TaskPool Mode = 批量任务执行会话层`

## 2. 角色

### 2.1 owner client

负责：

1. 部署并持有内部函数服务
2. 持有 `service_token`
3. 维持服务 keepalive

推荐入口：

1. `Service.deploy(...)`

### 2.2 caller client

负责：

1. 按 `service_name` 调已有服务
2. 不管理服务生命周期

推荐入口：

1. `Service.connect(..., route="gateway")`
2. `Service.connect(..., route="discovery")`
3. 如需更底层 HTTP client，再从 `pycloud_parallel.controlplane` 使用内部客户端

### 2.3 job client

负责：

1. 提交大任务
2. 进入队列等待调度
3. 查询 job 状态与结果

推荐入口：

1. `JobQueue`
2. 默认代码输入走 `submit(source=module)`

### 2.4 task pool client

负责：

1. 创建原生专属 pool
2. 往 pool 提交 subtasks
3. 拉结果、取消 job、关闭 pool

推荐入口：

1. `TaskPool`

## 3. Service Mode

服务模式当前仍然是“模块 + 多函数导出”模型：

1. 上传支持 `py / tar.gz / zip / whl`
2. 注册时指定 `entry_module + export_spec`
3. 导出模式支持 `decorator / explicit / all / single`
4. 当前更适合作为内部函数服务层，而不是对外 Web 应用层
5. 普通用户默认走 `Service.deploy(source=module)`；`Artifact(...)` 只保留给高级打包控制
6. `Service.connect(...)` 是当前 caller 侧主入口；gateway/discovery 是连接策略，不再是顶层产品类名

对外推荐入口：

1. `POST /svc/{service_name}/call/{method}`
2. `GET /svc/{service_name}/methods`
3. `GET /svc/{service_name}/status`

## 4. JobQueue Mode

`JobQueue Mode` 负责：

1. 大任务先入队
2. 同一时刻只放行一个大任务进入 `RUNNING`
3. 放行后再创建 `TaskPool`
4. 由 job module 的 `task_generator` 生成 payloads，交给 pool 执行
5. 可选通过 `update_globals` 先向 worker 广播共享全局数据

当前推荐入口：

1. `JobQueue`
2. 默认代码输入走 `submit(source=module)`

## 5. TaskPool Mode

`TaskPool Mode` 当前是批量任务执行会话：

1. `CreateTaskPool`
2. `HeartbeatTaskPool`
3. `SubmitPoolTasks`
4. `PullPoolResults`
5. `CancelPoolJob`
6. `GetTaskPoolStatus`
7. `CloseTaskPool`

特点：

1. 每个 pool 是独立资源会话
2. pool 自己 heartbeat 保活
3. subtasks 不走旧共享任务池
4. `Service` 与 `TaskPool` 共享 `ExecutorHost + ExecutionSession` 底座，但保留两类兄弟会话类型
5. 每个 pool 当前只暴露一个任务入口，也就是创建时的 `entry_callable`
6. `task_method` 是高层单入口校验参数，不是多方法路由协议
7. `runtime_key` 仍然保留，但它代表 runtime 逻辑隔离键，不再对应独立的 runtime-slot 资源
8. 普通用户默认走 `TaskPool.open(source=module)`；`Artifact(...)` 是高级能力

## 6. 统一调度核心

当前 `Service / TaskPool / JobQueue` 已经共享一套统一的“选谁”框架：

1. 统一候选对象
2. 统一硬过滤
3. 统一特征集合
4. 统一复合评分
5. 同分候选再做 round-robin 打散

默认 profile：

1. `SERVICE_DEFAULT`
   - `Service` owner / discovery / gateway 都默认走这套口径
   - 核心特征是 `predicted_busy + node_inflight + alive_workers`
2. `TASKPOOL_DEFAULT`
   - `TaskPool` 单次提交和 refill 补位都走这套口径
   - 额外考虑 `local_inflight`
3. `JOBQUEUE_DEFAULT`
   - `JobQueue` 建池选点用这套口径

要注意分层：

1. scheduler 只负责“选谁”
2. `TaskPool` 自己仍负责：
   - `max_in_flight`
   - refill
   - pull results
3. `Service` 自己仍负责 RPC 调用与失败切换

也就是：

1. **统一选点**
2. **不强行统一流控循环**

## 7. 已移除

以下旧共享任务池能力已经移除：

1. 旧共享任务池客户端
2. 旧共享任务池流式入口
3. 旧共享任务结果拉取与取消链路

## 8. Serialization Modes

当前数据传输层定义了 3 个 serialization mode：

1. `legacy_v1`
   - 当前老兼容模式
   - 默认仍是它
2. `structured_v1`
   - 结构化显式 codec
   - HTTP raw-bytes body 可显式识别 codec
3. `pickle_stable_v1`
   - 受信环境高保真 Python codec
   - 对 pandas/numpy 先做稳定 schema 规范化，再进入 pickle

边界说明：

1. `legacy_v1`
   - 适合最小风险兼容
2. `structured_v1`
   - 适合长期安全结构化模式
3. `pickle_stable_v1`
   - 适合内网受信环境下的高保真传输
   - 但不等于任意 Python 自定义对象都承诺稳定支持

当前明确支持矩阵：

1. `legacy_v1`
   - JSON scalar
   - dict/list/tuple
   - datetime/date/time/timedelta
   - DataFrame
   - Series
   - ndarray
   - DataRef
2. `structured_v1`
   - `legacy_v1` 的结构化对象
   - bytes/bytearray/memoryview
   - tuple 在结构化层按 list 语义传输
   - dict key 会收成 string
3. `pickle_stable_v1`
   - `structured_v1` 支持的对象
   - pandas `DataFrame / Series / Index`
   - numpy `ndarray`（非 `dtype=object`）
   - `dtype=object` 明确报错

## 9. Policy / Tags / Effective Policy

当前多节点执行面统一按三层模型收口：

1. `Policy Profile`
   - 由 controlplane/profile 中心统一定义
   - 决定逻辑策略，而不是某台 node 的本地偏好
2. `Node Tags / Runtime Filtering`
   - 由 tags、healthy 状态、runtime 兼容决定哪些 node 参与 session
   - 负责“选哪些节点”，不负责 policy 协商
3. `Effective Policy`
   - 在 `Service / TaskPool / JobQueue` 会话创建时，由 controlplane 根据
     `Policy Profile + requested_mode + context`
     计算并冻结
   - 同一 session 后续所有 node 都按这个冻结结果执行

核心原则：

1. node 不拥有 policy
2. 节点差异主要通过 tags / healthy / runtime 过滤表达
3. policy 由中心统一管理
4. effective policy 在会话创建时冻结
5. 执行期不允许同一 session 内 policy 漂移
6. node 本地 env 不参与 session policy / limit 决策，只能作为启动默认、物理执行边界或兼容入口

### 9.1 Policy Profile

`Policy Profile` 当前主要定义：

1. allowed modes
2. default mode
3. inline payload/result limits
4. 是否启用旧内部 `TransportPayload` adapter
5. 是否启用 HTTP raw-bytes body
5. 是否允许 `pickle_stable_v1`
6. soft limit 以上是否强制转 `DataRef`
7. public gateway 是否允许 pickle

当前内置 profile：

1. `default_safe`
2. `trusted_internal`
3. `pickle_internal_heavy`

当前内置默认绑定：

1. `gateway_public`
   - profile=`default_safe`
   - default mode=`legacy_v1`
2. `service_internal`
   - profile=`trusted_internal`
   - default mode=`pickle_stable_v1`
3. `taskpool_default`
   - profile=`trusted_internal`
   - default mode=`pickle_stable_v1`
4. `taskpool_heavy_dataframe_numpy`
   - profile=`pickle_internal_heavy`
   - default mode=`pickle_stable_v1`
5. `jobqueue_controlplane_transport`
   - profile=`default_safe`
   - default mode=`structured_v1`

### 9.2 Tags / Node Metadata

当前框架把节点差异主要收敛到：

1. tags
2. healthy_only
3. runtime 兼容

InfoCenter 仍然会保存 node capability 这类元数据，供观测和诊断使用；但运行时 `effective_policy` 不再与 candidate capability 做交集协商。

新的长期口径：

1. node 不作为 capability/tag/limit/policy 的 authority
2. `NodeCapability` 是兼容/观测字段，不是未来节点筛选的主 authority
3. 后续节点选择由 controlplane 节点管理服务统筹
4. 节点管理服务基于中心配置、标签、运维信息、健康状态和 runtime 兼容筛选 node
5. 同一物理机器可能有多个 node，不能把单个 node 的本地 capability 等同于 machine capability

### 9.3 Effective Policy

`Effective Policy` 当前至少冻结：

1. resolved mode
2. allowed modes
3. inline payload soft/hard limit
4. inline result hard limit
5. 是否启用旧内部 `TransportPayload` adapter
6. 是否启用 HTTP raw-bytes body
7. 是否允许 pickle

这意味着：

1. `Service.connect()` 建立后，不会因为某台 node 本地 env 改了就漂移 mode
2. `TaskPool.open()` 建立后，同一 pool 内 task submit / managed globals / result decode 共用同一执行策略
3. `JobQueue.connect()` 建立后，job submit 到 route call 也按同一冻结策略走

其中 `Service.connect()` 有一个额外约束：

1. caller 不再显式传 `policy_id`
2. connect 会从 deploy 后的 service route/status metadata 继承绑定的 profile
3. 然后按 route 绑定的 profile 和 connect 上下文冻结出自己的 `effective_policy`
4. 如果同名 service routes 的 `policy_id` 不一致，connect 会直接失败，避免普通调用面继续“选 profile”

动态部署还有一个 code version / owner 控制域约束：

1. 同名动态服务副本必须由唯一发布者统一发布管理
2. 运行中的同名副本必须保持同一 `code_version`，代码变化必须先结束旧服务再重新部署
3. node 断开后以新 `node_instance_id` 重连时，旧执行状态不再可信，必须由 owner 重新部署补齐
4. startup service 自己管理自己的生命周期；即使和动态服务的 `service_name/code_version` 一致，也不能被动态 owner 复用、接管或作为扩容副本加入，因为它自治运行，不在该 owner 的版本管控、回滚、keepalive 与 close 闭环内
5. startup service 不能动态加入任何现有服务组；同一个 `service_name` 上动态部署与 startup service 双向互斥
6. 任一方已存在时，另一方不能因为 code version 一致而启动/部署
7. 动态扩容由同一个动态 owner 调整目标副本数并重启/恢复 deploy session 完成；快速重启可接回该 owner 已部署的同 code version 服务，再补齐新增节点

`JobQueue.connect()` 也遵循同样的边界：

1. queue 自己的 controlplane session 固定绑定 `jobqueue_controlplane_transport`
2. 这个 binding 当前固定落到 `default_safe + structured_v1`
3. `job-orch` 作为系统内置 startup service 挂载，不作为用户 module deploy 入口暴露
4. `job-orch` 在启动时通过 startup managed globals 冻结自己的 `taskpool_policy_id`
5. 用户在 `submit(...)` 里只能改 `task_serialization_mode`；后续 `TaskPool` 会在 job 边界按这个 mode 软切
6. session 对外可见的是 queue 自己冻结后的 `effective_policy`
7. `job-orch` 运行期维护单共享 `TaskPool`（串行 job）；同 artifact/codeversion 优先软切复用，软切失败再回退重建

这里要特别注意：

1. codec 和 carrier 现在是两个层次
2. serialization mode 决定“怎么编解码”
3. carrier（JSON / HTTP raw-bytes body / 旧内部 adapter）优先由 `EffectivePolicy` 决定
4. `prefers_raw_bytes_payload()` 这类 mode helper 只在“当前没有 effective policy”时才作为 fallback

也就是说，`pickle_stable_v1` 不再天然等于“一定走 HTTP raw-bytes body”；
如果当前 effective policy 关闭了 HTTP raw-bytes body / adapter，运行时会继续用该 codec，但改走 JSON/Struct 这类兼容 carrier。

`JobQueue` 还有一个额外约束：

1. 初始化后 queue 自己就固定到 `structured_v1 + default_safe`
2. orchestrator route 只负责发现和路由，不再决定 queue 自己的 effective policy
3. `policy_id/taskpool_policy_id` 固定于 orch 启动时，不接受 submit 运行期覆盖
4. 真正给 task 执行面用的只有 `submit(...)` 上传的 `task_serialization_mode`，并在共享 `TaskPool` 的 job 边界软切
5. 共享池在空闲超过 idle TTL 时才会被回收；不是每个 job 结束就关池

### 9.4 Payload Policy 的来源

payload 限制现在不再只是“本机 `get_payload_policy()` 读 env”：

1. `Policy Profile` 给出逻辑目标值
2. `Effective Policy` 给出会话实际采用值
3. 具体 `PayloadPolicy` 再从 `Effective Policy` 派生

当前已经接到的主链：

1. `TaskPool` task submit
2. `Service` service call
3. `Gateway / Discovery` service call
4. `JobQueue` submit call
5. managed globals 上传准备

也就是说，同一 session 下 JSON inline、HTTP raw-bytes body 和旧内部 adapter 都会受同一个 effective payload limit 约束；节点差异主要通过 tags / healthy / runtime 过滤来表达，而不是再参与 policy 协商。

另外，carrier 的运行时选择也已经收口到 effective policy：

1. NodeControl 旧内部路径是否走 `TransportPayload` adapter
2. HTTP/service call 是否走 HTTP raw-bytes body
3. `CallService` 响应是否回 `transport_data`
4. `TaskResult` 是否回 `transport_result`

这些都不再由 codec helper 单独拍板；有 effective policy 或明确请求 carrier 时，运行时优先遵守它。

## 10. Transport Codec Pipeline

当前 3 个 serialization mode 不再只作用于 `put_data()` 这一条对象上传路径，而是统一进入了主传输层：

1. payload encode
2. request body / carrier
3. response / result decode
4. task submit / task result
5. object upload / DataRef materialize

统一规则：

1. `legacy_v1`
   - 继续走老的 Arrow-compatible / Struct-safe 语义
   - 也是当前默认
2. `structured_v1`
   - carrier 使用显式 envelope 标明 codec
   - bytes 通过结构化 sentinel 传输
3. `pickle_stable_v1`
   - carrier 同样显式标明 codec
   - pandas / numpy 先做稳定 schema，再进入 pickle

当前 carrier decode 规则：

1. 优先识别显式 carrier envelope
2. 只有 `legacy_v1` 允许裸 payload fallback
3. `structured_v1` / `pickle_stable_v1` 不再依赖 decode 端猜测
4. 接收端会按上下文重新校验 declared codec，不再无条件信任 envelope

分层边界：

1. object codec 层
   - `legacy_v1`
   - `structured_v1`
   - `pickle_stable_v1`
2. carrier 容器层
   - JSON / Struct
   - `TransportPayload` adapter
   - HTTP raw-bytes body
   - object upload blob

当前 NodeControl 消息主链仍保留双兼容路径：

1. 旧 carrier
   - `google.protobuf.Struct`
   - 继续兼容 `legacy_v1`
2. `TransportPayload` adapter
   - `TransportPayload { codec, version, payload(bytes) }`
   - 旧内部 state/proto 路径仍会用
   - 新 HTTP wire 设计不继续扩散这个概念

当前 HTTP 主线支持两种 body：

1. JSON carrier
   - `application/json`
   - 继续兼容 `legacy_v1`
   - `structured_v1` 也可继续走这条路径
2. HTTP raw-bytes body
   - `application/x-pycloud-transport`
   - `X-Pycloud-Codec`
   - `X-Pycloud-Transport-Version`
   - `pickle_stable_v1` 在 policy 允许时优先走这条 raw body

HTTP 接收端规则：

1. JSON body 继续走旧 JSON decode
2. HTTP raw-bytes body 先按 header 读 declared codec/version
3. 再按上下文做权限校验
4. `gateway_public` 默认仍然硬性禁止 `pickle_stable_v1`

`pickle_stable_v1` 的对象层原则：

1. 只负责稳定 schema
2. ndarray schema 里的数据字段保留 raw bytes
3. 不为了 JSON/Struct 预处理成 base64

当前如果某条 carrier 通道仍然是 JSON / Struct-only：

1. 由 carrier 层显式做 container adaptation
2. 或显式拒绝该 codec 走该通道
3. 不再把这个责任下推到 `pickle_stable_v1` codec 本身

mode 选择权也已经固定在少数边界：

1. 全局默认：system mode / env
2. 会话级：`Service` / `TaskPool` / `JobQueue`
3. 单次调用级：低层显式 call/submit API
4. 对象上传级：`put_data(...)` 及其变体

内部模块例如 HTTP client、payload helper、node execution helper 现在只接收/传递 mode，不再私自切换默认 mode。

`pickle_stable_v1` 的边界：

1. 适合 trusted session / object upload / owner-side service / taskpool
2. `gateway_public` 和 `untrusted_transport` 默认硬性禁止
3. 在受限上下文里会明确拒绝，而不是静默降级
