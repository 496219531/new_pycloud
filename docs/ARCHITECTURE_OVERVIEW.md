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

1. `Service.connect(..., transport="gateway")`
2. `Service.connect(..., transport="discovery")`
3. 如需更底层 transport client，再从 `pycloud_parallel.controlplane` 使用内部客户端

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
   - transport body 可显式识别 codec
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

## 9. Transport Codec Pipeline

当前 3 个 serialization mode 不再只作用于 `put_data()` 这一条对象上传路径，而是统一进入了主传输层：

1. payload encode
2. request body
3. response / result decode
4. task submit / task result
5. object upload / DataRef materialize

统一规则：

1. `legacy_v1`
   - 继续走老的 Arrow-compatible / Struct-safe 语义
   - 也是当前默认
2. `structured_v1`
   - transport body 使用显式 envelope 标明 codec
   - bytes 通过结构化 sentinel 传输
3. `pickle_stable_v1`
   - transport body 同样显式标明 codec
   - pandas / numpy 先做稳定 schema，再进入 pickle

当前 transport decode 规则：

1. 优先识别显式 transport envelope
2. 只有 `legacy_v1` 允许裸 payload fallback
3. `structured_v1` / `pickle_stable_v1` 不再依赖 decode 端猜测
4. 接收端会按上下文重新校验 declared codec，不再无条件信任 envelope

分层边界：

1. object codec 层
   - `legacy_v1`
   - `structured_v1`
   - `pickle_stable_v1`
2. transport 容器层
   - JSON / Struct
   - gRPC bytes payload
   - object upload blob

当前 gRPC/protobuf 主链已经是并行双通道：

1. 旧通道
   - `google.protobuf.Struct`
   - 继续兼容 `legacy_v1`
2. 新通道
   - `TransportPayload { codec, version, payload(bytes) }`
   - `pickle_stable_v1` 优先走这条 bytes 通道
   - 接收端优先读取这条通道

当前 HTTP 主链也已经是并行双通道：

1. 旧通道
   - `application/json`
   - 继续兼容 `legacy_v1`
   - `structured_v1` 也可继续走这条路径
2. 新通道
   - `application/x-pycloud-transport`
   - `X-Pycloud-Codec`
   - `X-Pycloud-Transport-Version`
   - `pickle_stable_v1` 优先走这条 bytes 通道

HTTP 接收端规则：

1. JSON body 继续走旧 JSON decode
2. bytes body 先按 header 读 declared codec/version
3. 再按上下文做权限校验
4. `gateway_public` 默认仍然硬性禁止 `pickle_stable_v1`

`pickle_stable_v1` 的对象层原则：

1. 只负责稳定 schema
2. ndarray schema 里的数据字段保留 raw bytes
3. 不为了 JSON/Struct 预处理成 base64

当前如果某条 transport 通道仍然是 JSON / Struct-only：

1. 由 transport 层显式做 container adaptation
2. 或显式拒绝该 codec 走该通道
3. 不再把这个责任下推到 `pickle_stable_v1` codec 本身

mode 选择权也已经固定在少数边界：

1. 全局默认：system mode / env
2. 会话级：`Service` / `TaskPool` / `JobQueue`
3. 单次调用级：低层显式 call/submit API
4. 对象上传级：`put_data(...)` 及其变体

内部模块例如 transport client、payload helper、node execution helper 现在只接收/传递 mode，不再私自切换默认 mode。

`pickle_stable_v1` 的边界：

1. 适合 trusted session / object upload / owner-side service / taskpool
2. `gateway_public` 和 `untrusted_transport` 默认硬性禁止
3. 在受限上下文里会明确拒绝，而不是静默降级
