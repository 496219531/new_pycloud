# PyCloud 流式分发架构方案（V1）

## 1. 目标与边界

1. 目标是内网流式分发，支持并发 200，避免中心堵点和内存积压。
2. 通信协议基线为 gRPC（HTTP/2 + Protobuf）。
3. 支持两种执行模式：
   - `persistent`：常驻执行模式
   - `ephemeral`：一次性执行模式（跑完回收）
4. 支持“服务会话模式”：
   - 客户端注册服务后由 NodeControl 维持进程组
   - 通过心跳续租实现常驻与断线自动回收
5. 框架不实现业务 `DBReader`、业务 `ResultWriter`，只提供 Hook。
6. 失败语义固定：
   - 有返回（成功或业务报错）不重试。
   - 无返回（断网/失联/超时）最多重试 3 次。

## 2. 架构拆分

1. `InfoCenter`（信息中心）
   - 仅负责节点注册、节点心跳、节点能力查询。
   - 不承载任务队列和结果大流量。
2. `NodeControl`（每台机器一个轻量控制器）
   - 接收代码包。
   - 管理本机任务队列。
   - 动态拉起/回收本机 worker 进程池（多进程，绕开 GIL）。
   - 接收 worker 执行结果并对客户端提供查询。
3. `WorkerProcess`（本机执行进程）
   - 主动从 `NodeControl` 取任务。
   - 执行后回传结果到 `NodeControl`。
4. `ClientScheduler`（客户端侧调度器）
   - 从 `InfoCenter` 拉可用节点。
   - 把代码发布到目标 `NodeControl`。
   - 流式向各节点发任务并拉结果。

## 3. 核心数据流

1. 节点发现流
   - Worker/NodeControl 向 `InfoCenter` 注册并持续心跳。
   - 客户端从 `InfoCenter` 获取可用节点地址与负载。
2. 代码发布流
   - 客户端先上传业务代码到每个目标 `NodeControl`。
   - `NodeControl` 校验 `sha256`，返回 `code_version`。
3. 任务流
   - 客户端提交任务到各节点 `NodeControl`。
   - 任务必须带 `task_id + code_version`。
   - `NodeControl` 入本机有界队列，并由本机进程池主动消费执行。
4. 结果流
   - 本机 worker 执行完成后回传到本机 `NodeControl`。
   - 客户端从各 `NodeControl` 拉取结果。
   - 默认结果 Hook 存在发任务客户端内存中，用户可重载落库逻辑。

## 4. 调度与背压

1. 初始预热：每个可用节点先发 10 个任务。
2. 后续分发：优先发给 `credit` 最高节点。  
   `credit = queue_capacity - (queued + inflight)`
3. 节点返回 `NO_CREDIT/429` 时，客户端立即改投其他节点。
4. 所有队列必须有界，禁止无限缓存。
5. 达到高水位暂停派发，低于低水位恢复派发。

## 5. 心跳与租约机制

1. `NodeControl -> InfoCenter` 每 30s 发送节点心跳。
2. `WorkerProcess -> NodeControl` 通过本机 IPC 上报运行状态。
3. `NodeControl` 统一对外发任务心跳，不依赖业务代码主动发心跳。
4. 判定规则：
   - 心跳间隔：30s
   - 失联阈值：90s
   - 扫描周期：10s
5. 长任务策略：
   - 10 分钟是建议目标，不是失败条件。
   - 30 分钟任务允许继续跑，不因时长报错。
   - 可上报 `progress_stale` 用于告警，不直接终止任务。

## 6. NodeControl 与子进程池通信

1. `TaskQueue`：`multiprocessing.Queue(maxsize=N)`，下发任务信封。
2. `ResultQueue`：`multiprocessing.Queue(maxsize=M)`，回传结果信封。
3. `HeartbeatPipe`：`multiprocessing.Pipe` 或共享状态，回传存活/进度。
4. 任务与结果尽量传引用或小对象，避免大对象占内存。

## 7. 错误与重试

1. `FAILED_USER`：业务代码报错且已返回，不重试。
2. `FAILED_INFRA`：无返回（失联/网络异常/超时），最多重试 3 次。
3. 重试建议退避：5s、20s、60s（可加抖动）。
4. 幂等要求：`task_id` 全局唯一，重复提交不得重复执行。

## 8. 代码版本与执行模式

1. 代码先发布后执行，任务只引用 `code_version`。
2. `persistent`：
   - 常驻进程池，适合高频任务，延迟低。
3. `ephemeral`：
   - 按任务创建临时运行目录和进程，结束立即清理。
   - 清理必须在 `finally`，并有 TTL 兜底回收。

## 9. 状态存储策略

1. V1：`InMemoryBackend`（轻量、单实例快速落地）。
2. 预留统一 `StateBackend` 接口，后续可切 `RedisBackend`。
3. 切 Redis 的触发条件：
   - 多实例高可用
   - 跨进程/重启后状态恢复
   - 更强一致性需求

## 10. 最小 API 契约（建议）

### 10.1 InfoCenter

1. `POST /nodes/register`
2. `POST /nodes/heartbeat`
3. `GET /nodes`

### 10.2 NodeControl

1. `POST /code/upload`
2. `POST /tasks/submit`
3. `GET /tasks/result`
4. `POST /tasks/cancel`
5. `GET /metrics`

## 11. 实施顺序

1. Phase 1：InfoCenter + NodeControl + 流式分发 + 背压 + 心跳 + 失败重试。
2. Phase 2：`ephemeral` 完整回收链路与 TTL 清理。
3. Phase 3：Redis 后端、多实例、监控告警与审计。

## 12. 服务会话模式（补充）

1. 服务注册由 owner 客户端发起：上传代码并声明 `worker_count`。
2. NodeControl 返回 `service_id` 与 HTTP 网关地址。
3. owner 定时心跳续租；超时则 NodeControl 自动回收服务进程与代码上下文。
4. owner 可主动发起 `EndService` 提前结束服务。
5. 其他客户端通过 HTTP 网关直接调用该服务。
6. 详细协议见 `SERVICE_SESSION_PROTOCOL_V1.md`。
