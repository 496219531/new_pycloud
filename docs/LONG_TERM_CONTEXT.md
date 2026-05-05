# 长期上下文（Long-Term Context）

最后更新：2026-04-29（Asia/Shanghai）
适用范围：`new_pycloud` 主仓库（V1 公开面与控制面实现）

## 1. 这份文件的目的

这份文件是项目的长期记忆基线，给后续维护者回答 4 个问题：

1. 这个项目当前“长期稳定”的边界是什么
2. 哪些决策已经定稿，不能被局部实现悄悄改掉
3. 发生行为偏移时，先查哪里、测哪里
4. 新需求进来时，怎么改才不会破坏既有共识

## 2. 当前架构基线（对外心智）

V1 公开概念固定为：

1. `Service`
2. `TaskPool`
3. `JobQueue`
4. `DataRef`
5. `export`

三层执行模型：

1. `Service Mode`：常驻函数服务会话
2. `JobQueue Mode`：大任务排队与单活编排
3. `TaskPool / Task Mode`：子任务执行层

## 3. 序列化与策略基线

### 3.1 mode 与 policy 的职责

1. `serialization_mode` 负责“对象如何编解码”（codec 语义）
2. `policy profile` 负责“是否允许该 mode + payload/carrier 限制”
3. `effective_policy` 是会话冻结后的最终执行策略

### 3.2 当前默认绑定（重要）

1. `jobqueue_controlplane_transport` -> `default_safe`，默认 mode=`structured_v1`
2. `service_internal` -> `trusted_internal`，默认 mode=`pickle_stable_v1`
3. `taskpool_default` -> `trusted_internal`，默认 mode=`pickle_stable_v1`
4. `gateway_public` -> `default_safe`，默认 mode=`legacy_v1`，走对外保守边界（默认不允许 pickle）

### 3.3 变更约束

1. 客户端可以表达 `serialization_mode` 偏好，但不能在普通调用面随意改 `policy_id`
2. 不再把 node capability 交集引入 effective policy 协商
3. 节点差异通过 `tags` / `healthy_only` / runtime 过滤处理

## 4. JobQueue / job-orch 长期约束（关键）

### 4.1 调用面约束

1. `JobQueue` 自身 controlplane session 固定：`structured_v1 + default_safe`
2. `JobQueue.submit(...)` 允许传 `task_serialization_mode`，只影响后续 TaskPool 执行面
3. `JobQueue.submit(...)` 不再接受 `policy_id/taskpool_policy_id`（应直接报错）

### 4.2 orch 侧约束

1. `job-orch` 是系统启动时挂载的 startup service，自身 submit 入口固定 `structured_v1`
2. `job-orch` 的 `taskpool_policy_id` 在启动时确定
3. 运行期不支持 submit 覆盖该 policy
4. 管理员如需改 policy，应通过部署/启动参数变更后重启生效

### 4.3 共享池约束

1. `job-orch` 运行期维护单个共享 `TaskPool`（串行 job）
2. 新 job 与当前池同 artifact/codeversion 时，优先在 job 边界软切 mode 并复用池
3. 软切失败时，回退为“关闭旧池 + 重建新池”
4. 共享池空闲超过 `idle_ttl` 后才主动回收，不是每个 job 结束都关池

## 5. 安全与会话边界基线

1. TaskPool 相关操作需要 `owner_client_id + pool_token` 双重校验
2. keepalive 不是纯本地状态刷新，会触发远端心跳与租约续期
3. 任何绕过接收端上下文校验的“声明式 mode 信任”都应视为高风险设计

### 5.1 DataRef 内部可信链路

1. 内部可信链路默认走少拷贝主路径：`upload_once -> forward DataRef -> final worker/client remote fetch`
2. `PYCLOUD_DATAREF_UPLOAD_STRATEGY` 默认 `upload_once`，旧 `fanout` 只作为显式回滚模式保留
3. `PYCLOUD_JOBQUEUE_RESOLVE_REFS` 默认 `defer_to_worker`，job-orch 不提前 materialize 业务 `DataRef`
4. `PYCLOUD_DATAREF_RESOLUTION` 默认 `remote_fetch`，worker 本地 miss 后按 locator/registry 拉取、校验并缓存
5. `PYCLOUD_GATEWAY_DATAREF_RELAY` 仍默认 `eager`；gateway_public 的外部 DataRef locator 信任边界后续单独收口

## 6. 动态补偿与失败可观测性基线

1. Service / TaskPool 动态补偿由 owner client 侧驱动，不增加 InfoCenter 的调度职责
2. 补偿按活跃副本数判断是否低于目标副本数，失败副本不占用目标数量
3. 失败副本以 `node_instance_id` 为准记录和跳过，不按可重复的 `node_id` 永久排除
4. 同一 `node_id` 重启后如果获得新的 `node_instance_id`，应重新进入补偿候选
5. service/taskpool 创建失败或 executor host 失败时，node 侧应保留 STOPPED 诊断记录
6. InfoCenter `/ops` 必须显示每条失败 service/taskpool 的 `failure_reason`
7. 健康服务路由查询仍只返回健康节点上的 `RUNNING` 服务，诊断记录不能污染调用路由

### 6.1 动态部署 code version 一致性

1. 动态部署服务由唯一发布者（owner deploy session）统一发布和管理
2. 同名服务副本必须在同一 owner 控制域内保持同一个 `code_version`
3. 同名但代码变化时，必须先结束旧服务，再由 owner 重新部署
4. node 断开、被 fenced、或用新的 `node_instance_id` 重连后，旧执行状态不再可信；owner 必须重新部署/补齐
5. startup service 只由启动它的进程自行管理；即使 `service_name/code_version` 与动态部署一致，也不能被动态 owner 复用、接管或作为扩容副本加入，因为它自治运行，不在该 owner 的版本管控、回滚、keepalive 与 close 闭环内
6. startup service 不能动态加入任何现有服务组；同一个 `service_name` 上动态部署与 startup service 双向互斥
7. 任一方已存在时，另一方不能因为 code version 一致而启动/部署
8. 动态扩容由同一个动态 owner 调整目标副本数并重启/恢复 deploy session 完成；快速重启可接回该 owner 已部署的同 code version 服务，再由 keepalive 补齐新增节点

### 6.2 node 实例身份与 fencing

1. `node_id` 是逻辑名，可以持久化和复用
2. `node_instance_id` 是执行实例身份，也是 service/taskpool token、DataRef 路由、动态补偿失败记录的 fencing 单位
3. 不再引入额外 `epoch`；失效实例必须换新的 `node_instance_id`
4. InfoCenter heartbeat timeout、`mark-lost`、node 自身发现 lease 过期、executor 状态不可恢复，都应使旧 `node_instance_id` 进入 fenced 状态
5. fenced 实例不能带着旧 service/taskpool 状态恢复；node 侧必须清理执行状态并重新注册新实例
6. code/object cache 可保留，但 runtime/service/taskpool/executor/worker/token 状态必须清空

### 6.3 unhealthy / drain / cordon

1. `unhealthy` 表示执行状态不可信，应触发 fencing/reset 语义
2. `drain` 表示不接新业务流量和新 task，但仍接 owner 控制命令
3. `cordon` 表示不接新部署；已有 RUNNING 服务是否继续路由由 `drain` 决定
4. 排他性部署和版本冲突检查不能因为 drain/cordon 就隐藏已有服务；只有 fenced unhealthy 实例可以从冲突检查中移除
5. owner 命令路径不要过滤 drain/cordon，否则旧版本服务可能无法被 update/close

### 6.4 TaskPool inflight retry

1. 已被 node 接收的 task 若要 infra retry，client/session 侧必须持有 replay record；不能依赖失联 node 上的 `TaskState.payload`
2. replay record 应保存逻辑 index/key、原始 payload、当前 task_id、当前 node_instance_id、attempt、最近错误
3. retry 到新 node 时重新执行 payload prepare，不复用旧 node 上的本地化 prepared payload
4. `imap_unordered` 公共链路支持 `max_infra_retries`，默认 1 次；耗尽后返回 `FAILED_INFRA / NodeInstanceLost`
5. 低层 submit/wait API 不应被假设有自动 replay；如果需要同等语义，应显式接入 replay ledger
6. 必须保留 retry 可观测字段：重试次数、成功次数、耗尽次数、lost-node replay/fail 数、retry prepare/submit 耗时

## 7. 回归测试最小集合（改动前后必看）

建议至少覆盖：

1. `tests/v1/test_jobqueue_shared_pool_mode_switch.py`
   - 软切复用
   - 软切失败回退重建
   - idle 过期回收
2. `tests/test_job_queue.py`
   - submit 参数解释
   - `policy_id` 禁止路径
3. `tests/test_effective_policy.py` / `tests/test_policy_profile.py`
   - 绑定与有效策略解析
4. `tests/test_service_api.py` / `tests/test_taskpool_api.py`
   - 动态补偿
   - 失败旧实例跳过
   - 同 `node_id` 新 `node_instance_id` 可重新加入
5. `tests/test_infocenter_registrar.py`
   - `/ops` 展示 service/taskpool 失败原因
   - fenced `node_instance_id` 要求新实例注册
6. `tests/test_taskpool_execution.py`
   - accepted task 在 node lost / `FAILED_INFRA` 后 replay 到健康 node
   - `max_infra_retries=0` 时直接返回 infra failure
   - retry exhausted 时返回 `NodeInstanceLost`
   - 旧 task_id 的迟到结果不污染新 task

## 8. 新需求进入时的决策流程

1. 先判断是“codec 问题”还是“policy 问题”
2. 若涉及默认行为，优先改 binding/profile，不在业务调用链硬编码分支
3. 若涉及 JobQueue 与 TaskPool 边界，优先保持“queue controlplane policy”与“task execution policy”分离
4. 所有默认值变更，必须同步：
   - `src/pycloud_parallel/controlplane/policy_profile.py`
   - `src/pycloud_parallel/controlplane/config.py`（涉及运行时 env 默认值时）
   - 对应文档（本文件 + `ARCHITECTURE_OVERVIEW.md` + `CLIENT_SURFACE_OVERVIEW.md`）
   - `docs/RUNTIME_LIMITS.md`（涉及 payload / DataRef / control message limit 时）
   - 对应测试

## 9. 近期精简路线

这部分不是行为边界，而是给 coder 的低风险重构顺序。目标是减少重复实现，降低后续继续加 timing / policy / carrier 字段时的同步成本。

### 9.1 优先级顺序

1. `nodecontrol_state.py`：收口 service/task-pool timing recorder
2. `node_control_client.py`：收口 object upload 的 file/bytes + precheck/single-pass 分支
3. `job_queue.py`：收口 plain job / hooks job 的共享 `TaskPool` 准备流程
4. `job_queue.py`：收口 job staged refs 的 touch/release helpers
5. `support.py` / `artifact.py`：集中 artifact packaging 默认值

### 9.2 重构约束

1. 第一轮只做结构精简，不改变对外 API 和默认行为
2. `timing_metrics` 字段名保持兼容，`/ops` 页面展示不回归
3. object upload 的四条路径行为保持一致：file/bytes 与 precheck/single-pass 都必须继续覆盖
4. `JobQueue` 的 `pool_action`、`pool_prepare_ms`、`warmup_ms`、`running_tasks_ms`、`first_result_wait_ms` 等 timing 字段不能丢
5. artifact packaging 的 `include_tests` 默认值先集中管理，是否切到 `False` 作为单独性能优化决策处理

### 9.3 验收建议

1. 跑现有 `JobQueue` / shared pool / mode switch 回归测试
2. 跑 object upload 相关测试，覆盖 bytes 与 file 两类输入
3. 手动或测试确认 `/ops` 仍能看到 service 与 task-pool timing 聚合，以及失败原因
4. 对 Windows 性能优化另开小步变更，不混入本轮精简

## 10. 文档维护规则

1. 只有“会影响默认行为/边界”的决策才写入本文件
2. 每次更新请改“最后更新”日期，并附一句变化摘要
3. 如果与其他文档冲突，以本文件为优先修正源，再回补其他文档

## 11. 更新准入规则（多人/多线程协作）

### 11.1 谁可以改

1. 任何有仓库写权限的维护者都可以修改本文件
2. 但修改应遵循“明确指令 + 可验证依据 + 同步测试/文档”的最小流程

### 11.2 何时允许改

满足任一条件可更新：

1. 默认行为发生变化（默认 mode、默认 policy、默认 limits、默认路由策略）
2. 边界约束发生变化（例如 submit 参数权限、共享池生命周期、安全校验）
3. 已有条目与代码事实不一致，需要纠偏

### 11.3 何时不该改

1. 仅是临时排障结论、尚未定稿的讨论
2. 仅当前线程上下文、不会影响全局行为的局部细节
3. 尚无代码/测试佐证的猜测性结论

### 11.4 自动化与触发方式

1. 本文件不会被系统自动追加或自动改写
2. 必须由维护者在对应线程中明确执行编辑动作（人工或 coder）
3. 未经明确编辑动作，不应假设“对话内容已经自动沉淀到本文件”

### 11.5 提交前检查清单（建议）

1. 是否更新“最后更新”日期与“本次更新摘要”
2. 是否给出可核对的代码/测试依据
3. 是否同步了受影响文档：
   - `docs/ARCHITECTURE_OVERVIEW.md`
   - `docs/CLIENT_SURFACE_OVERVIEW.md`
   - `docs/TASK_MODE.md` / `docs/TASK_CLIENT_GUIDE.md`（按需）
4. 是否补充或更新了对应回归测试（至少最小集合）

### 11.6 并发修改冲突处理

1. 以“更晚且有代码依据”的版本为主
2. 冲突合并时优先保留约束性条款，删去重复叙述
3. 若两条规则冲突，先在 PR/评审中做显式裁决，再落文档，不做隐式覆盖

### 11.7 自动守卫（CI）

仓库已配置：

1. `.github/PULL_REQUEST_TEMPLATE.md`
   - 强制 PR 作者显式勾选是否涉及长期边界变更
2. `.github/workflows/long-term-context-guard.yml`
   - 当关键边界文件发生改动时，若未同步修改 `docs/LONG_TERM_CONTEXT.md`，CI 直接失败

若后续新增关键边界文件，请同步维护 workflow 里的关键文件列表。

---

本次更新摘要（2026-04-29）：

1. 对齐 V1 默认绑定：JobQueue 控制面固定 `default_safe + structured_v1`，Service/TaskPool 内部可信默认 `trusted_internal + pickle_stable_v1`，gateway/public 保守禁止 pickle
2. 保留 JobQueue/job-orch 的长期边界：job-orch 作为系统内置 startup service module，服务端复用 startup service runtime，JobQueue client 复用 `Service.connect` 底层 route/protocol，policy 启动时固定，submit 仅允许 `task_serialization_mode`，共享池串行复用，软切失败回退重建
3. 确认内部可信 DataRef 主路径：`upload_once -> forward DataRef -> final worker/client remote_fetch`，gateway/public DataRef 边界后续单独收口
4. 明确默认值变更同步范围：policy profile、runtime config、相关文档与测试必须一起更新
