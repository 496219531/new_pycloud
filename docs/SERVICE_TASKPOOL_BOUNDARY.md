# Service / TaskPool Boundary

`Service` 和 `TaskPool` 共享一部分底座，但不是同一个运行时协议。

## 长期定位

1. `Service`
   - public
   - discovery-aware
   - remote service session
   - 面向 `call / stream`
2. `TaskPool`
   - private
   - owner-only
   - remote worker pool
   - 面向 `submit / map / results`

## 共享层

允许共享的层只有这些：

1. session/status view model
2. runtime transport helper
3. client/controlplane create pipeline
4. node-side low-frequency create skeleton

这些共享层的目标是减少重复代码，不是统一业务语义。

## 分离层

以下内容必须继续分离：

1. runtime protocol
2. service discovery
3. `service_name` namespace
4. taskpool `submit/results` model
5. service `call/stream` model
6. resource account

## 明确边界

1. taskpool 不会注册成 service route
2. taskpool 不进入 `service_name` 发现空间
3. service 不会改成 task submit/results 模型
4. taskpool 不会改成 service call/discovery 模型
5. taskpool 保留 bytes batch submit/results 协议，因为它服务于批量异步和性能
6. node 端 submit/call/result 热路径是性能敏感区，后续共享只允许发生在低频 create/status/control path
7. service stream 只承载小型 inline item；单个 item 超过 inline result hard limit 时直接失败，不自动转 `DataRef`

## 生命周期与诊断边界

1. service/taskpool 都由 owner heartbeat lease 驱动生命周期清理
2. 创建阶段（`accepted` / `artifact_prepare` / `executor_create` / `globals_warmup` / `readiness=initializing`）暂时过期时，node 侧刷新资源租约，不立即停止资源
3. 创建完成后仍按正常 owner heartbeat timeout 清理
4. executor host missing/died 会归类为对应 service/taskpool 的终态类 infra 错误
5. InfoCenter `/ops` 可以同时保留当前运行实例和历史诊断实例；历史 `stop_reason` 进入 `failure_reason`，不改变当前运行实例状态

## Startup Service

`Service.startup(...)` 可以复用 service route/call/status 模型，但它不等于中心统一 deploy 的多副本 service。

1. startup service 是本地启动时挂载的自治服务
2. 它不等于通过 deploy 建立的多节点统一服务组
3. 多节点同名统一服务必须走 deploy

## Node 管理边界

复杂 node 生命周期管理、资源打分、机器分组、拓扑、配额、权限和自动迁移不在本项目内继续扩展。

如果需要复杂 node 管理，应由外部成熟工具完成，再通过简单 tags 或控制面接口与 PyCloud 对接。
