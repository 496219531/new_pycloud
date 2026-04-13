# 架构总览

## 1. 当前边界

当前实现已经收敛为四层：

1. `External Web Layer`
   - 真正对外的轻网络入口层
   - 推荐独立使用 `FastAPI/Flask + uvicorn/gunicorn`
2. `Service Mode`
   - 内部常驻函数服务层
3. `JobQueue Mode`
   - 大任务排队与单活调度层
   - 默认经 `gateway -> 唯一 job-orchestrator -> TaskPool`
4. `TaskPool Mode`
   - 子任务执行层

一句话概括：

1. `Service Mode = 常驻函数服务层`
2. `JobQueue Mode = 大任务排队与单活调度层`
3. `TaskPool Mode = 专属子任务执行层`

## 2. 角色

### 2.1 owner client

负责：

1. 部署并持有内部函数服务
2. 持有 `service_token`
3. 维持服务 keepalive

推荐入口：

1. `DeployedService.deploy_from_infocenter(...)`

### 2.2 caller client

负责：

1. 按 `service_name` 调已有服务
2. 不管理服务生命周期

推荐入口：

1. `GatewayConnect`
2. `GatewayServiceClient`
3. `DirectConnect`

### 2.3 job client

负责：

1. 提交大任务
2. 进入队列等待调度
3. 查询 job 状态与结果

推荐入口：

1. `JobQueueClient`

### 2.4 task pool client

负责：

1. 创建原生专属 pool
2. 往 pool 提交 subtasks
3. 拉结果、取消 job、关闭 pool

推荐入口：

1. `TaskPoolSession`
2. `DedicatedTaskServiceSession`（兼容实现）

## 3. Service Mode

服务模式当前仍然是“模块 + 多函数导出”模型：

1. 上传支持 `py / tar.gz / zip / whl`
2. 注册时指定 `entry_module + export_spec`
3. 导出模式支持 `decorator / explicit / all / single`
4. 当前更适合作为内部函数服务层，而不是对外 Web 应用层

对外推荐入口：

1. `POST /svc/{service_name}/call/{method}`
2. `GET /svc/{service_name}/methods`
3. `GET /svc/{service_name}/status`

## 4. JobQueue Mode

`JobQueue Mode` 负责：

1. 大任务先入队
2. 同一时刻只放行一个大任务进入 `RUNNING`
3. 放行后再创建 `TaskPoolSession`
4. 由 driver 生成 subtasks，交给 pool 执行

当前推荐入口：

1. `JobQueueClient`

## 5. TaskPool Mode

`TaskPool Mode` 当前已经是原生 pool 协议：

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
4. 更适合作为 `JobQueue Mode` 的执行层
5. 每个 pool 当前只暴露一个任务入口，也就是创建时的 `entry_callable`
6. `task_method` 是高层单入口校验参数，不是多方法路由协议
7. `runtime_key` 仍然保留，但它代表 runtime 逻辑隔离键，不再对应独立的 runtime-slot 资源

## 6. 已移除

以下旧共享任务池能力已经移除：

1. 旧共享任务池客户端
2. 旧共享任务池流式入口
3. 旧共享任务结果拉取与取消链路
