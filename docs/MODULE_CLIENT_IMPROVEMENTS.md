# 模块化客户端说明

当前推荐保留的模块化入口有 5 个：

1. `DeployedService`
2. `TaskPoolSession`
3. `DedicatedTaskServiceSession`
4. `JobQueueClient`
5. `GatewayConnect`

说明：

1. 旧共享任务池模式已经移除
2. 任务执行现在优先走原生 `TaskPoolSession`
3. 大任务排队入口优先走 `JobQueueClient`

分工：

| 类 | 模式 | 用途 |
|---|---|---|
| `DeployedService` | Service | 部署并拥有内部函数服务 |
| `TaskPoolSession` | TaskPool | 创建原生专属任务池并执行 subtasks |
| `DedicatedTaskServiceSession` | Compat TaskPool | 兼容专属池实现，支持复用 `ServiceGroup.update_globals(...)` |
| `JobQueueClient` | JobQueue | 提交大任务、排队、单活调度 |
| `GatewayConnect` | Gateway | 按服务名调用内部函数服务 |

推荐资料：

- [QUICK_START.md](QUICK_START.md)
- [TASK_MODULE_CLIENT.md](TASK_MODULE_CLIENT.md)
- [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
- [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
