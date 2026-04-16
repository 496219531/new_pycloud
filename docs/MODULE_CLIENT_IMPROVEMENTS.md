# 模块化客户端说明

当前推荐保留的模块化入口有 5 个：

1. `Service`
2. `TaskPool`
3. `compat task facade`
4. `JobQueue`
5. `gateway caller facade`

说明：

1. 旧共享任务池模式已经移除
2. 任务执行现在优先走原生 `TaskPool`
3. 大任务排队入口优先走 `JobQueue`

分工：

| 类 | 模式 | 用途 |
|---|---|---|
| `Service` | Service | 部署并拥有内部函数服务 |
| `TaskPool` | TaskPool | 创建原生专属任务池并执行 subtasks |
| `compat task facade` | Compat TaskPool | 兼容专属池实现，支持复用 `Service.update_globals(...)` |
| `JobQueue` | JobQueue | 提交大任务、排队、单活调度 |
| `gateway caller facade` | Gateway | 按服务名调用内部函数服务 |

推荐资料：

- [QUICK_START.md](QUICK_START.md)
- [TASK_MODULE_CLIENT.md](TASK_MODULE_CLIENT.md)
- [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
- [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
