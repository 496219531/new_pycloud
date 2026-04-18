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

分工：

| 类 | 模式 | 用途 |
|---|---|---|
| `Service` | Service | 部署并拥有内部函数服务 |
| `TaskPool` | TaskPool | 创建原生专属任务池并执行 subtasks |
| `JobQueue` | JobQueue | 提交大任务、排队、单活调度 |
| `DataRef` | Data | 大对象 / 大结果 / 文件引用 |
| `export` | Artifact | 模块 / package 导出装饰器 |

推荐资料：

- [QUICK_START.md](QUICK_START.md)
- [TASK_CLIENT_GUIDE.md](TASK_CLIENT_GUIDE.md)
- [SERVICE_GUIDE.md](SERVICE_GUIDE.md)
- [SERVICE_GATEWAY_GUIDE.md](SERVICE_GATEWAY_GUIDE.md)
