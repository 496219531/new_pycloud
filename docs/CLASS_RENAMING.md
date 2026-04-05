# 当前入口类

当前任务与服务入口已经收敛为这些名字：

1. `DeployedService`
2. `TaskPoolSession`
3. `DedicatedTaskServiceSession`
4. `JobQueueClient`
5. `GatewayConnect`
6. `DirectConnect`

说明：

1. 旧共享任务池入口已移除
2. 共享任务池模式已废弃
3. `TaskPoolSession` 是当前原生专属任务池入口

最常用导入：

```python
from pycloud_parallel import (
    DeployedService,
    TaskPoolSession,
    DedicatedTaskServiceSession,
    JobQueueClient,
    GatewayConnect,
    DirectConnect,
)
```

相关资料：

- [QUICK_START.md](QUICK_START.md)
- [TASK_MODE.md](TASK_MODE.md)
- [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
- [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
