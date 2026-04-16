# 当前入口类

当前任务与服务入口已经收敛为这些名字：

1. `Service`
2. `TaskPool`
3. `compat task facade`
4. `JobQueue`
5. `gateway caller facade`
6. `discovery caller facade`

说明：

1. 旧共享任务池入口已移除
2. 共享任务池模式已废弃
3. `TaskPool` 是当前原生专属任务池入口

最常用导入：

```python
from pycloud_parallel import (
    Service,
    TaskPool,
    compat task facade,
    JobQueue,
    gateway caller facade,
    discovery caller facade,
)
```

相关资料：

- [QUICK_START.md](QUICK_START.md)
- [TASK_MODE.md](TASK_MODE.md)
- [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
- [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
