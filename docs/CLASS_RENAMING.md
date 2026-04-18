# V1 公开命名

V1 顶层公开面已经收敛为 5 个概念：

1. `Service`
2. `TaskPool`
3. `JobQueue`
4. `DataRef`
5. `export`

最常用导入：

```python
from pycloud_parallel import (
    Service,
    TaskPool,
    JobQueue,
    DataRef,
    export,
)
```

本地并行入口单独放到：

```python
from pycloud_parallel.local import configure, foreach, parallel_for
```

## 不再作为 V1 公开概念的名字

以下名字已经退出顶层公开面，不应再作为用户主心智：

1. gateway caller facade
2. discovery caller facade
3. compat task facade
4. 旧 queue client naming
5. 旧 task-pool session naming
6. `pycloud_export`
7. `ObjectRef / ResultRef`

如果你确实需要更底层 transport / controlplane client，请从 `pycloud_parallel.controlplane` 导入内部基础设施类，而不是从顶层公开面寻找这些旧名字。

相关资料：

- [QUICK_START.md](QUICK_START.md)
- [V1_ARCHITECTURE_TARGET.md](V1_ARCHITECTURE_TARGET.md)
- [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
