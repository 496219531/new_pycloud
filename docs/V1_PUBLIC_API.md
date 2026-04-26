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

## `Service.startup(...)`

启动时固定挂载服务归到 `Service` 产品入口下，和 `deploy(...)`、`connect(...)` 并列：

```python
from pycloud_parallel import Service

node = Service.startup(
    service_name="calc",
    entry_module="my_package.calc_service",
    bind="0.0.0.0:18080",
)
```

它的语义是“启动时部署”，不是运行期动态部署。返回对象是底层启动节点句柄，默认不接受运行期动态部署；普通 `NodeControl` 节点额外支持动态部署。

本地并行入口单独放到：

```python
from pycloud_parallel.local import configure, foreach, parallel_for
```

## 不再作为 V1 公开概念的名字

以下名字已经退出顶层公开面，不应再作为用户主心智：

1. 旧 transport facade 命名
2. 旧兼容任务会话命名
3. 旧 queue client 命名
4. 旧 task-pool session 命名
5. `pycloud_export`
6. 旧 large-object wrapper 命名

如果你确实需要更底层 transport / controlplane client，请从 `pycloud_parallel.controlplane` 导入内部基础设施类，而不是从顶层公开面寻找这些旧名字。

相关资料：

- [QUICK_START.md](QUICK_START.md)
- [V1_ARCHITECTURE_TARGET.md](V1_ARCHITECTURE_TARGET.md)
- [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
