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

如果 `target` 为空，`Service.startup(...)` 只在当前进程启动本地 HTTP service，不注册到 `InfoCenter`，也不参与 `InfoCenter` 的 `service_name` 全局排他检查。这是一种有用的本地孤岛模式：当你明确不想接受 `InfoCenter` 的排他性约束时，可以在不同端口启动多个同名 startup service。

```python
node_a = Service.startup(
    service_name="calc",
    entry_module="my_package.calc_service",
    bind="127.0.0.1:18080",
)
node_b = Service.startup(
    service_name="calc",
    entry_module="my_package.calc_service",
    bind="127.0.0.1:18081",
)
```

这种模式的硬约束只来自本机端口绑定：同一台机器上的同一个 `bind` 地址不能被两个进程同时占用。由于没有注册到 `InfoCenter`，它也不会被 `Service.connect(target=<infocenter>, ...)` 或 Gateway 自动发现；调用方需要直接使用它暴露的本地 service HTTP 地址。

如果传入 `target`，startup service 会先做 `InfoCenter` 排他检查并注册心跳。此时同名服务按已注册服务处理：同名不同 endpoint 会拒绝启动；同名同 endpoint 也必须先成功绑定端口，绑定失败说明已有进程在运行。

V1 删除旧的本地 `foreach/parallel_for` 辅助入口；公开执行入口收敛到 `Service`、`TaskPool`、`JobQueue`。

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
