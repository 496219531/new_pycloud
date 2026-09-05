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
from my_package import calc_service

node = Service.startup(
    service_name="calc",
    source=calc_service,
    bind="0.0.0.0:18080",
    managed_global_names=("CALC_DATA_DIR",),
)
node.update_globals({"CALC_DATA_DIR": "/srv/calc/data"})

node.join()
```

它的语义是“启动时部署”，不是运行期动态部署。返回对象是底层启动节点句柄，默认不接受运行期动态部署；普通 `NodeControl` 节点额外支持动态部署。
如果启动脚本退出，startup service 也会随进程关闭；长驻服务应调用 `node.join()` 或用自己的主循环保持进程运行。

主推写法是 `source=imported_module`。这种形式不做远程代码上传，也不把已 import 的 module 重新打包成远端 deploy artifact；本地 `ProcessPoolExecutor` worker 直接按模块名导入本地文件，`worker_count` 表示真实计算进程数。运行配置建议通过 `managed_global_names` + `update_globals(...)` 注入，避免 `cwd` / `os.environ` 这类进程全局状态污染同进程内其它服务。显式 bytes/path artifact 或非 module `package_format` 仍保留为完整 artifact 路径。

`worker_backend` 默认是 `"process"`。`"inline"` 只用于必须与 owner 共享内存状态的内置控制服务；它会在服务进程的 HTTP/IPC 工作线程中执行用户函数，并通过 service 独立 semaphore 将实际执行并发限制为 `worker_count`，不适合 CPU 密集计算。

内部 worker 状态统一包含 `requested_workers/alive_workers/busy_workers/queued/in_flight/worker_pids/executor_generation`。原有 `worker_count` 保留为实际获批进程数；新增字段通过 status/inventory 字典兼容输出，不改变现有 protobuf 字段编号。执行错误同时提供稳定 `error_code`：`UserCodeError`、`DependencyError`、`WorkerCrashed`、`ExecutorUnavailable`、`CallTimeout`、`QueueFull` 或 `SerializationError`。

如果 `target` 为空，`Service.startup(...)` 只在当前进程启动本地 HTTP service，不注册到 `InfoCenter`，也不参与 `InfoCenter` 的 `service_name` 全局排他检查。这是 startup 专属的未注册模式，不等于 `target="local"` 的本地 IPC 模式。`Service.deploy(...)`、`Service.connect(...)`、`TaskPool.open(...)` 等其它入口仍然必须显式传入 `target`；未来的 local 模式也必须显式写成 `target="local"`。

未注册 startup 模式适合不想接受 `InfoCenter` 排他性约束的场景，可以在不同端口启动多个同名 startup service：

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

如果传入普通 `InfoCenter` target，startup service 会先做 `InfoCenter` 排他检查并注册心跳。此时同名服务按已注册服务处理：同名不同 endpoint 会拒绝启动；同名同 endpoint 也必须先成功绑定端口，绑定失败说明已有进程在运行。显式 `target="local"` 将作为单独的本地 IPC 模式实现，不和空 target 混用。

V1 删除旧的本地 `foreach/parallel_for` 辅助入口；公开执行入口收敛到 `Service`、`TaskPool`、`JobQueue`。

## 不再作为 V1 公开概念的名字

以下名字已经退出顶层公开面，不应再作为用户主心智：

1. 旧 transport facade 命名
2. 旧兼容任务会话命名
3. 旧 queue client 命名
4. 旧 task-pool session 命名
5. `pycloud_export`
6. 旧 large-object wrapper 命名

如果你确实需要更底层 HTTP / controlplane client，请从 `pycloud_parallel.controlplane` 导入内部基础设施类，而不是从顶层公开面寻找这些旧名字。

相关资料：

- [QUICK_START.md](QUICK_START.md)
- [V1_ARCHITECTURE_TARGET.md](V1_ARCHITECTURE_TARGET.md)
- [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
