# MAX_WORKERS 功能说明（历史文档）

> 状态：已下线（2026-03-30 起不再适用）

本文档描述的“`foreach/parallel_for` 传 `max_workers` 自动创建临时 Runtime”方案已经移除。

当前行为以源码为准：

1. 本地并行 API 为固定的全局运行时单例（可通过 `configure(RuntimeConfig(...), reset=True)` 重建）。
2. `RuntimeConfig.max_workers` 仅用于配置该运行时的本地进程池大小。
3. `foreach` / `parallel_for` 不再支持函数级 `max_workers` 参数。

如需跨节点能力，请使用 `controlplane` 路径。
