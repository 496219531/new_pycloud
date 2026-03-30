# MAX_WORKERS 指标同步说明（历史文档）

> 状态：已下线（2026-03-30 起不再适用）

本文档原先描述“临时 Runtime 与全局 Runtime 的错误/指标同步”机制。  
由于函数级 `max_workers` 临时 Runtime 已移除，该机制也随之失效。

当前语义：

1. `last_errors()`：返回全局运行时最后一次 `foreach` 调用的错误。
2. `metrics()`：返回全局运行时累计指标（`submitted/succeeded/failed`）。
3. 不存在“临时 Runtime 指标同步到全局 Runtime”的路径。

请参考：

- [README.md](/Users/hkk/Documents/new_pycloud/README.md)
- [src/pycloud_parallel/local_runtime/api.py](/Users/hkk/Documents/new_pycloud/src/pycloud_parallel/local_runtime/api.py)
- [src/pycloud_parallel/local_runtime/runtime.py](/Users/hkk/Documents/new_pycloud/src/pycloud_parallel/local_runtime/runtime.py)
