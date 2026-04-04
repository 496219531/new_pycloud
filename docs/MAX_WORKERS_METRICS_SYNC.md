# MAX_WORKERS 指标同步说明（当前状态）

> 这份文档保留的目的，是说明这套机制为什么已经不存在，以及现在应该怎么看本地运行时指标。

## 1. 背景

历史上，这个项目曾经有过“函数级 `max_workers` 创建临时 Runtime”的设计。

当时会带来两个额外问题：

1. 错误怎么回流到全局运行时。
2. 临时运行时的指标怎么同步到全局运行时。

现在这套设计已经移除，所以“指标同步”本身也不再是当前实现的一部分。

## 2. 当前结论

当前本地运行时只有一个核心语义：

1. 本地并行 API 走单机多进程。
2. `RuntimeConfig.max_workers` 只影响当前本地运行时。
3. 不存在“函数级临时 Runtime”。
4. 因此也不存在“临时 Runtime 指标同步到全局 Runtime”的路径。

## 3. 当前指标含义

### 3.1 `metrics()`

当前返回的是全局本地运行时累计指标，例如：

1. `submitted`
2. `succeeded`
3. `failed`

这些指标描述的是当前进程里、本地运行时看到的执行情况。

### 3.2 `last_errors()`

当前返回的是全局本地运行时最近一次 `foreach` / `parallel_for` 执行产生的错误信息。

它不再需要考虑“临时 Runtime -> 全局 Runtime”的合并。

## 4. 当前不再存在的东西

以下概念都属于历史设计，不再适用：

1. 函数级 `max_workers` 触发临时 Runtime
2. 临时 Runtime 与全局 Runtime 的错误合并
3. 临时 Runtime 与全局 Runtime 的指标同步
4. 一次调用结束后把临时执行状态回灌给全局运行时

## 5. 现在该怎么理解

如果你只看当前实现，可以把它理解成：

1. 本地并行 API 只有一个本地运行时视角。
2. 本地指标就是本地指标。
3. 跨节点执行不走这里，统一走 `controlplane`。

也就是说：

1. `local_runtime` 只关心单机多进程。
2. `controlplane` 负责跨节点和服务会话。

## 6. 参考

1. [README.md](../README.md)
2. [ARCHITECTURE_OVERVIEW.md](./ARCHITECTURE_OVERVIEW.md)
3. [api.py](../src/pycloud_parallel/local_runtime/api.py)
4. [runtime.py](../src/pycloud_parallel/local_runtime/runtime.py)
