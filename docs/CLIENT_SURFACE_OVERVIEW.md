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
4. `Service / TaskPool / JobQueue` 当前共用统一 scheduler 核心来回答“这次该选谁”
5. `TaskPool` 的批量 refill / `Service` 的 RPC 发送循环仍然各自保留，不强行混成一套

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

## 序列化模式

当前公开面已经定义 3 个 serialization mode：

1. `legacy_v1`
   - 当前老兼容模式
   - 继续基于 Arrow-compatible / Struct-safe dict
2. `structured_v1`
   - 显式 versioned 结构化 codec
   - bytes 通过结构化 sentinel 表达
3. `pickle_stable_v1`
   - 受信环境高保真 Python codec
   - 外层仍是 pickle
   - 但 `DataFrame / Series / ndarray` 先转稳定 schema 再 pickle
   - 其中 ndarray 的原始数据直接保留为 raw bytes，不再先 base64 文本化

边界说明：

1. `structured_v1` 不是 pickle
2. `pickle_stable_v1` 也不是“任意 Python 对象全支持”的通用 pickle 模式
3. 当前明确支持：
   - pandas `DataFrame / Series / Index`
   - numpy `ndarray`（非 `dtype=object`）
   - 结构化标量 / 容器
4. `ndarray dtype=object` 明确不支持

分层原则：

1. `legacy_v1 / structured_v1 / pickle_stable_v1` 首先是对象 codec 层
2. JSON / Struct / protobuf bytes / object upload blob 属于 transport 容器层
3. `pickle_stable_v1` 不会为了 JSON/Struct 预先把 schema 里的 raw bytes 文本化
4. 如果当前 transport 是 JSON/Struct-only，base64 或拒绝都由 transport 适配层决定
5. 因此：
   - codec 层表达对象本身
   - transport 层表达“这个对象怎么进当前容器”

当前 protobuf/gRPC 主链已经有两条并行 transport 通道：

1. 旧 `Struct` 通道
   - 继续兼容 `legacy_v1`
2. 新 `TransportPayload(codec, version, payload)` 通道
   - `pickle_stable_v1` 优先走这条 bytes 通道
   - 旧字段仍保留以保证兼容

当前 HTTP 主链也有两条并行 transport 通道：

1. 旧 JSON 通道
   - `Content-Type: application/json`
   - 继续兼容 `legacy_v1`
2. 新 bytes 通道
   - `Content-Type: application/x-pycloud-transport`
   - `X-Pycloud-Codec`
   - `X-Pycloud-Transport-Version`
   - `pickle_stable_v1` 优先走这条 bytes 通道

这些 mode 当前已经统一作用于：

1. `put_data() / put_dataframe() / put_ndarray() / put_json()`
2. `Service.connect(...).method(...)` 的主调用链
3. `TaskPool.submit_payloads(...)` 与 task result decode
4. service HTTP request / response
5. `DataRef` 对象上传与物化

公开入口上的默认值优先级：

1. 单次调用显式 `serialization_mode=...`
2. 当前 session 的 `serialization_mode`
3. 当前 system mode / env
4. 默认回退 `legacy_v1`

权限边界：

1. `Service.connect(...)`
2. `Service.deploy(...)`
3. `TaskPool.open(...)`
4. `JobQueue.connect(...)`
5. `put_data() / put_dataframe() / put_ndarray() / put_json()`

这些边界负责选择 mode；内部 transport/helper 只消费和传递 mode，不再私自重选默认值。

另外：

1. 非 legacy transport body 必须显式带 codec/version envelope
2. decode 端优先按 envelope 解码
3. 没有 envelope 时只按 `legacy_v1` 兜底，不再按全局 env 猜 mode
4. 接收端会按当前边界上下文重新校验 declared mode，不是发送端声明什么就无条件接受什么
5. `gateway_public` 默认硬性禁止 `pickle_stable_v1`
