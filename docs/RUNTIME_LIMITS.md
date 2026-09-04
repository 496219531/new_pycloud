# 运行时限制与大小阈值

当前 controlplane 相关的大小限制已经集中到：

- `pycloud_parallel.controlplane.config`

这些限制默认值已经内置，但都可以通过环境变量覆盖。

详细 authority 分层见：

- [CONFIG_LIMIT_AUTHORITY.md](CONFIG_LIMIT_AUTHORITY.md)

新代码应优先使用 `config.py` 的推荐入口，例如 `resolve_payload_policy(...)`、`get_transport_bounds()`、`get_object_store_bounds()` 和 body/upload helper。裸常量继续保留是为了兼容旧代码和外部 import。

## 0. 默认值速查

| 环境变量 | 默认值 | 说明 |
| --- | ---: | --- |
| `PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES` | `33554432` | payload 是否尝试 inline 的全局分流上限；超过后直接转 `DataRef` |
| `PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES` | `67108864` | 单个 inline payload 全局硬上限 |
| `PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES` | `67108864` | result 是否尝试 inline 的全局分流上限；超过后直接转 object/DataRef |
| `PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES` | `134217728` | 单个 inline result 全局硬上限 |
| `PYCLOUD_OBJECT_CHUNK_SIZE_BYTES` | `262144` | 对象上传/下载默认分片大小 |
| `PYCLOUD_FILE_HASH_CHUNK_SIZE_BYTES` | `1048576` | 本地文件计算 SHA256 时的读取分片大小 |
| `PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES` | `1073741824` | 单个 object/DataRef 背后对象的业务硬上限 |
| `PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES` | `16777216` | 允许整包 bytes 下载/物化的保守阈值，超出后必须走 file/path/streaming |
| `PYCLOUD_OBJECT_SEGMENT_MAX_BYTES` | `8388608` | 单个结果段文件允许的最大大小 |
| `PYCLOUD_OBJECT_SEGMENT_TARGET_BYTES` | `67108864` | 结果段文件滚动写入的目标大小 |
| `PYCLOUD_DATAREF_UPLOAD_STRATEGY` | `upload_once` | 内部链路大对象默认只上传到一个 node，其他层转发 `DataRef` |
| `PYCLOUD_DATAREF_RESOLUTION` | `remote_fetch` | worker/client 解析 `DataRef` 时允许按 locator/registry 远程拉取并本地缓存 |
| `PYCLOUD_JOBQUEUE_RESOLVE_REFS` | `defer_to_worker` | JobQueue 默认不在 job-orch 实例化业务 `DataRef`，交给最终 worker 解析 |
| `PYCLOUD_GATEWAY_DATAREF_RELAY` | `lazy` | gateway 默认转发可信 locator，不复制对象本体；可显式回滚为流式 eager relay |
| `PYCLOUD_CONTROL_HTTP_MAX_SEND_BYTES` | `16777216` | node control HTTP 单条发送消息限制；旧名 `PYCLOUD_CONTROL_MAX_SEND_MESSAGE_LENGTH_BYTES` 仍兼容 |
| `PYCLOUD_CONTROL_HTTP_MAX_RECEIVE_BYTES` | `16777216` | node control HTTP 单条接收消息限制；旧名 `PYCLOUD_CONTROL_MAX_RECEIVE_MESSAGE_LENGTH_BYTES` 仍兼容 |
| `PYCLOUD_NODE_WORKER_CAPACITY` | `32` | `pycloud-control --role node` 与 `pycloudctl dev-start` 的默认 worker capacity；`pycloudctl start` 不启动 node |
| `PYCLOUD_NODE_QUEUE_CAPACITY` | `4000` | `pycloud-control --role node` 的默认 queue capacity；`pycloudctl start-node` 默认值为 `1000`，也可被它覆盖 |
| `PYCLOUD_NODE_MAX_WORKERS` | `64` | NodeControl HTTP server 的默认线程池大小 |
| `PYCLOUD_HTTP_MAX_CONNECTIONS_PER_ORIGIN` | `32` | client 对单个 HTTP origin 的最大并发连接数；空闲连接会复用 |
| `PYCLOUD_HTTP_IDLE_CONNECTION_TTL_SEC` | `0.25` | client 空闲连接保留时间；短于 server 0.5 秒 idle timeout，避免复用临界失效连接 |
| `PYCLOUD_NODE_INACTIVE_RESOURCE_HISTORY_LIMIT` | `100` | node 内保留的已停止 service/task-pool 诊断记录上限 |
| `PYCLOUD_PACKAGE_INCLUDE_TESTS` | `false` | artifact 打包是否包含 tests/test_*.py |
| `PYCLOUD_PACKAGE_CACHE_MAX_ENTRIES` | `128` | 本地 artifact package cache 最大条目数 |
| `PYCLOUD_PACKAGE_CACHE_MAX_BYTES` | `1073741824` | 本地 artifact package cache 最大总字节数 |
| `PYCLOUD_SERVICE_DEFAULT_WORKERS` | `10` | 单个 service 默认 worker 数 |
| `PYCLOUD_SERVICE_HEARTBEAT_TIMEOUT_SEC` | `30` | service 默认 heartbeat timeout |
| `PYCLOUD_TASKPOOL_HEARTBEAT_TIMEOUT_SEC` | `60` | TaskPool owner heartbeat timeout；可按长批量任务或慢网络场景调大 |

## 1. 适合调什么

常见调参场景：

1. 希望 inline payload 更小
   - 例如 1 MiB 内 inline，超过就走 `DataRef`
2. 希望 NodeControl 单条消息限制更大
3. 希望对象上传分片更大或更小
4. 希望 inline result 更保守，尽早走 `DataRef`

## 2. 当前支持的环境变量

### 2.1 inline payload / result

这些是 payload policy threshold，不是 HTTP body limit。它们决定 payload/result 是否 inline、转 `DataRef` 或拒绝；HTTP server/client 的 request body 上限见 `2.4`。

- `PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES`
  - 默认：`33554432` (`32 MiB`)
  - 用于“是否尝试 inline”的全局分流上限
  - cheap estimate 超过该值时直接走 `DataRef`，不再做完整序列化试算

- `PYCLOUD_DEFAULT_SAFE_INLINE_PAYLOAD_THRESHOLD_BYTES`
  - `default_safe` / `gateway_public` 的 inline payload threshold
  - 具体值可由管理员按环境调整

- `PYCLOUD_DEFAULT_SAFE_INLINE_PAYLOAD_HARD_LIMIT_BYTES`
  - `default_safe` / `gateway_public` 的 inline payload hard limit
  - 具体值可由管理员按环境调整

- `PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES`
  - 默认：`67108864` (`64 MiB`)
  - 单个 inline payload 的全局硬上限

- `PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES`
  - 默认：`67108864` (`64 MiB`)
  - 结果是否尝试 inline 的全局分流上限
  - cheap estimate 超过该值时直接走结果 object/DataRef，不再做完整 inline 试算

- `PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES`
  - 默认：`134217728` (`128 MiB`)
  - 单个 inline result 的全局硬上限；超出后更容易走对象缓存 / `DataRef`

实际计算式见 [CONFIG_LIMIT_AUTHORITY.md](CONFIG_LIMIT_AUTHORITY.md) 的“实际表达式”章节。最简版就是：

```text
payload_threshold = min(max(1, PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES), PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES)
result_threshold = min(max(1, PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES), PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES)
local_threshold = min(max(1, PYCLOUD_LOCAL_INLINE_PAYLOAD_THRESHOLD_BYTES), PYCLOUD_LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES)
```

如果调用链带有 policy profile / effective policy，最终 inline 决策还会继续和 profile 的 threshold / hard limit 取更严格值；如果同时传入 `object_threshold_bytes`，payload inline threshold 还会再被它收紧。完整公式见 [CONFIG_LIMIT_AUTHORITY.md](CONFIG_LIMIT_AUTHORITY.md) 的“Inline 最终决策公式”。

补充边界：

1. 多数 internal path 上，inline threshold 的语义是“超过后直接 objectify / DataRef”
2. 但 `gateway_public` 当前不是这样
3. `gateway_public` 不自动做大对象上传，也不接受 external `DataRef`
4. 因此在 `gateway_public` 上，public inline max 仍由 gateway policy 和 gateway body/path 共同约束
5. 具体阈值仍然来自 `default_safe` policy threshold，可由管理员按环境调整

### 2.1.1 hard limit 与 inline threshold

- hard limit 是协议/安全边界：真正 inline 编码后不能超过它。
- inline threshold 是分流边界：cheap estimate 超过它就直接走 `DataRef`。
- inline threshold 会在 `config.py` 中 clamp 到不超过对应 hard limit。
- 系统不再为了争取灰区 inline 命中率而先完整序列化试算。

### 2.2 object / file chunk

- `PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES`
  - 默认：`1073741824` (`1 GiB`)
  - 单个 object / DataRef 背后对象的业务硬上限
  - object upload 和大结果转 DataRef 都受这个限制
  - 这不是 HTTP body limit，也不是 segment layout 阈值

- `PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES`
  - 默认：`16777216` (`16 MiB`)
  - 控制 `download_object_bytes()`、`materialize_as="bytes"`、`structured_v1` / `pickle_stable_v1` 整包反序列化这类内存路径
  - 超过该阈值的大对象应使用 `download_object_to_file()`、`materialize_as="path"` 或 data-plane streaming
  - 这个阈值会被 clamp 到不超过 object size hard limit

- `PYCLOUD_OBJECT_CHUNK_SIZE_BYTES`
  - 默认：`262144` (`256 KiB`)
  - 对象上传、对象下载分片大小
  - 高层 `put_data()/put_dataframe()/put_json()/put_ndarray()` 默认也会用这个值

- `PYCLOUD_FILE_HASH_CHUNK_SIZE_BYTES`
  - 默认：`1048576` (`1 MiB`)
  - 本地文件做 SHA256 计算时的读取分片大小

- `PYCLOUD_OBJECT_SEGMENT_MAX_BYTES`
  - 默认：`8388608` (`8 MiB`)
  - 单个对象段文件允许的最大大小

- `PYCLOUD_OBJECT_SEGMENT_TARGET_BYTES`
  - 默认：`67108864` (`64 MiB`)
  - 对象 / 结果分段写入时，单个段文件的目标滚动大小

### 2.3 DataRef internal path

- `PYCLOUD_DATAREF_UPLOAD_STRATEGY`
  - 默认：`upload_once`
  - 回滚：显式设为 `fanout`
  - 含义：内部可信链路大对象只上传到一个 node，并把带 `node_control` locator 的 `DataRef` 继续向后转发

- `PYCLOUD_DATAREF_RESOLUTION`
  - 默认：`remote_fetch`
  - 回滚：显式设为 `local_only`
  - 含义：worker 先查本地 object cache，未命中时按 `control_addr` / registry 远程下载、校验 checksum 后写入本地缓存

- result data-plane download
  - 入口：`GET /data/refs/{ref_id}/download`
  - 含义：controlplane 通过 registry resolve 到真实 node，再从 node object HTTP 边读边转发给 client
  - 分片大小：复用 `PYCLOUD_OBJECT_CHUNK_SIZE_BYTES`
  - 边界：第一版只做结果下载，不做输入 upload，不让 gateway 承担大结果本体中转

- `PYCLOUD_JOBQUEUE_RESOLVE_REFS`
  - 默认：`defer_to_worker`
  - 回滚：显式设为 `eager`
  - 含义：JobQueue 不在 job-orch 提前 materialize 业务 `DataRef`，最终执行 worker 再解析

- `PYCLOUD_GATEWAY_DATAREF_RELAY`
  - 默认：`lazy`
  - 回滚：显式设为 `eager`
  - 含义：gateway 默认转发可信 locator；`eager` 会通过临时文件流式复制到目标 node，不在 gateway 内整包物化 bytes

### 2.4 control HTTP body size

这些是 transport bounds，限制 HTTP request/response body 或控制面消息的大小。它们不决定单个业务对象是否转 `DataRef`。
单个 object/DataRef 背后对象能有多大，见 `PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES`。

- `PYCLOUD_CONTROL_HTTP_MAX_SEND_BYTES`
  - 默认：`16777216` (`16 MiB`)

- `PYCLOUD_CONTROL_HTTP_MAX_RECEIVE_BYTES`
  - 默认：`16777216` (`16 MiB`)

这两个值表示轻控制消息能力边界，不等同于各 HTTP endpoint 的 body limit。Node runtime/task 通信和 object HTTP 上传都使用 `PYCLOUD_NODE_CONTROL_HTTP_BODY_MAX_BYTES`。

node 侧读取这些值只是为了执行本进程的物理 HTTP body 边界。业务 payload threshold、session effective policy 和最终 limit 仍以中心/session 分配为准，node 不自行协商或改写。

- `PYCLOUD_SERVICE_HTTP_BODY_MAX_BYTES`
  - 默认：`67108864` (`64 MiB`)
  - service HTTP endpoint 的 request body 上限

- `PYCLOUD_GATEWAY_HTTP_BODY_MAX_BYTES`
  - 默认：`67108864` (`64 MiB`)
  - gateway HTTP endpoint 的 request/response body 上限

- `PYCLOUD_INFOCENTER_HTTP_BODY_MAX_BYTES`
  - 默认：`67108864` (`64 MiB`)
  - InfoCenter HTTP endpoint 的 request body 上限

- `PYCLOUD_NODE_CONTROL_HTTP_BODY_MAX_BYTES`
  - 默认：`134217728` (`128 MiB`)
  - NodeControl runtime/control endpoint 的 request body 上限
  - `/objects/...` object 上传也使用这条 body 上限；object 不再拥有单独更大的 HTTP body 后门
  - 单个 object/DataRef 背后的业务大小仍由 `PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES` 控制

### 2.5 node 默认进程/并发参数

- `PYCLOUD_NODE_WORKER_CAPACITY`
  - 默认：`32`
  - 影响 `pycloud-control --role node` 默认 worker capacity
  - `pycloudctl dev-start` / `dev-restart` 如果没有显式给 `--node-worker-capacity`，也会优先使用它
  - `pycloudctl start-node` 如果没有显式给 `--worker-capacity`，也会优先使用它

- `PYCLOUD_NODE_QUEUE_CAPACITY`
  - 默认：`4000`
  - 影响 `pycloud-control --role node` 默认 queue capacity
  - `pycloudctl start-node` 的默认 queue capacity 也可被它覆盖

- `PYCLOUD_NODE_MAX_WORKERS`
  - 默认：`64`
  - NodeControl HTTP server 线程池大小；请求 worker 和等待队列都有硬上限

- `PYCLOUD_HTTP_MAX_CONNECTIONS_PER_ORIGIN`
  - 默认：`32`
  - heartbeat、status、service/task 控制请求共享按 origin 隔离的 HTTP/1.1 连接池
  - GET/HEAD/OPTIONS 遇到失效复用连接可重试一次；POST 不做传输层自动重试，避免重复副作用

- `PYCLOUD_HTTP_IDLE_CONNECTION_TTL_SEC`
  - 默认：`0.25`
  - server 默认 keep-alive idle timeout 为 `0.5` 秒；client 提前淘汰空闲连接，避免在服务端关闭临界点复用 stale socket
  - 配置值会被钳制到 server timeout 的 80% 以内，当前最大为 `0.4` 秒

- `PYCLOUD_SERVICE_DEFAULT_WORKERS`
  - 默认：`10`
  - service session 默认 worker 数

- `PYCLOUD_SERVICE_HEARTBEAT_TIMEOUT_SEC`
  - 默认：`30`
  - service session 默认 heartbeat timeout

## 3. 最常见的设置示例

### 3.0 通过 `pycloudctl --env` 透传

如果你用的是 `pycloudctl`，现在可以直接这样写：

```bash
pycloudctl start-controlplane \
  --env PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES=1048576 \
  --env PYCLOUD_CONTROL_HTTP_MAX_SEND_BYTES=16777216 \
  --env PYCLOUD_CONTROL_HTTP_MAX_RECEIVE_BYTES=16777216
```

也同样适用于：

```bash
pycloudctl start
pycloudctl restart
pycloudctl dev-start
pycloudctl dev-restart
pycloudctl start-gateway
pycloudctl start-node
pycloudctl start-infocenter
```

### 3.1 把 inline payload 缩到 1 MiB 以下就转 DataRef

如果你想更保守：

```bash
export PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES=1048576
```

或者 Windows PowerShell：

```powershell
$env:PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES=1048576
```

### 3.2 放大 control message limit

如果你确实需要更大的单条 NodeControl 消息：

```bash
export PYCLOUD_CONTROL_HTTP_MAX_SEND_BYTES=16777216
export PYCLOUD_CONTROL_HTTP_MAX_RECEIVE_BYTES=16777216
```

也就是 `16 MiB`。

### 3.3 调整对象上传分片大小

```bash
export PYCLOUD_OBJECT_CHUNK_SIZE_BYTES=524288
```

即 `512 KiB`。

### 3.4 调整 node 默认 worker / max_workers

```bash
export PYCLOUD_NODE_WORKER_CAPACITY=8
export PYCLOUD_NODE_MAX_WORKERS=128
export PYCLOUD_SERVICE_DEFAULT_WORKERS=4
```

如果你走 `pycloudctl`：

```bash
pycloudctl start-node \
  --env PYCLOUD_NODE_WORKER_CAPACITY=8 \
  --env PYCLOUD_NODE_MAX_WORKERS=128 \
  --env PYCLOUD_SERVICE_DEFAULT_WORKERS=4
```

## 4. 建议

推荐优先调整顺序：

1. 先调 `PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES`
   - 让大对象更早走 `DataRef`
2. 再考虑调 `PYCLOUD_OBJECT_CHUNK_SIZE_BYTES`
3. 最后才考虑直接放大 control message limit

原因：

1. 直接放大 control message limit 虽然简单
2. 但容易把本来应该走对象路径的大对象继续塞进 inline
3. 长期更难调试，也更容易把内存和带宽问题隐藏掉

## 4.1 常见调参组合

### 组合 A：希望 1 MiB 内尽量 inline

适合：

1. payload 不算大
2. 想少走 `DataRef`

```bash
export PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES=1048576
export PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES=1048576
```

### 组合 B：更激进地把大对象推到 `DataRef`

适合：

1. DataFrame / Series / ndarray 比较多
2. 不希望 HTTP inline 太重

```bash
export PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES=131072
export PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES=131072
```

### 组合 C：放大 control HTTP body size 到 16 MiB

适合：

1. 历史调用里仍有一些大 inline 请求
2. 你明确知道进程内存足够

```bash
export PYCLOUD_CONTROL_HTTP_MAX_SEND_BYTES=16777216
export PYCLOUD_CONTROL_HTTP_MAX_RECEIVE_BYTES=16777216
```

### 组合 D：提高对象分片大小到 512 KiB

适合：

1. 对象上传/下载较多
2. 网络比较稳定

```bash
export PYCLOUD_OBJECT_CHUNK_SIZE_BYTES=524288
```

### 组合 E：通过 `pycloudctl` 一次透传

```bash
pycloudctl start \
  --env PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES=131072 \
  --env PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES=131072 \
  --env PYCLOUD_CONTROL_HTTP_MAX_SEND_BYTES=16777216 \
  --env PYCLOUD_CONTROL_HTTP_MAX_RECEIVE_BYTES=16777216
```

## 5. 备注

这些环境变量是“进程启动时读取”的。

也就是说：

1. 你改完环境变量
2. 需要重新启动 caller / node / controlplane 进程
3. 新值才会生效
