# 运行时限制与大小阈值

当前 controlplane 相关的大小限制已经集中到：

- `pycloud_parallel.controlplane.config`

这些限制默认值已经内置，但都可以通过环境变量覆盖。

## 0. 默认值速查

| 环境变量 | 默认值 | 说明 |
| --- | ---: | --- |
| `PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES` | `524288` | inline payload 建议转 `DataRef` 阈值 |
| `PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES` | `2097152` | 单个 inline payload 硬限制 |
| `PYCLOUD_INLINE_PAYLOAD_REQUEST_LIMIT_BYTES` | `8388608` | 单次请求所有 inline payload 总硬限制 |
| `PYCLOUD_INLINE_RESULT_SOFT_LIMIT_BYTES` | `1048576` | inline result 建议转 `DataRef` 阈值 |
| `PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES` | `4194304` | 单个 inline result 硬限制 |
| `PYCLOUD_OBJECT_CHUNK_SIZE_BYTES` | `262144` | 对象上传/下载默认分片大小 |
| `PYCLOUD_FILE_HASH_CHUNK_SIZE_BYTES` | `1048576` | 本地文件计算 SHA256 时的读取分片大小 |
| `PYCLOUD_OBJECT_SEGMENT_MAX_BYTES` | `8388608` | 单个结果段文件允许的最大大小 |
| `PYCLOUD_OBJECT_SEGMENT_TARGET_BYTES` | `67108864` | 结果段文件滚动写入的目标大小 |
| `PYCLOUD_DATAREF_UPLOAD_STRATEGY` | `upload_once` | 内部链路大对象默认只上传到一个 node，其他层转发 `DataRef` |
| `PYCLOUD_DATAREF_RESOLUTION` | `remote_fetch` | worker/client 解析 `DataRef` 时允许按 locator/registry 远程拉取并本地缓存 |
| `PYCLOUD_JOBQUEUE_RESOLVE_REFS` | `defer_to_worker` | JobQueue 默认不在 job-orch 实例化业务 `DataRef`，交给最终 worker 解析 |
| `PYCLOUD_GATEWAY_DATAREF_RELAY` | `eager` | gateway 仍保持旧的 eager relay 默认，外部链路后续单独收口 |
| `PYCLOUD_CONTROL_MAX_SEND_MESSAGE_LENGTH_BYTES` | `16777216` | NodeControl 单条发送消息限制 |
| `PYCLOUD_CONTROL_MAX_RECEIVE_MESSAGE_LENGTH_BYTES` | `16777216` | NodeControl 单条接收消息限制 |
| `PYCLOUD_NODE_WORKER_CAPACITY` | `32` | `pycloud-control --role node` 的默认 worker capacity；`pycloudctl start` 未显式指定时优先读它 |
| `PYCLOUD_NODE_QUEUE_CAPACITY` | `4000` | `pycloud-control --role node` 的默认 queue capacity；`pycloudctl start-node` 默认值为 `1000`，也可被它覆盖 |
| `PYCLOUD_NODE_MAX_WORKERS` | `64` | NodeControl HTTP server 的默认线程池大小 |
| `PYCLOUD_SERVICE_DEFAULT_WORKERS` | `10` | 单个 service 默认 worker 数 |
| `PYCLOUD_SERVICE_HEARTBEAT_TIMEOUT_SEC` | `30` | service 默认 heartbeat timeout |

## 1. 适合调什么

常见调参场景：

1. 希望 inline payload 更小
   - 例如 1 MiB 内 inline，超过就走 `DataRef`
2. 希望 NodeControl 单条消息限制更大
3. 希望对象上传分片更大或更小
4. 希望 inline result 更保守，尽早走 `DataRef`

## 2. 当前支持的环境变量

### 2.1 inline payload / result

- `PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES`
  - 默认：`524288` (`512 KiB`)
  - 用于“建议转 DataRef”的阈值

- `PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES`
  - 默认：`2097152` (`2 MiB`)
  - 单个 inline payload 的硬限制

- `PYCLOUD_INLINE_PAYLOAD_REQUEST_LIMIT_BYTES`
  - 默认：`8388608` (`8 MiB`)
  - 一次请求里所有 inline payload 的总硬限制

- `PYCLOUD_INLINE_RESULT_SOFT_LIMIT_BYTES`
  - 默认：`1048576` (`1 MiB`)
  - 结果建议转 `DataRef` 的阈值

- `PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES`
  - 默认：`4194304` (`4 MiB`)
  - 单个 inline result 的硬限制；超出后更容易走对象缓存 / `DataRef`

### 2.2 object / file chunk

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

- `PYCLOUD_JOBQUEUE_RESOLVE_REFS`
  - 默认：`defer_to_worker`
  - 回滚：显式设为 `eager`
  - 含义：JobQueue 不在 job-orch 提前 materialize 业务 `DataRef`，最终执行 worker 再解析

- `PYCLOUD_GATEWAY_DATAREF_RELAY`
  - 默认：`eager`
  - 含义：gateway 仍使用旧默认；外部 gateway_public 的 DataRef locator 信任策略不在本轮调整

### 2.4 control message size

- `PYCLOUD_CONTROL_MAX_SEND_MESSAGE_LENGTH_BYTES`
  - 默认：`16777216` (`16 MiB`)

- `PYCLOUD_CONTROL_MAX_RECEIVE_MESSAGE_LENGTH_BYTES`
  - 默认：`16777216` (`16 MiB`)

当前 `NodeControlClient` 和 `build_nodecontrol_server(...)` 都会读取这两个值，统一设置 NodeControl HTTP inline message size 限制。

### 2.5 node 默认进程/并发参数

- `PYCLOUD_NODE_WORKER_CAPACITY`
  - 默认：`32`
  - 影响 `pycloud-control --role node` 默认 worker capacity
  - `pycloudctl start` / `start-node` 如果没有显式给 `--node-worker-capacity` / `--worker-capacity`，也会优先使用它

- `PYCLOUD_NODE_QUEUE_CAPACITY`
  - 默认：`4000`
  - 影响 `pycloud-control --role node` 默认 queue capacity
  - `pycloudctl start-node` 的默认 queue capacity 也可被它覆盖

- `PYCLOUD_NODE_MAX_WORKERS`
  - 默认：`64`
  - NodeControl HTTP server 线程池大小

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
  --env PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=1048576 \
  --env PYCLOUD_CONTROL_MAX_SEND_MESSAGE_LENGTH_BYTES=16777216 \
  --env PYCLOUD_CONTROL_MAX_RECEIVE_MESSAGE_LENGTH_BYTES=16777216
```

也同样适用于：

```bash
pycloudctl start
pycloudctl restart
pycloudctl start-gateway
pycloudctl start-node
pycloudctl start-infocenter
```

### 3.1 把 inline payload 缩到 1 MiB 以下就转 DataRef

如果你想更保守：

```bash
export PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=1048576
```

或者 Windows PowerShell：

```powershell
$env:PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=1048576
```

### 3.2 放大 control message limit

如果你确实需要更大的单条 NodeControl 消息：

```bash
export PYCLOUD_CONTROL_MAX_SEND_MESSAGE_LENGTH_BYTES=16777216
export PYCLOUD_CONTROL_MAX_RECEIVE_MESSAGE_LENGTH_BYTES=16777216
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

1. 先调 `PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES`
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
export PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=1048576
export PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES=1048576
```

### 组合 B：更激进地把大对象推到 `DataRef`

适合：

1. DataFrame / Series / ndarray 比较多
2. 不希望 HTTP inline 太重

```bash
export PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=131072
export PYCLOUD_INLINE_RESULT_SOFT_LIMIT_BYTES=131072
```

### 组合 C：放大 control message size 到 16 MiB

适合：

1. 历史调用里仍有一些大 inline 请求
2. 你明确知道进程内存足够

```bash
export PYCLOUD_CONTROL_MAX_SEND_MESSAGE_LENGTH_BYTES=16777216
export PYCLOUD_CONTROL_MAX_RECEIVE_MESSAGE_LENGTH_BYTES=16777216
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
  --env PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=131072 \
  --env PYCLOUD_INLINE_RESULT_SOFT_LIMIT_BYTES=131072 \
  --env PYCLOUD_CONTROL_MAX_SEND_MESSAGE_LENGTH_BYTES=16777216 \
  --env PYCLOUD_CONTROL_MAX_RECEIVE_MESSAGE_LENGTH_BYTES=16777216
```

## 5. 备注

这些环境变量是“进程启动时读取”的。

也就是说：

1. 你改完环境变量
2. 需要重新启动 caller / node / controlplane 进程
3. 新值才会生效
