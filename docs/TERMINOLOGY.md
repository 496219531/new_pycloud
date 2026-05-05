# 术语说明

本文档用于统一 PyCloud 当前通信、序列化、payload 与对象存储相关用语。

## 1. 分层原则

不要把 target、route、protocol、codec、carrier、object store 混成一个词。

| 层次 | 推荐用语 | 含义 |
| --- | --- | --- |
| 连接目标层 | `target` | 连接哪里，例如 `local` / `127.0.0.1:50051` / 远端地址 |
| 接入路径层 | `route` | 怎么接入，例如 `local` / `discovery` / `gateway`；`direct` 预留给未来直连 |
| 协议层 | `protocol` | 底层通信协议；当前主线只有 HTTP |
| 对象编码层 | `serialization mode` / `codec` | 一个 Python 对象如何编码与解码 |
| 调用载荷层 | `payload` / `result` | 一次调用的业务输入或输出 |
| 传输容器层 | `carrier` | payload/result 在当前协议里的承载形态 |
| HTTP 二进制体 | HTTP raw-bytes body | HTTP request/response body 直接放 codec bytes |
| 内部兼容 adapter | `TransportPayload` adapter | 旧 proto/state 路径里带 `codec/version/payload(bytes)` 的兼容结构 |
| 大对象层 | `DataRef` / object store | 长期或跨节点引用的大对象/文件/大结果 |

## 2. target / route / protocol

这三个词只描述“怎么连上服务”，不描述对象怎么编码，也不描述 payload 放在 JSON 还是 raw bytes body 里。

1. `target`
   - 含义：连接哪里
   - 例子：`local`、`127.0.0.1:50051`、远端 InfoCenter / ControlPlane 地址
   - `target="local"` 表示 local IPC runtime
2. `route`
   - 含义：怎么接入目标
   - 当前值：`local`、`discovery`、`gateway`
   - `discovery` 表示先查 service discovery，再直连具体 service HTTP endpoint
   - `gateway` 表示通过 gateway 统一入口转发
   - `direct` 是未来直连 endpoint 的预留概念，当前不要当成已实现能力
3. `protocol`
   - 含义：底层 wire protocol
   - 当前值：`http`
   - 以后如果出现其他协议，也应该放在 `protocol`，不要塞进 `route`

推荐写法：

```python
Service.connect(
    target="127.0.0.1:50051",
    service_name="demo",
    route="discovery",
    protocol="http",
)
```

不要再用 `transport="gateway"` 或 `transport="discovery"` 表达接入路径。
`transport` 这个词只允许在内部实现泛称中偶尔出现，不作为公开参数名。

## 3. serialization mode

`serialization_mode` 只描述对象怎么编解码，不描述走 HTTP 还是走内部消息。

当前 mode：

1. `legacy_v1`
   - 老兼容模式
   - 面向 JSON / Struct-safe dict
2. `structured_v1`
   - versioned 结构化 codec
   - bytes 通过结构化 sentinel 表达
3. `pickle_stable_v1`
   - 内部可信链路高保真 codec
   - `DataFrame / Series / ndarray` 会先归一到稳定 schema
   - gateway public 默认拒绝

不要说“pickle 模式一定走 bytes”。是否走 HTTP raw-bytes body 由 `effective_policy` 和当前调用路径决定。

## 4. carrier

`carrier` 是“把已经编码好的 payload/result 放进哪种协议容器”。

当前推荐主线：

1. JSON carrier
   - HTTP `Content-Type: application/json`
   - 适合 `legacy_v1` 和公开安全路径
2. HTTP raw-bytes body
   - HTTP body 直接是 codec bytes
   - header 带 `X-Pycloud-Codec` / `X-Pycloud-Transport-Version`
   - 不是 JSON 里 base64 字符串

兼容层：

1. Struct carrier
   - 内部 protobuf `Struct`
   - 仍用于兼容老路径
2. `TransportPayload` adapter
   - 形态：`TransportPayload { codec, version, payload(bytes) }`
   - 旧 NodeControl / TaskPool / Service state 路径仍会用
   - 新 HTTP wire 设计不应继续扩散这个概念

## 5. protobuf 与 gRPC

`protobuf` 不是 gRPC。

当前项目里：

1. `protobuf` 表示消息 schema / 生成的 `pb2` 类 / `Struct` 等数据结构。
2. `gRPC` 是一种 RPC 传输协议。
3. 清掉 gRPC runtime 不等于清掉 protobuf carrier。
4. `pb2.TransportPayload` 当前仍是内部兼容 adapter。
5. `proto/pycloud_v1.proto` 里的旧字段名会暂时保留，作为 schema 兼容层。

因此，看到 `TransportPayload` 或 `pb2` 时，不要自动理解成 gRPC。

## 6. HTTP raw-bytes body

推荐用语：HTTP raw-bytes body。

含义：

1. body 是 codec 输出的原始 bytes
2. header 声明 codec/version
3. 用于一次调用的 payload/result
4. 不负责长期保存对象
5. 不等价于 `DataRef`

相关 policy 字段：

1. `use_http_raw_bytes_body`
   - HTTP 调用是否使用 raw bytes body
2. `use_raw_bytes_payload`
   - 兼容旧内部 carrier 路径的开关；新 HTTP wire 语义优先看 `use_http_raw_bytes_body`

## 7. DataRef

`DataRef` 是大对象引用，不是一次调用的 HTTP raw-bytes body。

适用场景：

1. payload 超过 soft limit
2. result 超过 inline hard limit
3. 文件、大 DataFrame、大 ndarray
4. 需要跨节点 remote fetch / materialize

`DataRef` 背后通常会落到 object store / node-local cache。
HTTP raw-bytes body 则只是一条请求或响应的 body。

## 8. limit 用语

推荐这样理解：

1. `inline payload soft limit`
   - 单个对象建议转 `DataRef` 的阈值
   - 超过后优先 objectify
2. `inline payload hard limit`
   - 单个 inline payload 的硬限制
   - 超过就是错误或必须转 `DataRef`
3. `inline payload request limit`
   - 一次请求内所有 inline payload 的总硬限制
4. `inline result hard limit`
   - 单个 inline result 的硬限制
   - 超过后应转对象缓存 / `DataRef`
5. `HTTP body limit`
   - HTTP server/client 对整个 body 的限制
   - 与 DataRef/object store limit 是不同层次

## 9. 旧用语对照

| 旧用语 | 新推荐用语 |
| --- | --- |
| `transport="gateway"` | `route="gateway", protocol="http"` |
| `transport="discovery"` | `route="discovery", protocol="http"` |
| `transport_payload_bytes` | `TransportPayload` adapter / legacy internal raw bytes |
| `http_bytes_transport` | HTTP raw-bytes body |
| `protobuf bytes lane` | `TransportPayload` adapter |
| `HTTP bytes lane` | HTTP raw-bytes body |
| `control message size` | control HTTP message size / HTTP body size |

旧 proto 字段和兼容 alias 可以继续存在，但新文档、新 config、新 HTTP endpoint 说明应使用主线用语：JSON carrier、HTTP raw-bytes body、DataRef。
