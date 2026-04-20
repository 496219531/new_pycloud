# Gateway 客户端指南

## 1. 当前定位

Gateway 只服务于服务模式 caller：

1. 按 `service_name` 调用
2. 自动使用 route cache
3. 失败时触发刷新与切换
4. 不负责上传代码和心跳

任务模式不经过 Gateway。

边界上要注意：

1. 这里的 `Gateway` 面向的是内部函数服务调用
2. 它不是标准 Web 应用入口层
3. 如果你需要真正对外的轻网络服务，建议独立使用 `FastAPI/Flask + uvicorn/gunicorn`

## 2. 当前推荐入口

### 2.1 `Service.connect(..., transport="gateway")`

V1 推荐直接把 gateway 当成 `Service` 的一种连接策略：

```python
from pycloud_parallel import Service

client = Service.connect(
    target="127.0.0.1:50051",
    service_name="square-service",
    transport="gateway",
)

print(client.square.sync(x=7))
```

适合：

1. 业务侧只想要统一的 `Service` 心智
2. 希望保留 gateway 代理、route cache 与失败切换
3. 不想直接处理底层 `service_name + method + payload` 细节

### 2.2 `GatewayServiceClient`

这是最薄的 HTTP client。

```python
from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

with GatewayServiceClient("127.0.0.1:50051", timeout_sec=10.0) as client:
    methods = client.list_methods(service_name="square-service")
    status = client.get_status(service_name="square-service")
    result = client.call(
        service_name="square-service",
        method="square",
        payload={"x": 7},
        timeout_sec=10.0,
    )
```

适合：

1. 想明确传 `service_name + method + payload`
2. 想直接拿 HTTP 层返回
3. 脚本或系统集成场景

这是底层 transport client，不是 V1 主产品入口。

适合：

1. 想明确传 `service_name + method + payload`
2. 想直接拿 HTTP 层返回
3. 脚本或系统集成场景

## 3. 典型流程

### 3.1 先部署服务

```python
from pycloud_parallel import Service, export

blob = (
    b"def export(fn):\n"
    b"    fn.__pycloud_export__ = True\n"
    b"    return fn\n\n"
    b"@export\n"
    b"def square(x=0, **_kwargs):\n"
    b"    x = int(x)\n"
    b"    return {'x': x, 'y': x * x}\n"
)

group = Service.deploy(
    target="127.0.0.1:50051",
    service_name="square-service",
    blob=blob,
    runtime="py3",
    entry_module="square_service",
    export_mode="decorator",
    node_count=1,
)
```

### 3.2 再通过 `Service.connect(gateway)` 调用

```python
from pycloud_parallel import Service

client = Service.connect(
    target="127.0.0.1:50051",
    service_name="square-service",
    transport="gateway",
)

print(client.square.sync(x=7))
```

### 3.3 底层 `GatewayServiceClient` 调用

```python
from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

with GatewayServiceClient("127.0.0.1:50051", timeout_sec=10.0) as client:
    print(
        client.call(
            service_name="square-service",
            method="square",
            payload={"x": 7},
            timeout_sec=10.0,
        )
    )
```

## 4. HTTP 入口

当前 Gateway HTTP 已经支持两条并行 transport 通道：

1. JSON 通道
   - `Content-Type: application/json`
   - 继续兼容 `legacy_v1`
   - `structured_v1` 也可继续承载在这里
2. bytes 通道
   - `Content-Type: application/x-pycloud-transport`
   - `X-Pycloud-Codec`
   - `X-Pycloud-Transport-Version`
   - 用于显式 `codec + raw bytes` 载荷

注意：

1. `gateway_public` 默认仍然禁止 `pickle_stable_v1`
2. internal/discovery/owner HTTP 路径才允许 `pickle_stable_v1`
3. route-aware staging / DataRef 语义不因 bytes 通道而改变

Gateway 当前提供：

1. `POST /svc/{service_name}/call/{method}`
2. `GET /svc/{service_name}/methods`
3. `GET /svc/{service_name}/status`

示例：

```bash
curl -X POST 'http://127.0.0.1:50051/svc/square-service/call/square' \
  -H 'Content-Type: application/json' \
  -d '{"x": 7}'
```

## 5. 与 discovery transport 的区别

Gateway 路径：

1. 通过 Gateway 代理
2. 更稳定
3. 更适合外部 caller

Discovery 路径：

1. 客户端先查路由
2. 直接打实例
3. 更适合调试、旁路或特殊性能场景

两条路径当前的默认选路口径已经尽量统一：

1. 默认都按 `predicted_busy` 选路
2. 共同参考的核心指标是：
   - `predicted_busy`
   - `in_flight`
   - `alive_workers`
3. `round_robin` 只用于同分候选打散，不再是主策略

大 payload staging 规则：

1. discovery 只对当前选中 route staging
2. retry route 时重新 staging
3. gateway status lookup 失败时：
   - 有最近缓存 routes，就继续 route-aware staging
   - 没缓存且 payload 需要 staging，就明确报错

也就是说：

1. discovery route cache
2. gateway route cache
3. owner `Service`

默认都在同一套“忙闲预测优先”的心智下工作。

## 6. 常见问题

### 6.1 `no available route for service_name`

说明当前 `InfoCenter` 里没有可用路由：

1. 服务还没部署
2. 服务已经停止
3. 节点不健康或被摘掉

### 6.2 `AttributeError` 方法不存在

说明：

1. 方法没有导出
2. 你访问的方法名不在 `client.methods` 里

先看：

```python
print(client.methods)
```

另外，Gateway 调服务时默认把 JSON body 展开成 kwargs，所以服务函数推荐写成：

```python
def square(x=0, **_kwargs):
    ...
```

### 6.3 Gateway 可以发任务吗

不可以。

当前任务模式仍然是：

1. `InfoCenter` 提供节点事实
2. task client 直连 `NodeControl`
