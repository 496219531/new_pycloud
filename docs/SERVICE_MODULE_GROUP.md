# ServiceModuleGroup

`ServiceModuleGroup` 是 `ServiceGroup` 的薄封装，让远程服务更像本地 Python 模块来调用。

## 1. 核心体验

### 1.1 异步调用

```python
result = await group.square(x=7)
```

### 1.2 同步调用

```python
result = group.square.sync(x=7)
```

### 1.3 广播调用

```python
results = await group.square.broadcast(x=7)
```

### 1.4 通用接口

```python
result = await group.call("square", x=7)
result = group.call_sync("square", x=7)
```

## 2. 适合的场景

它适合这类服务：

1. 一个模块导出多个函数。
2. 希望调用体验接近本地模块。
3. 希望保留多节点部署、owner 长驻、熔断器和节点均衡能力。

## 3. 基本示例

```python
from pycloud_parallel.controlplane.client import ServiceModuleGroup

blob = (
    b"def pycloud_export(fn):\n"
    b"    fn.__pycloud_export__ = True\n"
    b"    return fn\n\n"
    b"@pycloud_export\n"
    b"def square(payload):\n"
    b"    x = int(payload.get('x', 0))\n"
    b"    return {'x': x, 'y': x * x}\n"
)

group = ServiceModuleGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="demo-client",
    service_name="square-service",
    blob=blob,
    filename="square_service.py",
    entry_module="square_service",
    export_mode="decorator",
    export_decorator="pycloud_export",
    worker_count=1,
    node_count=1,
)

joined = False

try:
    print(group.square.sync(x=7))
    print("service is now running; press Ctrl+C to stop")
    group.join(
        end_services_on_interrupt=True,
        end_reason="owner ctrl+c",
    )
    joined = True
finally:
    group.close(end_services=not joined)
```

## 4. 方法发现

`ServiceModuleGroup` 会从某个已建立的 session 调 `ListServiceMethods`，然后缓存方法名。

可直接查看：

```python
print(group.methods)
print(group.list_methods())
```

如果访问不存在的方法，会抛 `AttributeError`。

## 5. 与 ServiceGroup 的关系

它继承自 `ServiceGroup`，所以这些能力都还在：

1. `join()`
2. `call_balanced(...)`
3. `acall_balanced(...)`
4. `acall_all(...)`
5. `end(...)`
6. `close(...)`
7. 熔断器和节点选择策略

推荐 owner 侧优先使用：

1. 复用 `ServiceGroup.deploy_from_infocenter(...)` 的 owner 部署逻辑
2. `deploy_from_infocenter(...)` 成功后自动开始 keepalive
3. `group.join(...)` 作为长驻入口
4. `Ctrl+C` 作为正常结束路径
5. `group.close(...)` 作为异常场景兜底

## 6. 当前部署语义

`deploy_from_infocenter(...)` 当前默认策略：

1. 活跃 `service_name` 视为全局唯一。
2. 调用方对外仍然按 `service_name` 发现服务，`service_id` 只在实例管理时使用。
3. 同 `owner_client_id + service_name + code_version` 时，默认复用已有服务。
4. 同名但代码变化时，默认拒绝覆盖。
5. 显式 `replace_existing_if_code_changed=True` 才会替换。
6. 客户端会把 `service_id/service_token` 本地落盘，供重启后复用。
7. 默认只选择需要的节点数，不会默认铺满所有节点。

常用参数：

```python
group = ServiceModuleGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="demo-client",
    service_name="square-service",
    blob=blob,
    filename="square_service.py",
    entry_module="square_service",
    worker_count=1,
    node_count=1,
    reuse_existing_same_code=True,
    replace_existing_if_code_changed=False,
    session_cache_dir="./.demo_service_sessions",
)
```

## 7. 节点选择

如果不显式传 `node_ids`，客户端会：

1. 从 InfoCenter 查询节点。
2. 过滤 `healthy=false`。
3. 过滤 `schedulable=false`。
4. 过滤 `drain=true`。
5. 按 `service_worker_available` 选择前 N 个节点。

适合本地轻量部署或简单多节点部署。

## 8. 权限边界

1. owner 管理面依赖 `service_token`。
2. `ServiceModuleGroup` 适合“我自己部署、我自己持有 token、我自己长驻并维持服务”的场景。
3. 如果只是“发现已有服务并调用”，更适合走 `ControlPlane Gateway` 的 `POST /svc/{service_name}/call/{method}`，不把自己当 owner。
4. 只有在调试、旁路排查或内部直连时，才建议自己查 route 再打某个节点上的 `service_id` 数据面。

## 9. 推荐验证

1. `python scripts/demo_service_module_group.py`
2. `python scripts/demo_simple_deploy.py`
3. `python scripts/grpc_register_service_client_demo.py`
4. `docs/ARCHITECTURE_OVERVIEW.md`

## 10. 轻量 Caller 版本

如果你不是 owner，只想像本地模块一样调一个已经存在的服务，更适合使用 `GatewayModuleClient`：

```python
from pycloud_parallel.controlplane.client import GatewayModuleClient

client = GatewayModuleClient(
    "127.0.0.1:50051",
    service_name="square-service",
)

print(client.square.sync(x=7))
# 或
# result = await client.square(x=7)
```

它和 `ServiceModuleGroup` 的差别是：

1. 只做 caller
2. 不部署服务
3. 不心跳
4. 不结束服务
5. 底层统一走 `controlplane Gateway`
