# ModuleLikeServiceGroup

`ModuleLikeServiceGroup` 是 `MultiNodeServiceGroup` 的一个薄封装，让远程服务更像本地 Python 模块来用。

## 核心体验

1. 异步调用：

```python
result = await group.square(x=7)
```

2. 同步调用：

```python
result = group.square.sync(x=7)
```

3. 广播到所有节点：

```python
results = await group.square.broadcast(x=7)
```

4. 通用接口：

```python
result = await group.call("square", x=7)
result = group.call_sync("square", x=7)
```

## 相关类

实现位于 `src/pycloud_parallel/controlplane/client.py`：

1. `_CallProxy`
2. `_SyncCallProxy`
3. `_BroadcastProxy`
4. `ModuleLikeServiceGroup`

## 基本示例

```python
from pycloud_parallel.controlplane.client import ModuleLikeServiceGroup, pycloud_export

blob = (
    b"def pycloud_export(fn):\n"
    b"    fn.__pycloud_export__ = True\n"
    b"    return fn\n\n"
    b"@pycloud_export\n"
    b"def square(payload):\n"
    b"    x = int(payload.get('x', 0))\n"
    b"    return {'x': x, 'y': x * x}\n"
)

group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="demo-client",
    service_name="square-service",
    blob=blob,
    filename="square_service.py",
    entry_module="square_service",
    export_mode="decorator",
)

group.start_keepalive()

try:
    result = group.square.sync(x=7)
    print(result)
finally:
    group.close(end_services=True)
```

## 方法发现

`ModuleLikeServiceGroup` 会先从首个 session 拉取 `ListServiceMethods`，然后缓存方法名。

可直接查看：

```python
print(group.methods)
print(group.list_methods())
```

返回值都是 `List[str]`，例如：

```python
["square", "cube", "fibonacci"]
```

如果访问不存在的方法，会抛 `AttributeError`。

## 与 MultiNodeServiceGroup 的关系

`ModuleLikeServiceGroup` 继承自 `MultiNodeServiceGroup`，所以这些能力都还在：

1. `start_keepalive()`
2. `stop_keepalive()`
3. `call_balanced(...)`
4. `acall_balanced(...)`
5. `acall_all(...)`
6. `end(...)`
7. `close(...)`
8. 熔断器与节点选择策略

换句话说，它只是把调用入口包装得更像模块，不是另一套运行时。

## 当前部署语义

`ModuleLikeServiceGroup.deploy_from_infocenter(...)` 继承了 `MultiNodeServiceGroup` 的当前默认策略：

1. `service_name` 在活跃服务里视为全局唯一。
2. 同 `owner_client_id + service_name + code_version` 时，默认直接复用已有服务。
3. 同名但代码版本变化时，默认拒绝覆盖。
4. 只有显式传 `replace_existing_if_code_changed=True` 才会替换。
5. 客户端会把 `service_id/service_token` 本地落盘，供重启后复用。

常用参数：

```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="demo-client",
    service_name="square-service",
    blob=blob,
    filename="square_service.py",
    entry_module="square_service",
    reuse_existing_same_code=True,
    replace_existing_if_code_changed=False,
    session_cache_dir="./.demo_service_sessions",
)
```

## 权限边界

1. owner 管理面依赖 `service_token`。
2. `ModuleLikeServiceGroup` 适合“我自己部署、我自己持有 token、我自己持续心跳”的场景。
3. 如果只是“发现已有服务并调用”，更适合用 InfoCenter 路由查询 + HTTP/gRPC 调用，而不是把自己当成 owner。

## 推荐验证

1. 运行 `python scripts/demo_module_like_client.py`
2. 查看 `SERVICE_SESSION_PROTOCOL_V1.md`
3. 查看 `GRPC_CONTRACT_V1.md`
