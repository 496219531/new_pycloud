# ServiceModuleGroup / DeployedService

`DeployedService` 是 `ServiceModuleGroup` 的推荐别名。

它面向 owner 侧，职责是：

1. 部署服务
2. 持有 `service_token`
3. 自动 keepalive
4. 需要时 `join()` 长驻
5. 正常退出时 `EndService`
6. 可选声明 `dependency_allowlist`

## 1. 基本用法

```python
from pycloud_parallel import DeployedService

blob = (
    b"def pycloud_export(fn):\n"
    b"    fn.__pycloud_export__ = True\n"
    b"    return fn\n\n"
    b"@pycloud_export\n"
    b"def square(x=0, **_kwargs):\n"
    b"    x = int(x)\n"
    b"    return {'x': x, 'y': x * x}\n"
)

group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="demo-owner",
    service_name="square-service",
    blob=blob,
    filename="square_service.py",
    runtime="py3",
    entry_module="square_service",
    export_mode="decorator",
    export_decorator="pycloud_export",
    worker_count=1,
    node_count=1,
)

print(group.square.sync(x=7))
```

如果服务代码依赖节点未预装的包：

```python
group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="demo-owner",
    service_name="viewer-service",
    artifact_path="./viewer_pkg",
    runtime="py3",
    entry_module="viewer",
    export_mode="decorator",
    dependency_allowlist=[
        "./third_party/my_local_pkg",
        "orjson==3.10.18",
    ],
    node_count=1,
)
```

## 2. 长驻与退出

`deploy_from_infocenter(...)` 成功后会自动开始 keepalive。

owner 长驻推荐：

```python
joined = False
try:
    group.join(end_services_on_interrupt=True, end_reason="owner ctrl+c")
    joined = True
finally:
    group.close(end_services=not joined)
```

要点：

1. keepalive 只在 owner 侧部署路径自动开启
2. `join()` 用于把 owner 进程挂住
3. `Ctrl+C` 是正常退出路径

## 3. 调用体验

### 3.1 异步调用

```python
result = await group.square(x=7)
```

### 3.2 同步调用

```python
result = group.square.sync(x=7)
```

### 3.3 广播调用

```python
results = await group.square.broadcast(x=7)
```

### 3.4 通用接口

```python
result = await group.call("square", x=7)
result = group.call_sync("square", x=7)
```

## 4. 当前导出模型

服务已经不是单入口函数模型，而是模块导出模型：

1. 指定 `entry_module`
2. 决定 `export_mode`
3. 调用时按 `method` 路由

推荐默认：

1. `export_mode="decorator"`
2. 使用 `pycloud_export`

服务方法当前默认按 kwargs 调用：

1. HTTP / Gateway / Python 模块客户端传的 JSON body 会展开成关键字参数
2. 推荐服务函数写成 `def square(x=0, **_kwargs): ...`
3. 如果你需要位置参数，使用 `{"args": [...], "kwargs": {...}}` 约定

### 4.1 参数序列化边界

服务模式当前只额外支持这 3 种 Python 类型：

1. `pandas.DataFrame`
2. `pandas.Series`
3. `numpy.ndarray`

并且分两层看：

1. `HTTP` 数据面本质仍然是 `JSON`
2. 框架会把这 3 种类型自动转成简单 JSON 结构再发送
3. node 侧调用用户函数前再还原回 `DataFrame / Series / ndarray`
4. 返回值里这 3 种类型也会自动转回简单结构
5. 其他复杂 Python 对象不支持，直接报错

额外限制：

1. `numpy.ndarray` 只支持简单 `dtype`
2. 更复杂的对象数组、业务自定义类实例等，不做自动兼容
3. 报错会尽量带字段路径，方便定位是哪一段 payload 不被支持

## 5. 常用部署参数

```python
group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="demo-owner",
    service_name="square-service",
    artifact_path="./service_dir",
    runtime="py3",
    entry_module="viewer",
    export_mode="decorator",
    worker_count=2,
    node_count=2,
    reuse_existing_same_code=True,
    replace_existing_if_code_changed=False,
)
```

语义：

1. 同 `owner_client_id + service_name + code_version` 时可复用
2. 同名但代码变化时默认不覆盖
3. 显式 `replace_existing_if_code_changed=True` 才会替换
4. 客户端会本地缓存 `service_id/service_token`，便于重启后复用

## 5.1 依赖补装语义

当前策略：

1. 默认严格校验，缺依赖直接失败
2. 只有显式传 `dependency_allowlist` 才允许节点补装
3. 节点把依赖安装到当前 `code_version` 的隔离目录
4. 运行时调用服务方法时，也会把该依赖目录加入 `sys.path`
5. 同一个 `code_version` 不允许混用不同 `dependency_allowlist`

## 6. 节点选择

如果不显式传 `node_ids`，部署时会：

1. 从 `InfoCenter` 查询节点
2. 过滤 `healthy=false`
3. 过滤 `schedulable=false`
4. 过滤 `drain=true`
5. 如果指定了 `runtime`，先按节点 `python_version` 过滤
6. 按 `service_worker_available` 选节点

## 7. 与 GatewayConnect 的区别

`DeployedService`：

1. 是 owner
2. 会上传代码
3. 会创建服务
4. 会 keepalive
5. 可以 `end()` 服务

`GatewayConnect`：

1. 只是 caller
2. 不上传代码
3. 不持有 token
4. 不管理服务生命周期

## 8. 何时用 DirectConnect

如果你只是想调已有服务，一般优先：

1. `GatewayConnect`

只有在这些场景才更适合 `DirectConnect`：

1. 调试具体实例
2. 旁路 Gateway
3. 客户端本地自己维护 route cache
