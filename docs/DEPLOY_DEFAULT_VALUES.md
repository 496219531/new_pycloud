# Service.deploy 默认值与当前语义

## 1. 自动默认值

### 1.1 `owner_client_id`

如果不传，会自动生成：

```text
client-{本机IP}
```

例如：

```text
client-192.168.10.8
```

### 1.2 `service_name`

如果不传，会自动生成：

```text
{entry_module或推断模块名}-{本机IP}-{时间戳}
```

例如：

```text
square_service-192.168.10.8-20260331082337
```

如果无法推断模块名，则回退到：

```text
service-{本机IP}-{时间戳}
```

## 2. `entry_module` 推断顺序

当前顺序：

1. 显式传入的 `entry_module`
2. `func` / `module` 场景下，从对象自动推断
3. `artifact_path` 是 `.py` 时，从路径推断
4. `artifact_path` 是路径列表且首个元素是 `.py` 时，从第一个路径推断
5. `blob` 直传且未指定 `entry_module` 时，不再依赖外部 `filename`，服务名回退到 `service-...`

## 3. 当前最小部署示例

### 3.1 使用本地文件

```python
from pycloud_parallel.execution.service_session import Service

group = Service.deploy(
    target="127.0.0.1:50051",
    source="service.py",
)
```

### 3.2 使用 blob

```python
group = Service.deploy(
    target="127.0.0.1:50051",
    source=blob,
    package_format="py",
)
```

## 4. 当前节点选择默认语义

这是这版最重要的变化之一：

1. 默认不会把服务部署到所有发现到的节点。
2. 客户端只会选择“需要的节点数”。

当前选择逻辑：

1. 从 InfoCenter 拉节点。
2. 过滤 `healthy=false`。
3. 过滤 `schedulable=false`。
4. 过滤 `drain=true`。
5. 过滤 `accept_service_deploy=false`。
6. 按 `service_worker_available` 排序。
7. 选出 `node_ids` / `node_count` / `min_success_nodes` 决定的节点数。

动态补偿：

1. 如果初次部署目标是 3 个 node，但当前只成功部署 2 个，owner 会先返回可用的 2 个。
2. keepalive 后台会继续按目标副本数尝试补齐。
3. 失败副本按 `node_instance_id` 记录并跳过，不按可重复的 `node_id` 永久拉黑。
4. 如果同一个 `node_id` 重启后获得新的 `node_instance_id`，它会重新进入补偿候选。
5. 创建失败或 host 失败的原因会在 InfoCenter `/ops` 的 `failure_reason` 中显示。

调度状态边界：

1. `unhealthy` 表示实例执行状态不可信，应按 `node_instance_id` fence 并重置执行状态。
2. `drain` 不接新业务调用和新 task，但仍应接收 owner 控制命令。
3. `cordon` 不接新部署；已有服务是否继续路由由 `drain` 决定。
4. 排他性部署和版本冲突检查不能因为 drain/cordon 就隐藏已有服务。

### 4.1 显式指定节点

```python
group = Service.deploy(
    target="127.0.0.1:50051",
    source=blob,
    package_format="py",
    node_ids=["node-1", "node-3"],
)
```

### 4.2 指定节点数

```python
group = Service.deploy(
    target="127.0.0.1:50051",
    source=blob,
    package_format="py",
    node_count=2,
)
```

## 5. 服务名语义

当前实现中：

1. 活跃 `service_name` 视为全局唯一。
2. 服务端不再按 `owner_client_id` 区分同名服务。
3. 如果多个客户端需要不同实例，应自行生成不同 `service_name`。
4. 发现服务时先按 `service_name` 查 route，`service_id` 主要用于实例管理。

## 6. 复用与替换

### 6.1 默认复用

如果满足：

1. 同 `owner_client_id`
2. 同 `service_name`
3. 同 `code_version`
4. 本地 token 缓存还在

则客户端默认直接复用已有服务。

补充：

1. 同一台机器上，同一个 `owner_client_id + service_name`
2. 本地 session cache 文件会被 deployservice 持有独占锁
3. 第二个本地 deploy 进程会直接被拒绝，而不是晚一点在复用/部署阶段失败

### 6.2 运行中的同名服务不会被覆盖

如果远端同名服务仍在运行，且代码不同，客户端会直接拒绝这次部署。

要更新同名服务，应该先结束旧服务，再重新部署。

## 7. 一个更贴近当前实现的示例

```python
group = Service.deploy(
    target="127.0.0.1:50051",
    source=blob,
    package_format="py",
    worker_count=1,
    node_count=1,
    reuse_existing_same_code=True,
    replace_existing_if_code_changed=True,
)
```

这更符合“固定 `service_name` 代表同一个运行中服务”的部署语义。

## 8. owner 推荐长驻方式

部署成功后，owner 侧推荐直接进入：

```python
joined = False
try:
    group.join(
        end_services_on_interrupt=True,
        end_reason="owner ctrl+c",
    )
    joined = True
finally:
    group.close(end_services=not joined)
```

也就是：

1. `deploy(...)` 成功后就会自动启动 keepalive
2. 不再推荐手写 `start_keepalive() + while True`
3. `join()` 只负责长驻等待
4. `Ctrl+C` 作为正常退出路径
5. `close(...)` 负责异常时兜底清理

## 9. 部署后怎么调

部署完成后，对外推荐调用方式是：

1. 直接连 `controlplane`
2. 按 `service_name` 调 Gateway

例如：

```bash
curl -X POST 'http://127.0.0.1:50051/svc/square-service/call/square' \
  -H 'Content-Type: application/json' \
  -d '{"x": 7}'
```

而不是优先自己查 route 再直接拼某个节点上的 `service_id` URL。
