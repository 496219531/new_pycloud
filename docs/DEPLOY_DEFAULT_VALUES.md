# deploy_from_infocenter 默认值与当前语义

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
2. `filename` 是 `.py` 时，从文件名推断
3. `artifact_path` 是 `.py` 时，从路径推断
4. `artifact_paths[0]` 是 `.py` 时，从第一个路径推断
5. 否则不自动推断，服务名回退到 `service-...`

## 3. 当前最小部署示例

### 3.1 使用本地文件

```python
from pycloud_parallel.controlplane.client import ModuleLikeServiceGroup

group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="service.py",
)
```

### 3.2 使用 blob

```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="service.py",
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
5. 按 `service_worker_available` 排序。
6. 选出 `node_ids` / `node_count` / `min_success_nodes` 决定的节点数。

### 4.1 显式指定节点

```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="service.py",
    node_ids=["node-1", "node-3"],
)
```

### 4.2 指定节点数

```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="service.py",
    node_count=2,
)
```

## 5. 服务名语义

当前实现中：

1. 活跃 `service_name` 视为全局唯一。
2. 服务端不再按 `owner_client_id` 区分同名服务。
3. 如果多个客户端需要不同实例，应自行生成不同 `service_name`。

## 6. 复用与替换

### 6.1 默认复用

如果满足：

1. 同 `owner_client_id`
2. 同 `service_name`
3. 同 `code_version`
4. 本地 token 缓存还在

则客户端默认直接复用已有服务。

### 6.2 默认不覆盖

如果远端同名服务代码不同，默认会报错，不会自动替换。

只有显式传：

```python
replace_existing_if_code_changed=True
```

才会触发“先结束旧服务，再创建新服务”。

## 7. 一个更贴近当前实现的示例

```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="square_service.py",
    entry_module="square_service",
    worker_count=1,
    node_count=1,
    reuse_existing_same_code=True,
    replace_existing_if_code_changed=False,
)
```

这更适合当前“本地轻量、简单稳定”的默认思路。
