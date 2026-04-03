# PyCloud 模块化客户端说明

模块化调用当前统一收口为 4 个入口：

1. `DeployedService`
2. `TaskSubmitter`
3. `GatewayConnect`
4. `DirectConnect`

## 为什么需要它们

相比底层 `TaskBatchClient`、`GatewayServiceClient`、`DiscoveryServiceClient`，这些模块化入口更接近直接写 Python 调用：

```python
result = client.square.sync(x=7)
results = task.run(value=7)
```

而不是手工拼：

```python
client.call(service_name="demo", method="square", payload={"x": 7})
batch.submit_payloads([{"value": 7}])
```

## 四类入口的分工

| 类 | 模式 | 用途 | 生命周期 |
|----|------|------|----------|
| `DeployedService` | Service | 部署并拥有服务 | owner 管理 |
| `TaskSubmitter` | Task | 提交任务并收结果 | owner 管理 |
| `GatewayConnect` | Gateway | 按服务名调用 | caller 使用 |
| `DirectConnect` | Discovery | 先发现再直连 | caller 使用 |

## 示例

### TaskSubmitter

```python
from pycloud_parallel import TaskSubmitter

task = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    entry_module="task",
)

results = task.run(value=7)
```

### DeployedService

```python
from pycloud_parallel import DeployedService

service = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="service.py",
)

result = await service.square(x=7)
```

### GatewayConnect

```python
from pycloud_parallel import GatewayConnect

client = GatewayConnect("127.0.0.1:50051", service_name="square-service")
print(client.square.sync(x=7))
```

## 顶层导入

当前推荐直接从顶层包导入：

```python
from pycloud_parallel import (
    DeployedService,
    TaskSubmitter,
    GatewayConnect,
    DirectConnect,
)
```

## 相关资料

- [TASK_MODULE_CLIENT.md](TASK_MODULE_CLIENT.md)
- [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
- [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
- [QUICK_START.md](QUICK_START.md)
