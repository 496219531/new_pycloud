# PyCloud 统一类名

当前对外统一使用这 4 个控制面入口：

1. `DeployedService`
2. `TaskSubmitter`
3. `GatewayConnect`
4. `DirectConnect`

这些名字现在就是实际类定义本身，不再通过别名转发。

## 导入方式

```python
from pycloud_parallel import (
    DeployedService,
    TaskSubmitter,
    GatewayConnect,
    DirectConnect,
)
```

或按需从控制面子包导入：

```python
from pycloud_parallel.controlplane import DeployedService, GatewayConnect
```

## 各自职责

| 类 | 角色 | 适用场景 |
|----|------|----------|
| `DeployedService` | owner | 部署并持有服务生命周期 |
| `TaskSubmitter` | owner | 上传任务代码并提交任务 |
| `GatewayConnect` | caller | 通过 Gateway 按服务名访问 |
| `DirectConnect` | caller | 客户端先发现路由再直连 |

## 使用示例

### 1. DeployedService

```python
from pycloud_parallel import DeployedService

service = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="service.py",
)

result = await service.square(x=7)
service.close(end_services=True)
```

### 2. TaskSubmitter

```python
from pycloud_parallel import TaskSubmitter

task = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    entry_module="task",
)

results = task.run(value=7)
task.close()
```

### 3. GatewayConnect

```python
from pycloud_parallel import GatewayConnect

client = GatewayConnect(
    "127.0.0.1:50051",
    service_name="square-service",
)

result = client.square.sync(x=7)
```

### 4. DirectConnect

```python
from pycloud_parallel import DirectConnect

client = DirectConnect(
    "127.0.0.1:50051",
    service_name="square-service",
)

result = client.square.sync(x=7)
```

## 为什么这样收口

1. 名字更短，更接近实际职责。
2. `GatewayConnect` 和 `DirectConnect` 的差异一眼能看出来。
3. IDE 跳转会直接落到真实类定义，不再停在别名导出上。

## 相关文档

- [QUICK_START.md](QUICK_START.md)
- [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
- [TASK_MODULE_CLIENT.md](TASK_MODULE_CLIENT.md)
- [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
