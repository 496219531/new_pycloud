# 快速开始

## 1. 启动服务

```bash
./scripts/start_services.sh start
./scripts/start_services.sh status
```

默认会启动：

1. `controlplane`：`127.0.0.1:50051`
2. `node-1`：`127.0.0.1:50061`
3. `node-2`：`127.0.0.1:50062`

Web 运维页：

```text
http://127.0.0.1:50051/ops
```

## 2. 顶层 API

推荐从顶层包导入：

```python
from pycloud_parallel import (
    configure,
    foreach,
    parallel_for,
    DeployedService,
    TaskSubmitter,
    GatewayConnect,
    DirectConnect,
)
```

含义：

1. `DeployedService`
   - owner 侧部署服务
2. `TaskSubmitter`
   - 任务模式模块化客户端
3. `GatewayConnect`
   - 通过 Gateway 调用服务
4. `DirectConnect`
   - 客户端发现后直连实例

旧类名仍可从 `pycloud_parallel.controlplane` 导入，但顶层包推荐只用新名字。

## 3. 本地多进程

```python
from pycloud_parallel import foreach, parallel_for

print(foreach(lambda x: x * x, [1, 2, 3], max_workers=2))
print(parallel_for(range(5), lambda i: i + 10, max_workers=2))
```

## 4. 服务模式

```python
from pycloud_parallel import DeployedService

blob = (
    b"def pycloud_export(fn):\n"
    b"    fn.__pycloud_export__ = True\n"
    b"    return fn\n\n"
    b"@pycloud_export\n"
    b"def square(payload):\n"
    b"    x = int(payload.get('x', 0))\n"
    b"    return {'x': x, 'y': x * x}\n"
)

group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    service_name="square-service",
    blob=blob,
    filename="square_service.py",
    entry_module="square_service",
    export_mode="decorator",
    node_count=1,
)

print(group.square.sync(x=7))
# owner 长驻时可调用 group.join()
```

## 5. 任务模式

### 5.1 低样板方式

```python
from pycloud_parallel import TaskSubmitter

blob = (
    b"def run(payload):\n"
    b"    value = int(payload.get('value', 0))\n"
    b"    return {'value': value, 'square': value * value}\n"
)

with TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task_demo.py",
    entry_module="task_demo",
) as task:
    results = task.run(value=7, runtime_key="demo-runtime")
    print(results)
```

### 5.2 批量方式

```python
from pycloud_parallel.controlplane.client import TaskBatchClient

with TaskBatchClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task_demo.py",
    entry_module="task_demo",
    preferred_runtime_key="demo-runtime",
) as batch:
    batch.submit_payloads(
        [{"value": 2}, {"value": 3}],
        runtime_key="demo-runtime",
    )
    results = batch.wait_for_results(expected_count=2, timeout_sec=10.0)
    print(results)
```

## 6. Gateway 调用

```python
from pycloud_parallel import GatewayConnect

client = GatewayConnect("127.0.0.1:50051", service_name="square-service")

print(client.square.sync(x=9))
# 或
# result = await client.square(x=9)
```

## 7. Direct 直连

```python
from pycloud_parallel import DirectConnect

client = DirectConnect("127.0.0.1:50051", service_name="square-service")
print(client.square.sync(x=11))
```

## 8. 常用脚本

```bash
python scripts/grpc_task_client_demo.py
python scripts/grpc_register_service_client_demo.py
python scripts/demo_gateway_client.py
python scripts/demo_gateway_module_client.py
python scripts/demo_service_module_group.py
```

## 9. 下一步

1. [TASK_MODE.md](TASK_MODE.md)
2. [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
3. [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
4. [INFOCENTER_HTTP.md](INFOCENTER_HTTP.md)
