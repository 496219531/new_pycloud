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
    b"def square(x=0, **_kwargs):\n"
    b"    x = int(x)\n"
    b"    return {'x': x, 'y': x * x}\n"
)

group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    service_name="square-service",
    blob=blob,
    runtime="py3",
    entry_module="square_service",
    export_mode="decorator",
    node_count=1,
)

print(group.square.sync(x=7))
# owner 长驻时可调用 group.join()
```

依赖缺失时可显式给补装白名单：

```python
group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    service_name="dep-service",
    artifact_path="./service_src",
    runtime="py3",
    entry_module="viewer",
    dependency_allowlist=["./third_party/my_local_pkg"],
)
```

## 5. 任务模式

### 5.1 低样板方式

```python
from pycloud_parallel import TaskSubmitter

blob = (
    b"def run(value=0, **_kwargs):\n"
    b"    value = int(value)\n"
    b"    return {'value': value, 'square': value * value}\n"
)

with TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    runtime="py3",
    entry_module="task_demo",
) as task:
    results = task.run(value=7, runtime_key="demo-runtime")
    print(results)
```

如果任务代码 import 了节点上没有的包：

```python
with TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="./task_src",
    runtime="py3",
    entry_module="task_src.main",
    dependency_allowlist=["./third_party/my_local_pkg"],
) as task:
    print(task.run(value=7))
```

### 5.2 批量方式

```python
from pycloud_parallel.controlplane.client import TaskBatchClient

with TaskBatchClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    runtime="py3",
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
python examples/grpc_task_client_demo.py
python examples/grpc_register_service_client_demo.py
python examples/demo_gateway_client.py
python examples/demo_gateway_module_client.py
python examples/demo_service_module_group.py
```

## 9. Runtime 约束速记

`runtime` 当前表示 Python 版本约束：

1. `py3`
   - 任意 Python 3 节点
2. `py3.11`
   - 只匹配 Python 3.11 节点
3. `>=py3.11`
   - 匹配 Python 3.11 及以上节点

普通示例优先写 `runtime="py3"`，更可移植。

## 10. 下一步

1. [TASK_MODE.md](TASK_MODE.md)
2. [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
3. [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
4. [INFOCENTER_HTTP.md](INFOCENTER_HTTP.md)
5. [RUNTIME_PARAMETER_ANALYSIS.md](RUNTIME_PARAMETER_ANALYSIS.md)

## 11. 依赖补装约定

1. 默认严格校验，缺依赖直接报错
2. 显式传 `dependency_allowlist` 后，节点才会尝试补装
3. 支持本地路径、wheel 路径、普通 pip requirement 字符串
4. 安装目录位于节点 `code_cache/<sha>_deps`
5. 同一 `code_version` 不允许混用不同白名单
