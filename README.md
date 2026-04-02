# pycloud-parallel

`pycloud-parallel` 当前定位：

1. 主功能：多节点/跨集群任务与服务调度（ControlPlane + NodeControl）
2. 辅功能：本地多进程并行 API（轻量兜底）

## 安装

默认安装即包含集群控制面所需依赖：

```bash
pip install pycloud-parallel
```

## 当前架构

当前默认部署形态：

1. `ControlPlane = InfoCenter + Gateway`，对外 `HTTP + JSON`
2. `NodeControl`，对外 `gRPC`
3. 服务实例数据面，节点内 `HTTP + JSON`

## Payload 序列化边界

当前运行时数据流只额外支持很窄的一组 Python 类型：

1. `pandas.DataFrame`
2. `pandas.Series`
3. `numpy.ndarray`

其中：

1. `HTTP` 调服务时，底层仍然是 `JSON`
2. 框架会把上面 3 种类型自动转成简单 JSON 结构
3. 复杂对象不会自动兼容，直接报错
4. `gRPC` 任务/服务控制面会对这 3 种类型做显式包装与还原
5. `numpy.ndarray` 只接受简单 `dtype`：数值 / bool / 字符串

如果业务参数里有更复杂的 Python 对象，当前建议用户自己先转成普通 JSON 结构，或落到外部存储后只传引用。

职责划分：

1. `InfoCenter`
   - 节点注册与心跳
   - 节点与服务路由事实查询
   - 暴露节点 `python_version`
   - 轻量运维页面 `/ops`
2. `Gateway`
   - 对外服务调用入口
   - `service_name -> route` 缓存与失败切换
3. `NodeControl`
   - 代码上传
   - 任务模式执行
   - 服务模式生命周期管理

## 当前协议边界

### 服务模式

1. 对外推荐入口：`ControlPlane Gateway HTTP + JSON`
2. 节点管理面：`NodeControl gRPC`
3. 节点内服务执行：`POST /svc/{service_id}/call/{method}`

### 任务模式

1. 高频任务链路仍然走 `NodeControl gRPC`
2. `InfoCenter` 只提供节点事实，不代理任务
3. `Gateway` 不代理任务

## 顶层 Python API

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

语义：

1. `DeployedService`
   - owner 侧部署并持有服务
   - 是 `ServiceModuleGroup` 的推荐别名
2. `TaskSubmitter`
   - 任务模式的模块化客户端
   - 是 `TaskModuleClient` 的推荐别名
3. `GatewayConnect`
   - 通过 Gateway 按 `service_name` 调用服务
   - 是 `GatewayModuleClient` 的推荐别名
4. `DirectConnect`
   - 客户端本地查路由后直连实例
   - 是 `DiscoveryModuleClient` 的推荐别名

如果你需要更底层的控制面类，请从 `pycloud_parallel.controlplane` 导入。

## 快速开始

### 1. 启动控制面和节点

安装后的全局命令（推荐）：

```bash
pycloudctl start
pycloudctl status
```

macOS / Linux:

```bash
./scripts/start_services.sh start
./scripts/start_services.sh status
```

Windows `cmd`:

```bat
scripts\start_services.bat start
scripts\start_services.bat status
```

默认端口：

1. `ControlPlane`: `127.0.0.1:50051`
2. `NodeControl node-1`: `127.0.0.1:50061`
3. `NodeControl node-2`: `127.0.0.1:50062`
4. `node-1 service HTTP`: `127.0.0.1:18081`
5. `node-2 service HTTP`: `127.0.0.1:18082`

### 2. 服务模式

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
    filename="square_service.py",
    runtime="py3",
    entry_module="square_service",
    export_mode="decorator",
    node_count=1,
)

print(group.square.sync(x=7))
# group.join() 适合 owner 长驻
```

如果你的代码依赖节点上未预装的包，可以显式给白名单：

```python
group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    service_name="dep-service",
    artifact_path="./service_src",
    runtime="py3",
    entry_module="viewer",
    dependency_allowlist=[
        "./third_party/my_local_pkg",
        "orjson==3.10.18",
    ],
)
```

### 3. 任务模式

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
    filename="task_demo.py",
    runtime="py3",
    entry_module="task_demo",
) as task:
    results = task.run(value=7, runtime_key="demo-runtime")
    for item in results:
        print(item.task_id, item.status, item.result)
```

如果上传校验报 `ModuleNotFoundError`，默认会严格失败。
只有显式传了 `dependency_allowlist`，节点才会尝试在当前 `code_version` 的隔离目录里补装依赖。

### 4. Gateway 调用

```python
from pycloud_parallel import GatewayConnect

client = GatewayConnect("127.0.0.1:50051", service_name="square-service")
print(client.square.sync(x=9))
```

### 5. 本地并行（辅助能力）

```python
from pycloud_parallel import foreach, parallel_for

print(foreach(lambda x: x * x, [1, 2, 3], max_workers=2))
print(parallel_for(range(5), lambda i: i + 1, max_workers=2))
```

## 服务模式说明

服务模式已经收敛为“模块 + 多函数导出”：

1. 上传支持 `py / tar.gz / zip / whl`
2. 注册时传 `entry_module + export_spec`
3. 导出模式支持：`decorator / explicit / all / single`
4. 推荐默认：`decorator + pycloud_export`
5. 对外按 `service_name + method` 调用
6. owner 权限依赖 `service_token`

当前推荐调用路径：

1. owner：`DeployedService.deploy_from_infocenter(...)`
2. caller：`GatewayConnect(...)`
3. 调试直连：`DirectConnect(...)`

## 任务模式说明

任务模式当前是“流式入口 + runtime slot 调度”：

1. 客户端上传代码后，和目标节点建立 `TaskStream`
2. `TaskBatchClient` / `TaskSubmitter` 已经在内部使用任务流
3. 任务可显式传 `runtime_key`
4. 节点内部按 `runtime_key` 维护 runtime slot
5. slot 内复用单进程 worker，尽量少切代码
6. slot 空闲超过 `idle TTL` 后回收
7. 结果当前仍保存在节点内存中，不做持久化

热点与选点：

1. 节点向 `InfoCenter` 心跳上报 `active_runtimes`
2. `InfoCenter.select_task_nodes(...)` 支持 `preferred_runtime_key`
3. `TaskBatchClient.from_infocenter(...)` 默认会优先选择热 node

## Python Runtime 约束

`runtime` 当前表示 Python 版本约束，不是任意标签。

支持的写法：

1. `py3`
2. `py3.11`
3. `>=py3.11`
4. `<=py3.11`
5. `>py3.11`
6. `<py3.11`

当前行为：

1. `InfoCenter` 会暴露节点 `python_version`
2. 服务部署和任务选点会先按 `runtime` 过滤节点
3. 节点侧在上传代码和创建服务时会再做一次本地校验

建议：

1. 通用示例默认使用 `runtime="py3"`
2. 只有明确依赖某个次版本时，再使用精确版本或比较表达式

## 依赖补装策略

当前策略刻意保持保守：

1. 默认不自动安装任何缺失模块
2. 只有调用方显式传 `dependency_allowlist` 才允许节点补装
3. 节点不会猜测 `import 名 -> pip 包名`
4. 白名单会安装到 `code_cache/<sha>_deps`
5. 同一个 `code_version` 如果使用不同的 `dependency_allowlist`，会直接拒绝，避免缓存语义混乱

## 运维接口

当前 `ControlPlane` 端口同时提供：

1. `POST /nodes/register`
2. `POST /nodes/heartbeat`
3. `GET /nodes`
4. `GET /services/routes`
5. `GET /ops`
6. `POST /svc/{service_name}/call/{method}`
7. `GET /svc/{service_name}/methods`
8. `GET /svc/{service_name}/status`

节点运维：

1. `POST /ops/nodes/{node_id}/cordon`
2. `POST /ops/nodes/{node_id}/uncordon`
3. `POST /ops/nodes/{node_id}/drain`
4. `POST /ops/nodes/{node_id}/undrain`

## 推荐阅读顺序

1. [快速开始](docs/QUICK_START.md)
2. [架构总览](docs/ARCHITECTURE_OVERVIEW.md)
3. [任务模式](docs/TASK_MODE.md)
4. [ServiceModuleGroup](docs/SERVICE_MODULE_GROUP.md)
5. [Gateway 客户端指南](docs/GATEWAY_CLIENT_GUIDE.md)
6. [InfoCenter HTTP](docs/INFOCENTER_HTTP.md)
7. [Runtime 参数说明](docs/RUNTIME_PARAMETER_ANALYSIS.md)
