# 快速开始

## 0. 模式定位

建议先把三层角色区分开：

1. `Task Mode`
   - 子任务执行层
   - 更适合 CPU 密集型子任务、批处理、高吞吐执行
2. `JobQueue Mode`
   - 大任务排队与单活调度层
   - 大任务排到后，再展开成 subtasks 交给执行层
3. `Service Mode`
   - 常驻函数服务层
   - 更适合作为内部 RPC / 内部函数服务层
   - 当前不是标准 ASGI/WSGI 网络服务运行时
4. `External Web Layer`
   - 真正对外的轻网络入口层
   - 如果需要标准 Web 服务，建议独立使用 `FastAPI/Flask + uvicorn/gunicorn`

## 1. 启动服务

```bash
./scripts/start_services.sh start
./scripts/start_services.sh status
```

如果你想指定运行目录或端口，参数要写在子命令前面：

```bash
python -m pycloud_parallel.controlplane.ctl \
  --runtime-root /tmp/pycloud-dev \
  --controlplane-port 51051 \
  --node1-port 51061 \
  --node1-http-port 18181 \
  --node2-port 51062 \
  --node2-http-port 18182 \
  --node-worker-capacity 4 \
  start
```

如果已经安装了 CLI，也可以直接：

```bash
pycloudctl --runtime-root /tmp/pycloud-dev --controlplane-port 51051 start
```

默认会启动：

1. `controlplane`：`<auto-detected-local-ip>:50051`
2. `node-1`：`<auto-detected-local-ip>:50061`
3. `node-2`：`<auto-detected-local-ip>:50062`
4. `node-1 service HTTP`：`<auto-detected-local-ip>:18081`
5. `node-2 service HTTP`：`<auto-detected-local-ip>:18082`

默认情况下，`pycloudctl start` 会自动探测本机可达 IP 来填充 bind / advertise / service-http 地址，不再固定回退到 `127.0.0.1`。
如果你要单独起 `gateway` 或 `nodecontrol`，请显式传 `--infocenter-addr`：

```bash
pycloudctl start-gateway --infocenter-addr 127.0.0.1:50051
pycloudctl start-node --node-id node-1 --infocenter-addr 127.0.0.1:50051
```

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
    pycloud_export,
    DeployedService,
    DedicatedTaskServiceSession,
    JobQueueClient,
    TaskPoolSession,
    GatewayConnect,
    DirectConnect,
)
```

含义：

1. `DeployedService`
   - owner 侧部署内部函数服务
2. `TaskPoolSession`
   - 原生专属任务池会话
3. `DedicatedTaskServiceSession`
   - 复用 `ServiceGroup` 的兼容专属池实现
4. `JobQueueClient`
   - 大任务排队客户端
5. `GatewayConnect`
   - 通过 Gateway 调用内部函数服务
6. `DirectConnect`
   - 客户端发现后直连实例

如果你是模块 / package 部署，服务导出装饰器也可以直接从顶层包拿：

```python
from pycloud_parallel import pycloud_export
```

## 3. 本地多进程

```python
from pycloud_parallel import foreach, parallel_for

print(foreach(lambda x: x * x, [1, 2, 3], max_workers=2))
print(parallel_for(range(5), lambda i: i + 10, max_workers=2))
```

## 4. 服务模式

当前更建议把这里理解成“常驻函数服务层”，而不是直接对外的 Web 服务层。

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
# 固定 service_name 重新部署时，如果代码变化需先结束旧服务
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

当前任务层已经可以分成三种入口：

1. `TaskPoolSession`
   - 原生专属 pool，会自动 heartbeat
2. `DedicatedTaskServiceSession`
   - 兼容专属池实现，底层复用 `ServiceGroup`
3. `JobQueueClient`
   - 先提交大任务到队列，排到后再自动创建 `TaskPoolSession`

### 5.1 原生专属 pool

```python
from pycloud_parallel import TaskPoolSession

blob = (
    b"def run(value=0, **_kwargs):\n"
    b"    value = int(value)\n"
    b"    return {'value': value, 'square': value * value}\n"
)

with TaskPoolSession.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    job_id="demo-job",
    blob=blob,
    runtime="py3",
    entry_module="task_demo",
) as pool:
    resp = pool.submit_payloads([{"value": 7}])
    results = pool.wait_for_data(expected_count=len(resp.accepted), timeout_sec=10.0)
    print(results)

    task_id = pool.run(value=11)
    print(task_id)

    result = pool.run.sync(value=12)
    print(result)

    for task_id, data in pool.iter_data(max_count=1, timeout_sec=10.0):
        print(task_id, data)

    for task_id, data in pool.imap_unordered(
        [{"value": 20}, {"value": 21}, {"value": 22}],
        max_in_flight=2,
        receive_batch=1,
        result_timeout_sec=10.0,
    ):
        print(task_id, data)

    mapped = pool.map([8, 9, 10], timeout_sec=10.0)
    print(mapped)
```

说明：

1. `TaskPoolSession` 当前是单入口模式，入口名就是 `entry_callable`
2. `submit_payloads(..., task_method=...)` 只能传这个方法名
3. `runtime_key` 仍可用于 runtime 逻辑隔离，但不再表示独立 runtime-slot

如果你希望先排队，再由调度器自动创建专属 pool：

```python
from pycloud_parallel import JobQueueClient

client = JobQueueClient("127.0.0.1:50051")
client.submit_job_from_bytes(
    blob=driver_blob,
    driver_entry_module="job_driver_demo",
    runtime="py3",
    task_entry_module="task_demo",
    task_entry_callable="run",
    pool_worker_count=2,
    pool_node_count=2,
)
```

如果你已经有函数对象，也可以直接：

```python
client.submit_job_from_func(
    func=build_subtasks,
    task_func=run_subtask,
    pool_worker_count=2,
    pool_node_count=2,
)
```

如果你已有模块对象：

```python
client.submit_job_from_module(
    module=job_driver_module,
    task_module=task_module,
    task_entry_callable="run",
    pool_worker_count=2,
    pool_node_count=2,
)
```

等待 job 进入终态：

```python
final = client.wait_for_terminal(job_id, timeout_sec=30.0)
print(final["job"]["status"])
```

## 6. Gateway 调用

`Gateway` 当前服务的是内部函数服务 caller，不承担任务模式，也不等同于标准 Web 应用入口。

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
python examples/demo_task_pool_session.py
python examples/demo_job_queue.py
python examples/grpc_register_service_client_demo.py
python examples/demo_gateway_client.py --service-name square-service
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
4. 安装目录位于节点 `code_cache/codes/<sha>/deps`
5. 同一 `code_version` 不允许混用不同白名单
