# 快速开始

## 0. 模式定位

建议先把三层角色区分开：

1. `Task Mode`
   - 子任务执行层
   - 更适合 CPU 密集型子任务、批处理、高吞吐执行
2. `JobQueue Mode`
   - 大任务排队与单活调度层
   - `JobQueue` 默认先查 `InfoCenter` 找到唯一 `job-orchestrator` route，再直连它的 HTTP 数据面
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
  start

python -m pycloud_parallel.controlplane.ctl dev-start \
  --nodes 2 \
  --node-control-port 51061 \
  --node-service-http-port 18181 \
  --node-worker-capacity 4
```

如果已经安装了 CLI，也可以直接：

```bash
pycloudctl --runtime-root /tmp/pycloud-dev --controlplane-port 51051 start
```

`pycloudctl start` 默认会启动：

1. `controlplane`：`<auto-detected-local-ip>:50051`
2. `job-orchestrator`：`<auto-detected-local-ip>:50053`

如果需要本地执行节点，用 `pycloudctl dev-start --nodes 2`，默认还会启动：

1. `node-1 control HTTP`：`<auto-detected-local-ip>:50061`
2. `node-2 control HTTP`：`<auto-detected-local-ip>:50062`
3. `node-1 service HTTP`：`<auto-detected-local-ip>:18081`
4. `node-2 service HTTP`：`<auto-detected-local-ip>:18082`

默认情况下，`pycloudctl start` / `dev-start` 会自动探测本机可达 IP 来填充 bind / advertise / service-http 地址，不再固定回退到 `127.0.0.1`。
如果你要单独起 `gateway` 或 node control，请显式传 `--target`：

```bash
pycloudctl start-gateway --target 127.0.0.1:50051
pycloudctl start-node --node-id node-1 --target 127.0.0.1:50051
```

Web 运维页：

```text
http://127.0.0.1:50051/ops
```

如果你希望主进程直接在当前终端/窗口里打印详细报错，可以加：

```bash
pycloudctl start --debug
pycloudctl --local start --debug
```

说明：

1. `--debug` 会把主进程日志级别切到 `DEBUG`
2. 主进程 stdout/stderr 会直连当前控制台/窗口
3. `--local` 和 `--debug` 可以一起用

## 2. 顶层 API

V1 顶层公开面只保留：

```python
from pycloud_parallel import (
    Service,
    TaskPool,
    JobQueue,
    DataRef,
    export,
)
```

含义：

1. `Service`
   - 服务产品入口：`deploy(...)` 动态部署、`connect(...)` 连接已有服务、`startup(...)` 启动时挂载固定 module
2. `TaskPool`
   - 批量任务执行会话，对应专属任务池
3. `JobQueue`
   - 排队与单活编排入口
4. `DataRef`
   - 唯一公开的大对象引用类型
5. `export`
   - 模块 / package 部署时的导出装饰器

启动时固定服务建议走 `Service.startup(...)`：

```python
from pycloud_parallel import Service

node = Service.startup(
    service_name="calc",
    entry_module="my_package.calc_service",
    bind="0.0.0.0:18080",
)

node.join()
```

这条路径不会接受 `Service.deploy(...)` 的动态部署；需要动态部署时仍使用普通 `NodeControl` 节点。
如果脚本启动后立刻退出，本地 `18080` 端口也会随进程关闭，浏览器访问会看到连接被拒绝；长驻场景需要像上面一样调用 `node.join()` 或用自己的主循环保持进程运行。

不传 `target` 时，startup service 只在当前进程本地运行并暴露 service HTTP，不注册到 `InfoCenter`。这是 startup 专属的未注册模式，不等于通用 local 模式；`Service.deploy(...)`、`Service.connect(...)`、`TaskPool.open(...)` 仍然必须显式传入 `target`。未来本地 IPC 模式只通过 `target="local"` 触发。

未注册 startup 模式适合不想接受 `InfoCenter` 同名排他约束的场景，例如在不同端口上启动多个同名本地实例：

```python
Service.startup(service_name="calc", entry_module="my_package.calc_service", bind="127.0.0.1:18080")
Service.startup(service_name="calc", entry_module="my_package.calc_service", bind="127.0.0.1:18081")
```

这种模式不会被 `InfoCenter` / Gateway 自动发现，调用方要直接使用对应实例的本地 service HTTP 地址。传入普通 `InfoCenter` target 后才会注册到 `InfoCenter`，并参与 `service_name` 排他检查。

V1 不再提供本地单机并行入口；并行计算统一走 `TaskPool`、`Service` 或 `JobQueue`。

## 3. 并行执行

使用 `TaskPool` 做批量函数执行，使用 `Service` 做长驻服务，使用 `JobQueue` 做任务编排。

## 4. 服务模式

当前更建议把这里理解成“常驻服务会话层”，而不是直接对外的 Web 服务层。

```python
from pycloud_parallel import Service, export

import my_service_module

group = Service.deploy(
    target="127.0.0.1:50051",
    service_name="square-service",
    source=my_service_module,
    runtime="py3",
    node_count=1,
)

print(group.square.sync(x=7))
# owner 长驻时可调用 group.join()
# 固定 service_name 重新部署时，如果代码变化需先结束旧服务
```

如果你连接的是已部署好的服务，也可以直接做轻量批量 RPC：

```python
svc = Service.connect(
    target="127.0.0.1:50051",
    service_name="square-service",
    route="gateway",
)

results = svc.square.map([1, 2, 3], arg_name="x")
print(results)

for index, result in svc.square.unordered([{"x": 1}, {"x": 2}, {"x": 3}], max_in_flight=3):
    print(index, result)

# async 场景可选使用 amap(...) / aunordered(...)；
# 当前更推荐把它们理解成进阶能力，而不是 service 主调用路径
# results = await svc.square.amap([1, 2, 3], arg_name="x")
# async for index, result in svc.square.aunordered([{"x": 1}, {"x": 2}], max_in_flight=2):
#     ...

# 需要完整错误信息时，使用 iter_items / collect_items
# items = svc.square.collect_items([{"x": 1}, {"x": 2}], max_in_flight=2)
```

这是 `Service` 侧的轻量 RPC 批量调用辅助能力，不是 `TaskPool` 任务模型。

默认推荐直接传模块对象 `source=my_service_module`。
如果你需要更细的打包、依赖或导出控制，再使用高级 `Artifact(...)` 或显式依赖策略：

```python
from pycloud_parallel.artifact import ArtifactDeps

group = Service.deploy(
    target="127.0.0.1:50051",
    service_name="dep-service",
    source="./service_src",
    runtime="py3",
    deps=ArtifactDeps.allow_install(["./third_party/my_local_pkg"]),
)
```

## 5. 任务模式

当前任务层已经收敛为两种入口：

1. `TaskPool`
   - 批量任务执行会话，会自动 heartbeat
2. `JobQueue`
   - 先提交大任务到队列，排到后再自动创建 `TaskPool`

### 5.1 原生专属 pool

```python
from pycloud_parallel import TaskPool

import my_task_module

with TaskPool.open(
    target="127.0.0.1:50051",
    job_id="demo-job",
    source=my_task_module,
    runtime="py3",
) as pool:
    print(pool.status().alive)
    # 详细分节点状态再看 pool.status_map()
    resp = pool.submit_payloads([{"value": 7}])
    results = pool.wait_for_data(expected_count=len(resp.accepted), timeout_sec=10.0)
    print(results)

    task_id = pool.run(value=11)
    print(task_id)

    result = pool.run.sync(value=12)
    print(result)

    for task_id, data in pool.iter_data(max_count=1, timeout_sec=10.0):
        print(task_id, data)

    for index, data in pool.unordered(
        [{"value": 20}, {"value": 21}, {"value": 22}],
        max_in_flight=2,
    ):
        print(index, data)

    pool.consume_unordered(
        [{"value": 30}, {"value": 31}],
        handle=lambda index, data: print("handled", index, data),
        max_in_flight=2,
        receive_batch=1,
        result_timeout_sec=10.0,
    )

    mapped = pool.map([8, 9, 10], timeout_sec=10.0)
    print(mapped)
```

说明：

1. `TaskPool` 当前是单入口模式，入口名来自 artifact 的 `entry_callable`
2. `submit_payloads(..., task_method=...)` 只能传这个方法名
3. `runtime_key` 仍可用于 runtime 逻辑隔离，但不再表示独立 runtime-slot
4. `pool.unordered(...)` / `pool.aunordered(...)` 是统一批量接口，返回 `(index, result_or_none)`
5. 如果你需要 `receive_batch / wait_ms / raise_on_error` 这类低层流控参数，请显式使用 `pool.imap_unordered(...)`

如果你希望先排队，再由调度器自动创建专属 pool：

```python
from pycloud_parallel import JobQueue

import my_job_module

client = JobQueue.connect("127.0.0.1:50051", client_id="job-demo")
client.submit(
    source=my_job_module,
    runtime="py3",
    job_payload={"value": 10, "count": 6},
)
```

这里的目标地址应该指向 `InfoCenter` 或内嵌 `InfoCenter` 的 `controlplane`；`JobQueue` 会先发现 `job-orchestrator` route，再直连它自己的 HTTP 数据面。

`JobQueue` 的 job module 约定如下：

1. `run(payload...)`
   - 必选，子任务入口
2. `task_generator(...)`
   - 必选
   - 返回 `list[dict]` 或 payload 迭代器
3. `update_globals(...)`
   - 可选
   - 只负责在 job-orch 端生成共享数据 `dict`
4. `handle_result(index, result, state=..., ...)` / `handle_data(...)`
   - 可选，增量聚合中间结果
5. `finalize(state=..., ...)`
   - 可选，生成最终 `final_result`
6. `apply_managed_globals(values, **context)`
   - 可选
   - 在 worker 端运行
   - 负责把共享数据作用到入口模块 A 或它依赖的模块 B
   - 返回 `None` 时不做默认 raw assign
   - 返回 `dict` 时再把这个 dict 写回入口模块 A 的 globals

说明：

1. `job_payload` 是可选 `dict`
2. `submit(source=my_job_module, ...)` 会自动发现并绑定 `task_generator`
3. `handle_result` / `handle_data` / `finalize` / `update_globals` 都是可选，发现到才会写进 payload
4. `apply_managed_globals` 不走 payload，worker 固定按约定名在入口模块 A 中查找
5. 你也可以显式传 `update_globals=...`，支持 `dict`、callable 名称字符串，或 callable 对象
6. 如果 service/taskpool/job module 依赖 `.csv` 等非 Python 资源，默认不会自动打包；可以显式传 `resource_paths=[...]`
7. `JobQueue.submit(source=module, ...)` 如果 worker/taskpool 也需要这些资源，再额外传 `task_resource_paths=[...]`

这里默认推荐直接传模块对象：

```python
client.submit(
    source=job_module,
    job_payload={"value": 10, "count": 6},
)
```

这里推荐直接提交模块对象；`submit_job_from_func(...)` 已移除，避免把函数对象临时拼模块带来的隐式依赖问题。

等待 job 进入终态：

```python
final = client.wait_for_terminal(job_id, timeout_sec=30.0)
print(final["job"]["status"])
```

## 6. 本地并行

旧的本地 `foreach/parallel_for` 辅助入口已删除；V1 主路径是控制面驱动的集群执行。

## 7. 常用脚本

```bash
python examples/taskpool_basic.py
python examples/jobqueue_basic.py
python examples/service_deploy_register.py
python examples/service_connect_gateway.py --service-name square-service
python examples/gateway_route_client.py
python examples/service_deploy_basic.py
```

## 8. Runtime 约束速记

`runtime` 当前表示 Python 版本约束：

1. `py3`
   - 任意 Python 3 节点
2. `py3.11`
   - 只匹配 Python 3.11 节点
3. `>=py3.11`
   - 匹配 Python 3.11 及以上节点

普通示例优先写 `runtime="py3"`，更可移植。

## 9. 下一步

1. [TASK_MODE.md](TASK_MODE.md)
2. [SERVICE_GUIDE.md](SERVICE_GUIDE.md)
3. [INFOCENTER_HTTP.md](INFOCENTER_HTTP.md)
4. [RUNTIME_PARAMETER_ANALYSIS.md](RUNTIME_PARAMETER_ANALYSIS.md)

## 10. 依赖补装约定

1. 默认严格校验，缺依赖直接报错
2. 显式传 `deps=ArtifactDeps.allow_install(...)` 后，节点才会尝试补装
3. 支持本地路径、wheel 路径、普通 pip requirement 字符串
4. 安装目录位于节点 `code_cache/codes/<sha>/deps`
5. 同一 `code_version` 不允许混用不同白名单
