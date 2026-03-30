# pycloud-parallel

`pycloud-parallel` 是一个 Python 3.8+ 并行执行框架，包含两层能力：

1. `@parallel_for / foreach` 的业务并行 API。
2. 基于 gRPC 的分布式控制面（InfoCenter + NodeControl）。

## 快速开始（并行 API）

```python
from pc import parallel_for

@parallel_for(mode="ordered", on_error="skip", retries=1, project="default")
def calc(nums):
    out = []
    for n in nums:
        out.append(n * n)
    return out

print(calc(list(range(10))))
```

### 本地运行时（当前语义）

`pycloud_parallel` 的并行 API 当前为**纯本地多进程**实现：

1. 执行链路：`Runtime -> ProcessPoolRunner -> executor`（无本地 gateway）。
2. `RuntimeConfig` 仅保留：
   - `max_workers`
   - `projects: Dict[str, ProjectConfig]`
   - `default_project`
3. `ProjectConfig` 仅保留：
   - `name`
   - `cpu_quota`
4. `project(...)` 仅需 `project(name, cpu_quota)`。

分布式/跨节点能力统一走 `controlplane + grpc`。

## 控制面（当前实现）

### 组件

1. `InfoCenter`：节点注册、心跳、服务路由查询。
2. `NodeControl`：代码上传、任务执行、服务会话生命周期管理。
3. `Service HTTP Gateway`：NodeControl 暴露 `/svc/{service_id}/call/{method}`。

### 启动

```bash
pycloud-control --role infocenter --bind 0.0.0.0:50051
```

```bash
pycloud-control --role nodecontrol --bind 0.0.0.0:50061 --node-id node-local-01 \
  --infocenter-addr 127.0.0.1:50051 \
  --advertise-addr 127.0.0.1:50061
```

### gRPC Stub 生成

```bash
bash scripts/gen_grpc_stubs.sh
```

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\gen_grpc_stubs.ps1
```

## 服务会话模型（V1）

当前默认是“模块 + 多函数路由”模式：

1. 上传支持 `py / tar.gz / zip / whl`。
2. 注册时指定：`entry_module + export_spec`。
3. 方法导出支持：`decorator / explicit / all / single`。
4. 调用按方法名：gRPC `CallService` 或 HTTP `POST /svc/{service_id}/call/{method}`。
5. 支持 `ListServiceMethods` 先查再调。
6. `service_name` 在活跃服务范围内应视为全局唯一；服务端不按 `owner_client_id` 再做同名区分。
7. `CreateService` 返回 `service_token`；后续 `HeartbeatService / EndService / CallService` 都可用它做鉴权。

默认推荐：`export_mode="decorator"` + `export_decorator="pycloud_export"`。

## Python 客户端能力（controlplane.client）

1. `NodeControlClient.create_service_from_file(...)`
   - 支持单文件或目录（目录会自动打包）。
2. `NodeControlClient.create_service_from_paths(...)`
   - 支持按相对路径列表打包上传。
3. `ServiceSessionClient.list_methods()`
4. `ServiceSessionClient.call(method, payload, via="http"|"grpc")`
5. `MultiNodeServiceGroup.call_balanced(...)`
6. `MultiNodeServiceGroup.acall_balanced(...)`
7. `MultiNodeServiceGroup.acall_all(...)`

### `MultiNodeServiceGroup.deploy_from_infocenter(...)` 当前策略

1. 默认会检查 InfoCenter 中是否已有同名活跃服务。
2. 如果是同 `owner_client_id + service_name + code_version`，客户端会直接复用已有服务，不重复上传。
3. 如果同名但代码版本不同，默认报错；显式传 `replace_existing_if_code_changed=True` 才会替换。
4. Python 客户端会把 `service_id/service_token` 落到本地缓存目录，供客户端重启后继续心跳和注销。

## 示例脚本

当前 `scripts/grpc*.py` 示例已改为“无参数解析”，运行即使用脚本内默认值：

```bash
python scripts/grpc_service_session_demo.py
python scripts/grpc_multi_node_service_demo.py
python scripts/grpc_existing_service_client_demo.py
python scripts/grpc_register_service_client_demo.py
python scripts/grpc_client_complex_demo.py
```

服务编排脚本：

```bash
./scripts/start_services.sh start
./scripts/start_services.sh status
```

`status` 现在会显示：

1. `infocenter/node-1/node-2` 进程状态
2. 每个节点当前加载的服务名列表（`Loaded Services By Node`）

## 协议文档

1. `proto/pycloud_v1.proto`
2. `GRPC_CONTRACT_V1.md`
3. `SERVICE_SESSION_PROTOCOL_V1.md`
4. `ARCHITECTURE_V1.md`
5. `API_CONTRACT_V1.md`（REST 草案，gRPC 为当前基线）
