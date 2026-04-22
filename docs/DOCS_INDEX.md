# 文档索引

建议先把当前三层角色区分开：

1. `Task Mode`
   - 子任务执行层
2. `JobQueue Mode`
   - 大任务排队与单活调度层
3. `Service Mode`
   - 常驻函数服务层
4. `External Web Layer`
   - 真正对外的轻网络入口层，建议独立使用 `FastAPI/Flask + uvicorn/gunicorn`

## 建议阅读顺序

1. [QUICK_START.md](QUICK_START.md)
2. [V1_ARCHITECTURE_TARGET.md](V1_ARCHITECTURE_TARGET.md)
3. [LONG_TERM_CONTEXT.md](LONG_TERM_CONTEXT.md)
4. [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
5. [TASK_MODE.md](TASK_MODE.md)
6. [SERVICE_GUIDE.md](SERVICE_GUIDE.md)
7. [TASK_CLIENT_GUIDE.md](TASK_CLIENT_GUIDE.md)
8. [SERVICE_GATEWAY_GUIDE.md](SERVICE_GATEWAY_GUIDE.md)
9. [INFOCENTER_HTTP.md](INFOCENTER_HTTP.md)
10. [RUNTIME_PARAMETER_ANALYSIS.md](RUNTIME_PARAMETER_ANALYSIS.md)

## 入口文档

- [QUICK_START.md](QUICK_START.md)
  - 顶层 API、`Service/TaskPool/JobQueue/DataRef/export`、启动方式、三层定位、最短示例
- [V1_ARCHITECTURE_TARGET.md](V1_ARCHITECTURE_TARGET.md)
  - V1 最终公开面、执行基础模型、数据模型与迁移目标
- [PYCLOUDCTL_USAGE.md](PYCLOUDCTL_USAGE.md)
  - `pycloudctl` 的完整命令说明、host 自动探测、显式 `--infocenter-addr`、日志、GC 与常见示例
- [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
  - 当前控制面、节点、任务模式、服务模式与外部 Web 层的边界
- [LONG_TERM_CONTEXT.md](LONG_TERM_CONTEXT.md)
  - 长期稳定决策基线（mode/policy、JobQueue/shared pool、变更规约、回归清单）

## 任务模式

- [TASK_MODE.md](TASK_MODE.md)
  - 子任务执行层、原生 task pool、单入口 `entry_callable`、`runtime_key` 的当前语义
- [QUICK_START.md](QUICK_START.md)
  - `TaskPool.open(...)` / `JobQueue.connect(...).submit(source=...)` 最小入口

## 服务模式

- [SERVICE_GUIDE.md](SERVICE_GUIDE.md)
  - `Service` 的 owner 侧用法与常驻函数服务定位
- [SERVICE_GATEWAY_GUIDE.md](SERVICE_GATEWAY_GUIDE.md)
  - `Service.connect(..., transport="gateway")` 的推荐用法，以及 `GatewayServiceClient` 的底层定位
- [V1_PUBLIC_API.md](V1_PUBLIC_API.md)
  - V1 最终公开面与不再推荐的旧概念

## 控制面与运维

- [PYCLOUDCTL_USAGE.md](PYCLOUDCTL_USAGE.md)
  - 本地控制脚本 `pycloudctl` 的详细用法、示例与排障
- [INFOCENTER_HTTP.md](INFOCENTER_HTTP.md)
  - `/nodes`、`/services/routes`、`/ops`、`node_instance_id` 与 timing 指标
- [HTTP_SERVICE_DEBUG_FLOW.md](HTTP_SERVICE_DEBUG_FLOW.md)
  - HTTP 服务调用从 caller 到节点执行再到返回的关键函数链路
- [TASKPOOL_DEBUG_FLOW.md](TASKPOOL_DEBUG_FLOW.md)
  - TaskPool 从创建、提交、执行到结果返回的关键函数链路
- [PAYLOAD_FLOW_DEBUGGING.md](PAYLOAD_FLOW_DEBUGGING.md)
  - 如何通过 `pycloud_parallel.payload_flow` 判断 payload 走的是哪条路径
- [RUNTIME_LIMITS.md](RUNTIME_LIMITS.md)
  - inline / DataRef / gRPC 消息大小等运行时阈值的统一调参入口
- [RUNTIME_PARAMETER_ANALYSIS.md](RUNTIME_PARAMETER_ANALYSIS.md)
  - `runtime` 作为 Python 版本约束的当前语义
- [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
  - `ControlPlane` 与 `NodeControl` 的职责划分
- [../SERVICE_SESSION_PROTOCOL_V1.md](../SERVICE_SESSION_PROTOCOL_V1.md)
  - 服务创建、导出、keepalive、`dependency_allowlist`
- [../API_CONTRACT_V1.md](../API_CONTRACT_V1.md)
  - HTTP/JSON 契约与管理面边界

## 示例脚本

常用脚本：

- [../scripts/start_services.sh](../scripts/start_services.sh)
- [../examples/taskpool_basic.py](../examples/taskpool_basic.py)
- [../examples/jobqueue_basic.py](../examples/jobqueue_basic.py)
- [../examples/service_connect_gateway.py](../examples/service_connect_gateway.py)
- [../examples/gateway_transport_client.py](../examples/gateway_transport_client.py)
- [../examples/service_deploy_basic.py](../examples/service_deploy_basic.py)
- [../examples/service_deploy_simple.py](../examples/service_deploy_simple.py)
