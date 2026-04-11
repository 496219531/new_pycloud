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
2. [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
3. [TASK_MODE.md](TASK_MODE.md)
4. [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
5. [TASK_MODULE_CLIENT.md](TASK_MODULE_CLIENT.md)
6. [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
7. [INFOCENTER_HTTP.md](INFOCENTER_HTTP.md)
8. [RUNTIME_PARAMETER_ANALYSIS.md](RUNTIME_PARAMETER_ANALYSIS.md)

## 入口文档

- [QUICK_START.md](QUICK_START.md)
  - 顶层 API、`pycloud_export`、启动方式、三层定位、最短示例
- [PYCLOUDCTL_USAGE.md](PYCLOUDCTL_USAGE.md)
  - `pycloudctl` 的完整命令说明、host 自动探测、显式 `--infocenter-addr`、日志、GC 与常见示例
- [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
  - 当前控制面、节点、任务模式、服务模式与外部 Web 层的边界

## 任务模式

- [TASK_MODE.md](TASK_MODE.md)
  - 子任务执行层、原生 task pool、单入口 `entry_callable`、`runtime_key` 的当前语义
- [QUICK_START.md](QUICK_START.md)
  - `TaskPoolSession` / `DedicatedTaskServiceSession` / `JobQueueClient` 最小入口

## 服务模式

- [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
  - `DeployedService` 的 owner 侧用法与常驻函数服务定位
- [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
  - `GatewayConnect` / `GatewayServiceClient`，面向内部函数服务 caller
- [CLASS_RENAMING.md](CLASS_RENAMING.md)
  - 当前统一命名说明

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
  - inline / ObjectRef / gRPC 消息大小等运行时阈值的统一调参入口
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
- [../examples/demo_task_pool_session.py](../examples/demo_task_pool_session.py)
- [../examples/demo_job_queue.py](../examples/demo_job_queue.py)
- [../examples/demo_gateway_client.py](../examples/demo_gateway_client.py)
- [../examples/demo_gateway_module_client.py](../examples/demo_gateway_module_client.py)
- [../examples/demo_service_module_group.py](../examples/demo_service_module_group.py)
- [../examples/demo_simple_deploy.py](../examples/demo_simple_deploy.py)
