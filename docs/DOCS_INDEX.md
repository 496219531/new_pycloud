# 文档索引

建议先把当前三层角色区分开：

1. `Task Mode`
   - 重计算执行层
2. `Service Mode`
   - 常驻函数服务层
3. `External Web Layer`
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
  - 顶层 API、启动方式、三层定位、最短示例
- [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
  - 当前控制面、节点、任务模式、服务模式与外部 Web 层的边界

## 任务模式

- [TASK_MODE.md](TASK_MODE.md)
  - 重计算执行层、任务流、`runtime_key`、热点路由、runtime slot
- [TASK_MODULE_CLIENT.md](TASK_MODULE_CLIENT.md)
  - `TaskSubmitter` 的模块化调用体验
- [ID_SYSTEM_DESIGN.md](ID_SYSTEM_DESIGN.md)
  - `client_id / job_id / task_id / service_id` 设计说明

## 服务模式

- [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
  - `DeployedService` 的 owner 侧用法与常驻函数服务定位
- [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
  - `GatewayConnect` / `GatewayServiceClient`，面向内部函数服务 caller
- [CLASS_RENAMING.md](CLASS_RENAMING.md)
  - 当前统一命名说明

## 控制面与运维

- [INFOCENTER_HTTP.md](INFOCENTER_HTTP.md)
  - `/nodes`、`/services/routes`、`/ops`、节点事实字段
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
- [../examples/grpc_task_client_demo.py](../examples/grpc_task_client_demo.py)
- [../examples/grpc_register_service_client_demo.py](../examples/grpc_register_service_client_demo.py)
- [../examples/demo_gateway_client.py](../examples/demo_gateway_client.py)
- [../examples/demo_gateway_module_client.py](../examples/demo_gateway_module_client.py)
- [../examples/demo_service_module_group.py](../examples/demo_service_module_group.py)
- [../examples/demo_simple_deploy.py](../examples/demo_simple_deploy.py)
