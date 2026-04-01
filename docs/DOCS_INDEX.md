# 文档索引

## 建议阅读顺序

1. [QUICK_START.md](QUICK_START.md)
2. [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
3. [TASK_MODE.md](TASK_MODE.md)
4. [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
5. [TASK_MODULE_CLIENT.md](TASK_MODULE_CLIENT.md)
6. [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
7. [INFOCENTER_HTTP.md](INFOCENTER_HTTP.md)

## 入口文档

- [QUICK_START.md](QUICK_START.md)
  - 顶层 API、启动方式、最短示例
- [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
  - 当前控制面、节点、任务模式、服务模式的边界

## 任务模式

- [TASK_MODE.md](TASK_MODE.md)
  - 任务流、`runtime_key`、热点路由、runtime slot
- [TASK_MODULE_CLIENT.md](TASK_MODULE_CLIENT.md)
  - `TaskSubmitter` / `TaskModuleClient` 的模块化调用体验
- [ID_SYSTEM_DESIGN.md](ID_SYSTEM_DESIGN.md)
  - `client_id / job_id / task_id / service_id` 设计说明

## 服务模式

- [SERVICE_MODULE_GROUP.md](SERVICE_MODULE_GROUP.md)
  - `DeployedService` / `ServiceModuleGroup` 的 owner 侧用法
- [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md)
  - `GatewayConnect` / `GatewayModuleClient` / `GatewayServiceClient`
- [CLASS_RENAMING.md](CLASS_RENAMING.md)
  - 新旧类名映射

## 控制面与运维

- [INFOCENTER_HTTP.md](INFOCENTER_HTTP.md)
  - `/nodes`、`/services/routes`、`/ops`、节点事实字段
- [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
  - `ControlPlane` 与 `NodeControl` 的职责划分

## 示例脚本

常用脚本：

- [../scripts/start_services.sh](../scripts/start_services.sh)
- [../scripts/grpc_task_client_demo.py](../scripts/grpc_task_client_demo.py)
- [../scripts/grpc_register_service_client_demo.py](../scripts/grpc_register_service_client_demo.py)
- [../scripts/demo_gateway_client.py](../scripts/demo_gateway_client.py)
- [../scripts/demo_gateway_module_client.py](../scripts/demo_gateway_module_client.py)
- [../scripts/demo_service_module_group.py](../scripts/demo_service_module_group.py)
- [../scripts/demo_simple_deploy.py](../scripts/demo_simple_deploy.py)
