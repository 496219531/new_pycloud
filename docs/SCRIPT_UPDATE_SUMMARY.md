# 示例脚本说明

当前保留的主示例方向：

1. `demo_task_pool_session.py`
   - 原生 `TaskPool` 演示
2. `demo_job_queue.py`
   - `JobQueue` 演示
3. `demo_gateway_client.py`
   - Gateway smoke test
4. `demo_gateway_module_client.py`
   - `Service.connect(..., transport="gateway")` 调用示例
5. `demo_service_module_group.py`
   - `Service` owner 侧示例
6. `demo_simple_deploy.py`
   - 简化部署示例

当前约定：

1. 任务模式优先展示 `TaskPool`
2. 大任务排队优先展示 `JobQueue`
3. 服务模式优先展示 `Service`
4. Gateway 调用优先展示 `Service.connect(..., transport="gateway")`

已移除：

1. 共享任务池相关旧客户端入口
2. 共享任务池相关旧示例
