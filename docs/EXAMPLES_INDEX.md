# 示例脚本说明

当前保留的主示例方向：

1. `taskpool_basic.py`
   - `TaskPool` 批量任务执行示例
2. `jobqueue_basic.py`
   - `JobQueue` 排队与单活编排示例
3. `service_connect_gateway.py`
   - `Service.connect(..., transport="gateway")` 基本示例
4. `gateway_transport_client.py`
   - `GatewayServiceClient` 底层 transport 示例
5. `service_deploy_basic.py`
   - `Service.deploy(...)` owner 侧示例
6. `service_deploy_simple.py`
   - `Service.deploy(...)` 最小示例
7. `service_async_calls.py`
   - `Service` 单次异步调用示例
8. `service_arrow_types.py`
   - Arrow / DataFrame / ndarray 类型往返示例
9. `service_positional_args.py`
   - 服务位置参数和 kwargs 调用示例

当前约定：

1. 任务模式优先展示 `TaskPool`
2. 大任务排队优先展示 `JobQueue`
3. 服务模式优先展示 `Service`
4. Gateway 调用优先展示 `Service.connect(..., transport="gateway")`

已移除：

1. 共享任务池相关旧客户端入口
2. 共享任务池相关旧示例
3. 函数级 `max_workers` 本地并行 demo
4. Cloudpickle 诊断 demo
5. 旧 async transport demo
