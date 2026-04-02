# 示例脚本更新总结

当前示例脚本统一使用顶层 API：

```python
from pycloud_parallel import (
    DeployedService,
    TaskSubmitter,
    GatewayConnect,
    DirectConnect,
)
```

## 覆盖的示例

1. `demo_gateway_client.py`
2. `demo_gateway_complete.py`
3. `demo_gateway_module_client.py`
4. `demo_service_module_group.py`
5. `demo_simple_deploy.py`
6. `demo_task_module_client.py`
7. `demo_top_level_import.py`

## 当前约定

1. owner 侧服务部署示例统一使用 `DeployedService`
2. 任务模式示例统一使用 `TaskSubmitter`
3. Gateway 调用示例统一使用 `GatewayConnect`
4. 直连发现示例统一使用 `DirectConnect`
5. 示例优先从 `pycloud_parallel` 顶层导入，不再绕到旧导出层

## 验证点

1. 示例里不再依赖兼容别名
2. 文档代码片段与示例脚本保持一致
3. IDE 跳转会直接落到真实类定义
