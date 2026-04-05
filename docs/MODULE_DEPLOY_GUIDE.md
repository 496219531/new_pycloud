# 模块对象部署说明

当前模块对象自动打包仍然保留，但推荐入口已经变化：

## 1. 服务侧

推荐：

```python
import my_job.main
from pycloud_parallel import DeployedService

group = DeployedService.deploy_from_module(
    infocenter_target="127.0.0.1:50051",
    module=my_job.main,
    runtime="py3",
)
```

适合：

1. 内部常驻函数服务
2. 模块 / package 级别部署
3. 需要一起带上模块资源文件

## 2. 任务侧

当前不再推荐旧的共享任务池入口。

推荐改成：

1. `TaskPoolSession.from_infocenter(...)`
   - 直接创建原生专属 pool 执行 subtasks
2. `JobQueueClient.submit_job_from_bytes(...)`
   - 大任务先排队，排到后再自动创建 `TaskPoolSession`

## 3. 资源文件边界

如果你有共享静态数据文件：

1. 把数据文件放进模块 / package 目录树内部
2. 使用 `Path(__file__).resolve().parent / ...` 的相对路径读取
3. `module` 自动打包会把模块 / package 树里的资源文件一起带上

## 4. 已移除

以下旧入口已移除：

1. 共享任务池相关旧客户端入口
2. 共享任务池相关任务提交流程
