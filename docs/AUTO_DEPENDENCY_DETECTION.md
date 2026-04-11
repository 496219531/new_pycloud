# 自动依赖打包说明

当前系统仍然支持：

1. 函数对象自动打包
2. 模块对象自动打包
3. 本地模块 / package 树与资源文件一起打包

但任务侧入口已经更新：

## 当前推荐入口

1. `DeployedService.deploy_from_infocenter(func=...)`
2. `DeployedService.deploy_from_infocenter(entry_module=<module object>)`
3. `TaskPoolSession.from_infocenter(...)`
4. `JobQueueClient.submit_job_from_bytes(...)`

## 依赖策略

当前仍保持保守规则：

1. 自动打包本地源码依赖
2. 第三方依赖如果目标节点缺失，仍建议显式传 `dependency_allowlist`
3. 不做盲目自动安装

## 已移除

以下旧入口已移除：

1. 共享任务池相关旧客户端入口

如果你需要任务执行：

1. 直接执行一批 subtasks：使用 `TaskPoolSession`
2. 先排队再执行：使用 `JobQueueClient`
