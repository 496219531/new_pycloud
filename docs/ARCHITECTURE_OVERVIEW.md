# 架构总览

## 1. 当前边界

当前实现收敛为三条链路：

1. `ControlPlane(InfoCenter + Gateway) = HTTP + JSON`
2. `NodeControl 管理面与任务面 = gRPC`
3. `NodeControl 服务实例数据面 = HTTP + JSON`

默认部署建议：

1. 一个 `controlplane` 进程
2. 多个 `nodecontrol` 进程

## 2. 角色

### 2.1 owner client

负责：

1. 上传代码并创建服务
2. 持有 `service_token`
3. 长驻并维持心跳
4. 正常退出时 `EndService`

推荐入口：`DeployedService.deploy_from_infocenter(...)`

### 2.2 caller client

负责：

1. 按 `service_name` 调用已有服务
2. 不拥有服务生命周期
3. 不持有 owner token

推荐入口：

1. `GatewayConnect`
2. `GatewayServiceClient`
3. `DirectConnect`（调试或旁路直连）

### 2.3 task client

负责：

1. 上传任务代码
2. 从 `InfoCenter` 取节点事实
3. 向目标 `NodeControl` 建立任务流
4. 提交任务并接收结果

推荐入口：

1. `TaskSubmitter`
2. `TaskBatchClient`
3. 低层：`NodeControlClient.open_task_stream(...)`

## 3. 服务模式

服务模式当前是“模块 + 多函数导出”模型：

1. 上传支持 `py / tar.gz / zip / whl`
2. 注册时指定 `entry_module + export_spec`
3. 导出模式：`decorator / explicit / all / single`
4. 默认推荐 `decorator + pycloud_export`
5. 调用路由为 `service_name -> route -> service_id -> method`
6. 依赖缺失时默认严格失败，只有显式 `dependency_allowlist` 才允许节点补装

对外推荐入口：

1. `POST /svc/{service_name}/call/{method}`
2. `GET /svc/{service_name}/methods`
3. `GET /svc/{service_name}/status`

## 4. 任务模式

### 4.1 通信模型

任务模式当前已经从“批量提交 + 轮询”收敛为“流式入口 + 高层 helper”：

1. gRPC 协议包含 `TaskStream`
2. `TaskBatchClient` / `TaskSubmitter` 内部已经走任务流
3. 低层 `SubmitTasks / PullResults / CancelJob` 仍保留

### 4.2 节点内执行模型

节点内部不是简单共享池，而是 runtime slot 模型：

1. 任务可带 `runtime_key`
2. `runtime_key` 绑定到节点内 runtime slot
3. 每个 slot 复用单进程 worker
4. 同一 slot 内尽量少切代码
5. 节点只保留前 `K` 个活跃 slot
6. 空闲 slot 超过 `idle TTL` 自动回收

### 4.3 热点路由

任务选点目标不是单纯均衡 credit，而是：

1. 尽量少切代码
2. 尽量让热代码持续热
3. 同时避免把某个 node 撑爆

因此：

1. 节点会向 `InfoCenter` 心跳上报 `active_runtimes`
2. `InfoCenter.select_task_nodes(...)` 支持 `preferred_runtime_key`
3. `TaskBatchClient.from_infocenter(...)` 会优先选择热点 node

### 4.4 结果与生命周期

1. 结果当前保存在节点内存中
2. 不做持久化
3. `job_id` 是分组键，不是 heartbeat session
4. 任务模式没有 Gateway
5. 任务 client 不需要服务模式那种长驻 keepalive

### 4.5 `runtime` 约束

`runtime` 当前统一表示 Python 版本约束：

1. `py3`
2. `py3.11`
3. `>=py3.11`
4. `<=py3.11`

当前链路：

1. 节点向 `InfoCenter` 暴露 `python_version`
2. 服务部署和任务选点会先按 `runtime` 过滤节点
3. 节点侧上传代码 / 建服务时再做一次本地校验

注意：

1. 精确 `py3.11` 只匹配 Python 3.11
2. 如果你想表达“3.11 及以上”，要显式写 `>=py3.11`

### 4.6 依赖补装

当前实现保持保守：

1. 不做盲目自动安装
2. 只有调用方显式提供 `dependency_allowlist` 才允许节点补装
3. 依赖安装目录跟随 `code_version`
4. 这样能保持缓存语义简单、排障路径清晰

## 5. ControlPlane

`ControlPlane` 默认是 `InfoCenter + Gateway` 同进程：

1. `InfoCenter` 维护节点与服务事实
2. `Gateway` 维护 route cache
3. 同进程时不需要本机网络回环
4. 也支持单独起 `infocenter` 和 `gateway`

## 6. 设计取向

当前优先级是：

1. 简单
2. 稳定
3. 可预测
4. 易排障

因此当前刻意不做：

1. `InfoCenter` 内建复杂调度器
2. `InfoCenter` 代理任务执行
3. 任务结果持久化系统
4. 复杂鉴权中心
