# 架构总览

## 1. 当前边界

当前实现收敛为三条明确链路：

1. `ControlPlane(InfoCenter + Gateway) = HTTP + JSON`
2. `NodeControl 管理面/任务面 = gRPC`
3. `NodeControl 服务内部数据面 = HTTP + JSON`

对应职责：

1. `ControlPlane / InfoCenter`
   - 节点注册
   - 节点心跳
   - 服务路由聚合
   - 简单运维状态
2. `ControlPlane / Gateway`
   - 对外稳定服务调用入口
   - `service_name -> route` 缓存
   - 失败实例切换
3. `NodeControl`
   - 工程包上传
   - 代码缓存
   - 任务执行
   - 服务实例生命周期
4. `Caller Client`
   - 直接调 `ControlPlane Gateway`
   - 必要时查询路由事实

对 Python 调用方，当前已提供 `GatewayServiceClient` 作为薄封装。

## 2. 角色定义

### 2.1 owner client

负责：

1. 创建服务
2. 持有 `service_token`
3. 进入长驻态并持续心跳
4. 正常退出时发 `EndService`

推荐 owner 侧路径：

1. 部署服务
2. `deploy_from_infocenter(...)` 返回后 keepalive 已自动启动
3. 做少量预热调用
4. `group.join(...)`
5. `Ctrl+C` 自动结束服务

### 2.2 caller client

负责：

1. 调 `ControlPlane Gateway`
2. 以 `service_name` 作为发现键调用服务方法
3. 必要时可查 `service_name` 对应的 route 事实

它不持有 owner 权限，也不负责服务生命周期。

### 2.4 Python client 分层

当前 Python client 可以分成几类：

1. `InfoCenterClient`
   - 查节点 / 查 route / 任务选点
2. `GatewayServiceClient`
   - 走 Gateway 的薄调用 client
3. `GatewayModuleClient`
   - 走 Gateway 的 module-like caller
4. `DiscoveryServiceClient`
   - 客户端侧服务发现 + 本地 route cache + 直连实例
5. `DiscoveryModuleClient`
   - Discovery 风格的 module-like caller
6. `ServiceModuleGroup`
   - owner / deploy 侧的 module-like group
   - 推荐用 `join()` 长驻
7. `NodeControlClient`
   - 底层 gRPC 管理与任务 client
8. `TaskBatchClient`
   - 任务模式 helper

### 2.3 task client

负责：

1. 上传任务代码
2. 选节点
3. 提交任务
4. 拉取结果
5. 取消任务或取消一批任务

## 3. 服务模式

### 3.1 命名语义

1. `service_name` 是逻辑服务名，也是对外发现主键。
2. `service_id` 是某个实例的内部唯一标识。
3. 一个 `service_name` 可以对应多个 route，表示同一逻辑服务的多个实例/副本。
4. 不兼容“不同客户端注册同名但语义不同”的场景，命名唯一性由客户端自己保证。

### 3.2 调用语义

1. 上传支持 `py / tar.gz / zip / whl`
2. 注册时指定 `entry_module + export_spec`
3. 服务方法通过 `method_name -> callable` 路由
4. 对外推荐调用路径是 `ControlPlane Gateway HTTP + JSON`
5. 节点内部实际执行入口仍是 `NodeControl` 上的 `service_id` 级别 HTTP

## 4. 任务模式

### 4.1 当前模型

1. 任务模式仍然走 `NodeControl gRPC`
2. 不引入 task-client heartbeat
3. 节点内使用本地多进程共享执行池
4. 结果当前保存在内存中，不做持久化

### 4.2 标识语义

1. `task_id` 是单个任务的唯一标识
2. `job_id` 是一批任务的分组标识
3. `job_id` 不是 session，也不需要心跳

## 5. 设计取向

当前优先级是：

1. 简单
2. 稳定
3. 可预测
4. 易排障

因此当前刻意不做：

1. InfoCenter 内建调度器
2. InfoCenter 代理任务和服务调用
3. 复杂的任务会话模型
4. 复杂的内建鉴权中心

## 6. 部署形态

### 6.1 默认形态

默认推荐：

1. 一个 `controlplane` 进程
2. 多个 `nodecontrol` 进程

也就是：

1. `InfoCenter` 和 `Gateway` 同进程
2. Gateway 直接共享 `InfoCenterState`
3. 不走 `Gateway -> InfoCenter` 的本机网络回环

### 6.2 可选形态

如果需要，也支持：

1. 单独启动 `infocenter`
2. 单独启动 `gateway`

此时：

1. Gateway 仍保留本地 `GatewayRouteCache`
2. Gateway 通过 `InfoCenter HTTP + JSON` 拉取 route
3. 不会每次请求都查 InfoCenter，而是走缓存、失败刷新和后台刷新
