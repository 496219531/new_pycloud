# 当前部署模型总结

## 1. 当前部署模型

这版部署模型已经收敛为：

1. `ControlPlane(InfoCenter + Gateway)` 负责发现、简单运维、对外服务入口。
2. `NodeControl` 负责真正执行。
3. 客户端负责命名、选点，以及拒绝运行中的同名旧服务被新代码覆盖。

整体偏向：

1. 简单
2. 直接
3. 可预测
4. 易调试

## 2. 关键结论

### 2.1 控制面拆分

1. `ControlPlane = HTTP + JSON`
2. `NodeControl = gRPC`
3. 节点内部服务数据面 = HTTP

### 2.2 本地运行时收敛

1. `local_runtime` 只做单机多进程。
2. 不再承担跨集群功能。
3. 跨节点统一走 `controlplane`。

### 2.3 服务命名

1. 活跃 `service_name` 视为全局唯一。
2. 服务端不再兼容 `owner_client_id + service_name` 的多租户路由。
3. 如果需要多租户隔离，应由客户端自己生成唯一名字。

### 2.4 权限

1. `owner_client_id` 只是 owner 身份标识。
2. 真正的服务管理权限依赖 `service_token`。
3. 当前方法调用权限不做复杂内建鉴权，必要时建议接外部网关。

### 2.5 选点策略

客户端当前只做简单选点：

1. 过滤 unhealthy
2. 过滤 cordon
3. 过滤 drain
4. 按剩余 service worker 容量排序
5. 选择前 N 个节点

不做复杂调度器。

## 3. 当前部署路径

### 3.1 上传

1. 目录或文件列表打包为 `tar.gz` / `zip`
2. gRPC 流式上传到 NodeControl
3. NodeControl 边收边写临时文件
4. 校验 `sha256`
5. 落地为 `code_version=sha256:<digest>`

### 3.2 启动服务

1. 发现导出方法
2. 创建服务进程池
3. 返回 `service_id + service_token + http_base_url`
4. 节点通过心跳把路由上报给 InfoCenter

owner 推荐用法：

1. 部署完成后 keepalive 已自动启动
2. 做少量预热调用
3. 然后直接 `group.join(...)` 长驻
4. 用 `Ctrl+C` 作为正常结束路径
5. 异常退出时再由 `group.close(...)` 兜底

### 3.3 调用

1. 默认推荐路径：调用方直接走 `ControlPlane Gateway` 的 `POST /svc/{service_name}/call/{method}`
2. Gateway 内部按 `service_name` 选 route，并转发到对应 `NodeControl`
3. `service_id` 主要用于实例级管理，不是业务侧主发现名
4. gRPC `CallService` 仍保留为内部兼容入口

如果不想经过 Gateway，也支持客户端自己做发现和选路：

1. `DiscoveryServiceClient`
2. `DirectConnect`

它们会：

1. 先查 `InfoCenter`
2. 客户端本地维护 route cache
3. 直接调用节点内部 `service_id` 数据面

## 4. 当前推荐默认值

对本地轻量场景，推荐：

```python
worker_count=1
node_count=1
export_mode="decorator"
reuse_existing_same_code=True
replace_existing_if_code_changed=True
```

这样更接近“本地轻量服务”的预期，也更不容易把节点 service capacity 一次吃满。

## 5. 当前脚本建议

### 5.1 启动本地环境

```bash
./scripts/start_services.sh start
```

### 5.2 查看状态

```bash
./scripts/start_services.sh status
```

会显示：

1. 进程状态
2. 每个节点当前加载的服务名

## 7. 当前推荐部署形态

默认推荐：

1. 起一个 `controlplane`
2. 起多个 `nodecontrol`

可选支持：

1. 单独起 `infocenter`
2. 单独起 `gateway`

但默认本地/轻量部署优先用一体化 `controlplane`，更简单、更稳。

### 5.3 典型 demo

```bash
python examples/grpc_register_service_client_demo.py
python examples/demo_service_module_group.py
python examples/demo_simple_deploy.py
python examples/demo_deploy_from_files.py
python examples/grpc_existing_service_client_demo.py
```

## 6. 当前不做的复杂功能

1. 不做自动节点替换闭环。
2. 不做复杂资源协调器。
3. 不做统一调用鉴权中心。
4. 不做基于 owner 的同名服务兼容。

这些都不是当前版本的目标。
