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
2. `NodeControl = HTTP`
3. 节点内部服务数据面 = HTTP

### 2.2 执行入口收敛

1. V1 删除旧的 `local_runtime` 单机多进程入口。
2. 跨节点和本机执行都统一走 `controlplane` / `NodeControl`。

### 2.3 服务命名

1. 注册到 `InfoCenter` 的活跃 `service_name` 视为全局唯一。
2. 服务端不再兼容 `owner_client_id + service_name` 的多租户路由。
3. 如果需要多租户隔离，应由客户端自己生成唯一名字。
4. `Service.startup(target="")` 是 startup 专属的未注册模式，不注册 `InfoCenter`，因此不参与 `service_name` 全局排他；可以在不同端口启动多个同名 startup service，但调用方需要直接使用各自的本地 service HTTP 地址。
5. 空 `target` 不表示通用 local 模式。`Service.deploy(...)`、`Service.connect(...)`、`TaskPool.open(...)` 仍然必须显式传入 `target`；未来本地 IPC 模式只通过 `target="local"` 触发。

### 2.4 权限

1. `owner_client_id` 只是 owner 身份标识。
2. 真正的服务管理权限依赖 `service_token`。
3. 当前方法调用权限不做复杂内建鉴权，必要时建议接外部网关。

### 2.5 动态部署与 code version 一致性

动态部署的服务采用强一致发布模型：

1. 同一个动态服务由唯一发布者（owner deploy session）统一发布和管理。
2. 同名服务的运行中副本必须属于同一个发布者控制域，并保持同一个 `code_version`。
3. 如果某个 node 断开、被判定失效、或以新的 `node_instance_id` 重连，它不能带着旧进程继续算作已部署副本；owner keepalive 必须重新部署/补齐。
4. `Service.startup(...)` / 系统启动时挂载的 startup service 只由自身进程管理，不允许被其他动态发布者接管。
5. startup service 不能动态加入任何现有服务组；即使 `service_name/code_version` 完全一致，也不能作为已有动态服务的扩容副本，因为它自治运行，不在动态发布者的版本管控、回滚、keepalive 与 close 闭环内。
6. 对已注册到同一个 `InfoCenter` 的服务，动态部署与 startup service 对同一个 `service_name` 是互斥的：任一方已经存在时，另一方即使 `code_version` 一致也必须拒绝启动/部署。
7. 即使 startup service 的 `service_name` 和 `code_version` 与某个动态部署一致，也不能把它视为动态 owner 的可复用副本，因为它不在该 owner 的 token / keepalive / close 控制域内。

动态扩容的正确路径是同一个动态发布者扩大目标副本数（例如提高 `node_count`）后重启/恢复 deploy session。部署端会用本地缓存的 `service_id/service_token` 接回自己已经发布的同 code version 服务，并由 keepalive 补齐新增节点；这仍然属于同一个 owner 控制域，不需要也不允许 startup service 参与扩容。

### 2.6 选点策略

当前已经改成统一 scheduler 框架：

1. 统一候选对象
2. 统一硬过滤
3. 统一特征集合
4. 统一复合评分
5. 同分候选 round-robin 打散

默认 profile：

1. `SERVICE_DEFAULT`
2. `TASKPOOL_DEFAULT`
3. `JOBQUEUE_DEFAULT`

这里要注意分层：

1. scheduler 只负责“选谁”
2. `TaskPool` 自己仍负责 `max_in_flight / refill / pull_results`
3. `Service` 自己仍负责 RPC 调用与失败切换

## 3. 当前部署路径

### 3.1 上传

1. 目录或文件列表打包为 `tar.gz` / `zip`
2. HTTP 上传到 NodeControl
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
4. NodeControl HTTP `CallService` 只作为内部低层入口

如果不想经过 Gateway，也支持客户端自己做发现和选路：

1. `DiscoveryServiceClient`
2. `Service.connect(..., transport="discovery")`

它们会：

1. 先查 `InfoCenter`
2. 客户端本地维护 route cache
3. 直接调用节点内部 `service_id` 数据面

## 4. 当前推荐默认值

对本地轻量场景，推荐：

```python
worker_count=1
node_count=1
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
python examples/service_deploy_register.py
python examples/service_deploy_basic.py
python examples/service_deploy_simple.py
python examples/service_deploy_from_files.py
python examples/service_connect_discovery.py
```

## 6. 当前不做的复杂功能

1. 不做自动节点替换闭环。
2. 不做复杂资源协调器。
3. 不做统一调用鉴权中心。
4. 不做基于 owner 的同名服务兼容。

这些都不是当前版本的目标。
