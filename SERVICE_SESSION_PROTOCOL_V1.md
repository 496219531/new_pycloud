# PyCloud 服务会话协议（V1）

## 1. 目标

服务会话用于“上传一份工程包，在节点上长期驻留，并暴露多个可调用方法”。

它适合：

1. `viewer.py` / `compute_service.main` 这种模块型服务。
2. 一个包里导出多个业务函数。
3. owner 通过心跳控制生命周期。

## 2. 参与角色

1. `OwnerClient`
   - 创建服务
   - 持有 `service_token`
   - 发送心跳
   - 主动结束服务
2. `NodeControl`
   - 保存代码
   - 建立方法路由
   - 启动服务进程池
3. `CallerClient`
   - 查询路由
   - 调用服务方法
4. `InfoCenter`
   - 汇总节点和服务路由

## 3. 协议拆分

### 3.1 NodeControl gRPC

负责：

1. `CreateService`
2. `ListServiceMethods`
3. `CallService`
4. `HeartbeatService`
5. `EndService`
6. `GetServiceStatus`

### 3.2 InfoCenter HTTP + JSON

负责：

1. 节点注册
2. 节点心跳
3. 路由查询
4. 运维查看

### 3.3 服务 HTTP 数据面

负责：

1. `POST /svc/{service_id}/call/{method}`
2. `GET /svc/{service_id}/status`

## 4. 上传形态

### 4.1 支持的内容

1. 单文件 `py`
2. `tar.gz`
3. `zip`
4. `whl`

### 4.2 当前上传过程

1. 客户端准备工程包。
2. 通过 `CreateService(stream ...)` 分块上传。
3. NodeControl 边收边写临时文件。
4. 完成后校验 `sha256`。
5. 生成 `code_version=sha256:<digest>`。
6. 对归档文件解压到独立目录。

### 4.3 导入污染防护

对于包导入：

1. 导入前会清理 `entry_module` 及其父包缓存。
2. 避免重复部署同名包时命中旧路径。

## 5. 方法导出

### 5.1 导出规则

支持：

1. `decorator`
2. `explicit`
3. `all`
4. `single`

### 5.2 推荐模式

默认推荐：

1. `export_mode="decorator"`
2. `export_decorator="pycloud_export"`

原因：

1. 更安全。
2. 更可控。
3. 不会把模块里的所有函数都暴露出去。

### 5.3 方法路由

服务启动后会建立：

1. `method_name -> callable`

`ListServiceMethods` 可用于查询可调方法。

## 6. 生命周期

### 6.1 创建

1. Owner 调 `CreateService`。
2. NodeControl 创建服务进程池。
3. 返回 `service_id + service_token + http_base_url`。
4. 节点下一次向 InfoCenter 心跳时，路由会出现在 `/services/routes`。

注意：

1. 路由聚合不是强同步返回。
2. 刚创建后立刻查 InfoCenter，可能要等一个短心跳周期。

### 6.2 保活

1. Owner 周期性调用 `HeartbeatService`。
2. 超时未续租则服务会被回收。

### 6.3 主动结束

1. Owner 调用 `EndService`。
2. 服务进入停止并释放 worker 额度。

## 7. 权限与身份

### 7.1 当前权限边界

1. `owner_client_id` 表示谁是 owner。
2. `service_token` 才是管理权限凭证。

### 7.2 当前调用权限

1. 服务方法调用当前默认不做外部统一鉴权网关。
2. `CallService` / HTTP 调用可以选择携带 `service_token`。
3. 如果要做更严格的调用权限，建议由外部网关拦截。

## 8. 服务唯一性

1. 活跃 `service_name` 视为全局唯一。
2. 服务端不再按 `owner_client_id + service_name` 做二次路由区分。
3. 如果多个客户端需要不同实例，应自行生成不同 `service_name`。

## 9. 客户端重启复用

Python 客户端当前会在本地缓存：

1. `owner_client_id`
2. `service_name`
3. `artifact_code_version`
4. 每个节点的 `service_id`
5. 每个节点的 `service_token`

复用条件：

1. 同 `owner_client_id`
2. 同 `service_name`
3. 同 `artifact_code_version`
4. 本地 token 缓存仍然存在

如果满足这些条件，可以直接复用远端活跃服务，而不重新上传。

## 10. 节点选择

`deploy_from_infocenter(...)` 当前部署逻辑：

1. 查询 InfoCenter 节点。
2. 过滤：
   - `healthy=false`
   - `schedulable=false`
   - `drain=true`
3. 按 `service_worker_available` 排序。
4. 选出 `node_ids` 或 `node_count` / `min_success_nodes` 决定的节点数。

这版默认是“按需选点”，不是“默认部署到所有节点”。

## 11. 当前推荐流程

1. `CreateService`
2. 把 `service_token` 落本地
3. `ListServiceMethods`
4. 开启 keepalive
5. 通过 HTTP 或 gRPC 调方法
6. 完成后 `EndService`
