# PyCloud 类重命名说明

## 概述

为了提供更直观、更易记的类名，我们为四个核心客户端类引入了新的命名。新的命名更清楚地表达了每个类的用途和连接方式。

## 重命名对照表

| 旧命名 | 新命名 | 含义 |
|--------|--------|------|
| `ServiceModuleGroup` | **`DeployedService`** | 部署并拥有服务 |
| `TaskModuleClient` | **`TaskSubmitter`** | 提交任务 |
| `GatewayModuleClient` | **`GatewayConnect`** | 通过网关连接 |
| `DiscoveryModuleClient` | **`DirectConnect`** | 直接连接实例 |

## 导入方式

### 推荐方式（新命名）

```python
from pycloud_parallel import (
    DeployedService,   # 部署并��有服务
    TaskSubmitter,     # 提交任务
    GatewayConnect,    # 通过网关连接
    DirectConnect,     # 直接连接实例
)
```

### 旧命名（仍然可用）

```python
from pycloud_parallel import (
    ServiceModuleGroup,      # 旧名，向后兼容
    TaskModuleClient,        # 旧名，向后兼容
    GatewayModuleClient,     # 旧名，向后兼容
    DiscoveryModuleClient,   # 旧名，向后兼容
)
```

### 混合使用（也可以）

```python
from pycloud_parallel import DeployedService, GatewayConnect
from pycloud_parallel.controlplane import TaskSubmitter, DirectConnect
```

## 类的对比

| 特性 | DeployedService | TaskSubmitter | GatewayConnect | DirectConnect |
|------|----------------|---------------|----------------|---------------|
| **旧名** | ServiceModuleGroup | TaskModuleClient | GatewayModuleClient | DiscoveryModuleClient |
| **角色** | 拥有者 | 拥有者 | 消费者 | 消费者 |
| **管理生命周期** | ✅ 是 | ✅ 是 | ❌ 否 | ❌ 否 |
| **路由方式** | 部署时确定 | 部署时确定 | Gateway 代理 | 客户端发现 |
| **连接方式** | 直连服务实例 | 直连节点 | Gateway → 实例 | 直连实例 |
| **适用场景** | 长运行 RPC 服务 | 短期任务处理 | 按服务名调用 | 高性能直连 |
| **类似** | 服务提供者 | 任务提交者 | API 网关客户端 | Eureka 客户端 |

## 使用示例

### 1. DeployedService（部署服务）

```python
from pycloud_parallel import DeployedService

# 部署服务并拥有其生命周期
service = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="service.py",
)

# 像调用本地模块一样
result = await service.square(x=7)

# 清理
service.close(end_services=True)
```

### 2. TaskSubmitter（提交任务）

```python
from pycloud_parallel import TaskSubmitter

# 创建任务客户端
task = TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task.py",
)

# 像调用函数一样提交任务
results = task.run(value=7)

# 清理
task.close()
```

### 3. GatewayConnect（网关连接）

```python
from pycloud_parallel import GatewayConnect

# 通过网关连接（按服务名）
client = GatewayConnect(
    "127.0.0.1:50051",
    service_name="square-service",
)

# 像调用本地模块一样
result = client.square.sync(x=7)
```

### 4. DirectConnect（直连）

```python
from pycloud_parallel import DirectConnect

# 直接连接实例（客户端发现路由）
client = DirectConnect(
    "127.0.0.1:50051",
    service_name="square-service",
)

# 像调用本地模块一样
result = client.square.sync(x=7)
```

## 命名优势

### 1. 清晰表达用途

- `DeployedService`：明确表示是"部署的服务"
- `TaskSubmitter`：明确表示是"任务提交器"
- `GatewayConnect` vs `DirectConnect`：清楚表达连接方式的区别

### 2. 统一后缀

- `Connect` 后缀：`GatewayConnect`、`DirectConnect`（都是连接客户端）
- `Service`/`Submitter` 后缀：表达不同角色

### 3. 简洁易记

| 旧名长度 | 新名长度 | 改进 |
|---------|---------|------|
| `ServiceModuleGroup` (20) | `DeployedService` (15) | ✅ 更短 |
| `TaskModuleClient` (17) | `TaskSubmitter` (14) | ✅ 更短 |
| `GatewayModuleClient` (20) | `GatewayConnect` (14) | ✅ 更短 |
| `DiscoveryModuleClient` (22) | `DirectConnect` (14) | ✅ 更短 |

### 4. 连接方式对比

`GatewayConnect` vs `DirectConnect` 清楚表达了两种连接方式的区别：

- **GatewayConnect**：通过 Gateway 代理转发
  - 路径：`客户端 → Gateway → NodeControl → 服务实例`
  - 优点：统一入口，按服务名调用
  - 缺点：多一跳，轻微延迟

- **DirectConnect**：客户端直接发现并连接
  - 路径：`客户端 → InfoCenter发现 → NodeControl → 服务实例`
  - 优点：少一跳，性能更好
  - 缺点：需要客户端维护路由缓存

## 向后兼容性

✅ **100% 向后兼容**

1. 旧类名仍然可用
2. 旧代码无需修改
3. 新旧代码可以混合使用

```python
# 旧代码仍然正常工作
from pycloud_parallel import ServiceModuleGroup

service = ServiceModuleGroup.deploy_from_infocenter(...)
```

```python
# 新代码使用新名字
from pycloud_parallel import DeployedService

service = DeployedService.deploy_from_infocenter(...)
```

## 迁移建议

### 对于新代码

推荐使用新命名：

```python
from pycloud_parallel import DeployedService, TaskSubmitter, GatewayConnect, DirectConnect
```

### 对于现有代码

- **选项 1**：保持不变，继续使用旧命名
- **选项 2**：逐步迁移到新命名

```python
# 逐步迁移：先导入新名字
from pycloud_parallel import DeployedService as ServiceModuleGroup
```

## 相关文档

- [QUICK_START.md](QUICK_START.md) - 快速入门
- [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md) - 架构概览
- [GATEWAY_CLIENT_GUIDE.md](GATEWAY_CLIENT_GUIDE.md) - Gateway 调用指南
- [TASK_MODULE_CLIENT.md](TASK_MODULE_CLIENT.md) - Task 模式指南

## 总结

新的命名系统更加直观、简洁、易记，同时完全向后兼容。推荐新代码使用新命名，旧代码可以继续使用旧命名。
