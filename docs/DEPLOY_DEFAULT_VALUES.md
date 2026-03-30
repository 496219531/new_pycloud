# deploy_from_infocenter 默认值改进

## 概述

`deploy_from_infocenter` 方法现在支持更少的必填参数，`service_name` 和 `owner_client_id` 都可以自动生成。

## 改进内容

### 1. `owner_client_id` 可选

**之前**：必须手动提供
```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="my-client-123",  # ✅ 必须提供
    service_name="my-service",
    blob=blob,
    filename="service.py",
)
```

**现在**：自动生成为 `"client-{本机IP}"`
```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    # owner_client_id 省略，自动生成
    blob=blob,
    filename="service.py",
)
# owner_client_id 自动生成为 "client-192.168.1.100"
```

### 2. `service_name` 可选

**之前**：必须手动提供
```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="my-client",
    service_name="my-service",  # ✅ 必须提供
    blob=blob,
    filename="service.py",
)
```

**现在**：自动生成为 `"{entry_module}-{本机IP}-{时间戳}"`
```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    # service_name 省略，自动生成
    blob=blob,
    filename="my_service.py",  # 从文件名推断 entry_module
)
# service_name 自动生成为 "my_service-192.168.1.100-20260330183235"
```

## 默认值生成规则

### 命名约束

1. `service_name` 在活跃服务范围内应视为全局唯一。
2. 服务端不会按 `owner_client_id` 再对同名服务做兼容路由。
3. 如果有多租户或多实例需求，应由客户端自己生成唯一 `service_name`。

### owner_client_id

```python
if not owner_client_id:
    owner_client_id = f"client-{本机IP}"
```

**示例**：
- 未提供 → `"client-192.168.1.100"`
- `"my-client"` → `"my-client"`

### service_name

```python
# 1. 尝试从 entry_module 获取
if entry_module:
    service_name = f"{entry_module}-{本机IP}-{时间戳}"

# 2. 尝试从 filename 推断 entry_module
elif filename.endswith(".py"):
    entry_module = Path(filename).stem
    service_name = f"{entry_module}-{本机IP}-{时间戳}"

# 3. 尝试从 artifact_path 推断
elif artifact_path.endswith(".py"):
    entry_module = Path(artifact_path).stem
    service_name = f"{entry_module}-{本机IP}-{时间戳}"

# 4. 尝试从 artifact_paths 推断
elif artifact_paths[0].endswith(".py"):
    entry_module = Path(artifact_paths[0]).stem
    service_name = f"{entry_module}-{本机IP}-{时间戳}"

# 5. 回退到默认值
else:
    service_name = f"service-{本机IP}-{时间戳}"

# 时间戳格式: YYYYMMDDHHMMSS (精确到秒)
# 例如: 20260330183235 表示 2026年3月30日18:32:35
```

**示例**（时间戳为 `20260330183235`）：

| 输入 | 生成的 service_name |
|------|-------------------|
| `entry_module="my_service"` | `"my_service-192.168.1.100-20260330183235"` |
| `filename="my_service.py"` | `"my_service-192.168.1.100-20260330183235"` |
| `artifact_path="my_service.py"` | `"my_service-192.168.1.100-20260330183235"` |
| `artifact_paths=["my_service.py"]` | `"my_service-192.168.1.100-20260330183235"` |
| `filename="data.txt"` (非 .py) | `"service-192.168.1.100-20260330183235"` |
| 全部为空 | `"service-192.168.1.100-20260330183235"` |

**时间戳的优势**：
- ✅ **跨时间唯一**：每次运行生成不同的服务名
- ✅ **独享计算**：不与其他服务冲突
- ✅ **易于识别**：从服务名就能看出创建时间

### 获取本机 IP

```python
def _get_local_ip() -> str:
    """获取本机 IP 地址。"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "localhost"
```

**特点**：
- 不实际发送数据，只是创建连接
- 返回本机的实际 IP 地址（如 `192.168.1.100`）
- 如果获取失败，回退到 `"localhost"`

## 最小化部署示例

### 只需 3 个参数

```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",  # 1. 服务器地址
    blob=b"code here...",                  # 2. 代码内容
    filename="service.py",                 # 3. 文件名
)
# owner_client_id 和 service_name 自动生成 ✅
```

### 只需 2 个参数（使用本地文件）

```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",  # 1. 服务器地址
    artifact_path="service.py",            # 2. 本地文件路径
)
# owner_client_id, service_name, 代码内容自动生成 ✅
```

## 完整示例对比

### 之前（繁琐）

```python
import time
import socket

# 手动生成 owner_client_id
local_ip = socket.gethostbyname(socket.gethostname())
owner_client_id = f"my-client-{local_ip}"

# 手动生成 service_name
service_name = f"my-service-{local_ip}"

group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id=owner_client_id,  # 手动指定
    service_name=service_name,         # 手动指定
    blob=blob,
    filename="service.py",
    runtime="py3.11",
    entry_module="service",
)
```

### 现在（简洁）

```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="service.py",
    # 其他参数全部使用默认值 ✅
)
```

## 适用场景

### 快速原型开发

```python
# 不用考虑命名，直接部署测试
# 每次运行自动生成唯一的服务名
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="my_service.py",
)
# 服务名自动生成，例如: "my_service-192.168.1.100-20260330183235"
result = await group.my_method(x=1)
```

### 独享计算资源

```python
# 每次部署都是独立的实例，不会相互干扰
# 第一次运行（20260330120000）:
group1 = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="service.py",
)
# → service_name="service-192.168.1.100-20260330120000"

# 第二次运行（20260330120005）:
group2 = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="service.py",
)
# → service_name="service-192.168.1.100-20260330120005"
# 即使在同一台机器上，服务名也不同！

### 多客户端部署

```python
# 每个机器自动使用不同的 owner_client_id 和 service_name
# 机器 A (192.168.1.100, 18:32:35):
group_a = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="service.py",
)
# → owner_client_id="client-192.168.1.100"
# → service_name="service-192.168.1.100-20260330183235"

# 机器 B (192.168.1.101, 18:32:36):
group_b = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="service.py",
)
# → owner_client_id="client-192.168.1.101"
# → service_name="service-192.168.1.101-20260330183236"
```

### 生产环境（仍可手动指定）

```python
# 生产环境可以手动指定，确保一致性
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="prod-server:50051",
    owner_client_id="prod-worker-01",    # 手动指定
    service_name="data-processor",        # 手动指定
    blob=blob,
    filename="service.py",
)
```

## 验证方式

建议通过示例脚本验证默认值行为：

```bash
python scripts/demo_simple_deploy.py
```

说明：

1. 脚本已改为可重复运行（结束时会回收创建的服务）。
2. “方式 4”示例会为自定义服务名追加时间戳，避免重复执行时冲突。

## 向后兼容性

✅ **完全向后兼容**：所有现有代码无需修改

```python
# 旧代码仍然可以正常工作
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="my-client",      # 仍然可以手动指定
    service_name="my-service",         # 仍然可以手动指定
    blob=blob,
    filename="service.py",
)
```

## 总结

通过引入智能默认值，`deploy_from_infocenter` 的使用变得更简单：

- ✅ `owner_client_id` 可选，自动生成为 `"client-{IP}"`
- ✅ `service_name` 可选，自动生成为 `"{module}-{IP}"`
- ✅ 从多个来源自动推断 `entry_module`
- ✅ 自动获取本机 IP 地址
- ✅ 完全向后兼容
- ✅ 测试覆盖完整

**最小化部署只需 2-3 个参数**，大大降低了使用门槛！
