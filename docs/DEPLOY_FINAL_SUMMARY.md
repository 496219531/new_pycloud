# 部署默认值最终总结

## ✅ 实现的功能

### 1. `owner_client_id` 可选
- **默认值**：`"client-{本机IP}"`
- **示例**：`"client-192.168.1.100"`

### 2. `service_name` 可选
- **默认值**：`"{模块名}-{本机IP}-{时间戳}"`
- **时间戳格式**：`YYYYMMDDHHMMSS`（精确到秒）
- **示例**：`"my_service-192.168.1.100-20260330183235"`

### 3. 自动推断 entry_module
推断顺序：
1. `entry_module` 参数
2. `filename`（如果是 .py 文件）
3. `artifact_path`（如果是 .py 文件）
4. `artifact_paths` 第一个（如果是 .py 文件）
5. 回退到 `"service"`

### 4. service_name 语义
- `service_name` 在活跃服务范围内应视为全局唯一
- 服务端不按 `owner_client_id` 做同名兼容路由
- 多租户命名由客户端自行处理

### 4. 自动获取本机 IP
使用 UDP socket 获取，不实际发送数据：
```python
def _get_local_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "localhost"
```

## 服务名格式

### 组成部分
```
{模块名}-{IP地址}-{时间戳}
```

### 示例
```
compute-192.168.1.100-20260330183235
my_service-172.16.10.202-20260330183236
service-10.0.0.5-20260330183237
```

### 优势
1. **跨机器唯一**：包含 IP 地址
2. **跨时间唯一**：包含时间戳（精确到秒）
3. **语义清晰**：包含模块名
4. **独享计算**：每次运行都是独立实例
5. **易于调试**：从服务名就能看出创建时间和位置

## 最小化部署

### 只需 2 个参数（使用本地文件）
```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="service.py",
)
```

### 只需 3 个参数（使用 blob）
```python
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=b"code here...",
    filename="service.py",
)
```

## 实际运行示例

```bash
$ python scripts/demo_simple_deploy.py

方式 1：使用所有默认值
  自动生成的 owner_client_id: client-172.16.10.202
  自动生成的 service_name: compute-172.16.10.202-20260330183235

方式 2：提供 entry_module
  自动生成的 service_name: my_service-172.16.10.202-20260330183235

方式 3：只提供 owner_client_id
  使用的 owner_client_id: my-custom-client
  自动生成的 service_name: service-172.16.10.202-20260330183235

方式 4：只提供 service_name
  自动生成的 owner_client_id: client-172.16.10.202
  使用的 service_name: my-custom-service-1774872491
```

## 向后兼容性

✅ **完全向后兼容**：所有现有代码无需修改

```python
# 旧代码仍然正常工作
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="my-client",
    service_name="my-service",
    blob=blob,
    filename="service.py",
)
```

## 验证方式

```bash
python scripts/demo_simple_deploy.py
```

示例脚本会自动清理创建的服务，可重复执行。

## 文件变更

### 修改的文件
- [src/pycloud_parallel/controlplane/client.py](src/pycloud_parallel/controlplane/client.py)
  - 添加 `_get_local_ip()` 函数
  - 修改 `deploy_from_infocenter()` 参数
  - 实现默认值生成逻辑

### 新增的文件
- [scripts/demo_simple_deploy.py](scripts/demo_simple_deploy.py) - 简化部署演示
- [docs/DEPLOY_DEFAULT_VALUES.md](docs/DEPLOY_DEFAULT_VALUES.md) - 完整文档

## 使用建议

### 开发/测试环境
```python
# 使用所有默认值，每次运行都是独立实例
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="my_service.py",
)
# 服务名自动生成，不会与其他服务冲突 ✅
```

### 生产环境
```python
# 手动指定，确保可控性和一致性
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="prod-server:50051",
    owner_client_id="prod-worker-01",
    service_name="data-processor-v1",
    blob=blob,
    filename="service.py",
)
```

## 关键特性

1. ✅ **自动化**：减少手动配置
2. ✅ **唯一性**：时空双维度保证不冲突
3. ✅ **灵活性**：可选手动指定
4. ✅ **兼容性**：完全向后兼容
5. ✅ **可调试**：服务名包含关键信息
6. ✅ **独享性**：每次运行独立实例，不共享资源
