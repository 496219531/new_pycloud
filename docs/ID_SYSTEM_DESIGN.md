# ID 体系设计与改进

## 现状分析

### Service Session 模式（已优化）

**参数：**
- `owner_client_id`: 可选
  - 默认值：`"client-{IP}"`
  - 示例：`"client-192.168.1.100"`
- `service_name`: 可选
  - 默认值：`"{module}-{IP}-{timestamp_sec}"`
  - 示例：`"compute-192.168.1.100-20260330183235"`
- `service_id`: 系统生成（UUID）
  - 用户不关心，仅用于内部标识

**唯一性保证：**
- ✅ IP 地址维度（跨机器唯一）
- ✅ 时间戳秒级（跨时间唯一）
- ✅ 模块名语义（易于识别）

### Task Batch 模式（需要改进）

**参数：**
- `client_id`: 必填（有简单默认值）
  - 当前默认值：`"task-client-{timestamp_sec}"`
  - 示例：`"task-client-1746445200"`
  - **问题**：秒级精度，同一秒内多次调用会冲突
- `job_id`: 必填（有简单默认值）
  - 当前默认值：`"job-{timestamp_sec}"`
  - 示例：`"job-1746445200"`
  - **问题**：秒级精度，同一秒内多次调用会冲突
- `task_id`: 客户端生成（submit_payloads）
  - 格式：`"{job_id}-task-{seq:04d}"`
  - 示例：`"job-1746445200-task-0001"`
  - **问题**：依赖 job_id，如果 job_id 冲突，task_id 也冲突

**唯一性问题：**
- ❌ 缺少 IP 地址维度（多机器部署可能冲突）
- ❌ 时间戳秒级精度不够（同一秒内多次调用会生成相同 ID）
- ❌ 用户需要手动保证 client_id 和 job_id 唯一性

## 改进目标

1. **自动生成唯一 ID**：用户无需关心唯一性
2. **统一生成规则**：Service 和 Task 模式使用相同规则
3. **多维度保证唯一**：
   - IP 地址维度（跨机器唯一）
   - 时间戳毫秒级（跨时间唯一）
   - 序列号（同一毫秒内唯一）
4. **易于调试**：ID 包含关键信息（IP、时间、序列号）

## 统一 ID 生成规则

### 1. client_id

**格式：** `client-{IP}-{timestamp_ms}-{seq}`

**生成逻辑：**
```python
if not client_id:
    local_ip = _get_local_ip()
    timestamp_ms = int(time.time() * 1000)
    seq = _client_seq_counter  # 实例内自增
    client_id = f"client-{local_ip}-{timestamp_ms}-{seq:04d}"
```

**示例：**
- `"client-192.168.1.100-1746445200123-0001"`
- `"client-192.168.1.100-1746445200124-0002"`（下一毫秒）
- `"client-192.168.1.100-1746445200123-0002"`（同一毫秒，不同序列号）

### 2. job_id

**格式：** `job-{IP}-{timestamp_ms}-{seq}`

**生成逻辑：**
```python
if not job_id:
    local_ip = _get_local_ip()
    timestamp_ms = int(time.time() * 1000)
    seq = _job_seq_counter  # 实例内自��
    job_id = f"job-{local_ip}-{timestamp_ms}-{seq:04d}"
```

**示例：**
- `"job-192.168.1.100-1746445200123-0001"`
- `"job-192.168.1.100-1746445200124-0002"`

### 3. task_id（已有实现）

**格式：** `{job_id}-task-{seq:04d}`

**生成逻辑：**（已实现）
```python
self._submit_seq += 1
task_id = f"{job_id}-task-{self._submit_seq:04d}"
```

**示例：**
- `"job-192.168.1.100-1746445200123-0001-task-0001"`
- `"job-192.168.1.100-1746445200123-0001-task-0002"`

### 4. service_name（已有实现）

**格式：** `{module}-{IP}-{timestamp_sec}`

**生成逻辑：**（已实现）
```python
timestamp_sec = time.strftime("%Y%m%d%H%M%S")
service_name = f"{entry_module}-{local_ip}-{timestamp_sec}"
```

**示例：**
- `"compute-192.168.1.100-20260330183235"`

## 唯一性保证

| 维度 | Service 模式 | Task 模式（改进后） |
|------|-------------|------------------|
| IP 地址 | ✅ | ✅ |
| 时间戳精度 | 秒级 | 毫秒级 |
| 序列号 | ❌ | ✅（毫秒内自增） |
| 总体唯一性 | 跨机器+秒级唯一 | 跨机器+毫秒级+序列号唯一 |

**冲突概率：**
- Service 模式：同一机器同一秒内多次部署会冲突（需要手动加时间戳或序列号）
- Task 模式（改进后）：毫秒级+序列号，理论上不会冲突

## 实现改进

### 修改 TaskBatchClient

```python
class TaskBatchClient:
    _client_seq: int = 0  # 类级别计数器
    _job_seq: int = 0     # 类级别计数器

    @classmethod
    def from_infocenter(
        cls,
        *,
        infocenter_target: str,
        client_id: Optional[str] = None,  # 改为 Optional
        job_id: Optional[str] = None,     # 改为 Optional
        ...
    ) -> "TaskBatchClient":
        # 自动生成 client_id
        effective_client_id = client_id
        if not effective_client_id:
            local_ip = _get_local_ip()
            timestamp_ms = int(time.time() * 1000)
            cls._client_seq += 1
            effective_client_id = f"client-{local_ip}-{timestamp_ms}-{cls._client_seq:04d}"

        # 自动生成 job_id
        effective_job_id = job_id
        if not effective_job_id:
            local_ip = _get_local_ip()
            timestamp_ms = int(time.time() * 1000)
            cls._job_seq += 1
            effective_job_id = f"job-{local_ip}-{timestamp_ms}-{cls._job_seq:04d}"

        ...
```

## 使用示例

### 最简用法（所有 ID 自动生成）

```python
# Service Session
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    artifact_path="service.py",
)
# 自动生成：
# - owner_client_id: "client-192.168.1.100-1746445200123-0001"
# - service_name: "service-192.168.1.100-20260330183235"

# Task Batch
with TaskBatchClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task.py",
) as batch:
    # 自动生成：
    # - client_id: "client-192.168.1.100-1746445200123-0001"
    # - job_id: "job-192.168.1.100-1746445200123-0001"

    result = batch.submit_payloads([{"x": 1}, {"x": 2}])
    # 自动生成 task_id：
    # - "job-...-task-0001"
    # - "job-...-task-0002"
```

### 手动指定（生产环境）

```python
# Service Session（手动指定，确保一致性）
group = ModuleLikeServiceGroup.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    owner_client_id="prod-worker-01",
    service_name="data-processor-v1",
    artifact_path="service.py",
)

# Task Batch（手动指定，便于追踪）
with TaskBatchClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    client_id="etl-worker-01",
    job_id="data-load-20260330",
    blob=blob,
    filename="task.py",
) as batch:
    result = batch.submit_payloads([{"x": 1}, {"x": 2}])
```

## 向后兼容性

✅ **完全向后兼容**：
- 所有参数仍可手动指定
- 默认值生成逻辑仅在参数���空时触发
- 现有代码无需修改

```python
# 旧代码仍然正常工作
with TaskBatchClient.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    client_id="my-client",    # 手动指定，优先级高
    job_id="my-job",          # 手动指定，优先级高
    blob=blob,
    filename="task.py",
) as batch:
    ...
```

## 测试覆盖

```python
# tests/test_id_generation.py

def test_client_id_auto_generation():
    """测试 client_id 自动生成"""
    # 不提供 client_id
    batch = TaskBatchClient.from_infocenter(...)
    assert batch.client_id.startswith("client-")
    assert batch.client_id.count("-") >= 3  # client-{IP}-{timestamp}-{seq}

def test_job_id_auto_generation():
    """测试 job_id 自动生成"""
    batch = TaskBatchClient.from_infocenter(...)
    assert batch.job_id.startswith("job-")
    assert batch.job_id.count("-") >= 3

def test_id_uniqueness():
    """测试 ID 唯一性"""
    batch1 = TaskBatchClient.from_infocenter(...)
    batch2 = TaskBatchClient.from_infocenter(...)

    # 不同实例的 ID 应该不同
    assert batch1.client_id != batch2.client_id
    assert batch1.job_id != batch2.job_id

def test_task_id_generation():
    """测试 task_id 自动生成"""
    batch = TaskBatchClient.from_infocenter(...)
    result = batch.submit_payloads([{"x": 1}, {"x": 2}])

    # task_id 应该是唯一的
    task_ids = [item.task_id for item in result.accepted]
    assert len(set(task_ids)) == len(task_ids)  # 无重复
```

## 总结

| 改进点 | Service 模式 | Task 模式 |
|--------|-------------|----------|
| client_id 自动生成 | ✅ 已实现 | 🔧 需要改进 |
| service_name/job_id 自动生成 | ✅ 已实现 | 🔧 需要改进 |
| task_id 自动生成 | N/A | ✅ 已实现 |
| IP 地址维度 | ✅ 已实现 | 🔧 需要添加 |
| 时间戳精度 | 秒级（够用） | 🔧 毫秒级（更安全） |
| 序列号维度 | ❌ 缺少 | 🔧 需要添加 |

改进后，Task 模式将拥有与 Service 模式一致的智能默认值，用户无需关心 ID 唯一性。