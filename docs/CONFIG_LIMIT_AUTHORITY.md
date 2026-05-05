# Config Limit Authority

`pycloud_parallel.controlplane.config` 是运行时 limit 的唯一 authority。

本 PR 只整理归类与 loader，不改变默认值、不迁移调用点、不删除旧常量。

## 分层

| 层 | 含义 | 消费方 |
| --- | --- | --- |
| `policy thresholds` | policy profile 的 inline payload/result 阈值 | `policy_profile.py` / effective policy |
| `transport bounds` | HTTP request/response body 或 control message 边界 | HTTP client/server |
| `object/store bounds` | object upload、hash、segment、gateway upload 限制 | DataRef / object store |
| `job/staging bounds` | job submit、staged refs、gateway stage TTL | JobQueue / gateway staging |
| `capacity defaults` | node/service 默认容量和 worker 数 | `pycloudctl` / node startup |

## 代码入口

1. `load_config_from_env()`
   - 唯一 env loader
   - 每个 env 默认值只在 setting 表里定义一次
2. `reload_config()`
   - 测试和动态配置入口
   - 复用 `load_config_from_env()`
3. `get_config_limit_authority()`
   - 返回只读分层 dataclass
   - 用来阅读和测试 authority 结构
4. 旧常量
   - 继续导出
   - 继续作为外部兼容入口

## 合成 helper

`config.py` 内的合成 helper 是 limit authority 的边界函数。

1. `normalize_policy_limit_values(...)`
   - 负责修正 policy soft/hard/result hard 的基本关系
2. `merge_payload_limits_with_effective_policy(...)`
   - 负责把 runtime payload limit 和 session effective policy 合并
3. `merge_object_threshold_with_policy_soft_limit(...)`
   - 负责 objectify threshold 与 policy soft limit 的取小
4. `get_node_control_http_body_limit_bytes(...)`
   - 负责 NodeControl HTTP body 与 object body 下限合成
5. `get_managed_globals_control_limit_bytes(...)`
   - 负责 managed globals 的 policy hard limit 与 control send bound 合成
6. `get_job_staging_replica_count(...)`
   - 负责 job staged refs 的副本数默认值和下限修正
7. `get_job_staged_ref_ttl_sec(...)`
   - 负责 job staged refs 的 TTL 默认值和下限修正

## 不做

1. 不改变默认值
2. 不改变 Service / TaskPool / JobQueue API
3. 不迁移 `support.py` 的 limit 合成逻辑
4. 不重构 serialization / scheduler / DataRef 主流程
5. 不删除旧常量导出

## 增加新 limit 的规则

1. 先判断属于哪一层
2. 只在 `_INT_SETTINGS` / `_BOOL_SETTINGS` / `_CHOICE_SETTINGS` 中定义默认值
3. 如果需要旧名兼容，把旧 env 名放进同一个 setting 的 `names`
4. 在 `get_config_limit_authority()` 中放进对应 dataclass
5. 更新 `docs/RUNTIME_LIMITS.md`
