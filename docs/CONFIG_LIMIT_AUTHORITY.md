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
