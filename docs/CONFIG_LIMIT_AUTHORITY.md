# Config Limit Authority

`pycloud_parallel.controlplane.config` 是运行时 limit 的唯一 authority。

PR1 已完成归类与 loader。PR2 收口 payload policy resolver 与 `support.py` limit authority。PR3 开始迁移 transport/http 消费点，明确 payload threshold 与 HTTP body bound 的边界。

## 分层

| 层 | 含义 | 消费方 |
| --- | --- | --- |
| `policy thresholds` | policy profile 的 inline payload/result 阈值 | `policy_profile.py` / effective policy |
| `transport bounds` | HTTP request/response body 或 control message 边界 | HTTP client/server |
| `object/store bounds` | 单对象硬上限、object upload、hash、segment、gateway upload 限制 | DataRef / object store |
| `job/staging bounds` | job submit、staged refs、gateway stage TTL | JobQueue / gateway staging |
| `capacity defaults` | node/service 默认容量和 worker 数 | `pycloudctl` / node startup |

`policy thresholds` 只回答“业务 payload/result 的 policy 边界在哪里”。
`inline thresholds` 位于 runtime payload limit 中，只回答“是否值得尝试 inline”。
超过 inline threshold 的对象直接转 `DataRef`，不再做完整序列化试算。
`transport bounds` 只回答“HTTP request/response body 或控制消息最大能收发多少 bytes”。
`object/store bounds` 里的 object size hard limit 只回答“系统允许单个 object/DataRef 背后的对象最大多大”。
`bytes materialize threshold` 只回答“对象是否允许整包进入内存成为 bytes / 被整包反序列化”。
这些边界不能互相替代。

## 代码入口

### 推荐新代码入口

1. `load_config_from_env()`
   - 唯一 env loader
   - 每个 env 默认值只在 setting 表里定义一次
2. `reload_config()`
   - 测试和动态配置入口
   - 复用 `load_config_from_env()`
3. `get_config_limit_authority()`
   - 返回只读分层 dataclass
   - 用来阅读和测试 authority 结构
4. `resolve_payload_policy(...)`
   - payload policy 的统一入口
   - 负责合并 effective policy，并把 object threshold 与 policy inline threshold 对齐
5. `get_transport_bounds()` / `get_object_store_bounds()`
   - transport/http 与 object/store 消费侧读取分层默认值的入口
6. inline threshold helper
   - `get_payload_inline_threshold_bytes(...)`
   - `get_result_inline_threshold_bytes(...)`
   - 用于“cheap estimate 后是否尝试 inline”的稳定入口
7. body / upload helper
   - `get_service_http_body_limit_bytes(...)`
   - `get_gateway_http_body_limit_bytes(...)`
   - `get_infocenter_http_body_limit_bytes(...)`
   - `get_node_control_http_body_limit_bytes(...)`
   - `get_http_object_body_limit_bytes(...)`
   - `get_gateway_upload_limits(...)`
   - `get_object_size_hard_limit_bytes(...)`
   - `get_bytes_materialize_threshold_bytes(...)`
   - `validate_object_size_bytes(...)`
   - `validate_bytes_materialize_size(...)`

新代码应优先使用以上入口，不要直接 import 裸 limit 常量。

### 兼容桥接入口

以下名字继续导出，作为外部用户和旧调用点的兼容桥接，不作为新代码首选入口：

1. payload/result threshold 常量
   - `INLINE_*`
   - `LOCAL_INLINE_*`
   - `DEFAULT_SAFE_*`
   - `TRUSTED_INTERNAL_*`
2. transport/body 常量
   - `CONTROL_HTTP_*`
   - `*_HTTP_BODY_MAX_BYTES`
3. object/store/job/capacity 常量
   - `OBJECT_*`
   - `GATEWAY_MAX_UPLOAD_*`
   - `JOB_*`
   - `NODE_*`
   - `SERVICE_*`
4. mode/env 兼容名
   - `PYCLOUD_*`
   - `*_MODE`

## 合成 helper

`config.py` 内的合成 helper 是 limit authority 的边界函数。

1. `normalize_policy_limit_values(...)`
   - 负责修正 policy threshold/hard/result hard 的基本关系
2. `merge_payload_limits_with_effective_policy(...)`
   - 负责把 runtime payload limit 和 session effective policy 合并
3. `merge_object_threshold_with_policy_threshold(...)`
   - 负责 objectify threshold 与 policy threshold 的取小
4. `policy_with_threshold(...)`
   - 负责把已合成的 threshold 写回 `PayloadPolicy`
5. `resolve_payload_policy(...)`
   - 负责统一 `get_payload_policy(...)`、effective policy merge、object threshold 合成
6. `get_node_control_http_body_limit_bytes(...)`
   - 负责 NodeControl HTTP body 与 object body 下限合成
7. `get_service_http_body_limit_bytes(...)` / `get_gateway_http_body_limit_bytes(...)` / `get_infocenter_http_body_limit_bytes(...)`
   - 负责各 HTTP server 的 body bound 默认值和下限修正
8. `get_http_object_body_limit_bytes(...)`
   - 负责 object HTTP upload/download body bound 默认值和下限修正
9. `get_gateway_upload_limits(...)`
   - 负责 gateway upload 文件/总量 limit 的默认值和总量下限修正
10. `get_object_size_hard_limit_bytes(...)` / `validate_object_size_bytes(...)`
   - 负责单个 object/DataRef 背后对象大小的业务硬限制
   - 不等同于 HTTP body limit，也不等同于 segment layout 阈值
11. `get_bytes_materialize_threshold_bytes(...)` / `validate_bytes_materialize_size(...)`
   - 负责整包 bytes 下载、`materialize_as="bytes"` 和整包反序列化路径的内存保护
   - 不等同于 object size hard limit；大对象可以存在，但不能走 bytes 主路径
12. `get_managed_globals_control_limit_bytes(...)`
   - 负责 managed globals 的 policy hard limit 与 control send bound 合成
13. `get_job_staging_replica_count(...)`
   - 负责 job staged refs 的副本数默认值和下限修正
14. `get_job_staged_ref_ttl_sec(...)`
   - 负责 job staged refs 的 TTL 默认值和下限修正

## Node 侧职责边界

node 不是 policy / limit / capability authority。

1. node 只严格执行中心/session 分配下来的 effective policy 和 limit
2. node 不自行合成 effective policy，也不根据本地 env 改写 session limit
3. node 本地 env 只属于进程启动默认值、物理 HTTP body 边界或兼容路径
4. `NodeCapability` 当前只是兼容/观测模型，供 route metadata、诊断和旧协议字段使用
5. `NodeCapability` 不作为未来节点筛选、标签、分组或能力管理的主 authority
6. 当前只保留最小 endpoint profile：`managed_tags`、`enabled`、`drain`、`notes`
7. `capability_tags` 是自动事实标签，不持久化，也不参与 limit/policy authority
8. `legacy_node_tags` 只是兼容旧 node 启动参数，不推荐作为长期管理方式
9. 复杂 node 管理不在本项目内继续扩展；资源打分、机器分组、拓扑、配额、权限、自动迁移应交给外部成熟工具
10. 同一物理机器可以启动多个 node，单个 node 的局部信息不能代表整台机器的资源/能力

因此，node 上报或本地检测到的能力不能参与 session policy 协商；运行时最终口径以 controlplane/session 分配为准。

## 不做

1. 不改变默认值
2. 不改变 Service / TaskPool / JobQueue API
3. 不改变 serialization mode / policy profile 语义
4. 不重构 serialization / scheduler / DataRef 主流程
5. 不删除旧常量导出

## 增加新 limit 的规则

### 新增代码规则

1. payload policy 相关代码优先使用 `resolve_payload_policy(...)`
2. transport/http body bound 优先使用：
   - `get_transport_bounds()`
   - `get_service_http_body_limit_bytes(...)`
   - `get_gateway_http_body_limit_bytes(...)`
   - `get_infocenter_http_body_limit_bytes(...)`
   - `get_node_control_http_body_limit_bytes(...)`
   - `get_http_object_body_limit_bytes(...)`
3. object/store 和 gateway upload 优先使用：
   - `get_object_store_bounds()`
   - `get_gateway_upload_limits(...)`
   - `get_object_size_hard_limit_bytes(...)`
   - `validate_object_size_bytes(...)`
   - `get_bytes_materialize_threshold_bytes(...)`
   - `validate_bytes_materialize_size(...)`
4. 不要在核心 transport/http 新代码里直接 import body/upload 裸常量：
   - `SERVICE_HTTP_BODY_MAX_BYTES`
   - `GATEWAY_HTTP_BODY_MAX_BYTES`
   - `INFOCENTER_HTTP_BODY_MAX_BYTES`
   - `NODE_CONTROL_HTTP_BODY_MAX_BYTES`
   - `OBJECT_HTTP_BODY_MAX_BYTES`
   - `CONTROL_HTTP_MAX_SEND_BYTES`
   - `CONTROL_HTTP_MAX_RECEIVE_BYTES`
   - `GATEWAY_MAX_UPLOAD_FILE_BYTES`
   - `GATEWAY_MAX_UPLOAD_TOTAL_BYTES`
5. 这些裸常量只作为兼容桥接保留。需要改历史代码时，可以逐步迁移；不要让新路径继续扩散它们。

### 新增 limit 流程

1. 先判断属于哪一层
2. 只在 `_INT_SETTINGS` / `_BOOL_SETTINGS` / `_CHOICE_SETTINGS` 中定义默认值
3. 如果需要旧名兼容，把旧 env 名放进同一个 setting 的 `names`
4. 在 `get_config_limit_authority()` 中放进对应 dataclass
5. 如果消费侧需要组合逻辑，新增 helper，不在消费模块里手写合成规则
6. 把新 helper 加入 `STABLE_CONFIG_API_EXPORTS`
7. 如果必须保留旧常量，把旧名放入 `COMPATIBILITY_CONFIG_EXPORTS`
8. 更新 `docs/RUNTIME_LIMITS.md`
9. 补测试确认推荐入口和兼容常量一致，且 `reload_config()` 后同步
