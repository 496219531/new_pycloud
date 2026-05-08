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

## 实际表达式

下面这些是“最终实际使用的 limit”对应的计算式，后续新代码应直接按这组口径理解。

### Inline 最终决策公式

inline 最终决策分成三步：先得到 runtime 基础值，再把 profile / effective policy 收进来，最后按 threshold 分流、按 hard limit 拦截。

1. runtime 基础值
   - `runtime_payload_hard = max(1, INLINE_PAYLOAD_HARD_LIMIT_BYTES)`
   - `runtime_payload_threshold = min(max(1, INLINE_PAYLOAD_THRESHOLD_BYTES), runtime_payload_hard)`
   - `runtime_result_hard = max(1, INLINE_RESULT_HARD_LIMIT_BYTES)`
   - `runtime_result_threshold = min(max(1, INLINE_RESULT_THRESHOLD_BYTES), runtime_result_hard)`
2. profile 名义值
   - `profile_payload_hard = max(1, profile.inline_payload_hard_limit_bytes)`
   - `profile_payload_threshold = min(max(1, profile.inline_payload_threshold_bytes), profile_payload_hard)`
   - `profile_result_hard = max(1, profile.inline_result_hard_limit_bytes)`
   - `profile_result_threshold = min(max(1, profile.inline_result_threshold_bytes), profile_result_hard)`
3. effective policy 合并
   - `final_payload_threshold = min(runtime_payload_threshold, profile_payload_threshold)`
   - `final_payload_hard = min(runtime_payload_hard, profile_payload_hard)`
   - `final_result_threshold = min(runtime_result_threshold, profile_result_threshold)`
   - `final_result_hard = min(runtime_result_hard, profile_result_hard)`
4. object threshold 继续收紧 payload inline 分流线
   - 当 `object_threshold_bytes > 0`：
   - `final_payload_threshold = min(final_payload_threshold, object_threshold_bytes)`
5. payload inline 决策
   - `cheap_estimate(payload) > final_payload_threshold` 时，直接走 `DataRef` / objectify
   - 否则尝试 inline 编码
   - inline 编码后的真实大小必须 `<= final_payload_hard`
6. result inline 决策
   - `cheap_estimate(result) > final_result_threshold` 时，直接走 object / `DataRef`
   - 否则尝试 inline 编码
   - inline 编码后的真实大小必须 `<= final_result_hard`

因此，profile 里的 limit 不是单独生效的最终值。最终 inline limit 永远是 `runtime`、`profile/effective policy` 和可选 `object_threshold_bytes` 取更严格值后的结果。比如 `trusted_internal` profile 的名义值可以比 runtime 大，但最终仍会被 `min(runtime, profile)` 拉回 runtime 边界。

1. runtime payload threshold
   - `payload_hard = max(1, INLINE_PAYLOAD_HARD_LIMIT_BYTES)`
   - `payload_threshold = min(max(1, INLINE_PAYLOAD_THRESHOLD_BYTES), payload_hard)`
2. runtime result threshold
   - `result_hard = max(1, INLINE_RESULT_HARD_LIMIT_BYTES)`
   - `result_threshold = min(max(1, INLINE_RESULT_THRESHOLD_BYTES), result_hard)`
3. local inline payload limit
   - `local_hard = max(1, LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES)`
   - `local_threshold = min(max(1, LOCAL_INLINE_PAYLOAD_THRESHOLD_BYTES), local_hard)`
4. policy profile threshold
   - `profile_threshold = min(max(1, profile.inline_payload_threshold_bytes), profile.inline_payload_hard_limit_bytes)`
   - `profile_result_threshold = min(max(1, profile.inline_result_threshold_bytes), profile.inline_result_hard_limit_bytes)`
   - `profile_result_hard = max(1, profile.inline_result_hard_limit_bytes)`
5. effective policy
   - `effective = resolve_effective_policy(profile, requested_mode, context)`
   - `effective.inline_payload_threshold_bytes = min(profile_threshold, runtime_threshold_from_base)`
   - `effective.inline_payload_hard_limit_bytes = min(profile_hard, runtime_hard_from_base)`
   - `effective.inline_result_threshold_bytes = min(profile_result_threshold, runtime_result_threshold_from_base)`
   - `effective.inline_result_hard_limit_bytes = min(profile_result_hard, runtime_result_hard_from_base)`
6. runtime payload policy merge
   - `resolve_payload_policy(mode, effective_policy, object_threshold_bytes)`
   - `base_policy = get_payload_policy(mode)`
   - `merged_policy = merge_payload_limits_with_effective_policy(base_policy.limits, effective_policy)` when `effective_policy` exists
   - `final.inline_payload_threshold_bytes = min(merged_policy.inline_payload_threshold_bytes, object_threshold_bytes)` when `object_threshold_bytes > 0`
   - `final.inline_payload_hard_limit_bytes = merged_policy.inline_payload_hard_limit_bytes`
   - `final.inline_result_threshold_bytes = merged_policy.inline_result_threshold_bytes`
   - `final.inline_result_hard_limit_bytes = merged_policy.inline_result_hard_limit_bytes`
7. binding payload thresholds
   - `get_binding_payload_thresholds(binding_id, requested_mode, context)` returns
     `(effective.inline_payload_threshold_bytes, effective.inline_payload_hard_limit_bytes, effective.inline_result_threshold_bytes, effective.inline_result_hard_limit_bytes)`
8. object size hard limit
   - `object_size_hard_limit = max(1, OBJECT_SIZE_HARD_LIMIT_BYTES)`
9. bytes materialize threshold
   - `bytes_materialize_threshold = max(1, min(BYTES_MATERIALIZE_THRESHOLD_BYTES, object_size_hard_limit))`
10. NodeControl HTTP body limit
   - `node_control_http_body_limit = max(1, NODE_CONTROL_HTTP_BODY_MAX_BYTES)`
   - `/objects/...` object path 也使用这条 body 上限；object 不再拥有单独更大的 HTTP body 后门
11. gateway upload limits
   - `file_limit = max(1, max_file_bytes or GATEWAY_MAX_UPLOAD_FILE_BYTES)`
   - `total_limit = max(file_limit, max_total_bytes or GATEWAY_MAX_UPLOAD_TOTAL_BYTES)`
12. managed globals inline batch limit
   - `managed_globals_limit = max(1, min(policy_hard_limit_bytes, node_control_http_body_limit))`
   - managed globals 属于任务会话数据，不再被轻控制消息的 `CONTROL_HTTP_MAX_SEND_BYTES` 卡住
13. inline payload request validation
   - `validate_inline_request_size(size, limit_bytes=0)` 实际使用的默认 limit 是 `get_payload_policy("http_call").inline_payload_hard_limit_bytes`
   - 现在没有单独的 request limit 主概念，request 校验和 payload hard limit 共用同一条硬边界
14. inline result validation
   - `validate_inline_result_size(size, limit_bytes=0)` 实际使用的默认 limit 是 `get_payload_policy("result").inline_result_hard_limit_bytes`

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
   - 负责 NodeControl runtime/control 和 object HTTP path 共用的 body bound 默认值和下限修正
7. `get_service_http_body_limit_bytes(...)` / `get_gateway_http_body_limit_bytes(...)` / `get_infocenter_http_body_limit_bytes(...)`
   - 负责各 HTTP server 的 body bound 默认值和下限修正
8. `get_gateway_upload_limits(...)`
   - 负责 gateway upload 文件/总量 limit 的默认值和总量下限修正
9. `get_object_size_hard_limit_bytes(...)` / `validate_object_size_bytes(...)`
   - 负责单个 object/DataRef 背后对象大小的业务硬限制
   - 不等同于 HTTP body limit，也不等同于 segment layout 阈值
10. `get_bytes_materialize_threshold_bytes(...)` / `validate_bytes_materialize_size(...)`
   - 负责整包 bytes 下载、`materialize_as="bytes"` 和整包反序列化路径的内存保护
   - 不等同于 object size hard limit；大对象可以存在，但不能走 bytes 主路径
11. `get_managed_globals_inline_limit_bytes(...)`
   - 负责 managed globals 的 policy hard limit 与 node runtime body bound 合成
   - managed globals 属于任务会话数据面，不使用轻控制消息的 `CONTROL_HTTP_MAX_SEND_BYTES`
12. `get_job_staging_replica_count(...)`
   - 负责 job staged refs 的副本数默认值和下限修正
13. `get_job_staged_ref_ttl_sec(...)`
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
