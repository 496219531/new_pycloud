# HTTP 服务调用调试链路

这份文档只回答一个问题：

1. 一个 HTTP 服务请求是怎样从客户端发出
2. 怎样进入节点执行
3. 怎样把结果再返回给 caller

适合排查：

1. payload 序列化报错
2. Gateway 返回 4xx / 5xx
3. 服务方法没执行到
4. 返回值类型不符合预期

## 1. 常见入口

当前常见 HTTP caller 有两类：

1. gateway route
   - 统一 `Service.connect(..., route="gateway")` 的模块化调用体验
2. `GatewayServiceClient`
   - 显式 HTTP 调用体验，如 `client.call(service_name=..., method=..., payload=...)`

如果你走 gateway route：

1. `Service.connect(..., route="gateway")` 返回统一连接对象
2. 连接对象的 `__getattr__()` 动态返回 `_CallProxy`
3. `_SyncCallProxy.__call__()` 或 `_CallProxy.__call__()` 进入统一连接对象的 `call_balanced()`
4. 再调用父类 `GatewayServiceClient.call()`

关键位置：

1. `src/pycloud_parallel/controlplane/gateway_client.py`
2. 统一连接对象从 `src/pycloud_parallel/execution/service_session.py` 开始
3. `GatewayServiceClient.call()` 在 `1365` 左右

## 2. 客户端发请求

HTTP 请求真正发出前，关键函数顺序是：

1. 统一连接对象的 `call_balanced()`
2. `GatewayServiceClient.call()`
3. `_serialize_http_call_payload()`
4. `_http_json_request()`

其中：

1. `GatewayServiceClient.call()`
   - 组装 `/svc/{service_name}/call/{method}?timeout_sec=...`
   - 先把 payload 送去 `_serialize_http_call_payload()`
2. `_serialize_http_call_payload()`
   - 内部调用 `serialize_inline_payload(...)`
   - 这里会做 pandas / numpy / datetime / DataRef 等 inline 序列化
3. `_http_json_request()`
   - 负责真正的 JSON body 编码和 `urlopen(...)`
   - 这里现在只打 `logger.debug(...)`，默认不会直接 `print`

最适合下断点的点：

1. `GatewayServiceClient.call()`
2. `_serialize_http_call_payload()`
3. `_http_json_request()`

## 3. Gateway 入口

请求到达 HTTP Gateway 后，主入口是：

1. `GatewayHttpApp.handle_post()`

它做的事情：

1. 解析路径 `/svc/{service_name}/call/{method}`
2. `json.loads(body)`
3. 再次调用 `serialize_inline_payload(...)`
   - 这一步相当于网关侧的 payload 校验
4. 从 route cache 选 route
5. 调 `_invoke_route(...)` 把请求转发给目标节点

关键位置：

1. `src/pycloud_parallel/controlplane/gateway_http.py`
2. `handle_post()` 在 `47` 左右

如果问题发生在：

1. body 不是合法 JSON
2. payload 太大
3. route 选不到
4. Gateway 重试失败

大概率都在这一层就能看出来。

## 4. 节点 NodeControl HTTP 入口

Gateway 选到 route 后，会打到 NodeControl 的 `CallService` HTTP 入口：

1. `NodeControlHttpApp` 的 service call handler

这里做的事：

1. 校验 `service_id` / `method`
2. `validate_inline_payload_structs(...)`
3. `struct_to_dict(request.payload)`
4. 调 `self._state.call_service(...)`

关键位置：

1. `src/pycloud_parallel/controlplane/services.py`
2. `CallService()` 在 `1271` 左右

这里非常适合判断：

1. Gateway 有没有成功把请求送到节点
2. NodeControl HTTP 层有没有把 payload 反序列化成功
3. service token 是否正确

## 5. NodeState 内部调用

节点内部服务调用真正落到：

1. `NodeControlState.call_service()`
2. 再进入 `_invoke_service_call(...)`

这里负责：

1. 根据 `service_id` 找 session
2. 校验 `service_token`
3. 找到对应 artifact / method
4. 把任务提交给 executor host 或 service worker

文档里只需要先记住入口：

1. `src/pycloud_parallel/controlplane/nodecontrol_state.py`
2. `NodeControlState.call_service()`

如果想继续往下追用户代码执行，最关键是下一层。

## 6. 用户函数真正执行

真正执行用户方法的重要函数有两个：

1. `_execute_payload_in_subprocess()`
2. `_invoke_user_callable()`

执行顺序可以理解为：

1. 加载 artifact 和 callable router
2. `_resolve_object_refs_in_payload(...)`
3. `_invoke_user_callable(fn, resolved_payload)`
4. `_normalize_user_return(...)`

其中 `_invoke_user_callable()` 负责把 payload 解释成：

1. `{"args": [...], "kwargs": {...}}`
2. 或 HTTP 风格的 `{"x": 1, "y": 2}` -> `fn(**payload)`

关键位置：

1. `src/pycloud_parallel/controlplane/node/execution.py`
2. `_invoke_user_callable()`
3. `_execute_payload_in_subprocess()`
4. `src/pycloud_parallel/controlplane/node/results.py` 中 `_normalize_user_return()`

如果“请求到了，但函数行为不对”，通常就是在这里查：

1. 反序列化后 payload 到底长什么样
2. 最终是 `fn(*args, **kwargs)` 还是 `fn(**payload)`
3. 返回值是否被转成 `DataRef` / DataFrame / ndarray / inline dict

## 7. 返回结果路径

返回方向上，关键分两种：

1. inline 结果
   - 直接放进 `CallServiceResponse.data`
2. 大对象结果
   - 先落成对象文件，再包装成 `DataRef`

客户端侧取结果的关键函数：

1. `_normalize_http_response_body()`
2. `convert_dict_to_arrow(...)`
3. 如果结果里是 `DataRef`
   - `fetch_result_ref_data(...)`
   - `_materialize_downloaded_result(...)`

因此：

1. 你看到的是普通 dict/list/pandas/numpy
2. 还是一个 `DataRef`
3. 还是落盘后的 parquet / npy

都可以沿这条链路判断。

## 8. 建议的断点顺序

排查一次 HTTP 服务调用，建议按这个顺序下断点：

1. `GatewayServiceClient.call()`
2. `_serialize_http_call_payload()`
3. `_http_json_request()`
4. `GatewayHttpApp.handle_post()`
5. `NodeControlService.CallService()`
6. `NodeControlState.call_service()`
7. `_execute_payload_in_subprocess()`
8. `_invoke_user_callable()`

这样能很快判断问题是在：

1. caller 侧序列化
2. Gateway 路由
3. 节点 NodeControl HTTP
4. 用户函数执行
5. 返回值归一化

## 9. 最常见故障点

1. payload inline 序列化失败
   - 先看 `_serialize_http_call_payload()` / `serialize_inline_payload()`
2. Gateway 报 `no available route`
   - 看统一连接对象的 `_validate_service_ready()` 或 route cache
3. 节点报 `service/method not found`
   - 看 `NodeControlService.CallService()` 和 `NodeControlState.call_service()`
4. 函数签名不匹配
   - 看 `_invoke_user_callable()`
5. pandas / numpy 类型往返异常
   - 看 `serialize_arrow_compatible()` 和 `convert_dict_to_arrow()`

## 10. 备注

如果你不是走 `gateway route`，而是走 `discovery route`，前半段会变成：

1. 统一连接对象的 `call_balanced()`
2. route cache 直接选节点 route
3. `_call_route_http(...)`
4. 目标节点 service HTTP

但到 NodeControl 之后，后半段执行链路基本一致。
