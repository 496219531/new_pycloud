# Runtime 参数的当前语义

## 1. 结论

`runtime` 现在已经有明确作用：

1. 它表示 Python 版本约束
2. 客户端发现节点时会先按它筛选
3. 节点侧上传代码和创建服务时会再做一次本地校验

它已经不再只是“标签”。

## 2. 支持的写法

当前只支持轻量表达式：

1. `py3`
2. `py3.11`
3. `>=py3.11`
4. `>py3.11`
5. `<=py3.11`
6. `<py3.11`

也接受简写：

1. `3`
2. `3.11`
3. `>=3.11`

归一化后会变成：

1. `3` -> `py3`
2. `3.11` -> `py3.11`
3. `>=3.11` -> `>=py3.11`

## 3. 语义规则

### 3.1 精确版本

```python
runtime="py3.11"
```

只匹配 Python 3.11。

不会匹配：

1. Python 3.10
2. Python 3.12
3. Python 3.13

### 3.2 大版本

```python
runtime="py3"
```

匹配所有 Python 3 节点。

### 3.3 比较表达式

```python
runtime=">=py3.11"
```

匹配 Python 3.11 及以上。

如果你想表达“3.11 及以上”，必须显式写比较符。

## 4. 当前生效链路

### 4.1 任务模式

```python
TaskSubmitter.from_infocenter(..., runtime=">=py3.11")
TaskBatchClient.from_infocenter(..., runtime="py3")
```

行为：

1. `InfoCenterClient.select_task_nodes(...)` 先按节点 `python_version` 过滤
2. 选中的节点再接收上传代码
3. 节点侧 `put_code(...)` 再做一次版本校验

### 4.2 服务模式

```python
DeployedService.deploy_from_infocenter(..., runtime="py3.11")
```

行为：

1. owner 侧先按 `InfoCenter` 的节点 `python_version` 过滤
2. 只有符合约束的节点会尝试部署
3. NodeControl `create_service(...)` 内部还会再次校验

## 5. 节点如何暴露 Python 版本

当前 `NodeControl` 会把自己的 Python 版本上报到 `InfoCenter`。

常见字段值例如：

1. `py3.10`
2. `py3.11`
3. `py3.13`

这些字段会出现在：

1. `GET /nodes`
2. `InfoCenterClient.list_nodes(...)`
3. Web 运维页 `/ops`
4. `scripts/start_services.sh status`

## 6. 与 `runtime_key` 的区别

这两个字段不要混淆：

1. `runtime`
   - Python 版本约束
   - 用于选点和节点校验
2. `runtime_key`
   - 热点粘性键
   - 用于任务模式 runtime slot 复用

例子：

```python
with TaskSubmitter.from_infocenter(
    infocenter_target="127.0.0.1:50051",
    blob=blob,
    filename="task_demo.py",
    runtime=">=py3.11",
    entry_module="task_demo",
) as task:
    results = task.run(value=7, runtime_key="alpha-hot")
```

这里：

1. `runtime=">=py3.11"` 决定“能在哪些节点跑”
2. `runtime_key="alpha-hot"` 决定“同类任务尽量复用哪个热 slot”

## 7. 实用建议

推荐默认策略：

1. 普通示例和通用任务写 `runtime="py3"`
2. 只有明确依赖某个 Python 次版本时，才写精确版本
3. 如果要表达下限，用比较表达式，例如 `>=py3.11`

## 8. 当前不支持的内容

暂不支持复杂版本语法：

1. `~=3.11`
2. `^3.11`
3. `>=3.11,<3.13`
4. 多条件逗号表达式

当前故意保持轻量，避免把解析器做复杂。
