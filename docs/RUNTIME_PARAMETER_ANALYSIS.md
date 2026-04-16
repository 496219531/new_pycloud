# Runtime 参数的当前语义

## 1. 结论

`runtime` 当前表示 Python 版本约束：

1. 选点时先按节点 `python_version` 过滤
2. 节点侧上传代码、创建服务、创建 task pool 时再做一次本地校验
3. 它不是标签系统

## 2. 支持的写法

当前支持：

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

## 3. 当前生效链路

### 3.1 Service Mode

```python
Service.deploy_from_infocenter(..., runtime="py3.11")
```

### 3.2 TaskPool Mode

```python
TaskPool.from_infocenter(..., runtime=">=py3.11")
```

### 3.3 JobQueue Mode

`JobQueue` 提交 job 时，如果 driver 后续要创建 `TaskPool`，同样会把 `runtime` 透传到 pool 选点和节点校验链路。

## 4. 与其他字段的区别

### `runtime`
- Python 版本约束
- 决定能在哪些节点运行

### `job_id`
- 大任务队列里的任务标识
- 用于 JobQueue 调度和状态查询

### `pool_id`
- 原生 `TaskPool` 的资源会话标识
- 用于 pool 生命周期、heartbeat、结果拉取和关闭

## 5. 建议

1. 普通示例优先写 `runtime="py3"`
2. 只有明确依赖某个次版本时，再写精确版本或比较表达式
