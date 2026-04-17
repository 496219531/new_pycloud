# 自动依赖打包说明

当前系统仍然支持：

1. 函数对象自动打包
2. 模块对象自动打包
3. 本地 Python 模块 / package 自动闭包打包

但任务侧入口已经更新：

## 当前推荐入口

1. `Service.deploy(func=...)`
2. `Service.deploy(entry_module=<module object>)`
3. `TaskPool.open(...)`
4. `JobQueue.submit_job_from_bytes(...)`

## 依赖策略

当前仍保持保守规则：

1. 自动打包只收本地 Python 文件依赖
2. 第三方依赖如果目标节点缺失，仍建议显式传 `dependency_allowlist`
3. 不做盲目自动安装

## 当前自动打包边界

自动打包当前只会收以下文件：

1. `.py`
2. `.pyd`
3. `.so`

不会自动带上的内容：

1. `.csv` / `.json` / `.yaml` / `.toml`
2. `README` / `docs`
3. 图片、日志、shell 脚本
4. 其他非 Python 资源文件

## 模块对象打包规则

如果你传的是“已经加载好的模块对象”：

1. 系统会直接基于已加载 module object 和真实 `__file__` 分析依赖
2. 不再先退回 `module.__name__` 再靠模块名猜文件
3. 最终按“精确文件集合”写入 `tar.gz`
4. 不再按仓库根目录或 package 根目录粗暴递归整树打包

这意味着 `submit_job_from_module(module=...)` / `entry_module=<module object>` 这类入口现在更适合做稳定的本地源码闭包打包。

## 非 Python 资源如何处理

如果业务必须依赖 `.csv` 等非 Python 文件，不要依赖自动打包。

建议做法：

1. 预先自行构建 `zip / tar.gz / whl`
2. 再通过 `artifact_path=<archive file>` 或 `blob=...` 上传
3. 第三方依赖继续通过 `dependency_allowlist` 解决

## 本地调试打包结果

可以直接在仓库根目录运行：

```bash
python scripts/debug_package_module.py calc_asset_ratio_job_module
```

它会：

1. 在 `/tmp/new_pycloud_package_debug/` 下生成本地 `tar.gz`
2. 同时写出一个 `*.contents.txt` 清单
3. 方便直接检查这次自动打包到底带了哪些文件

## 已移除

以下旧入口已移除：

1. 共享任务池相关旧客户端入口

如果你需要任务执行：

1. 直接执行一批 subtasks：使用 `TaskPool`
2. 先排队再执行：使用 `JobQueue`
