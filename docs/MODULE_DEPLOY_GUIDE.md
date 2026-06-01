# 模块对象部署说明

当前模块对象自动打包仍然保留，但推荐入口已经变化：

## 1. 服务侧

推荐：

```python
import my_job.main
from pycloud_parallel import Service

group = Service.deploy(
    target="127.0.0.1:50051",
    source=my_job.main,
    runtime="py3",
)
```

适合：

1. 内部常驻函数服务
2. 模块 / package 级别部署
3. 需要自动收集本地 Python 依赖闭包

## 2. 任务侧

当前不再推荐旧的共享任务池入口。

推荐改成：

1. `TaskPool.open(...)`
   - 直接创建原生专属 pool 执行 subtasks
2. `JobQueue.submit(source=...)`
   - 大任务先排队，排到后再自动创建 `TaskPool`

## 3. 资源文件边界

如果你有共享静态数据文件：

1. 不要依赖模块对象自动打包把资源文件一起带上
2. 当前自动打包只会收 `.py / .pyd / .so`
3. `.csv / .json / README / docs` 等非 Python 文件都不会自动进入包
4. 如果必须带资源文件，请预先自行构建 `zip / tar.gz / whl`，再通过 `source=<archive file>` 上传
5. 代码里如果仍要走相对路径，建议在你自己构建的归档里保留所需目录结构

## 3.1 项目根 Python 模块

自动打包会处理项目根目录下的 Python 模块和 package 闭包。例如业务模块引用：

```python
from Api import local_db_api
```

而 `Api/local_db_api.py` 在运行到某个函数时才执行：

```python
import DBCfg
```

只要 `DBCfg.py` 位于同一项目根目录，自动打包会把它放在 artifact 根目录。节点加载和实际调用方法时都会保留该 artifact 根目录到 `sys.path`，因此延迟导入的根模块也能解析。

如果远端仍报 `ModuleNotFoundError: No module named 'DBCfg'`，优先确认：

1. 本地自动打包产物里是否有 `DBCfg.py`
2. 目标 node 是否已经升级到包含该运行时修复的 `pycloud_parallel`
3. 重新部署服务后，错误里的 `code_cache/codes/<code_version>/pkg` 是否对应新 artifact

## 4. 本地调试模块打包

可以直接用调试脚本查看自动打包结果：

```bash
python scripts/debug_package_module.py my_job.main
```

输出：

1. 本地生成的 `tar.gz` 路径
2. 包内文件清单
3. 同目录下的 `*.contents.txt` manifest

## 5. 已移除

以下旧入口已移除：

1. 共享任务池相关旧客户端入口
2. 共享任务池相关任务提交流程
