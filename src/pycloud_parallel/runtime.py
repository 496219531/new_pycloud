from __future__ import annotations

"""中文说明：运行时编排层。

负责把配置、网关、项目管理和执行器组装起来，
并提供全局 Runtime 单例（便于业务代码低侵入接入）。
"""

import threading
from dataclasses import dataclass, field
from typing import Dict, Optional

from .config import ProjectConfig, RuntimeConfig, load_runtime_config
from .executor import run_foreach
from .gateway import ClusterGateway
from .project_manager import ProjectManager
from .types import ForeachResult


@dataclass
class RuntimeMetrics:
    """运行时指标数据类。

    用于跟踪运行时的任务执行统计信息。
    """
    submitted_jobs: int = 0  # 已提交的任务总数
    succeeded_jobs: int = 0  # 成功完成的任务数
    failed_jobs: int = 0  # 失败的任务数


class Runtime:
    """PyCloud 运行时核心类。

    负责编排配置、网关、项目管理和执行器，提供统一的并行执行接口。
    采用单例模式，确保全局只有一个运行时实例。

    Attributes:
        config: 运行时配置
        gateway: 集群网关，负责多集群调度
        projects: 项目管理器，负责项目级资源隔离
        metrics: 运行时指标
        _metrics_lock: 指标锁，保护并发访问
        _last_errors: 线程本地存储，保存每个线程的最后错误
    """

    def __init__(self, config: RuntimeConfig) -> None:
        self.config = config
        self.gateway = ClusterGateway(config.clusters)
        self.projects = ProjectManager(config.projects)
        self.metrics = RuntimeMetrics()
        self._metrics_lock = threading.Lock()
        self._last_errors = threading.local()

    def shutdown(self) -> None:
        """关闭运行时，释放所有资源。

        会关闭所有集群连接，清理资源。
        """
        self.gateway.shutdown()

    def register_project(self, cfg: ProjectConfig) -> None:
        """注册一个新项目。

        Args:
            cfg: 项目配置对象
        """
        self.projects.register(cfg)

    def foreach(
        self,
        iterable,
        fn,
        *,
        mode: str,
        on_error: Optional[str],
        retries: Optional[int],
        project: Optional[str],
        cluster_policy: str,
        chunk_size: Optional[int],
    ) -> ForeachResult:
        """并行执行 foreach 操作。

        这是运行时的核心方法，负责协调各个组件完成并行执行。

        Args:
            iterable: 可迭代对象
            fn: 要应用的函数
            mode: 返回模式（"ordered" 或 "as_completed"）
            on_error: 错误处理策略
            retries: 重试次数
            project: 项目名称
            cluster_policy: 集群选择策略
            chunk_size: 分片大小

        Returns:
            ForeachResult: 包含结果和错误的对象
        """
        # 默认���略可由项目配置兜底：on_error/retries 支持按项目继承。
        project_name = project or self.config.default_project
        self.projects.ensure(project_name, default_cpu=1)
        project_cfg = self.projects.get(project_name)
        effective_on_error = on_error or project_cfg.default_on_error or "skip"
        effective_retries = project_cfg.default_retries if retries is None else retries
        with self._metrics_lock:
            self.metrics.submitted_jobs += 1
        try:
            result = run_foreach(
                iterable=iterable,
                fn=fn,
                gateway=self.gateway,
                projects=self.projects,
                mode=mode,
                on_error=effective_on_error,
                retries=effective_retries,
                project=project_name,
                cluster_policy=cluster_policy,
                chunk_size=chunk_size,
            )
        except Exception:
            with self._metrics_lock:
                self.metrics.failed_jobs += 1
            raise
        else:
            with self._metrics_lock:
                self.metrics.succeeded_jobs += 1
            self._last_errors.value = result.errors
            return result

    def get_last_errors(self):
        """获取最后一次 foreach 调用的错误列表。

        Returns:
            List[TaskError]: 错误列表
        """
        return getattr(self._last_errors, "value", [])

    def snapshot_metrics(self) -> Dict[str, int]:
        """获取运行时指标的快照。

        Returns:
            Dict[str, int]: 包含 submitted_jobs, succeeded_jobs, failed_jobs 的字典
        """
        with self._metrics_lock:
            return {
                "submitted_jobs": self.metrics.submitted_jobs,
                "succeeded_jobs": self.metrics.succeeded_jobs,
                "failed_jobs": self.metrics.failed_jobs,
            }


_RUNTIME_LOCK = threading.Lock()
_RUNTIME: Optional[Runtime] = None


def get_runtime(config_path: Optional[str] = None) -> Runtime:
    """获取运行时单例实例。

    采用懒加载模式，首次调用时才初始化资源。

    Args:
        config_path: 可选的配置文件路径

    Returns:
        Runtime: 运行时实例
    """
    # 懒加载单例：首次调用才真正初始化资源。
    global _RUNTIME
    with _RUNTIME_LOCK:
        if _RUNTIME is None:
            _RUNTIME = Runtime(load_runtime_config(config_path))
        return _RUNTIME


def configure_runtime(
    *,
    config: Optional[RuntimeConfig] = None,
    config_path: Optional[str] = None,
    reset: bool = True,
) -> Runtime:
    """配置或重新配置运行时。

    Args:
        config: 可选的运行时配置对象
        config_path: 可选的配置文件路径
        reset: 是否重置现有运行时（默认 True）

    Returns:
        Runtime: 配置好的运行时实例
    """
    # reset=True 时会关闭旧资源并重建，便于测试和热更新配置。
    global _RUNTIME
    with _RUNTIME_LOCK:
        if _RUNTIME is not None and reset:
            _RUNTIME.shutdown()
            _RUNTIME = None
        if _RUNTIME is None:
            cfg = config or load_runtime_config(config_path)
            _RUNTIME = Runtime(cfg)
        return _RUNTIME
