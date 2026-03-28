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
    submitted_jobs: int = 0
    succeeded_jobs: int = 0
    failed_jobs: int = 0


class Runtime:
    def __init__(self, config: RuntimeConfig) -> None:
        self.config = config
        self.gateway = ClusterGateway(config.clusters)
        self.projects = ProjectManager(config.projects)
        self.metrics = RuntimeMetrics()
        self._metrics_lock = threading.Lock()
        self._last_errors = threading.local()

    def shutdown(self) -> None:
        self.gateway.shutdown()

    def register_project(self, cfg: ProjectConfig) -> None:
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
        # 默认策略可由项目配置兜底：on_error/retries 支持按项目继承。
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
        return getattr(self._last_errors, "value", [])

    def snapshot_metrics(self) -> Dict[str, int]:
        with self._metrics_lock:
            return {
                "submitted_jobs": self.metrics.submitted_jobs,
                "succeeded_jobs": self.metrics.succeeded_jobs,
                "failed_jobs": self.metrics.failed_jobs,
            }


_RUNTIME_LOCK = threading.Lock()
_RUNTIME: Optional[Runtime] = None


def get_runtime(config_path: Optional[str] = None) -> Runtime:
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
