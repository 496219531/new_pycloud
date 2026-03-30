"""中文说明：主包导出层，集中暴露公共 API 和配置/结果类型。"""

from .local_runtime.api import configure, foreach, last_errors, metrics, parallel_for, project
from .local_runtime.config import ProjectConfig, RuntimeConfig
from .local_runtime.runtime import configure_runtime, get_runtime
from .local_runtime.types import ForeachResult, TaskError

__all__ = [
    "ProjectConfig",
    "RuntimeConfig",
    "ForeachResult",
    "TaskError",
    "get_runtime",
    "configure_runtime",
    "configure",
    "foreach",
    "last_errors",
    "metrics",
    "parallel_for",
    "project",
]
