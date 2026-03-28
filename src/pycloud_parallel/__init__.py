"""中文说明：主包导出层，集中暴露公共 API 和配置/结果类型。"""

from .api import configure, foreach, last_errors, metrics, parallel_for, project
from .config import ClusterConfig, ProjectConfig, RuntimeConfig
from .types import ForeachResult, TaskError

__all__ = [
    "ClusterConfig",
    "ProjectConfig",
    "RuntimeConfig",
    "ForeachResult",
    "TaskError",
    "configure",
    "foreach",
    "last_errors",
    "metrics",
    "parallel_for",
    "project",
]
