"""Public API for lightweight local parallel execution."""

from .local_runtime.api import configure, foreach, parallel_for

# ControlPlane 模块化客户端
from .controlplane import (
    pycloud_export,
    # 新命名（推荐）
    DeployedService,
    DirectConnect,
    GatewayConnect,
    TaskSubmitter,
)


def pycloud_export(fn):
    fn.__pycloud_export__ = True
    return fn

__all__ = [
    # Local Runtime
    "ForeachResult",
    "TaskError",
    "configure",
    "foreach",
    "parallel_for",
    # ControlPlane Module Clients（新命名）
    "DeployedService",
    "DirectConnect",
    "GatewayConnect",
    "TaskSubmitter",
]
