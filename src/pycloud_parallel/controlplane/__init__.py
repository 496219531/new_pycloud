"""PyCloud control-plane components.

Imports are intentionally lazy to avoid side effects on module startup.
"""

from .client import (
    pycloud_export,
    # 新命名（推荐）
    DeployedService,
    DirectConnect,
    GatewayConnect,
    TaskSubmitter,
    # 旧命名（向后兼容）
    DiscoveryModuleClient,
    DiscoveryServiceClient,
    GatewayServiceClient,
    GatewayModuleClient,
    InfoCenterClient,
    InfoCenterNode,
    InfoCenterServiceRoute,
    ServiceGroup,
    ServiceModuleGroup,
    TaskBatchClient,
    TaskModuleClient,
)

__all__ = [
    # 新命名（推荐）
    "DeployedService",
    "DirectConnect",
    "GatewayConnect",
    "TaskSubmitter",
    # 旧命名（向后兼容）
    "DiscoveryModuleClient",
    "DiscoveryServiceClient",
    "GatewayServiceClient",
    "GatewayModuleClient",
    "InfoCenterClient",
    "InfoCenterNode",
    "InfoCenterServiceRoute",
    "ServiceGroup",
    "ServiceModuleGroup",
    "TaskBatchClient",
    "TaskModuleClient",
]
