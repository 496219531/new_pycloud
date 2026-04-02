"""PyCloud control-plane components.

Imports are intentionally lazy to avoid side effects on module startup.
"""

from __future__ import annotations

import importlib
from typing import Any

_CLIENT_EXPORTS = {
    "pycloud_export",
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
}

_CONTROLPLANE_DEP_HINT = (
    "Control-plane dependencies are missing. "
    'Reinstall with `pip install pycloud-parallel` (or avoid `--no-deps`).'
)


def _import_client_module() -> Any:
    try:
        return importlib.import_module(".client", __name__)
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing == "grpc" or missing == "google" or missing.startswith("google."):
            raise ModuleNotFoundError(_CONTROLPLANE_DEP_HINT) from exc
        raise


def __getattr__(name: str):
    if name in _CLIENT_EXPORTS:
        module = _import_client_module()
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | _CLIENT_EXPORTS)


__all__ = [
    "pycloud_export",
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
