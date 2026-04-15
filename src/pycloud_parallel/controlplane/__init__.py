"""PyCloud control-plane components.

Imports are intentionally lazy to avoid side effects on module startup.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

_CLIENT_EXPORTS = {
    "Artifact",
    "ArtifactDeps",
    "ArtifactExports",
    "DataRef",
    "ObjectRef",
    "ResultRef",
    "pycloud_export",
    "DeployedService",
    "DirectConnect",
    "GatewayConnect",
    "DedicatedTaskServiceSession",
    "JobQueueClient",
    "TaskPoolSession",
    "DiscoveryServiceClient",
    "GatewayServiceClient",
    "InfoCenterClient",
    "InfoCenterNode",
    "InfoCenterServiceRoute",
    "ServiceGroup",
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


if TYPE_CHECKING:
    from .client import (
        Artifact,
        ArtifactDeps,
        ArtifactExports,
        DataRef,
        DedicatedTaskServiceSession,
        DeployedService,
        DirectConnect,
        DiscoveryServiceClient,
        GatewayConnect,
        GatewayServiceClient,
        InfoCenterClient,
        InfoCenterNode,
        InfoCenterServiceRoute,
        JobQueueClient,
        ObjectRef,
        ResultRef,
        ServiceGroup,
        TaskPoolSession,
        pycloud_export,
    )


def _try_bind_client_exports() -> None:
    try:
        from .client import (
            Artifact,
            ArtifactDeps,
            ArtifactExports,
            DataRef,
            DedicatedTaskServiceSession,
            DeployedService,
            DirectConnect,
            DiscoveryServiceClient,
            GatewayConnect,
            GatewayServiceClient,
            InfoCenterClient,
            InfoCenterNode,
            InfoCenterServiceRoute,
            JobQueueClient,
            ObjectRef,
            ResultRef,
            ServiceGroup,
            TaskPoolSession,
            pycloud_export,
        )
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing == "grpc" or missing == "google" or missing.startswith("google."):
            return
        raise

    globals().update(
        {
            "Artifact": Artifact,
            "ArtifactDeps": ArtifactDeps,
            "ArtifactExports": ArtifactExports,
            "DataRef": DataRef,
            "ObjectRef": ObjectRef,
            "ResultRef": ResultRef,
            "pycloud_export": pycloud_export,
            "DeployedService": DeployedService,
            "DirectConnect": DirectConnect,
            "GatewayConnect": GatewayConnect,
            "DedicatedTaskServiceSession": DedicatedTaskServiceSession,
            "JobQueueClient": JobQueueClient,
            "TaskPoolSession": TaskPoolSession,
            "DiscoveryServiceClient": DiscoveryServiceClient,
            "GatewayServiceClient": GatewayServiceClient,
            "InfoCenterClient": InfoCenterClient,
            "InfoCenterNode": InfoCenterNode,
            "InfoCenterServiceRoute": InfoCenterServiceRoute,
            "ServiceGroup": ServiceGroup,
        }
    )


_try_bind_client_exports()


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
    "ObjectRef",
    "ResultRef",
    "Artifact",
    "ArtifactDeps",
    "ArtifactExports",
    "DataRef",
    "pycloud_export",
    "DeployedService",
    "DirectConnect",
    "GatewayConnect",
    "DedicatedTaskServiceSession",
    "JobQueueClient",
    "TaskPoolSession",
    "DiscoveryServiceClient",
    "GatewayServiceClient",
    "InfoCenterClient",
    "InfoCenterNode",
    "InfoCenterServiceRoute",
    "ServiceGroup",
]
