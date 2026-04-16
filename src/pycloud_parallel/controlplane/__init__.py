"""PyCloud control-plane infrastructure package."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

_INFRA_EXPORTS = {
    "DiscoveryServiceClient",
    "GatewayServiceClient",
    "InfoCenterClient",
    "InfoCenterNode",
    "InfoCenterNodeService",
    "InfoCenterNodeTaskPool",
    "InfoCenterServiceRoute",
    "NodeCircuitState",
    "NodeControlClient",
}

_CONTROLPLANE_DEP_HINT = (
    "Control-plane dependencies are missing. "
    'Reinstall with `pip install pycloud-parallel` (or avoid `--no-deps`).'
)


if TYPE_CHECKING:
    from .discovery_client import DiscoveryServiceClient
    from .gateway_client import GatewayServiceClient
    from .infocenter_client import (
        InfoCenterClient,
        InfoCenterNode,
        InfoCenterNodeService,
        InfoCenterNodeTaskPool,
        InfoCenterServiceRoute,
        NodeCircuitState,
    )
    from .node_control_client import NodeControlClient


def _try_bind_infra_exports() -> None:
    try:
        from .discovery_client import DiscoveryServiceClient
        from .gateway_client import GatewayServiceClient
        from .infocenter_client import (
            InfoCenterClient,
            InfoCenterNode,
            InfoCenterNodeService,
            InfoCenterNodeTaskPool,
            InfoCenterServiceRoute,
            NodeCircuitState,
        )
        from .node_control_client import NodeControlClient
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing in {"grpc", "google", "protobuf"} or missing.startswith("google."):
            return
        raise

    globals().update(
        {
            "DiscoveryServiceClient": DiscoveryServiceClient,
            "GatewayServiceClient": GatewayServiceClient,
            "InfoCenterClient": InfoCenterClient,
            "InfoCenterNode": InfoCenterNode,
            "InfoCenterNodeService": InfoCenterNodeService,
            "InfoCenterNodeTaskPool": InfoCenterNodeTaskPool,
            "InfoCenterServiceRoute": InfoCenterServiceRoute,
            "NodeCircuitState": NodeCircuitState,
            "NodeControlClient": NodeControlClient,
        }
    )


_try_bind_infra_exports()


def __getattr__(name: str) -> Any:
    if name in _INFRA_EXPORTS and name not in globals():
        _try_bind_infra_exports()
        if name in globals():
            return globals()[name]
        raise ModuleNotFoundError(_CONTROLPLANE_DEP_HINT)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | _INFRA_EXPORTS)


__all__ = [
    "DiscoveryServiceClient",
    "GatewayServiceClient",
    "InfoCenterClient",
    "InfoCenterNode",
    "InfoCenterNodeService",
    "InfoCenterNodeTaskPool",
    "InfoCenterServiceRoute",
    "NodeCircuitState",
    "NodeControlClient",
]
