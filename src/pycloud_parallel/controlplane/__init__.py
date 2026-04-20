"""PyCloud control-plane infrastructure package."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

_INFRA_EXPORTS = {
    "DiscoveryServiceClient",
    "EffectivePolicy",
    "GatewayServiceClient",
    "InfoCenterClient",
    "InfoCenterNode",
    "InfoCenterNodeService",
    "InfoCenterNodeTaskPool",
    "InfoCenterServiceRoute",
    "NodeCircuitState",
    "NodeCapability",
    "NodeControlClient",
    "PolicyProfile",
}

_CONTROLPLANE_DEP_HINT = (
    "Control-plane dependencies are missing. "
    'Reinstall with `pip install pycloud-parallel` (or avoid `--no-deps`).'
)


if TYPE_CHECKING:
    from .effective_policy import EffectivePolicy
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
    from .node_capability import NodeCapability
    from .node_control_client import NodeControlClient
    from .policy_profile import PolicyProfile


def _try_bind_infra_exports() -> None:
    try:
        from .effective_policy import EffectivePolicy
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
        from .node_capability import NodeCapability
        from .node_control_client import NodeControlClient
        from .policy_profile import PolicyProfile
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing in {"grpc", "google", "protobuf"} or missing.startswith("google."):
            return
        raise

    globals().update(
        {
            "DiscoveryServiceClient": DiscoveryServiceClient,
            "EffectivePolicy": EffectivePolicy,
            "GatewayServiceClient": GatewayServiceClient,
            "InfoCenterClient": InfoCenterClient,
            "InfoCenterNode": InfoCenterNode,
            "InfoCenterNodeService": InfoCenterNodeService,
            "InfoCenterNodeTaskPool": InfoCenterNodeTaskPool,
            "InfoCenterServiceRoute": InfoCenterServiceRoute,
            "NodeCircuitState": NodeCircuitState,
            "NodeCapability": NodeCapability,
            "NodeControlClient": NodeControlClient,
            "PolicyProfile": PolicyProfile,
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
    "EffectivePolicy",
    "GatewayServiceClient",
    "InfoCenterClient",
    "InfoCenterNode",
    "InfoCenterNodeService",
    "InfoCenterNodeTaskPool",
    "InfoCenterServiceRoute",
    "NodeCircuitState",
    "NodeCapability",
    "NodeControlClient",
    "PolicyProfile",
]
