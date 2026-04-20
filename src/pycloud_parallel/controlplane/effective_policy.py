from __future__ import annotations

"""Effective-policy resolution from centralized profiles and node capabilities."""

from dataclasses import dataclass, replace
from math import inf
from typing import Iterable, Optional, Sequence, Tuple

from pycloud_parallel.controlplane.config import PayloadPolicy, get_payload_policy
from pycloud_parallel.controlplane.node_capability import NodeCapability, capability_from_candidate
from pycloud_parallel.controlplane.policy_profile import PolicyProfile
from pycloud_parallel.controlplane.serialization_mode import normalize_serialization_mode


@dataclass(frozen=True)
class EffectivePolicy:
    policy_id: str
    version: int
    resolved_mode: str
    allowed_modes: Tuple[str, ...]
    inline_payload_soft_limit_bytes: int
    inline_payload_hard_limit_bytes: int
    inline_result_hard_limit_bytes: int
    use_transport_payload_bytes: bool
    use_http_bytes_transport: bool
    allow_pickle_stable: bool

    def __post_init__(self) -> None:
        normalized_allowed = tuple(normalize_serialization_mode(item) for item in self.allowed_modes if str(item or "").strip())
        normalized_mode = normalize_serialization_mode(self.resolved_mode)
        if not normalized_allowed:
            raise ValueError("allowed_modes must not be empty")
        if not normalized_mode:
            normalized_mode = normalized_allowed[0]
        if normalized_mode not in normalized_allowed:
            raise ValueError(f"resolved_mode must be allowed by effective policy: {normalized_mode!r}")
        object.__setattr__(self, "policy_id", str(self.policy_id or "").strip().lower())
        object.__setattr__(self, "version", max(1, int(self.version or 1)))
        object.__setattr__(self, "resolved_mode", normalized_mode)
        object.__setattr__(self, "allowed_modes", normalized_allowed)
        object.__setattr__(self, "inline_payload_soft_limit_bytes", max(1, int(self.inline_payload_soft_limit_bytes or 1)))
        object.__setattr__(self, "inline_payload_hard_limit_bytes", max(1, int(self.inline_payload_hard_limit_bytes or 1)))
        object.__setattr__(self, "inline_result_hard_limit_bytes", max(1, int(self.inline_result_hard_limit_bytes or 1)))
        object.__setattr__(self, "use_transport_payload_bytes", bool(self.use_transport_payload_bytes))
        object.__setattr__(self, "use_http_bytes_transport", bool(self.use_http_bytes_transport))
        object.__setattr__(self, "allow_pickle_stable", bool(self.allow_pickle_stable))

    def assert_frozen_mode(self, request_mode: str = "") -> str:
        normalized_request = normalize_serialization_mode(request_mode)
        if normalized_request and normalized_request != self.resolved_mode:
            raise ValueError(
                f"serialization_mode is frozen for this session: requested={normalized_request!r}, "
                f"resolved={self.resolved_mode!r}"
            )
        return self.resolved_mode


def _iter_capabilities(values: Iterable[NodeCapability | object]) -> Tuple[NodeCapability, ...]:
    out = []
    for item in values:
        capability = capability_from_candidate(item)
        if capability is not None:
            out.append(capability)
    return tuple(out)


def _context_transport_limit(capabilities: Sequence[NodeCapability], *, context: str) -> float:
    normalized_context = str(context or "").strip().lower()
    if not capabilities:
        return inf
    if normalized_context in {"gateway_public", "service_connect", "http_call", "jobqueue_session"}:
        return min(cap.http_payload_limit_bytes() for cap in capabilities)
    return min(cap.grpc_payload_limit_bytes() for cap in capabilities)


def _allowed_modes_for_context(profile: PolicyProfile, *, context: str) -> Tuple[str, ...]:
    normalized_context = str(context or "").strip().lower()
    allowed = list(profile.allowed_modes)
    if not profile.allow_pickle_stable or (normalized_context == "gateway_public" and not profile.public_gateway_allow_pickle):
        allowed = [mode for mode in allowed if mode != "pickle_stable_v1"]
    if not allowed:
        raise ValueError(
            f"policy profile {profile.policy_id!r} does not allow any serialization modes for context={normalized_context!r}"
        )
    return tuple(allowed)


def resolve_effective_policy(
    profile: PolicyProfile,
    candidate_capabilities: Sequence[NodeCapability | object],
    requested_mode: str = "",
    context: str = "",
) -> EffectivePolicy:
    capabilities = _iter_capabilities(candidate_capabilities)
    allowed_by_profile = set(_allowed_modes_for_context(profile, context=context))
    if capabilities:
        supported_by_all = set(capabilities[0].supported_modes)
        for capability in capabilities[1:]:
            supported_by_all &= set(capability.supported_modes)
        allowed_modes = tuple(mode for mode in profile.allowed_modes if mode in allowed_by_profile and mode in supported_by_all)
    else:
        allowed_modes = tuple(mode for mode in profile.allowed_modes if mode in allowed_by_profile)
    if not allowed_modes:
        raise ValueError(
            f"no common serialization mode for profile={profile.policy_id!r} context={context!r} "
            f"across candidate node capabilities"
        )

    normalized_request_mode = normalize_serialization_mode(requested_mode)
    if normalized_request_mode:
        if normalized_request_mode not in allowed_modes:
            raise ValueError(
                f"requested_mode={normalized_request_mode!r} is not allowed by profile={profile.policy_id!r} "
                f"for context={context!r}; allowed={list(allowed_modes)}"
            )
        resolved_mode = normalized_request_mode
    elif profile.default_mode in allowed_modes:
        resolved_mode = profile.default_mode
    else:
        resolved_mode = allowed_modes[0]

    transport_limit = _context_transport_limit(capabilities, context=context)
    inline_payload_hard_limit_bytes = min(
        int(profile.inline_payload_hard_limit_bytes),
        int(transport_limit) if transport_limit != inf else int(profile.inline_payload_hard_limit_bytes),
    )
    inline_payload_soft_limit_bytes = min(int(profile.inline_payload_soft_limit_bytes), inline_payload_hard_limit_bytes)
    inline_result_hard_limit_bytes = min(
        int(profile.inline_result_hard_limit_bytes),
        int(transport_limit) if transport_limit != inf else int(profile.inline_result_hard_limit_bytes),
    )
    use_transport_payload_bytes = bool(profile.prefer_transport_payload_bytes) and (
        (not capabilities) or all(cap.supports_transport_payload_bytes for cap in capabilities)
    )
    use_http_bytes_transport = use_transport_payload_bytes and (
        str(context or "").strip().lower() in {"gateway_public", "service_connect", "http_call", "jobqueue_session"}
    ) and ((not capabilities) or all(cap.supports_http_bytes_transport for cap in capabilities))
    allow_pickle_stable = "pickle_stable_v1" in allowed_modes and bool(profile.allow_pickle_stable)

    return EffectivePolicy(
        policy_id=profile.policy_id,
        version=profile.version,
        resolved_mode=resolved_mode,
        allowed_modes=allowed_modes,
        inline_payload_soft_limit_bytes=inline_payload_soft_limit_bytes,
        inline_payload_hard_limit_bytes=inline_payload_hard_limit_bytes,
        inline_result_hard_limit_bytes=inline_result_hard_limit_bytes,
        use_transport_payload_bytes=use_transport_payload_bytes,
        use_http_bytes_transport=use_http_bytes_transport,
        allow_pickle_stable=allow_pickle_stable,
    )


def payload_policy_from_effective_policy(mode: str, effective_policy: Optional[EffectivePolicy]) -> PayloadPolicy:
    base = get_payload_policy(mode)  # type: ignore[arg-type]
    if effective_policy is None:
        return base
    limits = replace(
        base.limits,
        inline_payload_soft_limit_bytes=min(
            int(base.inline_payload_soft_limit_bytes),
            int(effective_policy.inline_payload_soft_limit_bytes),
        ),
        inline_payload_hard_limit_bytes=min(
            int(base.inline_payload_hard_limit_bytes),
            int(effective_policy.inline_payload_hard_limit_bytes),
        ),
        inline_result_hard_limit_bytes=min(
            int(base.inline_result_hard_limit_bytes),
            int(effective_policy.inline_result_hard_limit_bytes),
        ),
    )
    return replace(base, limits=limits)


def should_use_transport_payload_bytes(
    *,
    mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> bool:
    if effective_policy is not None:
        return bool(effective_policy.use_transport_payload_bytes)
    from pycloud_parallel.controlplane.serialization import prefers_transport_payload_bytes

    return bool(prefers_transport_payload_bytes(mode))


def should_use_http_bytes_transport(
    *,
    mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> bool:
    if effective_policy is not None:
        return bool(effective_policy.use_http_bytes_transport)
    return should_use_transport_payload_bytes(
        mode=mode,
        effective_policy=None,
    )


__all__ = [
    "EffectivePolicy",
    "payload_policy_from_effective_policy",
    "resolve_effective_policy",
    "should_use_http_bytes_transport",
    "should_use_transport_payload_bytes",
]
