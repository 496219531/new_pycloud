from __future__ import annotations

"""Effective-policy resolution from centralized profiles and session context."""

from dataclasses import dataclass, replace
from typing import Optional, Tuple

from pycloud_parallel.controlplane.config import (
    PayloadPolicy,
    effective_limits_from_profile,
    get_payload_policy,
    merge_payload_limits_with_effective_policy,
    normalize_policy_limit_values,
)
from pycloud_parallel.controlplane.policy_profile import PolicyProfile
from pycloud_parallel.controlplane.serialization_mode import PICKLE_SERIALIZATION_MODES, normalize_serialization_mode

_PICKLE_MODES = set(PICKLE_SERIALIZATION_MODES)


@dataclass(frozen=True)
class EffectivePolicy:
    policy_id: str
    version: int
    resolved_mode: str
    allowed_modes: Tuple[str, ...]
    inline_payload_threshold_bytes: int
    inline_payload_hard_limit_bytes: int
    inline_result_threshold_bytes: int
    inline_result_hard_limit_bytes: int
    use_raw_bytes_payload: bool
    use_http_raw_bytes_body: bool
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
        payload_threshold, payload_hard_limit, result_threshold, result_hard_limit = normalize_policy_limit_values(
            payload_threshold=int(self.inline_payload_threshold_bytes or 1),
            payload_hard=int(self.inline_payload_hard_limit_bytes or 1),
            result_threshold=int(self.inline_result_threshold_bytes or 1),
            result_hard=int(self.inline_result_hard_limit_bytes or 1),
        )
        object.__setattr__(self, "inline_payload_threshold_bytes", payload_threshold)
        object.__setattr__(self, "inline_payload_hard_limit_bytes", payload_hard_limit)
        object.__setattr__(self, "inline_result_threshold_bytes", result_threshold)
        object.__setattr__(self, "inline_result_hard_limit_bytes", result_hard_limit)
        object.__setattr__(self, "use_raw_bytes_payload", bool(self.use_raw_bytes_payload))
        object.__setattr__(self, "use_http_raw_bytes_body", bool(self.use_http_raw_bytes_body))
        object.__setattr__(self, "allow_pickle_stable", bool(self.allow_pickle_stable))

    def assert_frozen_mode(self, request_mode: str = "") -> str:
        normalized_request = normalize_serialization_mode(request_mode)
        if normalized_request and normalized_request != self.resolved_mode:
            raise ValueError(
                f"serialization_mode is frozen for this session: requested={normalized_request!r}, "
                f"resolved={self.resolved_mode!r}"
            )
        return self.resolved_mode


def _allowed_modes_for_context(profile: PolicyProfile, *, context: str) -> Tuple[str, ...]:
    normalized_context = str(context or "").strip().lower()
    allowed = list(profile.allowed_modes)
    if not profile.allow_pickle_stable or (normalized_context == "gateway_public" and not profile.public_gateway_allow_pickle):
        allowed = [mode for mode in allowed if mode not in _PICKLE_MODES]
    if not allowed:
        raise ValueError(
            f"policy profile {profile.policy_id!r} does not allow any serialization modes for context={normalized_context!r}"
        )
    return tuple(allowed)


def resolve_effective_policy(
    profile: PolicyProfile,
    requested_mode: str = "",
    context: str = "",
) -> EffectivePolicy:
    allowed_modes = _allowed_modes_for_context(profile, context=context)
    if not allowed_modes:
        raise ValueError(
            f"policy profile {profile.policy_id!r} does not allow any serialization modes for context={context!r}"
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

    (
        inline_payload_threshold_bytes,
        inline_payload_hard_limit_bytes,
        inline_result_threshold_bytes,
        inline_result_hard_limit_bytes,
    ) = (
        effective_limits_from_profile(profile)
    )
    use_raw_bytes_payload = bool(profile.use_raw_bytes_payload) and resolved_mode != "legacy_v1"
    use_http_raw_bytes_body = use_raw_bytes_payload and bool(profile.use_http_raw_bytes_body) and (
        str(context or "").strip().lower()
        in {"gateway_public", "service_connect", "http_call", "jobqueue_session", "taskpool_owner"}
    )
    allow_pickle_stable = any(mode in _PICKLE_MODES for mode in allowed_modes) and bool(profile.allow_pickle_stable)

    return EffectivePolicy(
        policy_id=profile.policy_id,
        version=profile.version,
        resolved_mode=resolved_mode,
        allowed_modes=allowed_modes,
        inline_payload_threshold_bytes=inline_payload_threshold_bytes,
        inline_payload_hard_limit_bytes=inline_payload_hard_limit_bytes,
        inline_result_threshold_bytes=inline_result_threshold_bytes,
        inline_result_hard_limit_bytes=inline_result_hard_limit_bytes,
        use_raw_bytes_payload=use_raw_bytes_payload,
        use_http_raw_bytes_body=use_http_raw_bytes_body,
        allow_pickle_stable=allow_pickle_stable,
    )


def payload_policy_from_effective_policy(mode: str, effective_policy: Optional[EffectivePolicy]) -> PayloadPolicy:
    base = get_payload_policy(mode)  # type: ignore[arg-type]
    if effective_policy is None:
        return base
    limits = merge_payload_limits_with_effective_policy(base.limits, effective_policy)
    return replace(base, limits=limits)


def should_use_raw_bytes_payload(
    *,
    mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> bool:
    if effective_policy is not None:
        return bool(effective_policy.use_raw_bytes_payload)
    from pycloud_parallel.controlplane.serialization import prefers_raw_bytes_payload

    return bool(prefers_raw_bytes_payload(mode))


def should_use_http_raw_bytes_body(
    *,
    mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> bool:
    if effective_policy is not None:
        return bool(effective_policy.use_http_raw_bytes_body)
    normalized_mode = str(mode or "").strip().lower() or "legacy_v1"
    if normalized_mode == "legacy_v1":
        return False
    return should_use_raw_bytes_payload(
        mode=mode,
        effective_policy=None,
    )


__all__ = [
    "EffectivePolicy",
    "payload_policy_from_effective_policy",
    "resolve_effective_policy",
    "should_use_http_raw_bytes_body",
    "should_use_raw_bytes_payload",
]
