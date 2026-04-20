from __future__ import annotations

"""Central policy-profile definitions for execution-mode and payload strategy."""

from dataclasses import dataclass
from typing import Dict, Sequence, Tuple

from pycloud_parallel.controlplane.serialization_mode import normalize_serialization_mode


def _normalize_modes(values: Sequence[str]) -> Tuple[str, ...]:
    out = []
    seen = set()
    for item in values:
        normalized = normalize_serialization_mode(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    if not out:
        raise ValueError("allowed_modes must not be empty")
    return tuple(out)


@dataclass(frozen=True)
class PolicyProfile:
    policy_id: str
    version: int
    allowed_modes: Tuple[str, ...]
    default_mode: str
    inline_payload_soft_limit_bytes: int
    inline_payload_hard_limit_bytes: int
    inline_result_hard_limit_bytes: int
    prefer_transport_payload_bytes: bool
    allow_pickle_stable: bool
    force_dataref_above_soft_limit: bool
    public_gateway_allow_pickle: bool = False

    def __post_init__(self) -> None:
        normalized_id = str(self.policy_id or "").strip().lower()
        if not normalized_id:
            raise ValueError("policy_id is required")
        normalized_modes = _normalize_modes(self.allowed_modes)
        normalized_default = normalize_serialization_mode(self.default_mode)
        if not normalized_default:
            normalized_default = normalized_modes[0]
        if normalized_default not in normalized_modes:
            raise ValueError(f"default_mode must be included in allowed_modes: {normalized_default!r}")
        if not self.allow_pickle_stable and "pickle_stable_v1" in normalized_modes:
            normalized_modes = tuple(mode for mode in normalized_modes if mode != "pickle_stable_v1")
            if not normalized_modes:
                raise ValueError("allowed_modes cannot become empty after pickle restriction")
            if normalized_default == "pickle_stable_v1":
                normalized_default = normalized_modes[0]
        object.__setattr__(self, "policy_id", normalized_id)
        object.__setattr__(self, "version", max(1, int(self.version or 1)))
        object.__setattr__(self, "allowed_modes", normalized_modes)
        object.__setattr__(self, "default_mode", normalized_default)
        object.__setattr__(self, "inline_payload_soft_limit_bytes", max(1, int(self.inline_payload_soft_limit_bytes or 1)))
        object.__setattr__(self, "inline_payload_hard_limit_bytes", max(1, int(self.inline_payload_hard_limit_bytes or 1)))
        object.__setattr__(self, "inline_result_hard_limit_bytes", max(1, int(self.inline_result_hard_limit_bytes or 1)))
        object.__setattr__(self, "prefer_transport_payload_bytes", bool(self.prefer_transport_payload_bytes))
        object.__setattr__(self, "allow_pickle_stable", bool(self.allow_pickle_stable))
        object.__setattr__(self, "force_dataref_above_soft_limit", bool(self.force_dataref_above_soft_limit))
        object.__setattr__(self, "public_gateway_allow_pickle", bool(self.public_gateway_allow_pickle))


_BUILTIN_POLICY_PROFILES: Dict[str, PolicyProfile] = {
    "default_safe": PolicyProfile(
        policy_id="default_safe",
        version=1,
        allowed_modes=("legacy_v1", "structured_v1"),
        default_mode="legacy_v1",
        inline_payload_soft_limit_bytes=512 * 1024,
        inline_payload_hard_limit_bytes=2 * 1024 * 1024,
        inline_result_hard_limit_bytes=4 * 1024 * 1024,
        prefer_transport_payload_bytes=False,
        allow_pickle_stable=False,
        force_dataref_above_soft_limit=True,
        public_gateway_allow_pickle=False,
    ),
    "trusted_internal": PolicyProfile(
        policy_id="trusted_internal",
        version=1,
        allowed_modes=("legacy_v1", "structured_v1", "pickle_stable_v1"),
        default_mode="structured_v1",
        inline_payload_soft_limit_bytes=512 * 1024,
        inline_payload_hard_limit_bytes=2 * 1024 * 1024,
        inline_result_hard_limit_bytes=4 * 1024 * 1024,
        prefer_transport_payload_bytes=True,
        allow_pickle_stable=True,
        force_dataref_above_soft_limit=True,
        public_gateway_allow_pickle=False,
    ),
    "pickle_internal_heavy": PolicyProfile(
        policy_id="pickle_internal_heavy",
        version=1,
        allowed_modes=("pickle_stable_v1", "structured_v1", "legacy_v1"),
        default_mode="pickle_stable_v1",
        inline_payload_soft_limit_bytes=1024 * 1024,
        inline_payload_hard_limit_bytes=4 * 1024 * 1024,
        inline_result_hard_limit_bytes=8 * 1024 * 1024,
        prefer_transport_payload_bytes=True,
        allow_pickle_stable=True,
        force_dataref_above_soft_limit=True,
        public_gateway_allow_pickle=False,
    ),
}


def builtin_policy_profiles() -> Dict[str, PolicyProfile]:
    return dict(_BUILTIN_POLICY_PROFILES)


def get_policy_profile(policy_id: str = "") -> PolicyProfile:
    normalized = str(policy_id or "").strip().lower() or "default_safe"
    try:
        return _BUILTIN_POLICY_PROFILES[normalized]
    except KeyError as exc:
        raise KeyError(f"unknown policy profile: {policy_id!r}") from exc


__all__ = [
    "PolicyProfile",
    "builtin_policy_profiles",
    "get_policy_profile",
]
