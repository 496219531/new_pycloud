from __future__ import annotations

"""Central policy-profile definitions for execution-mode and payload strategy."""

from dataclasses import dataclass
from typing import Dict, Sequence, Tuple

from pycloud_parallel.controlplane.config import get_policy_limit_defaults, normalize_policy_limit_values
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
    inline_payload_threshold_bytes: int
    inline_payload_hard_limit_bytes: int
    inline_result_threshold_bytes: int
    inline_result_hard_limit_bytes: int
    use_raw_bytes_payload: bool
    use_http_raw_bytes_body: bool
    allow_pickle_stable: bool
    force_dataref_above_threshold: bool
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
        object.__setattr__(self, "force_dataref_above_threshold", bool(self.force_dataref_above_threshold))
        object.__setattr__(self, "public_gateway_allow_pickle", bool(self.public_gateway_allow_pickle))


@dataclass(frozen=True)
class PolicyBinding:
    binding_id: str
    policy_id: str
    default_mode: str

    def __post_init__(self) -> None:
        normalized_binding_id = str(self.binding_id or "").strip().lower()
        normalized_policy_id = str(self.policy_id or "").strip().lower()
        normalized_default_mode = normalize_serialization_mode(self.default_mode) or "legacy_v1"
        if not normalized_binding_id:
            raise ValueError("binding_id is required")
        if not normalized_policy_id:
            raise ValueError("policy_id is required")
        object.__setattr__(self, "binding_id", normalized_binding_id)
        object.__setattr__(self, "policy_id", normalized_policy_id)
        object.__setattr__(self, "default_mode", normalized_default_mode)


_BUILTIN_POLICY_PROFILES: Dict[str, PolicyProfile] = {
    "default_safe": PolicyProfile(
        policy_id="default_safe",
        version=1,
        allowed_modes=("legacy_v1", "structured_v1"),
        default_mode="legacy_v1",
        inline_payload_threshold_bytes=get_policy_limit_defaults("default_safe")[0],
        inline_payload_hard_limit_bytes=get_policy_limit_defaults("default_safe")[1],
        inline_result_threshold_bytes=get_policy_limit_defaults("default_safe")[2],
        inline_result_hard_limit_bytes=get_policy_limit_defaults("default_safe")[3],
        use_raw_bytes_payload=False,
        use_http_raw_bytes_body=False,
        allow_pickle_stable=False,
        force_dataref_above_threshold=True,
        public_gateway_allow_pickle=False,
    ),
    "trusted_internal": PolicyProfile(
        policy_id="trusted_internal",
        version=1,
        allowed_modes=("legacy_v1", "structured_v1", "pickle_stable_v1"),
        default_mode="pickle_stable_v1",
        inline_payload_threshold_bytes=get_policy_limit_defaults("trusted_internal")[0],
        inline_payload_hard_limit_bytes=get_policy_limit_defaults("trusted_internal")[1],
        inline_result_threshold_bytes=get_policy_limit_defaults("trusted_internal")[2],
        inline_result_hard_limit_bytes=get_policy_limit_defaults("trusted_internal")[3],
        use_raw_bytes_payload=True,
        use_http_raw_bytes_body=True,
        allow_pickle_stable=True,
        force_dataref_above_threshold=True,
        public_gateway_allow_pickle=False,
    ),
    "pickle_internal_heavy": PolicyProfile(
        policy_id="pickle_internal_heavy",
        version=1,
        allowed_modes=("pickle_stable_v1", "structured_v1", "legacy_v1"),
        default_mode="pickle_stable_v1",
        inline_payload_threshold_bytes=get_policy_limit_defaults("pickle_internal_heavy")[0],
        inline_payload_hard_limit_bytes=get_policy_limit_defaults("pickle_internal_heavy")[1],
        inline_result_threshold_bytes=get_policy_limit_defaults("pickle_internal_heavy")[2],
        inline_result_hard_limit_bytes=get_policy_limit_defaults("pickle_internal_heavy")[3],
        use_raw_bytes_payload=True,
        use_http_raw_bytes_body=True,
        allow_pickle_stable=True,
        force_dataref_above_threshold=True,
        public_gateway_allow_pickle=False,
    ),
}


_BUILTIN_POLICY_BINDINGS: Dict[str, PolicyBinding] = {
    "gateway_public": PolicyBinding(
        binding_id="gateway_public",
        policy_id="default_safe",
        default_mode="legacy_v1",
    ),
    "service_internal": PolicyBinding(
        binding_id="service_internal",
        policy_id="trusted_internal",
        default_mode="pickle_stable_v1",
    ),
    "taskpool_default": PolicyBinding(
        binding_id="taskpool_default",
        policy_id="trusted_internal",
        default_mode="pickle_stable_v1",
    ),
    "taskpool_heavy_dataframe_numpy": PolicyBinding(
        binding_id="taskpool_heavy_dataframe_numpy",
        policy_id="pickle_internal_heavy",
        default_mode="pickle_stable_v1",
    ),
    "jobqueue_controlplane_transport": PolicyBinding(
        binding_id="jobqueue_controlplane_transport",
        policy_id="default_safe",
        default_mode="structured_v1",
    ),
}


def builtin_policy_profiles() -> Dict[str, PolicyProfile]:
    return dict(_BUILTIN_POLICY_PROFILES)


def builtin_policy_bindings() -> Dict[str, PolicyBinding]:
    return dict(_BUILTIN_POLICY_BINDINGS)


def get_policy_profile(policy_id: str = "") -> PolicyProfile:
    normalized = str(policy_id or "").strip().lower() or "default_safe"
    try:
        return _BUILTIN_POLICY_PROFILES[normalized]
    except KeyError as exc:
        raise KeyError(f"unknown policy profile: {policy_id!r}") from exc


def get_policy_binding(binding_id: str) -> PolicyBinding:
    normalized = str(binding_id or "").strip().lower()
    try:
        return _BUILTIN_POLICY_BINDINGS[normalized]
    except KeyError as exc:
        raise KeyError(f"unknown policy binding: {binding_id!r}") from exc


def get_default_policy_id_for_binding(binding_id: str) -> str:
    return get_policy_binding(binding_id).policy_id


def get_default_mode_for_binding(binding_id: str) -> str:
    return get_policy_binding(binding_id).default_mode


__all__ = [
    "PolicyBinding",
    "PolicyProfile",
    "builtin_policy_bindings",
    "builtin_policy_profiles",
    "get_default_mode_for_binding",
    "get_default_policy_id_for_binding",
    "get_policy_binding",
    "get_policy_profile",
]
