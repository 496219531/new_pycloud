from __future__ import annotations

"""Centralized resolved system-mode view for additive multi-mode experiments."""

from dataclasses import dataclass, replace
from typing import Any

from pycloud_parallel.controlplane.config import (
    get_dependency_policy_mode,
    get_object_transfer_mode,
    get_serialization_mode,
    get_system_mode,
    get_trust_mode,
)


_SUPPORTED_SYSTEM_MODES = {"trusted_default"}


@dataclass(frozen=True)
class ResolvedSystemMode:
    system_mode: str
    trust_mode: str
    object_transfer_mode: str
    serialization_mode: str
    dependency_policy_mode: str


def _normalize_mode_name(value: str) -> str:
    normalized = str(value or "").strip().lower()
    return normalized or "trusted_default"


def resolve_system_mode(system_mode: str = "", **overrides: Any) -> ResolvedSystemMode:
    normalized_system_mode = _normalize_mode_name(system_mode or get_system_mode())
    if normalized_system_mode not in _SUPPORTED_SYSTEM_MODES:
        raise ValueError(
            f"unsupported system_mode={normalized_system_mode!r}; "
            f"supported modes: {sorted(_SUPPORTED_SYSTEM_MODES)}"
        )

    resolved = ResolvedSystemMode(
        system_mode=normalized_system_mode,
        trust_mode="trusted",
        object_transfer_mode="auto",
        serialization_mode="legacy_v1",
        dependency_policy_mode="prebuilt",
    )

    trust_mode = str(overrides.get("trust_mode") or get_trust_mode() or resolved.trust_mode).strip().lower()
    object_transfer_mode = str(
        overrides.get("object_transfer_mode") or get_object_transfer_mode() or resolved.object_transfer_mode
    ).strip().lower()
    serialization_mode = str(
        overrides.get("serialization_mode") or get_serialization_mode() or resolved.serialization_mode
    ).strip().lower()
    dependency_policy_mode = str(
        overrides.get("dependency_policy_mode") or get_dependency_policy_mode() or resolved.dependency_policy_mode
    ).strip().lower()

    return replace(
        resolved,
        trust_mode=trust_mode or resolved.trust_mode,
        object_transfer_mode=object_transfer_mode or resolved.object_transfer_mode,
        serialization_mode=serialization_mode or resolved.serialization_mode,
        dependency_policy_mode=dependency_policy_mode or resolved.dependency_policy_mode,
    )


__all__ = [
    "ResolvedSystemMode",
    "resolve_system_mode",
]
