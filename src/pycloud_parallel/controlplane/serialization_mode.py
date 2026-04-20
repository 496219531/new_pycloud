from __future__ import annotations

"""Centralized serialization-mode authority helpers."""

from typing import Final

from pycloud_parallel.controlplane.config import get_serialization_mode, get_trust_mode


SUPPORTED_SERIALIZATION_MODES: Final[tuple[str, ...]] = (
    "legacy_v1",
    "structured_v1",
    "pickle_stable_v1",
)

_SUPPORTED_SET: Final[set[str]] = set(SUPPORTED_SERIALIZATION_MODES)
_PICKLE_RESTRICTED_CONTEXTS: Final[set[str]] = {
    "gateway_public",
    "untrusted_transport",
}


def normalize_serialization_mode(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    if normalized not in _SUPPORTED_SET:
        raise ValueError(
            f"unsupported serialization_mode={normalized!r}; supported modes: {list(SUPPORTED_SERIALIZATION_MODES)}"
        )
    return normalized


def validate_mode_for_context(
    mode: str,
    *,
    context: str = "",
    trust_mode: str = "",
) -> str:
    normalized = normalize_serialization_mode(mode) or "legacy_v1"
    normalized_context = str(context or "").strip().lower()
    effective_trust_mode = str(trust_mode or get_trust_mode() or "trusted").strip().lower() or "trusted"
    if normalized == "pickle_stable_v1" and normalized_context in _PICKLE_RESTRICTED_CONTEXTS:
        raise ValueError(
            f"pickle_stable_v1 is not allowed for {normalized_context or 'restricted'} transport; "
            "use structured_v1 or legacy_v1 instead"
        )
    del effective_trust_mode
    return normalized


def resolve_effective_serialization_mode(
    *,
    request_mode: str = "",
    session_mode: str = "",
    default_mode: str = "",
    context: str = "",
    trust_mode: str = "",
) -> str:
    for candidate in (
        request_mode,
        session_mode,
        default_mode,
        get_serialization_mode(),
        "legacy_v1",
    ):
        normalized = normalize_serialization_mode(candidate)
        if normalized:
            return validate_mode_for_context(
                normalized,
                context=context,
                trust_mode=trust_mode,
            )
    return "legacy_v1"


def resolve_declared_transport_mode(*, declared_mode: str = "", default_mode: str = "") -> str:
    declared = normalize_serialization_mode(declared_mode)
    if declared:
        return declared
    fallback = normalize_serialization_mode(default_mode)
    if fallback:
        return fallback
    return "legacy_v1"


def resolve_received_transport_mode(
    *,
    declared_mode: str = "",
    default_mode: str = "",
    context: str = "",
    trust_mode: str = "",
) -> str:
    resolved = resolve_declared_transport_mode(
        declared_mode=declared_mode,
        default_mode=default_mode,
    )
    return validate_mode_for_context(
        resolved,
        context=context,
        trust_mode=trust_mode,
    )


__all__ = [
    "SUPPORTED_SERIALIZATION_MODES",
    "normalize_serialization_mode",
    "resolve_declared_transport_mode",
    "resolve_effective_serialization_mode",
    "resolve_received_transport_mode",
    "validate_mode_for_context",
]
