from __future__ import annotations

"""Centralized serialization-mode authority helpers."""

from typing import Final

from pycloud_parallel.controlplane.config import get_serialization_mode, get_trust_mode


SUPPORTED_SERIALIZATION_MODES: Final[tuple[str, ...]] = (
    "legacy_v1",
    "structured_v1",
    "pickle_stable_v1",
    "pickle_native_v1",
)

_SUPPORTED_SET: Final[set[str]] = set(SUPPORTED_SERIALIZATION_MODES)
PICKLE_SERIALIZATION_MODES: Final[tuple[str, ...]] = (
    "pickle_stable_v1",
    "pickle_native_v1",
)
_PICKLE_SET: Final[set[str]] = set(PICKLE_SERIALIZATION_MODES)
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
    if normalized in _PICKLE_SET and normalized_context in _PICKLE_RESTRICTED_CONTEXTS:
        raise ValueError(
            f"{normalized} is not allowed for {normalized_context or 'restricted'} transport; "
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
    allowed_modes: tuple[str, ...] | list[str] | None = None,
    frozen_mode: str = "",
) -> str:
    normalized_frozen_mode = normalize_serialization_mode(frozen_mode)
    if normalized_frozen_mode:
        normalized_request_mode = normalize_serialization_mode(request_mode)
        if normalized_request_mode and normalized_request_mode != normalized_frozen_mode:
            raise ValueError(
                f"serialization_mode is frozen for this session: requested={normalized_request_mode!r}, "
                f"resolved={normalized_frozen_mode!r}"
            )
        return validate_mode_for_context(
            normalized_frozen_mode,
            context=context,
            trust_mode=trust_mode,
        )

    normalized_allowed_modes = {
        normalize_serialization_mode(item)
        for item in (allowed_modes or ())
        if normalize_serialization_mode(item)
    }
    for candidate in (
        request_mode,
        session_mode,
        default_mode,
        get_serialization_mode(),
        "legacy_v1",
    ):
        normalized = normalize_serialization_mode(candidate)
        if normalized_allowed_modes and normalized and normalized not in normalized_allowed_modes:
            if candidate == request_mode and normalize_serialization_mode(request_mode):
                raise ValueError(
                    f"serialization_mode={normalized!r} is not allowed; allowed modes: {sorted(normalized_allowed_modes)}"
                )
            continue
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
    "PICKLE_SERIALIZATION_MODES",
    "SUPPORTED_SERIALIZATION_MODES",
    "normalize_serialization_mode",
    "resolve_declared_transport_mode",
    "resolve_effective_serialization_mode",
    "resolve_received_transport_mode",
    "validate_mode_for_context",
]
