from __future__ import annotations

"""Helpers for preparing remote HTTP/service call payloads."""

from dataclasses import replace
from typing import Dict, Optional, Sequence

from pycloud_parallel.controlplane.config import INLINE_PAYLOAD_SOFT_LIMIT_BYTES, resolve_payload_policy
from pycloud_parallel.controlplane.effective_policy import EffectivePolicy
from pycloud_parallel.controlplane.payload_transport import prepare_outbound_payload
from pycloud_parallel.execution.support import (
    _estimate_managed_global_inline_size,
    _put_data_via_clients,
)


def prepare_remote_call_payload(
    clients: Sequence[object],
    payload: Optional[Dict[str, object]],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    managed_global_field_names: Sequence[str] = (),
    serialization_mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> Dict[str, object]:
    policy = resolve_payload_policy(
        "http_call",
        effective_policy=effective_policy,
        object_threshold_bytes=object_threshold_bytes,
    )
    if managed_global_field_names:
        policy = replace(policy, managed_global_field_names=tuple(str(name) for name in managed_global_field_names))
    put_kwargs = {}
    if str(serialization_mode or "").strip() and str(serialization_mode).strip().lower() != "legacy_v1":
        put_kwargs["default_serialization_mode"] = serialization_mode
    prepare_kwargs = {
        "put_data": lambda value, *, format="": _put_data_via_clients(clients, value, format=format, **put_kwargs),
        "estimate_inline_size": _estimate_managed_global_inline_size,
        "policy": policy,
    }
    if effective_policy is not None:
        prepare_kwargs["managed_global_policy"] = resolve_payload_policy(
            "managed_globals",
            effective_policy=effective_policy,
        )
    return prepare_outbound_payload(
        payload,
        **prepare_kwargs,
    )


__all__ = ["prepare_remote_call_payload"]
