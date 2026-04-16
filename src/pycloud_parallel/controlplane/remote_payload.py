from __future__ import annotations

"""Helpers for preparing remote HTTP/service call payloads."""

from dataclasses import replace
from typing import Dict, Optional, Sequence

from pycloud_parallel.controlplane.config import get_payload_policy
from pycloud_parallel.controlplane.payload_transport import prepare_outbound_payload
from pycloud_parallel.controlplane.serialization import INLINE_PAYLOAD_SOFT_LIMIT_BYTES
from pycloud_parallel.execution.support import (
    _estimate_managed_global_inline_size,
    _policy_with_soft_limit,
    _put_data_via_clients,
)


def prepare_remote_call_payload(
    clients: Sequence[object],
    payload: Optional[Dict[str, object]],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    managed_global_field_names: Sequence[str] = (),
) -> Dict[str, object]:
    policy = _policy_with_soft_limit(get_payload_policy("http_call"), object_threshold_bytes)
    if managed_global_field_names:
        policy = replace(policy, managed_global_field_names=tuple(str(name) for name in managed_global_field_names))
    return prepare_outbound_payload(
        payload,
        put_data=lambda value, *, format="": _put_data_via_clients(clients, value, format=format),
        estimate_inline_size=_estimate_managed_global_inline_size,
        policy=policy,
    )


__all__ = ["prepare_remote_call_payload"]
