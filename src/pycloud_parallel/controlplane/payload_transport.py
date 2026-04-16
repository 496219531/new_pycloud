from __future__ import annotations

"""Unified payload transport policy helpers."""

from dataclasses import replace
import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from pycloud_parallel.controlplane.config import PayloadPolicy, get_payload_policy
from pycloud_parallel.controlplane.data_ref import DataRef, maybe_data_ref
from pycloud_parallel.controlplane.serialization import (
    convert_dict_to_arrow,
    serialize_arrow_compatible,
    serialize_inline_payload,
    serialize_inline_result,
)

EstimateInlineSize = Callable[[Any], int]
PutPayloadData = Callable[..., object]
ResolveObjectRefs = Callable[[Any], Any]


def _put_prepared_value(
    value: Any,
    *,
    policy: PayloadPolicy,
    put_data: PutPayloadData,
    format: str = "",
) -> DataRef:
    prepared = put_data(value, format=format)
    data_ref = maybe_data_ref(prepared)
    if data_ref is None:
        raise TypeError(f"put_data must return DataRef-compatible value, got {type(prepared).__name__}")
    if policy.consume_on_read:
        return replace(data_ref, consume_on_read=True)
    return data_ref


def _prepare_value_for_transport(
    value: Any,
    *,
    policy: PayloadPolicy,
    estimate_inline_size: EstimateInlineSize,
    put_data: PutPayloadData,
    preserve_container: bool = False,
) -> Any:
    direct_ref = maybe_data_ref(value)
    if direct_ref is not None:
        return direct_ref
    if policy.objectify_pathlikes and isinstance(value, os.PathLike):
        return _put_prepared_value(value, policy=policy, put_data=put_data)
    if policy.objectify_strings_as_files and isinstance(value, str):
        path = Path(value).expanduser()
        if path.exists() and path.is_file():
            return _put_prepared_value(path, policy=policy, put_data=put_data)
        return value
    if policy.objectify_bytes and isinstance(value, (bytes, bytearray, memoryview)):
        return _put_prepared_value(bytes(value), policy=policy, put_data=put_data, format="bin")

    if policy.recurse_containers and isinstance(value, dict):
        if not preserve_container:
            try:
                inline_size = estimate_inline_size(value)
            except Exception:
                inline_size = 0
            if inline_size > max(1, int(policy.inline_payload_soft_limit_bytes)):
                return _put_prepared_value(value, policy=policy, put_data=put_data, format="json")
        return {
            key: _prepare_value_for_transport(
                item,
                policy=policy,
                estimate_inline_size=estimate_inline_size,
                put_data=put_data,
                preserve_container=False,
            )
            for key, item in value.items()
        }
    if policy.recurse_containers and isinstance(value, list):
        if not preserve_container:
            try:
                inline_size = estimate_inline_size(value)
            except Exception:
                inline_size = 0
            if inline_size > max(1, int(policy.inline_payload_soft_limit_bytes)):
                return _put_prepared_value(value, policy=policy, put_data=put_data, format="json")
        return [
            _prepare_value_for_transport(
                item,
                policy=policy,
                estimate_inline_size=estimate_inline_size,
                put_data=put_data,
                preserve_container=False,
            )
            for item in value
        ]
    if policy.recurse_containers and isinstance(value, tuple):
        return [
            _prepare_value_for_transport(
                item,
                policy=policy,
                estimate_inline_size=estimate_inline_size,
                put_data=put_data,
                preserve_container=False,
            )
            for item in value
        ]

    try:
        inline_size = estimate_inline_size(value)
    except Exception:
        return value
    if inline_size <= max(1, int(policy.inline_payload_soft_limit_bytes)):
        return value
    if isinstance(value, (dict, list)):
        return _put_prepared_value(value, policy=policy, put_data=put_data, format="json")
    return _put_prepared_value(value, policy=policy, put_data=put_data)


def prepare_outbound_payload(
    payload: Optional[Dict[str, object]],
    *,
    put_data: PutPayloadData,
    estimate_inline_size: EstimateInlineSize,
    policy: PayloadPolicy,
) -> Dict[str, object]:
    raw_payload = dict(payload or {})
    prepared: Dict[str, object] = {}
    if "args" in raw_payload:
        prepared["args"] = _prepare_value_for_transport(
            list(raw_payload.get("args") or []),
            policy=policy,
            estimate_inline_size=estimate_inline_size,
            put_data=put_data,
            preserve_container=policy.preserve_args_kwargs_container,
        )
    if "kwargs" in raw_payload:
        prepared["kwargs"] = _prepare_value_for_transport(
            dict(raw_payload.get("kwargs") or {}),
            policy=policy,
            estimate_inline_size=estimate_inline_size,
            put_data=put_data,
            preserve_container=policy.preserve_args_kwargs_container,
        )
    for key, value in raw_payload.items():
        if key in {"args", "kwargs"}:
            continue
        prepared[key] = _prepare_value_for_transport(
            value,
            policy=policy,
            estimate_inline_size=estimate_inline_size,
            put_data=put_data,
            preserve_container=False,
        )

    if not policy.managed_global_field_names:
        return prepared

    managed_global_policy = get_payload_policy("managed_globals")
    for field_name in policy.managed_global_field_names:
        normalized = str(field_name or "").strip()
        if not normalized:
            continue
        raw_value = raw_payload.get(normalized)
        if not isinstance(raw_value, dict):
            continue
        prepared[normalized] = {
            str(name): _prepare_value_for_transport(
                value,
                policy=managed_global_policy,
                estimate_inline_size=estimate_inline_size,
                put_data=put_data,
                preserve_container=False,
            )
            for name, value in raw_value.items()
        }
    return prepared


def prepare_outbound_value(
    value: Any,
    *,
    put_data: PutPayloadData,
    estimate_inline_size: EstimateInlineSize,
    policy: PayloadPolicy,
    preserve_container: bool = False,
) -> Any:
    return _prepare_value_for_transport(
        value,
        policy=policy,
        estimate_inline_size=estimate_inline_size,
        put_data=put_data,
        preserve_container=preserve_container,
    )


def estimate_payload_inline_size(value: Any) -> int:
    serialized = serialize_arrow_compatible(value)
    return len(json.dumps(serialized, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def encode_payload_for_transport(
    payload: Optional[Dict[str, object]],
    *,
    policy: PayloadPolicy,
    context: str = "payload",
) -> Dict[str, object]:
    serialized, _, _ = serialize_inline_payload(
        payload or {},
        context=context,
        limit_bytes=policy.inline_payload_hard_limit_bytes,
    )
    return serialized


def encode_result_for_transport(
    value: Any,
    *,
    policy: PayloadPolicy,
    context: str = "result",
) -> Dict[str, object]:
    serialized = serialize_arrow_compatible(value)
    wrapped = serialized if isinstance(serialized, dict) else {"value": serialized}
    serialize_inline_result(
        wrapped,
        context=context,
        limit_bytes=policy.inline_result_hard_limit_bytes,
    )
    return wrapped


def decode_payload_from_transport(
    payload: Any,
    *,
    policy: PayloadPolicy,
) -> Any:
    return normalize_inbound_payload(
        payload,
        object_dir="",
        policy=policy,
        resolve_object_refs=lambda value: value,
    )


def decode_result_from_transport(
    payload: Any,
    *,
    policy: Optional[PayloadPolicy] = None,
) -> Any:
    return decode_payload_from_transport(
        payload,
        policy=policy or get_payload_policy("result"),
    )


def normalize_inbound_payload(
    payload: Any,
    *,
    object_dir: str,
    policy: PayloadPolicy,
    resolve_object_refs: Optional[ResolveObjectRefs] = None,
) -> Any:
    del policy
    normalized = convert_dict_to_arrow(payload)
    resolver = resolve_object_refs
    if resolver is None:
        from pycloud_parallel.controlplane.node.results import _resolve_object_refs_in_payload

        resolver = lambda value: _resolve_object_refs_in_payload(value, object_dir=object_dir)
    return resolver(normalized)
