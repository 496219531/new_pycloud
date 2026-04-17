from __future__ import annotations

from dataclasses import replace
import json

from pycloud_parallel.controlplane import client_transport as client_transport_mod
from pycloud_parallel.controlplane.config import get_payload_policy
from pycloud_parallel.controlplane.data_ref import DataRef
from pycloud_parallel.data.ref import data_ref_to_payload
from pycloud_parallel.controlplane.payload_transport import (
    decode_payload_from_transport,
    encode_result_for_transport,
    estimate_payload_inline_size,
    normalize_inbound_payload,
    prepare_outbound_payload,
)

_decode_http_request_body = client_transport_mod._decode_http_request_body


def _fake_object_ref(*, object_id_suffix: str = "a", format: str = "bin", consume_on_read: bool = False) -> DataRef:
    object_id = f"sha256:{object_id_suffix * 64}"
    return DataRef(
        ref_id=object_id,
        storage_id=object_id,
        logical_type="",
        format=format,
        size_bytes=128,
        materialize_as="bytes",
        locator_kind="node_local",
        locator_token="",
        consume_on_read=consume_on_read,
    )


def test_get_payload_policy_defaults() -> None:
    http_policy = get_payload_policy("http_call")
    job_policy = get_payload_policy("job_submit")
    managed_globals_policy = get_payload_policy("managed_globals")

    assert http_policy.preserve_args_kwargs_container is True
    assert http_policy.consume_on_read is True
    assert job_policy.managed_global_field_names == ("update_globals",)
    assert managed_globals_policy.objectify_pathlikes is True
    assert managed_globals_policy.objectify_strings_as_files is True
    assert managed_globals_policy.objectify_bytes is True
    assert managed_globals_policy.consume_on_read is False


def test_prepare_outbound_payload_preserves_args_kwargs_container() -> None:
    policy = get_payload_policy("http_call")
    policy = replace(
        policy,
        limits=replace(policy.limits, inline_payload_soft_limit_bytes=32),
    )
    uploads: list[tuple[object, str]] = []

    def _put_data(value, *, format=""):
        uploads.append((value, format))
        return _fake_object_ref(object_id_suffix=chr(ord("a") + len(uploads) - 1), format=format or "bin")

    prepared = prepare_outbound_payload(
        {
            "args": ["small", "x" * 128],
            "kwargs": {"blob": "y" * 128},
        },
        put_data=_put_data,
        estimate_inline_size=estimate_payload_inline_size,
        policy=policy,
    )

    assert isinstance(prepared["args"], list)
    assert prepared["args"][0] == "small"
    assert isinstance(prepared["args"][1], DataRef)
    assert prepared["args"][1].consume_on_read is True
    assert isinstance(prepared["kwargs"], dict)
    assert isinstance(prepared["kwargs"]["blob"], DataRef)
    assert len(uploads) == 2


def test_prepare_outbound_payload_job_submit_applies_managed_globals_policy(tmp_path) -> None:
    policy = get_payload_policy("job_submit")
    path = tmp_path / "config.json"
    path.write_text('{"mode":"test"}', encoding="utf-8")
    uploads: list[object] = []

    def _put_data(value, *, format=""):
        uploads.append(value)
        return _fake_object_ref(object_id_suffix=chr(ord("a") + len(uploads) - 1), format=format or "bin")

    prepared = prepare_outbound_payload(
        {
            "artifact_path": path,
            "update_globals": {
                "cfg_path": path,
                "raw_bytes": b"abc",
            },
        },
        put_data=_put_data,
        estimate_inline_size=estimate_payload_inline_size,
        policy=policy,
    )

    assert prepared["artifact_path"] == path
    assert isinstance(prepared["update_globals"]["cfg_path"], DataRef)
    assert isinstance(prepared["update_globals"]["raw_bytes"], DataRef)
    assert uploads[0] == path
    assert uploads[1] == b"abc"


def test_normalize_inbound_payload_deserializes_before_object_resolution() -> None:
    captured = {}

    def _resolve(value):
        captured["value"] = value
        return {"resolved": True}

    normalized = normalize_inbound_payload(
        {
            "blob": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("c" * 64),
                    storage_id="sha256:" + ("c" * 64),
                    logical_type="",
                    format="json",
                    size_bytes=42,
                    materialize_as="json",
                    locator_kind="node_local",
                    locator_token="",
                )
            )
        },
        object_dir="/tmp/objects",
        policy=get_payload_policy("job_submit"),
        resolve_object_refs=_resolve,
    )

    assert normalized == {"resolved": True}
    assert isinstance(captured["value"]["blob"], DataRef)


def test_decode_payload_from_transport_keeps_payload_decoded_without_localizing() -> None:
    decoded = decode_payload_from_transport(
        {
            "blob": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("d" * 64),
                    storage_id="sha256:" + ("d" * 64),
                    logical_type="",
                    format="json",
                    size_bytes=99,
                    materialize_as="json",
                    locator_kind="node_local",
                    locator_token="",
                )
            )
        },
        policy=get_payload_policy("http_call"),
    )

    assert isinstance(decoded["blob"], DataRef)


def test_decode_http_request_body_returns_decoded_payload_objects() -> None:
    body = json.dumps(
        {
            "blob": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("e" * 64),
                    storage_id="sha256:" + ("e" * 64),
                    logical_type="",
                    format="json",
                    size_bytes=11,
                    materialize_as="json",
                    locator_kind="node_local",
                    locator_token="",
                )
            )
        }
    ).encode("utf-8")

    decoded = _decode_http_request_body(
        body,
        context="service call payload",
    )

    assert isinstance(decoded["blob"], DataRef)


def test_encode_result_for_transport_wraps_scalar_value() -> None:
    encoded = encode_result_for_transport(
        7,
        policy=get_payload_policy("result"),
        context="task result",
    )

    assert encoded == {"value": 7}


def test_decode_payload_from_transport_recognizes_data_ref_sentinel() -> None:
    decoded = decode_payload_from_transport(
        {
            "blob": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("f" * 64),
                    storage_id="sha256:" + ("f" * 64),
                    logical_type="bytes",
                    format="bin",
                    size_bytes=12,
                    materialize_as="auto",
                )
            )
        },
        policy=get_payload_policy("http_call"),
    )

    assert isinstance(decoded["blob"], DataRef)
    assert decoded["blob"].object_id == "sha256:" + ("f" * 64)


def test_decode_http_request_body_returns_data_ref_objects() -> None:
    body = json.dumps(
        {
            "blob": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("1" * 64),
                    storage_id="sha256:" + ("1" * 64),
                    logical_type="json",
                    format="json",
                    size_bytes=11,
                    materialize_as="json",
                )
            )
        }
    ).encode("utf-8")

    decoded = _decode_http_request_body(
        body,
        context="service call payload",
    )

    assert isinstance(decoded["blob"], DataRef)
    assert decoded["blob"].logical_type == "json"
