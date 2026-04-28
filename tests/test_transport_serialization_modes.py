from __future__ import annotations

import json

import numpy as np
import pandas as pd

from pycloud_parallel.controlplane.client_transport import (
    _decode_http_request_body,
    _normalize_http_response_body,
    _serialize_http_call_payload,
)
from pycloud_parallel.controlplane.config import get_payload_policy, reload_config
from pycloud_parallel.controlplane.payload_transport import (
    decode_payload_from_transport,
    decode_result_from_transport,
    encode_payload_for_transport,
    encode_result_for_transport,
)
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible
from pycloud_parallel.controlplane.serialization import (
    TRANSPORT_ENVELOPE_SENTINEL,
    decode_transport_payload_bytes,
    encode_transport_payload_bytes,
    encode_transport_value,
)
from pycloud_parallel.controlplane.pickle_stable_v1 import normalize_for_pickle_stable


def _roundtrip_payload(mode: str):
    payload = {
        "frame": pd.DataFrame({"a": [1, 2]}),
        "array": np.array([1, 2, 3], dtype=np.int64),
    }
    if mode != "legacy_v1":
        payload["blob"] = b"abc"
    encoded = encode_payload_for_transport(payload, policy=get_payload_policy("http_call"), mode=mode)
    decoded = decode_payload_from_transport(encoded, policy=get_payload_policy("http_call"), mode=mode)
    return payload, decoded


def test_transport_payload_modes_roundtrip():
    for mode in ("legacy_v1", "structured_v1", "pickle_stable_v1"):
        payload, decoded = _roundtrip_payload(mode)
        assert decoded["frame"].equals(payload["frame"])
        assert np.array_equal(decoded["array"], payload["array"])
        if mode != "legacy_v1":
            assert decoded["blob"] == payload["blob"]


def test_transport_payload_bytes_modes_roundtrip():
    payload = {"frame": pd.DataFrame({"a": [1, 2]}), "value": 3}
    for mode in ("legacy_v1", "structured_v1", "pickle_stable_v1"):
        transport = encode_transport_payload_bytes(payload, mode=mode, context="service_owner")
        decoded = decode_transport_payload_bytes(
            transport.codec,
            transport.version,
            transport.payload,
            context="service_owner",
        )
        assert decoded["frame"].equals(payload["frame"])
        assert decoded["value"] == 3


def test_pickle_stable_v1_transport_adapts_raw_codec_bytes_for_json_container():
    array = np.array([[1, 2], [3, 4]], dtype=np.int64)

    normalized = normalize_for_pickle_stable(array)
    assert normalized["__codec__"] == "np.ndarray.v1"
    assert isinstance(normalized["data"], bytes)

    encoded = encode_transport_value(array, mode="pickle_stable_v1", context="test payload")
    envelope = encoded[TRANSPORT_ENVELOPE_SENTINEL]
    assert envelope["codec"] == "pickle_stable_v1"
    assert envelope["payload"]["encoding"] == "base64"
    assert isinstance(envelope["payload"]["data"], str)


def test_http_request_body_respects_structured_mode(monkeypatch):
    monkeypatch.setenv("PYCLOUD_SERIALIZATION_MODE", "structured_v1")
    reload_config()
    payload = {"blob": b"abc", "value": 1}
    encoded = _serialize_http_call_payload(payload, context="test payload")
    body = json.dumps(serialize_arrow_compatible(encoded), ensure_ascii=False).encode("utf-8")
    decoded = _decode_http_request_body(body, context="test payload")
    assert decoded == payload


def test_http_response_body_respects_pickle_stable_mode(monkeypatch):
    monkeypatch.setenv("PYCLOUD_SERIALIZATION_MODE", "pickle_stable_v1")
    reload_config()
    payload = {"frame": pd.DataFrame({"a": [1, 2]})}
    encoded = encode_result_for_transport(
        payload,
        policy=get_payload_policy("result"),
        mode="pickle_stable_v1",
    )
    body = json.dumps(serialize_arrow_compatible({"ok": True, "data": encoded}), ensure_ascii=False).encode("utf-8")
    decoded = _normalize_http_response_body(json.loads(body.decode("utf-8")))
    assert decoded["data"]["frame"].equals(payload["frame"])


def test_gateway_public_decode_rejects_pickle_transport():
    payload = {"frame": pd.DataFrame({"a": [1, 2]})}
    encoded = encode_payload_for_transport(
        payload,
        policy=get_payload_policy("http_call"),
        mode="pickle_stable_v1",
    )
    try:
        decode_payload_from_transport(
            encoded,
            policy=get_payload_policy("http_call"),
            mode="pickle_stable_v1",
            context="gateway_public",
        )
    except ValueError as exc:
        assert "gateway_public" in str(exc)
        assert "pickle_stable_v1" in str(exc)
    else:
        raise AssertionError("expected gateway_public pickle decode to be rejected")


def test_gateway_public_decode_rejects_pickle_transport_payload_bytes():
    transport = encode_transport_payload_bytes(
        {"value": 1},
        mode="pickle_stable_v1",
        context="service_owner",
    )
    try:
        decode_transport_payload_bytes(
            transport.codec,
            transport.version,
            transport.payload,
            context="gateway_public",
        )
    except ValueError as exc:
        assert "gateway_public" in str(exc)
        assert "pickle_stable_v1" in str(exc)
    else:
        raise AssertionError("expected gateway_public pickle bytes decode to be rejected")


def test_service_owner_decode_accepts_pickle_transport():
    payload = {"frame": pd.DataFrame({"a": [1, 2]})}
    encoded = encode_payload_for_transport(
        payload,
        policy=get_payload_policy("http_call"),
        mode="pickle_stable_v1",
    )
    decoded = decode_payload_from_transport(
        encoded,
        policy=get_payload_policy("http_call"),
        mode="pickle_stable_v1",
        context="service_owner",
    )
    assert decoded["frame"].equals(payload["frame"])
