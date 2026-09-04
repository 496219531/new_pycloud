from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd

from pycloud_parallel.api import Service
from pycloud_parallel.controlplane.client_transport import (
    _call_route_http,
    _decode_http_request_body_with_mode,
    _decode_http_transport_request_body_with_mode,
    _encode_http_transport_response_body,
    _is_http_transport_content_type,
)
from pycloud_parallel.controlplane.infocenter_client import InfoCenterServiceRoute
from pycloud_parallel.controlplane.http_connection_pool import BufferedHttpResponse
from pycloud_parallel.controlplane.payload_transport import encode_result_for_transport
from pycloud_parallel.controlplane.config import get_payload_policy
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def _assert_payload_roundtrip(actual: object, expected: object) -> None:
    if isinstance(expected, pd.DataFrame):
        assert isinstance(actual, pd.DataFrame)
        assert actual.equals(expected)
        return
    if isinstance(expected, pd.Series):
        assert isinstance(actual, pd.Series)
        assert actual.equals(expected)
        return
    if isinstance(expected, np.ndarray):
        assert isinstance(actual, np.ndarray)
        assert np.array_equal(actual, expected)
        return
    assert actual == expected


def _demo_payload(mode: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "frame": pd.DataFrame({"a": [1, 2]}),
        "array": np.array([1, 2, 3], dtype=np.int64),
    }
    if mode != "legacy_v1":
        payload["blob"] = b"abc"
    return payload


def _demo_result(mode: str) -> dict[str, object]:
    result: dict[str, object] = {
        "frame": pd.DataFrame({"b": [3, 4]}),
        "array": np.array([[5, 6], [7, 8]], dtype=np.int64),
    }
    if mode != "legacy_v1":
        result["blob"] = b"xyz"
    return result


def test_call_route_http_roundtrips_transport_modes(monkeypatch):
    route = SimpleNamespace(
        http_base_url="http://127.0.0.1:18081/svc/demo",
        control_addr="",
    )

    for mode in ("legacy_v1", "structured_v1", "pickle_stable_v1", "pickle_native_v1"):
        payload = _demo_payload(mode)
        result = _demo_result(mode)

        def _fake_request(**kwargs):
            headers = dict(kwargs.get("headers") or {})
            request_body = kwargs.get("body") or b""
            content_type = headers.get("Content-Type", "") or headers.get("Content-type", "")
            if _is_http_transport_content_type(content_type):
                decoded_payload, request_mode = _decode_http_transport_request_body_with_mode(
                    request_body,
                    headers=headers,
                    context="service_internal",
                )
            else:
                decoded_payload, request_mode = _decode_http_request_body_with_mode(
                    request_body or b"{}",
                    context="service call payload",
                )
            assert request_mode == mode
            _assert_payload_roundtrip(decoded_payload["frame"], payload["frame"])
            _assert_payload_roundtrip(decoded_payload["array"], payload["array"])
            if mode != "legacy_v1":
                assert decoded_payload["blob"] == payload["blob"]
            if request_mode == "pickle_stable_v1":
                raw, response_headers = _encode_http_transport_response_body(
                    result,
                    context="service_result",
                    mode=request_mode,
                )
                return BufferedHttpResponse(200, "OK", response_headers, raw)
            body = {
                "ok": True,
                "data": encode_result_for_transport(
                    result,
                    policy=get_payload_policy("result"),
                    mode=request_mode,
                ),
            }
            raw = json.dumps(serialize_arrow_compatible(body), ensure_ascii=False).encode("utf-8")
            return BufferedHttpResponse(200, "OK", {"Content-Type": "application/json; charset=utf-8"}, raw)

        monkeypatch.setattr("pycloud_parallel.controlplane.client_transport.pooled_http_request", _fake_request)
        response = _call_route_http(
            route,
            method="run",
            payload=payload,
            timeout_sec=5.0,
            service_token="",
            serialization_mode=mode,
        )
        restored = response["data"]
        _assert_payload_roundtrip(restored["frame"], result["frame"])
        _assert_payload_roundtrip(restored["array"], result["array"])
        if mode != "legacy_v1":
            assert restored["blob"] == result["blob"]


def test_service_connect_propagates_serialization_mode_to_transport_client():
    for mode in ("legacy_v1", "structured_v1", "pickle_stable_v1", "pickle_native_v1"):
        route = InfoCenterServiceRoute(
            service_name="svc-demo",
            service_id="svc-id-1",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_instance_id="node-1-inst",
            node_id="node-1",
            control_addr="",
            node_healthy=True,
            worker_count=1,
            alive_workers=1,
            in_flight=0,
            lease_expire_at=pd.Timestamp.utcnow().to_pydatetime(),
            http_base_url="http://127.0.0.1:18081/svc/demo",
        )
        client = Service.connect(
            target="127.0.0.1:50051",
            service_name="svc-demo",
            route="discovery",
            serialization_mode=mode,
            validate_on_init=False,
        )
        try:
            with (
                patch.object(type(client), "list_methods", return_value=[{"method": "echo"}]),
                patch.object(client._route_cache, "select_route", return_value=route),
                patch.object(client._route_cache, "mark_success", return_value=None),
                patch(
                    "pycloud_parallel.execution.service_session._ConnectedService._prepare_discovery_route_payload",
                    return_value={"value": 1},
                ),
                patch(
                    "pycloud_parallel.controlplane.discovery_client.client_mod._call_route_http",
                    return_value={"ok": True, "data": {"mode": mode}},
                ) as mocked_call,
                ):
                    result = client.echo.sync(value=1)
            assert result == {"mode": mode}
            if mode == "legacy_v1":
                assert "serialization_mode" not in mocked_call.call_args.kwargs
            else:
                assert mocked_call.call_args.kwargs["serialization_mode"] == mode
        finally:
            client.close()
