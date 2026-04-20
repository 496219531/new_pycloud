from __future__ import annotations

from email.message import Message
import json
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.client_transport import (
    _decode_http_response_with_headers,
    _encode_http_json_body,
    _encode_http_transport_body,
    _serialize_http_call_payload,
)
from pycloud_parallel.controlplane.gateway_http import GatewayHttpApp
from pycloud_parallel.controlplane.http_gateway import ServiceHttpGateway
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible


def test_service_http_gateway_roundtrips_pickle_bytes_transport():
    captured = {}

    def _invoke(service_id, method, payload, token, timeout_sec, serialization_mode):
        captured["service_id"] = service_id
        captured["method"] = method
        captured["payload"] = dict(payload)
        captured["token"] = token
        captured["timeout_sec"] = timeout_sec
        captured["serialization_mode"] = serialization_mode
        return 200, {"ok": True, "data": {"value": int(payload["value"]) + 1}}

    gateway = ServiceHttpGateway(
        bind="127.0.0.1:0",
        invoke_handler=_invoke,
        status_handler=lambda service_id: (200, {"ok": True, "service_id": service_id}),
    )
    gateway.start()
    try:
        body, headers, _codec = _encode_http_transport_body(
            {"value": 4},
            context="service_internal",
            mode="pickle_stable_v1",
        )
        req = Request(
            url=f"{gateway.base_url}/svc/svc-1/call/run?timeout_sec=5.000",
            method="POST",
            headers=headers,
            data=body,
        )
        with urlopen(req, timeout=6.0) as resp:
            raw = resp.read()
            decoded = _decode_http_response_with_headers(raw, headers=resp.headers)
        assert captured["payload"] == {"value": 4}
        assert captured["serialization_mode"] == "pickle_stable_v1"
        assert decoded["data"] == {"value": 5}
    finally:
        gateway.stop()


def test_service_http_gateway_keeps_json_transport_compatible():
    def _invoke(service_id, method, payload, token, timeout_sec, serialization_mode):
        del service_id, method, token, timeout_sec, serialization_mode
        return 200, {"ok": True, "data": {"value": int(payload["value"]) * 2}}

    gateway = ServiceHttpGateway(
        bind="127.0.0.1:0",
        invoke_handler=_invoke,
        status_handler=lambda service_id: (200, {"ok": True, "service_id": service_id}),
    )
    gateway.start()
    try:
        payload = _serialize_http_call_payload({"value": 6}, context="service call payload", mode="legacy_v1")
        req = Request(
            url=f"{gateway.base_url}/svc/svc-1/call/run?timeout_sec=5.000",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=_encode_http_json_body(payload),
        )
        with urlopen(req, timeout=6.0) as resp:
            body = json.loads((resp.read() or b"{}").decode("utf-8"))
        assert body["ok"] is True
        assert body["data"] == {"value": 12}
    finally:
        gateway.stop()


def test_gateway_public_rejects_pickle_bytes_transport():
    class _RouteCache:
        def start(self) -> None:
            return None

        def stop(self) -> None:
            return None

    route_cache = _RouteCache()
    app = GatewayHttpApp(route_cache=route_cache, timeout_sec=2.0)
    app.start()
    try:
        body, _headers, _codec = _encode_http_transport_body(
            {"value": 4},
            context="service_internal",
            mode="pickle_stable_v1",
        )
        headers = Message()
        headers["Content-Type"] = "application/x-pycloud-transport"
        headers["X-Pycloud-Codec"] = "pickle_stable_v1"
        headers["X-Pycloud-Transport-Version"] = "1"

        code, response = app.handle_post(
            path="/svc/svc-demo/call/run?timeout_sec=5.000",
            headers=headers,
            body=body,
        )

        assert code == 400
        assert "gateway_public" in str(response.get("error", ""))
        assert "pickle_stable_v1" in str(response.get("error", ""))
    finally:
        app.stop()
