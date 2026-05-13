from __future__ import annotations

from email.message import Message
import json
from urllib.request import Request, urlopen

import pandas as pd

from pycloud_parallel.controlplane.client_transport import (
    _decode_http_response_with_headers,
    _encode_http_json_body,
    _encode_http_transport_body,
    _iter_route_http_stream,
    _serialize_http_call_payload,
)
from pycloud_parallel.controlplane.gateway_http import GatewayHttpApp
from pycloud_parallel.controlplane.http_gateway import ServiceHttpGateway, StreamingHttpResponse
from pycloud_parallel.controlplane.serialization import (
    INLINE_TRANSPORT_CARRIER_SENTINEL,
    _adapt_blob_for_json_transport,
    decode_inline_transport_carrier,
    encode_transport_payload_bytes,
    is_inline_transport_carrier,
    serialize_arrow_compatible,
    transport_payload_to_inline_carrier,
)


def test_service_http_gateway_root_returns_help_page():
    gateway = ServiceHttpGateway(
        bind="127.0.0.1:0",
        invoke_handler=lambda *_args: (200, {"ok": True}),
        status_handler=lambda service_id: (200, {"ok": True, "service_id": service_id}),
    )
    gateway.start()
    try:
        with urlopen(f"{gateway.base_url}/", timeout=6.0) as resp:
            body = resp.read().decode("utf-8")
            content_type = resp.headers.get("Content-Type", "")
        assert resp.status == 200
        assert "text/html" in content_type
        assert "PyCloud service HTTP gateway" in body
        assert "/svc/{service_id}/call/{method}" in body
    finally:
        gateway.stop()


def test_service_http_gateway_roundtrips_pickle_raw_bytes_body():
    captured = {}

    def _invoke(service_id, method, payload, token, timeout_sec, serialization_mode, use_transport_result, stream_response):
        captured["service_id"] = service_id
        captured["method"] = method
        captured["payload"] = dict(payload)
        captured["token"] = token
        captured["timeout_sec"] = timeout_sec
        captured["serialization_mode"] = serialization_mode
        captured["use_transport_result"] = use_transport_result
        captured["stream_response"] = stream_response
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
        assert captured["use_transport_result"] is True
        assert captured["stream_response"] is False
        assert decoded["data"] == {"value": 5}
    finally:
        gateway.stop()


def test_service_http_gateway_roundtrips_pickle_native_raw_bytes_body():
    captured = {}

    def _invoke(service_id, method, payload, token, timeout_sec, serialization_mode, use_transport_result, stream_response):
        del service_id, method, token, timeout_sec, stream_response
        captured["payload"] = payload
        captured["serialization_mode"] = serialization_mode
        captured["use_transport_result"] = use_transport_result
        return 200, {"ok": True, "data": {"frame": payload["frame"]}}

    gateway = ServiceHttpGateway(
        bind="127.0.0.1:0",
        invoke_handler=_invoke,
        status_handler=lambda service_id: (200, {"ok": True, "service_id": service_id}),
    )
    frame = pd.DataFrame({"param": ["窗口", "阈值"], "value": [{"n": 20}, [1, 2]]})
    gateway.start()
    try:
        body, headers, _codec = _encode_http_transport_body(
            {"frame": frame},
            context="service_internal",
            mode="pickle_native_v1",
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
        assert captured["serialization_mode"] == "pickle_native_v1"
        assert captured["use_transport_result"] is True
        assert captured["payload"]["frame"].equals(frame)
        assert decoded["data"]["frame"].equals(frame)
    finally:
        gateway.stop()


def test_service_http_gateway_keeps_json_transport_compatible():
    def _invoke(service_id, method, payload, token, timeout_sec, serialization_mode, use_transport_result, stream_response):
        del service_id, method, token, timeout_sec, serialization_mode, use_transport_result, stream_response
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


def test_route_http_stream_restores_nested_dataframe_event():
    frame = pd.DataFrame({"param": ["参数", "strategy"], "value": ["窗口", "demo"]})

    def _invoke(service_id, method, payload, token, timeout_sec, serialization_mode, use_transport_result, stream_response):
        del service_id, method, payload, token, timeout_sec, serialization_mode, use_transport_result
        assert stream_response is True
        event = {
            "event": "item",
            "index": 0,
            "data": {"chunk": serialize_arrow_compatible(frame), "meta": {"rows": len(frame)}},
        }
        done = {"event": "done", "ok": True, "item_count": 1}
        return StreamingHttpResponse(
            status_code=200,
            body_iter=[
                json.dumps(serialize_arrow_compatible(event), ensure_ascii=False).encode("utf-8") + b"\n",
                json.dumps(done).encode("utf-8") + b"\n",
            ],
        )

    gateway = ServiceHttpGateway(
        bind="127.0.0.1:0",
        invoke_handler=_invoke,
        status_handler=lambda service_id: (200, {"ok": True, "service_id": service_id}),
    )
    gateway.start()
    try:
        route = type("Route", (), {"http_base_url": f"{gateway.base_url}/svc/svc-1"})()
        events = list(
            _iter_route_http_stream(
                route,
                method="frames",
                payload={},
                timeout_sec=5.0,
                service_token="",
                serialization_mode="legacy_v1",
            )
        )
    finally:
        gateway.stop()

    assert events[1] == {"event": "done", "ok": True, "item_count": 1}
    item = events[0]["data"]
    assert item["meta"] == {"rows": 2}
    assert isinstance(item["chunk"], pd.DataFrame)
    assert item["chunk"].equals(frame)


def test_route_http_stream_restores_pickle_carrier_dataframe_event():
    frame = pd.DataFrame({"param": ["参数", "strategy"], "value": ["窗口", "demo"]})

    transport = encode_transport_payload_bytes(
        {"chunk": frame, "meta": {"rows": len(frame)}},
        mode="pickle_stable_v1",
        context="service stream item",
    )
    carrier = transport_payload_to_inline_carrier(
        transport,
        payload_mode="result",
        context="service_result",
    )
    carrier_meta = dict(carrier[INLINE_TRANSPORT_CARRIER_SENTINEL])
    carrier_meta["content_bytes"] = _adapt_blob_for_json_transport(carrier_meta["content_bytes"])
    json_carrier = {INLINE_TRANSPORT_CARRIER_SENTINEL: carrier_meta}

    def _invoke(service_id, method, payload, token, timeout_sec, serialization_mode, use_transport_result, stream_response):
        del service_id, method, payload, token, timeout_sec
        assert serialization_mode == "pickle_stable_v1"
        assert use_transport_result is True
        assert stream_response is True
        event = {"event": "item", "index": 0, "data": json_carrier}
        done = {"event": "done", "ok": True, "item_count": 1}
        return StreamingHttpResponse(
            status_code=200,
            body_iter=[
                json.dumps(serialize_arrow_compatible(event), ensure_ascii=False).encode("utf-8") + b"\n",
                json.dumps(done).encode("utf-8") + b"\n",
            ],
        )

    gateway = ServiceHttpGateway(
        bind="127.0.0.1:0",
        invoke_handler=_invoke,
        status_handler=lambda service_id: (200, {"ok": True, "service_id": service_id}),
    )
    gateway.start()
    try:
        route = type("Route", (), {"http_base_url": f"{gateway.base_url}/svc/svc-1"})()
        events = list(
            _iter_route_http_stream(
                route,
                method="frames",
                payload={},
                timeout_sec=5.0,
                service_token="",
                serialization_mode="pickle_stable_v1",
            )
        )
    finally:
        gateway.stop()

    assert is_inline_transport_carrier(events[0]["data"])
    item = decode_inline_transport_carrier(events[0]["data"], context="service_result")
    assert item["meta"] == {"rows": 2}
    assert isinstance(item["chunk"], pd.DataFrame)
    assert item["chunk"].equals(frame)


def test_gateway_public_rejects_pickle_raw_bytes_body():
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


def test_gateway_public_rejects_pickle_native_raw_bytes_body():
    class _RouteCache:
        def start(self) -> None:
            return None

        def stop(self) -> None:
            return None

    app = GatewayHttpApp(route_cache=_RouteCache(), timeout_sec=2.0)
    app.start()
    try:
        body, _headers, _codec = _encode_http_transport_body(
            {"value": 4},
            context="service_internal",
            mode="pickle_native_v1",
        )
        headers = Message()
        headers["Content-Type"] = "application/x-pycloud-transport"
        headers["X-Pycloud-Codec"] = "pickle_native_v1"
        headers["X-Pycloud-Transport-Version"] = "1"

        code, response = app.handle_post(
            path="/svc/svc-demo/call/run?timeout_sec=5.000",
            headers=headers,
            body=body,
        )

        assert code == 400
        assert "gateway_public" in str(response.get("error", ""))
        assert "pickle_native_v1" in str(response.get("error", ""))
    finally:
        app.stop()
