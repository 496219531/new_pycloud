from __future__ import annotations

import base64
from dataclasses import replace
from datetime import datetime, timezone
from email.message import Message
import io
import json
from pathlib import Path
import time
from typing import Tuple
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from pycloud_parallel.controlplane.client_transport import (
    _decode_http_request_body_with_mode,
    _encode_http_json_body,
    _encode_http_transport_body,
    _serialize_http_call_payload,
)
from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient
from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient, InfoCenterServiceRoute
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.gateway_http import EXTERNAL_DATA_REF_ERROR, GatewayCallError, GatewayHttpApp
from pycloud_parallel.controlplane.gateway_stage import GatewayStageManager
from pycloud_parallel.controlplane.gateway_cache import GatewayRouteCache
from pycloud_parallel.data.ref import DataRef
from pycloud_parallel.controlplane.effective_policy import resolve_effective_policy
from pycloud_parallel.controlplane.policy_profile import get_policy_profile
from pycloud_parallel.controlplane.server import (
    build_controlplane_server,
    build_gateway_server,
    build_infocenter_server,
    build_job_orchestrator_server,
)
from pycloud_parallel.controlplane.node_control_http import NodeControlHttpServer
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def _http_json(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(
        url=url,
        method=method,
        headers={"Content-Type": "application/json"},
        data=body,
    )
    with urlopen(req, timeout=10.0) as resp:
        return int(resp.status), json.loads(resp.read().decode("utf-8") or "{}")


def _wait_until(predicate, timeout_sec: float = 5.0, interval_sec: float = 0.1) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_sec)
    return False


def _start_nodecontrol_server(node_id: str, artifact_dir: str) -> Tuple[NodeControlHttpServer, str, NodeControlState]:
    state = NodeControlState(
        node_id=node_id,
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=artifact_dir,
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=1,
    )
    server = NodeControlHttpServer(bind="127.0.0.1:0", state=state)
    server.start()
    return server, server.base_url, state


def _gateway_route_variant(index: int, *, service_name: str = "svc-gateway-retry") -> InfoCenterServiceRoute:
    return InfoCenterServiceRoute(
        service_name=service_name,
        service_id=f"svc-gw-{index}",
        status=pb2.SERVICE_STATUS_RUNNING,
        node_instance_id=f"node-gw-{index}-inst",
        node_id=f"node-gw-{index}",
        control_addr=f"127.0.0.1:{50060 + index}",
        node_healthy=True,
        worker_count=1,
        alive_workers=1,
        in_flight=0,
        lease_expire_at=datetime.now(timezone.utc),
        http_base_url=f"http://127.0.0.1:{18080 + index}/svc/svc-gw-{index}",
    )


def test_gateway_http_only_route_marks_data_ref_as_service_http() -> None:
    source_route = _gateway_route_variant(1, service_name="svc-startup-http")
    route = replace(source_route, control_addr="")
    calls = []
    app = GatewayHttpApp(
        route_cache=_SequenceRouteCache([route]),
        controlplane_target="127.0.0.1:50051",
        register_data_ref=lambda **kwargs: calls.append(kwargs),
    )

    ref = DataRef(
        ref_id="sha256:1111111111111111111111111111111111111111111111111111111111111111",
        materialize_as="text",
    )
    body = app._attach_controlplane_locator({"ok": True, "data": ref}, route=route)  # noqa: SLF001

    updated = body["data"]
    assert updated.locator_kind == "service_http"
    assert updated.locator_token == route.http_base_url
    assert updated.node_instance_id == route.node_instance_id
    assert calls == []


def test_gateway_attaches_result_ref_locator_without_materializing() -> None:
    route = _gateway_route_variant(2, service_name="svc-result-ref")
    calls = []
    app = GatewayHttpApp(
        route_cache=_SequenceRouteCache([route]),
        controlplane_target="127.0.0.1:50051",
        register_data_ref=lambda **kwargs: calls.append(kwargs),
    )

    ref = DataRef(
        ref_id="sha256:2222222222222222222222222222222222222222222222222222222222222222",
        storage_id="sha256:2222222222222222222222222222222222222222222222222222222222222222",
        format="bin",
        size_bytes=12,
        materialize_as="bytes",
        locator_kind="node_local",
    )

    body = app._attach_controlplane_locator({"ok": True, "data": ref}, route=route)  # noqa: SLF001

    updated = body["data"]
    assert updated.object_id == ref.object_id
    assert updated.locator_kind == "node_control"
    assert updated.locator_token == route.control_addr
    assert updated.control_addr == route.control_addr
    assert updated.node_id == route.node_id
    assert updated.node_instance_id == route.node_instance_id
    assert calls
    assert calls[0]["control_addr"] == route.control_addr


class _NeverRouteCache:
    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None

    def select_route(self, service_name: str, exclude_service_ids=None, force_refresh: bool = False):
        raise AssertionError(f"route selection should not run: {service_name}")


def _gateway_public_post_payload(payload: dict) -> tuple[int, dict]:
    headers = Message()
    headers["Content-Type"] = "application/json"
    app = GatewayHttpApp(route_cache=_NeverRouteCache(), controlplane_target="127.0.0.1:50051")
    app.start()
    try:
        return app.handle_post(
            path="/svc/svc-public/call/run?timeout_sec=5.000",
            headers=headers,
            body=_encode_http_json_body(payload),
        )
    finally:
        app.stop()


def test_gateway_public_json_call_rejects_top_level_data_ref() -> None:
    ref = DataRef(
        ref_id="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        storage_id="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        format="bin",
        size_bytes=3,
    )

    code, body = _gateway_public_post_payload(ref.to_payload())

    assert code == 400
    assert body["error"] == EXTERNAL_DATA_REF_ERROR


def test_gateway_public_json_call_rejects_nested_data_ref() -> None:
    ref = DataRef(
        ref_id="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        storage_id="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        format="bin",
        size_bytes=3,
    )

    code, body = _gateway_public_post_payload({"items": [{"blob": ref.to_payload()}]})

    assert code == 400
    assert body["error"] == EXTERNAL_DATA_REF_ERROR


def test_gateway_public_json_call_rejects_legacy_data_ref_payload() -> None:
    code, body = _gateway_public_post_payload(
        {
            "blob": {
                "ref_id": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "storage_id": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "locator_kind": "node_control",
                "control_addr": "127.0.0.1:50061",
                "format": "bin",
            }
        }
    )

    assert code == 400
    assert body["error"] == EXTERNAL_DATA_REF_ERROR


def test_gateway_public_json_call_rejects_external_data_ref_locator_before_route_selection() -> None:
    code, body = _gateway_public_post_payload(
        {
            "blob": {
                "object_id": "sha256:edededededededededededededededededededededededededededededededed",
                "locator_kind": "node_control",
                "locator_token": "10.0.0.10:50061",
                "control_addr": "10.0.0.10:50061",
            }
        }
    )

    assert code == 400
    assert body["error"] == EXTERNAL_DATA_REF_ERROR


def test_gateway_public_http_bytes_transport_rejects_data_ref() -> None:
    ref = DataRef(
        ref_id="sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        storage_id="sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        format="bin",
        size_bytes=3,
    )
    body, transport_headers, _codec = _encode_http_transport_body(
        {"blob": ref},
        context="service_internal",
        mode="structured_v1",
    )
    headers = Message()
    for key, value in transport_headers.items():
        headers[key] = value
    app = GatewayHttpApp(route_cache=_NeverRouteCache(), controlplane_target="127.0.0.1:50051")
    app.start()
    try:
        code, response = app.handle_post(
            path="/svc/svc-public/call/run?timeout_sec=5.000",
            headers=headers,
            body=body,
        )
    finally:
        app.stop()

    assert code == 400
    assert response["error"] == EXTERNAL_DATA_REF_ERROR


def test_internal_http_decode_still_allows_system_data_ref() -> None:
    ref = DataRef(
        ref_id="sha256:abababababababababababababababababababababababababababababababab",
        storage_id="sha256:abababababababababababababababababababababababababababababababab",
        format="bin",
        size_bytes=3,
    )
    payload = _serialize_http_call_payload(
        {"blob": ref},
        context="service call payload",
        mode="legacy_v1",
    )

    decoded, _mode = _decode_http_request_body_with_mode(
        _encode_http_json_body(payload),
        context="service_internal",
    )

    assert decoded["blob"] == ref


class _SequenceRouteCache:
    def __init__(self, routes):
        self.routes = list(routes)
        self.failures = []
        self.successes = []
        self.releases = []
        self.observations = []
        self.refreshes = []

    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None

    def select_route(self, service_name: str, exclude_service_ids=None, force_refresh: bool = False):
        del service_name, force_refresh
        excluded = set(exclude_service_ids or ())
        for route in self.routes:
            if route.service_id not in excluded:
                return route
        raise RuntimeError("no available route")

    def refresh(self, service_name: str, force: bool = False):
        self.refreshes.append((service_name, force))
        return list(self.routes)

    def mark_success(self, route) -> None:
        self.successes.append(route.service_id)

    def mark_failure(self, route, error: str) -> None:
        self.failures.append((route.service_id, error))

    def release_route(self, route) -> None:
        self.releases.append(route.service_id)

    def record_call_observation(self, service_name: str, **kwargs) -> None:
        self.observations.append((service_name, kwargs))


def _create_exported_service(target: str, service_name: str) -> str:
    blob = (
        b"def pycloud_export(fn):\n"
        b"    fn.__pycloud_export__ = True\n"
        b"    return fn\n\n"
        b"@pycloud_export\n"
        b"def add(value=0, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'plus_one': value + 1}\n\n"
        b"@pycloud_export\n"
        b"def mul(value=0, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'square': value * value}\n"
    )
    with NodeControlClient(target, timeout_sec=10.0) as client:
        session = client.create_service_from_bytes(
            owner_client_id=f"owner-{service_name}",
            service_name=service_name,
            blob=blob,
            runtime="py3",
            entry_module=service_name,
            entry_callable="add",
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=True,
        )
    return session.service_id


def _create_uploaded_file_service(target: str, service_name: str) -> str:
    blob = (
        b"from pathlib import Path\n\n"
        b"def pycloud_export(fn):\n"
        b"    fn.__pycloud_export__ = True\n"
        b"    return fn\n\n"
        b"@pycloud_export\n"
        b"def inspect(doc=None, user_id='', **_kwargs):\n"
        b"    path = Path(doc)\n"
        b"    text = path.read_text(encoding='utf-8')\n"
        b"    return {\n"
        b"        'name': path.name,\n"
        b"        'suffix': path.suffix,\n"
        b"        'size': path.stat().st_size,\n"
        b"        'preview': text[:32],\n"
        b"        'user_id': user_id,\n"
        b"    }\n"
    )
    with NodeControlClient(target, timeout_sec=10.0) as client:
        session = client.create_service_from_bytes(
            owner_client_id=f"owner-{service_name}",
            service_name=service_name,
            blob=blob,
            runtime="py3",
            entry_module=service_name,
            entry_callable="inspect",
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=True,
        )
    return session.service_id


def _register_node_with_services(
    info_target: str,
    *,
    node_id: str,
    control_addr: str,
    state: NodeControlState,
) -> None:
    with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
        infocenter.register_node(
            node_id=node_id,
            control_addr=control_addr,
            capacity=8,
            queue_capacity=64,
            tags=["compute"],
            services=state.service_report_payloads(),
            service_worker_capacity=state.worker_capacity,
            service_worker_used=state.service_worker_used(),
        )


def test_controlplane_embeds_gateway_for_service_calls(tmp_path):
    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-gw-01", str(tmp_path / "node_gw_01"))

    try:
        service_id = _create_exported_service(node_target, "svc_gateway_controlplane")
        _register_node_with_services(
            controlplane.base_url,
            node_id="node-gw-01",
            control_addr=node_target,
            state=node_state,
        )

        assert _wait_until(
            lambda: len(
                InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_gateway_controlplane",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 1
        )

        code, call_resp = _http_json(
            "POST",
            f"{controlplane.base_url}/svc/svc_gateway_controlplane/call/add",
            {"value": 7},
        )
        assert code == 200
        assert call_resp["ok"] is True
        assert call_resp["data"]["plus_one"] == 8

        code, methods_resp = _http_json(
            "GET",
            f"{controlplane.base_url}/svc/svc_gateway_controlplane/methods?include_docs=false",
        )
        assert code == 200
        assert sorted(item["method"] for item in methods_resp["methods"]) == ["add", "mul"]
        assert methods_resp["service_id"] == service_id

        code, status_resp = _http_json(
            "GET",
            f"{controlplane.base_url}/svc/svc_gateway_controlplane/status",
        )
        assert code == 200
        assert status_resp["ok"] is True
        assert status_resp["service_name"] == "svc_gateway_controlplane"
        assert status_resp["route_count"] == 1
        assert status_resp["routes"][0]["service_id"] == service_id
        assert "predicted_busy" in status_resp["routes"][0]

        with GatewayServiceClient(controlplane.base_url, timeout_sec=5.0) as gateway:
            methods = gateway.list_methods(service_name="svc_gateway_controlplane", include_docs=False)
            assert sorted(item["method"] for item in methods) == ["add", "mul"]
            body = gateway.call(service_name="svc_gateway_controlplane", method="mul", payload={"value": 6}, timeout_sec=5.0)
            assert body["data"]["square"] == 36
            bytes_body = gateway.call(
                service_name="svc_gateway_controlplane",
                method="mul",
                payload={"value": 9},
                timeout_sec=5.0,
                serialization_mode="structured_v1",
                effective_policy=resolve_effective_policy(
                    get_policy_profile("trusted_internal"),
                    requested_mode="structured_v1",
                    context="gateway_public",
                ),
            )
            assert bytes_body["data"]["square"] == 81

        with GatewayServiceClient(controlplane.base_url, timeout_sec=5.0) as module_client:
            assert sorted(item["method"] for item in module_client.list_methods(service_name="svc_gateway_controlplane", include_docs=False)) == ["add", "mul"]
            assert module_client.call(service_name="svc_gateway_controlplane", method="add", payload={"value": 10}, timeout_sec=5.0)["data"] == {"value": 10, "plus_one": 11}
            assert module_client.call(service_name="svc_gateway_controlplane", method="mul", payload={"value": 8}, timeout_sec=5.0)["data"] == {"value": 8, "square": 64}
    finally:
        node_server.stop()
        node_state.close()
        controlplane.stop()


def test_controlplane_embeds_gateway_for_upload_call(tmp_path, monkeypatch):
    stage_dir = tmp_path / "gateway_stage_controlplane"
    monkeypatch.setenv("PYCLOUD_GATEWAY_STAGE_DIR", str(stage_dir))
    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-gw-upload-01", str(tmp_path / "node_gw_upload_01"))

    try:
        service_id = _create_uploaded_file_service(node_target, "svc_gateway_upload_controlplane")
        _register_node_with_services(
            controlplane.base_url,
            node_id="node-gw-upload-01",
            control_addr=node_target,
            state=node_state,
        )
        assert service_id
        assert _wait_until(
            lambda: len(
                InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_gateway_upload_controlplane",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 1
        )

        upload_path = tmp_path / "input_upload.txt"
        upload_path.write_text("hello gateway upload\nsecond line", encoding="utf-8")
        with GatewayServiceClient(controlplane.base_url, timeout_sec=5.0) as gateway:
            body = gateway.upload_call(
                service_name="svc_gateway_upload_controlplane",
                method="inspect",
                payload={"doc": {"kind": "uploaded_file", "slot": "input_txt"}, "user_id": "u1"},
                files={"input_txt": upload_path},
                timeout_sec=5.0,
            )

        assert body["ok"] is True
        assert body["data"]["suffix"] == ".txt"
        assert body["data"]["size"] == upload_path.stat().st_size
        assert body["data"]["preview"].startswith("hello gateway upload")
        assert body["data"]["user_id"] == "u1"
        requests_dir = stage_dir / "requests"
        assert not requests_dir.exists() or not any(requests_dir.iterdir())
    finally:
        node_server.stop()
        node_state.close()
        controlplane.stop()


def test_standalone_gateway_upload_call_supports_file_map(tmp_path, monkeypatch):
    stage_dir = tmp_path / "gateway_stage_standalone"
    monkeypatch.setenv("PYCLOUD_GATEWAY_STAGE_DIR", str(stage_dir))
    infocenter = build_infocenter_server("127.0.0.1:0")
    infocenter.start()
    gateway = build_gateway_server("127.0.0.1:0", infocenter_addr=infocenter.base_url)
    gateway.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-gw-upload-02", str(tmp_path / "node_gw_upload_02"))

    try:
        _create_uploaded_file_service(node_target, "svc_gateway_upload_remote")
        _register_node_with_services(
            infocenter.base_url,
            node_id="node-gw-upload-02",
            control_addr=node_target,
            state=node_state,
        )
        assert _wait_until(
            lambda: len(
                InfoCenterClient(infocenter.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_gateway_upload_remote",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 1
        )

        upload_path = tmp_path / "remote_upload.txt"
        upload_path.write_text("remote gateway upload", encoding="utf-8")
        with GatewayServiceClient(gateway.base_url, timeout_sec=5.0) as client:
            body = client.upload_call(
                service_name="svc_gateway_upload_remote",
                method="inspect",
                payload={"doc": None, "user_id": "u2"},
                file_map={"doc": "input_txt"},
                files={"input_txt": upload_path},
                timeout_sec=5.0,
            )

        assert body["ok"] is True
        assert body["data"]["suffix"] == ".txt"
        assert body["data"]["size"] == upload_path.stat().st_size
        assert body["data"]["preview"] == "remote gateway upload"
        assert body["data"]["user_id"] == "u2"
        requests_dir = stage_dir / "requests"
        assert not requests_dir.exists() or not any(requests_dir.iterdir())
    finally:
        node_server.stop()
        node_state.close()
        gateway.stop()
        infocenter.stop()


@pytest.mark.parametrize(
    ("payload", "file_map", "expected_error"),
    [
        ({"doc": None}, None, "must reference uploaded files"),
        ({"doc": None}, {"doc.path": "input_txt"}, "file_map path not found"),
    ],
)
def test_gateway_upload_call_rejects_invalid_payload_before_upload(
    tmp_path,
    payload,
    file_map,
    expected_error,
):
    stage_manager = GatewayStageManager(root_dir=str(tmp_path / "gateway_stage_invalid"), failure_ttl_sec=600, gc_interval_sec=60)

    class _NeverRouteCache:
        def start(self) -> None:
            return None

        def stop(self) -> None:
            return None

        def select_route(self, service_name: str, exclude_service_ids=None, force_refresh: bool = False):
            raise AssertionError(f"route selection should not run for invalid upload-call payload: {service_name}")

        def mark_success(self, route) -> None:
            del route
            return None

        def mark_failure(self, route, error: str) -> None:
            del route, error
            return None

        def refresh(self, service_name: str, force: bool = False) -> None:
            del service_name, force
            return None

    def _build_raw_upload(*, boundary: str, payload_obj: dict, file_map_obj: dict | None) -> bytes:
        chunks = [
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"payload\"\r\nContent-Type: application/json\r\n\r\n".encode("utf-8"),
            json.dumps(payload_obj).encode("utf-8"),
            b"\r\n",
        ]
        if file_map_obj:
            chunks.extend(
                [
                    f"--{boundary}\r\nContent-Disposition: form-data; name=\"file_map\"\r\nContent-Type: application/json\r\n\r\n".encode("utf-8"),
                    json.dumps(file_map_obj).encode("utf-8"),
                    b"\r\n",
                ]
            )
        chunks.extend(
            [
                f"--{boundary}\r\nContent-Disposition: form-data; name=\"files[input_txt]\"; filename=\"input.txt\"\r\nContent-Type: text/plain\r\n\r\n".encode("utf-8"),
                b"gateway invalid upload",
                b"\r\n",
                f"--{boundary}--\r\n".encode("utf-8"),
            ]
        )
        return b"".join(chunks)

    boundary = "invalid-upload-boundary"
    raw = _build_raw_upload(boundary=boundary, payload_obj=payload, file_map_obj=file_map)
    headers = Message()
    headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    headers["Content-Length"] = str(len(raw))

    app = GatewayHttpApp(route_cache=_NeverRouteCache(), stage_manager=stage_manager, controlplane_target="http://127.0.0.1:50051")
    app.start()
    try:
        code, body = app.handle_post_stream(
            path="/svc/svc-invalid/upload-call/inspect?timeout_sec=5.000",
            headers=headers,
            stream=io.BytesIO(raw),
            content_length=len(raw),
        )
        assert code == 400
        assert expected_error in str(body["error"])
        requests_dir = stage_manager.requests_dir
        assert not requests_dir.exists() or not any(requests_dir.iterdir())
    finally:
        app.stop()


def _build_raw_upload_call(*, boundary: str, payload_obj: dict, file_map_obj: dict | None = None) -> bytes:
    chunks = [
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"payload\"\r\nContent-Type: application/json\r\n\r\n".encode("utf-8"),
        json.dumps(payload_obj).encode("utf-8"),
        b"\r\n",
    ]
    if file_map_obj is not None:
        chunks.extend(
            [
                f"--{boundary}\r\nContent-Disposition: form-data; name=\"file_map\"\r\nContent-Type: application/json\r\n\r\n".encode("utf-8"),
                json.dumps(file_map_obj).encode("utf-8"),
                b"\r\n",
            ]
        )
    chunks.extend(
        [
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"files[input_txt]\"; filename=\"input.txt\"\r\nContent-Type: text/plain\r\n\r\n".encode("utf-8"),
            b"gateway uploaded file",
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )
    return b"".join(chunks)


def test_gateway_upload_call_rejects_payload_data_ref_before_route_selection(tmp_path) -> None:
    ref = DataRef(
        ref_id="sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        storage_id="sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        format="bin",
        size_bytes=3,
    )
    boundary = "upload-dataref-boundary"
    raw = _build_raw_upload_call(boundary=boundary, payload_obj={"doc": ref.to_payload()})
    headers = Message()
    headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    headers["Content-Length"] = str(len(raw))
    stage_manager = GatewayStageManager(root_dir=str(tmp_path / "gateway_stage_dataref"), failure_ttl_sec=600, gc_interval_sec=60)
    app = GatewayHttpApp(route_cache=_NeverRouteCache(), stage_manager=stage_manager, controlplane_target="127.0.0.1:50051")
    app.start()
    try:
        code, body = app.handle_post_stream(
            path="/svc/svc-public/upload-call/inspect?timeout_sec=5.000",
            headers=headers,
            stream=io.BytesIO(raw),
            content_length=len(raw),
        )
    finally:
        app.stop()

    assert code == 400
    assert body["error"] == EXTERNAL_DATA_REF_ERROR
    assert not stage_manager.requests_dir.exists() or not any(stage_manager.requests_dir.iterdir())


def test_gateway_upload_call_allows_raw_file_placeholder(tmp_path, monkeypatch) -> None:
    route = _gateway_route_variant(1, service_name="svc-upload-public")
    stage_manager = GatewayStageManager(root_dir=str(tmp_path / "gateway_stage_file"), failure_ttl_sec=600, gc_interval_sec=60)
    app = GatewayHttpApp(route_cache=_SequenceRouteCache([route]), stage_manager=stage_manager, controlplane_target="127.0.0.1:50051")
    captured = {}
    generated_ref = DataRef(
        ref_id="gateway-upload:req:svc:input_txt",
        storage_id="sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        format="txt",
        size_bytes=21,
        materialize_as="text",
        locator_kind="node_control",
        locator_token=route.control_addr,
        control_addr=route.control_addr,
    )

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.gateway_http.upload_staged_files_to_route",
        lambda **kwargs: {"input_txt": generated_ref},
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.gateway_http.release_uploaded_refs_on_route",
        lambda **kwargs: None,
    )

    def _fake_invoke(route_arg, **kwargs):
        captured["route"] = route_arg
        captured["payload"] = kwargs["payload"]
        return {"ok": True, "data": {"accepted": True}}

    monkeypatch.setattr(app, "_invoke_route", _fake_invoke)
    boundary = "upload-file-boundary"
    raw = _build_raw_upload_call(
        boundary=boundary,
        payload_obj={"doc": {"kind": "uploaded_file", "slot": "input_txt"}},
    )
    headers = Message()
    headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    headers["Content-Length"] = str(len(raw))

    app.start()
    try:
        code, body = app.handle_post_stream(
            path="/svc/svc-upload-public/upload-call/inspect?timeout_sec=5.000",
            headers=headers,
            stream=io.BytesIO(raw),
            content_length=len(raw),
        )
    finally:
        app.stop()

    assert code == 200
    assert body["data"] == {"accepted": True}
    assert captured["payload"]["doc"] == generated_ref


def test_gateway_upload_call_reuses_stage_file_on_route_retry(tmp_path):
    stage_manager = GatewayStageManager(root_dir=str(tmp_path / "gateway_stage_retry"), failure_ttl_sec=600, gc_interval_sec=60)

    class _FakeRouteCache:
        def __init__(self) -> None:
            self.failures = []
            self.successes = []
            self.refreshed = []
            self.route_1 = InfoCenterServiceRoute(
                service_name="svc-retry",
                service_id="svc-retry-1",
                status=pb2.SERVICE_STATUS_RUNNING,
                node_instance_id="node-1-inst",
                node_id="node-1",
                control_addr="127.0.0.1:50061",
                node_healthy=True,
                worker_count=1,
                alive_workers=1,
                in_flight=0,
                lease_expire_at=datetime.now(timezone.utc),
                http_base_url="http://127.0.0.1:18081/svc/svc-retry-1",
            )
            self.route_2 = InfoCenterServiceRoute(
                service_name="svc-retry",
                service_id="svc-retry-2",
                status=pb2.SERVICE_STATUS_RUNNING,
                node_instance_id="node-2-inst",
                node_id="node-2",
                control_addr="127.0.0.1:50062",
                node_healthy=True,
                worker_count=1,
                alive_workers=1,
                in_flight=0,
                lease_expire_at=datetime.now(timezone.utc),
                http_base_url="http://127.0.0.1:18082/svc/svc-retry-2",
            )

        def start(self) -> None:
            return None

        def stop(self) -> None:
            return None

        def select_route(self, service_name: str, exclude_service_ids=None, force_refresh: bool = False):
            del service_name, force_refresh
            excluded = set(exclude_service_ids or ())
            if "svc-retry-1" not in excluded:
                return self.route_1
            return self.route_2

        def mark_success(self, route) -> None:
            self.successes.append(route.service_id)

        def mark_failure(self, route, error: str) -> None:
            self.failures.append((route.service_id, error))

        def refresh(self, service_name: str, force: bool = False) -> None:
            self.refreshed.append((service_name, force))

    app = GatewayHttpApp(route_cache=_FakeRouteCache(), stage_manager=stage_manager, controlplane_target="http://127.0.0.1:50051")
    upload_attempts = []
    call_attempts = []
    release_attempts = []

    def _fake_upload(*, request, route, files, timeout_sec):
        del timeout_sec
        stage_file = files["input_txt"]
        upload_attempts.append((request.request_id, route.service_id, str(stage_file.path), stage_file.size_bytes))
        return {
            "input_txt": DataRef(
                ref_id=f"gateway-upload:{request.request_id}:{route.service_id}:input_txt",
                storage_id="sha256:" + ("a" * 64),
                format="txt",
                size_bytes=stage_file.size_bytes,
                locator_kind="node_control",
                locator_token=str(route.control_addr or ""),
                control_addr=str(route.control_addr or ""),
            )
        }

    def _fake_invoke(self, route, *, method, payload, timeout_sec, service_token):
        del timeout_sec, service_token
        call_attempts.append((route.service_id, method, payload))
        if route.service_id == "svc-retry-1":
            raise GatewayCallError(status_code=502, data={"ok": False, "error": "upstream unavailable"})
        return {"ok": True, "data": {"route": route.service_id, "doc": str(payload["doc"].ref_id)}}

    def _fake_release(*, route, refs_by_slot, timeout_sec):
        del timeout_sec
        release_attempts.append(
            (
                route.service_id,
                sorted((slot, ref.ref_id, ref.object_id) for slot, ref in refs_by_slot.items()),
            )
        )

    boundary = "retry-boundary"
    payload = json.dumps({"doc": {"kind": "uploaded_file", "slot": "input_txt"}}).encode("utf-8")
    file_bytes = b"retry via stage file"
    raw = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"payload\"\r\nContent-Type: application/json\r\n\r\n".encode("utf-8")
        + payload
        + b"\r\n"
        + f"--{boundary}\r\nContent-Disposition: form-data; name=\"files[input_txt]\"; filename=\"retry.txt\"\r\nContent-Type: text/plain\r\n\r\n".encode("utf-8")
        + file_bytes
        + b"\r\n"
        + f"--{boundary}--\r\n".encode("utf-8")
    )
    headers = Message()
    headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    headers["Content-Length"] = str(len(raw))

    app.start()
    try:
        with (
            pytest.MonkeyPatch.context() as monkeypatch,
        ):
            monkeypatch.setattr("pycloud_parallel.controlplane.gateway_http.upload_staged_files_to_route", _fake_upload)
            monkeypatch.setattr("pycloud_parallel.controlplane.gateway_http.release_uploaded_refs_on_route", _fake_release)
            monkeypatch.setattr(GatewayHttpApp, "_invoke_route", _fake_invoke)
            code, body = app.handle_post_stream(
                path="/svc/svc-retry/upload-call/inspect?timeout_sec=5.000",
                headers=headers,
                stream=io.BytesIO(raw),
                content_length=len(raw),
            )
        assert code == 200
        assert body["data"]["route"] == "svc-retry-2"
        assert len(upload_attempts) == 2
        assert upload_attempts[0][2] == upload_attempts[1][2]
        assert upload_attempts[0][3] == len(file_bytes)
        assert len(call_attempts) == 2
        assert release_attempts[0][0] == "svc-retry-1"
        assert release_attempts[1][0] == "svc-retry-2"
        assert release_attempts[0][1][0][0] == "input_txt"
        assert release_attempts[1][1][0][0] == "input_txt"
        assert release_attempts[0][1][0][2] == release_attempts[1][1][0][2]
        assert release_attempts[0][1][0][1] != release_attempts[1][1][0][1]
        assert call_attempts[0][2]["doc"].locator_token == "127.0.0.1:50061"
        assert call_attempts[1][2]["doc"].locator_token == "127.0.0.1:50062"
        requests_dir = stage_manager.requests_dir
        assert not requests_dir.exists() or not any(requests_dir.iterdir())
    finally:
        app.stop()


def test_upload_staged_files_to_route_pins_request_scoped_refs(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.gateway_upload import release_uploaded_refs_on_route, upload_staged_files_to_route
    from pycloud_parallel.controlplane.gateway_stage import GatewayStageRequest, GatewayStageFile
    from pycloud_parallel.data.ref import DataRef

    pinned = []
    released = []

    class _FakeNodeClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def upload_object_from_file(self, *, file_path: str, format: str = "", trusted_precheck=None, transfer_mode: str = "", chunk_size: int = 0):
            del file_path, trusted_precheck, transfer_mode, chunk_size
            return DataRef(
                ref_id="sha256:" + ("a" * 64),
                storage_id="sha256:" + ("a" * 64),
                logical_type="text",
                format=format or "txt",
                size_bytes=32,
                materialize_as="text",
                locator_kind="node_local",
                locator_token="",
            )

        def pin_object(self, *, object_id: str, ref_id: str) -> bool:
            pinned.append((object_id, ref_id))
            return True

        def release_object_ref(self, *, object_id: str, ref_id: str = "") -> bool:
            released.append((object_id, ref_id))
            return True

    monkeypatch.setattr("pycloud_parallel.controlplane.gateway_upload.NodeControlClient", _FakeNodeClient)

    request_dir = tmp_path / "req"
    files_dir = request_dir / "files"
    files_dir.mkdir(parents=True)
    upload_path = files_dir / "input.txt"
    upload_path.write_text("hello", encoding="utf-8")
    request = GatewayStageRequest(
        request_id="req-1",
        service_name="svc-upload",
        method="inspect",
        request_dir=request_dir,
        files_dir=files_dir,
        meta_path=request_dir / "meta.json",
    )
    route = InfoCenterServiceRoute(
        service_name="svc-upload",
        service_id="svc-upload-1",
        status=pb2.SERVICE_STATUS_RUNNING,
        node_instance_id="node-1-inst",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        node_healthy=True,
        worker_count=1,
        alive_workers=1,
        in_flight=0,
        lease_expire_at=datetime.now(timezone.utc),
        http_base_url="http://127.0.0.1:18081/svc/svc-upload-1",
    )
    files = {
        "input_txt": GatewayStageFile(
            slot="input_txt",
            field_name="files[input_txt]",
            original_name="input.txt",
            content_type="text/plain",
            path=upload_path,
            size_bytes=upload_path.stat().st_size,
        )
    }

    refs = upload_staged_files_to_route(
        request=request,
        route=route,
        files=files,
        timeout_sec=5.0,
    )

    ref = refs["input_txt"]
    assert ref.object_id == "sha256:" + ("a" * 64)
    assert ref.ref_id.startswith("gateway-upload:req-1:svc-upload-1:input_txt")
    assert pinned == [(ref.object_id, ref.ref_id)]

    release_uploaded_refs_on_route(
        route=route,
        refs_by_slot=refs,
        timeout_sec=5.0,
    )
    assert released == [(ref.object_id, ref.ref_id)]


def test_gateway_route_cache_defaults_to_predicted_busy():
    class _StaticSource:
        def __init__(self, routes):
            self._routes = list(routes)

        def list_service_routes(self, *, service_name: str, healthy_only: bool, limit: int):
            del service_name, healthy_only, limit
            return list(self._routes)

    routes = [
        InfoCenterServiceRoute(
            service_name="svc-gateway-cache",
            service_id="svc-low-inflight",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_instance_id="node-1-inst",
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            node_healthy=True,
            worker_count=2,
            alive_workers=2,
            in_flight=1,
            lease_expire_at=datetime.now(timezone.utc),
            http_base_url="http://127.0.0.1:18081/svc/svc-low-inflight",
            predicted_busy=24.0,
        ),
        InfoCenterServiceRoute(
            service_name="svc-gateway-cache",
            service_id="svc-low-predicted",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_instance_id="node-2-inst",
            node_id="node-2",
            control_addr="127.0.0.1:50062",
            node_healthy=True,
            worker_count=2,
            alive_workers=2,
            in_flight=3,
            lease_expire_at=datetime.now(timezone.utc),
            http_base_url="http://127.0.0.1:18082/svc/svc-low-predicted",
            predicted_busy=8.0,
        ),
    ]
    cache = GatewayRouteCache(source=_StaticSource(routes), refresh_interval_sec=60.0)
    try:
        assert cache.select_route("svc-gateway-cache", force_refresh=True).service_id == "svc-low-predicted"
    finally:
        cache.stop()


def test_gateway_route_cache_uses_local_inflight_before_infocenter_refresh():
    class _StaticSource:
        def __init__(self, routes):
            self._routes = list(routes)

        def list_service_routes(self, *, service_name: str, healthy_only: bool, limit: int):
            del service_name, healthy_only, limit
            return list(self._routes)

    routes = [
        InfoCenterServiceRoute(
            service_name="svc-gateway-cache",
            service_id="svc-busier-snapshot",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_instance_id="node-1-inst",
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            node_healthy=True,
            worker_count=2,
            alive_workers=2,
            in_flight=1,
            lease_expire_at=datetime.now(timezone.utc),
            http_base_url="http://127.0.0.1:18081/svc/svc-busier-snapshot",
            predicted_busy=40.0,
        ),
        InfoCenterServiceRoute(
            service_name="svc-gateway-cache",
            service_id="svc-lower-snapshot",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_instance_id="node-2-inst",
            node_id="node-2",
            control_addr="127.0.0.1:50062",
            node_healthy=True,
            worker_count=2,
            alive_workers=2,
            in_flight=3,
            lease_expire_at=datetime.now(timezone.utc),
            http_base_url="http://127.0.0.1:18082/svc/svc-lower-snapshot",
            predicted_busy=12.0,
        ),
    ]
    cache = GatewayRouteCache(source=_StaticSource(routes), refresh_interval_sec=60.0)
    try:
        first = cache.select_route("svc-gateway-cache", force_refresh=True)
        second = cache.select_route("svc-gateway-cache")

        assert first.service_id == "svc-lower-snapshot"
        assert second.service_id == "svc-busier-snapshot"

        cache.mark_success(first)
        cache.mark_success(second)

        assert cache.select_route("svc-gateway-cache").service_id == "svc-lower-snapshot"
    finally:
        cache.stop()


def test_gateway_route_cache_route_failure_opens_breaker_immediately():
    route = _gateway_route_variant(1, service_name="svc-gateway-cache")

    class _StaticSource:
        def list_service_routes(self, *, service_name: str, healthy_only: bool, limit: int):
            del service_name, healthy_only, limit
            return [route]

    cache = GatewayRouteCache(
        source=_StaticSource(),
        refresh_interval_sec=60.0,
        failure_threshold=3,
        open_sec=5.0,
    )
    try:
        selected = cache.select_route("svc-gateway-cache", force_refresh=True)
        cache.mark_failure(selected, "connection refused")

        with pytest.raises(RuntimeError, match="no available route"):
            cache.select_route("svc-gateway-cache")
    finally:
        cache.stop()


def test_gateway_call_failover_tries_all_candidate_routes():
    routes = [_gateway_route_variant(i) for i in range(1, 5)]
    route_cache = _SequenceRouteCache(routes)
    app = GatewayHttpApp(route_cache=route_cache)
    attempts = []

    def _fake_invoke(self, route, *, method, payload, timeout_sec, service_token, serialization_mode=""):
        del self
        del method, payload, timeout_sec, service_token, serialization_mode
        attempts.append(route.service_id)
        if route.service_id in {"svc-gw-1", "svc-gw-2"}:
            raise GatewayCallError(status_code=502, data={"ok": False, "error": "connection refused"})
        return {"ok": True, "data": {"route": route.service_id}}

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(GatewayHttpApp, "_invoke_route", _fake_invoke)
        code, body = app.handle_post(
            path="/svc/svc-gateway-retry/call/run?timeout_sec=5.000",
            headers={"Content-Type": "application/json"},
            body=json.dumps({"x": 1}).encode("utf-8"),
        )

    assert code == 200
    assert body["data"]["route"] == "svc-gw-3"
    assert attempts == ["svc-gw-1", "svc-gw-2", "svc-gw-3"]
    assert [item[0] for item in route_cache.failures] == ["svc-gw-1", "svc-gw-2"]
    assert route_cache.successes == ["svc-gw-3"]
    assert route_cache.observations[-1][1]["route_attempt_count"] == 3
    assert route_cache.observations[-1][1]["failed_route_count"] == 2
    assert route_cache.observations[-1][1]["last_failed_route_id"] == "svc-gw-2"
    assert route_cache.observations[-1][1]["selected_route_id"] == "svc-gw-3"


def test_gateway_stream_call_forwards_events_and_marks_success():
    route = _gateway_route_variant(1, service_name="svc-gateway-stream")
    route_cache = _SequenceRouteCache([route])
    app = GatewayHttpApp(route_cache=route_cache)
    attempts = []

    def _fake_stream(self, route, *, method, payload, timeout_sec, service_token, serialization_mode=""):
        del self
        attempts.append((route.service_id, method, payload, timeout_sec, service_token, serialization_mode))
        yield {"event": "item", "index": 0, "data": 1}
        yield {"event": "item", "index": 1, "data": 2}
        yield {"event": "done", "ok": True, "item_count": 2}

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(GatewayHttpApp, "_invoke_route_stream", _fake_stream)
        handled = app.handle_post(
            path="/svc/svc-gateway-stream/call/count?timeout_sec=5.000&stream=1",
            headers={"Content-Type": "application/json"},
            body=json.dumps({}).encode("utf-8"),
        )

    assert handled is not None
    response = handled[0]
    events = [json.loads(chunk.decode("utf-8")) for chunk in response.body_iter]
    assert events == [
        {"event": "item", "index": 0, "data": 1},
        {"event": "item", "index": 1, "data": 2},
        {"event": "done", "ok": True, "item_count": 2},
    ]
    assert [item[0] for item in attempts] == ["svc-gw-1"]
    assert route_cache.successes == ["svc-gw-1"]
    assert route_cache.failures == []


def test_gateway_call_user_error_does_not_failover():
    routes = [_gateway_route_variant(i) for i in range(1, 3)]
    route_cache = _SequenceRouteCache(routes)
    app = GatewayHttpApp(route_cache=route_cache)
    attempts = []

    def _fake_invoke(self, route, *, method, payload, timeout_sec, service_token, serialization_mode=""):
        del self
        del method, payload, timeout_sec, service_token, serialization_mode
        attempts.append(route.service_id)
        raise GatewayCallError(status_code=400, data={"ok": False, "error_type": "UserError", "error": "bad args"})

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(GatewayHttpApp, "_invoke_route", _fake_invoke)
        code, body = app.handle_post(
            path="/svc/svc-gateway-retry/call/run?timeout_sec=5.000",
            headers={"Content-Type": "application/json"},
            body=json.dumps({"x": 1}).encode("utf-8"),
        )

    assert code == 400
    assert body["error"] == "bad args"
    assert attempts == ["svc-gw-1"]
    assert route_cache.failures == []
    assert route_cache.releases == ["svc-gw-1"]


def test_standalone_gateway_reads_routes_from_infocenter(tmp_path):
    infocenter = build_infocenter_server("127.0.0.1:0")
    infocenter.start()
    gateway = build_gateway_server("127.0.0.1:0", infocenter_addr=infocenter.base_url)
    gateway.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-gw-02", str(tmp_path / "node_gw_02"))

    try:
        service_id = _create_exported_service(node_target, "svc_gateway_remote")
        _register_node_with_services(
            infocenter.base_url,
            node_id="node-gw-02",
            control_addr=node_target,
            state=node_state,
        )

        assert _wait_until(
            lambda: len(
                InfoCenterClient(infocenter.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_gateway_remote",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 1
        )

        code, call_resp = _http_json(
            "POST",
            f"{gateway.base_url}/svc/svc_gateway_remote/call/mul",
            {"value": 9},
        )
        assert code == 200
        assert call_resp["ok"] is True
        assert call_resp["data"]["square"] == 81

        code, methods_resp = _http_json(
            "GET",
            f"{gateway.base_url}/svc/svc_gateway_remote/methods",
        )
        assert code == 200
        assert sorted(item["method"] for item in methods_resp["methods"]) == ["add", "mul"]
        assert methods_resp["service_id"] == service_id
    finally:
        node_server.stop()
        node_state.close()
        gateway.stop()
        infocenter.stop()


def test_gateway_service_client_fetches_large_dataframe_result(tmp_path):
    pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")

    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-gw-large-01", str(tmp_path / "node_gw_large_01"))

    try:
        blob = (
            b"import pandas as pd\n\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return pd.DataFrame([{'x': value}, {'x': value + 1}])\n"
        )
        with NodeControlClient(node_target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-svc-gateway-large",
                service_name="svc_gateway_large_result",
                blob=blob,
                runtime="py3",
                entry_module="svc_gateway_large_result",
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            service_id = session.service_id
        _register_node_with_services(
            controlplane.base_url,
            node_id="node-gw-large-01",
            control_addr=node_target,
            state=node_state,
        )

        assert _wait_until(
            lambda: len(
                InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_gateway_large_result",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 1
        )

        with GatewayServiceClient(controlplane.base_url, timeout_sec=5.0) as gateway:
            body = gateway.call(
                service_name="svc_gateway_large_result",
                method="run",
                payload={"value": 11},
                timeout_sec=5.0,
            )
            assert body["ok"] is True
            if isinstance(body["data"], DataRef):
                assert body["data"].node_id == "node-gw-large-01"
                assert body["data"].locator_kind == "controlplane"
                assert body["data"].locator_token == controlplane.base_url
                assert body["data"].control_addr == node_target
            else:
                assert isinstance(body["data"], pd.DataFrame)
            frame = gateway.fetch_result_data(body)
            assert isinstance(frame, pd.DataFrame)
            assert list(frame["x"]) == [11, 12]

        assert service_id
    finally:
        node_server.stop()
        node_state.close()
        controlplane.stop()


def test_gateway_retries_second_route_when_first_route_is_broken(tmp_path):
    controlplane = build_controlplane_server("127.0.0.1:0", gateway_failure_threshold=1, gateway_open_sec=1.0)
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-good", str(tmp_path / "node_good"))

    try:
        service_id = _create_exported_service(node_target, "svc_gateway_failover")
        _register_node_with_services(
            controlplane.base_url,
            node_id="node-good",
            control_addr=node_target,
            state=node_state,
        )

        bad_route = pb2.ServiceRouteReport(
            service_name="svc_gateway_failover",
            service_id="svc-bad-route",
            status=pb2.SERVICE_STATUS_RUNNING,
            worker_count=4,
            alive_workers=4,
            in_flight=0,
            http_base_url="http://127.0.0.1:1/svc/svc-bad-route",
        )
        with InfoCenterClient(controlplane.base_url, timeout_sec=10.0) as infocenter:
            infocenter.register_node(
                node_id="node-aaa-bad",
                control_addr="127.0.0.1:1",
                capacity=8,
                queue_capacity=64,
                tags=["compute"],
                services=[bad_route],
                service_worker_capacity=8,
                service_worker_used=4,
            )

        assert _wait_until(
            lambda: len(
                InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_gateway_failover",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 2
        )

        code, call_resp = _http_json(
            "POST",
            f"{controlplane.base_url}/svc/svc_gateway_failover/call/mul",
            {"value": 11},
        )
        assert code == 200
        assert call_resp["ok"] is True
        assert call_resp["data"]["square"] == 121

        code, status_resp = _http_json(
            "GET",
            f"{controlplane.base_url}/svc/svc_gateway_failover/status",
        )
        assert code == 200
        route_map = {item["service_id"]: item for item in status_resp["routes"]}
        assert service_id in route_map
        assert "svc-bad-route" in route_map
    finally:
        node_server.stop()
        node_state.close()
        controlplane.stop()


def test_gateway_supports_http_only_job_orchestrator_service():
    admin_token = "job-admin-token"
    infocenter = build_infocenter_server("127.0.0.1:0")
    infocenter.start()
    gateway = build_gateway_server("127.0.0.1:0", infocenter_addr=infocenter.base_url)
    gateway.start()
    job_orchestrator = build_job_orchestrator_server(
        "127.0.0.1:0",
        infocenter_addr=infocenter.base_url,
        node_id="job-orchestrator-test",
        admin_token=admin_token,
    )
    job_orchestrator.start()
    with job_orchestrator.job_queue._cv:  # noqa: SLF001
        job_orchestrator.job_queue._stop = True  # noqa: SLF001

    try:
        assert _wait_until(
            lambda: len(
                InfoCenterClient(infocenter.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="job-orchestrator",
                    healthy_only=True,
                    limit=10,
                )
            )
            == 1
        )

        with GatewayServiceClient(gateway.base_url, timeout_sec=5.0) as client:
            methods = client.list_methods(service_name="job-orchestrator", include_docs=False)
            assert sorted(item["method"] for item in methods) == ["cancel_job", "get_job_status", "reorder_job", "submit_job"]

        with GatewayServiceClient(gateway.base_url, timeout_sec=5.0, service_token="job-owner-token") as owner_client:
            hook_blob = (
                b"def run(value=0, **_kwargs):\n"
                b"    return {'value': int(value)}\n\n"
                b"def task_generator(value=0, **_kwargs):\n"
                b"    return [{'value': value}]\n"
            )

            def _hook_payload(value: int) -> dict:
                return {
                    "client_id": "gw-job-test",
                    "job_mode": "hooks",
                    "blob_b64": base64.b64encode(hook_blob).decode("utf-8"),
                    "entry_module": "gateway_job_demo",
                    "entry_callable": "run",
                    "package_format": "py",
                    "task_generator_callable": "task_generator",
                    "job_payload": {"value": value},
                }

            submit = owner_client.call(
                service_name="job-orchestrator",
                method="submit_job",
                payload=_hook_payload(1),
                timeout_sec=5.0,
            )
            assert submit["ok"] is True
            job_id = str(submit["job"]["job_id"])
            assert job_id
            second = owner_client.call(
                service_name="job-orchestrator",
                method="submit_job",
                payload=_hook_payload(2),
                timeout_sec=5.0,
            )
            second_job_id = str(second["job"]["job_id"])
            third = owner_client.call(
                service_name="job-orchestrator",
                method="submit_job",
                payload=_hook_payload(3),
                timeout_sec=5.0,
            )
            third_job_id = str(third["job"]["job_id"])
            with pytest.raises(RuntimeError, match="admin auth required"):
                owner_client.call(
                    service_name="job-orchestrator",
                    method="reorder_job",
                    payload={"job_id": third_job_id, "direction": "up"},
                    timeout_sec=5.0,
                )

            with GatewayServiceClient(gateway.base_url, timeout_sec=5.0) as no_token_client:
                with pytest.raises(RuntimeError, match="admin auth required"):
                    no_token_client.call(
                        service_name="job-orchestrator",
                        method="reorder_job",
                        payload={"job_id": third_job_id, "direction": "up"},
                        timeout_sec=5.0,
                    )

            with GatewayServiceClient(gateway.base_url, timeout_sec=5.0, service_token=admin_token) as admin_client:
                reorder = admin_client.call(
                    service_name="job-orchestrator",
                    method="reorder_job",
                    payload={"job_id": third_job_id, "direction": "up"},
                    timeout_sec=5.0,
                )
                assert reorder["ok"] is True
                waiting_ids = [item["job_id"] for item in reorder["queue"]["waiting_jobs"]]
                assert second_job_id in waiting_ids and third_job_id in waiting_ids
                assert waiting_ids.index(third_job_id) < waiting_ids.index(second_job_id)

            with GatewayServiceClient(gateway.base_url, timeout_sec=5.0, service_token="job-other-token") as other_client:
                with pytest.raises(RuntimeError, match="cancel auth failed"):
                    other_client.call(
                        service_name="job-orchestrator",
                        method="cancel_job",
                        payload={"job_id": job_id},
                        timeout_sec=5.0,
                    )

            cancelled = owner_client.call(
                service_name="job-orchestrator",
                method="cancel_job",
                payload={"job_id": second_job_id},
                timeout_sec=5.0,
            )
            assert cancelled["ok"] is True
            assert cancelled["job"]["status"] == "CANCELLED"

        job_state = job_orchestrator.job_queue.get_job(job_id)
        assert job_state is not None
        job_state.status = "FAILED"
        job_state.final_result = {"processed": 2}
        job_state.results = [
            {
                "task_id": "task-ok-1",
                "status": int(pb2.TASK_STATUS_SUCCEEDED),
                "status_text": "SUCCEEDED",
                "attempt": 1,
                "result": {"value": 1, "square": 1},
            },
            {
                "task_id": "task-fail-2",
                "status": int(pb2.TASK_STATUS_FAILED_USER),
                "status_text": "FAILED_USER",
                "attempt": 1,
                "error": {"type": "UserError", "message": "boom"},
            },
        ]

        with urlopen(f"{job_orchestrator.base_url}/svc/{job_orchestrator.service_id}/jobs/{job_id}", timeout=5.0) as resp:
            detail = json.loads(resp.read().decode("utf-8") or "{}")
        assert detail["ok"] is True
        assert detail["job"]["job_id"] == job_id

        with urlopen(f"{job_orchestrator.base_url}/svc/{job_orchestrator.service_id}/jobs/{job_id}?view=html", timeout=5.0) as resp:
            html_detail = resp.read().decode("utf-8")
        assert "Job Detail" in html_detail
        assert "auto_refresh_sec=10" in html_detail
        assert "http-equiv='refresh' content='10'" in html_detail
        assert "white-space:pre-wrap" in html_detail
        assert "Payload" in html_detail
        assert "Checkpoint" in html_detail
        assert "Final Result" in html_detail
        assert "Results" in html_detail
        assert "task-filter" in html_detail
        assert "filterJobResults()" in html_detail
        assert "details" in html_detail
        assert "result-row-failed" in html_detail
        assert "task-ok-1" in html_detail
        assert "task-fail-2" in html_detail
        assert job_id in html_detail

        with pytest.raises(HTTPError):
            urlopen(f"{job_orchestrator.base_url}/svc/{job_orchestrator.service_id}/jobs/not-found", timeout=5.0)
    finally:
        job_orchestrator.stop()
        gateway.stop()
        infocenter.stop()


def test_gateway_service_client_call_uses_http_payload_policy(monkeypatch) -> None:
    captured = {}

    class _FakeNodeControlClient:
        def __init__(self, target: str, timeout_sec: float = 10.0) -> None:
            del timeout_sec
            self.target = target

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.gateway_client.client_mod.NodeControlClient",
        _FakeNodeControlClient,
    )
    monkeypatch.setattr(
        GatewayServiceClient,
        "get_status",
        lambda self, *, service_name: {"routes": [{"control_addr": "127.0.0.1:50061"}]},
    )

    def _fake_prepare(payload, *, put_data, estimate_inline_size, policy, managed_global_policy=None):
        del put_data, estimate_inline_size
        del managed_global_policy
        captured["mode"] = policy.mode
        captured["preserve_args_kwargs_container"] = policy.preserve_args_kwargs_container
        return dict(payload or {})

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.remote_payload.prepare_outbound_payload",
        _fake_prepare,
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request",
        lambda **kwargs: {"ok": True, "data": kwargs.get("payload", {})},
    )

    with GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0) as gateway:
        resp = gateway.call(service_name="svc-demo", method="run", payload={"args": [1], "kwargs": {"x": 2}})

    assert resp["ok"] is True
    assert captured["mode"] == "http_call"
    assert captured["preserve_args_kwargs_container"] is True
