from __future__ import annotations

from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.data_registry import ResolvedDataRef
from pycloud_parallel.controlplane.data_plane_http import DataPlaneHttpApp
from pycloud_parallel.controlplane.infocenter_http import InfoCenterHttpServer
from pycloud_parallel.controlplane.infocenter_state import InfoCenterState
from pycloud_parallel.controlplane.node_control_http import NodeControlHttpServer
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.data.ref import DataRef


def test_data_plane_download_streams_resolved_data_ref(monkeypatch):
    blob = b"abc" * 200_000
    reads = []

    class _FakeHeaders:
        def get(self, key, default=None):
            return {
                "Content-Length": str(len(blob)),
                "X-Pycloud-Object-Format": "bin",
                "X-Pycloud-Object-Size-Bytes": str(len(blob)),
            }.get(key, default)

    class _FakeResponse:
        headers = _FakeHeaders()

        def __init__(self):
            self.offset = 0
            self.closed = False

        def read(self, size=-1):
            reads.append(size)
            if size < 0:
                raise AssertionError("data-plane must not read the whole object at once")
            if self.offset >= len(blob):
                return b""
            chunk = blob[self.offset : self.offset + size]
            self.offset += len(chunk)
            return chunk

        def close(self):
            self.closed = True

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.data_plane_http.resolve_data_ref",
        lambda ref, **_kwargs: ResolvedDataRef(
            ref=DataRef(
                ref_id=ref.ref_id,
                storage_id="sha256:" + ("a" * 64),
                format="bin",
                size_bytes=len(blob),
            ),
            control_addr="127.0.0.1:50061",
            via_registry=True,
        ),
    )
    monkeypatch.setattr("pycloud_parallel.controlplane.data_plane_http.urlopen", lambda *_args, **_kwargs: _FakeResponse())

    response = DataPlaneHttpApp(target="127.0.0.1:50051").handle_get(
        "/data/refs/ref-1/download",
    )

    assert not isinstance(response, tuple)
    assert response.status_code == 200
    assert response.content_length == len(blob)
    assert b"".join(response.body_iter) == blob
    assert all(size > 0 for size in reads)


def test_data_plane_download_missing_ref_is_clear(monkeypatch):
    def _missing(*_args, **_kwargs):
        raise KeyError("ref-404")

    monkeypatch.setattr("pycloud_parallel.controlplane.data_plane_http.resolve_data_ref", _missing)

    status, _headers, body = DataPlaneHttpApp(target="127.0.0.1:50051").handle_get(
        "/data/refs/ref-404/download",
    )

    assert status == 404
    assert b"data ref not found" in body


def test_data_plane_download_missing_object_is_clear(monkeypatch):
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.data_plane_http.resolve_data_ref",
        lambda ref, **_kwargs: ResolvedDataRef(
            ref=DataRef(ref_id=ref.ref_id, storage_id="missing-object", format="bin", size_bytes=1),
            control_addr="127.0.0.1:50061",
            via_registry=True,
        ),
    )

    def _missing_object(*_args, **_kwargs):
        raise HTTPError("http://node/objects/missing-object/download", 404, "Not Found", None, None)

    monkeypatch.setattr("pycloud_parallel.controlplane.data_plane_http.urlopen", _missing_object)

    status, _headers, body = DataPlaneHttpApp(target="127.0.0.1:50051").handle_get(
        "/data/refs/ref-missing-object/download",
    )

    assert status == 404
    assert b"object not found" in body


def test_infocenter_data_plane_downloads_registered_object(tmp_path):
    info_state = InfoCenterState()
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    node_state = NodeControlState(
        node_id="node-data-plane-01",
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "node_data_plane_01"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    node_server = NodeControlHttpServer(bind="127.0.0.1:0", state=node_state)
    blob = b"data-plane-result" * 1024
    try:
        info_server.start()
        node_server.start()
        with NodeControlClient(node_server.base_url, timeout_sec=10.0) as client:
            uploaded = client.upload_object_from_bytes(blob=blob, format="bin")
        info_state.register_node_record(
            node_instance_id="node-data-plane-01-inst",
            node_id="node-data-plane-01",
            control_addr=node_server.base_url,
            capacity=4,
            queue_capacity=32,
            tags=["compute"],
            capability=SimpleNamespace(to_dict=lambda: {}),
        )
        info_state.register_data_ref_record(
            ref=DataRef(
                ref_id="ref-result-1",
                storage_id=uploaded.object_id,
                format="bin",
                size_bytes=len(blob),
                locator_kind="controlplane",
                locator_token=info_server.base_url,
            ),
            replicas=(
                {
                    "control_addr": node_server.base_url,
                    "node_id": "node-data-plane-01",
                    "node_instance_id": "node-data-plane-01-inst",
                },
            ),
        )

        with urlopen(f"{info_server.base_url}/data/refs/ref-result-1/download", timeout=10.0) as resp:
            assert resp.headers.get("X-Pycloud-Object-Id") == uploaded.object_id
            assert resp.headers.get("X-Pycloud-Object-Format") == "bin"
            assert resp.read() == blob
    finally:
        info_server.stop()
        node_server.stop()
        node_state.close()


def test_infocenter_data_registry_public_view_hides_node_locator(tmp_path):
    info_state = InfoCenterState()
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    try:
        info_server.start()
        info_state.register_data_ref_record(
            ref=DataRef(
                ref_id="ref-public-view",
                storage_id="sha256:" + ("2" * 64),
                format="bin",
                size_bytes=12,
                locator_kind="node_control",
                locator_token="127.0.0.1:50061",
                control_addr="127.0.0.1:50061",
            ),
            node_id="node-public-view",
            node_instance_id="node-public-view-inst",
            control_addr="127.0.0.1:50061",
            locator_kind="node_control",
            locator_token="127.0.0.1:50061",
            replicas=(
                {
                    "control_addr": "127.0.0.1:50061",
                    "node_id": "node-public-view",
                    "node_instance_id": "node-public-view-inst",
                },
            ),
        )

        with urlopen(f"{info_server.base_url}/data/resolve/ref-public-view", timeout=10.0) as resp:
            import json

            entry = json.loads(resp.read().decode("utf-8"))["entry"]
        assert entry["locator_kind"] == "controlplane"
        assert entry["locator_token"] == ""
        assert entry["control_addr"] == ""
        assert entry["replicas"] == []

        with urlopen(f"{info_server.base_url}/data/refs", timeout=10.0) as resp:
            import json

            refs = json.loads(resp.read().decode("utf-8"))["refs"]
        assert refs[0]["control_addr"] == ""
        assert refs[0]["replicas"] == []
    finally:
        info_server.stop()


def test_infocenter_data_endpoints_require_auth_when_token_is_set(tmp_path):
    info_state = InfoCenterState()
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state, auth_token="secret-token")
    node_state = NodeControlState(
        node_id="node-data-auth-01",
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "node_data_auth_01"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    node_server = NodeControlHttpServer(bind="127.0.0.1:0", state=node_state)
    blob = b"auth-data-plane"
    try:
        info_server.start()
        node_server.start()
        with NodeControlClient(node_server.base_url, timeout_sec=10.0) as client:
            uploaded = client.upload_object_from_bytes(blob=blob, format="bin")
        info_state.register_data_ref_record(
            ref=DataRef(
                ref_id="ref-auth-download",
                storage_id=uploaded.object_id,
                format="bin",
                size_bytes=len(blob),
                locator_kind="controlplane",
                locator_token=info_server.base_url,
            ),
            replicas=({"control_addr": node_server.base_url},),
        )

        try:
            urlopen(f"{info_server.base_url}/data/refs/ref-auth-download/download", timeout=10.0)
            raise AssertionError("expected unauthorized data-plane download")
        except HTTPError as exc:
            assert exc.code == 401

        req = Request(
            f"{info_server.base_url}/data/refs/ref-auth-download/download",
            headers={"X-Infocenter-Token": "secret-token"},
            method="GET",
        )
        with urlopen(req, timeout=10.0) as resp:
            assert resp.read() == blob
    finally:
        info_server.stop()
        node_server.stop()
        node_state.close()
