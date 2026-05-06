from __future__ import annotations

import hashlib
from pathlib import Path
import pytest

from pycloud_parallel.controlplane.config import (
    get_object_transfer_mode,
    get_trust_mode,
    reload_config,
    resolve_object_transfer_mode,
)
from pycloud_parallel.controlplane.node_control_http import NodeControlHttpServer
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.node_object_http import HttpNodeObjectClient, NodeObjectHttpServer
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState


def _start_nodecontrol_server(node_id: str, artifact_dir: str):
    state = NodeControlState(
        node_id=node_id,
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=artifact_dir,
        enable_internal_executor=False,
        enable_service_session=False,
    )
    server = NodeControlHttpServer(bind="127.0.0.1:0", state=state)
    server.start()
    return server, server.base_url, state


def _start_nodeobject_http_server(node_id: str, artifact_dir: str):
    state = NodeControlState(
        node_id=node_id,
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=artifact_dir,
        enable_internal_executor=False,
        enable_service_session=False,
    )
    server = NodeObjectHttpServer(bind="127.0.0.1:0", state=state)
    server.start()
    return server, server.base_url, state


def test_object_transfer_mode_auto_rules(monkeypatch):
    monkeypatch.setenv("PYCLOUD_TRUST_MODE", "trusted")
    monkeypatch.setenv("PYCLOUD_OBJECT_TRANSFER_MODE", "auto")
    reload_config()
    try:
        assert get_trust_mode() == "trusted"
        assert get_object_transfer_mode() == "auto"
        assert resolve_object_transfer_mode(source_kind="memory", local_digest_known=False) == "known_digest_precheck"
        assert resolve_object_transfer_mode(source_kind="file", local_digest_known=False) == "single_pass_authoritative"
        assert resolve_object_transfer_mode(source_kind="file", local_digest_known=True) == "known_digest_precheck"

        monkeypatch.setenv("PYCLOUD_OBJECT_TRANSFER_MODE", "single_pass_authoritative")
        reload_config()
        assert resolve_object_transfer_mode(source_kind="memory", local_digest_known=False) == "single_pass_authoritative"

        monkeypatch.setenv("PYCLOUD_TRUST_MODE", "strict")
        reload_config()
        assert resolve_object_transfer_mode(source_kind="file", local_digest_known=False) == "known_digest_precheck"
    finally:
        monkeypatch.delenv("PYCLOUD_TRUST_MODE", raising=False)
        monkeypatch.delenv("PYCLOUD_OBJECT_TRANSFER_MODE", raising=False)
        reload_config()


def test_upload_object_server_authoritative_http_returns_final_object_id(tmp_path):
    server, target, state = _start_nodecontrol_server("node-object-http-control-01", str(tmp_path / "node_object_http_control_01"))
    blob = b"server authoritative payload"
    expected_object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
    try:
        with NodeControlClient(target, timeout_sec=10.0) as client:
            response = client.upload_object_from_bytes(blob=blob, format="bin", transfer_mode="single_pass_authoritative")
        assert response.object_id == expected_object_id
        assert response.size_bytes == len(blob)
    finally:
        server.stop()
        state.close()


def test_upload_object_client_declared_digest_mismatch_errors(tmp_path):
    server, target, state = _start_nodecontrol_server("node-object-http-control-02", str(tmp_path / "node_object_http_control_02"))
    blob = b"mismatch payload"
    wrong_object_id = "sha256:" + ("a" * 64)
    try:
        import urllib.error
        import urllib.request

        req = urllib.request.Request(
            f"{target}/objects/upload",
            method="POST",
            data=blob,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Pycloud-Object-Format": "bin",
                "X-Pycloud-Integrity-Mode": "client_declared",
                "X-Pycloud-Object-Id": wrong_object_id,
            },
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req, timeout=10.0)
        assert exc_info.value.code == 400
    finally:
        server.stop()
        state.close()


def test_upload_object_from_file_auto_cache_miss_uses_single_pass_and_stores_digest(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path / "pycloud_home"))
    server, target, state = _start_nodecontrol_server("node-object-file-01", str(tmp_path / "node_object_file_01"))
    upload_path = tmp_path / "upload.bin"
    upload_path.write_bytes(b"cache miss single pass")
    try:
        with NodeControlClient(target, timeout_sec=10.0) as client:
            ref = client.upload_object_from_file(file_path=str(upload_path), format="bin")

        assert client.has_object(object_id=ref.object_id) is True
    finally:
        server.stop()
        state.close()


def test_upload_object_from_file_auto_cache_hit_uses_precheck_and_skips_upload(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path / "pycloud_home"))
    server, target, state = _start_nodecontrol_server("node-object-file-02", str(tmp_path / "node_object_file_02"))
    upload_path = tmp_path / "upload-hit.bin"
    upload_path.write_bytes(b"cache hit precheck")
    try:
        with NodeControlClient(target, timeout_sec=10.0) as client:
            first = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            second = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            assert second.object_id == first.object_id
            assert client.get_object_meta(object_id=first.object_id).size_bytes == upload_path.stat().st_size
    finally:
        server.stop()
        state.close()


def test_upload_object_from_file_cache_hit_remote_miss_reuploads_client_declared(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path / "pycloud_home"))
    server, target, state = _start_nodecontrol_server("node-object-file-03", str(tmp_path / "node_object_file_03"))
    upload_path = tmp_path / "upload-retry.bin"
    upload_path.write_bytes(b"cache hit remote miss")
    try:
        with NodeControlClient(target, timeout_sec=10.0) as client:
            first = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            assert state.release_object(first.object_id) is True
            second = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            assert second.object_id == first.object_id
            assert client.has_object(object_id=first.object_id) is True
    finally:
        server.stop()
        state.close()


def test_upload_object_from_bytes_defaults_to_known_digest_precheck(tmp_path):
    server, target, state = _start_nodecontrol_server("node-object-bytes-01", str(tmp_path / "node_object_bytes_01"))
    blob = b"bytes known digest precheck"
    try:
        with NodeControlClient(target, timeout_sec=10.0) as client:
            first = client.upload_object_from_bytes(blob=blob, format="bin")
            second = client.upload_object_from_bytes(blob=blob, format="bin")

        assert first.object_id == second.object_id
        with NodeControlClient(target, timeout_sec=10.0) as client:
            assert client.get_object_meta(object_id=first.object_id).size_bytes == len(blob)
    finally:
        server.stop()
        state.close()


def test_http_object_bytes_roundtrip_and_precheck(tmp_path):
    server, target, state = _start_nodeobject_http_server("node-object-http-01", str(tmp_path / "node_object_http_01"))
    blob = b"http bytes payload"
    try:
        with HttpNodeObjectClient(target, timeout_sec=10.0) as client:
            first = client.upload_object_from_bytes(blob=blob, format="bin")
            second = client.upload_object_from_bytes(blob=blob, format="bin")
            assert second.object_id == first.object_id
            assert client.download_object_bytes(object_id=first.object_id) == blob
            meta = client.get_object_meta(object_id=first.object_id)
            assert meta.exists is True
            assert meta.format == "bin"
            assert meta.size_bytes == len(blob)
    finally:
        server.stop()
        state.close()


def test_http_object_file_roundtrip(tmp_path):
    server, target, state = _start_nodeobject_http_server("node-object-http-02", str(tmp_path / "node_object_http_02"))
    source = tmp_path / "payload.dat"
    output = tmp_path / "downloaded.dat"
    source.write_bytes(b"http file payload")
    try:
        with HttpNodeObjectClient(target, timeout_sec=10.0) as client:
            ref = client.upload_object_from_file(file_path=str(source))
            client.download_object_to_file(object_id=ref.object_id, target_path=str(output))
        assert output.read_bytes() == source.read_bytes()
    finally:
        server.stop()
        state.close()


def test_http_object_download_to_file_streams_chunks(tmp_path, monkeypatch):
    blob = b"streamed-object" * 10000
    reads = []

    class FakeResponse:
        def __init__(self):
            self.offset = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def read(self, size=-1):
            reads.append(size)
            if size < 0:
                raise AssertionError("download_object_to_file must not read the whole response at once")
            if self.offset >= len(blob):
                return b""
            chunk = blob[self.offset : self.offset + size]
            self.offset += len(chunk)
            return chunk

    monkeypatch.setattr("pycloud_parallel.controlplane.node_object_http.urlopen", lambda *_args, **_kwargs: FakeResponse())

    output = tmp_path / "streamed.bin"
    with HttpNodeObjectClient("127.0.0.1:50061", timeout_sec=10.0) as client:
        path = client.download_object_to_file(object_id="sha256:" + ("3" * 64), target_path=str(output))

    assert Path(path) == output
    assert output.read_bytes() == blob
    assert reads
    assert all(size > 0 for size in reads)


def test_http_object_checksum_mismatch_errors(tmp_path):
    server, target, state = _start_nodeobject_http_server("node-object-http-03", str(tmp_path / "node_object_http_03"))
    try:
        import urllib.error
        import urllib.request

        req = urllib.request.Request(
            f"{target}/objects/upload",
            method="POST",
            data=b"actual payload",
            headers={
                "Content-Type": "application/octet-stream",
                "X-Pycloud-Object-Format": "bin",
                "X-Pycloud-Integrity-Mode": "client_declared",
                "X-Pycloud-Object-Id": "sha256:" + ("a" * 64),
            },
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req, timeout=10.0)
        assert exc_info.value.code == 400
    finally:
        server.stop()
        state.close()


def test_http_object_missing_and_pin_release(tmp_path):
    server, target, state = _start_nodeobject_http_server("node-object-http-04", str(tmp_path / "node_object_http_04"))
    try:
        with HttpNodeObjectClient(target, timeout_sec=10.0) as client:
            missing = client.get_object_meta(object_id="sha256:" + ("b" * 64))
            assert missing.exists is False
            ref = client.upload_object_from_bytes(blob=b"pin me", format="bin")
            assert client.pin_object(object_id=ref.object_id, ref_id="ref-1") is True
            assert client.release_object_ref(object_id=ref.object_id, ref_id="ref-1") is True
    finally:
        server.stop()
        state.close()
