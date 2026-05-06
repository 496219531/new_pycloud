from __future__ import annotations

import hashlib
import io
import json
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
from pycloud_parallel.controlplane.node_object_http import HttpNodeObjectClient, NodeObjectHttpApp, NodeObjectHttpServer
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


def test_upload_object_from_file_auto_uses_single_pass_and_stores_digest(tmp_path, monkeypatch):
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


def test_upload_object_from_file_auto_does_not_pre_hash(tmp_path, monkeypatch):
    upload_path = tmp_path / "upload-auto.bin"
    upload_path.write_bytes(b"default file upload should be single pass")
    calls = {}

    def _fail_sha256(self, path, *, chunk_size=0):  # noqa: ANN001
        raise AssertionError("default file upload must not pre-hash")

    def _fake_upload(self, *, path, headers, chunk_size=0):  # noqa: ANN001
        calls["headers"] = dict(headers)
        calls["path"] = Path(path)
        return {
            "ok": True,
            "object_id": "sha256:" + ("1" * 64),
            "format": "bin",
            "size_bytes": upload_path.stat().st_size,
        }

    monkeypatch.setattr(HttpNodeObjectClient, "_sha256_file", _fail_sha256)
    monkeypatch.setattr(HttpNodeObjectClient, "_upload_file_request", _fake_upload)

    with HttpNodeObjectClient("http://127.0.0.1:1", timeout_sec=1.0) as client:
        ref = client.upload_object_from_file(file_path=str(upload_path), format="bin")

    assert ref.object_id == "sha256:" + ("1" * 64)
    assert ref.size_bytes == upload_path.stat().st_size
    assert calls["path"] == upload_path
    assert calls["headers"]["X-Pycloud-Integrity-Mode"] == "server_authoritative"
    assert "X-Pycloud-Object-Id" not in calls["headers"]


def test_upload_object_from_file_single_pass_does_not_pre_hash_when_trusted_precheck_false(tmp_path, monkeypatch):
    upload_path = tmp_path / "upload-single-pass.bin"
    upload_path.write_bytes(b"single pass with trusted_precheck false")
    calls = {}

    def _fail_sha256(self, path, *, chunk_size=0):  # noqa: ANN001
        raise AssertionError("single_pass_authoritative file upload must not pre-hash")

    def _fake_upload(self, *, path, headers, chunk_size=0):  # noqa: ANN001
        calls["headers"] = dict(headers)
        return {
            "ok": True,
            "object_id": "sha256:" + ("2" * 64),
            "format": "bin",
            "size_bytes": upload_path.stat().st_size,
        }

    monkeypatch.setattr(HttpNodeObjectClient, "_sha256_file", _fail_sha256)
    monkeypatch.setattr(HttpNodeObjectClient, "_upload_file_request", _fake_upload)

    with HttpNodeObjectClient("http://127.0.0.1:1", timeout_sec=1.0) as client:
        ref = client.upload_object_from_file(
            file_path=str(upload_path),
            format="bin",
            transfer_mode="single_pass_authoritative",
            trusted_precheck=False,
        )

    assert ref.object_id == "sha256:" + ("2" * 64)
    assert calls["headers"]["X-Pycloud-Integrity-Mode"] == "server_authoritative"
    assert "X-Pycloud-Object-Id" not in calls["headers"]


def test_upload_object_from_file_known_digest_precheck_still_pre_hashes(tmp_path, monkeypatch):
    upload_path = tmp_path / "upload-known.bin"
    upload_path.write_bytes(b"known digest precheck")
    digest = hashlib.sha256(upload_path.read_bytes()).hexdigest()
    object_id = "sha256:" + digest
    calls = {"hash": 0, "precheck": 0}

    def _fake_sha256(self, path, *, chunk_size=0):  # noqa: ANN001
        calls["hash"] += 1
        assert Path(path) == upload_path
        return digest

    def _fake_precheck(self, *, object_id, fallback_format, fallback_size):  # noqa: ANN001
        calls["precheck"] += 1
        calls["precheck_object_id"] = object_id
        calls["fallback_format"] = fallback_format
        calls["fallback_size"] = fallback_size
        return None

    def _fake_upload(self, *, path, headers, chunk_size=0):  # noqa: ANN001
        calls["headers"] = dict(headers)
        return {
            "ok": True,
            "object_id": object_id,
            "format": "bin",
            "size_bytes": upload_path.stat().st_size,
        }

    monkeypatch.setattr(HttpNodeObjectClient, "_sha256_file", _fake_sha256)
    monkeypatch.setattr(HttpNodeObjectClient, "_object_ref_if_exists", _fake_precheck)
    monkeypatch.setattr(HttpNodeObjectClient, "_upload_file_request", _fake_upload)

    with HttpNodeObjectClient("http://127.0.0.1:1", timeout_sec=1.0) as client:
        ref = client.upload_object_from_file(
            file_path=str(upload_path),
            format="bin",
            transfer_mode="known_digest_precheck",
        )

    assert ref.object_id == object_id
    assert calls["hash"] == 1
    assert calls["precheck"] == 1
    assert calls["precheck_object_id"] == object_id
    assert calls["fallback_format"] == "bin"
    assert calls["fallback_size"] == upload_path.stat().st_size
    assert calls["headers"]["X-Pycloud-Integrity-Mode"] == "client_declared"
    assert calls["headers"]["X-Pycloud-Object-Id"] == object_id


def test_upload_object_from_file_auto_reupload_returns_same_digest(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path / "pycloud_home"))
    server, target, state = _start_nodecontrol_server("node-object-file-02", str(tmp_path / "node_object_file_02"))
    upload_path = tmp_path / "upload-hit.bin"
    upload_path.write_bytes(b"repeat single pass upload")
    try:
        with NodeControlClient(target, timeout_sec=10.0) as client:
            first = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            second = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            assert second.object_id == first.object_id
            assert client.get_object_meta(object_id=first.object_id).size_bytes == upload_path.stat().st_size
    finally:
        server.stop()
        state.close()


def test_upload_object_from_file_auto_remote_miss_reuploads_authoritatively(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path / "pycloud_home"))
    server, target, state = _start_nodecontrol_server("node-object-file-03", str(tmp_path / "node_object_file_03"))
    upload_path = tmp_path / "upload-retry.bin"
    upload_path.write_bytes(b"authoritative remote miss")
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


def test_http_object_download_bytes_rejects_over_materialize_threshold(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane import config as config_mod

    server, target, state = _start_nodeobject_http_server("node-object-http-bytes-limit", str(tmp_path / "node_object_http_bytes_limit"))
    blob = b"x" * 64
    monkeypatch.setenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", "8")
    config_mod.reload_config()
    try:
        with HttpNodeObjectClient(target, timeout_sec=10.0) as client:
            ref = client.upload_object_from_bytes(blob=blob, format="bin")
            with pytest.raises(ValueError) as exc_info:
                client.download_object_bytes(object_id=ref.object_id)
            output = tmp_path / "large-download.bin"
            client.download_object_to_file(object_id=ref.object_id, target_path=str(output))
        assert output.read_bytes() == blob
    finally:
        server.stop()
        state.close()
        monkeypatch.delenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", raising=False)
        config_mod.reload_config()

    message = str(exc_info.value)
    assert "too large for in-memory bytes materialize" in message
    assert "size_bytes=64" in message
    assert "limit_bytes=8" in message
    assert "file/path download" in message


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


def test_http_object_upload_streams_request_body_to_temp_file(tmp_path):
    state = NodeControlState(
        node_id="node-object-http-stream-upload",
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "node_object_http_stream_upload"),
        enable_internal_executor=False,
        enable_service_session=False,
    )

    class ChunkOnlyStream(io.BytesIO):
        def read(self, size=-1):
            if size < 0 or size > 16:
                raise AssertionError("object upload handler must read bounded chunks")
            return super().read(size)

    blob = b"stream-upload-body" * 8
    app = NodeObjectHttpApp(state, max_body_bytes=1024)
    try:
        code, headers, raw = app.handle_post_stream(
            "/objects/upload",
            {
                "X-Pycloud-Object-Format": "bin",
                "X-Pycloud-Integrity-Mode": "server_authoritative",
            },
            ChunkOnlyStream(blob),
            content_length=len(blob),
            chunk_size=16,
        )
        body = json.loads(raw.decode("utf-8"))
        assert code == 200
        assert headers["Content-Type"].startswith("application/json")
        assert body["ok"] is True
        assert body["size_bytes"] == len(blob)
    finally:
        state.close()


def test_http_object_upload_rejects_object_limit_separately_from_body_limit(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES", "8")
    from pycloud_parallel.controlplane import config

    config.reload_config()
    state = NodeControlState(
        node_id="node-object-http-object-limit",
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "node_object_http_object_limit"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    app = NodeObjectHttpApp(state, max_body_bytes=1024)
    try:
        code, _headers, raw = app.handle_post_stream(
            "/objects/upload",
            {
                "X-Pycloud-Object-Format": "bin",
                "X-Pycloud-Integrity-Mode": "server_authoritative",
            },
            io.BytesIO(b"larger-than-eight"),
            content_length=len(b"larger-than-eight"),
        )
        body = json.loads(raw.decode("utf-8"))
        assert code == 400
        assert "object upload exceeds object size hard limit" in body["error"]
        assert "size_bytes=17" in body["error"]
        assert "limit_bytes=8" in body["error"]
    finally:
        state.close()
        monkeypatch.delenv("PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES", raising=False)
        config.reload_config()


def test_upload_object_from_file_sends_file_without_reading_whole_file(tmp_path, monkeypatch):
    server, target, state = _start_nodeobject_http_server("node-object-http-file-stream", str(tmp_path / "node_object_http_file_stream"))
    source = tmp_path / "payload-streamed.dat"
    source.write_bytes(b"file upload should stream" * 1024)
    state._object_segment_max_bytes = 1  # noqa: SLF001

    def _fail_read_bytes(self):  # noqa: ANN001
        raise AssertionError("upload_object_from_file must not read the whole file into memory")

    monkeypatch.setattr(Path, "read_bytes", _fail_read_bytes)
    try:
        with HttpNodeObjectClient(target, timeout_sec=10.0) as client:
            ref = client.upload_object_from_file(file_path=str(source), format="bin")
            assert client.download_object_bytes(object_id=ref.object_id) == source.open("rb").read()
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
