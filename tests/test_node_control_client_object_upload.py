from __future__ import annotations

import itertools
from concurrent import futures
import hashlib
from pathlib import Path
from unittest.mock import patch

import grpc
import pytest

from pycloud_parallel.controlplane.config import (
    get_object_transfer_mode,
    get_trust_mode,
    reload_config,
    resolve_object_transfer_mode,
)
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.object_digest_cache import lookup_file_digest
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.controlplane.state import NodeControlState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


def _start_nodecontrol_server(node_id: str, artifact_dir: str):
    state = NodeControlState(
        node_id=node_id,
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=artifact_dir,
        enable_internal_executor=False,
        enable_service_session=False,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=24))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, f"127.0.0.1:{port}", state


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


def test_upload_object_server_authoritative_rpc_returns_final_object_id(tmp_path):
    server, target, state = _start_nodecontrol_server("node-object-rpc-01", str(tmp_path / "node_object_rpc_01"))
    blob = b"server authoritative payload"
    expected_object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
    try:
        channel = grpc.insecure_channel(target)
        stub = pb2_grpc.NodeControlServiceStub(channel)
        try:
            response = stub.UploadObject(
                iter(
                    [
                        pb2.UploadObjectRequest(
                            meta=pb2.UploadObjectMeta(
                                object_id="",
                                format="bin",
                                integrity_mode="server_authoritative",
                            )
                        ),
                        pb2.UploadObjectRequest(chunk=blob),
                    ]
                ),
                timeout=10.0,
            )
        finally:
            channel.close()
        assert response.ok is True
        assert response.object_id == expected_object_id
        assert response.cached is False
    finally:
        server.stop(grace=0)
        state.close()


def test_upload_object_client_declared_digest_mismatch_errors(tmp_path):
    server, target, state = _start_nodecontrol_server("node-object-rpc-02", str(tmp_path / "node_object_rpc_02"))
    blob = b"mismatch payload"
    wrong_object_id = "sha256:" + ("a" * 64)
    try:
        channel = grpc.insecure_channel(target)
        stub = pb2_grpc.NodeControlServiceStub(channel)
        try:
            with pytest.raises(grpc.RpcError):
                stub.UploadObject(
                    iter(
                        [
                            pb2.UploadObjectRequest(
                                meta=pb2.UploadObjectMeta(
                                    object_id=wrong_object_id,
                                    format="bin",
                                    integrity_mode="client_declared",
                                )
                            ),
                            pb2.UploadObjectRequest(chunk=blob),
                        ]
                    ),
                    timeout=10.0,
                )
        finally:
            channel.close()
    finally:
        server.stop(grace=0)
        state.close()


def test_upload_object_from_file_auto_cache_miss_uses_single_pass_and_stores_digest(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path / "pycloud_home"))
    server, target, state = _start_nodecontrol_server("node-object-file-01", str(tmp_path / "node_object_file_01"))
    upload_path = tmp_path / "upload.bin"
    upload_path.write_bytes(b"cache miss single pass")
    try:
        captured = {}
        with NodeControlClient(target, timeout_sec=10.0) as client:
            original_upload = client.stub.UploadObject

            def _wrapped(request_iterator, timeout=None):
                head, tail = itertools.tee(iter(request_iterator))
                first = next(head)
                captured["object_id"] = str(first.meta.object_id or "")
                captured["integrity_mode"] = str(first.meta.integrity_mode or "")
                return original_upload(tail, timeout=timeout)

            with patch.object(client.stub, "UploadObject", side_effect=_wrapped):
                ref = client.upload_object_from_file(file_path=str(upload_path), format="bin")

        assert captured["object_id"] == ""
        assert captured["integrity_mode"] == "server_authoritative"
        assert lookup_file_digest(upload_path, format="bin") == ref.object_id
    finally:
        server.stop(grace=0)
        state.close()


def test_upload_object_from_file_auto_cache_hit_uses_precheck_and_skips_upload(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path / "pycloud_home"))
    server, target, state = _start_nodecontrol_server("node-object-file-02", str(tmp_path / "node_object_file_02"))
    upload_path = tmp_path / "upload-hit.bin"
    upload_path.write_bytes(b"cache hit precheck")
    try:
        with NodeControlClient(target, timeout_sec=10.0) as client:
            first = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            assert lookup_file_digest(upload_path, format="bin") == first.object_id
            with (
                patch.object(client.stub, "GetObjectMeta", wraps=client.stub.GetObjectMeta) as mocked_meta,
                patch.object(client.stub, "UploadObject", wraps=client.stub.UploadObject) as mocked_upload,
            ):
                second = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            assert second.object_id == first.object_id
            assert mocked_meta.call_count == 1
            mocked_upload.assert_not_called()
    finally:
        server.stop(grace=0)
        state.close()


def test_upload_object_from_file_cache_hit_remote_miss_reuploads_client_declared(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path / "pycloud_home"))
    server, target, state = _start_nodecontrol_server("node-object-file-03", str(tmp_path / "node_object_file_03"))
    upload_path = tmp_path / "upload-retry.bin"
    upload_path.write_bytes(b"cache hit remote miss")
    try:
        captured = {}
        with NodeControlClient(target, timeout_sec=10.0) as client:
            first = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            assert lookup_file_digest(upload_path, format="bin") == first.object_id
            assert state.release_object(first.object_id) is True
            original_upload = client.stub.UploadObject

            def _wrapped(request_iterator, timeout=None):
                head, tail = itertools.tee(iter(request_iterator))
                first_req = next(head)
                captured["object_id"] = str(first_req.meta.object_id or "")
                captured["integrity_mode"] = str(first_req.meta.integrity_mode or "")
                return original_upload(tail, timeout=timeout)

            with patch.object(client.stub, "UploadObject", side_effect=_wrapped):
                second = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            assert second.object_id == first.object_id
            assert captured["object_id"] == first.object_id
            assert captured["integrity_mode"] == "client_declared"
    finally:
        server.stop(grace=0)
        state.close()


def test_upload_object_from_bytes_defaults_to_known_digest_precheck(tmp_path):
    server, target, state = _start_nodecontrol_server("node-object-bytes-01", str(tmp_path / "node_object_bytes_01"))
    blob = b"bytes known digest precheck"
    try:
        captured = {}
        with NodeControlClient(target, timeout_sec=10.0) as client:
            original_upload = client.stub.UploadObject

            def _wrapped(request_iterator, timeout=None):
                head, tail = itertools.tee(iter(request_iterator))
                first = next(head)
                captured["object_id"] = str(first.meta.object_id or "")
                captured["integrity_mode"] = str(first.meta.integrity_mode or "")
                return original_upload(tail, timeout=timeout)

            with patch.object(client.stub, "UploadObject", side_effect=_wrapped):
                first = client.upload_object_from_bytes(blob=blob, format="bin")
            with patch.object(client.stub, "UploadObject", wraps=client.stub.UploadObject) as mocked_upload:
                second = client.upload_object_from_bytes(blob=blob, format="bin")

        assert first.object_id == second.object_id
        assert captured["object_id"] == first.object_id
        assert captured["integrity_mode"] == "client_declared"
        mocked_upload.assert_not_called()
    finally:
        server.stop(grace=0)
        state.close()
