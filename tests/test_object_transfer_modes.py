from __future__ import annotations

from pathlib import Path

from pycloud_parallel.controlplane.object_transfer import (
    resolve_object_transfer_mode,
    upload_file_single_pass_authoritative,
    upload_known_digest_precheck,
    upload_memory_object_precheck,
)


class _FakeClient:
    def __init__(self) -> None:
        self.calls = []

    def upload_object_from_file(self, **kwargs):
        self.calls.append(("file", dict(kwargs)))
        return {"ok": True}

    def upload_object_from_bytes(self, **kwargs):
        self.calls.append(("bytes", dict(kwargs)))
        return {"ok": True}


def test_resolve_object_transfer_mode_delegates_to_config():
    assert resolve_object_transfer_mode(source_kind="memory", local_digest_known=False) in {
        "known_digest_precheck",
        "single_pass_authoritative",
    }
    assert resolve_object_transfer_mode(source_kind="file", local_digest_known=True) in {
        "known_digest_precheck",
        "single_pass_authoritative",
    }


def test_upload_file_single_pass_authoritative_sets_transfer_mode(tmp_path: Path):
    client = _FakeClient()
    path = tmp_path / "blob.bin"
    path.write_bytes(b"hello")

    upload_file_single_pass_authoritative(client, file_path=str(path), format="bin")

    kind, kwargs = client.calls[0]
    assert kind == "file"
    assert kwargs["transfer_mode"] == "single_pass_authoritative"
    assert kwargs["file_path"] == str(path)


def test_upload_known_digest_precheck_sets_transfer_mode(tmp_path: Path):
    client = _FakeClient()
    path = tmp_path / "blob.bin"
    path.write_bytes(b"hello")

    upload_known_digest_precheck(client, file_path=str(path), format="bin", trusted_precheck=True)

    kind, kwargs = client.calls[0]
    assert kind == "file"
    assert kwargs["transfer_mode"] == "known_digest_precheck"
    assert kwargs["trusted_precheck"] is True


def test_upload_memory_object_precheck_sets_transfer_mode():
    client = _FakeClient()

    upload_memory_object_precheck(client, blob=b"hello", format="bin", trusted_precheck=False)

    kind, kwargs = client.calls[0]
    assert kind == "bytes"
    assert kwargs["transfer_mode"] == "known_digest_precheck"
    assert kwargs["trusted_precheck"] is False
