from __future__ import annotations

from pathlib import Path

from pycloud_parallel.controlplane import object_digest_cache as cache_mod


def test_store_lookup_and_invalidate_file_digest(tmp_path: Path, monkeypatch):
    cache_root = tmp_path / "digest_cache"
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path))
    path = tmp_path / "demo.bin"
    path.write_bytes(b"hello")

    assert cache_mod.lookup_file_digest(path, format="bin") is None

    cache_mod.store_file_digest(path, format="bin", object_id="sha256:" + "a" * 64)
    assert cache_mod.lookup_file_digest(path, format="bin") == "sha256:" + "a" * 64

    cache_mod.invalidate_file_digest(path, format="bin")
    assert cache_mod.lookup_file_digest(path, format="bin") is None
