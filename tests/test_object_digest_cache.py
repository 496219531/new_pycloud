from __future__ import annotations

from pathlib import Path

from pycloud_parallel.controlplane import object_digest_cache as cache_mod


def test_store_lookup_and_invalidate_file_digest(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path))
    path = tmp_path / "demo.bin"
    path.write_bytes(b"hello")

    assert cache_mod.lookup_file_digest(path, format="bin") is None

    cache_mod.store_file_digest(path, format="bin", object_id="sha256:" + "a" * 64)
    assert cache_mod.lookup_file_digest(path, format="bin") == "sha256:" + "a" * 64

    cache_mod.invalidate_file_digest(path, format="bin")
    assert cache_mod.lookup_file_digest(path, format="bin") is None


def test_lookup_file_digest_reuses_warm_index_cache(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_HOME", str(tmp_path))
    path = tmp_path / "warm.bin"
    path.write_bytes(b"hello")
    cache_mod.store_file_digest(path, format="bin", object_id="sha256:" + "b" * 64)

    index_path = cache_mod._cache_path()  # noqa: SLF001
    original_read_text = Path.read_text

    def _fail_index_read_text(self, *args, **kwargs):  # noqa: ANN001
        if Path(self) == index_path:
            raise AssertionError("warm digest cache lookup must not re-read index.json")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _fail_index_read_text)

    assert cache_mod.lookup_file_digest(path, format="bin") == "sha256:" + "b" * 64
