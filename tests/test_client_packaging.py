from __future__ import annotations

import importlib
import io
import tarfile
from pathlib import Path


def _tar_names(blob: bytes) -> list[str]:
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tf:
        return sorted(tf.getnames())


def _tar_blob(path: Path) -> bytes:
    return path.read_bytes()


def test_dependency_packager_function_is_deterministic_and_skips_bytecode(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.dependency import DependencyPackager

    pkg_dir = tmp_path / "demo_packager_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (pkg_dir / "helper.py").write_text(
        "def normalize(value):\n"
        "    return int(value)\n",
        encoding="utf-8",
    )
    (pkg_dir / "worker.py").write_text(
        "from .helper import normalize\n\n"
        "def run(value=0, **_kwargs):\n"
        "    return {'value': normalize(value)}\n",
        encoding="utf-8",
    )
    pycache_dir = pkg_dir / "__pycache__"
    pycache_dir.mkdir()
    (pycache_dir / "worker.cpython-313.pyc").write_bytes(b"compiled-bytecode")

    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    worker = importlib.import_module("demo_packager_pkg.worker")

    packager = DependencyPackager()
    tar_a = Path(packager.package_function(worker.run))
    tar_b = Path(packager.package_function(worker.run))
    try:
        blob_a = _tar_blob(tar_a)
        blob_b = _tar_blob(tar_b)
        assert blob_a == blob_b
        names = _tar_names(blob_a)
        assert "demo_packager_pkg/__init__.py" in names
        assert "demo_packager_pkg/helper.py" in names
        assert "demo_packager_pkg/worker.py" in names
        assert not any("__pycache__" in name for name in names)
        assert not any(name.endswith(".pyc") for name in names)
    finally:
        tar_a.unlink(missing_ok=True)
        tar_b.unlink(missing_ok=True)


def test_prepare_code_blob_from_directory_is_deterministic_and_skips_bytecode(tmp_path):
    from pycloud_parallel.controlplane.client import _prepare_code_blob

    artifact_dir = tmp_path / "artifact_dir"
    artifact_dir.mkdir()
    (artifact_dir / "main.py").write_text("def run():\n    return 1\n", encoding="utf-8")
    (artifact_dir / "helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    pycache_dir = artifact_dir / "__pycache__"
    pycache_dir.mkdir()
    (pycache_dir / "main.cpython-313.pyc").write_bytes(b"compiled-bytecode")

    blob_a, filename_a = _prepare_code_blob(artifact_path=str(artifact_dir))
    blob_b, filename_b = _prepare_code_blob(artifact_path=str(artifact_dir))

    assert filename_a == "artifact_dir.tar.gz"
    assert filename_b == "artifact_dir.tar.gz"
    assert blob_a == blob_b
    names = _tar_names(blob_a or b"")
    assert "main.py" in names
    assert "helper.py" in names
    assert not any("__pycache__" in name for name in names)
    assert not any(name.endswith(".pyc") for name in names)


def test_prepare_code_blob_from_path_list_uses_deterministic_targz(tmp_path):
    from pycloud_parallel.controlplane.client import _prepare_code_blob

    pkg_dir = tmp_path / "bundle_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (pkg_dir / "worker.py").write_text("def run():\n    return 1\n", encoding="utf-8")
    pycache_dir = pkg_dir / "__pycache__"
    pycache_dir.mkdir()
    (pycache_dir / "worker.cpython-313.pyc").write_bytes(b"compiled-bytecode")
    (tmp_path / "alpha.py").write_text("VALUE = 1\n", encoding="utf-8")

    blob_a, filename_a = _prepare_code_blob(artifact_path=[tmp_path / "alpha.py", pkg_dir])
    blob_b, filename_b = _prepare_code_blob(artifact_path=[tmp_path / "alpha.py", pkg_dir])

    assert filename_a == "artifact_bundle.tar.gz"
    assert filename_b == "artifact_bundle.tar.gz"
    assert blob_a == blob_b
    names = _tar_names(blob_a or b"")
    assert "alpha.py" in names
    assert "bundle_pkg/__init__.py" in names
    assert "bundle_pkg/worker.py" in names
    assert not any("__pycache__" in name for name in names)
    assert not any(name.endswith(".pyc") for name in names)


def test_package_paths_to_targz_is_deterministic_and_relative(tmp_path):
    from pycloud_parallel.controlplane.client import _package_paths_to_targz

    root_dir = tmp_path / "root"
    pkg_dir = root_dir / "pkg"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (pkg_dir / "worker.py").write_text("def run():\n    return 1\n", encoding="utf-8")
    pycache_dir = pkg_dir / "__pycache__"
    pycache_dir.mkdir()
    (pycache_dir / "worker.cpython-313.pyc").write_bytes(b"compiled-bytecode")
    (root_dir / "standalone.py").write_text("VALUE = 1\n", encoding="utf-8")

    tar_a = _package_paths_to_targz(root_dir=root_dir, paths=["pkg", "standalone.py"])
    tar_b = _package_paths_to_targz(root_dir=root_dir, paths=["pkg", "standalone.py"])
    try:
        blob_a = _tar_blob(tar_a)
        blob_b = _tar_blob(tar_b)
        assert blob_a == blob_b
        names = _tar_names(blob_a)
        assert "pkg/__init__.py" in names
        assert "pkg/worker.py" in names
        assert "standalone.py" in names
        assert not any(name.startswith("root/") for name in names)
        assert not any("__pycache__" in name for name in names)
        assert not any(name.endswith(".pyc") for name in names)
    finally:
        tar_a.unlink(missing_ok=True)
        tar_b.unlink(missing_ok=True)
