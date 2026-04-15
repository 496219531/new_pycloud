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
    (pkg_dir / "notes.md").write_text("ignore me\n", encoding="utf-8")
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
        assert "demo_packager_pkg/notes.md" not in names
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
    (artifact_dir / "native_helper.so").write_bytes(b"binary-so")
    (artifact_dir / "data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
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
    assert "native_helper.so" in names
    assert "data.csv" not in names
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
    (pkg_dir / "ignored.json").write_text("{\"ok\": true}\n", encoding="utf-8")
    (tmp_path / "alpha.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / "native_ext.pyd").write_bytes(b"binary-pyd")
    (tmp_path / "notes.txt").write_text("skip me\n", encoding="utf-8")

    blob_a, filename_a = _prepare_code_blob(artifact_path=[tmp_path / "alpha.py", pkg_dir, tmp_path / "native_ext.pyd", tmp_path / "notes.txt"])
    blob_b, filename_b = _prepare_code_blob(artifact_path=[tmp_path / "alpha.py", pkg_dir, tmp_path / "native_ext.pyd", tmp_path / "notes.txt"])

    assert filename_a == "artifact_bundle.tar.gz"
    assert filename_b == "artifact_bundle.tar.gz"
    assert blob_a == blob_b
    names = _tar_names(blob_a or b"")
    assert "alpha.py" in names
    assert "native_ext.pyd" in names
    assert "bundle_pkg/__init__.py" in names
    assert "bundle_pkg/worker.py" in names
    assert "bundle_pkg/ignored.json" not in names
    assert "notes.txt" not in names
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
    (root_dir / "native_ext.so").write_bytes(b"binary-so")
    (root_dir / "ignored.csv").write_text("a,b\n1,2\n", encoding="utf-8")

    tar_a = _package_paths_to_targz(root_dir=root_dir, paths=["pkg", "standalone.py", "native_ext.so", "ignored.csv"])
    tar_b = _package_paths_to_targz(root_dir=root_dir, paths=["pkg", "standalone.py", "native_ext.so", "ignored.csv"])
    try:
        blob_a = _tar_blob(tar_a)
        blob_b = _tar_blob(tar_b)
        assert blob_a == blob_b
        names = _tar_names(blob_a)
        assert "pkg/__init__.py" in names
        assert "pkg/worker.py" in names
        assert "standalone.py" in names
        assert "native_ext.so" in names
        assert "ignored.csv" not in names
        assert not any(name.startswith("root/") for name in names)
        assert not any("__pycache__" in name for name in names)
        assert not any(name.endswith(".pyc") for name in names)
    finally:
        tar_a.unlink(missing_ok=True)
        tar_b.unlink(missing_ok=True)


def test_prepare_code_blob_from_loaded_module_uses_whitelisted_python_file_closure(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.client import _prepare_code_blob

    package_dir = tmp_path / "calc_asset_ratio"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("from . import calc_asset_ratio\n", encoding="utf-8")
    (package_dir / "calc_asset_ratio.py").write_text(
        "def get_fund_asset_ratio(value=0, **_kwargs):\n"
        "    return {'value': value}\n",
        encoding="utf-8",
    )
    (package_dir / "fund_nav_df.csv").write_text("FundID,AdjustedNav\n1,1.0\n", encoding="utf-8")
    (package_dir / "README.md").write_text("ignore me\n", encoding="utf-8")
    (tmp_path / "calc_asset_ratio_job_module.py").write_text(
        "from calc_asset_ratio import calc_asset_ratio\n\n"
        "def task_generator(**_kwargs):\n"
        "    return [{'value': 1}]\n\n"
        "def run(value=0, **_kwargs):\n"
        "    return calc_asset_ratio.get_fund_asset_ratio(value=value)\n\n"
        "def handle_data(task_id, result, state=None, **_kwargs):\n"
        "    state = state or {'items': []}\n"
        "    state['items'].append((task_id, result))\n"
        "    return state\n\n"
        "def finalize(state=None, **_kwargs):\n"
        "    return list((state or {}).get('items', []))\n",
        encoding="utf-8",
    )
    (tmp_path / "workspace_notes.md").write_text("skip me\n", encoding="utf-8")

    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    module = importlib.import_module("calc_asset_ratio_job_module")

    blob, filename = _prepare_code_blob(module=module)

    assert filename == "calc_asset_ratio_job_module.tar.gz"
    names = _tar_names(blob or b"")
    assert "calc_asset_ratio_job_module.py" in names
    assert "calc_asset_ratio/__init__.py" in names
    assert "calc_asset_ratio/calc_asset_ratio.py" in names
    assert "calc_asset_ratio/fund_nav_df.csv" not in names
    assert "calc_asset_ratio/README.md" not in names
    assert "workspace_notes.md" not in names


def test_prepare_local_artifact_for_upload_directory_uses_actual_targz_format(tmp_path):
    from pycloud_parallel.controlplane.client import _prepare_local_artifact_for_upload

    artifact_dir = tmp_path / "artifact_dir"
    artifact_dir.mkdir()
    (artifact_dir / "main.py").write_text("def run():\n    return 1\n", encoding="utf-8")

    prepared = _prepare_local_artifact_for_upload(artifact_dir, package_format="zip")
    try:
        assert prepared.source_path == artifact_dir
        assert prepared.upload_path.exists()
        assert prepared.upload_path.suffixes[-2:] == [".tar", ".gz"]
        assert prepared.filename == "artifact_dir.tar.gz"
        assert prepared.package_format == "tar.gz"
    finally:
        prepared.cleanup()

    assert not prepared.upload_path.exists()


def test_package_module_for_debug_writes_local_tar_and_lists_entries(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.dependency import package_module_for_debug

    package_dir = tmp_path / "debug_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("from . import worker\n", encoding="utf-8")
    (package_dir / "worker.py").write_text(
        "def run(value=0, **_kwargs):\n"
        "    return {'value': value}\n",
        encoding="utf-8",
    )
    (package_dir / "payload.csv").write_text("value\n1\n", encoding="utf-8")

    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    module = importlib.import_module("debug_pkg.worker")
    output_file = tmp_path / "debug_pkg_bundle.tar.gz"

    debug_info = package_module_for_debug(module, output_file=str(output_file))

    assert debug_info["package_path"] == str(output_file.resolve())
    assert output_file.exists()
    assert debug_info["entries"] == [
        "debug_pkg/__init__.py",
        "debug_pkg/worker.py",
    ]


def test_upload_code_from_file_directory_reuses_prepared_artifact(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.client import ArtifactDeps, NodeControlClient

    artifact_dir = tmp_path / "artifact_dir"
    artifact_dir.mkdir()
    (artifact_dir / "main.py").write_text("def run():\n    return 1\n", encoding="utf-8")

    captured = {}
    client = NodeControlClient.__new__(NodeControlClient)

    def fake_upload(self, **kwargs):
        captured.update(kwargs)
        assert kwargs["file_path"].exists()
        return "ok"

    monkeypatch.setattr(NodeControlClient, "_upload_code_from_local_file", fake_upload)

    result = client.upload_code_from_file(
        client_id="client-1",
        artifact_path=str(artifact_dir),
        package_format="zip",
        deps=ArtifactDeps.allow_install(["orjson==3.10.18"]),
    )

    assert result == "ok"
    assert captured["package_format"] == "tar.gz"
    assert captured["dependency_policy_mode"] == "allow_install"
    assert captured["dependency_allowlist"] == ("orjson==3.10.18",)
    assert captured["file_path"].name.endswith(".tar.gz")
    assert not captured["file_path"].exists()


def test_create_service_from_file_directory_reuses_prepared_artifact(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.client import ArtifactDeps, NodeControlClient

    artifact_dir = tmp_path / "artifact_dir"
    artifact_dir.mkdir()
    (artifact_dir / "main.py").write_text("def run():\n    return 1\n", encoding="utf-8")

    captured = {}
    client = NodeControlClient.__new__(NodeControlClient)

    def fake_create(self, **kwargs):
        captured.update(kwargs)
        assert kwargs["file_path"].exists()
        return "service"

    monkeypatch.setattr(NodeControlClient, "_create_service_from_local_file", fake_create)

    result = client.create_service_from_file(
        owner_client_id="owner-1",
        artifact_path=str(artifact_dir),
        service_name="svc",
        package_format="zip",
        deps=ArtifactDeps.node_preinstalled(),
    )

    assert result == "service"
    assert captured["package_format"] == "tar.gz"
    assert captured["dependency_policy_mode"] == "node_preinstalled"
    assert captured["dependency_allowlist"] == ()
    assert captured["file_path"].name.endswith(".tar.gz")
    assert not captured["file_path"].exists()
