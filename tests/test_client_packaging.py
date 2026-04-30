from __future__ import annotations

import importlib
import io
import sys
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


def test_prepare_code_blob_from_loaded_module_uses_whitelisted_python_file_closure(tmp_path, monkeypatch):
    from pycloud_parallel.execution.support import _prepare_code_blob

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


def test_prepare_code_blob_from_loaded_module_includes_only_explicit_resource_paths(tmp_path, monkeypatch):
    from pycloud_parallel.execution.support import _prepare_code_blob

    package_dir = tmp_path / "calc_asset_ratio"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("from . import calc_asset_ratio\n", encoding="utf-8")
    (package_dir / "calc_asset_ratio.py").write_text(
        "def get_fund_asset_ratio(value=0, **_kwargs):\n"
        "    return {'value': value}\n",
        encoding="utf-8",
    )
    (tmp_path / "calc_asset_ratio_job_module.py").write_text(
        "from pathlib import Path\n"
        "import pandas as pd\n"
        "def task_generator(**_kwargs):\n"
        "    return pd.read_csv(Path(__file__).resolve().parent / 'fund_nav_df.csv').to_dict('records')\n",
        encoding="utf-8",
    )
    (tmp_path / "fund_nav_df.csv").write_text("FundID,AdjustedNav\n1,1.0\n", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("keep out\n", encoding="utf-8")

    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("calc_asset_ratio_job_module", None)
    importlib.invalidate_caches()
    module = importlib.import_module("calc_asset_ratio_job_module")

    blob, filename = _prepare_code_blob(module=module, resource_paths=["fund_nav_df.csv"])

    assert filename == "calc_asset_ratio_job_module.tar.gz"
    names = _tar_names(blob or b"")
    assert "calc_asset_ratio_job_module.py" in names
    assert "fund_nav_df.csv" in names
    assert "notes.txt" not in names


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


def test_node_control_client_legacy_code_upload_wrappers_removed():
    from pycloud_parallel.controlplane.node_control_client import NodeControlClient

    assert not hasattr(NodeControlClient, "upload_code_from_file")
    assert not hasattr(NodeControlClient, "upload_code_from_bytes")


def test_node_control_client_legacy_service_file_wrappers_removed():
    from pycloud_parallel.controlplane.node_control_client import NodeControlClient

    assert not hasattr(NodeControlClient, "create_service_from_file")
    assert not hasattr(NodeControlClient, "create_service_from_paths")
