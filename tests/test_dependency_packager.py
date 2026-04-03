import importlib
import sys
import tarfile
from pathlib import Path

import pycloud_parallel

from pycloud_parallel.controlplane.client import _default_entry_module_for_func
from pycloud_parallel.controlplane.dependency import DependencyPackager, auto_deploy_function


def _write_demo_package(base_dir: Path, *, package_name: str = "apppkg") -> Path:
    pkg_dir = base_dir / package_name
    pkg_dir.mkdir(parents=True, exist_ok=True)
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (pkg_dir / "helper.py").write_text(
        "def double(value):\n"
        "    return int(value) * 2\n",
        encoding="utf-8",
    )
    (pkg_dir / "data.txt").write_text("hello-resource\n", encoding="utf-8")
    (pkg_dir / "main.py").write_text(
        "from .helper import double\n"
        "from pathlib import Path\n\n"
        "def run(value=0):\n"
        "    data = Path(__file__).with_name('data.txt').read_text(encoding='utf-8').strip()\n"
        "    return {'value': double(value), 'data': data}\n",
        encoding="utf-8",
    )
    return pkg_dir


def test_package_function_includes_package_tree_and_resources(tmp_path):
    _write_demo_package(tmp_path)
    sys.path.insert(0, str(tmp_path))
    try:
        importlib.invalidate_caches()
        mod = importlib.import_module("apppkg.main")
        packager = DependencyPackager()
        archive_path = Path(packager.package_function(mod.run))

        with tarfile.open(archive_path, "r:gz") as tf:
            names = set(tf.getnames())

        assert "apppkg/main.py" in names
        assert "apppkg/helper.py" in names
        assert "apppkg/__init__.py" in names
        assert "apppkg/data.txt" in names
        archive_path.unlink(missing_ok=True)
    finally:
        sys.path.remove(str(tmp_path))
        for name in ["apppkg.main", "apppkg.helper", "apppkg"]:
            sys.modules.pop(name, None)


def test_package_module_includes_full_package_tree(tmp_path):
    _write_demo_package(tmp_path, package_name="pkgmod")
    sys.path.insert(0, str(tmp_path))
    try:
        importlib.invalidate_caches()
        importlib.import_module("pkgmod.main")
        packager = DependencyPackager()
        archive_path = Path(packager.package_module("pkgmod.main"))

        with tarfile.open(archive_path, "r:gz") as tf:
            names = set(tf.getnames())

        assert "pkgmod/main.py" in names
        assert "pkgmod/helper.py" in names
        assert "pkgmod/data.txt" in names
        archive_path.unlink(missing_ok=True)
    finally:
        sys.path.remove(str(tmp_path))
        for name in ["pkgmod.main", "pkgmod.helper", "pkgmod"]:
            sys.modules.pop(name, None)


def test_default_entry_module_for_main_function_uses_source_filename():
    def local_func():
        return 1

    local_func.__module__ = "__main__"
    entry_module = _default_entry_module_for_func(local_func)
    assert entry_module == "test_dependency_packager"


def test_auto_deploy_function_uses_task_submitter_from_infocenter(tmp_path, monkeypatch):
    module_file = tmp_path / "demo_mod.py"
    module_file.write_text(
        "def work(x):\n"
        "    return x + 1\n",
        encoding="utf-8",
    )
    sys.path.insert(0, str(tmp_path))
    try:
        importlib.invalidate_caches()
        mod = importlib.import_module("demo_mod")
        captured = {}

        class FakeTaskSubmitter:
            @staticmethod
            def from_infocenter(**kwargs):
                captured.update(kwargs)
                return "ok"

        monkeypatch.setattr(pycloud_parallel, "TaskSubmitter", FakeTaskSubmitter)

        result = auto_deploy_function(
            mod.work,
            infocenter_target="127.0.0.1:50051",
            runtime="py3",
        )

        assert result == "ok"
        assert captured["infocenter_target"] == "127.0.0.1:50051"
        assert captured["entry_module"] == "demo_mod"
        assert captured["entry_callable"] == "work"
        assert captured["package_format"] == "tar.gz"
        assert isinstance(captured["blob"], bytes) and captured["blob"]
        assert "filename" not in captured
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("demo_mod", None)


def test_auto_deploy_function_infers_main_entry_module(monkeypatch):
    captured = {}

    class FakeTaskSubmitter:
        @staticmethod
        def from_infocenter(**kwargs):
            captured.update(kwargs)
            return "ok"

    monkeypatch.setattr(pycloud_parallel, "TaskSubmitter", FakeTaskSubmitter)

    def local_func():
        return 1

    local_func.__module__ = "__main__"

    result = auto_deploy_function(
        local_func,
        infocenter_target="127.0.0.1:50051",
        runtime="py3",
    )

    assert result == "ok"
    assert captured["entry_module"] == "test_dependency_packager"
    assert captured["entry_callable"] == "local_func"
    assert "filename" not in captured
