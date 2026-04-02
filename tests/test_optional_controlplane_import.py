from __future__ import annotations

import importlib
import sys

import pytest


def _import_root_module():
    sys.modules.pop("pycloud_parallel.controlplane.client", None)
    sys.modules.pop("pycloud_parallel.controlplane", None)
    sys.modules.pop("pycloud_parallel", None)
    return importlib.import_module("pycloud_parallel")


def test_import_pycloud_parallel_does_not_eager_import_controlplane() -> None:
    module = _import_root_module()
    assert callable(module.foreach)
    assert "pycloud_parallel.controlplane" not in sys.modules


def test_access_controlplane_symbol_without_grpc_shows_hint(monkeypatch) -> None:
    module = _import_root_module()
    module.__dict__.pop("TaskSubmitter", None)

    real_import_module = module.importlib.import_module

    def _fake_import_module(name, package=None):
        if name == ".controlplane" and package == "pycloud_parallel":
            exc = ModuleNotFoundError("No module named 'grpc'")
            exc.name = "grpc"
            raise exc
        return real_import_module(name, package)

    monkeypatch.setattr(module.importlib, "import_module", _fake_import_module)

    with pytest.raises(ModuleNotFoundError, match=r"pip install pycloud-parallel"):
        _ = module.TaskSubmitter
