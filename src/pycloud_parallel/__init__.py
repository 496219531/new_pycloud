"""Public API for lightweight local parallel execution."""

from __future__ import annotations

import importlib
from typing import Any

from .local_runtime.api import configure, foreach, parallel_for
from .local_runtime.types import ForeachResult, TaskError

_CONTROLPLANE_EXPORTS = {
    "ObjectRef",
    "ResultRef",
    "pycloud_export",
    "DeployedService",
    "DirectConnect",
    "GatewayConnect",
    "TaskSubmitter",
}

_CONTROLPLANE_DEP_HINT = (
    "Control-plane dependencies are missing. "
    'Reinstall with `pip install pycloud-parallel` (or avoid `--no-deps`).'
)


def pycloud_export(fn):
    fn.__pycloud_export__ = True
    return fn


def _import_controlplane() -> Any:
    try:
        return importlib.import_module(".controlplane", __name__)
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing == "grpc" or missing == "google" or missing.startswith("google."):
            raise ModuleNotFoundError(_CONTROLPLANE_DEP_HINT) from exc
        raise


def __getattr__(name: str):
    if name in _CONTROLPLANE_EXPORTS:
        module = _import_controlplane()
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | _CONTROLPLANE_EXPORTS)


__all__ = [
    # Local Runtime
    "ForeachResult",
    "TaskError",
    "configure",
    "foreach",
    "parallel_for",
    # ControlPlane Module Clients（新命名）
    "ObjectRef",
    "ResultRef",
    "pycloud_export",
    "DeployedService",
    "DirectConnect",
    "GatewayConnect",
    "TaskSubmitter",
]
