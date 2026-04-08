"""Public API for lightweight local parallel execution."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

_LOCAL_RUNTIME_EXPORTS = {
    "configure",
    "foreach",
    "parallel_for",
    "ForeachResult",
    "TaskError",
}

_CONTROLPLANE_EXPORTS = {
    "ObjectRef",
    "ResultRef",
    "pycloud_export",
    "DeployedService",
    "DedicatedTaskServiceSession",
    "DirectConnect",
    "GatewayConnect",
    "JobQueueClient",
    "TaskPoolSession",
}

_CONTROLPLANE_DEP_HINT = (
    "Control-plane dependencies are missing. "
    'Reinstall with `pip install pycloud-parallel` (or avoid `--no-deps`).'
)

_LOCAL_RUNTIME_DEP_HINT = (
    "Local runtime dependencies are missing. "
    'Reinstall with `pip install pycloud-parallel` (or avoid `--no-deps`).'
)

__version__ = "0.1.10"


def pycloud_export(fn):
    fn.__pycloud_export__ = True
    return fn


def _import_local_runtime(name: str) -> Any:
    module_name = ".local_runtime.types" if name in {"ForeachResult", "TaskError"} else ".local_runtime.api"
    try:
        return importlib.import_module(module_name, __name__)
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing == "cloudpickle":
            raise ModuleNotFoundError(_LOCAL_RUNTIME_DEP_HINT) from exc
        raise


def _import_controlplane() -> Any:
    try:
        return importlib.import_module(".controlplane", __name__)
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing == "grpc" or missing == "google" or missing.startswith("google."):
            raise ModuleNotFoundError(_CONTROLPLANE_DEP_HINT) from exc
        raise


if TYPE_CHECKING:
    from .controlplane import (
        DedicatedTaskServiceSession,
        DeployedService,
        DirectConnect,
        GatewayConnect,
        JobQueueClient,
        ObjectRef,
        ResultRef,
        TaskPoolSession,
    )
    from .local_runtime.api import configure, foreach, parallel_for
    from .local_runtime.types import ForeachResult, TaskError


def _try_bind_local_runtime_exports() -> None:
    try:
        from .local_runtime.api import configure, foreach, parallel_for
        from .local_runtime.types import ForeachResult, TaskError
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing == "cloudpickle":
            return
        raise

    globals().update(
        {
            "configure": configure,
            "foreach": foreach,
            "parallel_for": parallel_for,
            "ForeachResult": ForeachResult,
            "TaskError": TaskError,
        }
    )


def _try_bind_controlplane_exports() -> None:
    try:
        from .controlplane import (
            DedicatedTaskServiceSession,
            DeployedService,
            DirectConnect,
            GatewayConnect,
            JobQueueClient,
            ObjectRef,
            ResultRef,
            TaskPoolSession,
        )
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing == "grpc" or missing == "google" or missing.startswith("google."):
            return
        raise

    globals().update(
        {
            "ObjectRef": ObjectRef,
            "ResultRef": ResultRef,
            "DeployedService": DeployedService,
            "DedicatedTaskServiceSession": DedicatedTaskServiceSession,
            "DirectConnect": DirectConnect,
            "GatewayConnect": GatewayConnect,
            "JobQueueClient": JobQueueClient,
            "TaskPoolSession": TaskPoolSession,
        }
    )


_try_bind_local_runtime_exports()
_try_bind_controlplane_exports()


def __getattr__(name: str):
    if name in _LOCAL_RUNTIME_EXPORTS:
        module = _import_local_runtime(name)
        value = getattr(module, name)
        globals()[name] = value
        return value
    if name in _CONTROLPLANE_EXPORTS:
        module = _import_controlplane()
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | _LOCAL_RUNTIME_EXPORTS | _CONTROLPLANE_EXPORTS)


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
    "DedicatedTaskServiceSession",
    "DirectConnect",
    "GatewayConnect",
    "JobQueueClient",
    "TaskPoolSession",
]
