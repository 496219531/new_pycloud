"""Public API for the V1 architecture target."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

_API_EXPORTS = {
    "DataRef",
    "JobQueue",
    "Service",
    "TaskPool",
    "export",
}

_API_DEP_HINT = (
    "Control-plane dependencies are missing. "
    'Reinstall with `pip install pycloud-parallel` (or avoid `--no-deps`).'
)

__version__ = "0.2.13"


def _import_api() -> Any:
    try:
        return importlib.import_module(".api", __name__)
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing in {"google", "protobuf"} or missing.startswith("google."):
            raise ModuleNotFoundError(_API_DEP_HINT) from exc
        raise


if TYPE_CHECKING:
    from .api import DataRef, JobQueue, Service, TaskPool, export


def _try_bind_api_exports() -> None:
    try:
        from .api import DataRef, JobQueue, Service, TaskPool, export
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "")
        if missing in {"google", "protobuf"} or missing.startswith("google."):
            return
        raise

    globals().update(
        {
            "DataRef": DataRef,
            "JobQueue": JobQueue,
            "Service": Service,
            "TaskPool": TaskPool,
            "export": export,
        }
    )


_try_bind_api_exports()


def __getattr__(name: str):
    if name in _API_EXPORTS:
        module = _import_api()
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | _API_EXPORTS)


__all__ = [
    "DataRef",
    "JobQueue",
    "Service",
    "TaskPool",
    "export",
]
