from __future__ import annotations

"""Standalone helpers for node-side apply_managed_globals hook semantics."""

from typing import Any, Dict, Optional

from pycloud_parallel.controlplane.node.execution import _resolve_apply_managed_globals_hook


def has_apply_managed_globals_hook(module: Any) -> bool:
    return _resolve_apply_managed_globals_hook(module) is not None


def apply_managed_globals_hook(
    module: Any,
    *,
    values: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    hook = _resolve_apply_managed_globals_hook(module)
    if hook is None:
        return dict(values or {})
    result = hook(dict(values or {}), **dict(context or {}))
    if result is None:
        return None
    if not isinstance(result, dict):
        raise RuntimeError("apply_managed_globals must return None or dict")
    return dict(result)


__all__ = [
    "apply_managed_globals_hook",
    "has_apply_managed_globals_hook",
]
