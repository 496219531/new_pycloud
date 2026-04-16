from __future__ import annotations

"""Unified runtime compatibility errors for the V1 migration path."""

from dataclasses import dataclass
from typing import Sequence, Type


@dataclass(frozen=True)
class RuntimeMismatchCandidate:
    label: str
    python_version: str


def format_runtime_mismatch_message(
    *,
    requested_runtime: str,
    candidates: Sequence[RuntimeMismatchCandidate],
    scope: str = "nodes",
) -> str:
    normalized_runtime = str(requested_runtime or "").strip() or "(unspecified)"
    serialized_candidates = []
    for item in candidates:
        label = str(item.label or "").strip() or "unknown"
        python_version = str(item.python_version or "").strip() or "unknown"
        serialized_candidates.append(f"{label}({python_version})")
    discovered = ", ".join(serialized_candidates) if serialized_candidates else "(none discovered)"
    fix = (
        f"Fix: upgrade target nodes to satisfy {normalized_runtime}, "
        "relax the requested runtime, or choose nodes whose python_version already matches."
    )
    return (
        f"python runtime mismatch: requested_runtime={normalized_runtime}; "
        f"discovered_{str(scope or 'nodes').strip().replace(' ', '_')}={discovered}; "
        f"{fix}"
    )


def make_runtime_mismatch_error(
    exc_type: Type[BaseException],
    *,
    requested_runtime: str,
    candidates: Sequence[RuntimeMismatchCandidate],
    scope: str = "nodes",
) -> BaseException:
    return exc_type(
        format_runtime_mismatch_message(
            requested_runtime=requested_runtime,
            candidates=candidates,
            scope=scope,
        )
    )
