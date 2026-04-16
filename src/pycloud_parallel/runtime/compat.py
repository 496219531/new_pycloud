from __future__ import annotations

"""Helpers for building consistent runtime compatibility diagnostics."""

from typing import Any, Iterable, List

from .errors import RuntimeMismatchCandidate, format_runtime_mismatch_message


def runtime_mismatch_candidates_from_nodes(nodes: Iterable[Any]) -> List[RuntimeMismatchCandidate]:
    out: List[RuntimeMismatchCandidate] = []
    for node in nodes or ():
        label = (
            str(getattr(node, "node_instance_id", "") or "").strip()
            or str(getattr(node, "node_id", "") or "").strip()
            or str(getattr(node, "control_addr", "") or "").strip()
            or "unknown"
        )
        python_version = str(getattr(node, "python_version", "") or "").strip() or "unknown"
        out.append(RuntimeMismatchCandidate(label=label, python_version=python_version))
    return out


def runtime_mismatch_candidates_for_current_node(node_python_version: str) -> List[RuntimeMismatchCandidate]:
    return [
        RuntimeMismatchCandidate(
            label="current_node",
            python_version=str(node_python_version or "").strip() or "unknown",
        )
    ]


def runtime_mismatch_message_for_nodes(*, requested_runtime: str, nodes: Iterable[Any], scope: str = "nodes") -> str:
    return format_runtime_mismatch_message(
        requested_runtime=requested_runtime,
        candidates=runtime_mismatch_candidates_from_nodes(nodes),
        scope=scope,
    )


def runtime_mismatch_message_for_current_node(*, requested_runtime: str, node_python_version: str) -> str:
    return format_runtime_mismatch_message(
        requested_runtime=requested_runtime,
        candidates=runtime_mismatch_candidates_for_current_node(node_python_version),
        scope="nodes",
    )
