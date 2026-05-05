from __future__ import annotations

"""Shared HTTP NodeControl client selection helpers."""

from typing import Any

from pycloud_parallel.controlplane.node_capability import control_base_url_from_capability


def node_control_client(*args: Any, **kwargs: Any):
    kwargs.pop("transport", None)
    from pycloud_parallel.controlplane.node_control_client import NodeControlClient

    return NodeControlClient(*args, **kwargs)


def node_control_target_for_node(node: Any) -> str:
    control_base_url = control_base_url_from_capability(node)
    if control_base_url:
        return control_base_url
    return str(getattr(node, "control_addr", "") or "").strip()


def node_control_target_for_route(route: Any) -> str:
    control_base_url = control_base_url_from_capability(route)
    if control_base_url:
        return control_base_url
    return str(getattr(route, "control_addr", "") or "").strip()


def new_node_control_client(target: str, *, timeout_sec: float):
    return node_control_client(target, timeout_sec=timeout_sec)


__all__ = [
    "new_node_control_client",
    "node_control_client",
    "node_control_target_for_node",
    "node_control_target_for_route",
]
