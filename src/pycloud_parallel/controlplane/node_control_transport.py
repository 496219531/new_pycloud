from __future__ import annotations

"""Shared HTTP NodeControl client selection helpers."""

from typing import Any


def node_control_client(*args: Any, **kwargs: Any):
    kwargs.pop("transport", None)
    from pycloud_parallel.controlplane.node_control_client import NodeControlClient

    return NodeControlClient(*args, **kwargs)


def node_control_target_for_node(node: Any) -> str:
    capability = getattr(node, "capability", None)
    node_http_base_url = str(getattr(capability, "node_http_base_url", "") or "").strip()
    if node_http_base_url:
        return node_http_base_url
    return str(getattr(node, "control_addr", "") or "").strip()


def node_control_target_for_route(route: Any) -> str:
    capability = getattr(route, "capability", None)
    node_http_base_url = str(getattr(capability, "node_http_base_url", "") or "").strip()
    if node_http_base_url:
        return node_http_base_url
    return str(getattr(route, "control_addr", "") or "").strip()


def new_node_control_client(target: str, *, timeout_sec: float):
    return node_control_client(target, timeout_sec=timeout_sec)


__all__ = [
    "new_node_control_client",
    "node_control_client",
    "node_control_target_for_node",
    "node_control_target_for_route",
]
