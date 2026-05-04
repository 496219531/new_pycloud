from __future__ import annotations

"""Backward-compatible import name for the HTTP NodeControl client."""

from pycloud_parallel.controlplane.node_control_http import HttpNodeControlClient


class NodeControlClient(HttpNodeControlClient):
    pass


__all__ = ["NodeControlClient"]
