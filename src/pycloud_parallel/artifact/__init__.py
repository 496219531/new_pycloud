"""V1 artifact package target."""

from __future__ import annotations


def export(fn):
    fn.__pycloud_export__ = True
    return fn


pycloud_export = export

__all__ = ["export", "pycloud_export"]
