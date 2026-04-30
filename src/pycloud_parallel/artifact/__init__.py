"""V1 artifact package target."""

from __future__ import annotations

from pycloud_parallel.controlplane.artifact import Artifact, ArtifactDeps, ArtifactExports


def export(fn):
    fn.__pycloud_export__ = True
    return fn


__all__ = [
    "Artifact",
    "ArtifactDeps",
    "ArtifactExports",
    "export",
]
