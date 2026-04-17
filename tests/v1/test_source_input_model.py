from __future__ import annotations

import types

from pycloud_parallel.artifact import Artifact, ArtifactDeps
from pycloud_parallel.controlplane.artifact import _normalize_artifact_input, _prepare_artifact


def test_source_input_normalizes_to_artifact_for_service_task_and_job():
    module = types.ModuleType("demo_source_module")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    for consumer_kind in ("service", "task", "job"):
        artifact = _normalize_artifact_input(
            consumer_kind=consumer_kind,
            source=module,
            runtime="py3",
            entry_callable="run",
        )
        prepared = _prepare_artifact(artifact, consumer_kind=consumer_kind)
        assert artifact.source_kind == "module"
        assert prepared.entry_callable == "run"
        assert prepared.entry_module == "demo_source_module"
        assert prepared.runtime == "py3"


def test_artifact_package_exposes_advanced_types_without_top_level_promotion():
    artifact = Artifact.from_bytes(
        b"def run(**_kwargs):\n    return {'ok': True}\n",
        package_format="py",
        entry_module="artifact_demo",
        deps=ArtifactDeps.allow_install(["orjson==3.10.18"]),
    )

    prepared = _prepare_artifact(
        _normalize_artifact_input(
            consumer_kind="service",
            artifact=artifact,
        ),
        consumer_kind="service",
    )

    assert prepared.entry_module == "artifact_demo"
    assert prepared.dependency_policy.mode == "allow_install"
    assert prepared.dependency_allowlist == ("orjson==3.10.18",)
