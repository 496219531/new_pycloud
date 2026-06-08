"""中文说明：验证 HTTP 控制面的核心状态流转（内存后端）。"""

import hashlib
import importlib
import inspect
import io
import json
import math
import os
import sys
import tarfile
import time
import types
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.request import Request, urlopen

import pytest

from pycloud_parallel.controlplane.code_version import _code_version_from_digest
from pycloud_parallel.controlplane.infocenter.models import NodeServiceState
from pycloud_parallel.controlplane.infocenter_state import InfoCenterState
from pycloud_parallel.controlplane.node.execution import (
    _build_execute_spec,
    _describe_artifact_error,
    _execute_payload_in_subprocess,
    _load_user_module,
    _purge_loaded_artifact_modules,
)
from pycloud_parallel.controlplane.node.filesystem import (
    _code_content_dir,
    _code_data_dir,
    _code_index_link_path,
    _code_pkg_dir,
    _code_variant_dir,
    _load_code_meta,
    _write_code_meta,
)
from pycloud_parallel.controlplane.node.models import CodeArtifact, ServiceSession, StoredResultArtifact, TaskPoolState, TaskState
from pycloud_parallel.controlplane.node.results import (
    LargeResultError,
    ObjectResolutionError,
    _commit_result_file,
    _materialize_object_bytes,
    _normalize_user_return,
    _resolve_object_refs_in_payload,
    _resolve_single_data_ref,
)
from pycloud_parallel.controlplane.pickle_stable_v1 import stable_pickle_loads
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.controlplane.replica_client import ServiceSessionClient
from pycloud_parallel.controlplane.serialization import (
    decode_inline_transport_carrier,
    dict_to_struct,
    encode_transport_payload_bytes,
    is_inline_transport_carrier,
    struct_to_dict,
)
from pycloud_parallel.controlplane.state_time import utc_now
from pycloud_parallel.controlplane import client_transport as client_transport_mod
from pycloud_parallel.execution.service_session import Service
from pycloud_parallel.execution.support import _serialize_data_for_object_ref
from pycloud_parallel.execution.support import _prepare_code_blob
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2

_materialize_downloaded_result = client_transport_mod._materialize_downloaded_result


def _object_upload_source_blob(source):
    if source.is_file:
        return Path(source.file_path).read_bytes()
    return source.blob


def _cleanup_object_upload_source(source) -> None:
    if source.is_file:
        Path(source.file_path).unlink(missing_ok=True)


def test_commit_result_file_retries_transient_permission_error(tmp_path, monkeypatch):
    source = tmp_path / "source.bin"
    source.write_bytes(b"hello")
    object_dir = tmp_path / "objects"
    calls = {"count": 0}
    real_replace = os.replace

    def _flaky_replace(src, dst):
        calls["count"] += 1
        if calls["count"] == 1:
            raise PermissionError(13, "Access is denied")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", _flaky_replace)
    artifact = _commit_result_file(source, object_dir=str(object_dir), fmt="bin", size_bytes=5, materialize_as="path")

    assert artifact.object_id.startswith("sha256:")
    digest = artifact.object_id.replace("sha256:", "", 1)
    stored = object_dir / digest[:2] / f"{digest[2:]}.bin"
    assert stored.exists()
    assert stored.read_bytes() == b"hello"


def test_write_code_meta_retries_transient_permission_error(tmp_path, monkeypatch):
    calls = {"count": 0}
    real_replace = os.replace

    def _flaky_replace(src, dst):
        calls["count"] += 1
        if calls["count"] == 1:
            raise PermissionError(13, "Access is denied")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", _flaky_replace)
    code_version = "a" * 64
    artifact = CodeArtifact(
        code_version=code_version,
        path=str(tmp_path / "pkg"),
        runtime="py3",
        entry_module="demo_service",
        entry_callable="run",
        package_format="dir",
        export_mode="module",
        export_methods=(),
        export_decorator="",
        dependency_policy_mode="safe",
        dependency_allowlist=(),
        dependency_path="",
        size_bytes=12,
        created_at=utc_now(),
    )

    _write_code_meta(tmp_path, artifact)

    meta = _load_code_meta(tmp_path, code_version=code_version)
    assert meta["code_version"] == code_version
    assert meta["entry_module"] == "demo_service"
    assert calls["count"] >= 2


def test_nodecontrol_default_artifact_dir_isolated_by_bind_port(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.server import build_nodecontrol_server

    monkeypatch.chdir(tmp_path)
    server_a, state_a = build_nodecontrol_server(
        "127.0.0.1:50061",
        node_id="node-same",
        service_http_bind="127.0.0.1:0",
    )
    server_b, state_b = build_nodecontrol_server(
        "127.0.0.1:50062",
        node_id="node-same",
        service_http_bind="127.0.0.1:0",
    )
    try:
        assert state_a.artifact_dir != state_b.artifact_dir
        assert state_a.artifact_dir.name == "node-same-50061"
        assert state_b.artifact_dir.name == "node-same-50062"
    finally:
        state_a.close()
        state_b.close()
        server_a.stop()
        server_b.stop()


def test_nodecontrol_default_executor_backend_is_subprocess_host(tmp_path):
    state = NodeControlState(
        node_id="node-default-backend",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_default_backend"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        assert state.executor_backend == "subprocess_host"
        assert state._executor_host is not None  # noqa: SLF001
        assert state._executor_host.backend_name == "subprocess_host"  # noqa: SLF001
    finally:
        state.close()


def test_failed_create_service_is_reported_for_ops(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-service-create-fail",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_service_fail"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        monkeypatch.setattr(
            state,
            "_ensure_artifact_ready",
            lambda *args, **kwargs: (_ for _ in ()).throw(ModuleNotFoundError("missing_pkg")),
        )
        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()

        with pytest.raises(ModuleNotFoundError):
            state.create_service(
                owner_client_id="owner-fail",
                service_name="svc-fail",
                sha256=f"sha256:{digest}",
                runtime="py3",
                entry_module="svc_fail",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
                chunks=[blob],
            )

        reports = state.service_report_payloads(include_stopped=True)
        assert len(reports) == 1
        assert reports[0]["service_name"] == "svc-fail"
        assert reports[0]["status"] == pb2.SERVICE_STATUS_STOPPED
        assert "missing_pkg" in reports[0]["stop_reason"]
    finally:
        state.close()


def test_failed_create_task_pool_is_reported_for_ops(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-pool-create-fail",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_pool_fail"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        monkeypatch.setattr(
            state,
            "_ensure_artifact_ready",
            lambda *args, **kwargs: (_ for _ in ()).throw(ModuleNotFoundError("missing_pkg")),
        )
        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()

        with pytest.raises(ModuleNotFoundError):
            state.create_task_pool(
                owner_client_id="owner-fail",
                pool_name="pool-fail",
                sha256=f"sha256:{digest}",
                runtime="py3",
                entry_module="pool_fail",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                chunks=[blob],
            )

        reports = state.task_pool_reports()
        assert len(reports) == 1
        report = next(iter(reports.values()))
        assert report.pool_name == "pool-fail"
        assert report.status == "STOPPED"
        assert "missing_pkg" in report.failure_reason
    finally:
        state.close()


def test_task_pool_artifact_validation_runs_in_executor_host_not_node(tmp_path):
    module_name = "node_should_not_import_entry_module_demo"
    sys.modules.pop(module_name, None)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        source = b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n"
        info = tarfile.TarInfo(f"{module_name}.py")
        info.size = len(source)
        tf.addfile(info, io.BytesIO(source))
    blob = buf.getvalue()
    state = NodeControlState(
        node_id="node-no-import-validation",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_no_node_import"),
        enable_internal_executor=True,
        enable_service_session=False,
    )
    try:
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-no-node-import",
            pool_name="pool-no-node-import",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module=module_name,
            entry_callable="run",
            package_format="tar.gz",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )

        assert pool.task_method == "run"
        assert module_name not in sys.modules
    finally:
        state.close()
        sys.modules.pop(module_name, None)


def test_task_pool_artifact_prepare_is_cached_between_put_and_create(tmp_path):
    state = NodeControlState(
        node_id="node-prepare-cache",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_prepare_cache"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    prepare_calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def prepare_artifact(self, **kwargs):
            prepare_calls.append(kwargs)
            return {"ok": True, "methods": {"run": ("run", "")}}

        def create_task_pool(self, **_kwargs):
            pass

        def preload_pool(self, **_kwargs):
            return 1

        def stop_task_pool(self, **_kwargs):
            pass

        def drain_events(self):
            return []

        def close(self, **_kwargs):
            pass

    try:
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n"
        digest = hashlib.sha256(blob).hexdigest()
        state.create_task_pool(
            owner_client_id="owner-prepare-cache",
            pool_name="pool-prepare-cache",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_prepare_cache",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )

        assert len(prepare_calls) == 1
    finally:
        state.close()


def test_normalize_user_return_inlines_dataframe_when_limit_allows(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", str(8 * 1024 * 1024))
    config_mod.reload_config()
    try:
        frame = pd.DataFrame([{"x": 1}, {"x": 2}])
        status, result, err_type, err_message = _normalize_user_return(frame, object_dir=str(tmp_path))
    finally:
        monkeypatch.delenv("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", raising=False)
        config_mod.reload_config()

    assert status == "SUCCEEDED"
    assert err_type == ""
    assert err_message == ""
    assert not isinstance(result, dict) or "__pycloud_data_ref__" not in result
    restored = struct_to_dict(dict_to_struct({"frame": result}))
    pd.testing.assert_frame_equal(restored["frame"], frame)


def test_normalize_user_return_spills_dataframe_when_limit_too_small(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    import pycloud_parallel.controlplane.node.results as results_mod

    def _raise_inline_limit(*args, **kwargs):
        raise ValueError("inline result too large")

    monkeypatch.setattr(results_mod, "serialize_inline_result", _raise_inline_limit)
    frame = pd.DataFrame([{"x": idx, "y": "a" * 50} for idx in range(10)])
    status, result, _err_type, _err_message = _normalize_user_return(frame, object_dir=str(tmp_path))

    assert status == "SUCCEEDED"
    assert isinstance(result, StoredResultArtifact)


def test_normalize_user_return_large_dataframe_skips_inline_trial(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    import pycloud_parallel.controlplane.node.results as results_mod
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES", "128")
    monkeypatch.setenv("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", str(8 * 1024 * 1024))
    config_mod.reload_config()

    def _unexpected_inline_trial(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("large dataframe result should objectify without inline trial")

    monkeypatch.setattr(results_mod, "serialize_inline_result", _unexpected_inline_trial)
    frame = pd.DataFrame([{"x": idx, "y": "a" * 256} for idx in range(20)])
    try:
        status, result, _err_type, _err_message = _normalize_user_return(frame, object_dir=str(tmp_path))
    finally:
        monkeypatch.delenv("PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", raising=False)
        config_mod.reload_config()

    assert status == "SUCCEEDED"
    assert isinstance(result, StoredResultArtifact)


def test_normalize_user_return_large_ndarray_skips_inline_trial(tmp_path, monkeypatch):
    np = pytest.importorskip("numpy")
    import pycloud_parallel.controlplane.node.results as results_mod
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES", "128")
    monkeypatch.setenv("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", str(8 * 1024 * 1024))
    config_mod.reload_config()

    def _unexpected_inline_trial(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("large ndarray result should objectify without inline trial")

    monkeypatch.setattr(results_mod, "serialize_inline_result", _unexpected_inline_trial)
    array = np.arange(1024, dtype=np.int64)
    try:
        status, result, _err_type, _err_message = _normalize_user_return(array, object_dir=str(tmp_path))
    finally:
        monkeypatch.delenv("PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", raising=False)
        config_mod.reload_config()

    assert status == "SUCCEEDED"
    assert isinstance(result, StoredResultArtifact)


def test_normalize_user_return_large_json_skips_inline_trial(tmp_path, monkeypatch):
    import pycloud_parallel.controlplane.node.results as results_mod
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES", "128")
    monkeypatch.setenv("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", str(8 * 1024 * 1024))
    config_mod.reload_config()

    def _unexpected_inline_trial(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("large dict/list result should objectify without inline trial")

    monkeypatch.setattr(results_mod, "serialize_inline_result", _unexpected_inline_trial)
    value = {"rows": [{"x": idx, "text": "a" * 64} for idx in range(20)]}
    try:
        status, result, _err_type, _err_message = _normalize_user_return(value, object_dir=str(tmp_path))
    finally:
        monkeypatch.delenv("PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", raising=False)
        config_mod.reload_config()

    assert status == "SUCCEEDED"
    assert isinstance(result, StoredResultArtifact)
    assert result.format == "json"
    assert result.materialize_as == "json"


def test_normalize_user_return_pickle_struct_lane_spills_by_struct_limit(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    import pycloud_parallel.controlplane.node.results as results_mod

    def _raise_inline_limit(*args, **kwargs):
        raise ValueError("inline result too large")

    def _unexpected_transport_encode(*args, **kwargs):
        raise AssertionError("transport result adapter should not be used when use_transport_result=False")

    monkeypatch.setattr(results_mod, "serialize_inline_result", _raise_inline_limit)
    monkeypatch.setattr(results_mod, "encode_transport_payload_bytes", _unexpected_transport_encode)
    frame = pd.DataFrame([{"x": idx, "y": "a" * 50} for idx in range(10)])

    status, result, _err_type, _err_message = _normalize_user_return(
        frame,
        object_dir=str(tmp_path),
        serialization_mode="pickle_stable_v1",
        use_transport_result=False,
    )

    assert status == "SUCCEEDED"
    assert isinstance(result, StoredResultArtifact)


def test_normalize_user_return_large_plain_value_raises_instead_of_silent_none(tmp_path, monkeypatch):
    import pycloud_parallel.controlplane.node.results as results_mod

    monkeypatch.setattr(
        results_mod,
        "serialize_inline_result",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("inline result too large")),
    )

    with pytest.raises(LargeResultError, match="exceeds inline limit"):
        _normalize_user_return(
            {"value": "x" * 1024},
            object_dir=str(tmp_path),
            serialization_mode="legacy_v1",
        )


def test_normalize_user_return_rejects_result_object_over_hard_limit(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane import config as config_mod

    result_path = tmp_path / "large-result.bin"
    result_path.write_bytes(b"x" * 16)
    monkeypatch.setenv("PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES", "8")
    config_mod.reload_config()
    try:
        with pytest.raises(ValueError) as exc_info:
            _normalize_user_return(result_path, object_dir=str(tmp_path / "objects"))
    finally:
        monkeypatch.delenv("PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES", raising=False)
        config_mod.reload_config()

    message = str(exc_info.value)
    assert "result object exceeds object size hard limit" in message
    assert "size_bytes=16" in message
    assert "limit_bytes=8" in message


def test_normalize_user_return_path_objectify_does_not_read_whole_file(tmp_path, monkeypatch):
    from pycloud_parallel.data.ref import object_storage_path

    source = tmp_path / "path-result.bin"
    source.write_bytes(b"x" * 4096)
    original_read_bytes = Path.read_bytes

    def _guard_read_bytes(self):  # noqa: ANN001
        if self == source:
            raise AssertionError("path result objectify must not read the whole source file")
        return original_read_bytes(self)

    monkeypatch.setattr(Path, "read_bytes", _guard_read_bytes)

    status, result, _err_type, _err_message = _normalize_user_return(source, object_dir=str(tmp_path / "objects"))

    assert status == "SUCCEEDED"
    assert isinstance(result, StoredResultArtifact)
    assert result.storage_backend == "file"
    stored = object_storage_path(tmp_path / "objects", object_id=result.object_id, fmt=result.format)
    assert stored.exists()
    with stored.open("rb") as fp:
        assert fp.read() == b"x" * 4096


def test_normalize_user_return_file_backed_dataframe_series_ndarray_roundtrip(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")
    pytest.importorskip("pyarrow")
    from pycloud_parallel.controlplane import config as config_mod
    from pycloud_parallel.data.ref import object_storage_path

    frame = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    series = pd.Series([1.0, 2.0], name="nav")
    array = np.arange(8, dtype=np.int64)

    monkeypatch.setenv("PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES", "1")
    config_mod.reload_config()
    try:
        for value, materialize_as in (
            (frame, "dataframe"),
            (series, "series"),
            (array, "ndarray"),
        ):
            object_dir = tmp_path / materialize_as
            status, result, _err_type, _err_message = _normalize_user_return(value, object_dir=str(object_dir))
            assert status == "SUCCEEDED"
            assert isinstance(result, StoredResultArtifact)
            assert result.storage_backend == "file"
            stored = object_storage_path(object_dir, object_id=result.object_id, fmt=result.format)
            restored = _materialize_object_bytes(
                blob=stored.read_bytes(),
                fmt=result.format,
                materialize_as=result.materialize_as,
            )
            if materialize_as == "dataframe":
                pd.testing.assert_frame_equal(restored, value)
            elif materialize_as == "series":
                pd.testing.assert_series_equal(restored, value)
            else:
                np.testing.assert_array_equal(restored, value)
    finally:
        monkeypatch.delenv("PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES", raising=False)
        config_mod.reload_config()


def test_nested_arrow_payload_roundtrip():
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")

    payload = {
        "bundle": {
            "df": pd.DataFrame([{"x": 1}, {"x": 2}]),
            "series": pd.Series([10, 20], name="s"),
            "arr": np.array([3, 4, 5], dtype=np.int64),
        },
        "plain": [1, True, None],
    }

    restored = struct_to_dict(dict_to_struct(payload))

    assert list(restored["bundle"]["df"]["x"]) == [1, 2]
    assert restored["bundle"]["series"].name == "s"
    assert restored["bundle"]["series"].tolist() == [10, 20]
    assert restored["bundle"]["arr"].tolist() == [3, 4, 5]
    assert restored["plain"] == [1, True, None]


def test_code_version_distinguishes_dependency_policy_mode():
    digest = hashlib.sha256(b"print('hello')\n").hexdigest()

    prebuilt = _code_version_from_digest(
        digest,
        runtime="py3",
        entry_module="demo_mod",
        entry_callable="run",
        package_format="py",
        export_mode="single",
        export_methods=("run",),
        export_decorator="pycloud_export",
        dependency_policy_mode="prebuilt",
        dependency_allowlist=(),
    )
    node_preinstalled = _code_version_from_digest(
        digest,
        runtime="py3",
        entry_module="demo_mod",
        entry_callable="run",
        package_format="py",
        export_mode="single",
        export_methods=("run",),
        export_decorator="pycloud_export",
        dependency_policy_mode="node_preinstalled",
        dependency_allowlist=(),
    )
    allow_install = _code_version_from_digest(
        digest,
        runtime="py3",
        entry_module="demo_mod",
        entry_callable="run",
        package_format="py",
        export_mode="single",
        export_methods=("run",),
        export_decorator="pycloud_export",
        dependency_policy_mode="allow_install",
        dependency_allowlist=("orjson==3.10.18",),
    )

    assert prebuilt != node_preinstalled
    assert prebuilt != allow_install
    assert node_preinstalled != allow_install


def test_describe_artifact_error_mentions_dependency_policy():
    exc = ModuleNotFoundError("No module named 'missing_demo_dep'")
    exc.name = "missing_demo_dep"

    prebuilt_message = _describe_artifact_error(
        exc,
        entry_module="demo_mod",
        entry_callable="run",
        package_format="py",
        dependency_policy_mode="prebuilt",
    )
    node_preinstalled_message = _describe_artifact_error(
        exc,
        entry_module="demo_mod",
        entry_callable="run",
        package_format="py",
        dependency_policy_mode="node_preinstalled",
    )
    allow_install_message = _describe_artifact_error(
        exc,
        entry_module="demo_mod",
        entry_callable="run",
        package_format="py",
        dependency_policy_mode="allow_install",
        install_failed=True,
    )

    assert "artifact dependency policy is `prebuilt`" in prebuilt_message
    assert "ArtifactDeps.allow_install" in prebuilt_message
    assert "artifact dependency policy is `node_preinstalled`" in node_preinstalled_message
    assert "Preinstall it on the node" in node_preinstalled_message
    assert "artifact dependency policy is `allow_install`" in allow_install_message
    assert "dependency install failed" in allow_install_message


def test_dataframe_round_trips_int_columns_and_multiindex():
    pd = pytest.importorskip("pandas")

    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "stock"),
            (pd.Timestamp("2024-01-03"), "bond"),
        ],
        names=["trade_date", "asset_type"],
    )
    columns = pd.Index([10006, 10007], name="fund_id")
    frame = pd.DataFrame([[0.1, 0.2], [0.3, 0.4]], index=index, columns=columns)

    restored = struct_to_dict(dict_to_struct({"frame": frame}))

    pd.testing.assert_frame_equal(restored["frame"], frame)


def test_dict_to_struct_rejects_unsupported_object_with_clear_path():
    class DemoObject:
        pass

    with pytest.raises(TypeError, match=r"payload\.bundle\.bad has unsupported type DemoObject"):
        dict_to_struct({"bundle": {"bad": DemoObject()}})


def test_dict_to_struct_rejects_complex_ndarray_dtype():
    np = pytest.importorskip("numpy")

    with pytest.raises(TypeError, match=r"payload\.arr uses numpy\.ndarray dtype object"):
        dict_to_struct({"arr": np.array([{"x": 1}], dtype=object)})


def test_dict_to_struct_stringifies_scalar_dict_keys():
    restored = struct_to_dict(
        dict_to_struct(
            {
                "payload": [
                    {
                        10006: {"value": 1},
                        True: "flag",
                    }
                ]
            }
        )
    )

    assert restored["payload"][0]["10006"]["value"] == 1
    assert restored["payload"][0]["True"] == "flag"


def test_dict_to_struct_rejects_colliding_normalized_dict_keys():
    with pytest.raises(TypeError, match=r"normalize to '1'"):
        dict_to_struct({"payload": {1: "int-key", "1": "string-key"}})


def test_purge_loaded_artifact_modules_tolerates_broken_namespace_paths(tmp_path, monkeypatch):
    class _BrokenPaths:
        def __iter__(self):
            raise KeyError("broken_pkg")

        def __len__(self):
            raise KeyError("broken_pkg")

    broken_parent = types.ModuleType("broken_pkg")
    broken_child = types.ModuleType("broken_pkg.child")
    broken_child.__path__ = _BrokenPaths()  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "broken_pkg", broken_parent)
    monkeypatch.setitem(sys.modules, "broken_pkg.child", broken_child)

    artifact_path = tmp_path / "artifact.tar.gz"
    artifact_path.write_bytes(b"")

    _purge_loaded_artifact_modules(
        str(artifact_path),
        entry_module="demo_artifact",
        package_format="tar.gz",
    )

    assert "broken_pkg.child" not in sys.modules


def test_purge_loaded_artifact_modules_removes_temp_extracted_local_packages(tmp_path, monkeypatch):
    package_dir = tmp_path / "calc_asset_ratio"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("from . import calc_asset_ratio\n", encoding="utf-8")
    (package_dir / "calc_asset_ratio.py").write_text(
        "def get_fund_asset_ratio(value=0, **_kwargs):\n"
        "    return {'value': value}\n",
        encoding="utf-8",
    )
    (tmp_path / "calc_asset_ratio_job_module.py").write_text(
        "from calc_asset_ratio import calc_asset_ratio\n\n"
        "def task_generator(**_kwargs):\n"
        "    return [{'value': 1}]\n\n"
        "def run(value=0, **_kwargs):\n"
        "    return calc_asset_ratio.get_fund_asset_ratio(value=value)\n",
        encoding="utf-8",
    )

    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    for name in ("calc_asset_ratio", "calc_asset_ratio.calc_asset_ratio", "calc_asset_ratio_job_module"):
        sys.modules.pop(name, None)
    module = importlib.import_module("calc_asset_ratio_job_module")
    blob, _filename = _prepare_code_blob(module=module)
    artifact_path = tmp_path / "artifact.tar.gz"
    artifact_path.write_bytes(blob or b"")
    for name in ("calc_asset_ratio", "calc_asset_ratio.calc_asset_ratio", "calc_asset_ratio_job_module"):
        sys.modules.pop(name, None)

    loaded = _load_user_module(
        str(artifact_path),
        entry_module="calc_asset_ratio_job_module",
        package_format="tar.gz",
        dependency_path="",
    )
    extracted_dir = str(getattr(loaded, "__pycloud_temp_extract_dir__", "") or "").strip()

    assert extracted_dir
    assert "calc_asset_ratio" in sys.modules
    assert "calc_asset_ratio.calc_asset_ratio" in sys.modules

    _purge_loaded_artifact_modules(
        str(artifact_path),
        entry_module="calc_asset_ratio_job_module",
        package_format="tar.gz",
        extra_prefixes=[extracted_dir],
    )

    assert "calc_asset_ratio" not in sys.modules
    assert "calc_asset_ratio.calc_asset_ratio" not in sys.modules


def test_execute_payload_tar_gz_keeps_artifact_root_on_sys_path_for_late_imports(tmp_path):
    package_dir = tmp_path / "src"
    service_dir = package_dir / "ServicePkg"
    service_dir.mkdir(parents=True)
    (service_dir / "__init__.py").write_text("", encoding="utf-8")
    (service_dir / "worker.py").write_text(
        "def run(value=0, **_kwargs):\n"
        "    import RootConfig\n"
        "    return {'value': value + RootConfig.OFFSET}\n",
        encoding="utf-8",
    )
    (package_dir / "RootConfig.py").write_text("OFFSET = 7\n", encoding="utf-8")
    artifact_path = tmp_path / "artifact.tar.gz"
    with tarfile.open(artifact_path, "w:gz") as tar:
        tar.add(service_dir / "__init__.py", arcname="ServicePkg/__init__.py")
        tar.add(service_dir / "worker.py", arcname="ServicePkg/worker.py")
        tar.add(package_dir / "RootConfig.py", arcname="RootConfig.py")

    status, result, err_type, err_message, _timings = _execute_payload_in_subprocess(
        str(artifact_path),
        "ServicePkg.worker",
        "tar.gz",
        "",
        "prebuilt",
        str(tmp_path / "objects"),
        str(tmp_path / "work"),
        str(tmp_path / "globals"),
        "",
        "all",
        (),
        "pycloud_export",
        "run",
        "run",
        {"value": 5},
        payload_mode="http_call",
    )

    assert status == "SUCCEEDED", (err_type, err_message)
    assert result == {"value": 12}


def test_execute_payload_directory_keeps_artifact_root_on_sys_path_for_late_imports(tmp_path):
    artifact_dir = tmp_path / "pkg"
    service_dir = artifact_dir / "ServicePkg"
    service_dir.mkdir(parents=True)
    (service_dir / "__init__.py").write_text("", encoding="utf-8")
    (service_dir / "worker.py").write_text(
        "def run(value=0, **_kwargs):\n"
        "    import RootConfig\n"
        "    return {'value': value + RootConfig.OFFSET}\n",
        encoding="utf-8",
    )
    (artifact_dir / "RootConfig.py").write_text("OFFSET = 11\n", encoding="utf-8")

    status, result, err_type, err_message, _timings = _execute_payload_in_subprocess(
        str(artifact_dir),
        "ServicePkg.worker",
        "tar.gz",
        "",
        "prebuilt",
        str(tmp_path / "objects"),
        str(tmp_path / "work"),
        str(tmp_path / "globals"),
        "",
        "all",
        (),
        "pycloud_export",
        "run",
        "run",
        {"value": 5},
        payload_mode="http_call",
    )

    assert status == "SUCCEEDED", (err_type, err_message)
    assert result == {"value": 16}


def test_dict_to_struct_round_trips_temporal_scalars_and_series_index():
    pd = pytest.importorskip("pandas")

    ts = pd.Timestamp("2024-01-02T03:04:05+08:00")
    payload = {
        "when": datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        "series": pd.Series([10, 20], index=[ts, ts + pd.Timedelta(days=1)], name="nav"),
    }

    restored = struct_to_dict(dict_to_struct(payload))

    assert restored["when"] == payload["when"]
    assert restored["series"].name == "nav"
    assert list(restored["series"]) == [10, 20]
    assert restored["series"].index[0] == ts
    assert restored["series"].index[1] == ts + pd.Timedelta(days=1)


def test_dataframe_object_upload_parquet_preserves_index_and_int_columns():
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    from pycloud_parallel.data.ref import DataRef

    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
        names=["trade_date", "bucket"],
    )
    frame = pd.DataFrame([[1, 2], [3, 4]], index=index, columns=[10006, 10007])

    source = _serialize_data_for_object_ref(frame, format="parquet")
    kind, fmt, blob = source.materialize_as, source.format, _object_upload_source_blob(source)
    import tempfile

    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-dfbundle-", suffix=".dfbundle")
    import os
    os.close(fd)
    Path(tmp_name).write_bytes(blob)
    try:
        import io
        import zipfile

        with zipfile.ZipFile(Path(tmp_name)) as zf:
            with zf.open("data.parquet") as fh:
                stored_frame = pd.read_parquet(io.BytesIO(fh.read()))
        assert list(stored_frame.columns) == ["c0", "c1"]

        restored = _materialize_downloaded_result(
            Path(tmp_name),
            result_ref=DataRef(
                ref_id="sha256:" + "b" * 64,
                storage_id="sha256:" + "b" * 64,
                logical_type="dataframe",
                node_id="node-1",
                locator_kind="node_control",
                locator_token="",
                format=fmt,
                size_bytes=len(blob),
                materialize_as="dataframe",
            ),
        )

        assert kind == "dataframe"
        assert fmt == "dfbundle"
        pd.testing.assert_frame_equal(restored, frame)
    finally:
        Path(tmp_name).unlink(missing_ok=True)
        _cleanup_object_upload_source(source)


def test_series_object_upload_preserves_index_and_name():
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    from pycloud_parallel.data.ref import DataRef

    series = pd.Series(
        [1.1, 2.2],
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
            names=["trade_date", "bucket"],
        ),
        name=10006,
    )

    source = _serialize_data_for_object_ref(series)
    kind, fmt, blob = source.materialize_as, source.format, _object_upload_source_blob(source)

    import os
    import tempfile

    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-seriesbundle-", suffix=".seriesbundle")
    os.close(fd)
    Path(tmp_name).write_bytes(blob)
    try:
        restored = _materialize_downloaded_result(
            Path(tmp_name),
            result_ref=DataRef(
                ref_id="sha256:" + "c" * 64,
                storage_id="sha256:" + "c" * 64,
                logical_type="series",
                node_id="node-1",
                locator_kind="node_control",
                locator_token="",
                format=fmt,
                size_bytes=len(blob),
                materialize_as="series",
            ),
        )

        assert kind == "series"
        assert fmt == "seriesbundle"
        pd.testing.assert_series_equal(restored, series)
    finally:
        Path(tmp_name).unlink(missing_ok=True)
        _cleanup_object_upload_source(source)


def test_ndarray_object_upload_prefers_file_backed_source(tmp_path):
    np = pytest.importorskip("numpy")

    array = np.arange(12, dtype=np.int64).reshape(3, 4)
    source = _serialize_data_for_object_ref(array, format="npy")

    assert source.is_file is True
    assert source.format == "npy"
    assert source.materialize_as == "ndarray"

    path = Path(source.file_path)
    assert path.exists() is True
    try:
        restored = np.load(path, allow_pickle=False)
        np.testing.assert_array_equal(restored, array)
    finally:
        path.unlink(missing_ok=True)


def test_json_object_upload_roundtrip_supports_nested_series():
    pd = pytest.importorskip("pandas")

    from pycloud_parallel.controlplane.serialization import convert_dict_to_arrow

    payload = {
        "series": pd.Series([1.0, 2.0], index=[pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")], name="nav"),
        "value": 3,
    }
    kind, fmt, blob = _serialize_data_for_object_ref(payload, format="json")

    assert kind == "json"
    assert fmt == "json"
    restored = convert_dict_to_arrow(json.loads(blob.decode("utf-8")))
    assert restored["value"] == 3
    assert isinstance(restored["series"], pd.Series)
    assert restored["series"].name == "nav"
    assert list(restored["series"]) == [1.0, 2.0]


def test_object_ref_resolution_restores_dataframe_bundle_on_node(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    from pycloud_parallel.data.ref import DataRef, object_storage_path

    frame = pd.DataFrame(
        [[1, 2], [3, 4]],
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
            names=["trade_date", "bucket"],
        ),
        columns=[10006, 10007],
    )
    source = _serialize_data_for_object_ref(frame)
    _kind, fmt, blob = source.materialize_as, source.format, _object_upload_source_blob(source)
    object_id = "sha256:" + "d" * 64
    path = object_storage_path(tmp_path, object_id=object_id, fmt=fmt)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)
    _cleanup_object_upload_source(source)

    payload = {
        "frame": DataRef(
            ref_id=object_id,
            storage_id=object_id,
            logical_type="dataframe",
            format=fmt,
            size_bytes=len(blob),
            materialize_as="dataframe",
            locator_kind="node_local",
            locator_token="",
        )
    }

    restored = _resolve_object_refs_in_payload(payload, object_dir=str(tmp_path))
    pd.testing.assert_frame_equal(restored["frame"], frame)


def test_data_ref_resolution_restores_dataframe_bundle_on_node(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.data.ref import object_storage_path

    frame = pd.DataFrame(
        [[1, 2], [3, 4]],
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
            names=["trade_date", "bucket"],
        ),
        columns=[10006, 10007],
    )
    source = _serialize_data_for_object_ref(frame)
    _kind, fmt, blob = source.materialize_as, source.format, _object_upload_source_blob(source)
    object_id = "sha256:" + "e" * 64
    path = object_storage_path(tmp_path, object_id=object_id, fmt=fmt)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)
    _cleanup_object_upload_source(source)

    payload = {
        "frame": DataRef(
            ref_id=object_id,
            storage_id=object_id,
            logical_type="dataframe",
            format=fmt,
            size_bytes=len(blob),
            materialize_as="auto",
        )
    }

    restored = _resolve_object_refs_in_payload(payload, object_dir=str(tmp_path))
    pd.testing.assert_frame_equal(restored["frame"], frame)


def test_data_ref_resolution_local_only_does_not_remote_fetch(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.node_control_client as node_control_client_mod
    from pycloud_parallel.data.ref import DataRef

    monkeypatch.setenv("PYCLOUD_DATAREF_RESOLUTION", "local_only")
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    class FakeClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("remote fetch should not run in local_only mode")

    monkeypatch.setattr(node_control_client_mod, "NodeControlClient", FakeClient)
    blob = b"remote payload"
    object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
    ref = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(blob),
        materialize_as="bytes",
        locator_kind="node_control",
        locator_token="127.0.0.1:50061",
    )

    with pytest.raises(ObjectResolutionError, match="object not found on node"):
        _resolve_single_data_ref(ref, object_dir=str(tmp_path))


def test_data_ref_resolution_defaults_to_remote_fetch_and_caches(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.node_control_client as node_control_client_mod
    from pycloud_parallel.data.ref import DataRef, object_storage_path

    monkeypatch.delenv("PYCLOUD_DATAREF_RESOLUTION", raising=False)
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    blob = b"remote payload"
    object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
    calls = []

    class FakeClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def download_object_bytes(self, *, object_id):
            raise AssertionError("remote fetch must use download_object_to_file")

        def download_object_to_file(self, *, object_id, target_path):
            calls.append((self.target, object_id))
            Path(target_path).write_bytes(blob)
            return Path(target_path)

    monkeypatch.setattr(node_control_client_mod, "NodeControlClient", FakeClient)
    ref = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(blob),
        materialize_as="bytes",
        locator_kind="node_control",
        locator_token="127.0.0.1:50061",
    )

    assert _resolve_single_data_ref(ref, object_dir=str(tmp_path)) == blob
    assert _resolve_single_data_ref(ref, object_dir=str(tmp_path)) == blob
    assert calls == [("127.0.0.1:50061", object_id)]
    assert object_storage_path(tmp_path, object_id=object_id, fmt="bin").read_bytes() == blob


def test_data_ref_resolution_throttles_object_last_at_touch(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.node.object_meta import (
        _OBJECT_LAST_AT_TOUCH_TIMES,
        _write_object_meta,
    )
    from pycloud_parallel.data.ref import DataRef, object_storage_path

    blob = b"cached payload"
    object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
    object_path = object_storage_path(tmp_path, object_id=object_id, fmt="bin")
    object_path.parent.mkdir(parents=True, exist_ok=True)
    object_path.write_bytes(blob)
    _write_object_meta(
        tmp_path,
        object_id=object_id,
        fmt="bin",
        size_bytes=len(blob),
        created_at=utc_now(),
        storage_backend="file",
    )
    ref = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(blob),
        materialize_as="bytes",
    )
    touch_calls = []
    real_replace = os.replace

    def _count_meta_replace(source, target):
        if str(target).endswith(".json"):
            touch_calls.append((source, target))
        return real_replace(source, target)

    _OBJECT_LAST_AT_TOUCH_TIMES.clear()
    monkeypatch.setattr(os, "replace", _count_meta_replace)

    assert _resolve_single_data_ref(ref, object_dir=str(tmp_path)) == blob
    assert _resolve_single_data_ref(ref, object_dir=str(tmp_path)) == blob
    assert len(touch_calls) == 1


def test_data_ref_resolution_rejects_large_bytes_materialize_after_file_fetch(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.node_control_client as node_control_client_mod
    from pycloud_parallel.data.ref import DataRef

    monkeypatch.delenv("PYCLOUD_DATAREF_RESOLUTION", raising=False)
    monkeypatch.setenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", "8")
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    blob = b"x" * 64
    object_id = "sha256:" + hashlib.sha256(blob).hexdigest()

    class FakeClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def download_object_bytes(self, *, object_id):
            raise AssertionError("remote fetch must not use in-memory bytes download")

        def download_object_to_file(self, *, object_id, target_path):
            Path(target_path).write_bytes(blob)
            return Path(target_path)

    monkeypatch.setattr(node_control_client_mod, "NodeControlClient", FakeClient)
    ref = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(blob),
        materialize_as="bytes",
        locator_kind="node_control",
        locator_token="127.0.0.1:50061",
    )

    with pytest.raises(ValueError) as exc_info:
        _resolve_single_data_ref(ref, object_dir=str(tmp_path))

    assert "too large for in-memory bytes materialize" in str(exc_info.value)


def test_data_ref_resolution_remote_fetch_resolves_controlplane_locator(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.data_registry as data_registry_mod
    import pycloud_parallel.controlplane.node_control_client as node_control_client_mod
    from pycloud_parallel.data.ref import DataRef

    monkeypatch.setenv("PYCLOUD_DATAREF_RESOLUTION", "remote_fetch")
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    blob = b"registry routed payload"
    object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
    calls = []

    class FakeRegistryClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def resolve(self, ref):
            assert self.target == "infocenter:50051"
            return data_registry_mod.ResolvedDataRef(
                ref=ref,
                control_addr="10.0.0.2:50061",
                locator_kind="node_control",
                locator_token="10.0.0.2:50061",
                via_registry=True,
                replicas=({"control_addr": "10.0.0.2:50061"},),
            )

    class FakeClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def download_object_bytes(self, *, object_id):
            raise AssertionError("remote fetch must use download_object_to_file")

        def download_object_to_file(self, *, object_id, target_path):
            calls.append((self.target, object_id))
            Path(target_path).write_bytes(blob)
            return Path(target_path)

    monkeypatch.setattr(data_registry_mod, "DataRegistryClient", FakeRegistryClient)
    monkeypatch.setattr(node_control_client_mod, "NodeControlClient", FakeClient)
    ref = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(blob),
        materialize_as="bytes",
        locator_kind="controlplane",
        locator_token="infocenter:50051",
    )

    assert _resolve_single_data_ref(ref, object_dir=str(tmp_path)) == blob
    assert calls == [("10.0.0.2:50061", object_id)]


def test_data_ref_resolution_remote_fetch_rejects_checksum_mismatch(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.node_control_client as node_control_client_mod
    from pycloud_parallel.data.ref import DataRef, object_storage_path

    monkeypatch.setenv("PYCLOUD_DATAREF_RESOLUTION", "remote_fetch")
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    expected_blob = b"expected payload"
    wrong_blob = b"wrong payload"
    object_id = "sha256:" + hashlib.sha256(expected_blob).hexdigest()

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def download_object_bytes(self, *, object_id):
            raise AssertionError("remote fetch must use download_object_to_file")

        def download_object_to_file(self, *, object_id, target_path):
            Path(target_path).write_bytes(wrong_blob)
            return Path(target_path)

    monkeypatch.setattr(node_control_client_mod, "NodeControlClient", FakeClient)
    ref = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(expected_blob),
        materialize_as="bytes",
        locator_kind="node_control",
        locator_token="127.0.0.1:50061",
    )

    with pytest.raises(ObjectResolutionError, match="checksum mismatch"):
        _resolve_single_data_ref(ref, object_dir=str(tmp_path))
    assert not object_storage_path(tmp_path, object_id=object_id, fmt="bin").exists()


def test_data_ref_resolution_remote_fetch_error_names_object_and_target(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.node_control_client as node_control_client_mod
    from pycloud_parallel.data.ref import DataRef

    monkeypatch.delenv("PYCLOUD_DATAREF_RESOLUTION", raising=False)
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    blob = b"missing remote payload"
    object_id = "sha256:" + hashlib.sha256(blob).hexdigest()

    class FakeClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def download_object_bytes(self, *, object_id):
            raise AssertionError("remote fetch must use download_object_to_file")

        def download_object_to_file(self, *, object_id, target_path):
            raise FileNotFoundError(f"object missing: {object_id}")

    monkeypatch.setattr(node_control_client_mod, "NodeControlClient", FakeClient)
    ref = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(blob),
        materialize_as="bytes",
        locator_kind="node_control",
        locator_token="127.0.0.1:50061",
    )

    with pytest.raises(ObjectResolutionError) as exc_info:
        _resolve_single_data_ref(ref, object_dir=str(tmp_path))

    message = str(exc_info.value)
    assert "remote fetch failed" in message
    assert object_id in message
    assert "127.0.0.1:50061" in message
    assert "error_type=FileNotFoundError" in message
    assert "object missing" in message


def test_data_ref_resolution_remote_fetch_unreachable_is_distinct(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.node_control_client as node_control_client_mod
    from pycloud_parallel.data.ref import DataRef

    monkeypatch.delenv("PYCLOUD_DATAREF_RESOLUTION", raising=False)
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    blob = b"unreachable remote payload"
    object_id = "sha256:" + hashlib.sha256(blob).hexdigest()

    class FakeClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def __enter__(self):
            raise ConnectionError(f"cannot reach {self.target}")

        def __exit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setattr(node_control_client_mod, "NodeControlClient", FakeClient)
    ref = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(blob),
        materialize_as="bytes",
        locator_kind="node_control",
        locator_token="127.0.0.1:50061",
    )

    with pytest.raises(ObjectResolutionError) as exc_info:
        _resolve_single_data_ref(ref, object_dir=str(tmp_path))

    message = str(exc_info.value)
    assert "remote fetch failed" in message
    assert object_id in message
    assert "127.0.0.1:50061" in message
    assert "error_type=ConnectionError" in message
    assert "cannot reach" in message


def test_data_ref_resolution_registry_failure_is_distinct(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.data_registry as data_registry_mod
    from pycloud_parallel.data.ref import DataRef

    monkeypatch.delenv("PYCLOUD_DATAREF_RESOLUTION", raising=False)
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    blob = b"registry missing payload"
    object_id = "sha256:" + hashlib.sha256(blob).hexdigest()

    class FakeRegistryClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def resolve(self, ref):
            raise LookupError(f"registry miss: {ref.object_id}")

    monkeypatch.setattr(data_registry_mod, "DataRegistryClient", FakeRegistryClient)
    ref = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(blob),
        materialize_as="bytes",
        locator_kind="controlplane",
        locator_token="infocenter:50051",
    )

    with pytest.raises(ObjectResolutionError) as exc_info:
        _resolve_single_data_ref(ref, object_dir=str(tmp_path))

    message = str(exc_info.value)
    assert "data ref registry resolve failed" in message
    assert object_id in message
    assert "infocenter:50051" in message
    assert "registry miss" in message


def test_data_store_builds_result_and_data_refs() -> None:
    from pycloud_parallel.controlplane.data_store import DataStore, StoredDataArtifact

    store = DataStore(
        object_dir="/tmp/objects",
        node_id="node-1",
        node_instance_id="node-1-inst",
        control_addr="127.0.0.1:50061",
    )
    artifact = StoredDataArtifact(
        object_id="sha256:" + ("f" * 64),
        format="dfbundle",
        size_bytes=1234,
        materialize_as="dataframe",
    )

    data_ref = store.data_ref_from_stored_artifact(artifact)
    result_ref = store.result_ref_from_stored_artifact(artifact)

    assert data_ref.object_id == artifact.object_id
    assert data_ref.logical_type == "dataframe"
    assert data_ref.control_addr == "127.0.0.1:50061"
    assert result_ref.object_id == artifact.object_id
    assert result_ref.node_id == "node-1"
    assert result_ref.node_instance_id == "node-1-inst"
    assert result_ref.control_addr == "127.0.0.1:50061"


def test_node_service_stream_rejects_oversized_inline_items(tmp_path, monkeypatch) -> None:
    state = NodeControlState(
        node_id="node-stream-large",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_stream_large"),
        enable_internal_executor=False,
        enable_service_session=False,
    )

    class _TinyResultPolicy:
        inline_result_hard_limit_bytes = 8

    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.get_payload_policy", lambda mode: _TinyResultPolicy())
    try:
        with pytest.raises(ValueError, match="inline result limit"):
            state._encode_checked_stream_item_line({"event": "item", "index": 0, "data": "small"})  # noqa: SLF001
    finally:
        state.close()


def test_node_service_stream_rejects_spilled_result_artifacts(tmp_path) -> None:
    state = NodeControlState(
        node_id="node-stream-spill",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_stream_spill"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    artifact = StoredResultArtifact(
        object_id="sha256:" + ("6" * 64),
        format="bin",
        size_bytes=1024,
        materialize_as="bytes",
    )
    try:
        with pytest.raises(ValueError, match="stream item exceeds inline result limit"):
            state._stream_result_value(artifact)  # noqa: SLF001
    finally:
        state.close()


def test_data_registry_resolves_controlplane_data_ref(monkeypatch) -> None:
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.controlplane.data_registry import resolve_data_ref

    class _FakeInfoCenterClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0, **_kwargs) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def list_nodes(self, *, healthy_only: bool = True, tags=None, limit: int = 100):
            del healthy_only, tags, limit
            return [
                SimpleNamespace(
                    node_id="node-1",
                    node_instance_id="node-1-inst",
                    control_addr="127.0.0.1:50061",
                )
            ]

    monkeypatch.setattr("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient", _FakeInfoCenterClient)

    resolved = resolve_data_ref(
        DataRef(
            ref_id="sha256:" + ("1" * 64),
            storage_id="sha256:" + ("1" * 64),
            format="dfbundle",
            logical_type="dataframe",
            node_id="node-1",
            locator_kind="controlplane",
            locator_token="http://127.0.0.1:50051",
        ),
        timeout_sec=5.0,
    )

    assert resolved.control_addr == "127.0.0.1:50061"
    assert resolved.via_registry is True


def test_data_registry_resolve_skips_unhealthy_instance_replicas(monkeypatch) -> None:
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.controlplane.data_registry import resolve_data_ref

    class _FakeInfoCenterClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0, **_kwargs) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def resolve_data_ref(self, *, ref_id: str):
            del ref_id
            return {
                "entry": {
                    "ref_id": "sha256:" + ("4" * 64),
                    "replicas": [
                        {"control_addr": "127.0.0.1:50061", "node_id": "node-a", "node_instance_id": "node-a-old"},
                        {"control_addr": "127.0.0.1:50062", "node_id": "node-a", "node_instance_id": "node-a-new"},
                    ],
                }
            }

        def list_nodes(self, *, healthy_only: bool = True, tags=None, limit: int = 100):
            del healthy_only, tags, limit
            return [
                SimpleNamespace(node_id="node-a", node_instance_id="node-a-old", control_addr="127.0.0.1:50061", healthy=False),
                SimpleNamespace(node_id="node-a", node_instance_id="node-a-new", control_addr="127.0.0.1:50062", healthy=True),
            ]

    monkeypatch.setattr("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient", _FakeInfoCenterClient)

    resolved = resolve_data_ref(
        DataRef(
            ref_id="sha256:" + ("4" * 64),
            storage_id="sha256:" + ("4" * 64),
            format="dfbundle",
            logical_type="dataframe",
            locator_kind="controlplane",
            locator_token="http://127.0.0.1:50051",
        ),
        timeout_sec=5.0,
    )

    assert resolved.control_addr == "127.0.0.1:50062"
    assert resolved.node_instance_id == "node-a-new"
    assert [item["node_instance_id"] for item in resolved.replicas] == ["node-a-new"]


def test_data_registry_resolve_rejects_only_unhealthy_instance_replica(monkeypatch) -> None:
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.controlplane.data_registry import resolve_data_ref

    class _FakeInfoCenterClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0, **_kwargs) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def resolve_data_ref(self, *, ref_id: str):
            del ref_id
            return {
                "entry": {
                    "ref_id": "sha256:" + ("5" * 64),
                    "control_addr": "127.0.0.1:50061",
                    "node_id": "node-a",
                    "node_instance_id": "node-a-old",
                }
            }

        def list_nodes(self, *, healthy_only: bool = True, tags=None, limit: int = 100):
            del healthy_only, tags, limit
            return [
                SimpleNamespace(node_id="node-a", node_instance_id="node-a-old", control_addr="127.0.0.1:50061", healthy=False)
            ]

    monkeypatch.setattr("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient", _FakeInfoCenterClient)

    with pytest.raises(RuntimeError, match="data ref could not be resolved"):
        resolve_data_ref(
            DataRef(
                ref_id="sha256:" + ("5" * 64),
                storage_id="sha256:" + ("5" * 64),
                format="dfbundle",
                logical_type="dataframe",
                locator_kind="controlplane",
                locator_token="http://127.0.0.1:50051",
                node_instance_id="node-a-old",
            ),
            timeout_sec=5.0,
        )


def test_data_registry_client_roundtrip_via_controlplane_http() -> None:
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.controlplane.data_registry import DataRegistryClient
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
    from pycloud_parallel.controlplane.server import build_controlplane_server

    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    try:
        client = DataRegistryClient(controlplane.base_url, timeout_sec=5.0)
        ref = DataRef(
            ref_id="sha256:" + ("2" * 64),
            storage_id="sha256:" + ("2" * 64),
            logical_type="dataframe",
            format="dfbundle",
            size_bytes=2048,
            materialize_as="auto",
            locator_kind="controlplane",
            locator_token=controlplane.base_url,
            node_id="node-http",
            node_instance_id="node-http-inst",
        )

        registered = client.register(
            ref,
            ttl_sec=120,
            node_id="node-http",
            node_instance_id="node-http-inst",
            control_addr="127.0.0.1:50061",
            locator_kind="node_control",
            locator_token="127.0.0.1:50061",
        )
        assert registered["ok"] is True
        assert registered["entry"]["ref_id"] == ref.ref_id

        resolved_public = InfoCenterClient(controlplane.base_url, timeout_sec=5.0).resolve_data_ref(ref_id=ref.ref_id)
        assert resolved_public["ok"] is True
        assert resolved_public["entry"]["control_addr"] == ""
        assert resolved_public["entry"]["replicas"] == []

        touched = client.touch(ref.ref_id)
        assert touched["ok"] is True
        assert touched["entry"]["ref_id"] == ref.ref_id
        assert touched["entry"]["control_addr"] == ""

        released = client.release(ref.ref_id)
        assert released["ok"] is True

        with pytest.raises(RuntimeError, match="data ref could not be resolved|data ref not found"):
            client.resolve(ref)
    finally:
        controlplane.stop()


def test_data_registry_client_uses_infocenter_token_for_data_endpoints() -> None:
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.controlplane.data_registry import DataRegistryClient
    from pycloud_parallel.controlplane.infocenter_http import InfoCenterHttpServer
    from pycloud_parallel.controlplane.infocenter_state import InfoCenterState

    infocenter = InfoCenterHttpServer(bind="127.0.0.1:0", state=InfoCenterState(), auth_token="registry-secret")
    infocenter.start()
    try:
        ref = DataRef(
            ref_id="sha256:" + ("7" * 64),
            storage_id="sha256:" + ("7" * 64),
            format="bin",
            size_bytes=7,
            materialize_as="bytes",
            locator_kind="controlplane",
            locator_token=infocenter.base_url,
        )
        unauthenticated = DataRegistryClient(infocenter.base_url, timeout_sec=5.0)
        with pytest.raises(RuntimeError, match="unauthorized"):
            unauthenticated.register(ref, control_addr="127.0.0.1:50061")

        client = DataRegistryClient(infocenter.base_url, timeout_sec=5.0, infocenter_token="registry-secret")
        registered = client.register(ref, control_addr="127.0.0.1:50061")
        assert registered["ok"] is True
        assert registered["entry"]["control_addr"] == ""
        assert client.touch(ref.ref_id)["ok"] is True
        assert client.release(ref.ref_id)["ok"] is True
    finally:
        infocenter.stop()


def test_infocenter_rejects_data_ref_registration_from_fenced_instance() -> None:
    from pycloud_parallel.data.ref import DataRef

    state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    state.register_node_record(
        node_instance_id="node-dataref-old",
        node_id="node-dataref",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=16,
    )
    state.mark_node_lost("node-dataref-old", reason="lost")

    ref = DataRef(
        ref_id="sha256:" + ("6" * 64),
        storage_id="sha256:" + ("6" * 64),
        logical_type="bytes",
        format="bin",
        size_bytes=8,
        materialize_as="bytes",
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
        node_id="node-dataref",
        node_instance_id="node-dataref-old",
    )
    with pytest.raises(ValueError, match="node_instance_id fenced"):
        state.register_data_ref_record(
            ref=ref,
            node_id="node-dataref",
            node_instance_id="node-dataref-old",
            control_addr="127.0.0.1:50061",
        )
    with pytest.raises(ValueError, match="node_instance_id fenced"):
        state.register_data_ref_record(
            ref=ref,
            node_id="node-dataref-new",
            node_instance_id="node-dataref-new",
            control_addr="127.0.0.1:50062",
            replicas=[
                {
                    "node_id": "node-dataref",
                    "node_instance_id": "node-dataref-old",
                    "control_addr": "127.0.0.1:50061",
                }
            ],
        )


def test_data_registry_release_triggers_node_release_for_consume_on_read(monkeypatch) -> None:
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.controlplane.data_registry import DataRegistryClient
    from pycloud_parallel.controlplane.server import build_controlplane_server

    released: list[str] = []

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def pin_object(self, *, object_id: str, ref_id: str) -> bool:
            return True

        def release_object_ref(self, *, object_id: str, ref_id: str = "") -> bool:
            released.append(f"{object_id}:{ref_id}")
            return True

    monkeypatch.setattr("pycloud_parallel.controlplane.infocenter_http.NodeControlClient", _FakeNodeControlClient)

    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    try:
        client = DataRegistryClient(controlplane.base_url, timeout_sec=5.0)
        ref = DataRef(
            ref_id="sha256:" + ("3" * 64),
            storage_id="sha256:" + ("3" * 64),
            logical_type="bytes",
            format="bin",
            size_bytes=16,
            materialize_as="bytes",
            locator_kind="controlplane",
            locator_token=controlplane.base_url,
            consume_on_read=True,
            node_id="node-release",
            node_instance_id="node-release-inst",
        )

        registered = client.register(
            ref,
            ttl_sec=60,
            node_id="node-release",
            node_instance_id="node-release-inst",
            control_addr="127.0.0.1:50061",
            locator_kind="node_control",
            locator_token="127.0.0.1:50061",
        )
        assert registered["ok"] is True

        released_payload = client.release(ref.ref_id)
        assert released_payload["ok"] is True
        assert released == [f"{ref.object_id}:{ref.ref_id}"]
    finally:
        controlplane.stop()


def test_code_content_and_variant_dirs_use_digest_and_subversion_key(tmp_path):
    code_version = "sha256:" + ("a" * 64) + "." + ("b" * 16)
    content_dir = _code_content_dir(tmp_path, code_version=code_version)
    variant_dir = _code_variant_dir(tmp_path, code_version=code_version)
    assert content_dir.parent.name == "codes"
    assert content_dir.name != "a" * 64
    assert len(content_dir.name) == 20
    assert variant_dir.parent.name == "subversions"
    assert variant_dir.name == "b" * 16


def test_put_code_creates_readable_code_index_link(tmp_path):
    state = NodeControlState(
        node_id="node-code-index-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_index"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n"
        digest = hashlib.sha256(blob).hexdigest()
        artifact, cached = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="demo.cache_index",
            entry_callable="run",
            package_format="py",
            chunks=[blob],
        )

        assert cached is False
        index_path = _code_index_link_path(
            Path(state.artifact_dir),
            code_version=artifact.code_version,
            entry_module=artifact.entry_module,
            entry_callable=artifact.entry_callable,
        )
        assert index_path.name.startswith("demo.cache_index__run__")
        assert index_path.exists() or index_path.is_symlink()
        if index_path.is_symlink():
            assert index_path.resolve() == _code_variant_dir(Path(state.artifact_dir), code_version=artifact.code_version).resolve()
        assert Path(artifact.path).resolve() == (_code_pkg_dir(Path(state.artifact_dir), code_version=artifact.code_version) / "artifact.py").resolve()
        assert _code_data_dir(Path(state.artifact_dir), code_version=artifact.code_version).exists()
    finally:
        state.close()


def test_update_runtime_globals_rejects_missing_disk_artifact(tmp_path):
    import shutil

    state = NodeControlState(
        node_id="node-stale-artifact-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_stale_artifact"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"STATE = None\ndef run(**_kwargs):\n    return {'state': STATE}\n"
        digest = hashlib.sha256(blob).hexdigest()
        artifact, cached = state.put_code(
            client_id="client-stale",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="demo.stale_artifact",
            entry_callable="run",
            package_format="py",
            managed_global_names=["STATE"],
            code_token="token-stale",
            chunks=[blob],
        )

        assert cached is False
        shutil.rmtree(_code_content_dir(Path(state.artifact_dir), code_version=artifact.code_version))

        with pytest.raises(KeyError, match="code artifact not found"):
            state.update_runtime_globals(
                client_id="client-stale",
                code_version=artifact.code_version,
                runtime_key="client-stale",
                code_token="token-stale",
                values={"STATE": 123},
            )
    finally:
        state.close()


def test_execute_payload_in_subprocess_uses_subversion_data_dir_as_cwd(tmp_path):
    state = NodeControlState(
        node_id="node-workdir-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_workdir"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = (
            b"import os\n"
            b"def run(**_kwargs):\n"
            b"    return {'cwd': os.getcwd()}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        artifact, cached = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="demo.cwd_artifact",
            entry_callable="run",
            package_format="py",
            chunks=[blob],
        )

        assert cached is False
        work_dir = _code_data_dir(Path(state.artifact_dir), code_version=artifact.code_version)
        status, result, err_type, err_message, _timings = _execute_payload_in_subprocess(
            **_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                work_dir=work_dir,
                method_name="run",
                payload={},
            )
        )

        assert status == "SUCCEEDED"
        assert err_type == ""
        assert err_message == ""
        assert result == {"cwd": str(work_dir.resolve())}
    finally:
        state.close()


def test_struct_to_dict_preserves_nan_in_dataframe_payload(tmp_path):
    import pandas as pd

    payload = {
        "__type__": "DataFrame",
        "data": [[1.0], [float("nan")]],
        "index": {"kind": "range", "start": 0, "stop": 2, "step": 1, "name": {"__type__": "pd.label", "kind": "none"}},
        "columns": {"kind": "index", "values": [{"__type__": "pd.label", "kind": "str", "value": "x"}], "name": {"__type__": "pd.label", "kind": "none"}},
        "column_dtypes": ["float64"],
    }
    restored = struct_to_dict(dict_to_struct(payload))
    assert isinstance(restored["value"], pd.DataFrame)
    assert restored["value"].shape == (2, 1)
    assert math.isnan(float(restored["value"].iloc[1, 0]))


def test_execute_payload_in_subprocess_uses_unified_inbound_normalizer(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-normalize-subprocess-01",
        artifact_dir=str(tmp_path / "code_cache_normalize_subprocess"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n"
        artifact, _cached = state.put_code(
            client_id="client-a",
            sha256="sha256:" + hashlib.sha256(blob).hexdigest(),
            runtime="py3",
            entry_module="normalize_subprocess_demo",
            entry_callable="run",
            package_format="py",
            export_mode="single",
            export_methods=["run"],
            chunks=[blob],
        )

        captured = {}

        def _fake_normalize(payload, *, object_dir, policy, resolve_object_refs=None):
            del resolve_object_refs
            captured["payload"] = dict(payload or {})
            captured["object_dir"] = object_dir
            captured["mode"] = policy.mode
            return {"value": 9}

        monkeypatch.setattr(
            "pycloud_parallel.controlplane.payload_transport.normalize_inbound_payload",
            _fake_normalize,
        )

        status, result, err_type, err_message, _timings = _execute_payload_in_subprocess(
            **_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 5},
                payload_mode="task_submit",
            )
        )

        assert status == "SUCCEEDED"
        assert result == {"value": 9}
        assert err_type == ""
        assert err_message == ""
        assert captured["payload"] == {"value": 5}
        assert captured["mode"] == "task_submit"
    finally:
        state.close()


def test_execute_payload_marks_result_encode_permission_error_as_infra(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-result-encode-permission-01",
        artifact_dir=str(tmp_path / "code_cache_result_encode_permission"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        artifact, _cached = state.put_code(
            client_id="client-result-encode",
            sha256="sha256:" + hashlib.sha256(blob).hexdigest(),
            runtime="py3",
            entry_module="result_encode_permission_demo",
            entry_callable="run",
            package_format="py",
            export_mode="single",
            export_methods=["run"],
            chunks=[blob],
        )

        def _raise_permission(*_args, **_kwargs):
            raise PermissionError(13, "Access is denied")

        monkeypatch.setattr(
            "pycloud_parallel.controlplane.node.execution._normalize_user_return",
            _raise_permission,
        )

        status, _result, err_type, err_message, timings = _execute_payload_in_subprocess(
            **_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={},
                payload_mode="http_call",
            )
        )

        assert status == "FAILED_INFRA"
        assert err_type == "PermissionError"
        assert "Access is denied" in err_message
        assert timings["encode_ms"] >= 0
    finally:
        state.close()


def test_execute_payload_keeps_user_permission_error_as_user_failure(tmp_path):
    state = NodeControlState(
        node_id="node-user-permission-01",
        artifact_dir=str(tmp_path / "code_cache_user_permission"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"def run(**_kwargs):\n    raise PermissionError(13, 'Access is denied')\n"
        artifact, _cached = state.put_code(
            client_id="client-user-permission",
            sha256="sha256:" + hashlib.sha256(blob).hexdigest(),
            runtime="py3",
            entry_module="user_permission_demo",
            entry_callable="run",
            package_format="py",
            export_mode="single",
            export_methods=["run"],
            chunks=[blob],
        )

        status, _result, err_type, err_message, _timings = _execute_payload_in_subprocess(
            **_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={},
                payload_mode="http_call",
            )
        )

        assert status == "FAILED_USER"
        assert err_type == "PermissionError"
        assert "Access is denied" in err_message
    finally:
        state.close()


def test_put_object_from_uploaded_file_uses_segment_backend_for_medium_objects(tmp_path, monkeypatch):
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_MAX_BYTES", 1024)
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_TARGET_BYTES", 4096)
    node_state = NodeControlState(
        node_id="node-object-segment-01",
        artifact_dir=str(tmp_path / "code_cache_object_segment"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"x" * 512
        uploaded_path = tmp_path / "upload.bin"
        uploaded_path.write_bytes(blob)
        object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
        artifact, cached = node_state.put_object_from_uploaded_file(
            object_id=object_id,
            format="bin",
            uploaded_path=str(uploaded_path),
            actual_sha256=hashlib.sha256(blob).hexdigest(),
            size_bytes=len(blob),
        )
        assert cached is False
        assert artifact.storage_backend == "segment"
        assert Path(artifact.segment_path).exists()
        with open(artifact.segment_path, "rb") as fp:
            fp.seek(artifact.segment_offset)
            assert fp.read(artifact.segment_length) == blob
        loaded = node_state.get_object_artifact(object_id)
        assert loaded.storage_backend == "segment"
        assert loaded.segment_length == len(blob)
    finally:
        node_state.close()


def test_release_object_removes_orphan_segment_file(tmp_path, monkeypatch):
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_MAX_BYTES", 1024)
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_TARGET_BYTES", 4096)
    node_state = NodeControlState(
        node_id="node-release-segment-01",
        artifact_dir=str(tmp_path / "code_cache_release_segment"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = json.dumps({"x": 1}).encode("utf-8")
        object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
        uploaded_path = tmp_path / "upload-release-segment.json"
        uploaded_path.write_bytes(blob)
        artifact, cached = node_state.put_object_from_uploaded_file(
            object_id=object_id,
            format="json",
            uploaded_path=str(uploaded_path),
            actual_sha256=hashlib.sha256(blob).hexdigest(),
            size_bytes=len(blob),
        )
        assert cached is False
        assert artifact.storage_backend == "segment"
        segment_path = Path(artifact.segment_path)
        assert segment_path.exists()

        released = node_state.release_object(object_id)

        assert released is True
        assert not segment_path.exists()
        with pytest.raises(KeyError):
            node_state.get_object_artifact(object_id)
    finally:
        node_state.close()


def test_resolve_object_refs_reads_segment_backend(tmp_path, monkeypatch):
    from pycloud_parallel.data.ref import DataRef

    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_MAX_BYTES", 1024)
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_TARGET_BYTES", 4096)
    node_state = NodeControlState(
        node_id="node-object-resolve-01",
        artifact_dir=str(tmp_path / "code_cache_object_resolve"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = json.dumps({"x": 1}).encode("utf-8")
        object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
        uploaded_path = tmp_path / "upload.json"
        uploaded_path.write_bytes(blob)
        node_state.put_object_from_uploaded_file(
            object_id=object_id,
            format="json",
            uploaded_path=str(uploaded_path),
            actual_sha256=hashlib.sha256(blob).hexdigest(),
            size_bytes=len(blob),
        )
        payload = {
            "item": DataRef(
                ref_id=object_id,
                storage_id=object_id,
                logical_type="json",
                format="json",
                size_bytes=len(blob),
                materialize_as="json",
                locator_kind="node_local",
                locator_token="",
                consume_on_read=True,
            )
        }
        resolved = _resolve_object_refs_in_payload(payload, object_dir=str(node_state.object_dir))
        assert resolved == {"item": {"x": 1}}
    finally:
        node_state.close()


def test_resolve_object_refs_restores_seriesbundle_from_segment_backend(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    from pycloud_parallel.data.ref import DataRef

    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_MAX_BYTES", 10 * 1024 * 1024)
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_TARGET_BYTES", 10 * 1024 * 1024)
    node_state = NodeControlState(
        node_id="node-object-series-segment-01",
        artifact_dir=str(tmp_path / "code_cache_object_series_segment"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        series = pd.Series(
            [1.1, 2.2],
            index=pd.MultiIndex.from_tuples(
                [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
                names=["trade_date", "bucket"],
            ),
            name=10006,
        )
        source = _serialize_data_for_object_ref(series)
        kind, fmt, blob = source.materialize_as, source.format, _object_upload_source_blob(source)
        object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
        uploaded_path = tmp_path / "upload.seriesbundle"
        uploaded_path.write_bytes(blob)
        _cleanup_object_upload_source(source)

        artifact, cached = node_state.put_object_from_uploaded_file(
            object_id=object_id,
            format=fmt,
            uploaded_path=str(uploaded_path),
            actual_sha256=hashlib.sha256(blob).hexdigest(),
            size_bytes=len(blob),
        )

        assert cached is False
        assert kind == "series"
        assert fmt == "seriesbundle"
        assert artifact.storage_backend == "segment"

        payload = {
            "item": DataRef(
                ref_id=object_id,
                storage_id=object_id,
                logical_type="series",
                format=fmt,
                size_bytes=len(blob),
                materialize_as="series",
                locator_kind="node_local",
                locator_token="",
                consume_on_read=True,
            )
        }
        resolved = _resolve_object_refs_in_payload(payload, object_dir=str(node_state.object_dir))

        pd.testing.assert_series_equal(resolved["item"], series)
    finally:
        node_state.close()


def test_put_object_from_uploaded_file_uses_file_backend_for_large_objects(tmp_path, monkeypatch):
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_MAX_BYTES", 128)
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_TARGET_BYTES", 4096)
    node_state = NodeControlState(
        node_id="node-object-file-01",
        artifact_dir=str(tmp_path / "code_cache_object_file"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"x" * 512
        uploaded_path = tmp_path / "upload-large.bin"
        uploaded_path.write_bytes(blob)
        object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
        artifact, cached = node_state.put_object_from_uploaded_file(
            object_id=object_id,
            format="bin",
            uploaded_path=str(uploaded_path),
            actual_sha256=hashlib.sha256(blob).hexdigest(),
            size_bytes=len(blob),
        )
        assert cached is False
        assert artifact.storage_backend == "file"
        assert Path(artifact.path).exists()
    finally:
        node_state.close()


def test_service_extra_object_get_streams_file_backend_object(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.http_gateway import StreamingHttpResponse

    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_MAX_BYTES", 1)
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_TARGET_BYTES", 1)
    node_state = NodeControlState(
        node_id="node-extra-object-stream-file-01",
        artifact_dir=str(tmp_path / "code_cache_extra_object_stream_file"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"stream-file-object" * 32
        uploaded_path = tmp_path / "upload-file-object.bin"
        uploaded_path.write_bytes(blob)
        object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
        node_state.put_object_from_uploaded_file(
            object_id=object_id,
            format="bin",
            uploaded_path=str(uploaded_path),
            actual_sha256=hashlib.sha256(blob).hexdigest(),
            size_bytes=len(blob),
        )

        response = node_state._service_extra_get_http("", ["objects", object_id], {})

        assert isinstance(response, StreamingHttpResponse)
        assert response.content_type == "application/octet-stream"
        assert response.content_length == len(blob)
        assert b"".join(response.body_iter) == blob
    finally:
        node_state.close()


def test_service_extra_object_get_streams_segment_backend_object(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.http_gateway import StreamingHttpResponse

    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_MAX_BYTES", 1024)
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_TARGET_BYTES", 4096)
    node_state = NodeControlState(
        node_id="node-extra-object-stream-segment-01",
        artifact_dir=str(tmp_path / "code_cache_extra_object_stream_segment"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"segment-object" * 16
        uploaded_path = tmp_path / "upload-segment-object.bin"
        uploaded_path.write_bytes(blob)
        object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
        node_state.put_object_from_uploaded_file(
            object_id=object_id,
            format="bin",
            uploaded_path=str(uploaded_path),
            actual_sha256=hashlib.sha256(blob).hexdigest(),
            size_bytes=len(blob),
        )

        response = node_state._service_extra_get_http("", ["objects", object_id], {})

        assert isinstance(response, StreamingHttpResponse)
        assert response.content_length == len(blob)
        assert b"".join(response.body_iter) == blob
    finally:
        node_state.close()


def test_release_object_removes_file_backend_object(tmp_path, monkeypatch):
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_MAX_BYTES", 1)
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_TARGET_BYTES", 1)
    node_state = NodeControlState(
        node_id="node-release-object-file-01",
        artifact_dir=str(tmp_path / "code_cache_release_object_file"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"x" * 64
        object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
        uploaded_path = tmp_path / "upload.bin"
        uploaded_path.write_bytes(blob)
        artifact, cached = node_state.put_object_from_uploaded_file(
            object_id=object_id,
            format="bin",
            uploaded_path=str(uploaded_path),
            actual_sha256=hashlib.sha256(blob).hexdigest(),
            size_bytes=len(blob),
        )
        assert cached is False
        assert Path(artifact.path).exists()

        released = node_state.release_object(object_id)

        assert released is True
        assert not Path(artifact.path).exists()
        with pytest.raises(KeyError):
            node_state.get_object_artifact(object_id)
    finally:
        node_state.close()


def test_pin_and_release_object_refcount_keeps_file_until_last_ref(tmp_path, monkeypatch):
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_MAX_BYTES", 1)
    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.OBJECT_SEGMENT_TARGET_BYTES", 1)
    node_state = NodeControlState(
        node_id="node-pin-release-object-file-01",
        artifact_dir=str(tmp_path / "code_cache_pin_release_object_file"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"y" * 64
        object_id = "sha256:" + hashlib.sha256(blob).hexdigest()
        uploaded_path = tmp_path / "upload-pin.bin"
        uploaded_path.write_bytes(blob)
        artifact, cached = node_state.put_object_from_uploaded_file(
            object_id=object_id,
            format="bin",
            uploaded_path=str(uploaded_path),
            actual_sha256=hashlib.sha256(blob).hexdigest(),
            size_bytes=len(blob),
        )
        assert cached is False
        assert Path(artifact.path).exists()

        assert node_state.pin_object(object_id, ref_id="ref-a") is True
        assert node_state.pin_object(object_id, ref_id="ref-b") is True
        assert node_state.release_object(object_id, ref_id="ref-a") is True
        assert Path(artifact.path).exists()
        assert node_state.get_object_artifact(object_id).object_id == object_id

        assert node_state.release_object(object_id, ref_id="ref-b") is True
        assert not Path(artifact.path).exists()
        with pytest.raises(KeyError):
            node_state.get_object_artifact(object_id)
    finally:
        node_state.close()


def test_service_session_http_call_and_end(tmp_path):
    state = NodeControlState(
        node_id="node-svc-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        blob = (
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(value=0, **_kwargs):\n"
            b"    v = int(value)\n"
            b"    return {'v': v, 'square': v * v}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-a",
            service_name="svc-a",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_entry",
            entry_callable="run",
            package_format="py",
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=True,
            chunks=[blob],
        )
        assert session.status == pb2.SERVICE_STATUS_RUNNING
        assert session.http_base_url.startswith("http://")

        req = Request(
            url=f"{session.http_base_url}/call/run",
            method="POST",
            data=json.dumps({"value": 8}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        assert body["ok"] is True
        assert body["data"]["square"] == 64
        info = state.service_status_info(session.service_id)
        timing = dict(info.get("timing_metrics") or {})
        assert int(timing.get("call_count", 0) or 0) >= 1
        assert float(timing.get("last_total_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_build_execute_spec_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_child_decode_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_child_invoke_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_invoke_wrapper_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_user_fn_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_child_encode_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_invoke_ms", 0.0) or 0.0) == float(timing.get("last_child_invoke_ms", 0.0) or 0.0)
        assert float(timing.get("avg_invoke_wrapper_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("avg_user_fn_ms", 0.0) or 0.0) >= 0.0

        hb = state.heartbeat_service(
            owner_client_id="owner-a",
            service_id=session.service_id,
            service_token=session.service_token,
        )
        assert hb.status == pb2.SERVICE_STATUS_RUNNING

        ended = state.end_service(
            owner_client_id="owner-a",
            service_id=session.service_id,
            service_token=session.service_token,
            reason="done",
        )
        assert ended.status == pb2.SERVICE_STATUS_STOPPED
    finally:
        state.close()


def test_task_pool_timing_metrics_recorded(tmp_path):
    state = NodeControlState(
        node_id="node-pool-timing-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_pool_timing"),
        enable_internal_executor=True,
        enable_service_session=False,
        executor_poll_interval_sec=0.02,
    )
    try:
        blob = (
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-timing",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_timing_demo",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        accepted, rejected = state.submit_pool_tasks(
            pool_id=pool.pool_id,
            pool_token=pool.pool_token,
            tasks=[pb2.TaskSubmitItem(task_id="pool-timing-1", payload=dict_to_struct({"value": 5}))],
            job_id="job-pool-timing",
        )
        assert len(accepted) == 1
        assert not rejected

        deadline = time.time() + 10.0
        results = []
        while time.time() < deadline and not results:
            state._drain_executor_events()  # noqa: SLF001
            results, _cursor = state.pull_pool_results(
                pool_id=pool.pool_id,
                pool_token=pool.pool_token,
                limit=10,
                wait_ms=0,
                cursor="",
            )
            if not results:
                time.sleep(0.05)

        assert len(results) == 1
        assert results[0].status == pb2.TASK_STATUS_SUCCEEDED

        timing = dict(state.task_pool(pool.pool_id).timing_metrics)
        assert int(timing.get("call_count", 0) or 0) >= 1
        assert float(timing.get("last_total_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_queue_wait_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("avg_queue_wait_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_child_decode_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_child_invoke_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_invoke_wrapper_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_user_fn_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_child_encode_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_invoke_ms", 0.0) or 0.0) == float(timing.get("last_child_invoke_ms", 0.0) or 0.0)
        assert float(timing.get("avg_invoke_wrapper_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("avg_user_fn_ms", 0.0) or 0.0) >= 0.0
    finally:
        state.close()


def test_submit_pool_tasks_rejects_when_node_queue_capacity_exceeded(tmp_path):
    state = NodeControlState(
        node_id="node-pool-queue-full-01",
        queue_capacity=1,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_pool_queue_full"),
        enable_internal_executor=True,
        enable_service_session=False,
        executor_poll_interval_sec=0.02,
    )
    try:
        blob = (
            b"import time\n"
            b"def run(value=0, sleep_ms=0, **_kwargs):\n"
            b"    time.sleep(max(0, int(sleep_ms)) / 1000.0)\n"
            b"    return {'value': int(value)}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-queue-full",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_queue_full_demo",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        with patch(
            "pycloud_parallel.controlplane.nodecontrol_state.decode_transport_payload_bytes",
            side_effect=AssertionError("rejected payload should not be decoded"),
        ) as mocked_decode:
            accepted, rejected = state.submit_pool_tasks(
                pool_id=pool.pool_id,
                pool_token=pool.pool_token,
                tasks=[
                    pb2.TaskSubmitItem(task_id="pool-queue-1", payload=dict_to_struct({"value": 1, "sleep_ms": 200})),
                    pb2.TaskSubmitItem(
                        task_id="pool-queue-2",
                        transport_payload=pb2.TransportPayload(
                            codec="pickle_stable_v1",
                            version=1,
                            payload=b"large-rejected-payload",
                        ),
                    ),
                ],
                job_id="job-pool-queue-full",
            )
        assert [item.task_id for item in accepted] == ["pool-queue-1"]
        assert len(rejected) == 1
        assert rejected[0].task_id == "pool-queue-2"
        assert rejected[0].code == pb2.ERROR_CODE_QUEUE_FULL
        mocked_decode.assert_not_called()
    finally:
        state.close()


def test_submit_pool_tasks_computes_node_queue_occupancy_once_per_batch(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-pool-occupancy-once-01",
        queue_capacity=16,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_pool_occupancy_once"),
        enable_internal_executor=True,
        enable_service_session=False,
        executor_poll_interval_sec=0.02,
    )
    try:
        blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-occupancy-once",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_occupancy_once_demo",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        calls = {"count": 0}
        original = state._node_queue_occupancy_locked  # noqa: SLF001

        def _counting_occupancy():
            calls["count"] += 1
            return original()

        monkeypatch.setattr(state, "_node_queue_occupancy_locked", _counting_occupancy)

        accepted, rejected = state.submit_pool_tasks(
            pool_id=pool.pool_id,
            pool_token=pool.pool_token,
            tasks=[
                pb2.TaskSubmitItem(task_id="pool-occupancy-1", payload=dict_to_struct({"value": 1})),
                pb2.TaskSubmitItem(task_id="pool-occupancy-2", payload=dict_to_struct({"value": 2})),
                pb2.TaskSubmitItem(task_id="pool-occupancy-3", payload=dict_to_struct({"value": 3})),
            ],
            job_id="job-pool-occupancy-once",
        )

        assert len(accepted) == 3
        assert not rejected
        assert calls["count"] == 1
    finally:
        state.close()


def test_submit_pool_tasks_rejects_bad_item_without_rolling_back_prior_accepts(tmp_path):
    state = NodeControlState(
        node_id="node-pool-partial-reject-01",
        queue_capacity=16,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_pool_partial_reject"),
        enable_internal_executor=False,
        enable_service_session=False,
        executor_poll_interval_sec=0.02,
    )
    try:
        submitted = []

        class _FakeExecutorHost:
            def is_alive(self):
                return True

            def create_task_pool(self, **_kwargs):
                pass

            def preload_pool(self, **_kwargs):
                return 1

            def submit_pool_task(self, **kwargs):
                submitted.append(kwargs)

            def stop_task_pool(self, **_kwargs):
                pass

            def drain_events(self):
                return []

            def close(self, **_kwargs):
                pass

        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-partial-reject",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_partial_reject_demo",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        accepted, rejected = state.submit_pool_tasks(
            pool_id=pool.pool_id,
            pool_token=pool.pool_token,
            tasks=[
                pb2.TaskSubmitItem(task_id="pool-good-1", payload=dict_to_struct({"value": 1})),
                pb2.TaskSubmitItem(
                    task_id="pool-bad-1",
                    transport_payload=pb2.TransportPayload(
                        codec="unknown_v1",
                        version=1,
                        payload=b"{}",
                    ),
                ),
            ],
            job_id="job-partial-reject",
        )

        assert [item.task_id for item in accepted] == ["pool-good-1"]
        assert len(rejected) == 1
        assert rejected[0].task_id == "pool-bad-1"
        assert rejected[0].code == pb2.ERROR_CODE_INTERNAL_ERROR
        assert [item["task_id"] for item in submitted] == ["pool-good-1"]
        assert "pool-good-1" in state._pool_tasks  # noqa: SLF001
        assert "pool-bad-1" not in state._pool_tasks  # noqa: SLF001
        assert not state._pool_task_reserved_ids  # noqa: SLF001
    finally:
        state.close()


@pytest.mark.parametrize("mode", ["legacy_v1", "structured_v1", "pickle_stable_v1", "pickle_native_v1"])
def test_submit_pool_transport_payload_adapter_stays_opaque_until_worker(tmp_path, monkeypatch, mode):
    state = NodeControlState(
        node_id=f"node-pool-opaque-{mode}",
        queue_capacity=16,
        worker_capacity=1,
        artifact_dir=str(tmp_path / f"code_cache_pool_opaque_{mode}"),
        enable_internal_executor=False,
        enable_service_session=False,
        executor_poll_interval_sec=0.02,
    )
    try:
        submitted = []

        class _FakeExecutorHost:
            def is_alive(self):
                return True

            def create_task_pool(self, **_kwargs):
                pass

            def preload_pool(self, **_kwargs):
                return 1

            def submit_pool_task(self, **kwargs):
                submitted.append(kwargs)

            def stop_task_pool(self, **_kwargs):
                pass

            def drain_events(self):
                return []

            def close(self, **_kwargs):
                pass

        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value) + 1}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name=f"pool-opaque-{mode}",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module=f"pool_opaque_{mode}",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        monkeypatch.setattr(
            "pycloud_parallel.controlplane.nodecontrol_state.decode_transport_payload_bytes",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("node must not decode inline bytes")),
        )

        accepted, rejected = state.submit_pool_tasks(
            pool_id=pool.pool_id,
            pool_token=pool.pool_token,
            tasks=[
                pb2.TaskSubmitItem(
                    task_id=f"pool-opaque-{mode}",
                    transport_payload=encode_transport_payload_bytes(
                        {"value": 41},
                        mode=mode,
                        context="taskpool_session",
                    ),
                )
            ],
            job_id="job-opaque",
        )

        assert [item.task_id for item in accepted] == [f"pool-opaque-{mode}"]
        assert rejected == []
        execute_spec = submitted[0]["execute_spec"]
        assert is_inline_transport_carrier(execute_spec["payload"])
        assert decode_inline_transport_carrier(execute_spec["payload"], context="taskpool_session") == {"value": 41}

        status_text, result, err_type, err_message, _timings = _execute_payload_in_subprocess(
            execute_spec["artifact_path"],
            execute_spec["entry_module"],
            execute_spec["package_format"],
            execute_spec["dependency_path"],
            execute_spec["dependency_policy_mode"],
            execute_spec["object_dir"],
            execute_spec["work_dir"],
            execute_spec["managed_globals_scope_dir"],
            execute_spec["managed_globals_digest"],
            execute_spec["export_mode"],
            execute_spec["export_methods"],
            execute_spec["export_decorator"],
            execute_spec["method_name"],
            execute_spec["entry_callable"],
            execute_spec["payload"],
            execute_spec["warmup_only"],
            execute_spec["payload_mode"],
            execute_spec["serialization_mode"],
            execute_spec["use_transport_result"],
        )

        assert status_text == "SUCCEEDED", (err_type, err_message)
        assert is_inline_transport_carrier(result)
        assert decode_inline_transport_carrier(result, context="service_result") == {"value": 42}
    finally:
        state.close()


def test_create_task_pool_skips_preload_for_default_fork_workers(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "name", "posix")
    monkeypatch.delenv("PYCLOUD_WORKER_START_METHOD", raising=False)
    state = NodeControlState(
        node_id="node-pool-preload-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_pool_preload"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def create_task_pool(self, **kwargs):
            calls.append(("create", kwargs))

        def preload_pool(self, **kwargs):
            calls.append(("preload", kwargs))
            return int(kwargs["fanout"])

        def stop_task_pool(self, **kwargs):
            calls.append(("stop", kwargs))

        def drain_events(self):
            return []

        def close(self, **_kwargs):
            pass

    try:
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-preload",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_preload",
            entry_callable="run",
            package_format="py",
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )

        assert pool.worker_count == 2
        assert [kind for kind, _payload in calls] == ["create"]
    finally:
        state.close()


def test_create_task_pool_preloads_entry_module_for_spawn_workers(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_WORKER_START_METHOD", "spawn")
    state = NodeControlState(
        node_id="node-pool-spawn-preload-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_pool_spawn_preload"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def create_task_pool(self, **kwargs):
            calls.append(("create", kwargs))

        def preload_pool(self, **kwargs):
            calls.append(("preload", kwargs))
            return int(kwargs["fanout"])

        def stop_task_pool(self, **kwargs):
            calls.append(("stop", kwargs))

        def drain_events(self):
            return []

        def close(self, **_kwargs):
            pass

    try:
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-spawn-preload",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_spawn_preload",
            entry_callable="run",
            package_format="py",
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )

        assert pool.worker_count == 2
        assert [kind for kind, _payload in calls] == ["create", "preload"]
        preload = calls[1][1]
        assert preload["pool_id"] == pool.pool_id
        assert preload["fanout"] == 2
        assert preload["execute_spec"]["entry_module"] == "pool_spawn_preload"
        assert preload["execute_spec"]["method_name"] == "run"
        assert preload["execute_spec"]["payload_mode"] == "task_submit"
        assert preload["execute_spec"]["warmup_only"] is True
    finally:
        state.close()


def test_create_service_skips_preload_for_default_fork_workers(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "name", "posix")
    monkeypatch.delenv("PYCLOUD_WORKER_START_METHOD", raising=False)
    state = NodeControlState(
        node_id="node-service-preload-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_service_preload"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def create_service(self, **kwargs):
            calls.append(("create", kwargs))

        def preload_service(self, **kwargs):
            calls.append(("preload", kwargs))
            return int(kwargs["fanout"])

        def stop_service(self, **kwargs):
            calls.append(("stop", kwargs))

        def drain_events(self):
            return []

        def close(self, **_kwargs):
            pass

    try:
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        blob = b"def serve(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-service",
            service_name="svc-preload",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="service_preload",
            entry_callable="serve",
            package_format="py",
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )

        assert session.worker_count == 2
        assert [kind for kind, _payload in calls] == ["create"]
    finally:
        state.close()


def test_create_service_preloads_entry_module_for_spawn_workers(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_WORKER_START_METHOD", "spawn")
    state = NodeControlState(
        node_id="node-service-spawn-preload-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_service_spawn_preload"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def create_service(self, **kwargs):
            calls.append(("create", kwargs))

        def preload_service(self, **kwargs):
            calls.append(("preload", kwargs))
            return int(kwargs["fanout"])

        def stop_service(self, **kwargs):
            calls.append(("stop", kwargs))

        def drain_events(self):
            return []

        def close(self, **_kwargs):
            pass

    try:
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        blob = b"def serve(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-service",
            service_name="svc-spawn-preload",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="service_spawn_preload",
            entry_callable="serve",
            package_format="py",
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )

        assert session.worker_count == 2
        assert [kind for kind, _payload in calls] == ["create", "preload"]
        preload = calls[1][1]
        assert preload["service_id"] == session.service_id
        assert preload["fanout"] == 2
        assert preload["execute_spec"]["entry_module"] == "service_spawn_preload"
        assert preload["execute_spec"]["method_name"] == "serve"
        assert preload["execute_spec"]["payload_mode"] == "http_call"
        assert preload["execute_spec"]["warmup_only"] is True
    finally:
        state.close()


def test_create_service_rejects_duplicate_running_service_name_on_node(tmp_path):
    state = NodeControlState(
        node_id="node-service-dupe-01",
        queue_capacity=16,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "code_cache_service_dupe"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def create_service(self, **kwargs):
            calls.append(("create", kwargs))

        def preload_service(self, **kwargs):
            calls.append(("preload", kwargs))
            return int(kwargs["fanout"])

        def stop_service(self, **kwargs):
            calls.append(("stop", kwargs))

        def drain_events(self):
            return []

        def close(self, **_kwargs):
            pass

    try:
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        blob = b"def serve(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        first = state.create_service(
            owner_client_id="owner-service",
            service_name="svc-dupe",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="service_dupe",
            entry_callable="serve",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )

        with pytest.raises(RuntimeError, match="service_name already exists on this node"):
            state.create_service(
                owner_client_id="owner-service",
                service_name="svc-dupe",
                sha256=f"sha256:{digest}",
                runtime="py3",
                entry_module="service_dupe",
                entry_callable="serve",
                package_format="py",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=False,
                chunks=[blob],
            )

        assert [kind for kind, _payload in calls] == ["create"]
        reports = [
            report
            for report in state.service_report_payloads(include_stopped=True)
            if report["service_name"] == "svc-dupe"
        ]
        assert [report["service_id"] for report in reports] == [first.service_id]
        assert reports[0]["status"] == pb2.SERVICE_STATUS_RUNNING
    finally:
        state.close()


def test_pull_pool_results_prunes_completed_task_state(tmp_path):
    state = NodeControlState(
        node_id="node-pool-prune-01",
        queue_capacity=16,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_pool_prune"),
        enable_internal_executor=True,
        enable_service_session=False,
        executor_poll_interval_sec=0.02,
    )
    try:
        blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-prune",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_prune_demo",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        accepted, rejected = state.submit_pool_tasks(
            pool_id=pool.pool_id,
            pool_token=pool.pool_token,
            tasks=[pb2.TaskSubmitItem(task_id="pool-prune-1", payload=dict_to_struct({"value": 7}))],
            job_id="job-pool-prune",
        )
        assert len(accepted) == 1
        assert not rejected

        deadline = time.time() + 10.0
        results = []
        while time.time() < deadline and not results:
            state._drain_executor_events()  # noqa: SLF001
            assert "pool-prune-1" in state._pool_tasks  # noqa: SLF001
            results, _cursor = state.pull_pool_results(
                pool_id=pool.pool_id,
                pool_token=pool.pool_token,
                limit=10,
                wait_ms=0,
                cursor="",
            )
            if not results:
                time.sleep(0.05)

        assert len(results) == 1
        assert results[0].status == pb2.TASK_STATUS_SUCCEEDED
        assert "pool-prune-1" not in state._pool_tasks  # noqa: SLF001
    finally:
        state.close()


def test_service_session_management_requires_token(tmp_path):
    state = NodeControlState(
        node_id="node-svc-auth-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-auth",
            service_name="svc-auth",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_auth",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )

        try:
            state.heartbeat_service(
                owner_client_id="owner-auth",
                service_id=session.service_id,
                service_token="",
            )
            assert False, "expected missing token to be rejected"
        except PermissionError as exc:
            assert "service_token mismatch" in str(exc)

        try:
            state.end_service(
                owner_client_id="owner-auth",
                service_id=session.service_id,
                service_token="bad-token",
                reason="should fail",
            )
            assert False, "expected bad token to be rejected"
        except PermissionError as exc:
            assert "service_token mismatch" in str(exc)
    finally:
        state.close()


def test_service_session_heartbeat_timeout_recycles(tmp_path):
    state = NodeControlState(
        node_id="node-svc-02",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=60,
    )
    try:
        blob = (
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(**_kwargs):\n"
            b"    return {'ok': True}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-b",
            service_name="svc-b",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_entry",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=1,
            idle_ttl_sec=0,
            expose_http=True,
            chunks=[blob],
        )
        assert session.status == pb2.SERVICE_STATUS_RUNNING
        session.last_heartbeat_at = utc_now() - timedelta(seconds=5)
        session.lease_expire_at = utc_now() - timedelta(seconds=1)
        state._handle_service_timeouts()  # noqa: SLF001
        info = state.service_status_info(session.service_id)
        assert info["status"] == pb2.SERVICE_STATUS_STOPPED
    finally:
        state.close()


def test_service_sessions_with_same_blob_and_different_managed_globals_can_coexist(tmp_path):
    state = NodeControlState(
        node_id="node-svc-managed-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_managed"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        blob = (
            b"A = 1\n"
            b"B = 2\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(**_kwargs):\n"
            b"    return {'A': A, 'B': B}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session_a = state.create_service(
            owner_client_id="owner-a",
            service_name="svc-a",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_managed",
            entry_callable="run",
            package_format="py",
            managed_global_names=["A"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )
        session_b = state.create_service(
            owner_client_id="owner-b",
            service_name="svc-b",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_managed",
            entry_callable="run",
            package_format="py",
            managed_global_names=["B"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )

        digest_a, updated_a = state.update_service_globals(
            owner_client_id="owner-a",
            service_id=session_a.service_id,
            service_token=session_a.service_token,
            values={"A": 10},
        )
        digest_b, updated_b = state.update_service_globals(
            owner_client_id="owner-b",
            service_id=session_b.service_id,
            service_token=session_b.service_token,
            values={"B": 20},
        )

        assert updated_a == ["A"]
        assert updated_b == ["B"]
        assert digest_a
        assert digest_b
        assert session_a.managed_global_names == ("A",)
        assert session_b.managed_global_names == ("B",)
    finally:
        state.close()


def test_managed_globals_validation_reuses_single_module_load(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-svc-managed-load-once",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_managed_once"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    calls = {"count": 0}
    original = sys.modules["pycloud_parallel.controlplane.node.execution"]._load_user_module

    def wrapped(*args, **kwargs):
        calls["count"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr("pycloud_parallel.controlplane.node.execution._load_user_module", wrapped)
    try:
        blob = (
            b"A = None\n"
            b"B = None\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(**_kwargs):\n"
            b"    return {'ok': True}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        artifact, _ = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_managed_once",
            entry_callable="run",
            package_format="py",
            export_mode="decorator",
            managed_global_names=["A", "B"],
            chunks=[blob],
            validate_load=False,
        )
        methods = state._validate_artifact_methods(artifact, dependency_path="")
        assert sorted(methods.keys()) == ["run"]
        assert calls["count"] == 1
    finally:
        state.close()


def test_update_service_globals_rejects_callable_module_and_class_values(tmp_path):
    state = NodeControlState(
        node_id="node-svc-managed-update-guard",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_managed_update"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        blob = (
            b"A = None\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(**_kwargs):\n"
            b"    return {'ok': True}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-a",
            service_name="svc-a",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_managed_update",
            entry_callable="run",
            package_format="py",
            managed_global_names=["A"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )

        class _CallableObject:
            def __call__(self):
                return "x"

        with pytest.raises(ValueError, match="managed globals must be data values"):
            state.update_service_globals(
                owner_client_id="owner-a",
                service_id=session.service_id,
                service_token=session.service_token,
                values={"A": len},
            )

        with pytest.raises(ValueError, match="managed globals must be data values"):
            state.update_service_globals(
                owner_client_id="owner-a",
                service_id=session.service_id,
                service_token=session.service_token,
                values={"A": _CallableObject},
            )

        with pytest.raises(ValueError, match="managed globals must be data values"):
            state.update_service_globals(
                owner_client_id="owner-a",
                service_id=session.service_id,
                service_token=session.service_token,
                values={"A": inspect},
            )

        with pytest.raises(ValueError, match="managed globals must be data values"):
            state.update_service_globals(
                owner_client_id="owner-a",
                service_id=session.service_id,
                service_token=session.service_token,
                values={"A": _CallableObject()},
            )
    finally:
        state.close()


def test_update_service_globals_triggers_warmup_with_worker_pids(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-svc-managed-warmup",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_managed_warmup"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    warmup_calls = []
    monkeypatch.setattr(
        state,
        "_log_warmup_result",
        lambda **kwargs: warmup_calls.append(("log", kwargs)),
    )
    try:
        blob = (
            b"A = None\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(**_kwargs):\n"
            b"    return {'ok': True}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-a",
            service_name="svc-a",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_managed_warmup",
            entry_callable="run",
            package_format="py",
            managed_global_names=["A"],
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )
        monkeypatch.setattr(
            state._executor_host,
            "warmup_service",
            lambda **kwargs: warmup_calls.append(("warmup", kwargs)) or [111, 222],
        )
        digest_a, updated = state.update_service_globals(
            owner_client_id="owner-a",
            service_id=session.service_id,
            service_token=session.service_token,
            values={"A": 10},
        )
        assert digest_a
        assert updated == ["A"]
        warmup = next(item for kind, item in warmup_calls if kind == "warmup")
        assert warmup["service_id"] == session.service_id
        assert warmup["fanout"] == 2
        logged = next(item for kind, item in warmup_calls if kind == "log")
        assert logged["worker_pids"] == [111, 222]
    finally:
        state.close()


def test_task_pool_keeps_instance_managed_global_names(tmp_path):
    state = NodeControlState(
        node_id="node-pool-managed-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_pool_managed"),
        enable_internal_executor=True,
        enable_service_session=False,
    )
    try:
        blob = b"STATE = 1\ndef run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-managed",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_managed",
            entry_callable="run",
            package_format="py",
            managed_global_names=["STATE"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        assert pool.managed_global_names == ("STATE",)
    finally:
        state.close()


def test_task_pool_execution_uses_private_managed_globals(tmp_path):
    state = NodeControlState(
        node_id="node-pool-managed-exec-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_pool_managed_exec"),
        enable_internal_executor=True,
        enable_service_session=False,
    )
    try:
        blob = b"STATE = None\ndef run(**_kwargs):\n    return {'state': STATE}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-managed-exec",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_managed_exec",
            entry_callable="run",
            package_format="py",
            managed_global_names=["STATE"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        assert pool.managed_globals_scope_dir
        assert pool.managed_globals_digest

        globals_digest, updated = state.update_runtime_globals(
            client_id=pool.pool_id,
            code_version=pool.code_version,
            runtime_key=pool.pool_id,
            code_token=state.get_client_code_token(client_id=pool.pool_id, code_version=pool.code_version),
            values={"STATE": 123},
        )
        assert updated == ["STATE"]
        assert pool.managed_globals_scope_dir
        assert pool.managed_globals_digest == globals_digest

        accepted, rejected = state.submit_pool_tasks(
            pool_id=pool.pool_id,
            pool_token=pool.pool_token,
            tasks=[pb2.TaskSubmitItem(task_id="pool-task-1", payload={})],
            job_id="job-managed",
        )
        assert [x.task_id for x in accepted] == ["pool-task-1"]
        assert rejected == []

        deadline = time.time() + 5.0
        results = []
        while time.time() < deadline:
            state._drain_executor_events()  # noqa: SLF001
            results, _cursor = state.pull_pool_results(
                pool_id=pool.pool_id,
                pool_token=pool.pool_token,
                limit=10,
                wait_ms=0,
                cursor="",
            )
            if results:
                break
            time.sleep(0.05)

        assert len(results) == 1
        assert struct_to_dict(results[0].result) == {"state": 123}
    finally:
        state.close()


def test_runtime_managed_globals_pickle_mode_keeps_binary_snapshot(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-pool-managed-pickle-01",
        queue_capacity=16,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_pool_managed_pickle"),
        enable_internal_executor=True,
        enable_service_session=False,
    )
    try:
        blob = b"STATE = None\ndef run(**_kwargs):\n    return {'state': STATE}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-managed-pickle",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_managed_pickle",
            entry_callable="run",
            package_format="py",
            managed_global_names=["STATE"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        monkeypatch.setattr(state._executor_host, "warmup_pool", lambda **_kwargs: [123])  # noqa: SLF001

        globals_digest, updated = state.update_runtime_globals(
            client_id=pool.pool_id,
            code_version=pool.code_version,
            runtime_key=pool.pool_id,
            code_token=state.get_client_code_token(client_id=pool.pool_id, code_version=pool.code_version),
            values={"STATE": {"value": [1, 2, 3]}},
            serialization_mode="pickle_stable_v1",
        )

        assert updated == ["STATE"]
        manifest_path = Path(pool.managed_globals_scope_dir) / "manifests" / f"{globals_digest.replace('sha256:', '')}.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        item = manifest["values"]["STATE"]
        assert item["codec"] == "pickle_stable_v1"
        value_path = Path(pool.managed_globals_scope_dir) / "values" / f"{item['sha256']}.bin"
        assert value_path.exists()
        assert not (Path(pool.managed_globals_scope_dir) / "values" / f"{item['sha256']}.json").exists()
        assert stable_pickle_loads(value_path.read_bytes()) == {"value": [1, 2, 3]}
    finally:
        state.close()


def test_runtime_managed_globals_scope_dirs_are_isolated_per_node(tmp_path):
    shared_artifact_dir = tmp_path / "code_cache_shared_runtime_globals"
    state_a = NodeControlState(
        node_id="node-runtime-a",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(shared_artifact_dir),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    state_b = NodeControlState(
        node_id="node-runtime-b",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(shared_artifact_dir),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"STATE = None\ndef run(**_kwargs):\n    return {'state': STATE}\n"
        digest = hashlib.sha256(blob).hexdigest()
        artifact_a, _ = state_a.put_code(
            client_id="pool-owner",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="runtime_scope_isolation",
            entry_callable="run",
            package_format="py",
            managed_global_names=["STATE"],
            code_token="token-a",
            chunks=[blob],
        )
        artifact_b, _ = state_b.put_code(
            client_id="pool-owner",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="runtime_scope_isolation",
            entry_callable="run",
            package_format="py",
            managed_global_names=["STATE"],
            code_token="token-b",
            chunks=[blob],
        )

        with state_a._lock:  # noqa: SLF001
            scope_a = state_a._ensure_runtime_managed_globals_state_locked(  # noqa: SLF001
                client_id="pool-owner",
                code_version=artifact_a.code_version,
                runtime_key="pool-owner",
                allowed_names=["STATE"],
            )
        with state_b._lock:  # noqa: SLF001
            scope_b = state_b._ensure_runtime_managed_globals_state_locked(  # noqa: SLF001
                client_id="pool-owner",
                code_version=artifact_b.code_version,
                runtime_key="pool-owner",
                allowed_names=["STATE"],
            )

        assert scope_a is not None
        assert scope_b is not None
        assert Path(scope_a.scope_dir).is_absolute()
        assert Path(scope_b.scope_dir).is_absolute()
        assert scope_a.scope_dir != scope_b.scope_dir
        assert "node-runtime-a" not in scope_a.scope_dir
        assert "node-runtime-b" not in scope_b.scope_dir
    finally:
        state_a.close()
        state_b.close()


def test_normalize_warmup_result_accepts_submitted_count_only():
    from pycloud_parallel.controlplane.node.session_views import normalize_warmup_result

    submitted, worker_pids = normalize_warmup_result(4, fanout=8)
    assert submitted == 4
    assert worker_pids == []


def test_update_runtime_globals_for_pool_triggers_pool_warmup(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-pool-managed-warmup",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_pool_warmup"),
        enable_internal_executor=True,
        enable_service_session=False,
    )
    warmup_calls = []
    monkeypatch.setattr(
        state,
        "_log_warmup_result",
        lambda **kwargs: warmup_calls.append(("log", kwargs)),
    )
    try:
        blob = b"STATE = None\ndef run(**_kwargs):\n    return {'state': STATE}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-managed-warmup",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_managed_warmup",
            entry_callable="run",
            package_format="py",
            managed_global_names=["STATE"],
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        monkeypatch.setattr(
            state._executor_host,
            "warmup_pool",
            lambda **kwargs: warmup_calls.append(("warmup", kwargs)) or [333, 444],
        )
        digest_out, updated = state.update_runtime_globals(
            client_id=pool.pool_id,
            code_version=pool.code_version,
            runtime_key=pool.pool_id,
            code_token=state.get_client_code_token(client_id=pool.pool_id, code_version=pool.code_version),
            values={"STATE": 123},
        )
        assert digest_out
        assert updated == ["STATE"]
        warmup = next(item for kind, item in warmup_calls if kind == "warmup")
        assert warmup["pool_id"] == pool.pool_id
        assert warmup["fanout"] == 2
        logged = next(item for kind, item in warmup_calls if kind == "log")
        assert logged["worker_pids"] == [333, 444]
    finally:
        state.close()


def test_runtime_managed_globals_mapping_prefers_runtime_key_and_falls_back(tmp_path):
    state = NodeControlState(
        node_id="node-runtime-managed-map-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_runtime_map"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        with state._lock:  # noqa: SLF001
            state._register_client_code_managed_globals_locked(  # noqa: SLF001
                client_id="client-a",
                code_version="sha256:" + ("a" * 64),
                runtime_key="",
                managed_global_names=["A"],
            )
            state._register_client_code_managed_globals_locked(  # noqa: SLF001
                client_id="client-a",
                code_version="sha256:" + ("a" * 64),
                runtime_key="rt-1",
                managed_global_names=["B"],
            )

        assert state.get_client_code_managed_globals(  # noqa: SLF001
            client_id="client-a",
            code_version="sha256:" + ("a" * 64),
            runtime_key="rt-1",
        ) == ("B",)
        assert state.get_client_code_managed_globals(  # noqa: SLF001
            client_id="client-a",
            code_version="sha256:" + ("a" * 64),
            runtime_key="rt-2",
        ) == ("A",)
    finally:
        state.close()


def test_apply_managed_globals_allows_indirect_runtime_binding_for_task_pool(tmp_path):
    state = NodeControlState(
        node_id="node-apply-managed-globals-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_apply_managed_globals"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = (
            b"RUNTIME_STATE = {}\n"
            b"GLOBAL_CFG = None\n\n"
            b"def apply_managed_globals(values, **context):\n"
            b"    RUNTIME_STATE['cfg'] = values.get('cfg')\n"
            b"    RUNTIME_STATE['context'] = dict(context)\n"
            b"    return {'GLOBAL_CFG': values.get('cfg')}\n\n"
            b"def run(**_kwargs):\n"
            b"    return {\n"
            b"        'cfg': GLOBAL_CFG,\n"
            b"        'runtime_cfg': RUNTIME_STATE.get('cfg'),\n"
            b"        'session_kind': RUNTIME_STATE.get('context', {}).get('session_kind'),\n"
            b"        'method_name': RUNTIME_STATE.get('context', {}).get('method_name'),\n"
            b"        'entry_module': RUNTIME_STATE.get('context', {}).get('entry_module'),\n"
            b"    }\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        artifact, _cached = state.put_code(
            client_id="owner-apply",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_apply_globals_demo",
            entry_callable="run",
            package_format="py",
            export_mode="single",
            export_methods=["run"],
            managed_global_names=["cfg"],
            chunks=[blob],
            validate_load=True,
        )
        globals_digest, updated_names = state.update_runtime_globals(
            client_id="owner-apply",
            code_version=artifact.code_version,
            runtime_key="rt-apply",
            code_token=state.get_client_code_token(client_id="owner-apply", code_version=artifact.code_version),
            values={"cfg": {"mode": "fast"}},
        )
        assert globals_digest
        assert updated_names == ["cfg"]

        with state._lock:  # noqa: SLF001
            artifact = state._codes[artifact.code_version]  # noqa: SLF001
            managed_state = state._runtime_managed_globals[("owner-apply", artifact.code_version, "rt-apply")]  # noqa: SLF001

        status, result, err_type, err_message, _timings = _execute_payload_in_subprocess(
            **_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                work_dir=_code_data_dir(Path(state.artifact_dir), code_version=artifact.code_version),
                method_name="run",
                payload={},
                payload_mode="task_submit",
                managed_globals_scope_dir=managed_state.scope_dir,
                managed_globals_digest=managed_state.globals_digest,
            )
        )

        assert status == "SUCCEEDED"
        assert err_type == ""
        assert err_message == ""
        assert result["cfg"] == {"mode": "fast"}
        assert result["runtime_cfg"] == {"mode": "fast"}
        assert result["session_kind"] == "task_pool"
        assert result["method_name"] == "run"
        assert result["entry_module"] == "pool_apply_globals_demo"
    finally:
        state.close()


def test_apply_managed_globals_pickle_mode_does_not_read_full_bytes(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-apply-managed-globals-pickle-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_apply_managed_globals_pickle"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = (
            b"GLOBAL_STATE = None\n\n"
            b"def apply_managed_globals(values, **_context):\n"
            b"    return {'GLOBAL_STATE': values.get('cfg')}\n\n"
            b"def run(**_kwargs):\n"
            b"    return {'cfg': GLOBAL_STATE}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        artifact, _cached = state.put_code(
            client_id="owner-apply-pickle",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_apply_globals_pickle_demo",
            entry_callable="run",
            package_format="py",
            export_mode="single",
            export_methods=["run"],
            managed_global_names=["cfg"],
            chunks=[blob],
            validate_load=True,
        )
        globals_digest, updated_names = state.update_runtime_globals(
            client_id="owner-apply-pickle",
            code_version=artifact.code_version,
            runtime_key="rt-apply-pickle",
            code_token=state.get_client_code_token(client_id="owner-apply-pickle", code_version=artifact.code_version),
            values={"cfg": {"value": [1, 2, 3]}},
            serialization_mode="pickle_stable_v1",
        )
        assert globals_digest
        assert updated_names == ["cfg"]

        with state._lock:  # noqa: SLF001
            artifact = state._codes[artifact.code_version]  # noqa: SLF001
            managed_state = state._runtime_managed_globals[("owner-apply-pickle", artifact.code_version, "rt-apply-pickle")]  # noqa: SLF001

        manifest_path = Path(managed_state.scope_dir) / "manifests" / f"{managed_state.globals_digest.replace('sha256:', '')}.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        value_digest = manifest["values"]["cfg"]["sha256"]
        value_path = Path(managed_state.scope_dir) / "values" / f"{value_digest}.bin"
        original_read_bytes = Path.read_bytes

        def _guard_read_bytes(self):  # noqa: ANN001
            if Path(self) == value_path:
                raise AssertionError("managed globals pickle load must not read full bytes")
            return original_read_bytes(self)

        monkeypatch.setattr(Path, "read_bytes", _guard_read_bytes)

        status, result, err_type, err_message, _timings = _execute_payload_in_subprocess(
            **_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                work_dir=_code_data_dir(Path(state.artifact_dir), code_version=artifact.code_version),
                method_name="run",
                payload={},
                payload_mode="task_submit",
                serialization_mode="pickle_stable_v1",
                managed_globals_scope_dir=managed_state.scope_dir,
                managed_globals_digest=managed_state.globals_digest,
            )
        )

        assert status == "SUCCEEDED"
        assert err_type == ""
        assert err_message == ""
        assert result["cfg"] == {"value": [1, 2, 3]}
    finally:
        state.close()


def test_managed_global_names_still_require_entry_globals_without_apply_hook(tmp_path):
    state = NodeControlState(
        node_id="node-managed-globals-strict-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_managed_globals_strict"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        with pytest.raises(ValueError, match="managed globals not found in entry module"):
            state.create_task_pool(
                owner_client_id="owner-strict",
                pool_name="pool-strict",
                sha256=f"sha256:{digest}",
                runtime="py3",
                entry_module="pool_strict_globals_demo",
                entry_callable="run",
                package_format="py",
                managed_global_names=["cfg"],
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                chunks=[blob],
            )
    finally:
        state.close()


def test_service_and_task_pool_with_same_code_version_keep_independent_managed_globals(tmp_path):
    state = NodeControlState(
        node_id="node-shared-code-managed-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_shared_managed"),
        enable_internal_executor=True,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        blob = (
            b"A = None\n"
            b"B = None\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(**_kwargs):\n"
            b"    return {'ok': True}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-service",
            service_name="svc-shared",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="shared_managed_demo",
            entry_callable="run",
            package_format="py",
            managed_global_names=["A"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-shared",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="shared_managed_demo",
            entry_callable="run",
            package_format="py",
            managed_global_names=["B"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )

        assert session.code_version == pool.code_version
        assert session.managed_global_names == ("A",)
        assert pool.managed_global_names == ("B",)
    finally:
        state.close()


def test_same_blob_with_different_export_specs_can_coexist(tmp_path):
    state = NodeControlState(
        node_id="node-code-cache-variants-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_variants"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': int(value)}\n\n"
            b"def alt(value=0, **_kwargs):\n"
            b"    return {'value': int(value) + 1}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        explicit_artifact, explicit_cached = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="cache_variant_demo",
            entry_callable="run",
            package_format="py",
            export_mode="explicit",
            export_methods=["alt"],
            chunks=[blob],
        )
        single_artifact, single_cached = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="cache_variant_demo",
            entry_callable="run",
            package_format="py",
            export_mode="single",
            export_methods=["run"],
            chunks=[blob],
        )

        assert explicit_cached is False
        assert single_cached is False
        assert explicit_artifact.code_version != single_artifact.code_version
        assert explicit_artifact.export_mode == "explicit"
        assert explicit_artifact.export_methods == ("alt",)
        assert single_artifact.export_mode == "single"
        assert single_artifact.export_methods == ("run",)

        failed_status, _failed_result, failed_type, failed_message, _ = _execute_payload_in_subprocess(
            **_build_execute_spec(
                explicit_artifact,
                object_dir=tmp_path / "objects",
                method_name="run",
                payload={"value": 5},
            )
        )
        assert failed_status == "FAILED_USER"
        assert failed_type == "RuntimeError"
        assert "method `run` not exported" in failed_message

        ok_status, ok_result, ok_type, ok_message, _ = _execute_payload_in_subprocess(
            **_build_execute_spec(
                single_artifact,
                object_dir=tmp_path / "objects",
                method_name="run",
                payload={"value": 5},
            )
        )
        assert ok_status == "SUCCEEDED"
        assert ok_result == {"value": 5}
        assert ok_type == ""
        assert ok_message == ""
    finally:
        state.close()


def test_infocenter_stale_node_degrades_service_route_status():
    state = InfoCenterState(lease_ttl_sec=1, heartbeat_interval_sec=1)
    state.register_node_record(
        node_id="node-stale",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=16,
        services={
            "svc-1": NodeServiceState(
                service_name="svc-stale",
                service_id="svc-1",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=2,
                alive_workers=2,
                in_flight=1,
            )
        },
    )
    state._nodes["node-stale"].last_seen_at = utc_now() - timedelta(seconds=5)  # noqa: SLF001

    routes = state.list_service_routes(service_name="svc-stale", healthy_only=False, limit=10)
    assert len(routes) == 1
    assert routes[0]["node_healthy"] is False
    assert routes[0]["stale"] is True
    assert routes[0]["status"] == pb2.SERVICE_STATUS_UNSPECIFIED
    assert routes[0]["status_text"] == "LOST"
    assert routes[0]["alive_workers"] == 0
    assert routes[0]["in_flight"] == 0
    assert state.list_service_routes(service_name="svc-stale", healthy_only=True, limit=10) == []


def test_infocenter_route_scopes_respect_drain_and_owner_semantics():
    state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    state.register_node_record(
        node_instance_id="node-drain",
        node_id="node-drain",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=16,
        services={
            "svc-drain": NodeServiceState(
                service_name="svc-demo",
                service_id="svc-drain",
                status=pb2.SERVICE_STATUS_RUNNING,
                owner_client_id="owner-a",
                code_version="sha256:aaa",
                policy_id="trusted_internal",
                worker_count=2,
                alive_workers=2,
                http_base_url="http://127.0.0.1:18081/svc/svc-drain",
            )
        },
    )
    state.update_node_schedule_state("node-drain", drain=True)

    call_routes = state.list_service_routes(
        service_name="svc-demo",
        healthy_only=True,
        limit=10,
        route_scope="call",
    )
    owner_routes = state.list_service_routes(
        service_name="svc-demo",
        healthy_only=True,
        limit=10,
        route_scope="owner_command",
    )
    exclusive_routes = state.list_service_routes(
        service_name="svc-demo",
        healthy_only=True,
        limit=10,
        route_scope="exclusive_check",
    )

    assert call_routes == []
    assert len(owner_routes) == 1
    assert len(exclusive_routes) == 1
    assert owner_routes[0]["node_drain"] is True
    assert owner_routes[0]["node_instance_id"] == "node-drain"
    assert owner_routes[0]["owner_client_id"] == "owner-a"
    assert owner_routes[0]["code_version"] == "sha256:aaa"
    assert owner_routes[0]["policy_id"] == "trusted_internal"


def test_infocenter_fences_stale_instance_and_rejects_heartbeat():
    state = InfoCenterState(lease_ttl_sec=1, heartbeat_interval_sec=1)
    state.register_node_record(
        node_instance_id="node-fenced",
        node_id="node-fenced",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=16,
    )
    state._nodes["node-fenced"].last_seen_at = utc_now() - timedelta(seconds=5)  # noqa: SLF001

    assert state.list_nodes(healthy_only=True, tags=(), limit=10) == []
    assert state.is_instance_fenced("node-fenced") is True
    assert (
        state.heartbeat_record(
            node_instance_id="node-fenced",
            node_id="node-fenced",
            healthy=True,
        )
        is None
    )


def test_infocenter_fenced_instance_cannot_re_register_but_new_instance_can():
    state = InfoCenterState(lease_ttl_sec=1, heartbeat_interval_sec=1)
    state.register_node_record(
        node_instance_id="node-old-inst",
        node_id="node-stable",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=16,
    )
    state.mark_node_lost("node-old-inst", reason="admin lost")

    with pytest.raises(ValueError, match="node_instance_id fenced"):
        state.register_node_record(
            node_instance_id="node-old-inst",
            node_id="node-stable",
            control_addr="127.0.0.1:50061",
            capacity=4,
            queue_capacity=16,
        )

    new_state = state.register_node_record(
        node_instance_id="node-new-inst",
        node_id="node-stable",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=16,
        services={},
        task_pools={},
        active_runtimes=[],
    )

    assert new_state.node_instance_id == "node-new-inst"
    assert new_state.node_id == "node-stable"
    assert new_state.services == {}
    assert new_state.task_pools == {}
    assert state.is_instance_fenced("node-old-inst") is True
    assert state.is_instance_fenced("node-new-inst") is False


def test_nodecontrol_tokens_are_bound_to_current_instance(tmp_path):
    state = NodeControlState(
        node_id="node-token-instance",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_token_instance"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    now = utc_now()
    try:
        service = ServiceSession(
            service_id="svc-token",
            owner_client_id="owner",
            service_name="svc-token",
            code_version="sha256:abc",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=True,
            service_token="service-token",
            http_base_url="",
            status=pb2.SERVICE_STATUS_RUNNING,
            created_at=now,
            last_heartbeat_at=now,
            lease_expire_at=now + timedelta(seconds=30),
            token_node_instance_id=state.node_instance_id,
        )
        pool = TaskPoolState(
            pool_id="pool-token",
            owner_client_id="owner",
            pool_name="pool-token",
            code_version="sha256:abc",
            task_method="run",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            pool_token="pool-token",
            status="RUNNING",
            created_at=now,
            last_heartbeat_at=now,
            lease_expire_at=now + timedelta(seconds=30),
            token_node_instance_id=state.node_instance_id,
        )

        state._require_service_token(service, "service-token")  # noqa: SLF001
        state._require_pool_token(pool, "pool-token")  # noqa: SLF001

        state.node_instance_id = f"{state.node_id}-replacement"
        with pytest.raises(PermissionError, match="node_instance_id mismatch"):
            state._require_service_token(service, "service-token")  # noqa: SLF001
        with pytest.raises(PermissionError, match="node_instance_id mismatch"):
            state._require_pool_token(pool, "pool-token")  # noqa: SLF001
    finally:
        state.close()


def test_nodecontrol_reset_fences_execution_until_process_restart(tmp_path):
    state = NodeControlState(
        node_id="node-reset-fence",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_reset_fence"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        assert state._executor_host is not None  # noqa: SLF001
        state.reset_execution_state(reason="test fence")
        assert state._executor_host is None  # noqa: SLF001
        assert state.execution_fenced is True
        assert state.can_accept_service_deploy is False
        snapshot = state.registrar_snapshot()
        assert snapshot["execution_fenced"] is True
        assert snapshot["accept_service_deploy"] is False

        with state._cv:  # noqa: SLF001
            state._ensure_executor_host_alive_locked()  # noqa: SLF001
        assert state._executor_host is None  # noqa: SLF001

        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        with pytest.raises(RuntimeError, match="execution is fenced"):
            state.create_service(
                owner_client_id="owner",
                service_name="svc-fenced",
                sha256=f"sha256:{digest}",
                runtime="py3",
                entry_module="svc_fenced",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=False,
                chunks=[blob],
            )
        with pytest.raises(RuntimeError, match="execution is fenced"):
            state.create_task_pool(
                owner_client_id="owner",
                pool_name="pool-fenced",
                sha256=f"sha256:{digest}",
                runtime="py3",
                entry_module="pool_fenced",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                chunks=[blob],
            )
    finally:
        state.close()


def test_nodecontrol_reset_closes_executor_with_fence_timeout(tmp_path):
    state = NodeControlState(
        node_id="node-reset-close-timeout",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_reset_close_timeout"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def drain_events(self):
            return []

        def close(self, **kwargs):
            calls.append(("close", kwargs))

    try:
        with state._lock:  # noqa: SLF001
            state._executor_host = _FakeExecutorHost()  # noqa: SLF001

        state.reset_execution_state(reason="test fence cleanup")

        assert calls == [("close", {"shutdown_timeout_sec": 8.0})]
        assert state._executor_host is None  # noqa: SLF001
        assert state.execution_fenced is True
    finally:
        state.close()


def test_startup_service_report_tracks_local_status_failure_and_recovery():
    from pycloud_parallel.controlplane.node_runtime_base import NodeRuntimeBase

    runtime = NodeRuntimeBase(
        node_id="startup-status-node",
        service_http_base_url="http://127.0.0.1:18080",
        accept_service_deploy=False,
    )
    status_mode = {"failed": True}

    def _invoke(*_args, **_kwargs):
        return 200, {"ok": True}

    def _methods(_include_docs):
        return 200, {"ok": True, "methods": []}

    def _status():
        if status_mode["failed"]:
            return 503, {"ok": False, "error": "worker process exited"}
        return 200, {
            "ok": True,
            "service": {
                "status": pb2.SERVICE_STATUS_RUNNING,
                "alive_workers": 1,
                "custom": "kept",
            },
        }

    mount = runtime.mount_startup_service(
        service_id="startup-svc",
        service_name="startup-svc",
        worker_count=1,
        invoke_handler=_invoke,
        methods_handler=_methods,
        status_handler=_status,
    )

    failed = runtime.startup_service_report_payloads()[0]
    assert failed["service_id"] == "startup-svc"
    assert failed["status"] == pb2.SERVICE_STATUS_STOPPED
    assert failed["status_text"] == "SERVICE_STATUS_STOPPED"
    assert failed["alive_workers"] == 0
    assert "worker process exited" in failed["stop_reason"]
    assert failed["failure_at"]
    assert mount.failure_at is not None

    status_mode["failed"] = False
    recovered = runtime.startup_service_report_payloads()[0]
    assert recovered["status"] == pb2.SERVICE_STATUS_RUNNING
    assert recovered["status_text"] == "SERVICE_STATUS_RUNNING"
    assert recovered["alive_workers"] == 1
    assert recovered["stop_reason"] == ""
    assert recovered["failure_at"] == ""

    code, body = runtime._status_mounted_startup_service("startup-svc")  # noqa: SLF001
    assert code == 200
    assert body["service"]["custom"] == "kept"
    assert body["service"]["status_text"] == "SERVICE_STATUS_RUNNING"


def test_owner_heartbeat_timeout_stops_only_target_service_and_reports_reason(tmp_path):
    state = NodeControlState(
        node_id="node-service-owner-timeout",
        queue_capacity=4,
        worker_capacity=2,
        service_worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_owner_timeout"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    now = utc_now()
    calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def stop_service(self, **kwargs):
            calls.append(("stop_service", kwargs))

        def service_worker_liveness(self):
            return {"svc-expired": 1, "svc-healthy": 1}

        def drain_events(self):
            return []

        def close(self, **kwargs):  # noqa: ARG002
            return None

    expired = ServiceSession(
        service_id="svc-expired",
        owner_client_id="owner",
        service_name="svc-expired",
        code_version="sha256:expired",
        worker_count=1,
        heartbeat_timeout_sec=5,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token-expired",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now - timedelta(seconds=10),
        lease_expire_at=now - timedelta(seconds=1),
        executor_ready=True,
        alive_workers=1,
        methods={"run": ("demo", "run")},
    )
    healthy = ServiceSession(
        service_id="svc-healthy",
        owner_client_id="owner",
        service_name="svc-healthy",
        code_version="sha256:healthy",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token-healthy",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=1,
        methods={"run": ("demo", "run")},
    )
    with state._lock:  # noqa: SLF001
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        state._services[expired.service_id] = expired  # noqa: SLF001
        state._services[healthy.service_id] = healthy  # noqa: SLF001

    try:
        state._handle_service_timeouts()  # noqa: SLF001

        assert expired.status == pb2.SERVICE_STATUS_STOPPED
        assert expired.stop_reason == "owner heartbeat timeout"
        assert expired.alive_workers == 0
        assert healthy.status == pb2.SERVICE_STATUS_RUNNING
        assert state.execution_fenced is False
        assert state.can_accept_service_deploy is True
        assert state.service_worker_used() == 1
        assert calls == [("stop_service", {"service_id": "svc-expired", "reason": "owner heartbeat timeout"})]

        code, body = state.call_service(
            service_id=expired.service_id,
            method="run",
            payload={},
            service_token=expired.service_token,
            timeout_sec=1.0,
        )
        assert code == 409
        assert body["error"] == "owner heartbeat timeout"
        assert body["stop_reason"] == "owner heartbeat timeout"
    finally:
        state.close()


def test_service_zero_alive_workers_stops_only_that_service(tmp_path):
    state = NodeControlState(
        node_id="node-service-zero-workers",
        queue_capacity=4,
        worker_capacity=2,
        service_worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_zero_workers"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    now = utc_now()
    calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def stop_service(self, **kwargs):
            calls.append(("stop_service", kwargs))

        def service_worker_liveness(self):
            return {"svc-dead-workers": 0, "svc-healthy": 1}

        def drain_events(self):
            return []

        def close(self, **kwargs):  # noqa: ARG002
            return None

    dead_workers = ServiceSession(
        service_id="svc-dead-workers",
        owner_client_id="owner",
        service_name="svc-dead-workers",
        code_version="sha256:dead",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token-dead",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=0,
        methods={"run": ("demo", "run")},
    )
    healthy = ServiceSession(
        service_id="svc-still-running",
        owner_client_id="owner",
        service_name="svc-still-running",
        code_version="sha256:healthy",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token-healthy",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=1,
        methods={"run": ("demo", "run")},
    )
    with state._lock:  # noqa: SLF001
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        state._services[dead_workers.service_id] = dead_workers  # noqa: SLF001
        state._services[healthy.service_id] = healthy  # noqa: SLF001

    try:
        for _idx in range(3):
            state._handle_service_timeouts()  # noqa: SLF001

        assert dead_workers.status == pb2.SERVICE_STATUS_STOPPED
        assert "service worker unavailable" in dead_workers.stop_reason
        assert healthy.status == pb2.SERVICE_STATUS_RUNNING
        assert state.execution_fenced is False
        assert state.can_accept_service_deploy is True
        assert state.service_worker_used() == 1
        assert calls == [
            (
                "stop_service",
                {
                    "service_id": "svc-dead-workers",
                    "reason": "service worker unavailable; owner should redeploy or compensate; alive_workers=0 worker_count=1",
                },
            )
        ]
    finally:
        state.close()


def test_service_worker_liveness_drives_degraded_then_stopped(tmp_path):
    state = NodeControlState(
        node_id="node-service-liveness",
        queue_capacity=4,
        worker_capacity=2,
        service_worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_liveness"),
        enable_internal_executor=False,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    now = utc_now()
    calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def service_worker_liveness(self):
            return {"svc-live-probe": 0, "svc-other": 1}

        def stop_service(self, **kwargs):
            calls.append(("stop_service", kwargs))

        def drain_events(self):
            return []

        def close(self, **kwargs):  # noqa: ARG002
            return None

    probe = ServiceSession(
        service_id="svc-live-probe",
        owner_client_id="owner",
        service_name="svc-live-probe",
        code_version="sha256:probe",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token-probe",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=1,
        methods={"run": ("demo", "run")},
    )
    other = ServiceSession(
        service_id="svc-other",
        owner_client_id="owner",
        service_name="svc-other",
        code_version="sha256:other",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token-other",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=1,
        methods={"run": ("demo", "run")},
    )
    with state._lock:  # noqa: SLF001
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        state._services[probe.service_id] = probe  # noqa: SLF001
        state._services[other.service_id] = other  # noqa: SLF001

    try:
        state._handle_service_timeouts()  # noqa: SLF001
        assert probe.status == pb2.SERVICE_STATUS_RUNNING
        assert probe.alive_workers == 0
        assert probe.degraded is True
        assert other.status == pb2.SERVICE_STATUS_RUNNING
        assert other.alive_workers == 1

        state._handle_service_timeouts()  # noqa: SLF001
        state._handle_service_timeouts()  # noqa: SLF001
        assert probe.status == pb2.SERVICE_STATUS_STOPPED
        assert "service worker unavailable" in probe.stop_reason
        assert other.status == pb2.SERVICE_STATUS_RUNNING
        assert calls
    finally:
        state.close()


def test_startup_managed_service_zero_workers_recovers_executor(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-startup-recover",
        queue_capacity=4,
        worker_capacity=1,
        service_worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_startup_recover"),
        enable_internal_executor=False,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    now = utc_now()
    create_calls = []
    stop_calls = []
    code_version = "sha256:" + "e" * 64
    artifact = CodeArtifact(
        code_version=code_version,
        path=str(tmp_path),
        runtime="py3",
        entry_module="startup_recover",
        entry_callable="run",
        package_format="py",
        export_mode="module",
        export_methods=(),
        export_decorator="",
        dependency_policy_mode="safe",
        dependency_allowlist=(),
        dependency_path="",
        size_bytes=1,
        created_at=now,
    )

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def service_worker_liveness(self):
            return {"svc-startup-recover": 0}

        def create_service(self, **kwargs):
            create_calls.append(kwargs)

        def stop_service(self, **kwargs):
            stop_calls.append(kwargs)

        def drain_events(self):
            return []

        def close(self, **kwargs):  # noqa: ARG002
            return None

    service = ServiceSession(
        service_id="svc-startup-recover",
        owner_client_id="owner",
        service_name="svc-startup-recover",
        code_version=code_version,
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token-startup",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=0,
        methods={"run": ("startup_recover", "run")},
        node_managed=True,
    )
    monkeypatch.setattr(state, "_get_live_code_artifact_locked", lambda _code_version: artifact)
    monkeypatch.setattr(state, "_ensure_artifact_ready", lambda *args, **kwargs: None)
    with state._lock:  # noqa: SLF001
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        state._services[service.service_id] = service  # noqa: SLF001

    try:
        state._handle_service_timeouts()  # noqa: SLF001
        state._handle_service_timeouts()  # noqa: SLF001
        state._handle_service_timeouts()  # noqa: SLF001

        assert create_calls == [{"service_id": "svc-startup-recover", "worker_count": 1}]
        assert service.status == pb2.SERVICE_STATUS_RUNNING
        assert service.executor_ready is True
        assert service.alive_workers == 1
        assert service.degraded is False
        assert service.stop_reason == ""
        assert stop_calls == []
    finally:
        state.close()


def test_stop_service_cleanup_failure_blocks_deploy(tmp_path):
    state = NodeControlState(
        node_id="node-service-cleanup-fail",
        queue_capacity=4,
        worker_capacity=1,
        service_worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_cleanup_fail"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    now = utc_now()

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def service_worker_liveness(self):
            return {"svc-cleanup-fail": 0}

        def stop_service(self, **kwargs):  # noqa: ARG002
            raise RuntimeError("taskkill denied")

        def drain_events(self):
            return []

        def close(self, **kwargs):  # noqa: ARG002
            return None

    service = ServiceSession(
        service_id="svc-cleanup-fail",
        owner_client_id="owner",
        service_name="svc-cleanup-fail",
        code_version="sha256:cleanup",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token-cleanup",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=0,
        methods={"run": ("demo", "run")},
    )
    with state._lock:  # noqa: SLF001
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        state._services[service.service_id] = service  # noqa: SLF001

    try:
        state._handle_service_timeouts()  # noqa: SLF001
        state._handle_service_timeouts()  # noqa: SLF001
        state._handle_service_timeouts()  # noqa: SLF001

        assert service.status == pb2.SERVICE_STATUS_STOPPED
        assert state.can_accept_service_deploy is False
        assert "service cleanup failed service_id=svc-cleanup-fail" in state.deploy_health_reason
        assert "taskkill denied" in state.deploy_health_reason
        snapshot = state.registrar_snapshot()
        assert snapshot["accept_service_deploy"] is False
        assert "svc-cleanup-fail" in snapshot["deploy_health_reason"]
    finally:
        state.close()


def test_executor_host_crash_disables_service_deploy_without_fencing_node(tmp_path):
    state = NodeControlState(
        node_id="node-executor-crash-deploy-health",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_executor_crash"),
        enable_internal_executor=False,
        enable_service_session=False,
    )

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def drain_events(self):
            return [
                {
                    "kind": "executor_host_crash",
                    "error_type": "RuntimeError",
                    "error": "executor crashed",
                    "traceback": "trace",
                }
            ]

        def close(self, **kwargs):  # noqa: ARG002
            return None

    with state._lock:  # noqa: SLF001
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001

    try:
        state._drain_executor_events()  # noqa: SLF001
        snapshot = state.registrar_snapshot()

        assert state.execution_fenced is False
        assert state.can_accept_service_deploy is False
        assert "executor host crashed" in state.deploy_health_reason
        assert snapshot["accept_service_deploy"] is False
        assert "executor host crashed" in snapshot["deploy_health_reason"]
    finally:
        state.close()


def test_executor_host_rebuild_restores_service_deploy_health(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-executor-rebuild-deploy-health",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_executor_rebuild"),
        enable_internal_executor=False,
        enable_service_session=True,
    )
    created = []

    class _DeadExecutorHost:
        def is_alive(self):
            return False

        def close(self, **kwargs):  # noqa: ARG002
            return None

    class _FreshExecutorHost:
        def is_alive(self):
            return True

        def drain_events(self):
            return []

        def close(self, **kwargs):  # noqa: ARG002
            return None

    monkeypatch.setattr(state, "_create_executor_backend", lambda: created.append(True) or _FreshExecutorHost())
    with state._lock:  # noqa: SLF001
        state._executor_host = _DeadExecutorHost()  # noqa: SLF001
        state._set_deploy_health_block_locked("executor host crashed: RuntimeError old")  # noqa: SLF001

    try:
        with state._cv:  # noqa: SLF001
            rebuilt = state._ensure_executor_host_alive_locked()  # noqa: SLF001

        assert rebuilt is True
        assert created == [True]
        assert state.can_accept_service_deploy is True
        assert state.deploy_health_reason == ""
        assert state.registrar_snapshot()["accept_service_deploy"] is True
    finally:
        state.close()


def test_service_create_rejects_when_deploy_health_blocked(tmp_path):
    state = NodeControlState(
        node_id="node-create-deploy-blocked",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_create_blocked"),
        enable_internal_executor=False,
        enable_service_session=True,
    )
    blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
    digest = hashlib.sha256(blob).hexdigest()
    with state._lock:  # noqa: SLF001
        state._set_deploy_health_block_locked("executor host crashed: blocked")  # noqa: SLF001

    try:
        with pytest.raises(RuntimeError, match="executor host crashed: blocked"):
            state.create_service(
                owner_client_id="owner",
                service_name="svc-blocked",
                sha256=f"sha256:{digest}",
                runtime="py3",
                entry_module="svc_blocked",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=False,
                chunks=[blob],
            )
    finally:
        state.close()


def test_nodecontrol_close_stops_service_and_taskpool_executors(tmp_path):
    state = NodeControlState(
        node_id="node-close-executors",
        queue_capacity=4,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_close_executors"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    now = utc_now()
    calls = []

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def stop_service(self, **kwargs):
            calls.append(("stop_service", kwargs))

        def stop_task_pool(self, **kwargs):
            calls.append(("stop_task_pool", kwargs))

        def drain_events(self):
            return []

        def close(self, **kwargs):
            calls.append(("close", kwargs))

    service = ServiceSession(
        service_id="svc-close",
        owner_client_id="owner",
        service_name="svc-close",
        code_version="sha256:svc",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="svc-token",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=1,
        methods={},
    )
    pool = TaskPoolState(
        pool_id="pool-close",
        owner_client_id="owner",
        pool_name="pool-close",
        code_version="sha256:pool",
        task_method="run",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        pool_token="pool-token",
        status="RUNNING",
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=1,
    )

    with state._lock:  # noqa: SLF001
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        state._services[service.service_id] = service  # noqa: SLF001
        state._task_pools[pool.pool_id] = pool  # noqa: SLF001

    state.close()

    assert ("stop_service", {"service_id": "svc-close", "reason": "nodecontrol shutdown"}) in calls
    assert ("stop_task_pool", {"pool_id": "pool-close"}) in calls
    assert ("close", {"shutdown_timeout_sec": 2.0}) in calls
    assert service.status == pb2.SERVICE_STATUS_STOPPED
    assert service.stop_reason == "nodecontrol shutdown"
    assert pool.status == "STOPPED"
    assert pool.stop_reason == "nodecontrol shutdown"
    assert state._executor_host is None  # noqa: SLF001


def test_close_task_pool_preserves_existing_stop_reason(tmp_path):
    state = NodeControlState(
        node_id="node-close-preserve",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_close_preserve"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    now = utc_now()
    pool = TaskPoolState(
        pool_id="pool-preserve",
        owner_client_id="owner",
        pool_name="pool-preserve",
        code_version="sha256:pool",
        task_method="run",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        pool_token="pool-token",
        status="STOPPED",
        stop_reason="owner heartbeat timeout",
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now,
        executor_ready=False,
        alive_workers=0,
    )
    with state._lock:  # noqa: SLF001
        state._task_pools[pool.pool_id] = pool  # noqa: SLF001

    state.close_task_pool(
        owner_client_id="owner",
        pool_id="pool-preserve",
        pool_token="pool-token",
        reason="task pool session close",
    )

    assert pool.status == "STOPPED"
    assert pool.stop_reason == "owner heartbeat timeout"


def test_nodecontrol_service_call_throttles_code_last_at_touch(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-touch-throttle",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_touch_throttle"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    now = utc_now()
    code_version = "sha256:" + "a" * 64
    artifact = CodeArtifact(
        code_version=code_version,
        path=str(tmp_path),
        runtime="py3",
        entry_module="demo_service",
        entry_callable="run",
        package_format="py",
        export_mode="module",
        export_methods=(),
        export_decorator="",
        dependency_policy_mode="safe",
        dependency_allowlist=(),
        dependency_path="",
        size_bytes=1,
        created_at=now,
    )
    service = ServiceSession(
        service_id="svc-touch",
        owner_client_id="owner",
        service_name="svc-touch",
        code_version=code_version,
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=1,
        methods={"run": ("demo_service", "run")},
    )

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def call_service(self, **kwargs):  # noqa: ARG002
            return {"ok": True, "status_text": "SUCCEEDED", "result": {"value": 1}}

        def drain_events(self):
            return []

        def close(self, **kwargs):  # noqa: ARG002
            return None

    calls = []
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.nodecontrol_state.touch_code_last_at",
        lambda artifact_dir, *, code_version: calls.append((artifact_dir, code_version)),
    )
    with state._lock:  # noqa: SLF001
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        state._codes[code_version] = artifact  # noqa: SLF001
        state._services[service.service_id] = service  # noqa: SLF001

    try:
        first_code, first_body = state.call_service(
            service_id=service.service_id,
            method="run",
            payload={},
            service_token=service.service_token,
            timeout_sec=1.0,
        )
        second_code, second_body = state.call_service(
            service_id=service.service_id,
            method="run",
            payload={},
            service_token=service.service_token,
            timeout_sec=1.0,
        )
    finally:
        state.close()

    assert first_code == 200
    assert second_code == 200
    assert first_body["ok"] is True
    assert second_body["ok"] is True
    assert len(calls) == 1


def test_nodecontrol_service_stream_reports_stop_reason_from_inflight_stop(tmp_path):
    state = NodeControlState(
        node_id="node-stream-stop-reason",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_stream_stop_reason"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    now = utc_now()
    code_version = "sha256:" + "f" * 64
    artifact = CodeArtifact(
        code_version=code_version,
        path=str(tmp_path),
        runtime="py3",
        entry_module="stream_stop_service",
        entry_callable="run",
        package_format="py",
        export_mode="module",
        export_methods=(),
        export_decorator="",
        dependency_policy_mode="safe",
        dependency_allowlist=(),
        dependency_path="",
        size_bytes=1,
        created_at=now,
    )
    service = ServiceSession(
        service_id="svc-stream-stop",
        owner_client_id="owner",
        service_name="svc-stream-stop",
        code_version=code_version,
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token-stream",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=1,
        methods={"run": ("stream_stop_service", "run")},
    )

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def call_service_stream(self, **kwargs):  # noqa: ARG002
            yield {
                "kind": "service_stream_done",
                "status_text": "FAILED_INFRA",
                "err_type": "ServiceStopped",
                "err_message": "owner heartbeat timeout",
                "result": {"item_count": 0},
            }

        def drain_events(self):
            return []

        def close(self, **kwargs):  # noqa: ARG002
            return None

    with state._lock:  # noqa: SLF001
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        state._codes[code_version] = artifact  # noqa: SLF001
        state._services[service.service_id] = service  # noqa: SLF001

    try:
        response = state._invoke_service_stream_http(  # noqa: SLF001
            service_id=service.service_id,
            method="run",
            payload={},
            service_token=service.service_token,
            timeout_sec=1.0,
        )
        lines = [json.loads(line.decode("utf-8")) for line in response.body_iter]
    finally:
        state.close()

    assert lines == [
        {
            "event": "done",
            "ok": False,
            "item_count": 0,
            "error_type": "ServiceStopped",
            "error": "owner heartbeat timeout",
        }
    ]


def test_nodecontrol_service_call_ignores_code_last_at_touch_permission_error(tmp_path, monkeypatch):
    state = NodeControlState(
        node_id="node-touch-permission",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_touch_permission"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    now = utc_now()
    code_version = "sha256:" + "b" * 64
    artifact = CodeArtifact(
        code_version=code_version,
        path=str(tmp_path),
        runtime="py3",
        entry_module="demo_service",
        entry_callable="run",
        package_format="py",
        export_mode="module",
        export_methods=(),
        export_decorator="",
        dependency_policy_mode="safe",
        dependency_allowlist=(),
        dependency_path="",
        size_bytes=1,
        created_at=now,
    )
    service = ServiceSession(
        service_id="svc-touch-permission",
        owner_client_id="owner",
        service_name="svc-touch-permission",
        code_version=code_version,
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        service_token="token",
        http_base_url="",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        alive_workers=1,
        methods={"run": ("demo_service", "run")},
    )

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def call_service(self, **kwargs):  # noqa: ARG002
            return {"ok": True, "status_text": "SUCCEEDED", "result": {"value": 2}}

        def drain_events(self):
            return []

        def close(self, **kwargs):  # noqa: ARG002
            return None

    def _raise_permission_error(*args, **kwargs):  # noqa: ARG001
        raise PermissionError("locked")

    monkeypatch.setattr("pycloud_parallel.controlplane.nodecontrol_state.touch_code_last_at", _raise_permission_error)
    with state._lock:  # noqa: SLF001
        state._executor_host = _FakeExecutorHost()  # noqa: SLF001
        state._codes[code_version] = artifact  # noqa: SLF001
        state._services[service.service_id] = service  # noqa: SLF001

    try:
        code, body = state.call_service(
            service_id=service.service_id,
            method="run",
            payload={},
            service_token=service.service_token,
            timeout_sec=1.0,
        )
    finally:
        state.close()

    assert code == 200
    assert body["ok"] is True
    assert body["data"] == {"value": 2}


def test_nodecontrol_discards_late_pool_event_for_stale_pool_id(tmp_path):
    state = NodeControlState(
        node_id="node-late-pool-event",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_late_pool_event"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    now = utc_now()

    class _FakeExecutorHost:
        def is_alive(self):
            return True

        def drain_events(self):
            return [
                {
                    "kind": "pool_task_done",
                    "pool_id": "old-pool",
                    "task_id": "task-reused",
                    "attempt": 1,
                    "status_text": "SUCCEEDED",
                    "result": {"value": "stale"},
                }
            ]

        def close(self, **_kwargs):
            pass

    try:
        pool = TaskPoolState(
            pool_id="new-pool",
            owner_client_id="owner",
            pool_name="new-pool",
            code_version="sha256:abc",
            task_method="run",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            pool_token="pool-token",
            status="RUNNING",
            created_at=now,
            last_heartbeat_at=now,
            lease_expire_at=now + timedelta(seconds=30),
            token_node_instance_id=state.node_instance_id,
        )
        task = TaskState(
            task_id="task-reused",
            client_id="new-pool",
            job_id="job-new",
            code_version="sha256:abc",
            runtime_key="",
            execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
            payload={},
            timeout_hint_sec=0,
            priority=1,
            status=pb2.TASK_STATUS_RUNNING,
            attempt=1,
            started_at=now,
            last_heartbeat_at=now,
        )
        with state._cv:  # noqa: SLF001
            state._executor_host = _FakeExecutorHost()  # noqa: SLF001
            state._task_pools[pool.pool_id] = pool  # noqa: SLF001
            state._pool_tasks[task.task_id] = task  # noqa: SLF001

        state._drain_executor_events()  # noqa: SLF001

        assert task.status == pb2.TASK_STATUS_RUNNING
        assert task.result is None
        assert pool.returned_count == 0
        results, _cursor = state.pull_pool_results(
            pool_id=pool.pool_id,
            pool_token=pool.pool_token,
            limit=10,
            wait_ms=0,
            cursor="",
        )
        assert results == []
    finally:
        state.close()


def test_infocenter_service_routes_compute_inflight_and_predicted_busy():
    state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    state.register_node_record(
        node_instance_id="node-high-inflight",
        node_id="node-high-inflight",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=16,
        services={
            "svc-high-inflight": NodeServiceState(
                service_name="svc-busy",
                service_id="svc-high-inflight",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=2,
                alive_workers=2,
                in_flight=99,
                received_count=12,
                returned_count=2,
                ema_child_invoke_ms=2.0,
                ema_samples=10,
            )
        },
    )
    state.register_node_record(
        node_instance_id="node-low-inflight",
        node_id="node-low-inflight",
        control_addr="127.0.0.1:50062",
        capacity=4,
        queue_capacity=16,
        services={
            "svc-low-inflight": NodeServiceState(
                service_name="svc-busy",
                service_id="svc-low-inflight",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=2,
                alive_workers=2,
                in_flight=1,
                received_count=4,
                returned_count=2,
                ema_child_invoke_ms=20.0,
                ema_samples=10,
            )
        },
    )

    routes = state.list_service_routes(service_name="svc-busy", healthy_only=True, limit=10)

    assert [item["service_id"] for item in routes] == ["svc-high-inflight", "svc-low-inflight"]
    assert routes[0]["in_flight"] == 10
    assert routes[0]["reported_in_flight"] == 99
    assert routes[0]["predicted_busy"] == pytest.approx(10.0)
    assert routes[1]["in_flight"] == 2
    assert routes[1]["predicted_busy"] == pytest.approx(20.0)


def test_infocenter_service_routes_fallback_to_reported_inflight_and_worker_normalized_busy():
    state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    state.register_node_record(
        node_instance_id="node-legacy",
        node_id="node-legacy",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=16,
        services={
            "svc-legacy": NodeServiceState(
                service_name="svc-legacy",
                service_id="svc-legacy",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=3,
                alive_workers=3,
                in_flight=6,
                ema_child_invoke_ms=999.0,
                ema_samples=5,
            )
        },
    )

    routes = state.list_service_routes(service_name="svc-legacy", healthy_only=True, limit=10)

    assert len(routes) == 1
    assert routes[0]["in_flight"] == 6
    assert routes[0]["reported_in_flight"] == 6
    assert routes[0]["ema_child_invoke_ms"] == pytest.approx(999.0)
    assert routes[0]["ema_samples"] == 5
    assert routes[0]["predicted_busy"] == pytest.approx(2.0)


def test_service_session_keepalive_fails_fast_after_consecutive_errors():
    class _FailingClient:
        def heartbeat_service(self, **_kwargs):
            raise RuntimeError("heartbeat unavailable")

    session = ServiceSessionClient(
        _client=_FailingClient(),
        owner_client_id="owner-x",
        service_id="svc-x",
        service_token="token-x",
        http_base_url="",
        heartbeat_timeout_sec=1,
        worker_count=1,
        status=pb2.SERVICE_STATUS_RUNNING,
        heartbeat_failure_threshold=2,
    )
    group = Service(
        owner_client_id="owner-x",
        service_name="svc-x",
        sessions={"node-1": session},
        nodes={},
    )

    group._start_keepalive(interval_sec=0.05)
    start = time.monotonic()
    group.join(poll_interval_sec=0.05)
    elapsed = time.monotonic() - start

    assert elapsed < 2.0
    assert session.failed is True
    assert session.status == pb2.SERVICE_STATUS_STOPPED
    assert "heartbeat unavailable" in session.last_error
    assert "node-1" in group.failures


def test_service_call_recovers_after_executor_host_restart(tmp_path):
    state = NodeControlState(
        node_id="node-svc-host-restart-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=1,
    )
    try:
        blob = (
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(value=0, **_kwargs):\n"
            b"    v = int(value)\n"
            b"    return {'v': v, 'square': v * v}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-host-restart",
            service_name="svc-host-restart",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_host_restart",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )

        assert state._executor_host is not None  # noqa: SLF001
        host_client = state._executor_host._service_clients[session.service_id]  # noqa: SLF001
        host_client._process.terminate()  # noqa: SLF001
        host_client._process.join(timeout=5.0)  # noqa: SLF001

        code, body = state.call_service(
            service_id=session.service_id,
            method="run",
            payload={"value": 8},
            service_token=session.service_token,
            timeout_sec=5.0,
        )
        assert code == 200
        assert body["ok"] is True
        assert body["data"] == {"v": 8, "square": 64}
    finally:
        state.close()


def test_service_create_does_not_keep_package_module_in_parent(tmp_path):
    state = NodeControlState(
        node_id="node-svc-03",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        blob = (
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(**_kwargs):\n"
            b"    return {'ok': True}\n"
        )

        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("compute_service/__init__.py", "")
            zf.writestr("compute_service/main.py", blob)
        archive = buf.getvalue()
        digest = hashlib.sha256(archive).hexdigest()

        session = state.create_service(
            owner_client_id="owner-c",
            service_name="svc-c",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="compute_service.main",
            entry_callable="run",
            package_format="zip",
            export_mode="decorator",
            export_methods=(),
            export_decorator="pycloud_export",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=True,
            chunks=[archive],
        )

        assert session.status == pb2.SERVICE_STATUS_RUNNING
        assert "compute_service" not in sys.modules
        assert "compute_service.main" not in sys.modules
    finally:
        state.close()


def test_extract_archive_tar_gz_avoids_extractall_deprecation_warning(tmp_path):
    state = NodeControlState(
        node_id="node-extract-tar-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_extract_tar"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        archive_path = tmp_path / "artifact.tar.gz"
        file_bytes = b"VALUE = 1\n"
        with tarfile.open(archive_path, "w:gz") as tf:
            info = tarfile.TarInfo("demo_pkg/__init__.py")
            info.size = len(file_bytes)
            tf.addfile(info, io.BytesIO(file_bytes))

        out_dir = tmp_path / "out_tar"
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            state._extract_archive(archive_path=archive_path, package_format="tar.gz", out_dir=out_dir)  # noqa: SLF001

        assert (out_dir / "demo_pkg" / "__init__.py").read_bytes() == file_bytes
        assert not any("extractall" in str(item.message).lower() for item in caught)
    finally:
        state.close()


def test_extract_archive_tar_gz_rejects_symlink_entries(tmp_path):
    state = NodeControlState(
        node_id="node-extract-tar-02",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_extract_tar_symlink"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        archive_path = tmp_path / "artifact_symlink.tar.gz"
        with tarfile.open(archive_path, "w:gz") as tf:
            link = tarfile.TarInfo("demo_pkg/link")
            link.type = tarfile.SYMTYPE
            link.linkname = "../outside"
            tf.addfile(link)

        with pytest.raises(ValueError, match="unsupported link entry"):
            state._extract_archive(archive_path=archive_path, package_format="tar.gz", out_dir=tmp_path / "out_symlink")  # noqa: SLF001
    finally:
        state.close()
