"""中文说明：验证 gRPC 控制面的核心状态流转（内存后端）。"""

import hashlib
import inspect
import io
import json
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

import pytest

from pycloud_parallel.controlplane.client import ServiceGroup, ServiceSessionClient
from pycloud_parallel.controlplane import serialization as serialization_mod
from pycloud_parallel.controlplane.state import (
    NodeControlState,
    _build_execute_spec,
    _code_content_dir,
    _code_data_dir,
    _code_index_link_path,
    _code_pkg_dir,
    _code_variant_dir,
    _commit_result_file,
    _execute_payload_in_subprocess,
    _normalize_user_return,
    _resolve_object_refs_in_payload,
    dict_to_struct,
    struct_to_dict,
    utc_now,
)
from pycloud_parallel.controlplane.state import InfoCenterState, NodeServiceState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

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


def test_normalize_user_return_inlines_dataframe_when_limit_allows(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")

    monkeypatch.setattr(serialization_mod, "INLINE_RESULT_HARD_LIMIT_BYTES", 8 * 1024 * 1024)
    frame = pd.DataFrame([{"x": 1}, {"x": 2}])
    status, result, err_type, err_message = _normalize_user_return(frame, object_dir=str(tmp_path))

    assert status == "SUCCEEDED"
    assert err_type == ""
    assert err_message == ""
    assert not isinstance(result, dict) or "__pycloud_result_ref__" not in result
    restored = struct_to_dict(dict_to_struct({"frame": result}))
    pd.testing.assert_frame_equal(restored["frame"], frame)


def test_normalize_user_return_spills_dataframe_when_limit_too_small(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    import pycloud_parallel.controlplane.state as state_mod
    from pycloud_parallel.controlplane.state import StoredResultArtifact

    def _raise_inline_limit(*args, **kwargs):
        raise ValueError("inline result too large")

    monkeypatch.setattr(state_mod, "serialize_inline_result", _raise_inline_limit)
    frame = pd.DataFrame([{"x": idx, "y": "a" * 50} for idx in range(10)])
    status, result, _err_type, _err_message = _normalize_user_return(frame, object_dir=str(tmp_path))

    assert status == "SUCCEEDED"
    assert isinstance(result, StoredResultArtifact)


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

    from pycloud_parallel.controlplane.client import _serialize_data_for_object_ref
    from pycloud_parallel.controlplane.client import _materialize_downloaded_result
    from pycloud_parallel.controlplane.result_ref import ResultRef

    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
        names=["trade_date", "bucket"],
    )
    frame = pd.DataFrame([[1, 2], [3, 4]], index=index, columns=[10006, 10007])

    kind, fmt, blob = _serialize_data_for_object_ref(frame, format="parquet")
    import tempfile
    from pathlib import Path

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
            result_ref=ResultRef(
                object_id="sha256:" + "b" * 64,
                node_id="node-1",
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


def test_series_object_upload_preserves_index_and_name():
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    from pycloud_parallel.controlplane.client import _materialize_downloaded_result
    from pycloud_parallel.controlplane.client import _serialize_data_for_object_ref
    from pycloud_parallel.controlplane.result_ref import ResultRef

    series = pd.Series(
        [1.1, 2.2],
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
            names=["trade_date", "bucket"],
        ),
        name=10006,
    )

    kind, fmt, blob = _serialize_data_for_object_ref(series)

    import os
    import tempfile
    from pathlib import Path

    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-seriesbundle-", suffix=".seriesbundle")
    os.close(fd)
    Path(tmp_name).write_bytes(blob)
    try:
        restored = _materialize_downloaded_result(
            Path(tmp_name),
            result_ref=ResultRef(
                object_id="sha256:" + "c" * 64,
                node_id="node-1",
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


def test_json_object_upload_roundtrip_supports_nested_series():
    pd = pytest.importorskip("pandas")

    from pycloud_parallel.controlplane.client import _serialize_data_for_object_ref
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

    from pycloud_parallel.controlplane.client import _serialize_data_for_object_ref
    from pycloud_parallel.controlplane.object_ref import ObjectRef
    from pycloud_parallel.controlplane.object_ref import object_storage_path
    from pycloud_parallel.controlplane.state import _resolve_object_refs_in_payload

    frame = pd.DataFrame(
        [[1, 2], [3, 4]],
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
            names=["trade_date", "bucket"],
        ),
        columns=[10006, 10007],
    )
    _kind, fmt, blob = _serialize_data_for_object_ref(frame)
    object_id = "sha256:" + "d" * 64
    path = object_storage_path(tmp_path, object_id=object_id, fmt=fmt)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)

    payload = {
        "frame": ObjectRef(
            object_id=object_id,
            format=fmt,
            size_bytes=len(blob),
            materialize_as="dataframe",
        )
    }

    restored = _resolve_object_refs_in_payload(payload, object_dir=str(tmp_path))
    pd.testing.assert_frame_equal(restored["frame"], frame)


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
    assert isinstance(restored, pd.DataFrame)
    assert restored.shape == (2, 1)
    assert math.isnan(float(restored.iloc[1, 0]))


def test_put_object_from_uploaded_file_uses_segment_backend_for_medium_objects(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane import state as state_mod

    monkeypatch.setattr(state_mod, "OBJECT_SEGMENT_MAX_BYTES", 1024)
    monkeypatch.setattr(state_mod, "OBJECT_SEGMENT_TARGET_BYTES", 4096)
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


def test_resolve_object_refs_reads_segment_backend(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.object_ref import ObjectRef
    from pycloud_parallel.controlplane import state as state_mod

    monkeypatch.setattr(state_mod, "OBJECT_SEGMENT_MAX_BYTES", 1024)
    monkeypatch.setattr(state_mod, "OBJECT_SEGMENT_TARGET_BYTES", 4096)
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
            "item": ObjectRef(
                object_id=object_id,
                format="json",
                size_bytes=len(blob),
                materialize_as="json",
                consume_on_read=True,
            )
        }
        resolved = _resolve_object_refs_in_payload(payload, object_dir=str(node_state.object_dir))
        assert resolved == {"item": {"x": 1}}
    finally:
        node_state.close()


def test_put_object_from_uploaded_file_uses_file_backend_for_large_objects(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane import state as state_mod

    monkeypatch.setattr(state_mod, "OBJECT_SEGMENT_MAX_BYTES", 128)
    monkeypatch.setattr(state_mod, "OBJECT_SEGMENT_TARGET_BYTES", 4096)
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
        assert float(timing.get("last_child_encode_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_invoke_ms", 0.0) or 0.0) == float(timing.get("last_child_invoke_ms", 0.0) or 0.0)

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
        assert float(timing.get("last_child_encode_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_invoke_ms", 0.0) or 0.0) == float(timing.get("last_child_invoke_ms", 0.0) or 0.0)
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
    original = sys.modules["pycloud_parallel.controlplane.state"]._load_user_module

    def wrapped(*args, **kwargs):
        calls["count"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr("pycloud_parallel.controlplane.state._load_user_module", wrapped)
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
        assert warmup["fanout"] == 4
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
        pool.managed_globals_digest = globals_digest

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


def test_normalize_warmup_result_accepts_submitted_count_only(tmp_path):
    state = NodeControlState(
        node_id="node-warmup-normalize",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_warmup_normalize"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        submitted, worker_pids = state._normalize_warmup_result(4, fanout=8)  # noqa: SLF001
        assert submitted == 4
        assert worker_pids == []
    finally:
        state.close()


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
        assert warmup["fanout"] == 4
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


def test_prepare_http_payload_for_call_objectifies_large_values(monkeypatch):
    from pycloud_parallel.controlplane.client import _prepare_http_payload_for_call
    from pycloud_parallel.controlplane.object_ref import ObjectRef

    captured = {}

    def fake_put(clients, data, *, format="", chunk_size=0):
        captured["data"] = data
        captured["format"] = format
        return ObjectRef(
            object_id="sha256:" + ("f" * 64),
            format=format or "json",
            size_bytes=2048,
            materialize_as="json" if format == "json" else "bytes",
        )

    monkeypatch.setattr("pycloud_parallel.controlplane.client._put_data_via_clients", fake_put)
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client._estimate_managed_global_inline_size",
        lambda value: 2048 if isinstance(value, (dict, list)) else 16,
    )

    payload = {"small": 1, "big": {"x": [1, 2, 3]}}
    prepared = _prepare_http_payload_for_call([object()], payload, object_threshold_bytes=1024)

    assert prepared["small"] == 1
    assert isinstance(prepared["big"], ObjectRef)
    assert prepared["big"].consume_on_read is True
    assert captured["format"] == "json"


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
    group = ServiceGroup(
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
        state._executor_host._process.terminate()  # noqa: SLF001
        state._executor_host._process.join(timeout=5.0)  # noqa: SLF001

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

