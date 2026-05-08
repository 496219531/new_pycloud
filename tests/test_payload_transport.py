from __future__ import annotations

from dataclasses import replace
import json

import pytest

from pycloud_parallel.controlplane import client_transport as client_transport_mod
from pycloud_parallel.controlplane.config import (
    effective_limits_from_profile,
    get_config_limit_authority,
    get_gateway_http_body_limit_bytes,
    get_gateway_upload_limits,
    get_http_object_body_limit_bytes,
    get_infocenter_http_body_limit_bytes,
    get_bytes_materialize_threshold_bytes,
    get_job_blob_inline_threshold_bytes,
    get_job_staged_ref_ttl_sec,
    get_job_staging_replica_count,
    get_local_service_payload_policy,
    get_managed_globals_control_limit_bytes,
    get_node_control_http_body_limit_bytes,
    get_object_size_hard_limit_bytes,
    get_service_http_body_limit_bytes,
    get_transport_bounds,
    get_payload_policy,
    merge_object_threshold_with_policy_threshold,
    merge_payload_limits_with_effective_policy,
    normalize_policy_limit_values,
    resolve_payload_policy,
    validate_object_size_bytes,
    validate_bytes_materialize_size,
)
from pycloud_parallel.data.ref import DataRef
from pycloud_parallel.data.ref import data_ref_to_payload
from pycloud_parallel.controlplane.payload_transport import (
    decode_payload_from_transport,
    encode_result_for_transport,
    estimate_payload_inline_size,
    normalize_inbound_payload,
    prepare_outbound_payload,
)
from pycloud_parallel.controlplane.client_transport import _materialize_downloaded_result

_decode_http_request_body_with_mode = client_transport_mod._decode_http_request_body_with_mode


def _fake_object_ref(*, object_id_suffix: str = "a", format: str = "bin", consume_on_read: bool = False) -> DataRef:
    object_id = f"sha256:{object_id_suffix * 64}"
    return DataRef(
        ref_id=object_id,
        storage_id=object_id,
        logical_type="",
        format=format,
        size_bytes=128,
        materialize_as="bytes",
        locator_kind="node_local",
        locator_token="",
        consume_on_read=consume_on_read,
    )


def test_get_payload_policy_defaults() -> None:
    http_policy = get_payload_policy("http_call")
    job_policy = get_payload_policy("job_submit")
    managed_globals_policy = get_payload_policy("managed_globals")
    local_policy = get_local_service_payload_policy()

    assert http_policy.preserve_args_kwargs_container is True
    assert http_policy.consume_on_read is True
    assert local_policy.inline_payload_threshold_bytes == 64 * 1024 * 1024
    assert local_policy.inline_payload_hard_limit_bytes == 256 * 1024 * 1024
    assert local_policy.inline_payload_hard_limit_bytes > http_policy.inline_payload_hard_limit_bytes
    assert job_policy.managed_global_field_names == ("update_globals",)
    assert managed_globals_policy.objectify_pathlikes is True
    assert managed_globals_policy.objectify_strings_as_files is True
    assert managed_globals_policy.objectify_bytes is True
    assert managed_globals_policy.consume_on_read is False


def test_config_limit_authority_groups_existing_defaults() -> None:
    authority = get_config_limit_authority()

    assert authority.runtime_payload.inline_payload_threshold_bytes == 512 * 1024
    assert authority.policy_thresholds.default_safe.inline_payload_threshold_bytes == 2 * 1024 * 1024
    assert authority.policy_thresholds.default_safe.inline_payload_hard_limit_bytes == 8 * 1024 * 1024
    assert authority.policy_thresholds.trusted_internal.inline_result_hard_limit_bytes == 1000 * 1024 * 1024
    assert authority.transport_bounds.control_http_max_send_bytes == 16 * 1024 * 1024
    assert authority.object_store_bounds.object_chunk_size_bytes == 256 * 1024
    assert authority.object_store_bounds.object_size_hard_limit_bytes == 1024 * 1024 * 1024
    assert authority.object_store_bounds.bytes_materialize_threshold_bytes == 16 * 1024 * 1024
    assert authority.job_staging_bounds.job_staging_replica_count == 2
    assert authority.capacity_defaults.node_worker_capacity == 32


def test_inline_thresholds_are_clamped_to_hard_limits(monkeypatch) -> None:
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES", "999")
    monkeypatch.setenv("PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES", "100")
    monkeypatch.setenv("PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES", "999")
    monkeypatch.setenv("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", "200")
    config_mod.reload_config()
    try:
        policy = get_payload_policy("http_call")
        result_policy = get_payload_policy("result")
        assert policy.inline_payload_threshold_bytes == 100
        assert result_policy.inline_result_threshold_bytes == 200
    finally:
        monkeypatch.delenv("PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_INLINE_RESULT_THRESHOLD_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", raising=False)
        config_mod.reload_config()


def test_bytes_materialize_threshold_is_clamped_to_object_hard_limit(monkeypatch) -> None:
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES", "100")
    monkeypatch.setenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", "999")
    config_mod.reload_config()
    try:
        assert get_bytes_materialize_threshold_bytes() == 100
        with pytest.raises(ValueError) as exc_info:
            validate_bytes_materialize_size(101, context="download")
    finally:
        monkeypatch.delenv("PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", raising=False)
        config_mod.reload_config()

    assert "too large for in-memory bytes materialize" in str(exc_info.value)


def test_estimate_payload_inline_size_uses_cheap_type_metadata(monkeypatch) -> None:
    import pycloud_parallel.controlplane.payload_transport as transport_mod

    def _unexpected(*_args, **_kwargs):
        raise AssertionError("estimate_payload_inline_size must not serialize for sizing")

    monkeypatch.setattr(transport_mod, "serialize_inline_payload", _unexpected)
    assert estimate_payload_inline_size({"blob": b"x" * 128}) >= 128


def test_materialize_downloaded_result_rejects_large_bytes(tmp_path, monkeypatch) -> None:
    from pycloud_parallel.controlplane import config as config_mod

    path = tmp_path / "large-bytes.bin"
    path.write_bytes(b"x" * 64)
    ref = DataRef(
        ref_id="sha256:" + ("b" * 64),
        storage_id="sha256:" + ("b" * 64),
        format="bin",
        size_bytes=64,
        materialize_as="bytes",
    )
    monkeypatch.setenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", "8")
    config_mod.reload_config()
    try:
        with pytest.raises(ValueError) as exc_info:
            _materialize_downloaded_result(path, result_ref=ref)
    finally:
        monkeypatch.delenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", raising=False)
        config_mod.reload_config()

    assert "too large for in-memory bytes materialize" in str(exc_info.value)


def test_materialize_downloaded_result_allows_large_path(tmp_path, monkeypatch) -> None:
    from pycloud_parallel.controlplane import config as config_mod

    path = tmp_path / "large-path.bin"
    path.write_bytes(b"x" * 64)
    ref = DataRef(
        ref_id="sha256:" + ("c" * 64),
        storage_id="sha256:" + ("c" * 64),
        format="bin",
        size_bytes=64,
        materialize_as="path",
    )
    monkeypatch.setenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", "8")
    config_mod.reload_config()
    try:
        assert _materialize_downloaded_result(path, result_ref=ref) == path
    finally:
        monkeypatch.delenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", raising=False)
        config_mod.reload_config()


def test_materialize_downloaded_result_rejects_large_structured_bytes(tmp_path, monkeypatch) -> None:
    from pycloud_parallel.controlplane import config as config_mod

    path = tmp_path / "large-structured.bin"
    path.write_bytes(b"x" * 64)
    ref = DataRef(
        ref_id="sha256:" + ("d" * 64),
        storage_id="sha256:" + ("d" * 64),
        format="structured_v1",
        size_bytes=64,
        materialize_as="auto",
    )
    monkeypatch.setenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", "8")
    config_mod.reload_config()
    try:
        with pytest.raises(ValueError) as exc_info:
            _materialize_downloaded_result(path, result_ref=ref)
    finally:
        monkeypatch.delenv("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES", raising=False)
        config_mod.reload_config()

    assert "too large for in-memory bytes materialize" in str(exc_info.value)


def test_config_env_loader_and_reload_share_defaults(monkeypatch) -> None:
    from pycloud_parallel.controlplane import config

    monkeypatch.setenv("PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES", "12345")
    monkeypatch.setenv("PYCLOUD_CONTROL_MAX_SEND_MESSAGE_LENGTH_BYTES", "23456")

    loaded = config.load_config_from_env()
    config.reload_config()
    try:
        assert loaded["INLINE_PAYLOAD_THRESHOLD_BYTES"] == 12345
        assert config.INLINE_PAYLOAD_THRESHOLD_BYTES == 12345
        assert loaded["CONTROL_HTTP_MAX_SEND_BYTES"] == 23456
        assert config.CONTROL_HTTP_MAX_SEND_BYTES == 23456
    finally:
        monkeypatch.delenv("PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_CONTROL_MAX_SEND_MESSAGE_LENGTH_BYTES", raising=False)
        config.reload_config()


def test_core_client_payload_paths_do_not_import_default_safe_payload_constants_directly() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    scanned_files = [
        root / "src/pycloud_parallel/controlplane/gateway_client.py",
        root / "src/pycloud_parallel/controlplane/discovery_client.py",
        root / "src/pycloud_parallel/controlplane/remote_payload.py",
        root / "src/pycloud_parallel/execution/service_session.py",
        root / "src/pycloud_parallel/execution/support.py",
    ]
    banned_terms = {
        "INLINE_PAYLOAD_THRESHOLD_BYTES",
        "INLINE_PAYLOAD_HARD_LIMIT_BYTES",
        "DEFAULT_SAFE_INLINE_PAYLOAD_THRESHOLD_BYTES",
        "DEFAULT_SAFE_INLINE_PAYLOAD_HARD_LIMIT_BYTES",
    }
    violations = []
    for path in scanned_files:
        text = path.read_text(encoding="utf-8")
        for term in banned_terms:
            if term in text:
                violations.append(f"{path.relative_to(root)} references {term} directly")
    assert not violations, "Payload limit consumers should resolve policy thresholds through helpers:\n" + "\n".join(violations)


def test_managed_globals_control_limit_clamps_policy_and_control_bounds() -> None:
    assert get_managed_globals_control_limit_bytes(policy_hard_limit_bytes=1000, control_send_bytes=2000) == 1000
    assert get_managed_globals_control_limit_bytes(policy_hard_limit_bytes=2000, control_send_bytes=1000) == 1000
    assert get_managed_globals_control_limit_bytes(policy_hard_limit_bytes=0, control_send_bytes=0) >= 1


def test_config_limit_helpers_normalize_and_merge_bounds() -> None:
    assert normalize_policy_limit_values(
        payload_threshold=200,
        payload_hard=100,
        result_threshold=200,
        result_hard=100,
    ) == (100, 100, 100, 100)
    assert merge_object_threshold_with_policy_threshold(object_threshold_bytes=500, policy_threshold_bytes=200) == 200
    assert merge_object_threshold_with_policy_threshold(object_threshold_bytes=100, policy_threshold_bytes=200) == 100
    assert get_job_blob_inline_threshold_bytes() == max(256 * 1024, int((2 * 1024 * 1024) / 1.5))
    assert get_job_staging_replica_count(0) == 2
    assert get_job_staging_replica_count(-3) == 1
    assert get_job_staging_replica_count(5) == 5
    assert get_job_staged_ref_ttl_sec(0) == 24 * 60 * 60
    assert get_job_staged_ref_ttl_sec(-5) == 1
    assert get_job_staged_ref_ttl_sec(10) == 10
    assert get_http_object_body_limit_bytes(123) == 123
    assert get_object_size_hard_limit_bytes(123) == 123
    assert get_node_control_http_body_limit_bytes(123) == 512 * 1024 * 1024
    assert get_service_http_body_limit_bytes(0) == get_transport_bounds().service_http_body_max_bytes
    assert get_gateway_http_body_limit_bytes(123) == 123
    assert get_infocenter_http_body_limit_bytes(123) == 123
    assert get_gateway_upload_limits(max_file_bytes=10, max_total_bytes=5) == (10, 10)
    validate_object_size_bytes(123, context="test object")


def test_config_limit_helpers_merge_effective_policy() -> None:
    from pycloud_parallel.controlplane.effective_policy import EffectivePolicy

    base = get_payload_policy("http_call").limits
    effective = EffectivePolicy(
        policy_id="test",
        version=1,
        resolved_mode="structured_v1",
        allowed_modes=("structured_v1",),
        inline_payload_threshold_bytes=128,
        inline_payload_hard_limit_bytes=256,
        inline_result_threshold_bytes=256,
        inline_result_hard_limit_bytes=512,
        use_raw_bytes_payload=False,
        use_http_raw_bytes_body=False,
        allow_pickle_stable=False,
    )

    merged = merge_payload_limits_with_effective_policy(base, effective)

    assert merged.inline_payload_threshold_bytes == 128
    assert merged.inline_payload_hard_limit_bytes == 256
    assert merged.inline_result_threshold_bytes == 256
    assert merged.inline_result_hard_limit_bytes == 512
    assert effective_limits_from_profile(effective) == (128, 256, 256, 512)


def test_resolve_payload_policy_merges_effective_policy_and_object_threshold() -> None:
    from pycloud_parallel.controlplane.effective_policy import EffectivePolicy, payload_policy_from_effective_policy

    effective = EffectivePolicy(
        policy_id="test",
        version=1,
        resolved_mode="structured_v1",
        allowed_modes=("structured_v1",),
        inline_payload_threshold_bytes=512,
        inline_payload_hard_limit_bytes=2048,
        inline_result_threshold_bytes=2048,
        inline_result_hard_limit_bytes=4096,
        use_raw_bytes_payload=False,
        use_http_raw_bytes_body=False,
        allow_pickle_stable=False,
    )

    resolved = resolve_payload_policy("http_call", effective_policy=effective, object_threshold_bytes=256)
    legacy = payload_policy_from_effective_policy("http_call", effective)

    assert resolved.mode == legacy.mode
    assert resolved.inline_payload_threshold_bytes == 256
    assert resolved.inline_payload_hard_limit_bytes == legacy.inline_payload_hard_limit_bytes
    assert resolved.inline_result_threshold_bytes == legacy.inline_result_threshold_bytes
    assert resolved.inline_result_hard_limit_bytes == legacy.inline_result_hard_limit_bytes
    assert resolved.consume_on_read == legacy.consume_on_read
    assert resolved.preserve_args_kwargs_container == legacy.preserve_args_kwargs_container


def test_resolve_payload_policy_keeps_default_threshold_when_threshold_is_zero() -> None:
    policy = resolve_payload_policy("task_submit", object_threshold_bytes=0)
    base = get_payload_policy("task_submit")

    assert policy.inline_payload_threshold_bytes == base.inline_payload_threshold_bytes
    assert policy.inline_payload_hard_limit_bytes == base.inline_payload_hard_limit_bytes


def test_prepare_outbound_payload_preserves_args_kwargs_container() -> None:
    policy = get_payload_policy("http_call")
    policy = replace(
        policy,
        limits=replace(
            policy.limits,
            inline_payload_threshold_bytes=32,
        ),
    )
    uploads: list[tuple[object, str]] = []

    def _put_data(value, *, format=""):
        uploads.append((value, format))
        return _fake_object_ref(object_id_suffix=chr(ord("a") + len(uploads) - 1), format=format or "bin")

    prepared = prepare_outbound_payload(
        {
            "args": ["small", "x" * 128],
            "kwargs": {"blob": "y" * 128},
        },
        put_data=_put_data,
        estimate_inline_size=estimate_payload_inline_size,
        policy=policy,
    )

    assert isinstance(prepared["args"], list)
    assert prepared["args"][0] == "small"
    assert isinstance(prepared["args"][1], DataRef)
    assert prepared["args"][1].consume_on_read is True
    assert isinstance(prepared["kwargs"], dict)
    assert isinstance(prepared["kwargs"]["blob"], DataRef)
    assert len(uploads) == 2


def test_prepare_outbound_payload_job_submit_applies_managed_globals_policy(tmp_path) -> None:
    policy = get_payload_policy("job_submit")
    path = tmp_path / "config.json"
    path.write_text('{"mode":"test"}', encoding="utf-8")
    uploads: list[object] = []

    def _put_data(value, *, format=""):
        uploads.append(value)
        return _fake_object_ref(object_id_suffix=chr(ord("a") + len(uploads) - 1), format=format or "bin")

    prepared = prepare_outbound_payload(
        {
            "artifact_path": path,
            "update_globals": {
                "cfg_path": path,
                "raw_bytes": b"abc",
            },
        },
        put_data=_put_data,
        estimate_inline_size=estimate_payload_inline_size,
        policy=policy,
    )

    assert prepared["artifact_path"] == path
    assert isinstance(prepared["update_globals"]["cfg_path"], DataRef)
    assert isinstance(prepared["update_globals"]["raw_bytes"], DataRef)
    assert uploads[0] == path
    assert uploads[1] == b"abc"


def test_prepare_managed_globals_batches_splits_inline_keys(monkeypatch) -> None:
    from pycloud_parallel.execution import support

    monkeypatch.setattr(support, "CONTROL_HTTP_MAX_SEND_BYTES", 1000)

    batches, stats = support._prepare_managed_globals_batches_for_upload(
        [],
        {"a": "x" * 400, "b": "y" * 400, "c": "z" * 20},
    )

    assert batches == [{"a": "x" * 400}, {"b": "y" * 400, "c": "z" * 20}]
    assert stats["globals_batch_count"] == 2
    assert stats["batch_keys"] == [["a"], ["b", "c"]]
    assert stats["staged_keys"] == []
    assert stats["inline_keys"] == ["a", "b", "c"]
    assert all(size <= 1000 for size in stats["batch_bytes"])


def test_prepare_managed_globals_batches_stages_single_oversized_key(monkeypatch) -> None:
    from pycloud_parallel.execution import support

    uploaded = []

    class _Client:
        def upload_object_from_bytes(self, *, blob, format, chunk_size):  # noqa: ANN001
            uploaded.append((bytes(blob), str(format), int(chunk_size)))
            return DataRef(ref_id="obj-1", storage_id="obj-1", format=format, size_bytes=len(blob))

    monkeypatch.setattr(support, "CONTROL_HTTP_MAX_SEND_BYTES", 1000)

    batches, stats = support._prepare_managed_globals_batches_for_upload(
        [_Client()],
        {"big": {"payload": "x" * 3000}},
    )

    assert len(uploaded) == 1
    assert stats["globals_batch_count"] == 1
    assert stats["staged_keys"] == ["big"]
    assert stats["inline_keys"] == []
    assert isinstance(batches[0]["big"], DataRef)
    assert stats["batch_bytes"][0] <= 1000


def test_put_data_file_path_uses_file_upload_without_reading_whole_file(tmp_path, monkeypatch) -> None:
    from pathlib import Path
    from pycloud_parallel.execution import support

    source = tmp_path / "payload.bin"
    source.write_bytes(b"stream local path upload")
    calls = []

    class _Client:
        control_addr = "node-a"
        node_id = "node-a"
        node_instance_id = "node-a-1"

        def upload_object_from_file(self, *, file_path, format, chunk_size):  # noqa: ANN001
            calls.append((str(file_path), str(format), int(chunk_size)))
            return DataRef(
                ref_id="sha256:" + ("f" * 64),
                storage_id="sha256:" + ("f" * 64),
                format=format,
                size_bytes=source.stat().st_size,
                materialize_as="path",
                locator_kind="node_control",
                locator_token=self.control_addr,
                control_addr=self.control_addr,
            )

        def upload_object_from_bytes(self, **_kwargs):  # noqa: ANN003
            raise AssertionError("file path put_data must use upload_object_from_file")

    def _fail_read_bytes(self):  # noqa: ANN001
        raise AssertionError("file path put_data must not read the whole file")

    monkeypatch.setattr(Path, "read_bytes", _fail_read_bytes)

    ref = support._put_data_via_clients([_Client()], source, format="bin")

    assert ref.object_id == "sha256:" + ("f" * 64)
    assert ref.materialize_as == "path"
    assert calls == [(str(source), "bin", 256 * 1024)]


def test_put_data_ndarray_uses_file_upload_without_bytes_materialization(tmp_path):
    np = pytest.importorskip("numpy")
    from pathlib import Path
    from pycloud_parallel.execution import support

    calls = []
    array = np.arange(16, dtype=np.int64).reshape(4, 4)

    class _Client:
        control_addr = "node-a"
        node_id = "node-a"
        node_instance_id = "node-a-1"

        def upload_object_from_file(self, *, file_path, format, chunk_size):  # noqa: ANN001
            path = Path(file_path)
            calls.append((str(path), str(format), int(chunk_size), path.exists(), path.stat().st_size))
            return DataRef(
                ref_id="sha256:" + ("a" * 64),
                storage_id="sha256:" + ("a" * 64),
                format=format,
                size_bytes=path.stat().st_size,
                materialize_as="ndarray",
                locator_kind="node_control",
                locator_token=self.control_addr,
                control_addr=self.control_addr,
            )

        def upload_object_from_bytes(self, **_kwargs):  # noqa: ANN003
            raise AssertionError("ndarray put_data must not materialize upload bytes")

    ref = support._put_data_via_clients([_Client()], array, format="npy")

    assert ref.object_id == "sha256:" + ("a" * 64)
    assert ref.materialize_as == "ndarray"
    assert len(calls) == 1
    assert calls[0][1] == "npy"


def test_put_data_dataframe_uses_file_upload_without_bundle_bytes_materialization():
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    from pathlib import Path
    from pycloud_parallel.execution import support

    calls = []
    frame = pd.DataFrame({"x": [1, 2], "y": [3, 4]})

    class _Client:
        control_addr = "node-a"
        node_id = "node-a"
        node_instance_id = "node-a-1"

        def upload_object_from_file(self, *, file_path, format, chunk_size):  # noqa: ANN001
            path = Path(file_path)
            calls.append((str(path), str(format), int(chunk_size), path.exists(), path.stat().st_size))
            return DataRef(
                ref_id="sha256:" + ("b" * 64),
                storage_id="sha256:" + ("b" * 64),
                format=format,
                size_bytes=path.stat().st_size,
                materialize_as="dataframe",
                locator_kind="node_control",
                locator_token=self.control_addr,
                control_addr=self.control_addr,
            )

        def upload_object_from_bytes(self, **_kwargs):  # noqa: ANN003
            raise AssertionError("dataframe put_data must not materialize bundle bytes")

    ref = support._put_data_via_clients([_Client()], frame, format="parquet")

    assert ref.object_id == "sha256:" + ("b" * 64)
    assert ref.materialize_as == "dataframe"
    assert len(calls) == 1
    assert calls[0][1] == "dfbundle"


def test_put_data_series_uses_file_upload_without_bundle_bytes_materialization():
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    from pathlib import Path
    from pycloud_parallel.execution import support

    calls = []
    series = pd.Series([1.0, 2.0], name="nav")

    class _Client:
        control_addr = "node-a"
        node_id = "node-a"
        node_instance_id = "node-a-1"

        def upload_object_from_file(self, *, file_path, format, chunk_size):  # noqa: ANN001
            path = Path(file_path)
            calls.append((str(path), str(format), int(chunk_size), path.exists(), path.stat().st_size))
            return DataRef(
                ref_id="sha256:" + ("c" * 64),
                storage_id="sha256:" + ("c" * 64),
                format=format,
                size_bytes=path.stat().st_size,
                materialize_as="series",
                locator_kind="node_control",
                locator_token=self.control_addr,
                control_addr=self.control_addr,
            )

        def upload_object_from_bytes(self, **_kwargs):  # noqa: ANN003
            raise AssertionError("series put_data must not materialize bundle bytes")

    ref = support._put_data_via_clients([_Client()], series, format="parquet")

    assert ref.object_id == "sha256:" + ("c" * 64)
    assert ref.materialize_as == "series"
    assert len(calls) == 1
    assert calls[0][1] == "seriesbundle"


def test_normalize_inbound_payload_deserializes_before_object_resolution() -> None:
    captured = {}

    def _resolve(value):
        captured["value"] = value
        return {"resolved": True}

    normalized = normalize_inbound_payload(
        {
            "blob": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("c" * 64),
                    storage_id="sha256:" + ("c" * 64),
                    logical_type="",
                    format="json",
                    size_bytes=42,
                    materialize_as="json",
                    locator_kind="node_local",
                    locator_token="",
                )
            )
        },
        object_dir="/tmp/objects",
        policy=get_payload_policy("job_submit"),
        resolve_object_refs=_resolve,
    )

    assert normalized == {"resolved": True}
    assert isinstance(captured["value"]["blob"], DataRef)


def test_decode_payload_from_transport_keeps_payload_decoded_without_localizing() -> None:
    decoded = decode_payload_from_transport(
        {
            "blob": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("d" * 64),
                    storage_id="sha256:" + ("d" * 64),
                    logical_type="",
                    format="json",
                    size_bytes=99,
                    materialize_as="json",
                    locator_kind="node_local",
                    locator_token="",
                )
            )
        },
        policy=get_payload_policy("http_call"),
    )

    assert isinstance(decoded["blob"], DataRef)


def test_decode_http_request_body_returns_raw_payload_for_worker_decode() -> None:
    body = json.dumps(
        {
            "blob": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("e" * 64),
                    storage_id="sha256:" + ("e" * 64),
                    logical_type="",
                    format="json",
                    size_bytes=11,
                    materialize_as="json",
                    locator_kind="node_local",
                    locator_token="",
                )
            )
        }
    ).encode("utf-8")

    decoded, mode = _decode_http_request_body_with_mode(
        body,
        context="service call payload",
    )

    assert mode == "legacy_v1"
    assert isinstance(decoded["blob"], dict)
    assert decoded["blob"]["__pycloud_data_ref__"]["ref_id"] == "sha256:" + ("e" * 64)


def test_encode_result_for_transport_wraps_scalar_value() -> None:
    encoded = encode_result_for_transport(
        7,
        policy=get_payload_policy("result"),
        context="task result",
    )

    assert encoded == {"value": 7}


def test_decode_payload_from_transport_recognizes_data_ref_sentinel() -> None:
    decoded = decode_payload_from_transport(
        {
            "blob": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("f" * 64),
                    storage_id="sha256:" + ("f" * 64),
                    logical_type="bytes",
                    format="bin",
                    size_bytes=12,
                    materialize_as="auto",
                )
            )
        },
        policy=get_payload_policy("http_call"),
    )

    assert isinstance(decoded["blob"], DataRef)
    assert decoded["blob"].object_id == "sha256:" + ("f" * 64)


def test_decode_http_request_body_defers_data_ref_decode_to_worker() -> None:
    body = json.dumps(
        {
            "blob": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("1" * 64),
                    storage_id="sha256:" + ("1" * 64),
                    logical_type="json",
                    format="json",
                    size_bytes=11,
                    materialize_as="json",
                )
            )
        }
    ).encode("utf-8")

    decoded, mode = _decode_http_request_body_with_mode(
        body,
        context="service call payload",
    )

    assert mode == "legacy_v1"
    assert isinstance(decoded["blob"], dict)
    assert decoded["blob"]["__pycloud_data_ref__"]["logical_type"] == "json"
