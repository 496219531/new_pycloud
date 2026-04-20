from __future__ import annotations

from unittest.mock import patch

from pycloud_parallel.controlplane.data_ref import DataRef
from pycloud_parallel.controlplane.job_staging import (
    collect_payload_data_refs,
    release_job_staged_refs,
    resolve_staged_payload,
    stage_job_value,
    touch_job_staged_refs,
)


def test_collect_payload_data_refs_reads_nested_refs():
    ref = DataRef(
        ref_id="sha256:" + "a" * 64,
        storage_id="sha256:" + "a" * 64,
        format="bin",
    )
    payload = {"job_payload": {"x": ref, "items": [ref]}}
    assert collect_payload_data_refs(payload) == [ref.ref_id]


def test_stage_job_value_delegates_to_existing_stager():
    staged = DataRef(
        ref_id="sha256:" + "b" * 64,
        storage_id="sha256:" + "b" * 64,
        format="bin",
        locator_kind="controlplane",
        locator_token="127.0.0.1:50051",
    )
    with patch("pycloud_parallel.controlplane.job_staging._stage_job_submit_value", return_value=staged) as mocked:
        result = stage_job_value(target="127.0.0.1:50051", value={"big": "value"})
    assert result == staged
    mocked.assert_called_once()


def test_touch_and_release_job_staged_refs_delegate_to_registry():
    with patch("pycloud_parallel.controlplane.job_staging.DataRegistryClient") as mocked_client:
        client = mocked_client.return_value
        assert touch_job_staged_refs(target="127.0.0.1:50051", ref_ids=["r1", "r2"]) == ["r1", "r2"]
        assert release_job_staged_refs(target="127.0.0.1:50051", ref_ids=["r1"]) == ["r1"]
    assert client.touch.call_count == 2
    client.release.assert_called_once_with("r1")


def test_resolve_staged_payload_delegates_to_existing_resolver():
    with patch("pycloud_parallel.controlplane.job_staging._resolve_payload_data_refs", return_value={"ok": True}) as mocked:
        result = resolve_staged_payload({"payload": 1}, registry_target="127.0.0.1:50051")
    assert result == {"ok": True}
    mocked.assert_called_once()
