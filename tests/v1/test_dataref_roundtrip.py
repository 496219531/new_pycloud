from __future__ import annotations

from dataclasses import replace

from pycloud_parallel.controlplane.data_ref import DataRef as ControlplaneDataRef
from pycloud_parallel.controlplane.config import get_payload_policy
from pycloud_parallel.controlplane.data_store import DataStore, StoredDataArtifact
from pycloud_parallel.controlplane.data_ref import coerce_data_ref, data_ref_to_payload
from pycloud_parallel.controlplane.payload_transport import estimate_payload_inline_size, prepare_outbound_payload
from pycloud_parallel.controlplane.serialization import convert_dict_to_arrow
from pycloud_parallel.data.ref import DataRef, DATA_REF_SENTINEL, maybe_data_ref


def test_dataref_module_is_authoritative():
    assert ControlplaneDataRef is DataRef


def test_prepare_outbound_payload_converts_large_object_uploads_to_dataref():
    policy = replace(
        get_payload_policy("http_call"),
        limits=replace(get_payload_policy("http_call").limits, inline_payload_soft_limit_bytes=32),
    )
    prepared = prepare_outbound_payload(
        {"blob": "x" * 2048},
        put_data=lambda value, *, format="": data_ref_to_payload(
            DataRef(
                ref_id="sha256:" + ("a" * 64),
                storage_id="sha256:" + ("a" * 64),
                logical_type="text",
                format=format or "txt",
                size_bytes=2048,
                materialize_as="text",
                locator_kind="node_local",
                locator_token="",
            )
        ),
        estimate_inline_size=estimate_payload_inline_size,
        policy=policy,
    )

    assert isinstance(prepared["blob"], DataRef)
    assert prepared["blob"].logical_type == "text"


def test_dataref_payload_roundtrip_stays_canonical():
    serialized = data_ref_to_payload(
        coerce_data_ref(
            DataRef(
                ref_id="sha256:" + ("b" * 64),
                storage_id="sha256:" + ("b" * 64),
                logical_type="json",
                format="json",
                size_bytes=64,
                materialize_as="json",
                locator_kind="node_local",
                locator_token="",
            )
        )
    )

    assert list(serialized.keys()) == [DATA_REF_SENTINEL]
    assert isinstance(convert_dict_to_arrow(serialized), DataRef)


def test_data_store_large_results_now_return_dataref():
    store = DataStore(object_dir="/tmp/objects", node_id="node-1", control_addr="127.0.0.1:50061")
    ref = store.result_ref_from_stored_artifact(
        StoredDataArtifact(
            object_id="sha256:" + ("d" * 64),
            format="dfbundle",
            size_bytes=1024,
            materialize_as="dataframe",
        )
    )

    assert isinstance(ref, DataRef)
    assert ref.locator_kind == "node_control"
    assert ref.node_id == "node-1"
    assert ref.logical_type == "dataframe"


def test_maybe_data_ref_accepts_canonical_payload_only():
    canonical = data_ref_to_payload(
        DataRef(
            ref_id="sha256:" + ("e" * 64),
            storage_id="sha256:" + ("e" * 64),
            logical_type="bytes",
            format="bin",
            size_bytes=12,
            materialize_as="bytes",
            locator_kind="node_local",
            locator_token="",
        )
    )

    assert isinstance(maybe_data_ref(canonical), DataRef)
