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
        put_data=lambda value, *, format="": {
            "__pycloud_object_ref__": {
                "object_id": "sha256:" + ("a" * 64),
                "format": format or "txt",
                "size_bytes": 2048,
                "materialize_as": "text",
                "consume_on_read": False,
            }
        },
        estimate_inline_size=estimate_payload_inline_size,
        policy=policy,
    )

    assert isinstance(prepared["blob"], DataRef)
    assert prepared["blob"].logical_type == "text"


def test_legacy_object_and_result_refs_serialize_as_dataref_payloads():
    serialized_object = data_ref_to_payload(
        coerce_data_ref(
            {
                "__pycloud_object_ref__": {
                    "object_id": "sha256:" + ("b" * 64),
                    "format": "json",
                    "size_bytes": 64,
                    "materialize_as": "json",
                    "consume_on_read": False,
                }
            }
        )
    )
    serialized_result = data_ref_to_payload(
        coerce_data_ref(
            {
                "__pycloud_result_ref__": {
                    "object_id": "sha256:" + ("c" * 64),
                    "node_id": "node-1",
                    "control_addr": "127.0.0.1:50061",
                    "format": "bin",
                    "size_bytes": 128,
                    "materialize_as": "bytes",
                }
            }
        )
    )

    assert list(serialized_object.keys()) == [DATA_REF_SENTINEL]
    assert list(serialized_result.keys()) == [DATA_REF_SENTINEL]
    assert isinstance(convert_dict_to_arrow(serialized_object), DataRef)
    assert isinstance(convert_dict_to_arrow(serialized_result), DataRef)


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


def test_maybe_data_ref_accepts_legacy_refs():
    object_ref = {
        "__pycloud_object_ref__": {
            "object_id": "sha256:" + ("e" * 64),
            "format": "bin",
            "size_bytes": 12,
            "materialize_as": "bytes",
            "consume_on_read": False,
        }
    }
    result_ref = {
        "__pycloud_result_ref__": {
            "object_id": "sha256:" + ("f" * 64),
            "node_id": "node-2",
            "control_addr": "127.0.0.1:50062",
            "format": "txt",
            "size_bytes": 24,
            "materialize_as": "text",
        }
    }

    assert isinstance(maybe_data_ref(object_ref), DataRef)
    assert isinstance(maybe_data_ref(result_ref), DataRef)
