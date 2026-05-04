from __future__ import annotations

"""Shared managed-globals upload and replica fan-out helpers."""

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from pycloud_parallel.controlplane.effective_policy import EffectivePolicy
from pycloud_parallel.execution.support import (
    _encode_managed_globals_batches,
    _prepare_managed_globals_batches_for_upload,
)
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2

PreparedValues = Dict[str, object]
EncodedBatch = Tuple[PreparedValues, object, Optional[pb2.TransportPayload]]
ReplicaTarget = Tuple[str, Any]
UpdateBatchFunc = Callable[[str, Any, PreparedValues, object, Optional[pb2.TransportPayload]], Any]


def update_managed_globals_across_replicas(
    *,
    upload_clients: Sequence[Any],
    values: Dict[str, object],
    targets: Sequence[ReplicaTarget],
    serialization_mode: str,
    effective_policy: Optional[EffectivePolicy],
    context: str,
    thread_name_prefix: str,
    update_batch: UpdateBatchFunc,
    include_empty_digest: bool = True,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    prepare_kwargs: Dict[str, object] = {}
    if str(serialization_mode or "").strip() and serialization_mode != "legacy_v1":
        prepare_kwargs["serialization_mode"] = serialization_mode
    prepared_batches, _ = _prepare_managed_globals_batches_for_upload(
        upload_clients,
        values,
        effective_policy=effective_policy,
        context=context,
        **prepare_kwargs,
    )
    encoded_batches = _encode_managed_globals_batches(
        prepared_batches,
        serialization_mode=serialization_mode,
        effective_policy=effective_policy,
        context=context,
    )

    def _update_one(node_id: str, replica: Any) -> Tuple[str, str, str]:
        try:
            resp = None
            for prepared_values, values_struct, transport_values in encoded_batches:
                resp = update_batch(node_id, replica, prepared_values, values_struct, transport_values)
            return node_id, str(getattr(resp, "globals_digest", "") or ""), ""
        except Exception as exc:
            return node_id, "", repr(exc)

    target_list = list(targets)
    if len(target_list) == 1:
        update_results = [_update_one(*target_list[0])]
    else:
        with ThreadPoolExecutor(max_workers=max(1, len(target_list)), thread_name_prefix=thread_name_prefix) as executor:
            futures = [executor.submit(_update_one, node_id, replica) for node_id, replica in target_list]
            update_results = [future.result() for future in futures]

    digests: Dict[str, str] = {}
    failed_nodes: Dict[str, str] = {}
    for node_id, digest, error_text in update_results:
        if error_text:
            failed_nodes[node_id] = error_text
        elif include_empty_digest or digest:
            digests[node_id] = digest
    return digests, failed_nodes


__all__ = ["update_managed_globals_across_replicas"]
