from __future__ import annotations

"""Control-plane data reference resolution helpers."""

from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Tuple

from pycloud_parallel.data.ref import DataRef, coerce_data_ref


@dataclass(frozen=True)
class ResolvedDataRef:
    ref: DataRef
    control_addr: str
    node_id: str = ""
    node_instance_id: str = ""
    locator_kind: str = ""
    locator_token: str = ""
    via_registry: bool = False
    replicas: Tuple[Dict[str, str], ...] = ()


def _normalize_resolved_replicas(replicas: Sequence[Dict[str, object]]) -> Tuple[Dict[str, str], ...]:
    out = []
    seen: set[tuple[str, str, str]] = set()
    for item in replicas or ():
        if not isinstance(item, dict):
            continue
        control_addr = str(item.get("control_addr", "") or "").strip()
        node_id = str(item.get("node_id", "") or "").strip()
        node_instance_id = str(item.get("node_instance_id", "") or "").strip()
        if not control_addr:
            continue
        key = (control_addr, node_id, node_instance_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "control_addr": control_addr,
                "node_id": node_id,
                "node_instance_id": node_instance_id,
            }
        )
    return tuple(out)


def _node_known_unhealthy(node: object) -> bool:
    if not hasattr(node, "healthy"):
        return False
    return not bool(getattr(node, "healthy", False))


def _replica_known_unhealthy(replica: Dict[str, str], healthy_map: Dict[str, bool]) -> bool:
    node_instance_id = str(replica.get("node_instance_id", "") or "").strip()
    return bool(node_instance_id) and healthy_map.get(node_instance_id) is False


def _data_ref_from_registry_entry(entry: Dict[str, object], fallback: DataRef) -> DataRef:
    return DataRef(
        ref_id=str(entry.get("ref_id", "") or fallback.ref_id or ""),
        storage_id=str(entry.get("storage_id", "") or fallback.storage_id or fallback.object_id or ""),
        logical_type=str(entry.get("logical_type", "") or fallback.logical_type or ""),
        format=str(entry.get("format", "") or fallback.format or ""),
        size_bytes=int(entry.get("size_bytes", 0) or fallback.size_bytes or 0),
        materialize_as=str(entry.get("materialize_as", "") or fallback.materialize_as or ""),
        locator_kind=str(entry.get("locator_kind", "") or fallback.locator_kind or ""),
        locator_token=str(entry.get("locator_token", "") or fallback.locator_token or ""),
        consume_on_read=bool(entry.get("consume_on_read", fallback.consume_on_read)),
        node_id=str(entry.get("node_id", "") or fallback.node_id or ""),
        node_instance_id=str(entry.get("node_instance_id", "") or fallback.node_instance_id or ""),
        control_addr=str(entry.get("control_addr", "") or fallback.control_addr or ""),
    )


class DataRegistryClient:
    def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
        self.target = str(target or "").strip()
        self.timeout_sec = max(0.1, float(timeout_sec))

    def register(
        self,
        ref: DataRef | object,
        *,
        ttl_sec: int = 3600,
        node_id: str = "",
        node_instance_id: str = "",
        control_addr: str = "",
        locator_kind: str = "",
        locator_token: str = "",
        replicas: Optional[Sequence[Dict[str, object]]] = None,
    ) -> Dict[str, object]:
        data_ref = ref if isinstance(ref, DataRef) else coerce_data_ref(ref)
        from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

        with InfoCenterClient(self.target, timeout_sec=self.timeout_sec) as client:
            return client.register_data_ref(
                ref=data_ref,
                ttl_sec=ttl_sec,
                node_id=node_id,
                node_instance_id=node_instance_id,
                control_addr=control_addr,
                locator_kind=locator_kind,
                locator_token=locator_token,
                replicas=replicas,
            )

    def touch(self, ref_id: str) -> Dict[str, object]:
        from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

        with InfoCenterClient(self.target, timeout_sec=self.timeout_sec) as client:
            return client.touch_data_ref(ref_id=ref_id)

    def release(self, ref_id: str) -> Dict[str, object]:
        from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

        with InfoCenterClient(self.target, timeout_sec=self.timeout_sec) as client:
            return client.release_data_ref(ref_id=ref_id)

    def resolve(self, ref: DataRef | object) -> ResolvedDataRef:
        data_ref = ref if isinstance(ref, DataRef) else coerce_data_ref(ref)

        if str(data_ref.control_addr or "").strip():
            direct_replicas = _normalize_resolved_replicas(
                (
                    {
                        "control_addr": str(data_ref.control_addr or "").strip(),
                        "node_id": str(data_ref.node_id or "").strip(),
                        "node_instance_id": str(data_ref.node_instance_id or "").strip(),
                    },
                )
            )
            return ResolvedDataRef(
                ref=data_ref,
                control_addr=str(data_ref.control_addr or "").strip(),
                node_id=str(data_ref.node_id or "").strip(),
                node_instance_id=str(data_ref.node_instance_id or "").strip(),
                locator_kind=str(data_ref.locator_kind or ""),
                locator_token=str(data_ref.locator_token or ""),
                via_registry=False,
                replicas=direct_replicas,
            )

        locator_kind = str(data_ref.locator_kind or "").strip().lower()
        locator_token = str(data_ref.locator_token or self.target or "").strip()

        if locator_kind == "node_control" and locator_token:
            direct_replicas = _normalize_resolved_replicas(
                (
                    {
                        "control_addr": locator_token,
                        "node_id": str(data_ref.node_id or "").strip(),
                        "node_instance_id": str(data_ref.node_instance_id or "").strip(),
                    },
                )
            )
            return ResolvedDataRef(
                ref=data_ref,
                control_addr=locator_token,
                node_id=str(data_ref.node_id or "").strip(),
                node_instance_id=str(data_ref.node_instance_id or "").strip(),
                locator_kind=locator_kind,
                locator_token=locator_token,
                via_registry=False,
                replicas=direct_replicas,
            )

        if locator_kind in {"controlplane", "node_local", ""} and locator_token:
            from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

            with InfoCenterClient(locator_token, timeout_sec=self.timeout_sec) as client:
                try:
                    payload = client.resolve_data_ref(ref_id=data_ref.ref_id)
                except Exception:
                    payload = {}
                entry = dict(payload.get("entry") or {})
                registry_ref = _data_ref_from_registry_entry(entry, data_ref) if entry else data_ref
                control_addr = str(entry.get("control_addr", "") or "").strip()
                replicas = [
                    dict(item)
                    for item in (entry.get("replicas") or ())
                    if isinstance(item, dict) and str(item.get("control_addr", "") or "").strip()
                ]
                if control_addr:
                    replicas.append(
                        {
                            "control_addr": control_addr,
                            "node_id": str(entry.get("node_id", "") or data_ref.node_id or ""),
                            "node_instance_id": str(entry.get("node_instance_id", "") or data_ref.node_instance_id or ""),
                        }
                    )
                replicas = list(_normalize_resolved_replicas(replicas))
                had_registry_locator = bool(replicas)
                if replicas:
                    nodes = list(client.list_nodes(healthy_only=False, limit=2000))
                    healthy_map = {}
                    for node in nodes:
                        node_instance_id = str(getattr(node, "node_instance_id", "") or "").strip()
                        if not node_instance_id or not hasattr(node, "healthy"):
                            continue
                        healthy_map[node_instance_id] = bool(getattr(node, "healthy", False))
                    replicas = [item for item in replicas if not _replica_known_unhealthy(item, healthy_map)]
                if replicas:
                    replicas.sort(
                        key=lambda item: (
                            0 if healthy_map.get(str(item.get("node_instance_id", "") or "").strip(), True) else 1,
                            str(item.get("node_instance_id", "") or ""),
                            str(item.get("node_id", "") or ""),
                            str(item.get("control_addr", "") or ""),
                        )
                    )
                    best = replicas[0]
                    return ResolvedDataRef(
                        ref=registry_ref,
                        control_addr=str(best.get("control_addr", "") or "").strip(),
                        node_id=str(best.get("node_id", "") or registry_ref.node_id or ""),
                        node_instance_id=str(best.get("node_instance_id", "") or registry_ref.node_instance_id or ""),
                        locator_kind="node_control",
                        locator_token=str(best.get("control_addr", "") or "").strip(),
                        via_registry=True,
                        replicas=tuple(
                            {
                                "control_addr": str(item.get("control_addr", "") or "").strip(),
                                "node_id": str(item.get("node_id", "") or "").strip(),
                                "node_instance_id": str(item.get("node_instance_id", "") or "").strip(),
                            }
                            for item in replicas
                        ),
                    )
                if control_addr and not had_registry_locator:
                    return ResolvedDataRef(
                        ref=registry_ref,
                        control_addr=control_addr,
                        node_id=str(entry.get("node_id", "") or registry_ref.node_id or ""),
                        node_instance_id=str(entry.get("node_instance_id", "") or registry_ref.node_instance_id or ""),
                        locator_kind=str(entry.get("locator_kind", "") or locator_kind),
                        locator_token=str(entry.get("locator_token", "") or locator_token),
                        via_registry=True,
                        replicas=_normalize_resolved_replicas(
                            (
                                {
                                    "control_addr": control_addr,
                                    "node_id": str(entry.get("node_id", "") or data_ref.node_id or ""),
                                    "node_instance_id": str(entry.get("node_instance_id", "") or data_ref.node_instance_id or ""),
                                },
                            )
                        ),
                    )

                nodes = list(client.list_nodes(healthy_only=False, limit=2000))

            node_instance_id = str(data_ref.node_instance_id or "").strip()
            node_id = str(data_ref.node_id or "").strip()
            if node_instance_id:
                matches = [
                    node
                    for node in nodes
                    if str(getattr(node, "node_instance_id", "") or "").strip() == node_instance_id
                    and not _node_known_unhealthy(node)
                ]
                if len(matches) == 1:
                    node = matches[0]
                    control_addr = str(getattr(node, "control_addr", "") or "").strip()
                    if control_addr:
                        return ResolvedDataRef(
                            ref=data_ref,
                            control_addr=control_addr,
                            node_id=str(getattr(node, "node_id", "") or node_id),
                            node_instance_id=node_instance_id,
                            locator_kind=locator_kind,
                            locator_token=locator_token,
                            via_registry=True,
                            replicas=_normalize_resolved_replicas(
                                (
                                    {
                                        "control_addr": control_addr,
                                        "node_id": str(getattr(node, "node_id", "") or node_id),
                                        "node_instance_id": node_instance_id,
                                    },
                                )
                            ),
                        )
                if len(matches) > 1:
                    raise RuntimeError(f"data ref resolution is ambiguous for node_instance_id={node_instance_id!r}")

            if node_id:
                matches = [
                    node
                    for node in nodes
                    if str(getattr(node, "node_id", "") or "").strip() == node_id
                    and not _node_known_unhealthy(node)
                ]
                if len(matches) == 1:
                    node = matches[0]
                    control_addr = str(getattr(node, "control_addr", "") or "").strip()
                    if control_addr:
                        return ResolvedDataRef(
                            ref=data_ref,
                            control_addr=control_addr,
                            node_id=node_id,
                            node_instance_id=str(getattr(node, "node_instance_id", "") or ""),
                            locator_kind=locator_kind,
                            locator_token=locator_token,
                            via_registry=True,
                            replicas=_normalize_resolved_replicas(
                                (
                                    {
                                        "control_addr": control_addr,
                                        "node_id": node_id,
                                        "node_instance_id": str(getattr(node, "node_instance_id", "") or ""),
                                    },
                                )
                            ),
                        )
                if len(matches) > 1:
                    raise RuntimeError(
                        f"data ref resolution is ambiguous for node_id={node_id!r}; use node_instance_id-backed refs"
                    )
            raise RuntimeError(
                f"data ref could not be resolved via controlplane target={locator_token!r}: "
                f"ref_id={data_ref.ref_id!r} node_id={node_id!r} node_instance_id={node_instance_id!r}"
            )

        raise RuntimeError(
            f"data ref is missing a resolvable locator: ref_id={data_ref.ref_id!r} locator_kind={locator_kind!r}"
        )


def resolve_data_ref(ref: DataRef | object, *, target: str = "", timeout_sec: float = 10.0) -> ResolvedDataRef:
    return DataRegistryClient(target, timeout_sec=timeout_sec).resolve(ref)


__all__ = [
    "DataRegistryClient",
    "ResolvedDataRef",
    "resolve_data_ref",
]
