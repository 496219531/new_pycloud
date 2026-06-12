from __future__ import annotations

"""Lightweight resource progress/readiness signal primitives.

ResourceSignalStore is for long-running resource operations, not lease traffic:
progress/failure/stop are retained as an operation log, readiness is coalesced
as the current routing gate, and heartbeat must stay outside this store.
"""

from collections import OrderedDict, deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import threading
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple


RESOURCE_SIGNAL_COALESCE_TYPES = {"readiness"}
RESOURCE_SIGNAL_RETAIN_TYPES = {"progress", "failure", "stop"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class ResourceSignal:
    seq: int = 0
    node_instance_id: str = ""
    resource_kind: str = ""
    resource_id: str = ""
    epoch: int = 0
    signal_type: str = ""
    state: str = ""
    reason: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    updated_at: datetime = field(default_factory=utc_now)

    def with_seq(self, seq: int, *, updated_at: Optional[datetime] = None) -> "ResourceSignal":
        return replace(self, seq=int(seq), updated_at=updated_at or utc_now())

    def as_dict(self) -> Dict[str, Any]:
        return {
            "seq": int(self.seq or 0),
            "node_instance_id": str(self.node_instance_id or ""),
            "resource_kind": str(self.resource_kind or ""),
            "resource_id": str(self.resource_id or ""),
            "epoch": int(self.epoch or 0),
            "signal_type": str(self.signal_type or ""),
            "state": str(self.state or ""),
            "reason": str(self.reason or ""),
            "payload": dict(self.payload or {}),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass
class ResourceOperation:
    op_id: str
    op_type: str
    resource_kind: str
    resource_id: str
    status: str = "accepted"
    stage: str = ""
    last_signal_seq: int = 0
    error: str = ""
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "op_id": str(self.op_id or ""),
            "op_type": str(self.op_type or ""),
            "resource_kind": str(self.resource_kind or ""),
            "resource_id": str(self.resource_id or ""),
            "status": str(self.status or ""),
            "stage": str(self.stage or ""),
            "last_signal_seq": int(self.last_signal_seq or 0),
            "error": str(self.error or ""),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class ResourceSignalStore:
    def __init__(self, *, maxlen: int = 2000) -> None:
        self._maxlen = max(1, int(maxlen or 1))
        self._seq = 0
        self._signals: Deque[ResourceSignal] = deque()
        self._latest: Dict[Tuple[str, str], ResourceSignal] = {}
        self._coalesced_index: Dict[Tuple[str, str, str], ResourceSignal] = {}
        self._operations: "OrderedDict[str, ResourceOperation]" = OrderedDict()
        self._lock = threading.Lock()

    @property
    def cursor(self) -> int:
        with self._lock:
            return int(self._seq)

    def publish(self, signal: ResourceSignal) -> int:
        with self._lock:
            self._seq += 1
            published = signal.with_seq(self._seq)
            key = (published.resource_kind, published.resource_id)
            self._latest[key] = published
            signal_type = str(published.signal_type or "")
            if signal_type in RESOURCE_SIGNAL_COALESCE_TYPES:
                self._coalesced_index[(published.resource_kind, published.resource_id, signal_type)] = published
            elif signal_type in RESOURCE_SIGNAL_RETAIN_TYPES:
                self._signals.append(published)
            self._trim_locked()
            return published.seq

    def latest(self, resource_kind: str, resource_id: str) -> Optional[ResourceSignal]:
        with self._lock:
            return self._latest.get((str(resource_kind or ""), str(resource_id or "")))

    def since(self, cursor: int, *, limit: int = 100) -> List[ResourceSignal]:
        normalized_cursor = max(0, int(cursor or 0))
        normalized_limit = max(1, int(limit or 100))
        with self._lock:
            retained = list(self._signals)
            coalesced = list(self._coalesced_index.values())
            items = [signal for signal in [*retained, *coalesced] if int(signal.seq or 0) > normalized_cursor]
            items.sort(key=lambda signal: int(signal.seq or 0))
            return items[:normalized_limit]

    def snapshot(self, *, resource_kind: str = "", resource_id: str = "") -> List[ResourceSignal]:
        kind = str(resource_kind or "")
        rid = str(resource_id or "")
        with self._lock:
            items = list(self._latest.values())
        if kind:
            items = [item for item in items if item.resource_kind == kind]
        if rid:
            items = [item for item in items if item.resource_id == rid]
        items.sort(key=lambda signal: int(signal.seq or 0))
        return items

    def upsert_operation(self, operation: ResourceOperation) -> ResourceOperation:
        with self._lock:
            existing = self._operations.get(operation.op_id)
            if existing is not None:
                existing.op_type = operation.op_type or existing.op_type
                existing.resource_kind = operation.resource_kind or existing.resource_kind
                existing.resource_id = operation.resource_id or existing.resource_id
                existing.status = operation.status or existing.status
                existing.stage = operation.stage or existing.stage
                existing.last_signal_seq = int(operation.last_signal_seq or existing.last_signal_seq or 0)
                existing.error = operation.error or existing.error
                existing.updated_at = operation.updated_at or utc_now()
                self._operations.move_to_end(operation.op_id)
                return existing
            self._operations[operation.op_id] = operation
            self._trim_operations_locked()
            return operation

    def get_operation(self, op_id: str) -> Optional[ResourceOperation]:
        with self._lock:
            return self._operations.get(str(op_id or ""))

    def operations_snapshot(self, *, resource_kind: str = "", resource_id: str = "") -> List[ResourceOperation]:
        kind = str(resource_kind or "")
        rid = str(resource_id or "")
        with self._lock:
            items = list(self._operations.values())
        if kind:
            items = [item for item in items if item.resource_kind == kind]
        if rid:
            items = [item for item in items if item.resource_id == rid]
        return items

    def _trim_locked(self) -> None:
        while len(self._signals) > self._maxlen:
            self._signals.popleft()
        if len(self._coalesced_index) <= self._maxlen:
            return
        items = sorted(self._coalesced_index.items(), key=lambda item: int(item[1].seq or 0))
        for key, _signal in items[: max(0, len(items) - self._maxlen)]:
            self._coalesced_index.pop(key, None)

    def _trim_operations_locked(self) -> None:
        while len(self._operations) > self._maxlen:
            self._operations.popitem(last=False)


def signals_to_dicts(signals: Iterable[ResourceSignal]) -> List[Dict[str, Any]]:
    return [signal.as_dict() for signal in signals]


__all__ = [
    "ResourceOperation",
    "ResourceSignal",
    "ResourceSignalStore",
    "signals_to_dicts",
    "utc_now",
]
