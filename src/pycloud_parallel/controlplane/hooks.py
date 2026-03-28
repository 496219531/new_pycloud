from __future__ import annotations

"""Result hook abstractions for NodeControl."""

import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Protocol, Tuple

from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


@dataclass(frozen=True)
class QueuedResult:
    seq: int
    result: pb2.TaskResult


class ResultHook(Protocol):
    def on_result(self, client_id: str, item: QueuedResult) -> None:
        """Handle one finished task result."""

    def pull(
        self,
        client_id: str,
        *,
        limit: int,
        wait_ms: int,
        cursor: str,
    ) -> Tuple[List[pb2.TaskResult], str]:
        """Return a batch of results and next cursor."""


class InMemoryResultHook:
    """Default in-memory result sink.

    This is intentionally bounded to avoid unlimited memory growth.
    When full, the oldest result for that client is dropped.
    """

    def __init__(self, per_client_limit: int = 20_000) -> None:
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._per_client_limit = max(1, per_client_limit)
        self._queues: Dict[str, Deque[QueuedResult]] = defaultdict(deque)
        self._seq = 0

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def on_result(self, client_id: str, item: QueuedResult) -> None:
        with self._cv:
            q = self._queues[client_id]
            if len(q) >= self._per_client_limit:
                q.popleft()
            q.append(item)
            self._cv.notify_all()

    def push(self, client_id: str, result: pb2.TaskResult) -> int:
        with self._cv:
            seq = self._next_seq()
            q = self._queues[client_id]
            if len(q) >= self._per_client_limit:
                q.popleft()
            q.append(QueuedResult(seq=seq, result=result))
            self._cv.notify_all()
            return seq

    def pull(
        self,
        client_id: str,
        *,
        limit: int,
        wait_ms: int,
        cursor: str,
    ) -> Tuple[List[pb2.TaskResult], str]:
        timeout = max(0.0, wait_ms / 1000.0)
        start_seq = 0
        if cursor:
            try:
                start_seq = int(cursor)
            except ValueError:
                start_seq = 0

        with self._cv:
            if not self._has_new_locked(client_id, start_seq) and timeout > 0:
                self._cv.wait(timeout=timeout)

            q = self._queues[client_id]
            out: List[pb2.TaskResult] = []
            last_seq = start_seq
            for item in q:
                if item.seq <= start_seq:
                    continue
                out.append(item.result)
                last_seq = item.seq
                if len(out) >= max(1, limit):
                    break
            return out, str(last_seq)

    def _has_new_locked(self, client_id: str, seq: int) -> bool:
        q = self._queues.get(client_id)
        if not q:
            return False
        return q[-1].seq > seq

    def clear_client(self, client_id: str) -> None:
        with self._cv:
            self._queues.pop(client_id, None)
