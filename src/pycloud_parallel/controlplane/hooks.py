from __future__ import annotations

"""Result hook abstractions for NodeControl."""

import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Tuple

from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


@dataclass(frozen=True)
class QueuedResult:
    """队列结果项。

    Attributes:
        seq: 序列号
        result: 任务结果
    """
    seq: int
    result: pb2.TaskResult


class InMemoryResultHook:
    """默认的内存结果钩子。

    使用有界队列存储每个客户端的结果，避免无限内存增长。
    当队列满时，丢弃最旧的结果。

    Attributes:
        _lock: 线程锁
        _cv: 条件变量
        _per_client_limit: 每个客户端的结果数量限制
        _queues: 客户端 ID 到结果队列的映射
        _seq: 全局序列号计数器
    """

    def __init__(self, per_client_limit: int = 20_000) -> None:
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._per_client_limit = max(1, per_client_limit)
        self._queues: Dict[str, Deque[QueuedResult]] = defaultdict(deque)
        self._seq = 0

    def _next_seq(self) -> int:
        """生成下一个序列号。

        Returns:
            int: 新的序列号
        """
        self._seq += 1
        return self._seq

    def on_result(self, client_id: str, item: QueuedResult) -> None:
        """处理单个完成的任务结果。

        Args:
            client_id: 客户端 ID
            item: 队列结果项
        """
        with self._cv:
            if int(item.seq or 0) <= 0:
                item = QueuedResult(seq=self._next_seq(), result=item.result)
            q = self._queues[client_id]
            if len(q) >= self._per_client_limit:
                q.popleft()
            q.append(item)
            self._cv.notify_all()

    def push(self, client_id: str, result: pb2.TaskResult) -> int:
        """推送一个结果到队列。

        Args:
            client_id: 客户端 ID
            result: 任务结果

        Returns:
            int: 分配的序列号
        """
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
        """拉取一批结果。

        Args:
            client_id: 客户端 ID
            limit: 批次大小限制
            wait_ms: 等待时间（毫秒）
            cursor: 当前游标

        Returns:
            Tuple[List[pb2.TaskResult], str]: (结果列表, 下一个游标)
        """
        timeout = max(0.0, wait_ms / 1000.0)
        try:
            cursor_seq = int(str(cursor or "0") or "0")
        except Exception:
            cursor_seq = 0

        with self._cv:
            if not self._has_new_locked(client_id, cursor_seq) and timeout > 0:
                self._cv.wait(timeout=timeout)

            q = self._queues[client_id]
            out: List[pb2.TaskResult] = []
            last_seq = 0
            max_items = max(1, limit)
            while q and len(out) < max_items:
                item = q.popleft()
                out.append(item.result)
                last_seq = item.seq
            return out, str(last_seq)

    def _has_new_locked(self, client_id: str, seq: int) -> bool:
        """检查是否有新结果（需要在锁内调用）。

        Args:
            client_id: 客户端 ID
            seq: 起始序列号

        Returns:
            bool: 是否有新结果
        """
        q = self._queues.get(client_id)
        if not q:
            return False
        return q[-1].seq > seq

    def clear_client(self, client_id: str) -> None:
        """清除客户端的结果队列。

        Args:
            client_id: 客户端 ID
        """
        with self._cv:
            self._queues.pop(client_id, None)
