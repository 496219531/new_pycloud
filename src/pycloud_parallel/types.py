from __future__ import annotations

"""中文说明：跨模块共享的数据结构与异常类型。"""

from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class TaskError:
    index: int
    item_repr: str
    error: str
    cluster: str
    attempts: int = 0


@dataclass
class ForeachResult:
    values: List[Any] = field(default_factory=list)
    errors: List[TaskError] = field(default_factory=list)


class UserFunctionError(RuntimeError):
    # 用户函数执行失败（任务级），用于区分与集群级故障。
    def __init__(self, index: int, item_repr: str, cluster: str, cause: str) -> None:
        super().__init__(f"user function failed at index={index} on cluster={cluster}: {cause}")
        self.index = index
        self.item_repr = item_repr
        self.cluster = cluster
        self.cause = cause


class ClusterExecutionError(RuntimeError):
    pass


@dataclass
class ChunkMeta:
    # 每个分片在调度过程中的元数据，用于 failover 与追踪。
    indexed_items: List[tuple]
    cluster: str
    failovers: int = 0
    excluded_clusters: Optional[set] = None
