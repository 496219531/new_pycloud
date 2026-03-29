from __future__ import annotations

"""中文说明：跨模块共享的数据结构与异常类型。"""

from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class TaskError:
    """任务错误信息数据类。

    记录单个任务执行失败时的详细信息。

    Attributes:
        index: 任务在原始迭代器中的索引
        item_repr: 项目的字符串表示
        error: 错误信息
        cluster: 执行该任务的集群
        attempts: 尝试次数
    """
    index: int
    item_repr: str
    error: str
    cluster: str
    attempts: int = 0


@dataclass
class ForeachResult:
    """Foreach 操作结果数据类。

    包含执行结果和错误信息。

    Attributes:
        values: 成功执行的结果列表
        errors: 错误列表
    """
    values: List[Any] = field(default_factory=list)
    errors: List[TaskError] = field(default_factory=list)


class UserFunctionError(RuntimeError):
    """用户函数执行错误。

    用户函数执行失败时抛出（任务级错误），用于区分与集群级故障。

    Attributes:
        index: 任务索引
        item_repr: 项目表示
        cluster: 集群名称
        cause: 错误原因
    """
    # 用户函数执行失败（任务级），用于区分与集群级故障。
    def __init__(self, index: int, item_repr: str, cluster: str, cause: str) -> None:
        super().__init__(f"user function failed at index={index} on cluster={cluster}: {cause}")
        self.index = index
        self.item_repr = item_repr
        self.cluster = cluster
        self.cause = cause


class ClusterExecutionError(RuntimeError):
    """集群执行错误。

    集群级故障时抛出，通常用于故障转移场景。
    """
    pass


@dataclass
class ChunkMeta:
    """分片元数据。

    记录每个分片在调度过程中的信息，用于故障转移与追踪。

    Attributes:
        indexed_items: 带索引的项目列表
        cluster: 执行该分片的集群
        failovers: 已进行的故障转移次数
        excluded_clusters: 被排除的集群集合
    """
    # 每个分片在调度过程中的元数据，用于 failover 与追踪。
    indexed_items: List[tuple]
    cluster: str
    failovers: int = 0
    excluded_clusters: Optional[set] = None
