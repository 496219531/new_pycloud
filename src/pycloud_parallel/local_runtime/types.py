from __future__ import annotations

"""中文说明：跨模块共享的数据结构与异常类型。"""

from dataclasses import dataclass, field
from typing import Any, List


@dataclass
class TaskError:
    """任务错误信息数据类。

    记录单个任务执行失败时的详细信息。

    Attributes:
        index: 任务在原始迭代器中的索引
        item_repr: 项目的字符串表示
        error: 错误信息
        attempts: 尝试次数
    """
    index: int
    item_repr: str
    error: str
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
