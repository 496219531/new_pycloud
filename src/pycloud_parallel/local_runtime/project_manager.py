from __future__ import annotations

"""中文说明：项目级并发隔离管理器。

通过每个项目一个 BoundedSemaphore 控制并发上限，
满足“多个项目同时跑但互不抢占”的需求。
"""

import threading
from dataclasses import dataclass
from typing import Dict, Optional

from .config import ProjectConfig


@dataclass
class _ProjectState:
    """项目内部状态。

    Attributes:
        config: 项目配置
        semaphore: 并发控制信号量
    """
    config: ProjectConfig
    semaphore: threading.BoundedSemaphore


class ProjectManager:
    """项目管理器。

    通过每个项目一个 BoundedSemaphore 控制并发上限，
    满足"多个项目同时跑但互不抢占"的需求。

    Attributes:
        _lock: 线程锁
        _projects: 项目名称到状态的映射
    """

    def __init__(self, projects: Dict[str, ProjectConfig]) -> None:
        self._lock = threading.Lock()
        self._projects: Dict[str, _ProjectState] = {}
        for config in projects.values():
            self.register(config)

    def register(self, config: ProjectConfig) -> None:
        """注册一个新项目。

        Args:
            config: 项目配置
        """
        with self._lock:
            self._projects[config.name] = _ProjectState(
                config=config,
                semaphore=threading.BoundedSemaphore(value=max(1, config.cpu_quota)),
            )

    def ensure(self, name: str, default_cpu: int = 1) -> None:
        """确保项目存在，不存在则创建。

        惰性创建项目，避免调用端必须先注册才能运行。

        Args:
            name: 项目名称
            default_cpu: 默认 CPU 配额
        """
        # 惰性创建项目，避免调用端必须先注册才能运行。
        with self._lock:
            if name in self._projects:
                return
            cfg = ProjectConfig(name=name, cpu_quota=max(1, default_cpu))
            self._projects[name] = _ProjectState(
                config=cfg,
                semaphore=threading.BoundedSemaphore(value=cfg.cpu_quota),
            )

    def acquire(self, name: str, timeout: Optional[float] = None) -> bool:
        """获取项目并发令牌。

        拿不到会阻塞，从而形成天然限流。

        Args:
            name: 项目名称
            timeout: 超时时间（秒），None 表示无限等待

        Returns:
            bool: 是否成功获取令牌

        Raises:
            KeyError: 当项目不存在时
        """
        # 获取项目并发令牌：拿不到会阻塞，从而形成天然限流。
        with self._lock:
            state = self._projects.get(name)
            if state is None:
                raise KeyError(f"project `{name}` is not registered")
            sem = state.semaphore
        if timeout is None:
            return sem.acquire()
        return sem.acquire(timeout=timeout)

    def release(self, name: str) -> None:
        """释放项目并发令牌。

        Args:
            name: 项目名称
        """
        with self._lock:
            state = self._projects.get(name)
            if state is None:
                return
            sem = state.semaphore
        try:
            sem.release()
        except ValueError:
            return

    def names(self) -> list:
        """获取所有项目名称列表。

        Returns:
            list: 项目名称列表
        """
        with self._lock:
            return list(self._projects.keys())
