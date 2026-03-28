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
    config: ProjectConfig
    semaphore: threading.BoundedSemaphore


class ProjectManager:
    def __init__(self, projects: Dict[str, ProjectConfig]) -> None:
        self._lock = threading.Lock()
        self._projects: Dict[str, _ProjectState] = {}
        for config in projects.values():
            self.register(config)

    def register(self, config: ProjectConfig) -> None:
        with self._lock:
            self._projects[config.name] = _ProjectState(
                config=config,
                semaphore=threading.BoundedSemaphore(value=max(1, config.cpu_quota)),
            )

    def get(self, name: str) -> ProjectConfig:
        with self._lock:
            if name not in self._projects:
                raise KeyError(f"project `{name}` is not registered")
            return self._projects[name].config

    def ensure(self, name: str, default_cpu: int = 1) -> None:
        # 惰性创建项目，避免调用端必须先注册才能运行。
        with self._lock:
            if name in self._projects:
                return
            cfg = ProjectConfig(name=name, cpu_quota=max(1, default_cpu), mem_quota=0, priority=1)
            self._projects[name] = _ProjectState(
                config=cfg,
                semaphore=threading.BoundedSemaphore(value=cfg.cpu_quota),
            )

    def acquire(self, name: str, timeout: Optional[float] = None) -> bool:
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
        with self._lock:
            return list(self._projects.keys())
