from __future__ import annotations

"""中文说明：多集群网关（元调度器）。

职责：维护多个集群 Runner，按 weighted_least_load 路由，
并记录健康状态/在途任务数/提交量，为 failover 提供依据。
"""

import threading
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Set, Tuple

from .config import ClusterConfig
from .runners import ProcessClusterRunner


@dataclass
class ClusterStats:
    """集群统计数据类。

    跟踪单个集群的运行时统计信息。

    Attributes:
        in_flight: 正在执行的任务数
        failures: 失败次数
        unhealthy_until: 不健康状态截止时间（Unix 时间戳）
        submitted: 已提交的任务总数
    """
    in_flight: int = 0
    failures: int = 0
    unhealthy_until: float = 0.0
    submitted: int = 0


class ClusterGateway:
    """多集群网关（元调度器）。

    负责维护多个集群 Runner，按 weighted_least_load 策略路由任务，
    记录健康状态、在途任务数和提交量，为故障转移提供依据。

    Attributes:
        _lock: 线程锁，保护内部状态
        _runners: 集群名称到 Runner 的映射
        _configs: 集群名称到配置的映射
        _stats: 集群名称到统计数据的映射
    """

    def __init__(self, clusters: Iterable[ClusterConfig]) -> None:
        self._lock = threading.Lock()
        self._runners: Dict[str, ProcessClusterRunner] = {}
        self._configs: Dict[str, ClusterConfig] = {}
        self._stats: Dict[str, ClusterStats] = {}

        for cfg in clusters:
            self._configs[cfg.name] = cfg
            self._runners[cfg.name] = ProcessClusterRunner(cfg)
            self._stats[cfg.name] = ClusterStats()

        if not self._runners:
            raise RuntimeError("at least one cluster is required")

    def shutdown(self) -> None:
        """关闭所有集群 Runner。"""
        for runner in self._runners.values():
            runner.shutdown()

    def total_parallelism(self) -> int:
        """获取总并行度。

        Returns:
            int: 所有集群的容量之和
        """
        return max(1, sum(cfg.capacity for cfg in self._configs.values()))

    def mark_unhealthy(self, cluster: str, cooldown_sec: int = 15) -> None:
        """标记集群为不健康状态。

        在冷却窗口内该集群不会被优先选择，避免持续雪崩。

        Args:
            cluster: 集群名称
            cooldown_sec: 冷却时间（秒）
        """
        # 冷却窗口内不再优先选该集群，避免持续雪崩。
        with self._lock:
            stats = self._stats.get(cluster)
            if stats is None:
                return
            stats.failures += 1
            stats.unhealthy_until = time.time() + cooldown_sec

    def _healthy_candidates(self, exclude: Optional[Set[str]] = None) -> Dict[str, ClusterConfig]:
        """获取健康的集群候选列表。

        Args:
            exclude: 要排除的集群集合

        Returns:
            Dict[str, ClusterConfig]: 健康集群的配置字典
        """
        now = time.time()
        excluded = exclude or set()
        with self._lock:
            candidates = {}
            for name, cfg in self._configs.items():
                if name in excluded:
                    continue
                stats = self._stats[name]
                if stats.unhealthy_until > now:
                    continue
                candidates[name] = cfg
        return candidates

    def select_cluster(
        self,
        policy: str = "weighted_least_load",
        exclude: Optional[Set[str]] = None,
    ) -> str:
        """根据策略选择一个集群。

        Args:
            policy: 选择策略（当前仅支持 "weighted_least_load"）
            exclude: 要排除的集群集合

        Returns:
            str: 选中的集群名称
        """
        # weighted_least_load：低负载且高权重集群优先。
        candidates = self._healthy_candidates(exclude=exclude)
        if not candidates:
            if exclude:
                for name in self._configs:
                    if name not in exclude:
                        return name
            return next(iter(self._configs.keys()))

        if policy != "weighted_least_load":
            return next(iter(candidates.keys()))

        best_name = None
        best_score = None
        with self._lock:
            for name, cfg in candidates.items():
                stats = self._stats[name]
                load_penalty = 1000.0 if stats.in_flight >= cfg.capacity else 0.0
                score = ((stats.in_flight + 1) / max(0.1, cfg.weight)) + load_penalty
                if best_score is None or score < best_score:
                    best_name = name
                    best_score = score

        if best_name is None:
            return next(iter(candidates.keys()))
        return best_name

    def submit(
        self,
        serialized_fn: bytes,
        indexed_items: list,
        retries: int,
        on_error: str,
        policy: str = "weighted_least_load",
        force_cluster: Optional[str] = None,
        exclude: Optional[Set[str]] = None,
    ) -> Tuple[object, str]:
        """提交任务到集群执行。

        Args:
            serialized_fn: 序列化的函数
            indexed_items: 带索引的项目列表
            retries: 重试次数
            on_error: 错误处理策略
            policy: 集群选择策略
            force_cluster: 强制指定的集群（可选）
            exclude: 要排除的集群集合

        Returns:
            Tuple[object, str]: (Future 对象, 集群名称)
        """
        # 提交前后维护 in_flight 计数，供后续调度决策使用。
        cluster_name = force_cluster or self.select_cluster(policy=policy, exclude=exclude)
        runner = self._runners[cluster_name]

        with self._lock:
            self._stats[cluster_name].in_flight += 1
            self._stats[cluster_name].submitted += 1

        future = runner.submit_chunk(
            serialized_fn=serialized_fn,
            indexed_items=indexed_items,
            retries=retries,
            on_error=on_error,
        )

        def _on_done(_future) -> None:
            with self._lock:
                stats = self._stats[cluster_name]
                stats.in_flight = max(0, stats.in_flight - 1)
                if _future.exception() is not None:
                    stats.failures += 1

        future.add_done_callback(_on_done)
        return future, cluster_name

    def snapshot(self) -> Dict[str, ClusterStats]:
        """获取所有集群的统计快照。

        Returns:
            Dict[str, ClusterStats]: 集群名称到统计数据的映射
        """
        with self._lock:
            return {name: ClusterStats(**vars(stats)) for name, stats in self._stats.items()}
