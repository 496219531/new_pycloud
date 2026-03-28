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
    in_flight: int = 0
    failures: int = 0
    unhealthy_until: float = 0.0
    submitted: int = 0


class ClusterGateway:
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
        for runner in self._runners.values():
            runner.shutdown()

    def total_parallelism(self) -> int:
        return max(1, sum(cfg.capacity for cfg in self._configs.values()))

    def mark_unhealthy(self, cluster: str, cooldown_sec: int = 15) -> None:
        # 冷却窗口内不再优先选该集群，避免持续雪崩。
        with self._lock:
            stats = self._stats.get(cluster)
            if stats is None:
                return
            stats.failures += 1
            stats.unhealthy_until = time.time() + cooldown_sec

    def _healthy_candidates(self, exclude: Optional[Set[str]] = None) -> Dict[str, ClusterConfig]:
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
        with self._lock:
            return {name: ClusterStats(**vars(stats)) for name, stats in self._stats.items()}
