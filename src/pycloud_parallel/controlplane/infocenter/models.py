from __future__ import annotations

"""InfoCenter-facing state models."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pycloud_parallel.controlplane.node_capability import NodeCapability
from pycloud_parallel.controlplane.state_time import utc_now


@dataclass
class NodeMetricsState:
    queued: int = 0
    inflight: int = 0
    running: int = 0
    credit: int = 0
    cpu_percent: float = 0.0
    mem_percent: float = 0.0


@dataclass
class NodeServiceState:
    service_name: str
    service_id: str
    status: int
    policy_id: str = "default_safe"
    owner_client_id: str = ""
    code_version: str = ""
    entry_module: str = ""
    entry_callable: str = ""
    serialization_mode: str = ""
    status_text: str = ""
    resource_health: str = ""
    degraded: bool = False
    worker_count: int = 0
    alive_workers: int = 0
    in_flight: int = 0
    received_count: int = 0
    returned_count: int = 0
    ema_child_invoke_ms: float = 0.0
    ema_samples: int = 0
    lease_expire_at: datetime = field(default_factory=utc_now)
    http_base_url: str = ""
    stop_reason: str = ""
    failure_at: Optional[datetime] = None
    readiness: str = ""
    readiness_reason: str = ""
    create_stage: str = ""
    operation_id: str = ""
    operation_updated_at: Optional[datetime] = None
    signal_cursor: int = 0


@dataclass
class NodeTaskPoolInfo:
    pool_id: str
    owner_client_id: str
    pool_name: str
    code_version: str
    status: str
    resource_health: str = ""
    degraded: bool = False
    worker_count: int = 0
    alive_workers: int = 0
    task_count: int = 0
    inflight: int = 0
    received_count: int = 0
    returned_count: int = 0
    ema_child_invoke_ms: float = 0.0
    ema_samples: int = 0
    created_at: datetime = field(default_factory=utc_now)
    last_heartbeat_at: datetime = field(default_factory=utc_now)
    lease_expire_at: datetime = field(default_factory=utc_now)
    stop_reason: str = ""
    failure_reason: str = ""
    failure_at: Optional[datetime] = None
    readiness: str = ""
    readiness_reason: str = ""
    create_stage: str = ""
    operation_id: str = ""
    operation_updated_at: Optional[datetime] = None
    signal_cursor: int = 0


@dataclass
class NodeProfile:
    profile_key: str
    managed_tags: List[str] = field(default_factory=list)
    enabled: bool = True
    drain: bool = False
    notes: str = ""


@dataclass
class NodeState:
    node_instance_id: str
    node_id: str
    control_addr: str
    capacity: int
    queue_capacity: int
    tags: List[str] = field(default_factory=list)
    profile_key: str = ""
    managed_tags: List[str] = field(default_factory=list)
    capability_tags: List[str] = field(default_factory=list)
    legacy_node_tags: List[str] = field(default_factory=list)
    profile_enabled: bool = True
    profile_notes: str = ""
    version: str = ""
    python_version: str = ""
    metadata: Dict[str, str] = field(default_factory=dict)
    healthy: bool = True
    last_seen_at: datetime = field(default_factory=utc_now)
    metrics: NodeMetricsState = field(default_factory=NodeMetricsState)
    services: Dict[str, NodeServiceState] = field(default_factory=dict)
    task_pools: Dict[str, NodeTaskPoolInfo] = field(default_factory=dict)
    active_runtimes: List[str] = field(default_factory=list)
    service_worker_capacity: int = 0
    service_worker_used: int = 0
    task_pool_worker_capacity: int = 0
    task_pool_worker_used: int = 0
    accept_service_deploy: bool = True
    schedulable: bool = True
    drain: bool = False
    reason: str = ""
    capability: NodeCapability = field(default_factory=NodeCapability)

    def service_worker_available(self) -> int:
        capacity = max(0, int(self.service_worker_capacity or 0))
        used = max(0, int(self.service_worker_used or 0))
        return max(0, capacity - used)

    def active_runtime_count(self) -> int:
        return len(self.active_runtimes)

    def task_pool_worker_available(self) -> int:
        capacity = max(0, int(self.task_pool_worker_capacity or 0))
        used = max(0, int(self.task_pool_worker_used or 0))
        return max(0, capacity - used)


@dataclass
class FencedNodeInstance:
    """Deprecated registry artifact; InfoCenter no longer fences NodeControl."""

    node_instance_id: str
    fenced_at: datetime = field(default_factory=utc_now)
    reason: str = ""


@dataclass(frozen=True)
class DataRegistryEntry:
    ref_id: str
    storage_id: str
    logical_type: str
    format: str
    size_bytes: int
    materialize_as: str
    locator_kind: str
    locator_token: str
    consume_on_read: bool
    node_id: str = ""
    node_instance_id: str = ""
    control_addr: str = ""
    replicas: Tuple[Dict[str, object], ...] = ()
    created_at: datetime = field(default_factory=utc_now)
    last_at: datetime = field(default_factory=utc_now)
    ttl_sec: int = 3600


__all__ = [
    "DataRegistryEntry",
    "FencedNodeInstance",
    "NodeMetricsState",
    "NodeProfile",
    "NodeCapability",
    "NodeServiceState",
    "NodeState",
    "NodeTaskPoolInfo",
]
