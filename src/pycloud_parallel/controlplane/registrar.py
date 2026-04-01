from __future__ import annotations

"""Background registrar for NodeControl -> InfoCenter heartbeats."""

import threading
from typing import Dict, Iterable, Optional

from pycloud_parallel.controlplane.client import InfoCenterClient
from pycloud_parallel.controlplane.state import NodeControlState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


class NodeInfoCenterRegistrar:
    def __init__(
        self,
        *,
        infocenter_addr: str,
        node_id: str,
        control_addr: str,
        state: NodeControlState,
        capacity: int,
        queue_capacity: int,
        tags: Optional[Iterable[str]] = None,
        version: str = "",
        metadata: Optional[Dict[str, str]] = None,
        fallback_heartbeat_sec: int = 10,
        rpc_timeout_sec: float = 5.0,
    ) -> None:
        self.infocenter_addr = infocenter_addr
        self.node_id = node_id
        self.control_addr = control_addr
        self.state = state
        self.capacity = max(1, int(capacity))
        self.queue_capacity = max(1, int(queue_capacity))
        self.tags = list(tags or [])
        self.version = version
        self.metadata = dict(metadata or {})
        self.fallback_heartbeat_sec = max(1, int(fallback_heartbeat_sec))
        self.rpc_timeout_sec = max(0.5, float(rpc_timeout_sec))

        self._client = InfoCenterClient(self.infocenter_addr, timeout_sec=self.rpc_timeout_sec)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._registered = False
        self._next_hb_sec = self.fallback_heartbeat_sec

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, name=f"node-registrar-{self.node_id}", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        self._client.close()

    def _register_once(self) -> bool:
        resp = self._client.register_node(
            node_id=self.node_id,
            control_addr=self.control_addr,
            capacity=self.capacity,
            queue_capacity=self.queue_capacity,
            tags=self.tags,
            version=self.version,
            metadata=self.metadata,
            services=self.state.service_reports(),
            active_runtimes=self.state.active_runtime_keys(limit=10),
            service_worker_capacity=self.state.service_worker_capacity,
            service_worker_used=self.state.service_worker_used(),
        )
        self._registered = True
        self._next_hb_sec = max(1, int(resp.get("heartbeat_interval_sec", self.fallback_heartbeat_sec) or self.fallback_heartbeat_sec))
        return True

    def _heartbeat_once(self) -> bool:
        metrics = self.state.metrics()
        resp = self._client.heartbeat_node(
            node_id=self.node_id,
            healthy=True,
            metrics={
                "queued": metrics["queued"],
                "inflight": metrics["inflight"],
                "running": metrics["running"],
                "credit": metrics["credit"],
                "cpu_percent": 0.0,
                "mem_percent": 0.0,
            },
            services=self.state.service_reports(),
            active_runtimes=self.state.active_runtime_keys(limit=10),
            service_worker_capacity=self.state.service_worker_capacity,
            service_worker_used=self.state.service_worker_used(),
        )
        if not resp.get("accepted", False):
            self._registered = False
            return False
        self._next_hb_sec = max(1, int(resp.get("next_heartbeat_in_sec", self.fallback_heartbeat_sec) or self.fallback_heartbeat_sec))
        return True

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if not self._registered:
                    self._register_once()
                else:
                    self._heartbeat_once()
            except Exception:
                self._registered = False

            wait_sec = self._next_hb_sec if self._registered else self.fallback_heartbeat_sec
            self._stop_event.wait(max(1, int(wait_sec)))
