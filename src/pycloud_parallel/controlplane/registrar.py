from __future__ import annotations

"""Background registrar for NodeControl -> InfoCenter heartbeats."""

import threading
from typing import Dict, Iterable, Optional

import grpc

from pycloud_parallel.controlplane.state import NodeControlState, dt_to_ts, utc_now
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


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

        self._channel = grpc.insecure_channel(self.infocenter_addr)
        self._stub = pb2_grpc.InfoCenterServiceStub(self._channel)
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
        self._channel.close()

    def _register_once(self) -> bool:
        req = pb2.RegisterNodeRequest(
            node_id=self.node_id,
            control_addr=self.control_addr,
            capacity=self.capacity,
            queue_capacity=self.queue_capacity,
            tags=self.tags,
            version=self.version,
            metadata=self.metadata,
            services=self.state.service_reports(),
        )
        resp = self._stub.RegisterNode(req, timeout=self.rpc_timeout_sec)
        if not resp.ok:
            return False
        self._registered = True
        self._next_hb_sec = max(1, int(resp.heartbeat_interval_sec or self.fallback_heartbeat_sec))
        return True

    def _heartbeat_once(self) -> bool:
        metrics = self.state.metrics()
        req = pb2.HeartbeatNodeRequest(
            node_id=self.node_id,
            timestamp=dt_to_ts(utc_now()),
            healthy=True,
            metrics=pb2.NodeMetrics(
                queued=metrics["queued"],
                inflight=metrics["inflight"],
                running=metrics["running"],
                credit=metrics["credit"],
                cpu_percent=0.0,
                mem_percent=0.0,
            ),
            services=self.state.service_reports(),
        )
        resp = self._stub.HeartbeatNode(req, timeout=self.rpc_timeout_sec)
        if not resp.ok or not resp.accepted:
            self._registered = False
            return False
        self._next_hb_sec = max(1, int(resp.next_heartbeat_in_sec or self.fallback_heartbeat_sec))
        return True

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if not self._registered:
                    self._register_once()
                else:
                    self._heartbeat_once()
            except grpc.RpcError:
                self._registered = False
            except Exception:
                self._registered = False

            wait_sec = self._next_hb_sec if self._registered else self.fallback_heartbeat_sec
            self._stop_event.wait(max(1, int(wait_sec)))

