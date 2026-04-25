from __future__ import annotations

"""Tests for the V1 task-pool-facing API helpers."""

from types import SimpleNamespace

from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode
from pycloud_parallel.execution.task_pool import TaskPool


def test_taskpool_route_summary_reports_fixed_routes():
    node = InfoCenterNode(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="10.0.0.1:50061",
        healthy=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    pool = SimpleNamespace(
        pool_id="pool-1",
        pool_name="calc-pool",
        owner_client_id="owner-1",
        node_id="node-1",
    )
    session = TaskPool(
        pools={"node-inst-1": pool},
        nodes={"node-inst-1": node},
        task_method="run",
    )

    assert session.routes() == [
        {
            "node_instance_id": "node-inst-1",
            "node_id": "node-1",
            "control_addr": "10.0.0.1:50061",
            "pool_id": "pool-1",
            "pool_name": "calc-pool",
            "owner_client_id": "owner-1",
        }
    ]


def test_prepare_task_payload_for_submit_uses_task_submit_policy(monkeypatch) -> None:
    from pycloud_parallel.execution.support import _prepare_task_payload_for_submit

    captured = {}

    def _fake_prepare(payload, *, put_data, estimate_inline_size, policy, managed_global_policy=None):
        del put_data, estimate_inline_size
        del managed_global_policy
        captured["payload"] = dict(payload or {})
        captured["mode"] = policy.mode
        captured["consume_on_read"] = policy.consume_on_read
        return dict(payload or {})

    monkeypatch.setattr(
        "pycloud_parallel.execution.support.prepare_outbound_payload",
        _fake_prepare,
    )

    client = SimpleNamespace(target="127.0.0.1:50061")
    prepared = _prepare_task_payload_for_submit(client, {"value": 7})

    assert prepared == {"value": 7}
    assert captured["payload"] == {"value": 7}
    assert captured["mode"] == "task_submit"
    assert captured["consume_on_read"] is True
