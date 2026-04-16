from __future__ import annotations

import pytest

from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient, InfoCenterNode
from pycloud_parallel.controlplane.node.execution import _validate_python_runtime_or_raise
from pycloud_parallel.runtime.compat import runtime_mismatch_message_for_nodes


def test_runtime_mismatch_message_contains_requested_runtime_versions_and_fix():
    message = runtime_mismatch_message_for_nodes(
        requested_runtime=">=py3.11",
        nodes=[
            InfoCenterNode(
                node_instance_id="node-a-inst",
                node_id="node-a",
                control_addr="127.0.0.1:50061",
                healthy=True,
                capacity=4,
                queue_capacity=16,
                queued=0,
                inflight=0,
                credit=4,
                python_version="py3.10",
            ),
            InfoCenterNode(
                node_instance_id="node-b-inst",
                node_id="node-b",
                control_addr="127.0.0.1:50062",
                healthy=True,
                capacity=4,
                queue_capacity=16,
                queued=0,
                inflight=0,
                credit=4,
                python_version="py3.9",
            ),
        ],
    )

    assert "requested_runtime=>=py3.11" in message
    assert "node-a-inst(py3.10)" in message
    assert "node-b-inst(py3.9)" in message
    assert "Fix:" in message


def test_validate_python_runtime_or_raise_uses_unified_message():
    with pytest.raises(ValueError) as exc_info:
        _validate_python_runtime_or_raise(node_python_version="py3.10", runtime=">=py3.11")

    message = str(exc_info.value)
    assert "requested_runtime=>=py3.11" in message
    assert "current_node(py3.10)" in message
    assert "Fix:" in message


def test_infocenter_select_task_nodes_uses_unified_runtime_mismatch_message():
    client = InfoCenterClient("http://127.0.0.1:50051")
    client.list_nodes = lambda **kwargs: [  # type: ignore[method-assign]
        InfoCenterNode(
            node_instance_id="node-a-inst",
            node_id="node-a",
            control_addr="127.0.0.1:50061",
            healthy=True,
            capacity=4,
            queue_capacity=16,
            queued=0,
            inflight=0,
            credit=4,
            python_version="py3.10",
        )
    ]

    with pytest.raises(RuntimeError) as exc_info:
        client.select_task_nodes(node_ids=["node-a"], runtime=">=py3.11")

    message = str(exc_info.value)
    assert "requested_runtime=>=py3.11" in message
    assert "node-a-inst(py3.10)" in message
    assert "Fix:" in message
