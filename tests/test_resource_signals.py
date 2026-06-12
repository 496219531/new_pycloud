from pycloud_parallel.controlplane.resource_signals import (
    ResourceOperation,
    ResourceSignal,
    ResourceSignalStore,
)


def test_resource_signal_publish_seq_and_since_are_monotonic():
    store = ResourceSignalStore(maxlen=10)

    first = store.publish(ResourceSignal(resource_kind="service", resource_id="svc-1", signal_type="progress"))
    second = store.publish(ResourceSignal(resource_kind="service", resource_id="svc-1", signal_type="failure"))

    assert first == 1
    assert second == 2
    assert [signal.seq for signal in store.since(0)] == [1, 2]
    assert [signal.seq for signal in store.since(1)] == [2]


def test_resource_signal_ring_buffer_is_bounded_for_retained_signals():
    store = ResourceSignalStore(maxlen=3)

    for index in range(6):
        store.publish(
            ResourceSignal(
                resource_kind="service",
                resource_id=f"svc-{index}",
                signal_type="progress",
                state=str(index),
            )
        )

    assert [signal.seq for signal in store.since(0, limit=10)] == [4, 5, 6]


def test_resource_signal_latest_returns_latest_resource_state():
    store = ResourceSignalStore(maxlen=10)

    store.publish(ResourceSignal(resource_kind="service", resource_id="svc-1", signal_type="progress", state="accepted"))
    store.publish(ResourceSignal(resource_kind="service", resource_id="svc-1", signal_type="readiness", state="ready"))

    latest = store.latest("service", "svc-1")

    assert latest is not None
    assert latest.signal_type == "readiness"
    assert latest.state == "ready"


def test_resource_signal_incremental_stream_ignores_heartbeat():
    store = ResourceSignalStore(maxlen=10)

    store.publish(ResourceSignal(resource_kind="service", resource_id="svc-1", signal_type="progress", state="prepare"))
    store.publish(ResourceSignal(resource_kind="service", resource_id="svc-1", signal_type="heartbeat", state="running"))
    store.publish(ResourceSignal(resource_kind="service", resource_id="svc-1", signal_type="heartbeat", state="running"))
    store.publish(ResourceSignal(resource_kind="service", resource_id="svc-1", signal_type="failure", state="failed"))

    signals = store.since(0, limit=10)

    assert not any(signal.signal_type == "heartbeat" for signal in signals)
    assert any(signal.signal_type == "progress" for signal in signals)
    assert any(signal.signal_type == "failure" for signal in signals)


def test_resource_operation_upsert_and_snapshot():
    store = ResourceSignalStore(maxlen=10)

    op = store.upsert_operation(
        ResourceOperation(
            op_id="op-1",
            op_type="create",
            resource_kind="task_pool",
            resource_id="pool-1",
            status="accepted",
            stage="queued",
        )
    )
    store.upsert_operation(
        ResourceOperation(
            op_id="op-1",
            op_type="create",
            resource_kind="task_pool",
            resource_id="pool-1",
            status="running",
            stage="executor_create",
            last_signal_seq=3,
        )
    )

    assert op.op_id == "op-1"
    current = store.get_operation("op-1")
    assert current is not None
    assert current.status == "running"
    assert current.stage == "executor_create"
    assert current.last_signal_seq == 3
    assert [item.op_id for item in store.operations_snapshot(resource_kind="task_pool", resource_id="pool-1")] == ["op-1"]
