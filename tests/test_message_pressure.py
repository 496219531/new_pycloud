import queue
import threading
import time

import pytest

from pycloud_parallel.controlplane.executor_host import ExecutorHostClient
from pycloud_parallel.controlplane.executor_core import ExecutorCore


def test_full_send_queue_respects_timeout():
    host = object.__new__(ExecutorHostClient)
    host._closed = False
    host._cv = threading.Condition()
    host._seq = 0
    host._request_q = queue.Queue(maxsize=1)
    host._request_q.put(b"occupied")
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="send queue"):
        host._request("probe", timeout_sec=0.05)
    assert time.monotonic() - started < 0.5


def test_stream_drain_yields_to_other_work():
    core = ExecutorCore(task_worker_capacity=1)
    messages = queue.Queue()
    for index in range(100):
        messages.put({"kind": "item", "result": index})
    emitted = []
    core._emit_event = emitted.append
    core._drain_stream_meta({"request_id": "stream", "stream_queue": messages})
    assert len(emitted) == 32
    assert messages.qsize() == 68
    core.close()


def test_slow_stream_consumer_has_bounded_buffer(monkeypatch):
    from collections import deque
    from pycloud_parallel.controlplane import executor_host as host_module

    host = object.__new__(ExecutorHostClient)
    host._cv = threading.Condition()
    host._reader_stop = threading.Event()
    host._event_q = object()
    host._stream_events = {}
    host._stream_buffer_bytes = {}
    host._overflowed_streams = set()
    host._expired_requests = set()
    host._responses = {}
    host._async_events = deque()
    events = iter([
        *({"kind": "service_stream_item", "request_id": "slow", "result": i} for i in range(200)),
        {"kind": "response", "request_id": "other", "ok": True},
    ])

    def receive(*args, **kwargs):
        item = next(events, None)
        if item is None:
            host._reader_stop.set()
        return item

    monkeypatch.setattr(host_module, "_simple_queue_get_if_ready", receive)
    host._reader_loop()
    assert not host._stream_events
    assert not host._stream_buffer_bytes
    assert "buffer limit" in host._responses["slow"]["error"]
    assert host._responses["other"]["ok"]


def test_ipc_rejects_excess_connections_and_releases_idle_slot():
    from types import SimpleNamespace
    from pycloud_parallel.controlplane.local_ipc import LocalServiceIpcServer

    server = object.__new__(LocalServiceIpcServer)
    server._stop = threading.Event()
    server._connection_slots = threading.BoundedSemaphore(1)
    server._connection_slots.acquire()
    closed = []
    conn = SimpleNamespace(close=lambda: closed.append(True), poll=lambda timeout: False)

    def accept():
        server._stop.set()
        return conn

    server._listener = SimpleNamespace(accept=accept)
    server._serve()
    assert closed == [True]
    assert not server._connection_slots.acquire(blocking=False)
    server._stop.clear()
    server._handle_conn(conn)
    assert closed == [True, True]
    assert server._connection_slots.acquire(blocking=False)


def test_pool_result_encoding_does_not_hold_node_lock(tmp_path, monkeypatch):
    from pycloud_parallel import TaskPool
    from pycloud_parallel.controlplane.artifact import Artifact
    from pycloud_parallel.controlplane.node.models import TaskState

    monkeypatch.chdir(tmp_path)
    with TaskPool.open(target="local", artifact=Artifact.from_bytes(
        b"def run(value=0):\n    return {'value': value}\n",
        package_format="py", entry_callable="run",
    )) as pool:
        state = next(iter(pool._pools.values()))._client._state
        original = TaskState.as_result
        checked = threading.Event()

        def encode(task):
            assert state._lock.acquire(blocking=False), "result encoding holds node lock"
            state._lock.release()
            checked.set()
            return original(task)

        monkeypatch.setattr(TaskState, "as_result", encode)
        assert list(pool.unordered([{"value": 7}], timeout_sec=10)) == [(0, {"value": 7})]
        assert checked.is_set()
