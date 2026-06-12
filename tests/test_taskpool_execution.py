from __future__ import annotations

"""Execution-focused tests for the V1 TaskPool implementation."""

import asyncio
from datetime import date, datetime
import importlib
import io
import logging
import sys
import tarfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pycloud_parallel.controlplane.artifact import Artifact
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.controlplane.serialization import dict_to_struct


def _build_task_entry_module(tmp_path, monkeypatch, *, with_init: bool = True):
    package_name = "demo_task_pkg_entry"
    sys.modules.pop(package_name, None)
    sys.modules.pop(f"{package_name}.worker", None)
    sys.modules.pop(f"{package_name}.helper", None)
    package_dir = tmp_path / package_name
    package_dir.mkdir()
    if with_init:
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "helper.py").write_text(
        "def normalize(value):\n"
        "    return int(value)\n",
        encoding="utf-8",
    )
    (package_dir / "ignored.csv").write_text("value\n1\n", encoding="utf-8")
    (package_dir / "worker.py").write_text(
        "from .helper import normalize\n\n"
        "def run(value=0, **_kwargs):\n"
        "    return {'value': normalize(value)}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    return importlib.import_module(f"{package_name}.worker")


def _build_task_entry_module_with_resource(tmp_path, monkeypatch):
    worker_module = _build_task_entry_module(tmp_path, monkeypatch)
    package_dir = tmp_path / worker_module.__package__
    (package_dir / "data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    return worker_module


def test_native_task_pool_session_submit_and_wait() -> None:
    from pycloud_parallel import TaskPool

    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(
            ok=True,
            accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
            rejected=[],
        ),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(
            ok=True,
            results=[
                pb2.TaskResult(
                    task_id="pool-task-0001",
                    job_id="job-native",
                    status=pb2.TASK_STATUS_SUCCEEDED,
                    result=dict_to_struct({"value": 1}),
                )
            ],
            next_cursor="",
        ),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        return_value=fake_pool_client,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native",
            source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )

    try:
        resp = session.submit_payloads([{"value": 1}])
        assert len(resp.accepted) == 1
        assert session.job_id == "job-native"
        assert session.node_ids == ["node-1"]
        assert session.methods == ["run"]
    finally:
        session.close()


def test_task_pool_open_local_uses_private_node_pool(tmp_path) -> None:
    from pycloud_parallel import TaskPool

    blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value) * 3}\n"
    )

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_demo",
            entry_callable="run",
        ),
        worker_count=1,
    ) as pool:
        resp = pool.submit_payloads([{"value": 2}, {"value": 5}])
        assert len(resp.accepted) == 2
        values = pool.wait_for_data(expected_count=2, timeout_sec=10.0)
        assert sorted(item["value"] for item in values) == [6, 15]
        assert pool.route_summary()[0]["control_addr"] == "local"


def test_task_pool_open_local_module_defaults_to_direct_no_package(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "pycloud_parallel.execution.task_pool._prepare_artifact",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("local direct module must not package")),
    )

    with TaskPool.open(
        target="local",
        source=worker_module,
        worker_count=1,
    ) as pool:
        assert pool.collect_items([{"value": 6}], timeout_sec=10.0)[0].result == {"value": 6}
        assert pool.route_summary()[0]["control_addr"] == "local"


def test_task_pool_open_local_callable_defaults_to_direct_no_package(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    def run(value=0, **_kwargs):
        return {"value": int(value) + 2}

    monkeypatch.setattr(
        "pycloud_parallel.execution.task_pool._prepare_artifact",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("local direct callable must not package")),
    )

    with TaskPool.open(
        target="local",
        source=run,
        worker_count=1,
    ) as pool:
        assert pool.collect_items([{"value": 6}], timeout_sec=10.0)[0].result == {"value": 8}


def test_task_pool_open_local_callable_preserves_dict_int_scalar(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    def run(inner_code=None, **_kwargs):
        return {"value": inner_code, "type": type(inner_code).__name__}

    monkeypatch.setattr(
        "pycloud_parallel.execution.task_pool._prepare_artifact",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("local direct callable must not package")),
    )

    with TaskPool.open(
        target="local",
        source=run,
        worker_count=1,
    ) as pool:
        result = pool.collect_items([{"inner_code": 83996}], timeout_sec=10.0)[0].result
        assert result == {"value": 83996, "type": "int"}


def test_task_pool_open_local_callable_accepts_date_time_args_kwargs(monkeypatch) -> None:
    pd = pytest.importorskip("pandas")
    from pycloud_parallel import TaskPool

    def run(day, *, asof=None, timestamp=None):
        return {
            "day_type": type(day).__name__,
            "day": day.isoformat(),
            "asof_type": type(asof).__name__,
            "asof": asof.isoformat(),
            "timestamp_type": type(timestamp).__name__,
            "timestamp": timestamp.isoformat(),
        }

    monkeypatch.setattr(
        "pycloud_parallel.execution.task_pool._prepare_artifact",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("local direct callable must not package")),
    )

    with TaskPool.open(
        target="local",
        source=run,
        worker_count=1,
    ) as pool:
        result = pool.run.sync(
            date(2024, 1, 2),
            asof=datetime(2024, 1, 2, 9, 30),
            timestamp=pd.Timestamp("2024-01-03 10:15:00"),
        )
        assert result == {
            "day_type": "date",
            "day": "2024-01-02",
            "asof_type": "datetime",
            "asof": "2024-01-02T09:30:00",
            "timestamp_type": "Timestamp",
            "timestamp": "2024-01-03T10:15:00",
        }


def test_task_pool_open_local_supports_unordered_wrappers(tmp_path) -> None:
    from pycloud_parallel import TaskPool

    blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value) * 2}\n"
    )

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_unordered",
            entry_callable="run",
        ),
        worker_count=1,
    ) as pool:
        pairs = sorted(pool.unordered([{"value": 3}, {"value": 4}], timeout_sec=10.0))
        assert pairs == [(0, {"value": 6}), (1, {"value": 8})]

        items = sorted(
            pool.unordered(
                [{"value": 5}, {"value": 6}],
                timeout_sec=10.0,
                return_items=True,
            ),
            key=lambda item: item.index,
        )
        assert [item.result for item in items] == [{"value": 10}, {"value": 12}]
        assert all(item.ok for item in items)
        assert all(item.task_id for item in items)
        assert all(item.node_instance_id for item in items)


def test_task_pool_open_local_ignores_external_serialization_mode(tmp_path) -> None:
    from pycloud_parallel import TaskPool
    from pycloud_parallel.controlplane.serialization import decode_transport_payload_bytes

    blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value) + 1}\n"
    )

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_ignore_mode",
            entry_callable="run",
        ),
        worker_count=1,
        serialization_mode="structured_v1",
    ) as pool:
        assert pool.serialization_mode == "pickle_native_v1"
        captured_items = []
        original_build_item = pool._build_task_submit_item

        def _capture_build_item(*args, **kwargs):
            item = original_build_item(*args, **kwargs)
            captured_items.append(item)
            return item

        pool._build_task_submit_item = _capture_build_item
        resp = pool.submit_payloads([{"value": 1}], serialization_mode="legacy_v1")
        assert len(resp.accepted) == 1
        assert len(captured_items) == 1
        assert captured_items[0].transport_payload.codec == "pickle_native_v1"
        assert (
            decode_transport_payload_bytes(
                captured_items[0].transport_payload.codec,
                captured_items[0].transport_payload.version,
                captured_items[0].transport_payload.payload,
                context="taskpool_session",
            )
            == {"value": 1}
        )
        values = pool.wait_for_data(expected_count=1, timeout_sec=10.0)
        assert values == [{"value": 2}]


def test_task_pool_open_local_supports_imap_unordered_return_items(tmp_path) -> None:
    from pycloud_parallel import TaskPool

    blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value) * 4}\n"
    )

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_imap_unordered",
            entry_callable="run",
        ),
        worker_count=1,
    ) as pool:
        items = sorted(
            pool.imap_unordered(
                [{"value": 2}, {"value": 3}],
                timeout_sec=10.0,
                return_items=True,
            ),
            key=lambda item: item.index,
        )
        assert [item.result for item in items] == [{"value": 8}, {"value": 12}]
        assert all(item.ok for item in items)
        assert all(item.task_id for item in items)


def test_task_pool_open_local_unordered_uses_local_transport_payload(tmp_path) -> None:
    from pycloud_parallel import TaskPool
    from pycloud_parallel.controlplane.serialization import decode_transport_payload_bytes

    blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value) * 5}\n"
    )

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_plain_unordered",
            entry_callable="run",
        ),
        worker_count=1,
    ) as pool:
        captured_items = []
        original_build_item = pool._build_task_submit_item

        def _capture_build_item(*args, **kwargs):
            item = original_build_item(*args, **kwargs)
            captured_items.append(item)
            return item

        pool._build_task_submit_item = _capture_build_item
        items = list(pool.imap_unordered([{"value": 2}], timeout_sec=10.0, return_items=True))

        assert items[0].ok is True
        assert items[0].result == {"value": 10}
        assert len(captured_items) == 1
        assert captured_items[0].transport_payload.codec == "pickle_native_v1"
        assert (
            decode_transport_payload_bytes(
                captured_items[0].transport_payload.codec,
                captured_items[0].transport_payload.version,
                captured_items[0].transport_payload.payload,
                context="taskpool_session",
            )
            == {"value": 2}
        )


def test_task_pool_open_local_supports_async_unordered_wrapper(tmp_path) -> None:
    from pycloud_parallel import TaskPool

    blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value) + 10}\n"
    )

    async def _collect(pool):
        return [
            item
            async for item in pool.aunordered(
                [{"value": 1}, {"value": 2}],
                timeout_sec=10.0,
                return_items=True,
            )
        ]

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_async_unordered",
            entry_callable="run",
        ),
        worker_count=1,
    ) as pool:
        items = sorted(asyncio.run(_collect(pool)), key=lambda item: item.index)
        assert [item.result for item in items] == [{"value": 11}, {"value": 12}]
        assert all(item.ok for item in items)


def test_task_pool_open_local_resolves_dataref_payload_and_result(tmp_path) -> None:
    from pycloud_parallel import TaskPool

    blob = (
        b"from pathlib import Path\n"
        b"import tempfile\n\n"
        b"def run(blob=None, result_size=0, **_kwargs):\n"
        b"    payload_size = len(blob if isinstance(blob, (bytes, bytearray)) else Path(blob).read_bytes()) if blob is not None else 0\n"
        b"    out = Path(tempfile.gettempdir()) / 'pycloud-local-task-result.bin'\n"
        b"    out.write_bytes(b'r' * int(result_size))\n"
        b"    return out if result_size else {'payload_size': payload_size}\n"
    )

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_dataref",
            entry_callable="run",
        ),
        worker_count=1,
    ) as pool:
        ref = pool.put_data(b"p" * (1024 * 1024 + 3), format="bin")
        payload_items = list(pool.imap_unordered([{"blob": ref}], timeout_sec=10.0, return_items=True))
        assert payload_items[0].ok is True
        assert payload_items[0].result == {"payload_size": 1024 * 1024 + 3}

        result_items = list(
            pool.imap_unordered(
                [{"result_size": 1024 * 1024 + 5}],
                timeout_sec=10.0,
                return_items=True,
            )
        )
        assert result_items[0].ok is True
        result_path = result_items[0].result
        assert result_path.read_bytes() == b"r" * (1024 * 1024 + 5)


def test_task_pool_open_local_put_data_file_path_does_not_read_whole_file(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    blob = (
        b"from pathlib import Path\n\n"
        b"def run(blob=None, **_kwargs):\n"
        b"    return {'payload_size': Path(blob).stat().st_size}\n"
    )
    source = tmp_path / "local-taskpool-payload.bin"
    source.write_bytes(b"x" * (1024 * 1024 + 11))

    original_read_bytes = Path.read_bytes

    def _guard_read_bytes(self):  # noqa: ANN001
        if self == source:
            raise AssertionError("local taskpool put_data(file_path) must not read the whole source file")
        return original_read_bytes(self)

    monkeypatch.setattr(Path, "read_bytes", _guard_read_bytes)

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_file_dataref",
            entry_callable="run",
        ),
        worker_count=1,
    ) as pool:
        ref = pool.put_data(source, format="bin")
        payload_items = list(pool.imap_unordered([{"blob": ref}], timeout_sec=10.0, return_items=True))
        assert payload_items[0].ok is True
        assert payload_items[0].result == {"payload_size": source.stat().st_size}


def test_task_pool_put_data_accepts_object_format_alias(tmp_path) -> None:
    from pycloud_parallel import TaskPool

    blob = (
        b"from pathlib import Path\n\n"
        b"def run(blob=None, **_kwargs):\n"
        b"    return {'payload_size': Path(blob).stat().st_size}\n"
    )
    source = tmp_path / "local-taskpool-object-format.bin"
    source.write_bytes(b"x" * 128)

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_object_format",
            entry_callable="run",
        ),
        worker_count=1,
    ) as pool:
        ref = pool.put_data(source, object_format="bin")

    assert ref.format == "bin"


def test_task_pool_open_local_applies_managed_globals(tmp_path) -> None:
    from pycloud_parallel import TaskPool

    blob = (
        b"cfg = {}\n\n"
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value) + int(cfg.get('offset', 0))}\n"
    )

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_globals",
            entry_callable="run",
            managed_global_names=["cfg"],
        ),
        worker_count=1,
    ) as pool:
        digest = pool.update_globals({"cfg": {"offset": 7}})
        assert digest
        items = list(pool.imap_unordered([{"value": 5}], timeout_sec=10.0, return_items=True))
        assert items[0].ok is True
        assert items[0].result == {"value": 12}


def test_task_pool_open_local_initial_globals(tmp_path) -> None:
    from pycloud_parallel import TaskPool

    blob = (
        b"cfg = {}\n\n"
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value) + int(cfg.get('offset', 0))}\n"
    )

    with TaskPool.open(
        target="local",
        artifact=Artifact.from_bytes(
            blob,
            package_format="py",
            entry_module="local_task_pool_initial_globals",
            entry_callable="run",
        ),
        initial_globals={"cfg": {"offset": 9}},
        worker_count=1,
    ) as pool:
        items = list(pool.imap_unordered([{"value": 5}], timeout_sec=10.0, return_items=True))
        assert items[0].ok is True
        assert items[0].result == {"value": 14}


def test_task_pool_open_local_includes_resource_paths(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    module_path = tmp_path / "local_task_pool_resource_worker.py"
    module_path.write_text(
        "from pathlib import Path\n\n"
        "def run(**_kwargs):\n"
        "    return {'text': Path(__file__).with_name('data.csv').read_text(encoding='utf-8').strip()}\n",
        encoding="utf-8",
    )
    (tmp_path / "data.csv").write_text("hello-local-resource\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    worker = importlib.import_module("local_task_pool_resource_worker")

    with TaskPool.open(
        target="local",
        source=worker,
        resource_paths=["data.csv"],
        worker_count=1,
    ) as pool:
        items = list(pool.imap_unordered([{}], timeout_sec=10.0, return_items=True))
        assert items[0].ok is True
        assert items[0].result == {"text": "hello-local-resource"}


def test_task_pool_from_infocenter_creates_node_pools_concurrently(monkeypatch) -> None:
    from pycloud_parallel import TaskPool
    from pycloud_parallel.execution import task_pool as task_pool_mod

    nodes = [
        SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061"),
        SimpleNamespace(node_id="node-2", control_addr="127.0.0.1:50062"),
    ]
    started = []
    finished = []
    lock = threading.Lock()
    both_started = threading.Event()

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def select_task_nodes(self, **_kwargs):
            return list(nodes)

    class _FakeNodeClient:
        def __init__(self, addr: str):
            self.addr = addr

        def create_task_pool_from_bytes(self, **_kwargs):
            with lock:
                started.append(self.addr)
                if len(started) == len(nodes):
                    both_started.set()
            assert both_started.wait(timeout=1.0)
            with lock:
                finished.append(self.addr)
            suffix = self.addr.rsplit(":", 1)[-1]
            return SimpleNamespace(
                owner_client_id="owner-demo",
                pool_id=f"pool-{suffix}",
                pool_token=f"token-{suffix}",
                code_version="sha256:test",
                worker_count=2,
                heartbeat_timeout_sec=30,
                submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
                pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
                heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
                cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
                close=lambda reason="": None,
                _client=SimpleNamespace(close=lambda: None),
            )

    monkeypatch.setattr(task_pool_mod, "_infocenter_client", lambda *_args, **_kwargs: _FakeInfoCenter())
    monkeypatch.setattr(task_pool_mod, "_new_node_control_client", lambda addr, **_kwargs: _FakeNodeClient(addr))

    session = TaskPool._from_infocenter(
        infocenter_target="127.0.0.1:50051",
        job_id="job-native-parallel-create",
        source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
        entry_module="task_demo",
        entry_callable="run",
        worker_count=2,
        node_count=2,
    )
    try:
        assert set(started) == {"127.0.0.1:50061", "127.0.0.1:50062"}
        assert set(finished) == set(started)
        assert session.node_ids == ["node-1", "node-2"]
    finally:
        session.close()


def test_task_pool_update_globals_fans_out_to_nodes_concurrently(monkeypatch) -> None:
    from google.protobuf import struct_pb2
    from pycloud_parallel import TaskPool
    from pycloud_parallel.execution import managed_globals as managed_globals_mod

    nodes = {
        "node-1": SimpleNamespace(node_id="node-1"),
        "node-2": SimpleNamespace(node_id="node-2"),
    }
    started = []
    encode_calls = []
    lock = threading.Lock()
    both_started = threading.Event()

    def _fake_encode_batches(prepared_batches, **_kwargs):
        encoded = []
        for values in prepared_batches:
            encode_calls.append(dict(values))
            encoded.append((dict(values), struct_pb2.Struct(), None))
        return encoded

    monkeypatch.setattr(managed_globals_mod, "_encode_managed_globals_batches", _fake_encode_batches)

    class _FakeClient:
        def __init__(self, node_id: str):
            self.node_id = node_id

        def update_runtime_globals_encoded(self, **kwargs):
            assert kwargs["prepared_keys"] == ["STATE"]
            assert kwargs.get("values") is not None or kwargs.get("transport_values") is not None
            with lock:
                started.append(self.node_id)
                if len(started) == len(nodes):
                    both_started.set()
            assert both_started.wait(timeout=1.0)
            return SimpleNamespace(globals_digest="sha256:globals")

        def update_runtime_globals_prepared(self, **_kwargs):
            raise AssertionError("update_globals should use pre-encoded payloads when supported")

        def close(self):
            pass

    def _pool(node_id: str):
        return SimpleNamespace(
            owner_client_id="owner-demo",
            pool_id=f"pool-{node_id}",
            pool_token=f"token-{node_id}",
            code_version="sha256:test",
            worker_count=2,
            heartbeat_timeout_sec=30,
            close=lambda reason="": None,
            _client=_FakeClient(node_id),
        )

    session = TaskPool(
        pools={node_id: _pool(node_id) for node_id in nodes},
        nodes=nodes,
        task_method="run",
        job_id="job-update-globals-parallel",
    )
    try:
        assert session.update_globals({"STATE": 1}) == "sha256:globals"
        assert set(started) == set(nodes)
        assert len(encode_calls) == 1
    finally:
        session.close()


def test_native_task_pool_session_dynamic_default_max_in_flight_uses_effective_workers() -> None:
    from pycloud_parallel import TaskPool

    fake_pool_1 = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        worker_count=2,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    fake_pool_2 = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        worker_count=3,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-1": fake_pool_1, "node-2": fake_pool_2},
        nodes={
            "node-1": SimpleNamespace(node_id="node-1", task_pool_worker_available=2),
            "node-2": SimpleNamespace(node_id="node-2", task_pool_worker_available=3),
        },
        task_method="run",
    )
    try:
        assert session._resolve_max_in_flight(None) == 8  # noqa: SLF001
    finally:
        session.close()


def test_native_task_pool_session_dynamic_default_max_in_flight_prefers_pool_worker_count_over_node_capacity() -> None:
    from pycloud_parallel import TaskPool

    fake_status_1 = SimpleNamespace(alive_workers=5, worker_count=5)
    fake_status_2 = SimpleNamespace(alive_workers=5, worker_count=5)
    fake_pool_1 = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        worker_count=5,
        get_status=lambda: fake_status_1,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    fake_pool_2 = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        worker_count=5,
        get_status=lambda: fake_status_2,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-1": fake_pool_1, "node-2": fake_pool_2},
        nodes={
            "node-1": SimpleNamespace(node_id="node-1", task_pool_worker_available=10),
            "node-2": SimpleNamespace(node_id="node-2", task_pool_worker_available=10),
        },
        task_method="run",
    )
    try:
        assert session._resolve_max_in_flight(None) == 15  # noqa: SLF001
    finally:
        session.close()


def test_iter_items_merges_shared_kwargs_lazily() -> None:
    from pycloud_parallel import TaskPool

    fake_pool = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        worker_count=1,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-1": fake_pool},
        nodes={"node-1": SimpleNamespace(node_id="node-1", task_pool_worker_available=1)},
        task_method="run",
    )
    consumed = {"count": 0}

    def _payloads():
        for value in range(1000):
            consumed["count"] += 1
            yield {"value": value}

    def _fake_imap(payloads, **_kwargs):
        first = next(iter(payloads))
        assert first == {"value": 0, "shared": 7}
        assert consumed["count"] == 1
        yield 0, first

    try:
        with patch.object(session, "imap_unordered", side_effect=_fake_imap):
            items = list(session.iter_items(_payloads(), max_in_flight=1, timeout_sec=0.1, shared=7))
        assert consumed["count"] == 1
        assert items[0].result == {"value": 0, "shared": 7}
    finally:
        session.close()


def test_task_pool_map_uses_dynamic_default_max_in_flight_by_default() -> None:
    from pycloud_parallel import TaskPool

    fake_pool = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        worker_count=2,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-1": fake_pool},
        nodes={"node-1": SimpleNamespace(node_id="node-1", task_pool_worker_available=2)},
        task_method="run",
    )
    try:
        expected = session._resolve_max_in_flight(None)  # noqa: SLF001
        with patch.object(session, "collect_items", return_value=[]) as mocked_collect:
            session.map([1, 2, 3], arg_name="value", timeout_sec=0.1)
        assert mocked_collect.call_args.kwargs["max_in_flight"] == expected
    finally:
        session.close()


def test_submit_payloads_reuses_scheduler_candidate_snapshot_for_batch() -> None:
    from pycloud_parallel import TaskPool

    submit_calls = {"node-a": 0, "node-b": 0}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 2

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._client = SimpleNamespace(close=lambda: None)

        def submit_tasks(self, tasks, job_id=""):
            submit_calls[self.node_id] += 1
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def close(self, reason=""):
            return None

    session = TaskPool(
        pools={"node-a": _Pool("node-a"), "node-b": _Pool("node-b")},
        nodes={},
        task_method="run",
        job_id="job-submit-batch-plan",
    )

    original = session._build_pool_scheduler_candidates  # noqa: SLF001
    call_count = {"value": 0}

    def _wrapped(*args, **kwargs):
        call_count["value"] += 1
        return original(*args, **kwargs)

    try:
        with patch.object(session, "_build_pool_scheduler_candidates", side_effect=_wrapped):
            resp = session.submit_payloads([{"value": 1}, {"value": 2}, {"value": 3}])
        assert len(resp.accepted) == 3
        assert call_count["value"] == 1
        assert sum(submit_calls.values()) >= 1
    finally:
        session.close()


def test_task_pool_close_forwards_reason_to_replicas() -> None:
    from pycloud_parallel import TaskPool

    reasons = []
    client_closed = []
    fake_pool = SimpleNamespace(
        owner_client_id="owner",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        worker_count=1,
        close=lambda reason="": reasons.append(reason),
        _client=SimpleNamespace(close=lambda: client_closed.append(True)),
    )
    session = TaskPool(
        pools={"node-a": fake_pool},
        nodes={},
        task_method="run",
        job_id="job-close-reason",
    )

    session.close(reason="owner interrupted")

    assert reasons == ["owner interrupted"]
    assert client_closed == [True]


def test_imap_unordered_reuses_scheduler_candidate_snapshot_per_fill() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(
                fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id}
            )

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"value": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

        def close(self, reason=""):
            return None

    session = TaskPool(
        pools={"node-a": _Pool("node-a"), "node-b": _Pool("node-b")},
        nodes={},
        task_method="run",
        job_id="job-imap-batch-plan",
    )
    original = session._build_pool_scheduler_candidates  # noqa: SLF001
    call_count = {"value": 0}

    def _wrapped(*args, **kwargs):
        call_count["value"] += 1
        return original(*args, **kwargs)

    try:
        with patch.object(session, "_build_pool_scheduler_candidates", side_effect=_wrapped):
            out = list(
                session.imap_unordered(
                    [{"value": 1}, {"value": 2}],
                    max_in_flight=2,
                    timeout_sec=0.1,
                )
            )
        assert [index for index, _item in out] == [0, 1]
        assert call_count["value"] == 2
    finally:
        session.close()


def test_native_task_pool_session_cancel_job_aggregates_pool_responses() -> None:
    from pycloud_parallel import TaskPool

    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True, queued_cancelled=1, running_marked=2, already_done=3, not_found=0),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        return_value=fake_pool_client,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-cancel",
            source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )
    try:
        resp = session.cancel_job(job_id="job-native-cancel", reason="test cancel")
        assert resp.queued_cancelled == 1
        assert resp.running_marked == 2
        assert resp.already_done == 3
        assert session._pending_task_ids == set()  # noqa: SLF001
    finally:
        session.close()


def test_task_pool_from_infocenter_includes_only_explicit_resource_paths(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module_with_resource(tmp_path, monkeypatch)
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    create_calls = []
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True, queued_cancelled=0, running_marked=0, already_done=0, not_found=0),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )

    def _fake_create_task_pool(self, **kwargs):
        del self
        create_calls.append(dict(kwargs))
        return fake_pool_client

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        _fake_create_task_pool,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-resource",
            source=worker_module,
            resource_paths=["data.csv"],
            worker_count=2,
            node_count=1,
        )

    try:
        with tarfile.open(fileobj=io.BytesIO(create_calls[0]["blob"]), mode="r:gz") as tar:
            names = set(tar.getnames())
        assert f"{worker_module.__package__}/data.csv" in names
    finally:
        session.close()


def test_native_task_pool_session_submit_payloads_rejects_unknown_task_method() -> None:
    from pycloud_parallel import TaskPool

    fake_pool = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-1": fake_pool},
        nodes={},
        task_method="run",
        job_id="job-native-method-check",
    )
    try:
        with pytest.raises(AttributeError, match="has no method 'other'"):
            session.submit_payloads([{"value": 1}], task_method="other")
    finally:
        session.close()


def test_native_task_pool_session_status_map() -> None:
    from pycloud_parallel import TaskPool

    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_status = SimpleNamespace(status="RUNNING", worker_count=2, task_count=0)
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
        get_status=lambda: fake_status,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        return_value=fake_pool_client,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-status",
            source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )
    try:
        status_map = session.status_map()
        assert status_map["node-1"].status == "RUNNING"
    finally:
        session.close()


def test_task_pool_from_infocenter_keeps_partial_create_success(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    node_1 = SimpleNamespace(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
    )
    node_2 = SimpleNamespace(
        node_instance_id="node-inst-2",
        node_id="node-2",
        control_addr="127.0.0.1:50062",
    )

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node_1, node_2]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec
            self.closed = False

        def close(self):
            self.closed = True

        def create_task_pool_from_bytes(self, **kwargs):
            if self.target.endswith(":50062"):
                raise RuntimeError("connection refused")
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-node-1",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
                pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
                heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
                cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
                close=lambda reason="": None,
                _client=self,
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    session = TaskPool._from_infocenter(
        infocenter_target="127.0.0.1:50051",
        job_id="job-partial-create",
        source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
        entry_module="task_demo",
        entry_callable="run",
        worker_count=1,
        node_count=2,
        timeout_sec=0.1,
    )
    try:
        assert session.node_instance_ids == ["node-inst-1"]
        assert "node-inst-2" in session.failures
        assert "connection refused" in session.failures["node-inst-2"]
        assert session._compensation_spec["node_count"] == 2  # noqa: SLF001
    finally:
        session.close()


def test_task_pool_from_infocenter_retries_alternate_nodes(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    nodes = [
        SimpleNamespace(
            node_instance_id=f"node-inst-{idx}",
            node_id=f"node-{idx}",
            control_addr=f"127.0.0.1:5006{idx}",
            task_pool_worker_available=2,
            task_pool_worker_capacity=2,
        )
        for idx in range(1, 4)
    ]
    created_targets = []
    expected_node_instance_ids = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **kwargs):
            assert kwargs["node_count"] == 10
            return list(nodes)

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec
            self.closed = False

        def close(self):
            self.closed = True

        def create_task_pool_from_bytes(self, **kwargs):
            created_targets.append(self.target)
            expected_node_instance_ids.append(kwargs.get("expected_node_instance_id"))
            if self.target.endswith(":50061"):
                raise RuntimeError("node instance execution is fenced")
            if self.target.endswith(":50062"):
                raise ConnectionResetError(10054, "connection reset")
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-node-3",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
                pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
                heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
                cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
                close=lambda reason="": None,
                _client=self,
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    session = TaskPool._from_infocenter(
        infocenter_target="127.0.0.1:50051",
        job_id="job-alternate-create",
        source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
        entry_module="task_demo",
        entry_callable="run",
        worker_count=1,
        node_count=1,
        node_limit=10,
        timeout_sec=0.1,
    )
    try:
        assert session.node_instance_ids == ["node-inst-3"]
        assert "node-inst-1" in session.failures
        assert "node-inst-2" in session.failures
        assert created_targets == ["127.0.0.1:50061", "127.0.0.1:50062", "127.0.0.1:50063"]
        assert expected_node_instance_ids == ["node-inst-1", "node-inst-2", "node-inst-3"]
    finally:
        session.close()


def test_task_pool_from_infocenter_retries_transient_discovery_failure(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    node = SimpleNamespace(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        task_pool_worker_available=2,
        task_pool_worker_capacity=2,
    )
    calls = {"select": 0}

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            calls["select"] += 1
            if calls["select"] == 1:
                raise ConnectionResetError(10054, "connection reset")
            return [node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def close(self):
            return None

        def create_task_pool_from_bytes(self, **kwargs):
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-node-1",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
                pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
                heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
                cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
                close=lambda reason="": None,
                _client=self,
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    session = TaskPool._from_infocenter(
        infocenter_target="127.0.0.1:50051",
        job_id="job-discovery-retry",
        source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
        entry_module="task_demo",
        entry_callable="run",
        worker_count=1,
        node_count=1,
        timeout_sec=1.0,
    )
    try:
        assert session.node_instance_ids == ["node-inst-1"]
        assert calls["select"] >= 2
    finally:
        session.close()


def test_task_pool_from_infocenter_create_uses_longer_rpc_timeout(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    node = SimpleNamespace(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        task_pool_worker_available=2,
        task_pool_worker_capacity=2,
    )
    seen_timeouts = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec
            seen_timeouts.append(timeout_sec)

        def close(self):
            return None

        def create_task_pool_from_bytes(self, **kwargs):
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-node-1",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
                pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
                heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
                cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
                close=lambda reason="": None,
                _client=self,
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    session = TaskPool._from_infocenter(
        infocenter_target="127.0.0.1:50051",
        job_id="job-create-rpc-timeout",
        source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
        entry_module="task_demo",
        entry_callable="run",
        worker_count=1,
        node_count=1,
        timeout_sec=10.0,
    )
    try:
        assert seen_timeouts == [30.0]
    finally:
        session.close()


def test_task_pool_from_infocenter_retries_same_instance_after_transient_create_timeout(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    node = SimpleNamespace(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        task_pool_worker_available=2,
        task_pool_worker_capacity=2,
    )
    calls = {"create": 0, "select": 0}
    create_request_ids = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **kwargs):
            assert kwargs["healthy_only"] is True
            return [node]

        def select_task_nodes(self, **_kwargs):
            calls["select"] += 1
            return [node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def close(self):
            return None

        def create_task_pool_from_bytes(self, **kwargs):
            calls["create"] += 1
            create_request_ids.append(kwargs.get("create_request_id"))
            if calls["create"] == 1:
                raise TimeoutError("timed out")
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-node-1",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
                pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
                heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
                cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
                close=lambda reason="": None,
                _client=self,
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    session = TaskPool._from_infocenter(
        infocenter_target="127.0.0.1:50051",
        job_id="job-same-instance-transient-retry",
        source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
        entry_module="task_demo",
        entry_callable="run",
        worker_count=1,
        node_count=1,
        timeout_sec=1.0,
    )
    try:
        assert session.node_instance_ids == ["node-inst-1"]
        assert calls["select"] >= 2
        assert calls["create"] == 2
        assert create_request_ids[0]
        assert create_request_ids[0] == create_request_ids[1]
        assert "node-inst-1" not in session.failures
    finally:
        session.close()


def test_task_pool_from_infocenter_does_not_retry_same_instance_after_disconnect(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    node = SimpleNamespace(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        task_pool_worker_available=2,
        task_pool_worker_capacity=2,
    )
    calls = {"create": 0, "select": 0, "list_nodes": 0}

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **kwargs):
            assert kwargs["healthy_only"] is True
            calls["list_nodes"] += 1
            return []

        def select_task_nodes(self, **_kwargs):
            calls["select"] += 1
            return [node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def close(self):
            return None

        def create_task_pool_from_bytes(self, **_kwargs):
            calls["create"] += 1
            raise TimeoutError("timed out")

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    with pytest.raises(RuntimeError, match="task pool create failed"):
        TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-same-instance-disconnected",
            source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=1,
            node_count=1,
            timeout_sec=1.0,
        )

    assert calls["select"] >= 2
    assert calls["list_nodes"] >= 1
    assert calls["create"] == 1


def test_task_pool_from_infocenter_does_not_rediscover_permanent_create_failure(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    node = SimpleNamespace(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        task_pool_worker_available=2,
        task_pool_worker_capacity=2,
    )
    calls = {"select": 0}

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            calls["select"] += 1
            return [node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def close(self):
            return None

        def create_task_pool_from_bytes(self, **_kwargs):
            raise ModuleNotFoundError("No module named 'missing_pkg'")

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    with pytest.raises(RuntimeError, match="task pool create failed"):
        TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-permanent-create-failure",
            source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=1,
            node_count=1,
            timeout_sec=1.0,
        )
    assert calls["select"] == 1


def test_task_pool_from_infocenter_rediscover_restarted_requested_node_id(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    old_node = SimpleNamespace(
        node_instance_id="node-1-old",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        task_pool_worker_available=2,
        task_pool_worker_capacity=2,
    )
    new_node = SimpleNamespace(
        node_instance_id="node-1-new",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        task_pool_worker_available=2,
        task_pool_worker_capacity=2,
    )
    calls = {"select": 0}
    expected_node_instance_ids = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **kwargs):
            calls["select"] += 1
            assert kwargs["node_ids"] == ["node-1"]
            if calls["select"] == 1:
                return [old_node]
            return [new_node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def close(self):
            return None

        def create_task_pool_from_bytes(self, **kwargs):
            expected_node_instance_id = kwargs.get("expected_node_instance_id")
            expected_node_instance_ids.append(expected_node_instance_id)
            if expected_node_instance_id == "node-1-old":
                raise RuntimeError("cannot connect to 127.0.0.1:50061")
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-node-1-new",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
                pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
                heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
                cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
                close=lambda reason="": None,
                _client=self,
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    session = TaskPool._from_infocenter(
        infocenter_target="127.0.0.1:50051",
        job_id="job-requested-node-id-restart",
        source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
        entry_module="task_demo",
        entry_callable="run",
        worker_count=1,
        node_ids=["node-1"],
        timeout_sec=1.0,
    )
    try:
        assert calls["select"] >= 2
        assert expected_node_instance_ids == ["node-1-old", "node-1-new"]
        assert session.node_instance_ids == ["node-1-new"]
        assert "node-1-old" in session.failures
    finally:
        session.close()


def test_task_pool_from_infocenter_does_not_rediscover_requested_node_instance_id(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    old_node = SimpleNamespace(
        node_instance_id="node-1-old",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        task_pool_worker_available=2,
        task_pool_worker_capacity=2,
    )
    new_node = SimpleNamespace(
        node_instance_id="node-1-new",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        task_pool_worker_available=2,
        task_pool_worker_capacity=2,
    )
    calls = {"select": 0}

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **kwargs):
            calls["select"] += 1
            assert kwargs["node_instance_ids"] == ["node-1-old"]
            return [old_node] if calls["select"] == 1 else [new_node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def close(self):
            return None

        def create_task_pool_from_bytes(self, **_kwargs):
            raise RuntimeError("cannot connect to 127.0.0.1:50061")

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    with pytest.raises(RuntimeError, match="task pool create failed"):
        TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-requested-node-instance-fixed",
            source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=1,
            node_instance_ids=["node-1-old"],
            timeout_sec=1.0,
        )
    assert calls["select"] == 1


def test_task_pool_session_packages_module_object_entry_module(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module(tmp_path, monkeypatch)
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    captured = {}

    def _fake_create_task_pool(self, **kwargs):
        captured.update(kwargs)
        return fake_pool_client

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        _fake_create_task_pool,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-module-entry",
            source=worker_module,
            worker_count=2,
            node_count=1,
        )

    try:
        assert captured["entry_module"] == worker_module.__name__
        assert captured["package_format"] == "tar.gz"
        with tarfile.open(fileobj=io.BytesIO(captured["blob"]), mode="r:gz") as tar:
            names = set(tar.getnames())
        assert f"{worker_module.__package__}/__init__.py" in names
        assert f"{worker_module.__package__}/worker.py" in names
        assert f"{worker_module.__package__}/helper.py" in names
        assert f"{worker_module.__package__}/ignored.csv" not in names
    finally:
        session.close()


def test_task_pool_session_packages_callable_object_entry_callable(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module(tmp_path, monkeypatch)
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    captured = {}

    def _fake_create_task_pool(self, **kwargs):
        captured.update(kwargs)
        return fake_pool_client

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        _fake_create_task_pool,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-callable-entry",
            source=worker_module.run,
            worker_count=2,
            node_count=1,
        )

    try:
        assert captured["entry_module"] == worker_module.__name__
        assert captured["entry_callable"] == "run"
        assert captured["package_format"] == "tar.gz"
        with tarfile.open(fileobj=io.BytesIO(captured["blob"]), mode="r:gz") as tar:
            names = set(tar.getnames())
        assert f"{worker_module.__package__}/__init__.py" in names
        assert f"{worker_module.__package__}/worker.py" in names
        assert f"{worker_module.__package__}/helper.py" in names
        assert f"{worker_module.__package__}/ignored.csv" not in names
    finally:
        session.close()


def test_task_pool_session_packages_namespace_module_with_synthetic_init(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module(tmp_path, monkeypatch, with_init=False)
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    captured = {}

    def _fake_create_task_pool(self, **kwargs):
        captured.update(kwargs)
        return fake_pool_client

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        _fake_create_task_pool,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-namespace-module-entry",
            source=worker_module,
            worker_count=2,
            node_count=1,
        )

    try:
        with tarfile.open(fileobj=io.BytesIO(captured["blob"]), mode="r:gz") as tar:
            names = set(tar.getnames())
            synthetic_init = tar.extractfile(f"{worker_module.__package__}/__init__.py")
            init_blob = synthetic_init.read() if synthetic_init is not None else None
        assert f"{worker_module.__package__}/__init__.py" in names
        assert init_blob == b""
        assert f"{worker_module.__package__}/worker.py" in names
        assert f"{worker_module.__package__}/helper.py" in names
    finally:
        session.close()


def test_task_pool_session_packages_callable_source(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module(tmp_path, monkeypatch)
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    captured = {}

    def _fake_create_task_pool(self, **kwargs):
        captured.update(kwargs)
        return fake_pool_client

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        _fake_create_task_pool,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-callable-source",
            source=worker_module.run,
            worker_count=2,
            node_count=1,
        )

    try:
        assert captured["entry_module"] == worker_module.__name__
        assert captured["entry_callable"] == "run"
        assert captured["package_format"] == "tar.gz"
    finally:
        session.close()


def test_native_task_pool_session_update_globals_aggregates_digests() -> None:
    from pycloud_parallel import TaskPool

    prepared_values = {}

    def _fake_prepare(clients, values, **_kwargs):
        prepared_values["clients"] = clients
        prepared_values["values"] = values
        return [{"cfg": {"k": "v"}}], {
            "globals_batch_count": 1,
            "batch_keys": [["cfg"]],
            "batch_bytes": [1],
            "staged_keys": [],
            "inline_keys": ["cfg"],
        }

    pool_a = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-a",
        pool_token="token-a",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        _client=SimpleNamespace(
            update_runtime_globals_prepared=lambda **kwargs: SimpleNamespace(globals_digest="sha256:same"),
        ),
    )
    pool_b = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-b",
        pool_token="token-b",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        _client=SimpleNamespace(
            update_runtime_globals_prepared=lambda **kwargs: SimpleNamespace(globals_digest="sha256:same"),
        ),
    )
    session = TaskPool(
        pools={"node-a": pool_a, "node-b": pool_b},
        nodes={},
        task_method="run",
        job_id="job-update-globals",
    )
    with patch("pycloud_parallel.execution.managed_globals._prepare_managed_globals_batches_for_upload", _fake_prepare):
        digest = session.update_globals({"cfg": {"k": "v"}})
    assert digest == "sha256:same"
    assert session.globals_digests == {"node-a": "sha256:same", "node-b": "sha256:same"}
    assert prepared_values["values"] == {"cfg": {"k": "v"}}


def test_native_task_pool_session_submit_values_delegates() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={
            "node-1": SimpleNamespace(
                owner_client_id="owner",
                code_version="sha256:test",
                heartbeat_timeout_sec=30,
                worker_count=2,
            )
        },
        nodes={},
        task_method="run",
        job_id="job-values",
    )
    captured = {"chunks": []}

    def _fake_submit(payloads, **kwargs):
        captured["chunks"].append(list(payloads))
        captured["kwargs"] = kwargs
        return pb2.SubmitTasksResponse(
            ok=True,
            accepted=[pb2.TaskAccepted(task_id=f"t-{len(captured['chunks'])}", status=pb2.TASK_STATUS_QUEUED)],
            rejected=[],
        )

    session.submit_payloads = _fake_submit  # type: ignore[method-assign]
    resp = session.submit_values([1, 2, 3, 4], arg_name="x", extra=9)
    assert captured["chunks"] == [
        [{"x": 1, "extra": 9}, {"x": 2, "extra": 9}, {"x": 3, "extra": 9}],
        [{"x": 4, "extra": 9}],
    ]
    assert len(resp.accepted) == 2


def test_native_task_pool_session_is_alive_tracks_remaining_nodes() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={
            "node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30),
            "node-2": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30),
        },
        nodes={},
        task_method="run",
        job_id="job-alive",
    )
    assert session.is_alive() is True
    session._active_nodes.discard("node-1")  # noqa: SLF001
    assert session.is_alive() is True
    session._active_nodes.clear()  # noqa: SLF001
    session.failed = True
    assert session.is_alive() is False


def test_native_task_pool_session_submit_payloads_avoids_degraded_nodes() -> None:
    from pycloud_parallel import TaskPool

    submitted_to: list[str] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._client = SimpleNamespace()

        def submit_tasks(self, tasks, job_id=""):
            submitted_to.extend([self.node_id] * len(tasks))
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

    session = TaskPool(
        pools={
            "node-good": _Pool("node-good"),
            "node-bad": _Pool("node-bad"),
        },
        nodes={},
        task_method="run",
        job_id="job-active-submit",
    )
    session._active_nodes = {"node-good"}  # noqa: SLF001

    session.submit_payloads([{"value": 1}, {"value": 2}, {"value": 3}])
    assert submitted_to == ["node-good", "node-good", "node-good"]


def test_native_task_pool_session_submit_payloads_splits_by_node_http_limit() -> None:
    from pycloud_parallel import TaskPool

    batch_sizes: list[int] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self) -> None:
            self._client = SimpleNamespace()

        def submit_tasks(self, tasks, job_id=""):
            batch_sizes.append(len(tasks))
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={
            "node-1": SimpleNamespace(
                capability={
                    "max_http_body_bytes": 4096,
                    "max_control_send_bytes": 4096,
                    "max_control_recv_bytes": 4096,
                },
                inflight=0,
                task_pool_worker_available=1,
            )
        },
        task_method="run",
        job_id="job-split-submit",
    )

    payloads = [{"value": "x" * 2048} for _ in range(4)]
    resp = session.submit_payloads(payloads, serialization_mode="pickle_stable_v1")

    assert len(resp.accepted) == 4
    assert len(batch_sizes) > 1
    assert sum(batch_sizes) == 4


def test_native_task_pool_session_imap_unordered_logs_zero_submitted(caplog) -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        kind = "task_pool"
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        heartbeat_failure_threshold = 1
        worker_count = 1
        pool_id = "pool-zero"
        pool_name = "pool-zero-name"
        node_id = "node-1"
        status = "RUNNING"

        def __init__(self) -> None:
            self._client = SimpleNamespace()

        def get_status(self):
            return SimpleNamespace(
                status=self.status,
                task_count=0,
                worker_count=self.worker_count,
                pool_name=self.pool_name,
            )

        def lease(self):
            return SimpleNamespace(last_heartbeat_at=None, lease_expire_at=None)

        def close(self, *, reason: str = ""):
            self.status = "STOPPED"

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-zero-submit",
    )

    with caplog.at_level(logging.WARNING):
        assert list(session.imap_unordered([], timeout_sec=1.0, result_timeout_sec=1.0)) == []

    assert any(
        "zero submitted tasks" in record.getMessage()
        and "pool-zero-name" in record.getMessage()
        for record in caplog.records
    )


def test_native_task_pool_session_submit_payloads_fail_when_no_active_nodes() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={
            "node-good": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30),
            "node-bad": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30),
        },
        nodes={},
        task_method="run",
        job_id="job-no-active-submit",
    )
    session._active_nodes = set()  # noqa: SLF001

    with pytest.raises(RuntimeError, match="no node pools|no active"):
        session.submit_payloads([{"value": 1}])


def test_native_task_pool_session_submit_payloads_accepts_throughput_strategy() -> None:
    from pycloud_parallel import TaskPool

    captured = {}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._client = SimpleNamespace()

        def submit_tasks(self, tasks, job_id=""):
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

    session = TaskPool(
        pools={"node-a": _Pool("node-a"), "node-b": _Pool("node-b")},
        nodes={},
        task_method="run",
        job_id="job-strategy-submit",
    )

    def _fake_select(candidates, *, profile, state, round_robin_counter=0):
        captured["profile"] = profile.name
        return candidates[0]

    with patch("pycloud_parallel.execution.task_pool.select_one_candidate", side_effect=_fake_select):
        session.submit_payloads([{"value": 1}], strategy="taskpool_throughput")

    assert captured["profile"] == "taskpool_throughput"


def test_native_task_pool_session_iter_items_batch_uses_imap_unordered_core() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-iter-items-core",
    )

    with patch.object(
        session,
        "imap_unordered",
        return_value=iter([(0, {"value": 1}), (1, None)]),
    ) as mocked:
        items = list(session.iter_items([{"value": 1}, {"value": 2}], timeout_sec=0.1))

    assert [item.index for item in items] == [0, 1]
    assert items[0].ok is True and items[0].result == {"value": 1}
    assert items[1].ok is False and items[1].result is None
    mocked.assert_called_once()


def test_native_task_pool_session_iter_items_forwards_progress_to_imap_core() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-iter-items-progress",
    )
    callback = lambda event: None

    with patch.object(session, "imap_unordered", return_value=iter([])) as mocked:
        assert list(session.iter_items([{"value": 1}], timeout_sec=0.1, progress=callback, progress_interval_sec=0.25)) == []

    assert mocked.call_args.kwargs["progress"] is callback
    assert mocked.call_args.kwargs["progress_interval_sec"] == 0.25


def test_native_task_pool_session_map_forwards_strategy_to_collect_items() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-map-strategy",
    )

    with patch.object(session, "collect_items", return_value=[]) as mocked:
        session.map([1, 2], strategy="taskpool_throughput")

    assert mocked.call_args.kwargs["strategy"] == "taskpool_throughput"


def test_native_task_pool_session_map_forwards_progress_to_collect_items() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-map-progress",
    )
    callback = lambda event: None

    with patch.object(session, "collect_items", return_value=[]) as mocked:
        session.map([1, 2], progress=callback, progress_interval_sec=0.5)

    assert mocked.call_args.kwargs["progress"] is callback
    assert mocked.call_args.kwargs["progress_interval_sec"] == 0.5
    assert len(mocked.call_args.args[0]) == 2


def test_native_task_pool_session_map_keeps_non_option_progress_as_payload_field() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-map-progress-payload",
    )

    with patch.object(session, "collect_items", return_value=[]) as mocked:
        session.map([1], progress="business-progress")

    payloads = list(mocked.call_args.args[0])
    assert payloads == [{"value": 1, "progress": "business-progress"}]
    assert "progress" not in mocked.call_args.kwargs


def test_native_task_pool_session_map_values_alias_matches_map() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-map-values",
    )

    with patch.object(session, "map", return_value=[{"value": 1}]) as mocked:
        assert session.map_values([1], arg_name="value", extra=9) == [{"value": 1}]

    assert mocked.call_args.args[0] == [1]
    assert mocked.call_args.kwargs["arg_name"] == "value"
    assert mocked.call_args.kwargs["extra"] == 9


def test_native_task_pool_session_call_async_alias_delegates_to_call() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-call-async-alias",
    )

    async def _fake_call(method: str, **kwargs):
        return {"method": method, "kwargs": kwargs}

    session.call = _fake_call  # type: ignore[method-assign]

    assert asyncio.run(session.call_async("run", value=1)) == {"method": "run", "kwargs": {"value": 1}}


def test_native_task_pool_session_imap_unordered_accepts_server_wait_ms_alias() -> None:
    from pycloud_parallel import TaskPool

    pull_waits = []
    accepted_task_ids = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        _client = SimpleNamespace(
            close=lambda: None,
            fetch_result_data=lambda task_result, target_path="": {"value": 1},
        )
        worker_count = 1

        def submit_tasks(self, tasks, job_id=""):
            del job_id
            accepted_task_ids.extend(str(item.task_id) for item in tasks)
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, *, limit=1, wait_ms=0, cursor=""):
            del limit, cursor
            pull_waits.append(int(wait_ms))
            return pb2.PullResultsResponse(
                ok=True,
                results=[
                    pb2.TaskResult(
                        task_id=accepted_task_ids.pop(0),
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"value": 1}),
                    )
                ],
                next_cursor="",
            )

        def close(self, reason=""):
            del reason
            return pb2.CloseTaskPoolResponse(ok=True, accepted=True)

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-server-wait-ms",
    )

    try:
        assert list(session.imap_unordered([{"value": 1}], server_wait_ms=7, timeout_sec=0.5)) == [(0, {"value": 1})]
        assert pull_waits == [7]
    finally:
        session.close()


def test_native_task_pool_session_map_builds_payloads_lazily() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-map-lazy",
    )
    consumed = {"count": 0}

    def _values():
        for value in range(1000):
            consumed["count"] += 1
            yield value

    def _fake_collect(payloads, **kwargs):
        first = next(iter(payloads))
        assert first == {"value": 0, "extra": 9}
        assert consumed["count"] == 1
        return []

    with patch.object(session, "collect_items", side_effect=_fake_collect):
        assert session.map(_values(), arg_name="value", extra=9) == []
    assert consumed["count"] == 1


def test_native_task_pool_session_keepalive_degrades_per_node() -> None:
    from pycloud_parallel import TaskPool

    calls: list[tuple[str, int]] = []

    class _Pool:
        def __init__(self, node_id: str, *, should_fail: bool) -> None:
            self.node_id = node_id
            self.owner_client_id = "owner"
            self.code_version = "sha256:test"
            self.heartbeat_timeout_sec = 1
            self._should_fail = should_fail

        def heartbeat(self, *, seq: int = 0):
            calls.append((self.node_id, seq))
            if self._should_fail:
                raise RuntimeError(f"{self.node_id} heartbeat failed")
            return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=1)

    session = TaskPool(
        pools={
            "node-bad": _Pool("node-bad", should_fail=True),
            "node-good": _Pool("node-good", should_fail=False),
        },
        nodes={},
        task_method="run",
        job_id="job-hb",
    )

    session._start_keepalive(interval_sec=0.05)
    try:
        import time

        deadline = time.time() + 1.0
        while time.time() < deadline and "node-bad" not in session.failures:
            time.sleep(0.05)

        assert "node-bad" in session.failures
        assert "heartbeat failed" in session.failures["node-bad"]
        assert "node-good" in session._active_nodes  # noqa: SLF001
        assert "node-bad" not in session._active_nodes  # noqa: SLF001
        assert session.failed is False
        assert session.is_alive() is True
    finally:
        session.close()


def test_native_task_pool_session_keepalive_heartbeats_replicas_concurrently() -> None:
    from pycloud_parallel import TaskPool

    good_seen = threading.Event()
    good_second_seen = threading.Event()
    slow_started = threading.Event()
    release_slow = threading.Event()
    good_count = {"value": 0}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id

        def heartbeat(self, *, seq: int = 0):
            del seq
            if self.node_id == "node-slow":
                slow_started.set()
                release_slow.wait(1.0)
            else:
                good_count["value"] += 1
                good_seen.set()
                if good_count["value"] >= 2:
                    good_second_seen.set()
            return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=1)

    session = TaskPool(
        pools={
            "node-slow": _Pool("node-slow"),
            "node-good": _Pool("node-good"),
        },
        nodes={},
        task_method="run",
        job_id="job-hb-concurrent",
    )

    session._active_replica_ids = {"node-slow", "node-good"}  # noqa: SLF001
    thread = threading.Thread(target=session._keepalive_loop, args=(0.01,), daemon=True)  # noqa: SLF001
    thread.start()
    try:
        assert slow_started.wait(0.5)
        assert good_seen.wait(0.2)
        assert good_second_seen.wait(0.5)
        assert "node-good" in session._active_replica_ids  # noqa: SLF001
    finally:
        release_slow.set()
        session._hb_stop.set()  # noqa: SLF001
        thread.join(timeout=1.0)


def test_native_task_pool_session_single_replica_heartbeat_does_not_block_loop(caplog) -> None:
    from pycloud_parallel import TaskPool

    started = threading.Event()
    release = threading.Event()

    class _SlowPool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 1

        def heartbeat(self, *, seq: int = 0):
            del seq
            started.set()
            release.wait(2.0)
            return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=1)

    session = TaskPool(
        pools={"node-slow": _SlowPool()},
        nodes={},
        task_method="run",
        job_id="job-single-hb",
    )

    with caplog.at_level(logging.WARNING):
        session._start_keepalive(interval_sec=0.01)  # noqa: SLF001
        try:
            assert started.wait(0.5)
            deadline = time.time() + 1.5
            while time.time() < deadline:
                if any("keepalive heartbeat pending" in record.getMessage() for record in caplog.records):
                    break
                time.sleep(0.05)
            assert any("keepalive heartbeat pending" in record.getMessage() for record in caplog.records)
            assert session._hb_thread is not None  # noqa: SLF001
            assert session._hb_thread.is_alive()  # noqa: SLF001
        finally:
            release.set()
            session.close()


def test_native_task_pool_session_transient_timeout_keeps_retrying_replica() -> None:
    from pycloud_parallel import TaskPool

    calls = {"value": 0}

    class _FlakyPool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 1
        heartbeat_failure_threshold = 1

        def heartbeat(self, *, seq: int = 0):
            del seq
            calls["value"] += 1
            if calls["value"] == 1:
                raise TimeoutError("temporary heartbeat timeout")
            return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=1)

    session = TaskPool(
        pools={"node-flaky": _FlakyPool()},
        nodes={},
        task_method="run",
        job_id="job-flaky-hb",
    )

    session._start_keepalive(interval_sec=0.02)  # noqa: SLF001
    try:
        deadline = time.monotonic() + 1.0
        while (
            time.monotonic() < deadline
            and (
                calls["value"] < 2
                or "node-flaky" not in session._active_replica_ids  # noqa: SLF001
                or "node-flaky" in session.failures
            )
        ):
            time.sleep(0.02)

        assert calls["value"] >= 2
        assert session.failed is False
        assert "node-flaky" in session._active_replica_ids  # noqa: SLF001
        assert "node-flaky" not in session.failures
    finally:
        session.close()



def test_native_task_pool_late_success_does_not_reactivate_removed_replica() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        kind = "task_pool"
        pool_id = "pool-old"
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 1
        heartbeat_failure_threshold = 1
        failed = True
        last_error = "timeout"
        status = "RUNNING"

        def heartbeat(self, *, seq: int = 0):
            del seq
            return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=1)

    pool = _Pool()
    session = TaskPool(
        pools={"node-flaky": pool},
        nodes={},
        task_method="run",
        job_id="job-late-success",
    )
    session._discard_active_replica("node-flaky")  # noqa: SLF001
    session._discard_retry_probe_replica("node-flaky")  # noqa: SLF001

    session._mark_replica_heartbeat_success("node-flaky", pool)  # noqa: SLF001

    assert "node-flaky" not in session._active_replica_ids  # noqa: SLF001
    assert pool.failed is True
    assert pool.last_error == "timeout"
    session.close()

def test_native_task_pool_late_failure_does_not_probe_removed_replica() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        kind = "task_pool"
        pool_id = "pool-old"
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 1
        heartbeat_failure_threshold = 1
        failed = False
        last_error = ""
        status = "RUNNING"

    pool = _Pool()
    session = TaskPool(
        pools={"node-flaky": pool},
        nodes={},
        task_method="run",
        job_id="job-late-failure",
    )
    session._discard_active_replica("node-flaky")  # noqa: SLF001
    session._discard_retry_probe_replica("node-flaky")  # noqa: SLF001

    session._record_heartbeat_failure("node-flaky", pool, TimeoutError("late timeout"))  # noqa: SLF001

    assert "node-flaky" not in session._retry_probe_replica_ids  # noqa: SLF001
    assert "node-flaky" not in session._active_replica_ids  # noqa: SLF001
    assert pool.failed is False
    assert pool.last_error == ""
    session.close()

def test_native_task_pool_session_started_stuck_heartbeat_does_not_block_retry() -> None:
    from pycloud_parallel import TaskPool

    first_started = threading.Event()
    second_seen = threading.Event()
    release_first = threading.Event()
    calls = {"value": 0}

    class _StuckThenHealthyPool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 1
        heartbeat_failure_threshold = 1

        def heartbeat(self, *, seq: int = 0):
            del seq
            calls["value"] += 1
            if calls["value"] == 1:
                first_started.set()
                release_first.wait(3.0)
                return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=1)
            second_seen.set()
            return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=1)

    session = TaskPool(
        pools={"node-stuck": _StuckThenHealthyPool()},
        nodes={},
        task_method="run",
        job_id="job-stuck-hb",
    )

    session._start_keepalive(interval_sec=0.02)  # noqa: SLF001
    try:
        assert first_started.wait(0.5)
        assert second_seen.wait(2.5)
        assert session.failed is False
    finally:
        release_first.set()
        session.close()


def test_native_task_pool_session_stale_heartbeat_pending_is_bounded(caplog) -> None:
    from pycloud_parallel import TaskPool

    started = threading.Event()
    release = threading.Event()

    class _AlwaysStuckPool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 1
        heartbeat_failure_threshold = 1

        def heartbeat(self, *, seq: int = 0):
            del seq
            started.set()
            release.wait(5.0)
            return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=1)

    session = TaskPool(
        pools={
            "node-stuck-1": _AlwaysStuckPool(),
            "node-stuck-2": _AlwaysStuckPool(),
        },
        nodes={},
        task_method="run",
        job_id="job-stale-hb",
    )
    session._stale_heartbeat_pending_limit = lambda: 1  # type: ignore[method-assign]  # noqa: SLF001

    with caplog.at_level(logging.WARNING):
        session._start_keepalive(interval_sec=0.02)  # noqa: SLF001
        try:
            assert started.wait(0.5)
            deadline = time.monotonic() + 6.0
            while time.monotonic() < deadline:
                if any("stale heartbeat pending limit reached" in record.getMessage() for record in caplog.records):
                    break
                time.sleep(0.05)
            assert any("stale heartbeat pending limit reached" in record.getMessage() for record in caplog.records)
            assert session.failed is False
        finally:
            release.set()
            session.close()


def test_native_task_pool_client_heartbeat_uses_short_rpc_timeout() -> None:
    from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient

    seen = {}

    class _Client:
        def heartbeat_task_pool(self, **kwargs):
            seen.update(kwargs)
            return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=30)

    pool = NativeTaskPoolClient(
        _client=_Client(),
        owner_client_id="owner",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=1,
        heartbeat_timeout_sec=60,
    )

    pool.heartbeat(seq=7)

    assert seen["seq"] == 7
    assert seen["timeout_sec"] == 5.0


def test_native_task_pool_client_local_adapter_accepts_heartbeat_timeout() -> None:
    from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient
    from pycloud_parallel.execution.task_pool import _LocalTaskPoolNodeClient

    pool_state = SimpleNamespace(
        owner_client_id="owner-local",
        pool_id="pool-local-hb",
        pool_token="token-local-hb",
        code_version="sha256:local",
        worker_count=1,
        heartbeat_timeout_sec=5,
        pool_name="pool-local-hb",
        idle_ttl_sec=0,
        status="RUNNING",
        created_at=datetime.utcnow(),
        last_heartbeat_at=datetime.utcnow(),
    )

    class _State:
        node_id = "node-local-hb"
        node_instance_id = "node-local-hb-instance"

        def heartbeat_task_pool(self, **kwargs):
            assert kwargs == {
                "owner_client_id": "owner-local",
                "pool_id": "pool-local-hb",
                "pool_token": "token-local-hb",
            }
            return pool_state

    adapter = _LocalTaskPoolNodeClient(_State(), pool=pool_state)
    pool = NativeTaskPoolClient(
        _client=adapter,
        owner_client_id=adapter.owner_client_id,
        pool_id=adapter.pool_id,
        pool_token=adapter.pool_token,
        code_version=adapter.code_version,
        worker_count=adapter.worker_count,
        heartbeat_timeout_sec=adapter.heartbeat_timeout_sec,
    )

    response = pool.heartbeat(seq=3)

    assert response.ok is True
    assert response.accepted is True


def test_native_task_pool_session_keepalive_fails_when_all_nodes_fail() -> None:
    from pycloud_parallel import TaskPool

    class _FailPool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 1

        def heartbeat(self, *, seq: int = 0):
            raise RuntimeError(f"heartbeat failed seq={seq}")

    session = TaskPool(
        pools={"node-1": _FailPool()},
        nodes={},
        task_method="run",
        job_id="job-fail-all",
    )

    session._start_keepalive(interval_sec=0.05)
    try:
        import time

        deadline = time.time() + 1.0
        while time.time() < deadline and not session.failed:
            time.sleep(0.05)

        assert session.failed is True
        assert session.is_alive() is False
        assert "node-1" in session.failures
    finally:
        session.close()


def test_native_task_pool_session_keepalive_stops_terminal_pool_errors(caplog) -> None:
    from pycloud_parallel import TaskPool

    class _StoppedPool:
        kind = "task_pool"
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 1
        heartbeat_failure_threshold = 3
        pool_id = "pool-terminal-1"
        pool_name = "pool-terminal"
        node_id = "node-1"
        status = "RUNNING"

        def __init__(self) -> None:
            self.calls = 0
            self.failed = False
            self.last_error = ""

        def heartbeat(self, *, seq: int = 0):
            del seq
            self.calls += 1
            raise RuntimeError("task pool not running")

    pool = _StoppedPool()
    session = TaskPool(
        pools={"node-1": pool},
        nodes={},
        task_method="run",
        job_id="job-terminal-hb",
    )

    with caplog.at_level("WARNING"):
        session._start_keepalive(interval_sec=0.02)  # noqa: SLF001
        try:
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline and not session.failed:
                time.sleep(0.02)

            first_call_count = pool.calls
            time.sleep(0.12)
            assert pool.calls == first_call_count == 1
            assert session.failed is True
            assert "node-1" in session.failures
            assert "node-1" not in session._active_replica_ids  # noqa: SLF001
        finally:
            session.close()

    stopped_logs = [
        record
        for record in caplog.records
        if "keepalive replica stopped" in record.getMessage()
    ]
    failed_logs = [
        record
        for record in caplog.records
        if "keepalive heartbeat failed" in record.getMessage()
    ]
    assert len(stopped_logs) == 1
    stopped_message = stopped_logs[0].getMessage()
    assert "pool-terminal-1" in stopped_message
    assert "pool-terminal" in stopped_message
    assert "node-1" in stopped_message
    assert failed_logs == []


def test_task_pool_compensation_replaces_retryable_failed_same_node(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    node = SimpleNamespace(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
    )
    closed_reasons = []
    created = []
    create_kwargs = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec
            self.closed = False

        def close(self):
            self.closed = True

        def create_task_pool_from_bytes(self, **kwargs):
            create_kwargs.append(dict(kwargs))
            pool = SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id=f"new-pool-{len(created) + 1}",
                pool_name=kwargs["pool_name"],
                pool_token="token-new",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
                pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
                heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
                cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
                close=lambda reason="": closed_reasons.append(("new", reason)),
                _client=self,
            )
            created.append(pool)
            return pool

    old_client = SimpleNamespace(close=lambda: closed_reasons.append(("old-client", "")))
    old_pool = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="old-pool",
        pool_name="demo-pool",
        pool_token="token-old",
        code_version="sha256:test",
        worker_count=1,
        heartbeat_timeout_sec=30,
        close=lambda reason="": closed_reasons.append(("old", reason)),
        _client=old_client,
    )
    session = TaskPool(
        pools={"node-inst-1": old_pool},
        nodes={"node-inst-1": node},
        task_method="run",
        job_id="job-compensate-replace",
    )
    session.failures["node-inst-1"] = "RuntimeError('task pool not running')"
    session._discard_active_replica("node-inst-1")  # noqa: SLF001
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "owner_client_id": "owner-demo",
            "pool_name": "demo-pool",
            "blob": b"def run(**kwargs):\n    return kwargs\n",
            "runtime": "py3",
            "entry_module": "task_demo",
            "entry_callable": "run",
            "package_format": "",
            "deps": None,
            "managed_global_names": [],
            "initial_globals": {},
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "chunk_size": 1024,
            "healthy_only": True,
            "tags": [],
            "node_ids": [],
            "node_instance_ids": [],
            "node_count": 1,
            "node_limit": 10,
            "timeout_sec": 1.0,
            "api_token": "",
        }
    )
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    try:
        added = session.try_compensate_replicas()
        assert added == 1
        assert created
        assert create_kwargs[0]["expected_node_instance_id"] == "node-inst-1"
        assert session._pools["node-inst-1"] is created[0]  # noqa: SLF001
        assert "node-inst-1" in session._active_replica_ids  # noqa: SLF001
        assert "node-inst-1" not in session.failures
        assert ("old", "replace failed task pool replica") in closed_reasons
        assert ("old-client", "") in closed_reasons
    finally:
        session.close()


def test_task_pool_keepalive_compensates_pool_not_running(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    node = SimpleNamespace(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
    )
    created = []
    closed_reasons = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def close(self):
            return None

        def create_task_pool_from_bytes(self, **kwargs):
            pool = SimpleNamespace(
                kind="task_pool",
                owner_client_id=kwargs["owner_client_id"],
                pool_id=f"new-pool-{len(created) + 1}",
                pool_name=kwargs["pool_name"],
                pool_token="token-new",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=1),
                submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
                pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
                cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
                close=lambda reason="": closed_reasons.append(("new", reason)),
                _client=self,
            )
            created.append(pool)
            return pool

    class _StoppedPool:
        kind = "task_pool"
        owner_client_id = "owner-demo"
        pool_id = "old-pool"
        pool_name = "demo-pool"
        pool_token = "token-old"
        code_version = "sha256:test"
        worker_count = 1
        heartbeat_timeout_sec = 1
        status = "RUNNING"
        _client = SimpleNamespace(close=lambda: closed_reasons.append(("old-client", "")))

        def heartbeat(self, *, seq: int = 0):
            del seq
            raise RuntimeError("task pool not running")

        def close(self, reason=""):
            closed_reasons.append(("old", reason))

    session = TaskPool(
        pools={"node-inst-1": _StoppedPool()},
        nodes={"node-inst-1": node},
        task_method="run",
        job_id="job-keepalive-compensate-pool-stopped",
    )
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "owner_client_id": "owner-demo",
            "pool_name": "demo-pool",
            "blob": b"def run(**kwargs):\n    return kwargs\n",
            "runtime": "py3",
            "entry_module": "task_demo",
            "entry_callable": "run",
            "package_format": "",
            "deps": None,
            "managed_global_names": [],
            "initial_globals": {},
            "worker_count": 1,
            "heartbeat_timeout_sec": 1,
            "idle_ttl_sec": 0,
            "chunk_size": 1024,
            "healthy_only": True,
            "tags": [],
            "node_ids": [],
            "node_instance_ids": [],
            "node_count": 1,
            "node_limit": 10,
            "timeout_sec": 1.0,
            "api_token": "",
            "check_interval_sec": 0.01,
        }
    )
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    try:
        session._start_keepalive(interval_sec=0.02)  # noqa: SLF001
        deadline = time.monotonic() + 1.5
        while time.monotonic() < deadline and not created:
            time.sleep(0.02)
        assert created
        assert session.failed is False
        assert session._pools["node-inst-1"] is created[0]  # noqa: SLF001
        assert "node-inst-1" in session._active_replica_ids  # noqa: SLF001
        assert "node-inst-1" not in session.failures
        assert ("old", "replace failed task pool replica") in closed_reasons
    finally:
        session.close()


def test_native_task_pool_session_close_retries_replica_close() -> None:
    from pycloud_parallel import TaskPool

    close_calls = {"count": 0}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        _client = SimpleNamespace(close=lambda: None)

        def close(self, reason=""):
            del reason
            close_calls["count"] += 1
            if close_calls["count"] == 1:
                raise RuntimeError("temporary close failure")
            return pb2.CloseTaskPoolResponse(ok=True, accepted=True)

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-close-retry",
    )

    session.close()

    assert close_calls["count"] >= 2


def test_native_task_pool_session_iter_and_collect_results_consume_incrementally() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-1", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="task-2", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 2})),
                pb2.TaskResult(task_id="task-3", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 3})),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    pool = _Pool()
    session = TaskPool(
        pools={"node-1": pool},
        nodes={},
        task_method="run",
        job_id="job-iter",
    )
    session._pending_task_ids = {"task-1", "task-2", "task-3"}  # noqa: SLF001

    first_batch = list(session.iter_results(max_count=2, timeout_sec=0.1))
    assert [item.task_id for item in first_batch] == ["task-1", "task-2"]
    assert session._pending_task_ids == {"task-3"}  # noqa: SLF001

    second_batch = session.collect_results(timeout_sec=0.1)
    assert [item.task_id for item in second_batch] == ["task-3"]
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_session_iter_data_materializes_per_result() -> None:
    from pycloud_parallel import TaskPool

    fetched: list[str] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(
                fetch_result_data=lambda task_result, target_path="": fetched.append(task_result.task_id) or {"value": task_result.task_id}
            )
            self._results = [
                pb2.TaskResult(task_id="task-a", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="task-b", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 2})),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-data",
    )
    session._pending_task_ids = {"task-a", "task-b"}  # noqa: SLF001

    items = session.collect_data(timeout_sec=0.1)
    assert items == [("task-a", {"value": "task-a"}), ("task-b", {"value": "task-b"})]
    assert fetched == ["task-a", "task-b"]


def test_native_task_pool_session_collect_results_with_none_waits_pending_results() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-1", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="task-2", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 2})),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-none",
    )
    session._pending_task_ids = {"task-1", "task-2"}  # noqa: SLF001

    out = session.collect_results(max_count=None, timeout_sec=0.1)
    assert [item.task_id for item in out] == ["task-1", "task-2"]
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_session_imap_unordered_streams_results() -> None:
    from pycloud_parallel import TaskPool

    submitted: list[str] = []
    materialized: list[str] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(
                fetch_result_data=lambda task_result, target_path="": materialized.append(task_result.task_id) or {"value": task_result.task_id}
            )

        def submit_tasks(self, tasks, job_id=""):
            task_ids = [item.task_id for item in tasks]
            submitted.extend(task_ids)
            for task_id in task_ids:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"value": task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=task_id, status=pb2.TASK_STATUS_QUEUED) for task_id in task_ids],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-stream",
    )

    items = list(
        session.imap_unordered(
            [{"value": 1}, {"value": 2}, {"value": 3}],
            max_in_flight=2,
            receive_batch=1,
            submit_timeout_sec=1.0,
            result_timeout_sec=1.0,
        )
    )

    assert submitted == ["job-stream-task-0001", "job-stream-task-0002", "job-stream-task-0003"]
    assert [index for index, _ in items] == [0, 1, 2]
    assert materialized == submitted
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_session_imap_unordered_can_return_execution_items() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"task_id": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-inst-1": _Pool()},
        nodes={"node-inst-1": SimpleNamespace(node_id="node-A", task_pool_worker_available=1)},
        task_method="run",
        job_id="job-stream-items",
    )

    items = list(
        session.imap_unordered(
            [{"value": 1}],
            max_in_flight=1,
            receive_batch=1,
            submit_timeout_sec=1.0,
            result_timeout_sec=1.0,
            return_items=True,
        )
    )

    assert len(items) == 1
    assert items[0].index == 0
    assert items[0].task_id == "job-stream-items-task-0001"
    assert items[0].status == pb2.TASK_STATUS_SUCCEEDED
    assert items[0].node_id == "node-A"
    assert items[0].node_instance_id == "node-inst-1"
    assert items[0].ok is True


def test_native_task_pool_session_iter_items_payload_batch_preserves_execution_metadata() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_FAILED_USER,
                        error=pb2.TaskError(type="UserError", message="boom"),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-inst-1": _Pool()},
        nodes={"node-inst-1": SimpleNamespace(node_id="node-A", task_pool_worker_available=1)},
        task_method="run",
        job_id="job-batch-items",
    )

    items = list(
        session.iter_items(
            [{"value": 1}],
            max_in_flight=1,
            timeout_sec=1.0,
        )
    )

    assert len(items) == 1
    assert items[0].index == 0
    assert items[0].task_id == "job-batch-items-task-0001"
    assert items[0].status == pb2.TASK_STATUS_FAILED_USER
    assert items[0].node_id == "node-A"
    assert items[0].node_instance_id == "node-inst-1"
    assert items[0].error_type == "UserError"
    assert items[0].error_message == "boom"


def test_native_task_pool_session_unordered_can_return_execution_items() -> None:
    from pycloud_parallel import TaskPool
    from pycloud_parallel.execution.base import ExecutionItem

    item = ExecutionItem(
        index=0,
        ok=True,
        result={"value": 1},
        task_id="task-1",
        status=pb2.TASK_STATUS_SUCCEEDED,
        node_instance_id="node-inst-1",
    )
    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-unordered-items",
    )

    with patch.object(session, "iter_items", return_value=iter([item])):
        out = list(session.unordered([{"value": 1}], return_items=True))

    assert out == [item]


def test_native_task_pool_session_aunordered_can_return_execution_items() -> None:
    from pycloud_parallel import TaskPool
    from pycloud_parallel.execution.base import ExecutionItem

    item = ExecutionItem(
        index=0,
        ok=False,
        result=None,
        error_type="UserError",
        error_message="boom",
        task_id="task-1",
        status=pb2.TASK_STATUS_FAILED_USER,
        node_instance_id="node-inst-1",
    )
    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-aunordered-items",
    )

    async def _fake_aiter_items(*args, **kwargs):
        yield item

    async def _collect():
        with patch.object(session, "aiter_items", side_effect=_fake_aiter_items):
            return [value async for value in session.aunordered([{"value": 1}], return_items=True)]

    assert asyncio.run(_collect()) == [item]


def test_indexed_payload_buffer_wraps_non_mapping_payloads(caplog) -> None:
    from pycloud_parallel.execution.task_pool import _IndexedPayloadBuffer

    buffer = _IndexedPayloadBuffer([None, {"x": 1}])

    with caplog.at_level("WARNING"):
        assert buffer.next() == (0, {"value": None})
    assert buffer.next() == (1, {"x": 1})
    assert "wrapping it as {'value': item}" in caplog.text


def test_native_task_pool_session_submit_payloads_keeps_round_robin_without_polling() -> None:
    from pycloud_parallel import TaskPool

    submissions: dict[str, list[str]] = {"node-1": [], "node-2": []}
    pull_calls: list[tuple[str, int]] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._client = SimpleNamespace()

        def submit_tasks(self, tasks, job_id=""):
            submissions[self.node_id].extend(item.task_id for item in tasks)
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            pull_calls.append((self.node_id, int(wait_ms)))
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool("node-1"), "node-2": _Pool("node-2")},
        nodes={},
        task_method="run",
        job_id="job-submit-rr",
    )

    resp = session.submit_payloads([{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}], job_id="job-submit-override")

    assert len(resp.accepted) == 4
    assert submissions["node-1"] == ["job-submit-rr-task-0001", "job-submit-rr-task-0003"]
    assert submissions["node-2"] == ["job-submit-rr-task-0002", "job-submit-rr-task-0004"]
    assert pull_calls == []
    assert session._pending_task_ids == set(resp_task.task_id for resp_task in resp.accepted)  # noqa: SLF001


def test_native_task_pool_session_imap_unordered_rotates_poll_order() -> None:
    from pycloud_parallel import TaskPool

    pull_calls: list[tuple[str, int]] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            pull_calls.append((self.node_id, int(wait_ms)))
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool("node-1"), "node-2": _Pool("node-2")},
        nodes={},
        task_method="run",
        job_id="job-poll-rotate",
    )

    with pytest.raises(TimeoutError, match="imap_unordered did not receive results"):
        list(
            session.imap_unordered(
                [{"value": 1}],
                max_in_flight=1,
                receive_batch=1,
                result_timeout_sec=0.08,
                wait_ms=1,
            )
        )

    assert [node_id for node_id, _wait_ms in pull_calls[:2]] == ["node-1", "node-2"]
    assert sum(1 for _node_id, wait_ms in pull_calls[:2] if wait_ms == 1) == 1
    assert sum(1 for _node_id, wait_ms in pull_calls[:2] if wait_ms == 0) == 1
    assert len(pull_calls) >= 2


def test_native_task_pool_session_collect_results_waits_on_one_node_per_round() -> None:
    from pycloud_parallel import TaskPool

    pull_calls: list[tuple[str, int]] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": task_result.task_id)

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            pull_calls.append((self.node_id, int(wait_ms)))
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool("node-1"), "node-2": _Pool("node-2")},
        nodes={},
        task_method="run",
        job_id="job-collect-wait",
    )
    session._pending_task_ids = {"task-1"}  # noqa: SLF001
    session._pending_task_node_ids = {"task-1": "node-2"}  # noqa: SLF001

    assert session.collect_results(timeout_sec=0.05, wait_ms=7) == []

    assert len(pull_calls) >= 2
    assert [node_id for node_id, _wait_ms in pull_calls[:2]] == ["node-1", "node-2"]
    assert sum(1 for _node_id, wait_ms in pull_calls[:2] if wait_ms == 7) == 1
    assert sum(1 for _node_id, wait_ms in pull_calls[:2] if wait_ms == 0) == 1


def test_native_task_pool_session_imap_unordered_refills_fast_node() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node: dict[str, list[str]] = {"node-slow": [], "node-fast": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str, *, ready_after_pulls: int) -> None:
            self.node_id = node_id
            self._ready_after_pulls = ready_after_pulls
            self._inflight: list[list[object]] = []
            self._client = SimpleNamespace(
                fetch_result_data=lambda task_result, target_path="": {
                    "task_id": task_result.task_id,
                    "node_id": self.node_id,
                }
            )

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                submitted_by_node[self.node_id].append(item.task_id)
                self._inflight.append([item.task_id, self._ready_after_pulls])
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            ready: list[pb2.TaskResult] = []
            kept: list[list[object]] = []
            for task_id, remaining in self._inflight:
                next_remaining = int(remaining) - 1
                if next_remaining <= 0 and len(ready) < limit:
                    ready.append(
                        pb2.TaskResult(
                            task_id=str(task_id),
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"node_id": self.node_id}),
                        )
                    )
                else:
                    kept.append([task_id, next_remaining])
            self._inflight = kept
            return pb2.PullResultsResponse(ok=True, results=ready, next_cursor="")

    session = TaskPool(
        pools={
            "node-slow": _Pool("node-slow", ready_after_pulls=3),
            "node-fast": _Pool("node-fast", ready_after_pulls=1),
        },
        nodes={},
        task_method="run",
        job_id="job-fast-refill",
    )

    items = list(
        session.imap_unordered(
            [{"value": idx} for idx in range(6)],
            max_in_flight=4,
            receive_batch=2,
            result_timeout_sec=0.5,
            wait_ms=1,
        )
    )

    assert len(items) == 6
    assert len(submitted_by_node["node-fast"]) == 4
    assert len(submitted_by_node["node-slow"]) == 2
    assert {data["node_id"] for _task_id, data in items} == {"node-fast", "node-slow"}


def test_native_task_pool_session_imap_unordered_uses_full_global_max_in_flight() -> None:
    from pycloud_parallel import TaskPool

    submit_batch_sizes: list[int] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self) -> None:
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(
                fetch_result_data=lambda task_result, target_path="": {
                    "task_id": task_result.task_id,
                }
            )

        def submit_tasks(self, tasks, job_id=""):
            submit_batch_sizes.append(len(tasks))
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"task_id": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-global-window",
    )

    items = list(
        session.imap_unordered(
            [{"value": idx} for idx in range(8)],
            max_in_flight=8,
            receive_batch=8,
            result_timeout_sec=0.5,
            wait_ms=1,
        )
    )

    assert len(items) == 8
    assert submit_batch_sizes[0] == 8


def test_native_task_pool_session_imap_unordered_times_out_when_results_do_not_arrive() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-timeout",
    )

    with pytest.raises(TimeoutError, match="imap_unordered did not receive results"):
        list(
            session.imap_unordered(
                [{"value": 1}],
                max_in_flight=1,
                receive_batch=1,
                result_timeout_sec=0.1,
                wait_ms=10,
            )
        )


def test_native_task_pool_session_imap_unordered_cancels_outstanding_on_error() -> None:
    from pycloud_parallel import TaskPool

    cancel_calls: list[tuple[str, str]] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self) -> None:
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                if item.task_id.endswith("0001"):
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_FAILED_USER,
                            error=pb2.TaskError(type="UserError", message="boom"),
                        )
                    )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

        def cancel_job(self, job_id="", reason=""):
            cancel_calls.append((job_id, reason))
            return pb2.CancelJobResponse(ok=True, queued_cancelled=1, running_marked=0, already_done=0, not_found=0)

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-error-cancel",
    )

    with pytest.raises(RuntimeError, match="boom"):
        list(
            session.imap_unordered(
                [{"value": 1}, {"value": 2}],
                max_in_flight=2,
                receive_batch=1,
                result_timeout_sec=0.5,
                wait_ms=1,
            )
        )

    assert cancel_calls == [("job-error-cancel", "imap_unordered task failure")]
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_proxy_sync_requires_clean_session() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-sync-clean",
    )
    session._pending_task_ids = {"task-old"}  # noqa: SLF001

    with pytest.raises(RuntimeError, match="requires a clean task pool session"):
        session.run.sync(value=7)


def test_native_task_pool_session_imap_unordered_requires_clean_session() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-imap-clean",
    )
    session._pending_task_ids = {"task-old"}  # noqa: SLF001

    with pytest.raises(RuntimeError, match="requires a clean task pool session"):
        list(session.imap_unordered([{"value": 1}]))


def test_native_task_pool_session_exclusive_mode_blocks_concurrent_submit_and_iter() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-exclusive",
    )
    session._exclusive_mode = "imap_unordered"  # noqa: SLF001
    session._exclusive_owner_thread_id = 999999  # noqa: SLF001
    session._exclusive_depth = 1  # noqa: SLF001

    with pytest.raises(RuntimeError, match="exclusively used by imap_unordered"):
        session.submit_payloads([{"value": 1}])

    with pytest.raises(RuntimeError, match="exclusively used by imap_unordered"):
        list(session.iter_data(timeout_sec=0.1))


def test_native_task_pool_session_drops_late_results_for_non_pending_tasks() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-late", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1}))
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-drop-late",
    )
    session._pending_task_ids = set()  # noqa: SLF001

    assert session.collect_results(timeout_sec=0.1) == []


def test_native_task_pool_session_collect_results_calls_iter_results() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-collect",
    )

    with patch.object(
        session,
        "iter_results",
        return_value=iter(
            [
                pb2.TaskResult(task_id="task-1", status=pb2.TASK_STATUS_SUCCEEDED),
                pb2.TaskResult(task_id="task-2", status=pb2.TASK_STATUS_SUCCEEDED),
            ]
        ),
    ) as mocked:
        out = session.collect_results(max_count=2, timeout_sec=1.0)

    assert [item.task_id for item in out] == ["task-1", "task-2"]
    mocked.assert_called_once_with(max_count=2, timeout_sec=1.0, wait_ms=500, limit=100, job_id="")


def test_native_task_pool_session_collect_results_accepts_server_wait_ms_alias() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-collect-server-wait",
    )

    with patch.object(session, "iter_results", return_value=iter([])) as mocked:
        assert session.collect_results(max_count=2, timeout_sec=1.0, server_wait_ms=9) == []

    mocked.assert_called_once_with(max_count=2, timeout_sec=1.0, server_wait_ms=9, wait_ms=500, limit=100, job_id="")


def test_native_task_pool_session_collect_data_calls_iter_data() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-collect-data",
    )

    with patch.object(
        session,
        "iter_data",
        return_value=iter([("task-1", {"value": 1}), ("task-2", {"value": 2})]),
    ) as mocked:
        out = session.collect_data(max_count=2, timeout_sec=1.0)

    assert out == [("task-1", {"value": 1}), ("task-2", {"value": 2})]
    mocked.assert_called_once_with(max_count=2, timeout_sec=1.0, wait_ms=500, limit=100, job_id="", raise_on_error=False, task_ids=None)


def test_native_task_pool_session_collect_data_accepts_server_wait_ms_alias() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-collect-data-server-wait",
    )

    with patch.object(session, "iter_data", return_value=iter([])) as mocked:
        assert session.collect_data(max_count=2, timeout_sec=1.0, server_wait_ms=11) == []

    mocked.assert_called_once_with(
        max_count=2,
        timeout_sec=1.0,
        server_wait_ms=11,
        wait_ms=500,
        limit=100,
        job_id="",
        raise_on_error=False,
        task_ids=None,
    )


def test_native_task_pool_session_unordered_has_strict_signature() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-unordered",
    )
    payloads = [{"value": 1}, {"value": 2}]

    with pytest.raises(TypeError):
        list(
            session.unordered(
                payloads,
                max_in_flight=4,
                receive_batch=2,
            )
        )


def test_native_task_pool_session_consume_unordered_calls_handle() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-consume-unordered",
    )
    payloads = [{"value": 1}, {"value": 2}]
    handled: list[tuple[object, object]] = []

    with patch.object(
        session,
        "imap_unordered",
        return_value=iter([("task-1", {"value": 1}), ("task-2", {"value": 2})]),
    ) as mocked:
        processed = session.consume_unordered(
            payloads,
            handle=lambda task_id, result: handled.append((task_id, result)),
            max_in_flight=3,
            receive_batch=1,
            submit_timeout_sec=1.5,
            result_timeout_sec=2.5,
            wait_ms=15,
            raise_on_error=False,
            node_window_factor=1.25,
        )

    assert processed == 2
    assert handled == [(0, {"value": 1}), (1, {"value": 2})]
    mocked.assert_called_once_with(
        payloads,
        task_method="",
        strategy="taskpool_default",
        max_in_flight=3,
        receive_batch=1,
        submit_timeout_sec=1.5,
        result_timeout_sec=2.5,
        wait_ms=15,
        raise_on_error=False,
        node_window_factor=1.25,
        return_items=False,
    )


def test_native_task_pool_session_iter_items_includes_failures() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-ok", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(
                    task_id="task-fail",
                    status=pb2.TASK_STATUS_FAILED_USER,
                    error=pb2.TaskError(type="UserError", message="boom"),
                ),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-items",
    )
    session._pending_task_ids = {"task-ok", "task-fail"}  # noqa: SLF001

    items = session.collect_items(timeout_sec=0.1)
    assert len(items) == 2
    assert items[0].task_id == "task-ok"
    assert items[0].ok is True
    assert items[0].data == {"value": "task-ok"}
    assert items[1].task_id == "task-fail"
    assert items[1].ok is False
    assert items[1].error_type == "UserError"
    assert items[1].error_message == "boom"


def test_native_task_pool_session_collect_data_returns_none_on_failure_by_default() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(
                    task_id="task-fail",
                    status=pb2.TASK_STATUS_FAILED_INFRA,
                    error=pb2.TaskError(type="InfraError", message="node lost"),
                )
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-fail-data",
    )
    session._pending_task_ids = {"task-fail"}  # noqa: SLF001

    out = session.collect_data(timeout_sec=0.1)
    assert out == [("task-fail", None)]


def test_native_task_pool_session_aiter_items_supports_receiving_existing_results() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-a", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="task-b", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 2})),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    async def _collect(session):
        items = []
        async for item in session.aiter_items(timeout_sec=0.1):
            items.append(item)
        return items

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-aiter-items",
    )
    session._pending_task_ids = {"task-a", "task-b"}  # noqa: SLF001

    items = asyncio.run(_collect(session))
    assert [item.task_id for item in items] == ["task-a", "task-b"]


def test_native_task_pool_session_acollect_items_supports_receiving_existing_results() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-a", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="task-b", status=pb2.TASK_STATUS_FAILED_USER, error=pb2.TaskError(type="UserError", message="boom")),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-acollect-items",
    )
    session._pending_task_ids = {"task-a", "task-b"}  # noqa: SLF001

    items = asyncio.run(session.acollect_items(timeout_sec=0.1))
    assert len(items) == 2
    assert items[0].task_id == "task-a"
    assert items[1].task_id == "task-b"
    assert items[1].ok is False


def test_native_task_pool_session_collect_data_raises_on_failure_when_enabled() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(
                    task_id="task-fail",
                    status=pb2.TASK_STATUS_FAILED_INFRA,
                    error=pb2.TaskError(type="InfraError", message="node lost"),
                )
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-fail-data-raise",
    )
    session._pending_task_ids = {"task-fail"}  # noqa: SLF001

    with pytest.raises(RuntimeError, match="node lost"):
        session.collect_data(timeout_sec=0.1, raise_on_error=True)


def test_native_task_pool_proxy_submit_returns_task_id() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-submit-id",
    )

    with patch.object(
        session,
        "submit_payloads",
        return_value=pb2.SubmitTasksResponse(
            ok=True,
            accepted=[pb2.TaskAccepted(task_id="task-submit-1", status=pb2.TASK_STATUS_QUEUED)],
            rejected=[],
        ),
    ) as mocked:
        task_id = session.run.submit(value=7)

    assert task_id == "task-submit-1"
    mocked.assert_called_once()


def test_native_task_pool_proxy_call_waits_for_own_task_id() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-own-task",
    )

    with patch.object(
        session,
        "submit_payloads",
        return_value=pb2.SubmitTasksResponse(
            ok=True,
            accepted=[pb2.TaskAccepted(task_id="task-own-1", status=pb2.TASK_STATUS_QUEUED)],
            rejected=[],
        ),
    ), patch.object(
        session,
        "_collect_data_for_task_ids",
        return_value=[("task-own-1", {"value": 49})],
    ) as mocked_collect:
        task_id = session.run(value=7)
        result = session.run.sync(value=7)

    assert task_id == "task-own-1"
    assert result == {"value": 49}
    mocked_collect.assert_called_once_with({"task-own-1"}, timeout_sec=30.0)


def test_native_task_pool_map_returns_none_on_failure() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._ready: list[pb2.TaskResult] = []

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                if "0002" in item.task_id:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_FAILED_USER,
                            error=pb2.TaskError(type="UserError", message="boom"),
                        )
                    )
                else:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"value": item.task_id}),
                        )
                    )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-map-none",
    )

    out = session.map([1, 2, 3], arg_name="value", timeout_sec=0.1)
    assert out[0] == {"value": "job-map-none-task-0001"}
    assert out[1] is None
    assert out[2] == {"value": "job-map-none-task-0003"}


def test_native_task_pool_unordered_returns_index_and_result_or_none() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._ready: list[pb2.TaskResult] = []

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                if "0002" in item.task_id:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_FAILED_INFRA,
                            error=pb2.TaskError(type="InfraError", message="node lost"),
                        )
                    )
                else:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"value": item.task_id}),
                        )
                    )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-unordered-batch",
    )

    out = list(session.unordered([{"value": 1}, {"value": 2}, {"value": 3}], timeout_sec=0.1))
    assert sorted(out) == [
        (0, {"value": "job-unordered-batch-task-0001"}),
        (1, None),
        (2, {"value": "job-unordered-batch-task-0003"}),
    ]


def test_native_task_pool_imap_unordered_requeues_after_submit_failure_to_healthy_node() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node = {"node-bad": [], "node-good": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submitted_by_node[self.node_id].extend(item.task_id for item in tasks)
            if self.node_id == "node-bad":
                raise RuntimeError("submit failed on bad node")
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"value": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-bad": _Pool("node-bad"), "node-good": _Pool("node-good")},
        nodes={},
        task_method="run",
        job_id="job-requeue-after-submit-fail",
    )

    with patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-bad")
            if "node-bad" in [candidate.id for candidate in candidates] and "node-bad" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-good")
        ),
    ):
        out = list(session.imap_unordered([{"value": 1}, {"value": 2}], timeout_sec=0.1))

    assert [index for index, _ in out] == [0, 1]
    assert submitted_by_node["node-bad"]
    assert len(submitted_by_node["node-good"]) >= 2
    assert session._submit_breaker_states["node-bad"].consecutive_failures >= 1
    assert "node-bad" in session._scheduler_state.disabled_candidates


def test_native_task_pool_imap_unordered_requeues_when_pool_not_running() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node = {"node-dead": [], "node-good": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submitted_by_node[self.node_id].extend(item.task_id for item in tasks)
            if self.node_id == "node-dead":
                raise RuntimeError("task pool not running")
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"value": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-dead": _Pool("node-dead"), "node-good": _Pool("node-good")},
        nodes={},
        task_method="run",
        job_id="job-pool-not-running-requeue",
    )

    with patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-dead")
            if "node-dead" in [candidate.id for candidate in candidates] and "node-dead" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-good")
        ),
    ):
        out = list(session.imap_unordered([{"value": 1}, {"value": 2}], timeout_sec=0.2))

    assert [index for index, _ in out] == [0, 1]
    assert submitted_by_node["node-dead"]
    assert len(submitted_by_node["node-good"]) >= 2
    assert "node-dead" in session._scheduler_state.disabled_candidates


def test_native_task_pool_imap_unordered_retries_accepted_task_after_pull_lost() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node = {"node-bad": [], "node-good": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submitted_by_node[self.node_id].extend(item.task_id for item in tasks)
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"task_id": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            if self.node_id == "node-bad":
                raise RuntimeError("lost connection to bad node")
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-bad": _Pool("node-bad"), "node-good": _Pool("node-good")},
        nodes={},
        task_method="run",
        job_id="job-retry-after-pull-lost",
    )

    with patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-bad")
            if "node-bad" in [candidate.id for candidate in candidates] and "node-bad" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-good")
        ),
    ):
        out = list(
            session.imap_unordered(
                [{"value": 1}],
                max_in_flight=1,
                timeout_sec=0.2,
                max_infra_retries=1,
            )
        )

    assert out == [(0, {"task_id": submitted_by_node["node-good"][0]})]
    assert len(submitted_by_node["node-bad"]) == 1
    assert len(submitted_by_node["node-good"]) == 1
    assert submitted_by_node["node-bad"][0] != submitted_by_node["node-good"][0]
    assert session.task_retry_success_count == 1
    assert session._pending_task_ids == set()  # noqa: SLF001
    assert "node-bad" in session._scheduler_state.disabled_candidates


def test_native_task_pool_imap_unordered_retries_accepted_task_after_result_timeout() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node = {"node-stalled": [], "node-good": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submitted_by_node[self.node_id].extend(item.task_id for item in tasks)
            if self.node_id == "node-good":
                for item in tasks:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"task_id": item.task_id}),
                        )
                    )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            if self.node_id == "node-stalled":
                return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-stalled": _Pool("node-stalled"), "node-good": _Pool("node-good")},
        nodes={},
        task_method="run",
        job_id="job-retry-after-result-timeout",
    )

    with patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-stalled")
            if "node-stalled" in [candidate.id for candidate in candidates] and "node-stalled" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-good")
        ),
    ):
        out = list(
            session.imap_unordered(
                [{"value": 1}],
                max_in_flight=1,
                receive_batch=1,
                result_timeout_sec=0.05,
                wait_ms=1,
                max_infra_retries=1,
            )
        )

    assert out == [(0, {"task_id": submitted_by_node["node-good"][0]})]
    assert len(submitted_by_node["node-stalled"]) == 1
    assert len(submitted_by_node["node-good"]) == 1
    assert submitted_by_node["node-stalled"][0] != submitted_by_node["node-good"][0]
    assert session.task_retry_success_count == 1
    assert session._pending_task_ids == set()  # noqa: SLF001
    assert "node-stalled" not in session._scheduler_state.disabled_candidates


def test_native_task_pool_imap_unordered_spreads_result_timeout_retries() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node = {"node-stalled": [], "node-good-1": [], "node-good-2": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 4

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submitted_by_node[self.node_id].extend(item.task_id for item in tasks)
            if self.node_id != "node-stalled":
                for item in tasks:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"task_id": item.task_id}),
                        )
                    )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            if self.node_id == "node-stalled":
                return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={
            "node-stalled": _Pool("node-stalled"),
            "node-good-1": _Pool("node-good-1"),
            "node-good-2": _Pool("node-good-2"),
        },
        nodes={},
        task_method="run",
        job_id="job-spread-timeout-retry",
    )

    with patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-stalled")
            if "node-stalled" in [candidate.id for candidate in candidates] and "node-stalled" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-good-1")
        ),
    ):
        out = list(
            session.imap_unordered(
                [{"value": idx} for idx in range(4)],
                max_in_flight=4,
                receive_batch=4,
                result_timeout_sec=0.05,
                wait_ms=1,
                max_infra_retries=1,
            )
        )

    assert len(out) == 4
    assert len(submitted_by_node["node-stalled"]) == 4
    assert len(submitted_by_node["node-good-1"]) == 2
    assert len(submitted_by_node["node-good-2"]) == 2
    assert session.task_retry_success_count == 4
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_imap_unordered_spreads_timeout_retries_when_all_nodes_pending() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node = {"node-1": [], "node-2": []}
    successful_retry_ids: set[str] = set()

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 2

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submitted_before = sum(len(values) for values in submitted_by_node.values())
            submitted_by_node[self.node_id].extend(item.task_id for item in tasks)
            for item in tasks:
                if submitted_before >= 4:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"task_id": item.task_id}),
                        )
                    )
                successful_retry_ids.add(item.task_id)
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool("node-1"), "node-2": _Pool("node-2")},
        nodes={},
        task_method="run",
        job_id="job-spread-all-pending-timeout-retry",
    )

    out = list(
        session.imap_unordered(
            [{"value": idx} for idx in range(4)],
            max_in_flight=4,
            receive_batch=4,
            result_timeout_sec=0.05,
            wait_ms=1,
            max_infra_retries=1,
        )
    )

    assert len(out) == 4
    assert len(submitted_by_node["node-1"]) == 4
    assert len(submitted_by_node["node-2"]) == 4
    assert session.task_retry_success_count == 4
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_imap_unordered_reprepares_payload_for_retry_node() -> None:
    from pycloud_parallel import TaskPool

    prepare_calls: list[tuple[str, dict]] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(
                node_id=node_id,
                fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id},
            )

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"task_id": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            if self.node_id == "node-bad":
                raise RuntimeError("lost connection to bad node")
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    def _fake_prepare(client, payload, **_kwargs):
        prepare_calls.append((client.node_id, dict(payload)))
        return {**dict(payload), "prepared_for": client.node_id}

    session = TaskPool(
        pools={"node-bad": _Pool("node-bad"), "node-good": _Pool("node-good")},
        nodes={},
        task_method="run",
        job_id="job-retry-reprepare",
    )

    with patch("pycloud_parallel.execution.task_pool._prepare_task_payload_for_submit", side_effect=_fake_prepare), patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-bad")
            if "node-bad" in [candidate.id for candidate in candidates] and "node-bad" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-good")
        ),
    ):
        out = list(
            session.imap_unordered(
                [{"value": 1}],
                max_in_flight=1,
                timeout_sec=0.2,
                max_infra_retries=1,
            )
        )

    assert out[0][0] == 0
    assert prepare_calls == [
        ("node-bad", {"value": 1}),
        ("node-good", {"value": 1}),
    ]


def test_native_task_pool_imap_unordered_retries_remote_infra_failure_result() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node = {"node-bad": [], "node-good": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submitted_by_node[self.node_id].extend(item.task_id for item in tasks)
            for item in tasks:
                if self.node_id == "node-bad":
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_FAILED_INFRA,
                            error=pb2.TaskError(type="WorkerLost", message="worker crashed"),
                        )
                    )
                else:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"task_id": item.task_id}),
                        )
                    )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-bad": _Pool("node-bad"), "node-good": _Pool("node-good")},
        nodes={},
        task_method="run",
        job_id="job-retry-remote-infra",
    )

    with patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-bad")
            if "node-bad" in [candidate.id for candidate in candidates] and "node-bad" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-good")
        ),
    ):
        out = list(
            session.imap_unordered(
                [{"value": 1}],
                max_in_flight=1,
                timeout_sec=0.2,
                max_infra_retries=1,
            )
        )

    assert out == [(0, {"task_id": submitted_by_node["node-good"][0]})]
    assert len(submitted_by_node["node-bad"]) == 1
    assert len(submitted_by_node["node-good"]) == 1
    assert session.task_retry_success_count == 1


def test_native_task_pool_imap_unordered_drops_late_result_from_lost_node() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node = {"node-bad": [], "node-good": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._pull_count = 0
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submitted_by_node[self.node_id].extend(item.task_id for item in tasks)
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"task_id": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            if self.node_id == "node-bad":
                self._pull_count += 1
                if self._pull_count == 1:
                    raise RuntimeError("lost connection to bad node")
            if self.node_id == "node-good" and self._pull_count == 0:
                self._pull_count += 1
                return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-bad": _Pool("node-bad"), "node-good": _Pool("node-good")},
        nodes={},
        task_method="run",
        job_id="job-drop-late-result",
    )

    with patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-bad")
            if "node-bad" in [candidate.id for candidate in candidates] and "node-bad" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-good")
        ),
    ):
        out = list(
            session.imap_unordered(
                [{"value": 1}],
                max_in_flight=1,
                timeout_sec=0.2,
                max_infra_retries=1,
            )
        )

    assert out == [(0, {"task_id": submitted_by_node["node-good"][0]})]
    assert submitted_by_node["node-bad"][0] != submitted_by_node["node-good"][0]
    assert session._pools["node-bad"]._pull_count == 1  # noqa: SLF001
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_imap_unordered_retries_lost_accepted_tasks_by_default() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node = {"node-bad": [], "node-good": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 3

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submitted_by_node[self.node_id].extend(item.task_id for item in tasks)
            if self.node_id == "node-good":
                for item in tasks:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"task_id": item.task_id}),
                        )
                    )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            if self.node_id == "node-bad":
                raise RuntimeError("node vanished")
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-bad": _Pool("node-bad"), "node-good": _Pool("node-good")},
        nodes={},
        task_method="run",
        job_id="job-retry-default",
    )

    with patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-bad")
            if "node-bad" in [candidate.id for candidate in candidates] and "node-bad" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-good")
        ),
    ):
        out = list(session.imap_unordered([{"value": idx} for idx in range(3)], max_in_flight=3, timeout_sec=0.2))

    assert sorted(index for index, _result in out) == [0, 1, 2]
    assert len(submitted_by_node["node-bad"]) == 3
    assert len(submitted_by_node["node-good"]) == 3
    assert set(submitted_by_node["node-bad"]).isdisjoint(submitted_by_node["node-good"])
    assert session.task_retry_success_count == 3
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_imap_unordered_reports_callback_progress() -> None:
    from pycloud_parallel import TaskPool

    events = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self) -> None:
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"task_id": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-imap-progress",
    )

    out = list(session.imap_unordered([{"value": 1}, {"value": 2}], max_in_flight=2, timeout_sec=0.1, progress=events.append))

    assert sorted(index for index, _result in out) == [0, 1]
    assert events[0].phase == "running"
    assert events[-1].phase == "done"
    assert events[-1].total == 2
    assert events[-1].completed == 2
    assert events[-1].succeeded == 2


def test_native_task_pool_imap_unordered_does_not_retry_pull_lost_when_disabled() -> None:
    from pycloud_parallel import TaskPool

    submit_count = {"value": 0}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": None)

        def submit_tasks(self, tasks, job_id=""):
            submit_count["value"] += len(tasks)
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            raise RuntimeError("node vanished")

    session = TaskPool(
        pools={"node-bad": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-retry-exhausted",
    )

    out = list(
        session.imap_unordered(
            [{"value": 1}],
            max_in_flight=1,
            timeout_sec=0.2,
            max_infra_retries=0,
            raise_on_error=False,
        )
    )

    assert out == [(0, None)]
    assert submit_count["value"] == 1
    assert session.task_retry_exhausted_count == 1
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_imap_unordered_raises_final_infra_error_when_enabled() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": None)

        def submit_tasks(self, tasks, job_id=""):
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            raise RuntimeError("node vanished")

    session = TaskPool(
        pools={"node-bad": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-retry-exhausted-raise",
    )

    with pytest.raises(RuntimeError, match="node vanished"):
        list(
            session.imap_unordered(
                [{"value": 1}],
                max_in_flight=1,
                timeout_sec=0.2,
                max_infra_retries=0,
                raise_on_error=True,
            )
        )
    assert session.task_retry_exhausted_count == 1
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_imap_unordered_submit_rejected_requeues_without_retry_count() -> None:
    from pycloud_parallel import TaskPool

    submit_count = {"node-a": 0, "node-b": 0}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submit_count[self.node_id] += len(tasks)
            if self.node_id == "node-a":
                return pb2.SubmitTasksResponse(
                    ok=True,
                    accepted=[],
                    rejected=[pb2.TaskRejected(task_id=item.task_id, status=pb2.TASK_STATUS_REJECTED, message="no credit") for item in tasks],
                )
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"task_id": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-a": _Pool("node-a"), "node-b": _Pool("node-b")},
        nodes={},
        task_method="run",
        job_id="job-requeue-submit-rejected",
    )

    with patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-a")
            if "node-a" in [candidate.id for candidate in candidates] and "node-a" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-b")
        ),
    ):
        out = list(session.imap_unordered([{"value": 1}], max_in_flight=1, timeout_sec=0.2))

    assert out == [(0, {"task_id": "job-requeue-submit-rejected-task-0002"})]
    assert submit_count == {"node-a": 1, "node-b": 1}
    assert session.task_retry_count == 0


def test_native_task_pool_imap_unordered_does_not_retry_user_failure() -> None:
    from pycloud_parallel import TaskPool

    submit_count = {"value": 0}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self) -> None:
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": None)

        def submit_tasks(self, tasks, job_id=""):
            submit_count["value"] += len(tasks)
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_FAILED_USER,
                        error=pb2.TaskError(type="UserError", message="boom"),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-no-retry-user-failure",
    )

    out = list(
        session.imap_unordered(
            [{"value": 1}],
            max_in_flight=1,
            timeout_sec=0.2,
            max_infra_retries=2,
            raise_on_error=False,
        )
    )

    assert out == [(0, None)]
    assert submit_count["value"] == 1
    assert session.task_retry_count == 0


def test_native_task_pool_iter_data_marks_non_replay_pending_task_failed_on_pull_lost() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": None)

        def submit_tasks(self, tasks, job_id=""):
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            raise RuntimeError("pull failed")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-submit-pull-lost",
    )

    resp = session.submit_payloads([{"value": 1}])
    task_id = resp.accepted[0].task_id
    out = list(session.iter_data(timeout_sec=0.2, raise_on_error=False))

    assert out == [(task_id, None)]
    assert session.node_lost_failed_tasks == 1
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_collect_items_batch_returns_execution_items_in_input_order() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._ready: list[pb2.TaskResult] = []

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"value": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = list(reversed(self._ready[:limit]))
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-collect-items",
    )

    items = session.collect_items([{"value": 1}, {"value": 2}, {"value": 3}], timeout_sec=0.1)
    assert [item.index for item in items] == [0, 1, 2]
    assert [item.result for item in items] == [
        {"value": "job-collect-items-task-0001"},
        {"value": "job-collect-items-task-0002"},
        {"value": "job-collect-items-task-0003"},
    ]


def test_native_task_pool_session_collect_items_replays_buffered_results_without_deadlock() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-buffered-items",
    )
    session._pending_task_ids = {"task-buffered"}  # noqa: SLF001
    session._buffered_result_items.append(  # noqa: SLF001
        (
            "node-1",
            pb2.TaskResult(
                task_id="task-buffered",
                status=pb2.TASK_STATUS_SUCCEEDED,
                result=dict_to_struct({"value": 1}),
            ),
        )
    )

    items = session.collect_items(timeout_sec=0.1)

    assert len(items) == 1
    assert items[0].task_id == "task-buffered"
    assert items[0].ok is True
    assert items[0].result == {"value": "task-buffered"}


def test_native_task_pool_session_wait_for_results_replays_buffered_results_without_deadlock() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-buffered-results",
    )
    session._pending_task_ids = {"task-buffered"}  # noqa: SLF001
    session._buffered_result_items.append(  # noqa: SLF001
        (
            "node-1",
            pb2.TaskResult(
                task_id="task-buffered",
                status=pb2.TASK_STATUS_SUCCEEDED,
                result=dict_to_struct({"value": 1}),
            ),
        )
    )

    results = session.wait_for_results(expected_count=1, timeout_sec=0.1)

    assert len(results) == 1
    assert results[0].task_id == "task-buffered"


def test_native_task_pool_wait_for_results_reports_callback_progress() -> None:
    from pycloud_parallel import TaskPool

    events = []
    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-wait-progress",
    )
    result = pb2.TaskResult(task_id="task-1", status=pb2.TASK_STATUS_SUCCEEDED)

    with patch.object(session, "iter_results", return_value=iter([result])) as mocked:
        out = session.wait_for_results(expected_count=1, timeout_sec=1.0, progress=events.append)

    assert out == [result]
    mocked.assert_called_once_with(max_count=1, timeout_sec=1.0, limit=100, job_id="", wait_ms=500)
    assert events[0].phase == "running"
    assert events[-1].phase == "done"
    assert events[-1].total == 1
    assert events[-1].completed == 1
    assert events[-1].succeeded == 1


def test_native_task_pool_wait_for_data_reports_failed_progress_before_reraising() -> None:
    from pycloud_parallel import TaskPool

    events = []
    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-wait-data-progress",
    )

    def _iter_data_failure(*args, **kwargs):  # noqa: ARG001
        yield "task-1", {"value": 1}
        raise RuntimeError("boom")

    with patch.object(session, "iter_data", side_effect=_iter_data_failure):
        with pytest.raises(RuntimeError, match="boom"):
            session.wait_for_data(expected_count=2, timeout_sec=1.0, progress=events.append)

    assert events[-1].phase == "failed"
    assert events[-1].completed == 1
    assert events[-1].succeeded == 1
    assert events[-1].failed == 1
    assert events[-1].last_error == "boom"


def test_native_task_pool_async_batch_helpers_exist() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-async-batch",
    )

    with patch.object(session, "map", return_value=[{"value": 1}, None]) as mocked_map, patch.object(
        session,
        "collect_items",
        return_value=[],
    ) as mocked_collect:
        out = asyncio.run(session.amap([1, 2], timeout_sec=0.1))
        collected = asyncio.run(session.acollect_items([{"value": 1}], timeout_sec=0.1))

    assert out == [{"value": 1}, None]
    assert collected == []
    mocked_map.assert_called_once()
    mocked_collect.assert_called_once()
