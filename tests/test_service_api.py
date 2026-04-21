"""Tests for the V1 service-facing API surface."""

import asyncio
import importlib
import io
import sys
import tarfile
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch

import pytest


def _build_service_entry_module(tmp_path, monkeypatch):
    package_name = "demo_service_pkg_entry"
    sys.modules.pop(package_name, None)
    sys.modules.pop(f"{package_name}.worker", None)
    sys.modules.pop(f"{package_name}.helper", None)
    package_dir = tmp_path / package_name
    package_dir.mkdir()
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


def _build_service_entry_module_with_resource(tmp_path, monkeypatch):
    worker_module = _build_service_entry_module(tmp_path, monkeypatch)
    package_dir = tmp_path / worker_module.__package__
    (package_dir / "data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    return worker_module


class TestCallProxy:
    """测试 _CallProxy 类。"""

    def test_repr(self):
        """测试 __repr__ 方法。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        assert "square" in repr(proxy)

    def test_method_property(self):
        """测试 method 属性。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("fibonacci", mock_group)

        assert proxy.method == "fibonacci"

    def test_sync_property(self):
        """测试 sync 属性。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy, _SyncCallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        sync_proxy = proxy.sync

        assert isinstance(sync_proxy, _SyncCallProxy)
        assert sync_proxy._method == "square"

    def test_broadcast_property(self):
        """测试 broadcast 属性。"""
        from pycloud_parallel.execution.call_proxy import _BroadcastProxy, _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        broadcast_proxy = proxy.broadcast

        assert isinstance(broadcast_proxy, _BroadcastProxy)
        assert broadcast_proxy._method == "square"

    def test_with_options(self):
        """测试 with_options 方法。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group, timeout_sec=60.0)

        new_proxy = proxy.with_options(timeout_sec=30.0, strategy="round_robin")

        assert new_proxy._timeout_sec == 30.0
        assert new_proxy._strategy == "round_robin"
        assert new_proxy._method == "square"

    def test_with_options_accepts_service_latency_profile(self):
        """测试 with_options 支持显式 service profile 名称。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group, timeout_sec=60.0)

        new_proxy = proxy.with_options(strategy="service_latency_first")

        assert new_proxy._strategy == "service_latency_first"

    def test_map_delegates_to_group_batch_map(self):
        """测试 map 会委托给 group.map_calls。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        mock_group.map_calls = MagicMock(return_value=[{"value": 1}, {"value": 4}])
        proxy = _CallProxy("square", mock_group)

        result = proxy.map([1, 2], arg_name="x")

        assert result == [{"value": 1}, {"value": 4}]
        mock_group.map_calls.assert_called_once()

    def test_amap_delegates_to_group_async_batch_map(self):
        """测试 amap 会委托给 group.amap_calls。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = AsyncMock()
        mock_group.amap_calls = AsyncMock(return_value=[{"value": 1}, {"value": 4}])
        proxy = _CallProxy("square", mock_group)

        async def _run():
            return await proxy.amap([1, 2], arg_name="x")

        result = asyncio.run(_run())

        assert result == [{"value": 1}, {"value": 4}]
        mock_group.amap_calls.assert_awaited_once()

    def test_unordered_returns_stream_object(self):
        """测试 unordered 返回同步可迭代流对象。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        stream = proxy.unordered([{"x": 1}, {"x": 2}], max_in_flight=2)

        assert hasattr(stream, "__iter__")
        assert not hasattr(stream, "__aiter__")

    def test_aunordered_returns_async_iterable_stream_object(self):
        """测试 aunordered 返回异步可迭代流对象。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        stream = proxy.aunordered([{"x": 1}, {"x": 2}], max_in_flight=2)

        assert hasattr(stream, "__aiter__")
        assert not hasattr(stream, "__iter__")

    def test_iter_items_returns_sync_iterable_stream_object(self):
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        stream = proxy.iter_items([{"x": 1}, {"x": 2}], max_in_flight=2)

        assert hasattr(stream, "__iter__")
        assert not hasattr(stream, "__aiter__")

    def test_aiter_items_returns_async_iterable_stream_object(self):
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        stream = proxy.aiter_items([{"x": 1}, {"x": 2}], max_in_flight=2)

        assert hasattr(stream, "__aiter__")
        assert not hasattr(stream, "__iter__")

    def test_collect_items_delegates_to_group_collect_item_calls(self):
        from pycloud_parallel.execution.base import ExecutionItem
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        mock_group.collect_item_calls = MagicMock(return_value=[ExecutionItem(index=0, ok=True, result={"value": 1})])
        proxy = _CallProxy("square", mock_group)

        result = proxy.collect_items([{"x": 1}])

        assert len(result) == 1
        assert result[0].result == {"value": 1}
        mock_group.collect_item_calls.assert_called_once()

    def test_acollect_items_delegates_to_group_acollect_item_calls(self):
        from pycloud_parallel.execution.base import ExecutionItem
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = AsyncMock()
        mock_group.acollect_item_calls = AsyncMock(return_value=[ExecutionItem(index=0, ok=True, result={"value": 1})])
        proxy = _CallProxy("square", mock_group)

        async def _run():
            return await proxy.acollect_items([{"x": 1}])

        result = asyncio.run(_run())

        assert len(result) == 1
        assert result[0].result == {"value": 1}
        mock_group.acollect_item_calls.assert_awaited_once()

    def test_async_call(self):
        """测试异步调用。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = AsyncMock()
        mock_group.acall_balanced = AsyncMock(return_value=("node1", {"data": {"result": 49}}))
        proxy = _CallProxy("square", mock_group)

        async def test():
            result = await proxy(x=7)
            # resp.get("data", resp) 返回 {"result": 49}
            assert result == {"result": 49}
            mock_group.acall_balanced.assert_called_once_with(
                "square",
                {"x": 7},
                timeout_sec=60.0,
                strategy="predicted_busy",
                refresh_status=True,
            )

        asyncio.run(test())

    def test_await_syntax(self):
        """测试 await 语法。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = AsyncMock()
        mock_group.acall_balanced = AsyncMock(return_value=("node1", {"data": {"y": 100}}))
        proxy = _CallProxy("square", mock_group)

        async def test():
            result = await proxy(x=10)
            assert result == {"y": 100}

        asyncio.run(test())


class TestSyncCallProxy:
    """测试 _SyncCallProxy 类。"""

    def test_repr(self):
        """测试 __repr__ 方法。"""
        from pycloud_parallel.execution.call_proxy import _SyncCallProxy

        mock_group = MagicMock()
        proxy = _SyncCallProxy("square", mock_group)

        assert "square" in repr(proxy)

    def test_sync_call(self):
        """测试同步调用。"""
        from pycloud_parallel.execution.call_proxy import _SyncCallProxy

        mock_group = MagicMock()
        mock_group.call_balanced = MagicMock(return_value=("node1", {"data": {"result": 64}}))
        proxy = _SyncCallProxy("square", mock_group)

        result = proxy(x=8)

        assert result == {"result": 64}
        mock_group.call_balanced.assert_called_once()


class TestBroadcastProxy:
    """测试 _BroadcastProxy 类。"""

    def test_repr(self):
        """测试 __repr__ 方法。"""
        from pycloud_parallel.execution.call_proxy import _BroadcastProxy

        mock_group = MagicMock()
        proxy = _BroadcastProxy("square", mock_group)

        assert "square" in repr(proxy)

    def test_async_broadcast(self):
        """测试异步广播调用。"""
        from pycloud_parallel.execution.call_proxy import _BroadcastProxy

        mock_group = AsyncMock()
        mock_results = [
            ("node1", {"data": {"result": 49}}, None),
            ("node2", {"data": {"result": 49}}, None),
        ]
        mock_group.acall_all = AsyncMock(return_value=mock_results)
        proxy = _BroadcastProxy("square", mock_group)

        async def test():
            results = await proxy(x=7)
            assert len(results) == 2
            assert results[0][1] == {"result": 49}

        asyncio.run(test())


class TestOwnerServiceFacade:
    """测试 OwnerServiceFacade 类。"""

    def test_deploy_from_bytes_defaults_replace_changed_code(self):
        """测试高层入口默认开启同名变更代码替换。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        sentinel = object()
        with patch("pycloud_parallel.execution.service_session.Service.deploy_from_infocenter", return_value=sentinel) as mocked:
            result = OwnerServiceFacade.deploy_from_bytes(
                infocenter_target="127.0.0.1:50051",
                blob=b"def run(**_kwargs):\n    return {'ok': True}\n",
                entry_module="demo_service",
                service_name="demo-service",
            )

        assert result is sentinel
        assert mocked.call_args.kwargs["replace_existing_if_code_changed"] is True

    def test_deploy_from_bytes_can_disable_replace_changed_code(self):
        """测试高层入口允许显式关闭同名变更代码替换。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        sentinel = object()
        with patch("pycloud_parallel.execution.service_session.Service.deploy_from_infocenter", return_value=sentinel) as mocked:
            result = OwnerServiceFacade.deploy_from_bytes(
                infocenter_target="127.0.0.1:50051",
                blob=b"def run(**_kwargs):\n    return {'ok': True}\n",
                entry_module="demo_service",
                service_name="demo-service",
                replace_existing_if_code_changed=False,
            )

        assert result is sentinel
        assert mocked.call_args.kwargs["replace_existing_if_code_changed"] is False

    def test_managed_global_large_value_uses_dataref_upload(self):
        """测试超阈值 managed global 会强制转成 DataRef。"""
        from pycloud_parallel.execution.support import _prepare_managed_global_value_for_upload
        from pycloud_parallel.controlplane.data_ref import DataRef
        from pycloud_parallel.data.ref import DataRef

        ref = DataRef(
            ref_id="sha256:" + "a" * 64,
            storage_id="sha256:" + "a" * 64,
            logical_type="dataframe",
            format="parquet",
            size_bytes=123,
            materialize_as="dataframe",
            locator_kind="node_local",
            locator_token="",
        )
        with patch(
            "pycloud_parallel.execution.support._estimate_managed_global_inline_size",
            return_value=1024,
        ):
            with patch(
                "pycloud_parallel.execution.support._put_data_via_clients",
                return_value=ref,
            ) as mocked:
                prepared = _prepare_managed_global_value_for_upload(
                    [MagicMock()],
                    object(),
                    object_threshold_bytes=128,
                )

        assert isinstance(prepared, DataRef)
        assert prepared.object_id == ref.object_id
        mocked.assert_called_once()

    def test_managed_global_large_value_upload_failure_raises(self):
        """测试超阈值 managed global 上传失败时不能静默回退 inline。"""
        from pycloud_parallel.execution.support import _prepare_managed_global_value_for_upload

        with patch(
            "pycloud_parallel.execution.support._estimate_managed_global_inline_size",
            return_value=1024,
        ):
            with patch(
                "pycloud_parallel.execution.support._put_data_via_clients",
                side_effect=RuntimeError("parquet engine missing"),
            ):
                with pytest.raises(ValueError, match="large-object upload failed"):
                    _prepare_managed_global_value_for_upload(
                        [MagicMock()],
                        object(),
                        object_threshold_bytes=128,
                    )

    def test_service_session_cache_lock_rejects_second_local_owner(self, tmp_path):
        """测试同一个 session cache 文件不能被第二个本地 deploy 进程持有。"""
        from pycloud_parallel.execution.service_session import _ServiceSessionFileLock

        path = tmp_path / "owner" / "svc.json"
        first = _ServiceSessionFileLock(path).acquire()
        try:
            with pytest.raises(RuntimeError, match="already holds cache lock|already active"):
                _ServiceSessionFileLock(path).acquire()
        finally:
            first.close()

        second = _ServiceSessionFileLock(path).acquire()
        second.close()

    def test_getattr_creates_proxy(self):
        """测试 __getattr__ 创建代理。"""
        from pycloud_parallel import Service as OwnerServiceFacade
        from pycloud_parallel.execution.call_proxy import _CallProxy
        from unittest.mock import MagicMock

        # 模拟有方法的 session
        mock_session = MagicMock()
        mock_method_info = MagicMock()
        mock_method_info.method = "square"
        mock_session.list_methods.return_value = [mock_method_info]

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={"node1": mock_session},
            nodes={"node1": MagicMock()},
        )
        group._discovered_methods = None

        proxy = group.square

        assert isinstance(proxy, _CallProxy)
        assert proxy._method == "square"
        assert proxy._strategy == "predicted_busy"

    def test_getattr_with_empty_methods_raises(self):
        """测试当方法列表为空时，访问任何方法都应该报错。"""
        from pycloud_parallel import Service as OwnerServiceFacade
        from unittest.mock import MagicMock

        # 模拟返回空方法列表的 session
        mock_session = MagicMock()
        mock_session.list_methods.return_value = []

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={"node1": mock_session},
            nodes={"node1": MagicMock()},
        )
        group._discovered_methods = None

        # 当列表为空时，访问任何方法都应该报错
        with pytest.raises(AttributeError, match="has no method 'square'"):
            _ = group.square

    def test_getattr_with_discovered_methods(self):
        """测试已发现方法时的 __getattr__。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        group._discovered_methods = ["square", "fibonacci"]

        proxy = group.square
        assert proxy._method == "square"

    def test_getattr_unknown_method_raises(self):
        """测试访问已知列表中不存在的方法时抛出异常。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        # 设置已知的非空方法列表
        group._discovered_methods = ["square", "fibonacci"]

        # 当列表非空且包含已知方法时，访问未知方法应该报错
        with pytest.raises(AttributeError, match="has no method 'unknown'"):
            _ = group.unknown

    def test_getattr_private_raises(self):
        """测试访问私有属性时抛出异常。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )

        with pytest.raises(AttributeError):
            _ = group._private

    def test_methods_property(self):
        """测试 methods 属性。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        group._discovered_methods = ["square", "fibonacci"]

        assert group.methods == ["square", "fibonacci"]

    def test_repr(self):
        """测试 __repr__ 方法。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="compute-service",
            sessions={"node1": MagicMock()},
            nodes={"node1": MagicMock()},
        )
        group._discovered_methods = ["square", "fibonacci"]

        repr_str = repr(group)

        assert "compute-service" in repr_str
        assert "square" in repr_str

    def test_repr_not_discovered(self):
        """测试未发现方法时的 __repr__。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="compute-service",
            sessions={},
            nodes={},
        )
        group._discovered_methods = None

        repr_str = repr(group)

        assert "compute-service" in repr_str

    def test_async_call_interface(self):
        """测试异步 call 接口。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        group.acall_balanced = AsyncMock(return_value=("node1", {"data": {"result": 100}}))

        async def test():
            result = await group.call("square", x=10)
            assert result == {"result": 100}

        asyncio.run(test())

    def test_sync_call_interface(self):
        """测试同步 call_sync 接口。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        group.call_balanced = MagicMock(return_value=("node1", {"data": {"result": 100}}))

        result = group.call_sync("square", x=10)
        assert result == {"result": 100}

    def test_deploy_from_infocenter_emits_message_when_no_nodes(self, capsys):
        from pycloud_parallel.execution.service_session import Service

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), []),
        ):
            with pytest.raises(RuntimeError, match="no available nodes from InfoCenter"):
                Service.deploy_from_infocenter(
                    infocenter_target="127.0.0.1:50051",
                    owner_client_id="owner-demo",
                    service_name="demo-service",
                    blob=b"def run(**_kwargs):\n    return {'ok': True}\n",
                    entry_module="demo_service",
                    entry_callable="run",
                )

        err = capsys.readouterr().err
        assert "[Service] deploy start" in err
        assert "[Service] deploy failed: no available nodes" in err
        assert "127.0.0.1:50051" in err

    def test_deploy_from_infocenter_emits_success_message(self, tmp_path, capsys):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        fake_node = SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **_kwargs):
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [fake_node]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-service",
                blob=b"def run(**_kwargs):\n    return {'ok': True}\n",
                entry_module="demo_service",
                entry_callable="run",
                session_cache_dir=str(tmp_path),
            )

        err = capsys.readouterr().err
        assert "[Service] deploy start" in err
        assert "[Service] deploy success service_name=demo-service nodes=['node-1']" in err
        for client in group._clients.values():  # noqa: SLF001
            client.close()

    def test_deploy_from_infocenter_retries_briefly_until_nodes_register(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        fake_node = SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        discovery_calls = {"count": 0}

        class _FakeInfoCenter:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

            def list_service_routes(self, **_kwargs):
                return []

            def list_nodes(self, **_kwargs):
                discovery_calls["count"] += 1
                if discovery_calls["count"] == 1:
                    return []
                return [fake_node]

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **_kwargs):
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._infocenter_client",
            return_value=_FakeInfoCenter(),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ), patch(
            "pycloud_parallel.execution.service_session.time.sleep",
            return_value=None,
        ) as mocked_sleep:
            group = Service.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-retry-service",
                blob=b"def run(**_kwargs):\n    return {'ok': True}\n",
                entry_module="demo_service",
                entry_callable="run",
                timeout_sec=1.0,
                session_cache_dir=str(tmp_path),
            )

        try:
            assert discovery_calls["count"] == 2
            mocked_sleep.assert_called()
            assert list(group.sessions.keys()) == ["node-1"]
        finally:
            for client in group._clients.values():  # noqa: SLF001
                client.close()

    def test_deploy_from_infocenter_packages_module_object_entry_module(self, tmp_path, monkeypatch):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        worker_module = _build_service_entry_module(tmp_path, monkeypatch)
        fake_node = SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                create_calls.append(dict(kwargs))
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [fake_node]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-module-service",
                entry_module=worker_module,
                entry_callable="run",
                session_cache_dir=str(tmp_path),
            )

        try:
            assert len(create_calls) == 1
            create_call = create_calls[0]
            assert create_call["entry_module"] == worker_module.__name__
            assert create_call["package_format"] == "tar.gz"
            with tarfile.open(fileobj=io.BytesIO(create_call["blob"]), mode="r:gz") as tar:
                names = set(tar.getnames())
            assert f"{worker_module.__package__}/__init__.py" in names
            assert f"{worker_module.__package__}/worker.py" in names
            assert f"{worker_module.__package__}/helper.py" in names
            assert f"{worker_module.__package__}/ignored.csv" not in names
        finally:
            for client in group._clients.values():  # noqa: SLF001
                client.close()

    def test_deploy_from_infocenter_includes_only_explicit_resource_paths(self, tmp_path, monkeypatch):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        worker_module = _build_service_entry_module_with_resource(tmp_path, monkeypatch)
        fake_node = SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                create_calls.append(dict(kwargs))
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [fake_node]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-module-service-resource",
                source=worker_module,
                resource_paths=["data.csv"],
                session_cache_dir=str(tmp_path),
            )

        try:
            create_call = create_calls[0]
            with tarfile.open(fileobj=io.BytesIO(create_call["blob"]), mode="r:gz") as tar:
                names = set(tar.getnames())
            assert f"{worker_module.__package__}/data.csv" in names
        finally:
            for client in group._clients.values():  # noqa: SLF001
                client.close()

    def test_deploy_from_infocenter_packages_callable_object_entry_callable(self, tmp_path, monkeypatch):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        worker_module = _build_service_entry_module(tmp_path, monkeypatch)
        fake_node = SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                create_calls.append(dict(kwargs))
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [fake_node]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-callable-service",
                entry_callable=worker_module.run,
                session_cache_dir=str(tmp_path),
            )

        try:
            assert len(create_calls) == 1
            create_call = create_calls[0]
            assert create_call["entry_module"] == worker_module.__name__
            assert create_call["entry_callable"] == "run"
            assert create_call["package_format"] == "tar.gz"
            with tarfile.open(fileobj=io.BytesIO(create_call["blob"]), mode="r:gz") as tar:
                names = set(tar.getnames())
            assert f"{worker_module.__package__}/__init__.py" in names
            assert f"{worker_module.__package__}/worker.py" in names
            assert f"{worker_module.__package__}/helper.py" in names
            assert f"{worker_module.__package__}/ignored.csv" not in names
        finally:
            for client in group._clients.values():  # noqa: SLF001
                client.close()

    def test_join_emits_failure_summary(self, capsys):
        from pycloud_parallel.execution.service_session import Service

        failed_session = SimpleNamespace(
            failed=True,
            last_error="RuntimeError('heartbeat unavailable')",
            _hb_lock=threading.Lock(),
            _hb_thread=None,
        )
        group = Service(
            owner_client_id="owner-demo",
            service_name="demo-service",
            sessions={"node-1": failed_session},
            nodes={},
        )

        group.join(poll_interval_sec=0.01)
        err = capsys.readouterr().err
        assert "[Service] owner keepalive stopped service_name=demo-service" in err
        assert "node-1" in err

    def test_service_group_update_globals_prepares_values_once_for_all_nodes(self):
        from pycloud_parallel.execution.service_session import Service

        session_a = SimpleNamespace(failed=False, last_error="")
        session_a.update_globals_prepared = MagicMock(return_value=SimpleNamespace(globals_digest="sha256:same"))
        session_b = SimpleNamespace(failed=False, last_error="")
        session_b.update_globals_prepared = MagicMock(return_value=SimpleNamespace(globals_digest="sha256:same"))

        client_a = MagicMock()
        client_b = MagicMock()
        group = Service(
            owner_client_id="owner-demo",
            service_name="svc-demo",
            sessions={"node-a": session_a, "node-b": session_b},
            nodes={},
            _clients={"node-a": client_a, "node-b": client_b},
        )

        with patch(
            "pycloud_parallel.execution.service_session._prepare_managed_globals_values_for_upload",
            return_value={"cfg": {"k": "v"}},
        ) as mocked_prepare:
            digest = group.update_globals({"cfg": {"k": "v"}})

        assert digest == "sha256:same"
        assert group.globals_digests == {"node-a": "sha256:same", "node-b": "sha256:same"}
        mocked_prepare.assert_called_once()
        prepare_args, prepare_kwargs = mocked_prepare.call_args
        assert prepare_args == ([client_a, client_b], {"cfg": {"k": "v"}})
        assert prepare_kwargs["effective_policy"] == group.effective_policy
        session_a.update_globals_prepared.assert_called_once_with({"cfg": {"k": "v"}})
        session_b.update_globals_prepared.assert_called_once_with({"cfg": {"k": "v"}})

    def test_service_group_update_globals_prunes_failed_nodes(self):
        from pycloud_parallel.execution.service_session import Service

        session_a = SimpleNamespace(failed=False, last_error="")
        session_a.update_globals_prepared = MagicMock(return_value=SimpleNamespace(globals_digest="sha256:same"))
        session_b = SimpleNamespace(failed=False, last_error="")
        session_b.update_globals_prepared = MagicMock(side_effect=RuntimeError("node-b unavailable"))

        client_a = MagicMock()
        client_b = MagicMock()
        group = Service(
            owner_client_id="owner-demo",
            service_name="svc-demo",
            sessions={"node-a": session_a, "node-b": session_b},
            nodes={"node-a": MagicMock(), "node-b": MagicMock()},
            _clients={"node-a": client_a, "node-b": client_b},
        )

        with patch(
            "pycloud_parallel.execution.service_session._prepare_managed_globals_values_for_upload",
            return_value={"cfg": {"k": "v"}},
        ):
            digest = group.update_globals({"cfg": {"k": "v"}})

        assert digest == "sha256:same"
        assert set(group.sessions.keys()) == {"node-a"}
        assert set(group._clients.keys()) == {"node-a"}  # noqa: SLF001
        assert "node-b" in group.failures
        assert group.globals_digests == {"node-a": "sha256:same"}
        client_b.close.assert_called_once()

    def test_service_group_update_globals_allows_per_node_digests(self):
        from pycloud_parallel.execution.service_session import Service

        session_a = SimpleNamespace(failed=False, last_error="")
        session_a.update_globals_prepared = MagicMock(return_value=SimpleNamespace(globals_digest="sha256:a"))
        session_b = SimpleNamespace(failed=False, last_error="")
        session_b.update_globals_prepared = MagicMock(return_value=SimpleNamespace(globals_digest="sha256:b"))
        client_a = MagicMock()
        client_b = MagicMock()
        group = Service(
            owner_client_id="owner-demo",
            service_name="svc-demo",
            sessions={"node-a": session_a, "node-b": session_b},
            nodes={},
            _clients={"node-a": client_a, "node-b": client_b},
        )

        with patch(
            "pycloud_parallel.execution.service_session._prepare_managed_globals_values_for_upload",
            return_value={"cfg": {"k": "v"}},
        ):
            digest = group.update_globals({"cfg": {"k": "v"}})

        assert digest in {"sha256:a", "sha256:b"}
        assert group.globals_digests == {"node-a": "sha256:a", "node-b": "sha256:b"}

    def test_service_group_update_globals_fails_when_all_nodes_fail(self):
        from pycloud_parallel.execution.service_session import Service

        session_a = SimpleNamespace(failed=False, last_error="")
        session_a.update_globals_prepared = MagicMock(side_effect=RuntimeError("node-a unavailable"))
        client_a = MagicMock()
        group = Service(
            owner_client_id="owner-demo",
            service_name="svc-demo",
            sessions={"node-a": session_a},
            nodes={"node-a": MagicMock()},
            _clients={"node-a": client_a},
        )

        with patch(
            "pycloud_parallel.execution.service_session._prepare_managed_globals_values_for_upload",
            return_value={"cfg": {"k": "v"}},
        ):
            with pytest.raises(RuntimeError, match="update_globals failed on all nodes"):
                group.update_globals({"cfg": {"k": "v"}})

    def test_deploy_from_infocenter_clamps_worker_count_per_node_capacity(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        node_a = SimpleNamespace(
            node_id="node-a",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=10,
            capacity=10,
            queued=0,
            python_version="py3.11",
        )
        node_b = SimpleNamespace(
            node_id="node-b",
            control_addr="127.0.0.1:50062",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=7,
            capacity=7,
            queued=0,
            python_version="py3.11",
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                create_calls.append((self.target, dict(kwargs)))
                return SimpleNamespace(
                    service_id=f"svc-{self.target.rsplit(':', 1)[-1]}",
                    service_token="token",
                    http_base_url=f"http://{self.target}/svc/demo",
                    heartbeat_timeout_sec=30,
                    worker_count=int(kwargs["worker_count"]),
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [node_a, node_b]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="svc-clamp",
                blob=b"def run(**_kwargs):\n    return {'ok': True}\n",
                entry_module="svc_clamp",
                entry_callable="run",
                worker_count=8,
                node_count=2,
                min_success_nodes=2,
                allow_partial=False,
                session_cache_dir=str(tmp_path),
            )

        try:
            assert len(create_calls) == 2
            call_map = {target: kwargs for target, kwargs in create_calls}
            assert call_map["127.0.0.1:50061"]["worker_count"] == 8
            assert call_map["127.0.0.1:50062"]["worker_count"] == 7
        finally:
            for client in group._clients.values():  # noqa: SLF001
                client.close()

    def test_deploy_from_infocenter_ignores_inspected_stopped_routes(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        fake_node = SimpleNamespace(
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        fake_route = SimpleNamespace(
            service_name="demo-stopped-service",
            service_id="svc-old",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            http_base_url="http://127.0.0.1:18081/svc/svc-old",
        )
        stopped_info = SimpleNamespace(
            owner_client_id="owner-demo",
            code_version="sha256:old",
            status=pb2.SERVICE_STATUS_STOPPED,
            service_name="demo-stopped-service",
            http_base_url=fake_route.http_base_url,
            worker_count=1,
            created_at=None,
            last_heartbeat_at=None,
            lease_expire_at=None,
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                create_calls.append(dict(kwargs))
                return SimpleNamespace(
                    service_id="svc-new",
                    service_token="token-new",
                    http_base_url="http://127.0.0.1:18081/svc/svc-new",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=([fake_route], [fake_node]),
        ), patch.object(
            Service,
            "_inspect_existing_routes",
            return_value=[(fake_route, stopped_info)],
        ), patch(
            "pycloud_parallel.execution.service_session._node_control_client",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-stopped-service",
                blob=b"def run(**_kwargs):\n    return {'ok': True}\n",
                runtime="py3",
                entry_module="demo_service",
                entry_callable="run",
                session_cache_dir=str(tmp_path),
            )

        try:
            assert len(create_calls) == 1
            assert create_calls[0]["service_name"] == "demo-stopped-service"
        finally:
            group.close(end_services=False)

    def test_deploy_from_infocenter_redeploys_when_reuse_heartbeat_hits_stopped_service(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.execution.support import _artifact_code_version
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        effective_code_version = _artifact_code_version(
            blob=blob,
            runtime="py3",
            entry_module="demo_service",
            entry_callable="run",
            package_format="py",
            export_mode="decorator",
        )
        fake_node = SimpleNamespace(
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        fake_route = SimpleNamespace(
            service_name="demo-race-service",
            service_id="svc-old",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            http_base_url="http://127.0.0.1:18081/svc/svc-old",
        )
        running_info = SimpleNamespace(
            owner_client_id="owner-demo",
            code_version=effective_code_version,
            status=pb2.SERVICE_STATUS_RUNNING,
            service_name="demo-race-service",
            http_base_url=fake_route.http_base_url,
            worker_count=1,
            created_at=None,
            last_heartbeat_at=None,
            lease_expire_at=None,
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def heartbeat_service(self, **kwargs):
                del kwargs
                raise RuntimeError("service is stopped")

            def create_service_from_bytes(self, **kwargs):
                create_calls.append(dict(kwargs))
                return SimpleNamespace(
                    service_id="svc-new",
                    service_token="token-new",
                    http_base_url="http://127.0.0.1:18081/svc/svc-new",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=([fake_route], [fake_node]),
        ), patch.object(
            Service,
            "_inspect_existing_routes",
            return_value=[(fake_route, running_info)],
        ), patch(
            "pycloud_parallel.execution.service_session._node_control_client",
            _FakeNodeControlClient,
        ), patch(
            "pycloud_parallel.execution.service_session._load_service_session_cache",
            return_value={
                "artifact_code_version": effective_code_version,
                "nodes": {
                    "node-1-inst": {
                        "service_id": "svc-old",
                        "service_token": "token-old",
                        "http_base_url": fake_route.http_base_url,
                        "worker_count": 1,
                        "heartbeat_timeout_sec": 30,
                    }
                },
            },
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-race-service",
                blob=blob,
                runtime="py3",
                entry_module="demo_service",
                entry_callable="run",
                session_cache_dir=str(tmp_path),
            )

        try:
            assert len(create_calls) == 1
            assert create_calls[0]["service_name"] == "demo-race-service"
        finally:
            group.close(end_services=False)

    def test_async_call_all_interface(self):
        """测试异步 call_all 接口。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        mock_results = [("node1", {"result": 49}, None)]
        group.acall_all = AsyncMock(return_value=mock_results)

        async def test():
            results = await group.call_all("square", x=7)
            assert len(results) == 1
            assert results[0][1] == {"result": 49}

        asyncio.run(test())


class TestIntegration:
    """集成测试，测试完整的调用流程。"""

    def test_full_async_flow(self):
        """测试完整的异步调用流程。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        # 模拟 session
        mock_session = MagicMock()
        mock_method_info = MagicMock()
        mock_method_info.method = "square"
        mock_session.list_methods.return_value = [mock_method_info]

        # 模拟 group
        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="compute-service",
            sessions={"node1": mock_session, "node2": MagicMock()},
            nodes={"node1": MagicMock(), "node2": MagicMock()},
        )

        # 模拟 acall_balanced
        async def mock_acall(method, payload, **kwargs):
            if method == "square":
                x = payload.get("x", 0)
                return ("node1", {"data": {"x": x, "y": x * x}})
            raise ValueError(f"Unknown method: {method}")

        group.acall_balanced = mock_acall

        async def run_test():
            # 调用远程方法，就像本地函数一样
            result1 = await group.square(x=7)
            assert result1 == {"x": 7, "y": 49}

            result2 = await group.square(x=10)
            assert result2 == {"x": 10, "y": 100}

        asyncio.run(run_test())

    def test_full_sync_flow(self):
        """测试完整的同步调用流程。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        # 模拟 session
        mock_session = MagicMock()
        mock_method_info = MagicMock()
        mock_method_info.method = "square"
        mock_session.list_methods.return_value = [mock_method_info]

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="compute-service",
            sessions={"node1": mock_session},
            nodes={"node1": MagicMock()},
        )

        def mock_call(method, payload, **kwargs):
            if method == "square":
                x = payload.get("x", 0)
                return ("node1", {"data": {"x": x, "y": x * x}})
            raise ValueError(f"Unknown method: {method}")

        group.call_balanced = mock_call

        # 同步调用
        result = group.square.sync(x=5)
        assert result == {"x": 5, "y": 25}

    def test_full_broadcast_flow(self):
        """测试完整的广播调用流程。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        # 模拟 session
        mock_session = MagicMock()
        mock_method_info = MagicMock()
        mock_method_info.method = "square"
        mock_session.list_methods.return_value = [mock_method_info]

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="compute-service",
            sessions={"node1": mock_session, "node2": MagicMock()},
            nodes={"node1": MagicMock(), "node2": MagicMock()},
        )

        async def mock_acall_all(method, payload, **kwargs):
            return [
                ("node1", {"data": {"x": 7, "y": 49}}, None),
                ("node2", {"data": {"x": 7, "y": 49}}, None),
            ]

        group.acall_all = mock_acall_all

        async def run_test():
            results = await group.square.broadcast(x=7)

            assert len(results) == 2
            for node_id, result, error in results:
                assert error is None
                assert result == {"x": 7, "y": 49}

        asyncio.run(run_test())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
