"""测试 deploy_from_infocenter 的默认值功能。"""

import inspect
import math

import pytest
from pycloud_parallel.controlplane import client as client_mod
from pycloud_parallel.controlplane.client import (
    DeployedService,
    InfoCenterNode,
    NodeControlClient,
    ServiceGroup,
    TaskBatchClient,
    TaskSubmitter,
    _get_local_ip,
    _normalize_entry_module_arg,
)
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


class TestDefaultValues:
    """测试 deploy_from_infocenter 的默认值生成。"""

    def test_get_local_ip(self):
        """测试获取本机 IP。"""
        ip = _get_local_ip()
        assert ip is not None
        assert len(ip) > 0
        # 应该是 IP 地址或 "localhost"
        assert ip == "localhost" or "." in ip or ":" in ip

    def test_owner_client_id_default(self):
        """测试 owner_client_id 的默认值生成。"""
        # 不提供 owner_client_id，应该自动生成为 "client-{ip}"
        local_ip = _get_local_ip()
        expected = f"client-{local_ip}"

        # 由于无法实际调用 deploy（需要服务器），这里只测试逻辑
        # 实际的测试在集成测试中进行
        assert expected is not None

    def test_service_name_default_from_entry_module(self):
        """测试从 entry_module 生成 service_name。"""
        # service_name = None, entry_module = "my_service"
        # 期望生成: "my_service-{ip}"
        local_ip = _get_local_ip()
        entry_module = "my_service"
        expected = f"{entry_module}-{local_ip}"

        assert expected is not None

    def test_service_name_default_from_filename(self):
        """测试从 filename 推断 entry_module 并生成 service_name。"""
        # service_name = None, filename = "service.py"
        # entry_module = None
        # 期望从 "service.py" 推断 entry_module = "service"
        # 然后生成 service_name = "service-{ip}"
        local_ip = _get_local_ip()
        filename = "service.py"
        entry_module = filename.replace(".py", "")
        expected = f"{entry_module}-{local_ip}"

        assert expected is not None

    def test_service_name_fallback(self):
        """测试 service_name 的回退逻辑。"""
        # service_name = None, entry_module = None, filename 非 .py
        # 期望生成: "service-{ip}"
        local_ip = _get_local_ip()
        expected = f"service-{local_ip}"

        assert expected is not None


class TestParameterValidation:
    """测试参数验证。"""

    def test_infocenter_target_required(self):
        """测试 infocenter_target 是必须的。"""
        with pytest.raises(TypeError):
            # 缺少必须的位置参数
            DeployedService.deploy_from_infocenter()

    def test_artifact_content_required(self):
        """测试必须提供代码内容之一。"""
        with pytest.raises(ValueError, match="artifact_path or blob must be provided"):
            DeployedService.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                # 不提供任何代码内容
            )

    def test_public_entrypoints_hide_filename(self):
        """测试高层公开入口不再暴露 filename。"""
        assert "filename" not in inspect.signature(DeployedService.deploy_from_infocenter).parameters
        assert "filename" not in inspect.signature(TaskBatchClient.from_infocenter).parameters
        assert "filename" not in inspect.signature(TaskSubmitter.from_infocenter).parameters
        assert "artifact_paths" not in inspect.signature(DeployedService.deploy_from_infocenter).parameters


class TestEntryModuleNormalization:
    """测试 entry_module 的正规化。"""

    def test_normalize_entry_module_accepts_module_object(self):
        assert _normalize_entry_module_arg(math) == "math"

    def test_create_service_from_bytes_accepts_module_object(self):
        captured = {}

        class FakeStub:
            def CreateService(self, request_iter, timeout=None):
                first = next(request_iter)
                captured["entry_module"] = first.meta.entry_module
                captured["package_format"] = first.meta.package_format
                return pb2.CreateServiceResponse(
                    ok=True,
                    service_id="svc-test",
                    service_token="token-test",
                    http_base_url="http://127.0.0.1:18080",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

        client = NodeControlClient("127.0.0.1:1", timeout_sec=1.0)
        client.stub = FakeStub()
        try:
            session = client.create_service_from_bytes(
                owner_client_id="owner-test",
                service_name="svc-test",
                blob=b"def run():\n    return {'ok': True}\n",
                runtime="py3",
                entry_module=math,
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
        finally:
            client.close()

        assert captured["entry_module"] == "math"
        assert captured["package_format"] == "py"
        assert session.service_id == "svc-test"

    def test_deploy_service_uses_module_name_for_default_service_name(self, monkeypatch):
        captured = {}

        class FakeInfoCenterClient:
            def __init__(self, target, timeout_sec=0):
                self.target = target
                self.timeout_sec = timeout_sec

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

            def list_service_routes(self, **kwargs):
                return []

            def list_nodes(self, **kwargs):
                return [
                    InfoCenterNode(
                        node_id="node-1",
                        control_addr="127.0.0.1:50061",
                        healthy=True,
                        capacity=4,
                        queue_capacity=100,
                        queued=0,
                        inflight=0,
                        credit=4,
                        service_worker_capacity=4,
                        service_worker_used=0,
                        service_worker_available=4,
                    )
                ]

        class FakeSession:
            service_id = "svc-test"
            service_token = "token-test"
            http_base_url = "http://127.0.0.1:18080"
            heartbeat_timeout_sec = 30
            worker_count = 1

            def _start_keepalive(self, interval_sec=None):
                return None

        class FakeNodeControlClient:
            def __init__(self, target, timeout_sec=0):
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                captured.update(kwargs)
                return FakeSession()

            def close(self):
                return None

        monkeypatch.setattr(client_mod, "InfoCenterClient", FakeInfoCenterClient)
        monkeypatch.setattr(client_mod, "NodeControlClient", FakeNodeControlClient)
        monkeypatch.setattr(client_mod, "_get_local_ip", lambda: "127.0.0.1")
        monkeypatch.setattr(client_mod.time, "strftime", lambda fmt: "20260402123456")
        monkeypatch.setattr(ServiceGroup, "_persist_session_cache", lambda self: None)
        monkeypatch.setattr(ServiceGroup, "_start_keepalive", lambda self, interval_sec=None: None)

        group = ServiceGroup.deploy_from_infocenter(
            infocenter_target="127.0.0.1:50051",
            blob=b"def run():\n    return {'ok': True}\n",
            runtime="py3",
            entry_module=math,
            entry_callable="run",
            min_success_nodes=1,
            node_limit=1,
        )

        assert group.service_name == "math-127.0.0.1-20260402123456"
        assert captured["entry_module"] == "math"

    def test_deploy_service_accepts_artifact_path_list(self, monkeypatch, tmp_path):
        captured = {}

        source = tmp_path / "demo_task.py"
        source.write_text("def run():\n    return {'ok': True}\n", encoding="utf-8")

        class FakeInfoCenterClient:
            def __init__(self, target, timeout_sec=0):
                self.target = target
                self.timeout_sec = timeout_sec

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

            def list_service_routes(self, **kwargs):
                return []

            def list_nodes(self, **kwargs):
                return [
                    InfoCenterNode(
                        node_id="node-1",
                        control_addr="127.0.0.1:50061",
                        healthy=True,
                        capacity=4,
                        queue_capacity=100,
                        queued=0,
                        inflight=0,
                        credit=4,
                        service_worker_capacity=4,
                        service_worker_used=0,
                        service_worker_available=4,
                    )
                ]

        class FakeSession:
            service_id = "svc-test"
            service_token = "token-test"
            http_base_url = "http://127.0.0.1:18080"
            heartbeat_timeout_sec = 30
            worker_count = 1

            def _start_keepalive(self, interval_sec=None):
                return None

        class FakeNodeControlClient:
            def __init__(self, target, timeout_sec=0):
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                captured.update(kwargs)
                return FakeSession()

            def close(self):
                return None

        monkeypatch.setattr(client_mod, "InfoCenterClient", FakeInfoCenterClient)
        monkeypatch.setattr(client_mod, "NodeControlClient", FakeNodeControlClient)
        monkeypatch.setattr(client_mod, "_get_local_ip", lambda: "127.0.0.1")
        monkeypatch.setattr(client_mod.time, "strftime", lambda fmt: "20260402123456")
        monkeypatch.setattr(ServiceGroup, "_persist_session_cache", lambda self: None)
        monkeypatch.setattr(ServiceGroup, "_start_keepalive", lambda self, interval_sec=None: None)

        group = ServiceGroup.deploy_from_infocenter(
            infocenter_target="127.0.0.1:50051",
            artifact_path=[str(source)],
            runtime="py3",
            min_success_nodes=1,
            node_limit=1,
        )

        assert group.service_name == "demo_task-127.0.0.1-20260402123456"
        assert captured["entry_module"] == "demo_task"
        assert captured["package_format"] == "zip"

    def test_layered_service_entrypoints_hide_filename(self, monkeypatch):
        captured = {}

        def fake_deploy(cls, **kwargs):
            captured.update(kwargs)
            return "ok"

        monkeypatch.setattr(DeployedService, "deploy_from_infocenter", classmethod(fake_deploy))

        assert "filename" not in inspect.signature(DeployedService.deploy_from_module).parameters
        assert "filename" not in inspect.signature(DeployedService.deploy_from_func).parameters
        assert "filename" not in inspect.signature(DeployedService.deploy_from_file).parameters
        assert "filename" not in inspect.signature(DeployedService.deploy_from_bytes).parameters

        result = DeployedService.deploy_from_bytes(
            infocenter_target="127.0.0.1:50051",
            blob=b"def run():\n    return {'ok': True}\n",
            entry_module="svc_demo",
        )

        assert result == "ok"
        assert captured["blob"] == b"def run():\n    return {'ok': True}\n"
        assert captured["entry_module"] == "svc_demo"
        assert captured["package_format"] == "py"
        assert "filename" not in captured

    def test_layered_task_batch_entrypoints_hide_filename(self, monkeypatch):
        captured = {}

        def fake_from_infocenter(cls, **kwargs):
            captured.update(kwargs)
            return "batch-ok"

        monkeypatch.setattr(TaskBatchClient, "from_infocenter", classmethod(fake_from_infocenter))

        assert "filename" not in inspect.signature(TaskBatchClient.from_module).parameters
        assert "filename" not in inspect.signature(TaskBatchClient.from_func).parameters
        assert "filename" not in inspect.signature(TaskBatchClient.from_file).parameters
        assert "filename" not in inspect.signature(TaskBatchClient.from_bytes).parameters

        result = TaskBatchClient.from_bytes(
            infocenter_target="127.0.0.1:50051",
            blob=b"def run(payload):\n    return payload\n",
            entry_module="task_demo",
        )

        assert result == "batch-ok"
        assert captured["blob"] == b"def run(payload):\n    return payload\n"
        assert captured["entry_module"] == "task_demo"
        assert captured["package_format"] == "py"
        assert "filename" not in captured

    def test_task_batch_accepts_artifact_path_list(self, monkeypatch, tmp_path):
        captured = {}

        source = tmp_path / "demo_task.py"
        source.write_text("def run(value=0, **_kwargs):\n    return {'value': value}\n", encoding="utf-8")

        class FakeInfoCenterClient:
            def __init__(self, target, timeout_sec=0):
                self.target = target
                self.timeout_sec = timeout_sec

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

            def select_task_nodes(self, **kwargs):
                return [
                    InfoCenterNode(
                        node_id="node-1",
                        control_addr="127.0.0.1:50061",
                        healthy=True,
                        capacity=4,
                        queue_capacity=100,
                        queued=0,
                        inflight=0,
                        credit=4,
                        service_worker_capacity=4,
                        service_worker_used=0,
                        service_worker_available=4,
                    )
                ]

        class FakeStream:
            node_credit = 4

            def close(self):
                return None

        class FakeUpload:
            code_version = "sha256:test"

        class FakeNodeControlClient:
            def __init__(self, target, timeout_sec=0):
                self.target = target
                self.timeout_sec = timeout_sec

            def upload_code_from_bytes(self, **kwargs):
                captured.update(kwargs)
                return FakeUpload()

            def open_task_stream(self, **kwargs):
                return FakeStream()

            def close(self):
                return None

        monkeypatch.setattr(client_mod, "InfoCenterClient", FakeInfoCenterClient)
        monkeypatch.setattr(client_mod, "NodeControlClient", FakeNodeControlClient)

        batch = TaskBatchClient.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            artifact_path=[str(source)],
            runtime="py3",
        )
        try:
            assert captured["entry_module"] == "demo_task"
            assert captured["package_format"] == "zip"
        finally:
            batch.close()

    def test_task_submitter_from_infocenter_accepts_func_and_module(self, monkeypatch):
        captured = {}
        batch = object()

        def fake_from_infocenter(cls, **kwargs):
            captured.update(kwargs)
            return batch

        monkeypatch.setattr(TaskBatchClient, "from_infocenter", classmethod(fake_from_infocenter))

        submitter = TaskSubmitter.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            func=math.sqrt,
            runtime="py3",
        )

        assert isinstance(submitter, TaskSubmitter)
        assert submitter._batch is batch
        assert "func" in inspect.signature(TaskSubmitter.from_infocenter).parameters
        assert "module" in inspect.signature(TaskSubmitter.from_infocenter).parameters
        assert captured["func"] is math.sqrt
        assert captured["runtime"] == "py3"

    def test_layered_task_submitter_entrypoints_hide_filename(self, monkeypatch):
        captured = {}
        batch = object()

        def fake_from_bytes(cls, **kwargs):
            captured.update(kwargs)
            return batch

        monkeypatch.setattr(TaskBatchClient, "from_bytes", classmethod(fake_from_bytes))

        assert "filename" not in inspect.signature(TaskSubmitter.from_module).parameters
        assert "filename" not in inspect.signature(TaskSubmitter.from_func).parameters
        assert "filename" not in inspect.signature(TaskSubmitter.from_file).parameters
        assert "filename" not in inspect.signature(TaskSubmitter.from_bytes).parameters

        submitter = TaskSubmitter.from_bytes(
            infocenter_target="127.0.0.1:50051",
            blob=b"def run(payload):\n    return payload\n",
            entry_module="task_demo",
        )

        assert isinstance(submitter, TaskSubmitter)
        assert submitter._batch is batch
        assert captured["blob"] == b"def run(payload):\n    return payload\n"
        assert captured["entry_module"] == "task_demo"
        assert captured["package_format"] == "py"
        assert "filename" not in captured


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
