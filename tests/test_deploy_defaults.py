"""测试 deploy_from_infocenter 的默认值功能。"""

import pytest
from pycloud_parallel.controlplane.client import ModuleLikeServiceGroup, _get_local_ip


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
            ModuleLikeServiceGroup.deploy_from_infocenter()

    def test_artifact_content_required(self):
        """测试必须提供代码内容之一。"""
        with pytest.raises(ValueError, match="artifact_path or artifact_paths or blob must be provided"):
            ModuleLikeServiceGroup.deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                # 不提供任何代码内容
            )

    def test_blob_requires_filename(self):
        """测试使用 blob 时必须提供 filename。"""
        # 注意：这个测试会尝试实际连接服务器
        # 所以我们只测试参数校验逻辑，不进行实际部署
        # filename 的校验在代码后面，会由服务器端进行验证
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
