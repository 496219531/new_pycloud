#!/usr/bin/env python3
"""
PyCloud 简化部署示例

展示使用默认值简化服务部署。
"""
from pycloud_parallel import DeployedService


def main():
    # 服务代码
    # 如果服务依赖节点未预装的包，可显式填 dependency_allowlist。
    dependency_allowlist = []
    blob = (
        b"from pycloud_parallel import pycloud_export\n\n"
        b"@pycloud_export\n"
        b"def square(x):\n"
        b"    return {'x': x, 'y': x * x}\n"
    )

    print("=" * 60)
    print("  简化部署示例 - 使用默认值")
    print("=" * 60)
    print()

    groups = []
    try:
        # 方式 1：完全不提供 service_name 和 owner_client_id
        print("方式 1：使用所有默认值")
        print("-" * 60)
        group1 = DeployedService.deploy_from_infocenter(
            infocenter_target="127.0.0.1:50051",
            # service_name 和 owner_client_id 会自动生成
            blob=blob,
            runtime="py3",
            entry_module="compute",
            dependency_allowlist=dependency_allowlist,
            worker_count=1,
        )
        groups.append(group1)
        print(f"  自动生成的 owner_client_id: {group1.owner_client_id}")
        print(f"  自动生成的 service_name: {group1.service_name}")
        print()

        # 方式 2：只提供 entry_module，自动生成 service_name
        print("方式 2：提供 entry_module")
        print("-" * 60)
        group2 = DeployedService.deploy_from_infocenter(
            infocenter_target="127.0.0.1:50051",
            # entry_module 会用于生成 service_name
            blob=blob,
            entry_module="my_service",  # 指定 entry_module
            dependency_allowlist=dependency_allowlist,
            worker_count=1,
        )
        groups.append(group2)
        print(f"  自动生成的 service_name: {group2.service_name}")
        print()

        # 方式 3：只提供 owner_client_id，使用默认 service_name
        print("方式 3：只提供 owner_client_id")
        print("-" * 60)
        group3 = DeployedService.deploy_from_infocenter(
            infocenter_target="127.0.0.1:50051",
            owner_client_id="my-custom-client",  # 自定义 owner
            blob=blob,
            dependency_allowlist=dependency_allowlist,
            worker_count=1,
        )
        groups.append(group3)
        print(f"  使用的 owner_client_id: {group3.owner_client_id}")
        print(f"  自动生成的 service_name: {group3.service_name}")
        print()

        # 方式 4：只提供 service_name，使用默认 owner_client_id
        # 固定 service_name 重新部署时，如果代码变化需要先结束旧服务。
        print("方式 4：只提供 service_name")
        print("-" * 60)
        custom_name = "my-custom-service"
        group4 = DeployedService.deploy_from_infocenter(
            infocenter_target="127.0.0.1:50051",
            service_name=custom_name,  # 自定义 service_name
            blob=blob,
            dependency_allowlist=dependency_allowlist,
            worker_count=1,
        )
        groups.append(group4)
        print(f"  自动生成的 owner_client_id: {group4.owner_client_id}")
        print(f"  使用的 service_name: {group4.service_name}")
        print("  重新部署同名服务且代码变化时，需要先结束旧版本")
        print()
    finally:
        # 清理：结束服务，确保脚本可重复运行。
        for group in groups:
            group.close(end_services=True)

    print("=" * 60)
    print("  完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
