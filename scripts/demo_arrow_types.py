#!/usr/bin/env python3
"""
Arrow 兼容类型序列化演示

展示 PyCloud 如何自动处理 Arrow 兼容类型的序列化/反序列化。
支持：DataFrame, Series, numpy array
"""

import asyncio
from pycloud_parallel import DeployedService


def main():
    gateway_target = "127.0.0.1:50051"
    service_name = "arrow-demo"
    # pandas / numpy 如果目标节点未预装，也可以通过 dependency_allowlist 显式补装。
    dependency_allowlist = []

    print("=" * 60)
    print("  Arrow 兼容类型序列化演示")
    print("=" * 60)
    print()

    # 服务代码：直接使用 Arrow 类型
    blob = (
        b"def pycloud_export(fn):\n"
        b"    fn.__pycloud_export__ = True\n"
        b"    return fn\n\n"
        b"@pycloud_export\n"
        b"def process_dataframe(df):\n"
        b"    import pandas as pd\n"
        b"    return {\n"
        b"        'type': 'DataFrame',\n"
        b"        'rows': len(df),\n"
        b"        'columns': list(df.columns),\n"
        b"        'shape': df.shape\n"
        b"    }\n\n"
        b"@pycloud_export\n"
        b"def process_series(series):\n"
        b"    import pandas as pd\n"
        b"    return {\n"
        b"        'type': 'Series',\n"
        b"        'length': len(series),\n"
        b"        'name': series.name,\n"
        b"        'sum': float(series.sum())\n"
        b"    }\n\n"
        b"@pycloud_export\n"
        b"def process_array(arr):\n"
        b"    import numpy as np\n"
        b"    return {\n"
        b"        'type': 'ndarray',\n"
        b"        'shape': arr.shape,\n"
        b"        'dtype': str(arr.dtype),\n"
        b"        'sum': float(arr.sum()),\n"
        b"        'mean': float(arr.mean())\n"
        b"    }\n"
    )

    try:
        print("[1] 部署服务...")
        print("-" * 60)

        group = DeployedService.deploy_from_infocenter(
            infocenter_target=gateway_target,
            service_name=service_name,
            blob=blob,
            filename="arrow_service.py",
            runtime="py3",
            entry_module="arrow_service",
            export_mode="decorator",
            export_decorator="pycloud_export",
            dependency_allowlist=dependency_allowlist,
            worker_count=2,
            tags=["compute"],  # 修改为 compute 标签
            min_success_nodes=1,
        )
        print(f"✓ 服务部署成功")
        print(f"  服务名: {group.service_name}")
        print()

        import time
        time.sleep(3)

    except Exception as e:
        print(f"✗ 部署失败: {e}")
        import traceback
        traceback.print_exc()
        return

    try:
        # 测试 DataFrame
        print("[2] 测试 DataFrame 序列化")
        print("-" * 60)
        import pandas as pd

        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10.5, 20.3, 30.1]
        })
        print(f"原始 DataFrame:")
        print(df)
        print(f"  类型: {type(df)}")
        print(f"  形状: {df.shape}")
        print()

        result = group.process_dataframe.sync(df)
        print(f"✓ 服务端处理结果:")
        for key, value in result.items():
            print(f"  {key}: {value}")
        print()

        # 测试 Series
        print("[3] 测试 Series 序列化")
        print("-" * 60)

        series = pd.Series([1.5, 2.5, 3.5], name="numbers")
        print(f"原始 Series:")
        print(series)
        print(f"  类型: {type(series)}")
        print(f"  名称: {series.name}")
        print()

        result = group.process_series.sync(series)
        print(f"✓ 服务端处理结果:")
        for key, value in result.items():
            print(f"  {key}: {value}")
        print()

        # 测试 numpy array
        print("[4] 测试 numpy array 序列化")
        print("-" * 60)
        import numpy as np

        arr = np.array([[1.0, 2.0], [3.0, 4.0]])
        print(f"原始 numpy array:")
        print(arr)
        print(f"  类型: {type(arr)}")
        print(f"  形状: {arr.shape}")
        print(f"  dtype: {arr.dtype}")
        print()

        result = group.process_array.sync(arr)
        print(f"✓ 服务端处理结果:")
        for key, value in result.items():
            print(f"  {key}: {value}")
        print()

        # 测试嵌套结构
        print("[5] 测试嵌套结构（混合类型）")
        print("-" * 60)

        df2 = pd.DataFrame({"x": [1, 2]})
        arr2 = np.array([10, 20])

        print(f"混合数据: DataFrame + numpy array")
        print(f"  DataFrame: {df2}")
        print(f"  Array: {arr2}")
        print()

        # 异步调用混合数据
        async def mixed_test():
            result_df = await group.process_dataframe(df2)
            result_arr = await group.process_array(arr2)
            return {"df_result": result_df, "arr_result": result_arr}

        results = asyncio.run(mixed_test())
        print(f"✓ DataFrame 结果: {results['df_result']}")
        print(f"✓ Array 结果: {results['arr_result']}")
        print()

    except Exception as e:
        print(f"✗ 调用失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        print("[6] 清理服务")
        print("-" * 60)
        group.close(end_services=True)
        print("✓ 服务已停止")
        print()

    print("=" * 60)
    print("  完成")
    print("=" * 60)
    print()
    print("✅ 所有 Arrow 兼容类型都可以自动序列化/反序列化！")
    print()
    print("支持的类型:")
    print("  ✅ pd.DataFrame  → JSON records")
    print("  ✅ pd.Series    → dict")
    print("  ✅ np.ndarray   → list (保持 dtype 信息)")
    print("  ✅ 嵌套结构")
    print()
    print("序列化流程:")
    print("  客户端: Arrow 对象")
    print("    ↓ dict_to_struct")
    print("  转换:   → JSON (DataFrame: records, Series: dict, ndarray: list)")
    print("    ↓ gRPC 传输")
    print("  接收:   → JSON")
    print("    ↓ convert_dict_to_arrow")
    print("  服务端: Arrow 对象 (还原)")
    print()


if __name__ == "__main__":
    main()
