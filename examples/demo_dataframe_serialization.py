#!/usr/bin/env python3
"""
DataFrame 序列化演示

展示 PyCloud 如何自动处理 DataFrame 和 Series 的序列化/反序列化。
"""

import asyncio
from pycloud_parallel import Service


def main():
    gateway_target = "127.0.0.1:50051"
    service_name = "dataframe-demo"

    print("=" * 60)
    print("  DataFrame 序列化演示")
    print("=" * 60)
    print()

    # 服务代码：直接使用 DataFrame
    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def process_dataframe(df):\n"
        b"    import pandas as pd\n"
        b"    return pd.DataFrame(columns=[1,3,4,5],index=range(100000))\n" 
        b"    \n\n"
        b"@export\n"
        b"def process_series(series):\n"
        b"    import pandas as pd\n"
        b"    return {\n"
        b"        'length': len(series),\n"
        b"        'name': series.name,\n"
        b"        'sum': series.sum(),\n"
        b"        'mean': series.mean()\n"
        b"    }\n"
    )

    print("[1] 部署服务...")
    print("-" * 60)

    group = Service.deploy_from_infocenter(
        infocenter_target=gateway_target,
        service_name=service_name,
        blob=blob,
        runtime="py3",
        entry_module="dataframe_service",
        export_mode="decorator",
        worker_count=2,
        tags=["compute"],  # 修改为 compute 标签
        min_success_nodes=1,
    )
    print(f"✓ 服务部署成功")
    print(f"  服务名: {group.service_name}")
    print(f"  节点: {list(group.sessions.keys())}")
    print()

    import time
    time.sleep(3)

    try:
        print("[2] 测试 DataFrame 序列化")
        print("-" * 60)

        # 创建 DataFrame
        import pandas as pd
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50]
        })

        print(f"原始 DataFrame:")
        print(df)
        print()

        # 同步调用
        result = group.process_dataframe.sync(df)
        print(f"✓ 服务端处理结果:")
        print(f"  类型: {type(result).__name__}")
        print(f"  行数: {len(result)}")
        print(f"  列: {list(result.columns)}")
        print("  前 3 行:")
        print(result.head(3))
        print()

        # 异步调用
        print("异步调用:")

        async def async_test():
            result = await group.process_dataframe(df)
            print(f"  类型: {type(result).__name__}, 行数: {len(result)}")

        asyncio.run(async_test())
        print()

        print("[3] 测试 Series 序列化")
        print("-" * 60)

        series = pd.Series([1, 2, 3, 4, 5], name="numbers")
        print(f"原始 Series:")
        print(series)
        print()

        result = group.process_series.sync(series)
        print(f"✓ 服务端处理结果:")
        print(f"  长度: {result['length']}")
        print(f"  名称: {result['name']}")
        print(f"  总和: {result['sum']}")
        print(f"  平均: {result['mean']}")
        print()

    finally:
        print("[4] 清理服务")
        print("-" * 60)
        group.close(end_services=True)
        print("✓ 服务已停止")
        print()

    print("=" * 60)
    print("  完成")
    print("=" * 60)
    print()
    print("✅ DataFrame 和 Series 可以自动序列化/反序列化！")
    print()
    print("说明：")
    print("  - 小对象：DataFrame/Series 会自动走 inline 传输")
    print("  - 大对象：会自动转成 DataRef")
    print("  - 对象路径：DataFrame/Series 使用 bundle(data.parquet + meta.json)")
    print("  - 服务端：调用前会自动还原成 DataFrame/Series")
    print()


if __name__ == "__main__":
    main()
