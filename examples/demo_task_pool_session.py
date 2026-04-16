#!/usr/bin/env python3
"""
原生 TaskPool 演示。

创建专属 pool，提交一批 task，查看 pool 状态，拿结果并关闭。
"""

from __future__ import annotations

import time

from pycloud_parallel import TaskPool


def main() -> None:
    blob = (
        b"def run(value=0, sleep_ms=0, **_kwargs):\n"
        b"    import time\n"
        b"    sleep_ms = int(sleep_ms)\n"
        b"    if sleep_ms > 0:\n"
        b"        time.sleep(sleep_ms / 1000.0)\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'square': value * value}\n"
    )

    with TaskPool.from_infocenter(
        infocenter_target="127.0.0.1:50051",
        job_id=f"demo-pool-{int(time.time())}",
        blob=blob,
        entry_module="task_pool_demo",
        entry_callable="run",
        worker_count=2,
        node_count=1,
        tags=["compute"],
    ) as pool:
        print("pool nodes:", pool.node_ids)
        print("pool status:", {k: v.status for k, v in pool.status_map().items()})

        resp = pool.submit_payloads(
            [
                {"value": 2, "sleep_ms": 100},
                {"value": 3, "sleep_ms": 100},
                {"value": 4, "sleep_ms": 100},
            ]
        )
        print("accepted:", [item.task_id for item in resp.accepted])

        results = pool.wait_for_data(expected_count=len(resp.accepted), timeout_sec=30.0)
        print("results:", results)

        mapped = pool.map([5, 6, 7], timeout_sec=30.0)
        print("mapped:", mapped)


if __name__ == "__main__":
    main()
