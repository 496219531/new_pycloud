#!/usr/bin/env python3
"""
调用已部署服务示例

通过 InfoCenter 服务发现，从已部署的服务列表中选择节点进行调用。
支持同步和异步两种模式。
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.client import InfoCenterClient, InfoCenterServiceRoute


def _call_http(route_url: str, method: str, payload: dict, timeout_sec: float) -> dict:
    """通过 HTTP 调用服务节点。"""
    url = f"{route_url}/call/{method}?timeout_sec={max(0.1, timeout_sec):.3f}"
    req = Request(
        url=url,
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload).encode("utf-8"),
    )
    try:
        with urlopen(req, timeout=max(2.0, timeout_sec + 1.0)) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8") if hasattr(exc, 'read') else ""
        raise RuntimeError(f"HTTP {exc.code}: {body or exc.reason}")


def call_service(
    routes: list[InfoCenterServiceRoute],
    method: str,
    payload: dict,
    timeout_sec: float = 10.0,
    strategy: str = "least_inflight",
) -> tuple[InfoCenterServiceRoute, dict]:
    """同步调用服务（自动选择最优节点）。

    Args:
        routes: 服务路由列表
        method: 方法名
        payload: 调用参数
        timeout_sec: 超时时间
        strategy: 选择策略 ("least_inflight" | "round_robin")

    Returns:
        (路由, 响应结果)
    """
    if not routes:
        raise RuntimeError("no routes available")

    if strategy == "least_inflight":
        # 按 in_flight 升序，然后按 alive_workers 降序
        routes = sorted(routes, key=lambda x: (x.in_flight, -x.alive_workers))
    # else: round_robin 可以通过外部计数器实现

    route = routes[0]
    body = _call_http(route.http_base_url, method, payload, timeout_sec=timeout_sec)

    if not body.get("ok", False):
        raise RuntimeError(f"call failed: {body.get('error', 'unknown')}")

    return route, body


async def acall_service(
    routes: list[InfoCenterServiceRoute],
    method: str,
    payload: dict,
    timeout_sec: float = 10.0,
    strategy: str = "least_inflight",
) -> tuple[InfoCenterServiceRoute, dict]:
    """异步调用服务（自动选择最优节点）。"""
    if not routes:
        raise RuntimeError("no routes available")

    if strategy == "least_inflight":
        routes = sorted(routes, key=lambda x: (x.in_flight, -x.alive_workers))

    route = routes[0]

    # 在线程池中执行同步 HTTP 调用
    loop = asyncio.get_running_loop()
    body = await loop.run_in_executor(
        None,
        lambda: _call_http(route.http_base_url, method, payload, timeout_sec)
    )

    if not body.get("ok", False):
        raise RuntimeError(f"call failed: {body.get('error', 'unknown')}")

    return route, body


async def acall_all_nodes(
    routes: list[InfoCenterServiceRoute],
    method: str,
    payload: dict,
    timeout_sec: float = 10.0,
    max_concurrency: int = 50,
) -> list[tuple[Optional[InfoCenterServiceRoute], Optional[dict], Optional[Exception]]]:
    """并发调用所有节点。

    Returns:
        [(路由, 响应, 异常), ...]
    """
    if not routes:
        return []

    semaphore = asyncio.Semaphore(max_concurrency)
    loop = asyncio.get_running_loop()

    async def _call_one(route: InfoCenterServiceRoute):
        async with semaphore:
            try:
                body = await loop.run_in_executor(
                    None,
                    lambda: _call_http(route.http_base_url, method, payload, timeout_sec)
                )
                if not body.get("ok", False):
                    return route, None, RuntimeError(body.get('error', 'unknown'))
                return route, body, None
            except Exception as exc:
                return route, None, exc

    tasks = [_call_one(route) for route in routes]
    return await asyncio.gather(*tasks)


async def batch_call(
    routes: list[InfoCenterServiceRoute],
    method: str,
    payloads: list[dict],
    timeout_sec: float = 10.0,
    max_concurrency: int = 100,
) -> list[tuple[Optional[InfoCenterServiceRoute], Optional[dict], Optional[Exception]]]:
    """批量并发调用（自动分配到最优节点）。"""
    if not routes:
        return []

    semaphore = asyncio.Semaphore(max_concurrency)
    loop = asyncio.get_running_loop()

    async def _call_one(i: int):
        async with semaphore:
            # 每次选择最优节点
            sorted_routes = sorted(routes, key=lambda x: (x.in_flight, -x.alive_workers))
            route = sorted_routes[i % len(sorted_routes)]
            payload = payloads[i]

            try:
                body = await loop.run_in_executor(
                    None,
                    lambda: _call_http(route.http_base_url, method, payload, timeout_sec)
                )
                if not body.get("ok", False):
                    return route, None, RuntimeError(body.get('error', 'unknown'))
                return route, body, None
            except Exception as exc:
                return route, None, exc

    tasks = [_call_one(i) for i in range(len(payloads))]
    return await asyncio.gather(*tasks)


def main() -> None:
    # 配置
    infocenter_addr = "127.0.0.1:50051"
    service_name = "square-service"  # 修改为你的服务名
    method = "square"                  # 修改为你的方法名

    print("=" * 60)
    print("  PyCloud Service Client Demo")
    print("=" * 60)
    print()
    print(f"  InfoCenter: {infocenter_addr}")
    print(f"  Service:     {service_name}")
    print(f"  Method:      {method}")
    print()

    # 获取服务路由
    print("-" * 60)
    print("  Step 1: 查询服务路由")
    print("-" * 60)

    with InfoCenterClient(infocenter_addr) as client:
        routes = list(client.list_service_routes(
            service_name=service_name,
            healthy_only=True,
            limit=200,
        ))

    if not routes:
        print(f"  [!] 没有找到服务: {service_name}")
        print()
        print("  请先部署服务，或检查服务名是否正确")
        print("  可用的服务名示例: compute-service, square-service")
        return

    print(f"  找到 {len(routes)} 个可用节点:")
    for route in routes:
        print(f"    - {route.node_id}: {route.http_base_url}")
        print(f"      in_flight={route.in_flight}, alive_workers={route.alive_workers}")
    print()

    # 单次调用测试
    print("-" * 60)
    print("  Step 2: 单次调用测试")
    print("-" * 60)

    payload = {"x": 42}
    print(f"  Payload: {payload}")

    try:
        route, resp = call_service(routes, method, payload, timeout_sec=10.0)
        print(f"  节点: {route.node_id}")
        print(f"  结果: {resp.get('data')}")
    except Exception as exc:
        print(f"  [ERROR] {exc}")
    print()


def demo_async():
    """异步调用演示"""
    infocenter_addr = "127.0.0.1:50051"
    service_name = "compute-service"
    method = "square"

    print("=" * 60)
    print("  PyCloud Async Service Client Demo")
    print("=" * 60)
    print()

    # 获取路由
    with InfoCenterClient(infocenter_addr) as client:
        routes = list(client.list_service_routes(
            service_name=service_name,
            healthy_only=True,
        ))

    if not routes:
        print(f"  [!] 没有找到服务: {service_name}")
        return

    print(f"  找到 {len(routes)} 个节点")
    print()

    async def run():
        # 示例 1: 批量并发调用
        print("-" * 60)
        print("  示例 1: 批量并发调用 (100 次)")
        print("-" * 60)

        payloads = [{"x": i} for i in range(100)]
        start = time.time()

        results = await batch_call(
            routes,
            method,
            payloads,
            timeout_sec=10.0,
            max_concurrency=50,
        )

        success = sum(1 for _, _, exc in results if exc is None)
        elapsed = time.time() - start
        print(f"  成功: {success}/{len(results)}")
        print(f"  耗时: {elapsed:.3f}s, QPS: {success/elapsed:.1f}")
        print()

        # 示例 2: 调用所有节点
        print("-" * 60)
        print("  示例 2: 同时调用所有节点")
        print("-" * 60)

        payload = {"x": 999}
        results = await acall_all_nodes(routes, method, payload, timeout_sec=10.0)

        for route, resp, exc in results:
            if exc:
                print(f"  {route.node_id}: FAILED - {exc}")
            else:
                print(f"  {route.node_id}: {resp.get('data')}")
        print()

    asyncio.run(run())


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--async":
        demo_async()
    else:
        main()
