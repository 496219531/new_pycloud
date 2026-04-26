from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from typing import Sequence

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pycloud_parallel import JobQueue, Service, TaskPool

from calc_asset_ratio.ok import calc_asset_ratio
import calc_asset_ratio_job_module


CONTROLPLANE_TARGET = "127.0.0.1:50051"
SERVICE_NAME = "calc_asset_ratio"
MANAGED_GLOBAL_NAMES = (
    "bench_mark_yield_df",
    "bench_mark_yield_df_weekly",
    "bench_mark_closeprice_df",
)
# pickle_stable_v1 currently uses the protobuf/gRPC bytes transport path.
TASKPOOL_SERIALIZATION_MODE = "pickle_stable_v1"
SERVICE_HTTP_BYTES_SERIALIZATION_MODE = "pickle_stable_v1"


def get_fund_nav(fund_list: Sequence[int] | None = None, frequency: int = 1) -> pd.DataFrame:
    del frequency
    fund_nav_df = pd.read_csv("/Users/hkk/Documents/new_pycloud/fund_nav_df.csv")
    fund_nav_df["TradingDay"] = pd.to_datetime(fund_nav_df["TradingDay"])
    if fund_list:
        fund_nav_df = fund_nav_df[fund_nav_df["FundID"].isin(list(fund_list))]
    return fund_nav_df


def _fund_net_value_pivot(
    fund_list: Sequence[int] | None = None,
    *,
    frequency: int = 1,
) -> pd.DataFrame:
    fund_nav_df = get_fund_nav(fund_list, frequency=frequency)
    fund_net_value_pvt = fund_nav_df.pivot(index="TradingDay", columns="FundID", values="AdjustedNav")
    return fund_net_value_pvt[fund_net_value_pvt.count().sort_values(ascending=False).index.values]


def _iter_payloads(
    fund_net_value_pvt: pd.DataFrame,
    *,
    strategy_type: int = 1,
    frequency: int = 0,
) -> list[dict[str, object]]:
    return [
        {
            "fund_net_value_series": fund_net_value_series.dropna().copy(),
            "strategy_type": strategy_type,
            "frequency": frequency,
        }
        for _, fund_net_value_series in fund_net_value_pvt.items()
    ]


def _normalize_result_item(value):
    if isinstance(value, dict) and tuple(value.keys()) == ("value",):
        return value["value"]
    return value


def _normalize_result_items(values):
    return [_normalize_result_item(value) for value in values]


def _ordered_results_from_pairs(
    items: Sequence[tuple[int, object]],
    *,
    expected_count: int,
):
    ordered = [None] * max(0, int(expected_count))
    for index, value in items:
        normalized_index = int(index)
        if 0 <= normalized_index < len(ordered):
            ordered[normalized_index] = _normalize_result_item(value)
    return ordered


def _connect_service(*, transport: str):
    return Service.connect(
        target=CONTROLPLANE_TARGET,
        service_name=SERVICE_NAME,
        transport=transport,
        timeout_sec=300.0,
    )


def _call_service(payload: dict[str, object], *, transport: str):
    with _connect_service(transport=transport) as service:
        return _normalize_result_item(service.call_sync("get_fund_asset_ratio", **payload))



def calc_fund_list_asset_ratio(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
):
    fund_net_value_pvt = _fund_net_value_pivot(fund_list, frequency=frequency)

    async def async_calls():
        with Service.connect(
            target=CONTROLPLANE_TARGET,
            service_name=SERVICE_NAME,
            transport="discovery",
            timeout_sec=300.0,
            serialization_mode=SERVICE_HTTP_BYTES_SERIALIZATION_MODE,
        ) as client:
            tasks = [
                client.get_fund_asset_ratio(fund_net_value_series.dropna().copy(), strategy_type, 0)
                for _, fund_net_value_series in fund_net_value_pvt.items()
            ]
            return _normalize_result_items(await asyncio.gather(*tasks))
            
    return asyncio.run(async_calls())


def calc_fund_list_asset_ratio_sync(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
):
    fund_net_value_pvt = _fund_net_value_pivot(fund_list, frequency=frequency)

    ret = [
        _call_service(
            {
                "fund_net_value_series": fund_net_value_series.dropna().copy(),
                "strategy_type": strategy_type,
                "frequency": 0,
            },
            transport="discovery",
        )
        for _, fund_net_value_series in fund_net_value_pvt.items()
    ]
    ret = _normalize_result_items(ret)
    print(ret)
    return ret


def calc_fund_list_asset_ratio_gateway_service(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
):
    fund_net_value_pvt = _fund_net_value_pivot(fund_list, frequency=frequency)

    async def async_calls():
        tasks = [
            asyncio.to_thread(
                _call_service,
                {
                    "fund_net_value_series": fund_net_value_series.dropna().copy(),
                    "strategy_type": strategy_type,
                    "frequency": 0,
                },
                transport="gateway",
            )
            for _, fund_net_value_series in fund_net_value_pvt.items()
        ]
        return _normalize_result_items(await asyncio.gather(*tasks))

    return asyncio.run(async_calls())


def calc_fund_list_asset_ratio_gateway_service_sync(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
):
    fund_net_value_pvt = _fund_net_value_pivot(fund_list, frequency=frequency)

    ret = [
        _call_service(
            {
                "fund_net_value_series": fund_net_value_series.dropna().copy(),
                "strategy_type": strategy_type,
                "frequency": 0,
            },
            transport="gateway",
        )
        for _, fund_net_value_series in fund_net_value_pvt.items()
    ]
    ret = _normalize_result_items(ret)
    print(ret)
    return ret


def calc_fund_list_asset_ratio_gateway(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
):
    fund_net_value_pvt = _fund_net_value_pivot(fund_list, frequency=frequency)

    with _connect_service(transport="gateway") as service:
        status = service.status()
        print("gateway route_count:", status.get("route_count"))
        methods = set(service.methods)
        if "get_fund_asset_ratio" not in methods:
            raise RuntimeError(f"gateway service {SERVICE_NAME!r} has no method 'get_fund_asset_ratio': {sorted(methods)}")

        results = []
        for _, fund_net_value_series in fund_net_value_pvt.items():
            results.append(
                _normalize_result_item(
                    service.call_sync(
                        "get_fund_asset_ratio",
                        **{
                            "fund_net_value_series": fund_net_value_series.dropna().copy(),
                            "strategy_type": strategy_type,
                            "frequency": 0,
                        },
                    )
                )
            )
        return results


def calc_fund_list_asset_ratio_service_unordered(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
    *,
    transport: str = "discovery",
):
    fund_net_value_pvt = _fund_net_value_pivot(fund_list, frequency=frequency)
    payloads = _iter_payloads(fund_net_value_pvt, strategy_type=strategy_type, frequency=0)

    with _connect_service(transport=transport) as service:
        items = list(
            service.get_fund_asset_ratio.unordered(
                payloads,
                max_in_flight=min(8, max(1, len(payloads))),
            )
        )
    return _ordered_results_from_pairs(items, expected_count=len(payloads))


def calc_fund_list_asset_ratio_service_aunordered(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
    *,
    transport: str = "discovery",
):
    fund_net_value_pvt = _fund_net_value_pivot(fund_list, frequency=frequency)
    payloads = _iter_payloads(fund_net_value_pvt, strategy_type=strategy_type, frequency=0)

    async def _collect():
        with _connect_service(transport=transport) as service:
            items = []
            async for item in service.get_fund_asset_ratio.aunordered(
                payloads,
                max_in_flight=min(8, max(1, len(payloads))),
            ):
                items.append(item)
            return items

    items = asyncio.run(_collect())
    return _ordered_results_from_pairs(items, expected_count=len(payloads))


def calc_fund_list_asset_ratio2(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
):
    fund_net_value_pvt = _fund_net_value_pivot(fund_list, frequency=frequency)
    payloads = _iter_payloads(fund_net_value_pvt, strategy_type=strategy_type, frequency=0)


    t0 = time.time()
    with TaskPool.open(
        infocenter_target=CONTROLPLANE_TARGET,
        job_id=f"demo-pool-{int(time.time())}",
        source=calc_asset_ratio.get_fund_asset_ratio,
        worker_count=5,
        node_count=2,
        tags=["compute"],
        timeout_sec=300.0,
        managed_global_names=MANAGED_GLOBAL_NAMES,
        serialization_mode=TASKPOOL_SERIALIZATION_MODE,
    ) as pool:
        pool.update_globals(calc_asset_ratio.update_globals())
        print("pool nodes:", pool.node_ids)
        print("pool status:", {k: v.status for k, v in pool.status_map().items()})
        t1 = time.time()
        print(t1 - t0)
        results = []
        for _task_id, data in pool.unordered(payloads, timeout_sec=300):
            results.append(_normalize_result_item(data))
        t2 = time.time()
        print(t2 - t1)
        return results


def calc_fund_list_asset_ratio_taskpool_aunordered(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
):
    fund_net_value_pvt = _fund_net_value_pivot(fund_list, frequency=frequency)
    payloads = _iter_payloads(fund_net_value_pvt, strategy_type=strategy_type, frequency=0)

    async def _collect():
        with TaskPool.open(
            target=CONTROLPLANE_TARGET,
            job_id=f"demo-pool-async-{int(time.time())}",
            source=calc_asset_ratio.get_fund_asset_ratio,
            worker_count=5,
            node_count=2,
            tags=["compute"],
            timeout_sec=300.0,
            managed_global_names=MANAGED_GLOBAL_NAMES,
            # serialization_mode=TASKPOOL_SERIALIZATION_MODE,
        ) as pool:
            pool.update_globals(calc_asset_ratio.update_globals())
            items = []
            async for item in pool.aunordered(
                payloads,
                timeout_sec=300.0,
            ):
                items.append(item)
            return items

    items = asyncio.run(_collect())
    return _ordered_results_from_pairs(items, expected_count=len(payloads))


def calc_fund_list_asset_ratio3(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
):
    fund_net_value_pvt = _fund_net_value_pivot(fund_list, frequency=frequency)
    payloads = _iter_payloads(fund_net_value_pvt, strategy_type=strategy_type, frequency=0)

    t0 = time.time()
    with TaskPool.open(
        infocenter_target=CONTROLPLANE_TARGET,
        job_id=f"demo-pool-{int(time.time())}",
        source=calc_asset_ratio.get_fund_asset_ratio,
        worker_count=7,
        node_count=2,
        tags=["compute"],
        timeout_sec=300.0,
        managed_global_names=MANAGED_GLOBAL_NAMES,
        serialization_mode=TASKPOOL_SERIALIZATION_MODE,
    ) as pool:
        pool.update_globals(calc_asset_ratio.update_globals())
        print("pool nodes:", pool.node_ids)
        print("pool status:", {k: v.status for k, v in pool.status_map().items()})
        t1 = time.time()
        print(t1 - t0)

        resp = pool.submit_payloads(
            payloads,
            serialization_mode=TASKPOOL_SERIALIZATION_MODE,
        )
        results = _normalize_result_items(
            pool.wait_for_data(expected_count=len(resp.accepted), timeout_sec=300.0)
        )

        t2 = time.time()
        print(t2 - t1)
        return results


def calc_fund_list_asset_ratio_job(
    fund_list: Sequence[int] | None,
    strategy_type: int = 1,
    frequency: int = 1,
):
    t0 = time.time()
    client_id = f"asset-ratio-job-{int(t0)}"
    with JobQueue(
        CONTROLPLANE_TARGET,
        client_id=client_id,
        timeout_sec=300.0,
    ) as client:
        resp = client.submit_job_from_module(
            module=calc_asset_ratio_job_module,
            # resource_paths=["fund_nav_df.csv"],
            # task_resource_paths=[
            #     "bench_mark_closeprice_df.csv",
            #     "bench_mark_yield_df_weekly.csv",
            #     "bench_mark_yield_df.csv",
            # ],
            job_payload={
                "fund_list": list(fund_list or ()),
                "strategy_type": strategy_type,
                "frequency": frequency,
            },
            runtime="py3",
            task_serialization_mode=TASKPOOL_SERIALIZATION_MODE,
        )
        job_id = resp["job"]["job_id"]
        print("submitted job:", job_id)
        terminal = client.wait_for_terminal(job_id, timeout_sec=300.0, poll_interval_sec=1.0)

    t1 = time.time()
    print(t1 - t0)
    job = terminal["job"]
    print("job status:", job["status"])
    if job["status"] != "SUCCEEDED":
        raise RuntimeError(job.get("error", "job failed"))

    t2 = time.time()
    print(t2 - t1)
    final_result = job.get("final_result")
    if isinstance(final_result, list):
        return _normalize_result_items(final_result)
    return _normalize_result_item(final_result)


if __name__ == "__main__":
    fund_list = [
        156695,
        157112,
        157541,
        157670,
        158463,
        158624,
        158875,
        159467,
        159858,
        159879,
        160041,
        160057,
        160216,
        160217,
        160996,
        161044,
        161081,
        161629,
        161663,
        161820,
        161860,
        161990,
        162175,
        162192,
        162193,
        162430,
        162453,
        163269,
        163388,
        164852,
        165747,
        165901,
        166299,
        236965,
        237213,
        237422,
        238019,
        262084,
        262120,
        393169,
        442942,
        449552,
        452965,
        452989,
        460460,
        478874,
        485939,
        494206,
        527458,
        527469,
        557725,
        575845,
        676027,
        685885,
        812974,
        894218,
        902755,
        944418,
        951556,
        952128,
        952172,
        952469,
        1401164,
        1407003,
        1452421,
        1465293,
        1487334,
        1508664,
        1529050,
        1535088,
        1535620,
        1537797,
        1560731,
        1574709,
        1578139,
        1581602,
        1600394,
        1616759,
        1624096,
        1652875,
    ]
    t1 = time.time()
    # result = calc_fund_list_asset_ratio(fund_list, 1, 1)
    # result = calc_fund_list_asset_ratio_sync(fund_list, 1, 1)
    # result = calc_fund_list_asset_ratio_gateway_service(fund_list, 1, 1)
    # result = calc_fund_list_asset_ratio_gateway_service_sync(fund_list, 1, 1)
    # result = calc_fund_list_asset_ratio_gateway(fund_list, 1, 1)
    # result = calc_fund_list_asset_ratio3(fund_list, 1, 1)
    result = calc_fund_list_asset_ratio2(fund_list, 1, 1)
    # result = calc_fund_list_asset_ratio_job(fund_list, 1, 1)
    # result = calc_fund_list_asset_ratio_service_aunordered(fund_list,1,1)
    # result = calc_fund_list_asset_ratio_taskpool_aunordered(fund_list,1,1)
    # result = calc_fund_list_asset_ratio_service_unordered(fund_list,1,1)
    t2 = time.time()
    print(result)
    print(t2 - t1)
