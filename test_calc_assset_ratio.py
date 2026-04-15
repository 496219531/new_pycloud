import asyncio
import sys
import time
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pycloud_parallel import GatewayConnect,DirectConnect,TaskPoolSession,JobQueueClient
from calc_asset_ratio import calc_asset_ratio
import calc_asset_ratio_job_module

def get_fund_nav(fund_list=None,frequency=1):
    fund_nav_df=pd.read_csv('fund_nav_df.csv')
    fund_nav_df['TradingDay']=pd.to_datetime(fund_nav_df['TradingDay'])
    return fund_nav_df

def calc_fund_list_asset_ratio(fund_list,strategy_type=1,frequency=1):
    fund_nav_df=get_fund_nav(fund_list,frequency=frequency)
    fund_net_value_pvt=fund_nav_df.pivot(index='TradingDay',columns='FundID',values='AdjustedNav')

    async def async_calls():
        with DirectConnect(
            "127.0.0.1:50051",
            service_name='calc_asset_ratio1',
            timeout_sec=300.0,
        ) as client:
            tasks = [
                client.get_fund_asset_ratio(fund_net_value_series.dropna().copy(), strategy_type, 0)
                for _, fund_net_value_series in fund_net_value_pvt.items()
            ]
            return await asyncio.gather(*tasks)

    ret=asyncio.run(async_calls())  
    return ret

def calc_fund_list_asset_ratio_sync(fund_list,strategy_type=1,frequency=1):
    fund_nav_df=get_fund_nav(fund_list,frequency=frequency)
    fund_net_value_pvt=fund_nav_df.pivot(index='TradingDay',columns='FundID',values='AdjustedNav')

    with DirectConnect(
             "127.0.0.1:50051",
            service_name='calc_asset_ratio',
            timeout_sec=300.0,
        ) as client:
        ret = [
                client.get_fund_asset_ratio.sync(fund_net_value_series.dropna().copy(), strategy_type, 0)
                for _, fund_net_value_series in fund_net_value_pvt.items()
            ]
        print(ret)
        return ret

def calc_fund_list_asset_ratio2(fund_list,strategy_type=1,frequency=1):
    fund_nav_df=get_fund_nav(fund_list,frequency=frequency)
    fund_net_value_pvt=fund_nav_df.pivot(index='TradingDay',columns='FundID',values='AdjustedNav')
    fund_net_value_pvt=fund_net_value_pvt[fund_net_value_pvt.count().sort_values(ascending=False).index.values]
    # source_dir_list =['calc_asset_ratio',] 
    from calc_asset_ratio import calc_asset_ratio
    t0=time.time()
    with TaskPoolSession.from_infocenter(
        infocenter_target= "127.0.0.1:50051",
        job_id=f"demo-pool-{int(time.time())}",
        entry_callable=calc_asset_ratio.get_fund_asset_ratio,
        managed_global_names=[
            "bench_mark_yield_df",
            "bench_mark_yield_df_weekly",
            "bench_mark_closeprice_df",
        ],
        worker_count=5,
        node_count=2,
        tags=["compute"],
        timeout_sec=300.0,
        # artifact_path=source_dir_list,
    ) as pool:
        pool.update_globals(calc_asset_ratio.update_globals())
        print("pool nodes:", pool.node_ids)
        print("pool status:", {k: v.status for k, v in pool.status_map().items()})
        t1=time.time()
        print(t1-t0)
        for task_id,data in  pool.imap_unordered(
           [
                {'fund_net_value_series':fund_net_value_series.dropna(), 'strategy_type':strategy_type,'frequency': 0}
                for _, fund_net_value_series in fund_net_value_pvt.items()
            ],
            result_timeout_sec=300,
        ):
            pass
              
        t2=time.time()
        print(t2-t1)

def calc_fund_list_asset_ratio3(fund_list,strategy_type=1,frequency=1):
    fund_nav_df=get_fund_nav(fund_list,frequency=frequency)
    fund_net_value_pvt=fund_nav_df.pivot(index='TradingDay',columns='FundID',values='AdjustedNav')
    fund_net_value_pvt=fund_net_value_pvt[fund_net_value_pvt.count().sort_values(ascending=False).index.values]
    from calc_asset_ratio import calc_asset_ratio
    t0=time.time()
    with TaskPoolSession.from_infocenter(
        infocenter_target= "127.0.0.1:50051",
        job_id=f"demo-pool-{int(time.time())}",
        entry_module=calc_asset_ratio,
        entry_callable="get_fund_asset_ratio",
        managed_global_names=[
            "bench_mark_yield_df",
            "bench_mark_yield_df_weekly",
            "bench_mark_closeprice_df",
        ],
        worker_count=7,
        node_count=2,
        tags=["compute"],
        timeout_sec=300.0,
        # artifact_path=source_dir_list,
    ) as pool:
        pool.update_globals(calc_asset_ratio.update_globals())
        print("pool nodes:", pool.node_ids)
        print("pool status:", {k: v.status for k, v in pool.status_map().items()})
        t1=time.time()
        print(t1-t0)

        payloads = [
            {
                "fund_net_value_series": fund_net_value_series.dropna(),
                "strategy_type": strategy_type,
                "frequency": 0,
            }
            for _, fund_net_value_series in fund_net_value_pvt.items()
        ]
        resp = pool.submit_payloads(payloads)
        a = pool.wait_for_data(expected_count=len(resp.accepted), timeout_sec=300.0)

        t2=time.time()
        print(t2-t1)
        return a


def calc_fund_list_asset_ratio_job(fund_list,strategy_type=1,frequency=1):
    t0=time.time()
    client_id=f"asset-ratio-job-{int(t0)}"
    # 这里直连 controlplane 的 /jobs 接口，不经过 gateway。
    with JobQueueClient("127.0.0.1:50051", client_id=client_id, timeout_sec=300.0) as client:
        resp = client.submit_job_from_module(
            module=calc_asset_ratio_job_module,
            job_payload={
                "fund_list": fund_list,
                "strategy_type": strategy_type,
                "frequency": frequency,
                "root_dir": str(ROOT_DIR),
            },
            runtime="py3",
            
        )
        job_id = resp["job"]["job_id"]
        print("submitted job:", job_id)
        terminal = client.wait_for_terminal(job_id, timeout_sec=300.0, poll_interval_sec=1.0)

    t1=time.time()
    print(t1-t0)
    job = terminal["job"]
    print("job status:", job["status"])
    if job["status"] != "SUCCEEDED":
        raise RuntimeError(job.get("error", "job failed"))

    t2=time.time()
    print(t2-t1)
    return job.get("final_result")



if __name__ == "__main__":  
#     from pycloud_parallel.controlplane.client import GatewayServiceClient
#     gw = GatewayServiceClient("10.168.70.123:50051")
#     print(gw.get_status(service_name="calc_asset_ratio"))
# from pycloud_parallel.controlplane.client import GatewayServiceClient, InfoCenterClient

# gw = GatewayServiceClient("10.168.70.123:50051")
# print("GW status:", gw.get_status(service_name="calc_asset_ratio"))

# with InfoCenterClient("10.168.70.123:50051") as c:
#     print("IC routes:", c.list_service_routes(service_name="calc_asset_ratio", healthy_only=False, limit=100))
    fund_list=[156695,157112,157541,157670,158463,158624,158875,159467,159858,159879,160041,160057,160216,160217,160996,161044,161081,161629,161663,161820,161860,161990,162175,162192,162193,162430,162453,163269,163388,164852,165747,165901,166299,236965,237213,237422,238019,262084,262120,393169,442942,449552,452965,452989,460460,478874,485939,494206,527458,527469,557725,575845,676027,685885,812974,894218,902755,944418,951556,952128,952172,952469,1401164,1407003,1452421,1465293,1487334,1508664,1529050,1535088,1535620,1537797,1560731,1574709,1578139,1581602,1600394,1616759,1624096,1652875]
    t1=time.time()
    b=calc_fund_list_asset_ratio_job(fund_list,1,1)
    # b=calc_fund_list_asset_ratio2(fund_list,1,1)
    t2=time.time()
    print(t2-t1)
