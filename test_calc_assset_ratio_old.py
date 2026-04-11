import asyncio
import sys
import time
from pathlib import Path
from pycloud import CloudClient
from  calc_asset_ratio import calc_asset_ratio

import pandas as pd

def get_fund_nav(fund_list=None,frequency=1):
    fund_nav_df=pd.read_csv('fund_nav_df.csv')
    fund_nav_df['TradingDay']=pd.to_datetime(fund_nav_df['TradingDay'])
    if fund_list:
        fund_nav_df=fund_nav_df[fund_nav_df['FundID'].isin(fund_list)].copy()
    return fund_nav_df

def calc_fund_list_asset_ratio(fund_list,strategy_type=1,frequency=1):
    fund_nav_df=get_fund_nav(fund_list,frequency=frequency)
    fund_net_value_pvt=fund_nav_df.pivot(index='TradingDay',columns='FundID',values='AdjustedNav')
    calc_asset_ratio_async=CloudClient(calc_asset_ratio,'tcp://172.16.10.75:2233',service_name='calc_asset_ratio',async_mode=0)
    async def async_calls():
        tasks = [
                calc_asset_ratio_async.get_fund_asset_ratio(fund_net_value_series.dropna().copy(), strategy_type, 0)
                for _, fund_net_value_series in fund_net_value_pvt.items()
            ]
        return await asyncio.gather(*tasks)

    ret=asyncio.run(async_calls())  
    return ret


def calc_fund_list_asset_ratio2(fund_list,strategy_type=1,frequency=1):
    fund_nav_df=get_fund_nav(fund_list,frequency=frequency)
    fund_net_value_pvt=fund_nav_df.pivot(index='TradingDay',columns='FundID',values='AdjustedNav')
    source_dir_list =['calc_asset_ratio',] 
    with TaskPoolSession.from_infocenter(
        infocenter_target= "127.0.0.1:50051",
        job_id=f"demo-pool-{int(time.time())}",
        entry_module="calc_asset_ratio.calc_asset_ratio",
        entry_callable="get_fund_asset_ratio",
        worker_count=7,
        node_count=2,
        tags=["compute"],
        timeout_sec=300.0,
        artifact_path=source_dir_list,
    ) as pool:
        print("pool nodes:", pool.node_ids)
        print("pool status:", {k: v.status for k, v in pool.status_map().items()})

        for task_id,data in  pool.imap_unordered(
           [
                {'fund_net_value_series':fund_net_value_series.dropna().copy(), 'strategy_type':strategy_type,'frequency': 0}
                for _, fund_net_value_series in fund_net_value_pvt.items()
            ],
            result_timeout_sec=300
        ):
              
            print("results:", task_id,data)



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
    b=calc_fund_list_asset_ratio2(fund_list,1,1)
    t2=time.time()
    print(t2-t1)
