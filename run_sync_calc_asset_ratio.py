
import os,sys,time
import datetime as dt

from pycloud_parallel import DeployedService

from calc_asset_ratio import calc_asset_ratio

if __name__=='__main__':
    source_dir_list =['calc_asset_ratio',] 
    group = DeployedService.deploy_from_infocenter(
        infocenter_target= "127.0.0.1:50051",
        service_name=f"calc_asset_ratio1" ,
        entry_module="calc_asset_ratio.calc_asset_ratio",
        export_mode="all",
        artifact_path=source_dir_list,
        worker_count=7,
        node_count=2,
        managed_global_names=[
        "bench_mark_yield_df",
        "bench_mark_yield_df_weekly",
        "bench_mark_closeprice_df",
        ],
    )
    data_dict=calc_asset_ratio.update_globals()

    resp = group.update_globals(
                        data_dict
                    )
    group.join()
