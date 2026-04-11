
import pandas as pd

#from . import local_db_api

from pycloud_parallel import GatewayConnect
def get_bench_mark_yield(bench_id_list,frequency=1):
    with GatewayConnect(
     "10.168.70.123:50051",
        service_name='public_data_source4',
        timeout_sec=300.0,
    ) as client:
        ret=client.get_bench_list_yield.sync(bench_id_list,frequency) 
        return ret

def get_bench_mark_close_price(bench_id_list,frequency=1):
    with GatewayConnect(
        "10.168.70.123:50051",
        service_name='public_data_source4',
        timeout_sec=300.0,
    ) as client:
        ret=client.get_bench_list_closeprice.sync(bench_id_list,frequency) 
        return ret
    

