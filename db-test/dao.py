
import pandas as pd

#from .

from pycloud_parallel import GatewayConnect
def get_bench_mark_yield(bench_id_list,frequency=1):
    with GatewayConnect(
        "127.0.0.1:50051",
        service_name='public_data_source4',
        timeout_sec=10.0,
    ) as client:
        ret=client.get_bench_list_yield.sync(bench_id_list,frequency) 
        return ret

def get_bench_mark_close_price(bench_id_list,frequency=1):
    with GatewayConnect(
        "127.0.0.1:50051",
        service_name='public_data_source4',
        timeout_sec=10.0,
    ) as client:
        ret=client.get_bench_list_closeprice.sync(bench_id_list,frequency) 
        return ret
    


