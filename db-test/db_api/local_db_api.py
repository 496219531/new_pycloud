# -*- coding:UTF-8 -*-
import os,sys,shutil
import time
import datetime as dt
import pandas as pd
from pandas.core import series
from sqlalchemy import create_engine
import pickle
from threading import Thread,Lock
from collections import deque
import numpy as np
from importlib import reload

from pycloud.LOG import LOG
from pycloud.dbsynchelper import dbsynchelper
from pycloud.Hdf5_Api import Hdf5Api,intnan
from .getdata import GetData
from datetime import datetime, timedelta


base_path = 'Local_DB'
logging=LOG()

public_data_dict={}
label_data_net_value_dict={}
future_cache={}
tradingday_df=None

dbhelper_dict={}
fund_net_value_df=None


def get_db_helper(db_address=None,db_name='Flare-Public'):
    global dbhelper_dict
    from .import DBCfg
    if db_address is None:
        db_address='10.168.20.22,1433'
    key=f'{db_address}_{db_name}'
    dbhelper=dbhelper_dict.get(key)
    if dbhelper is None: 
        dbinfo = DBCfg.get_dbcfg(db_address, db_name)
        dbhelper=dbsynchelper()
        dbhelper.SetDBCfg(dbinfo)
        dbhelper_dict[key]=dbhelper
    return dbhelper

def get_all_tradingday(start_date=None,end_date=None):
    global tradingday_df
    
    if  tradingday_df is None:
        dbhelper=get_db_helper()
        sql = '''select TradingDay, IfNatureWeekEnd, IfWeekEnd from [Flare-Public]..HD_TradingDay where 1=1 '''
        if start_date is not None:
            sql += ''' and TradingDay>='{0}' '''.format(str(start_date))
        if end_date is not None:
            sql += ''' and TradingDay<='{0}' '''.format(str(end_date))
        tradingday_df=dbhelper.select(sql)
    return tradingday_df



def reset_cache():
    global public_data_dict, label_data_net_value_dict,dbhelper_dict,future_cache,fund_net_value_df
    public_data_dict={}
    label_data_net_value_dict={}
    dbhelper_dict={}
    future_cache=None
    fund_net_value_df=None


def get_bench_list_yield(bench_list, frequency):
    # table_name = 'BenchMarkYield'
    # res_df=get_public_data_dict(table_name)
    # if res_df is None:
    #     return
    # ret=res_df[(res_df.BenchID.isin(bench_list))&(res_df.Period==frequency)]
    dbhelper=get_db_helper()
    str_bench_list=str(bench_list)[1:-1]
    sql=f'select * from [Dderive]..benchmarkyield where Period={frequency} and BenchID in ({str_bench_list}) and yieldtype=1'
    ret=dbhelper.select(sql)
    if ret is not None and len(ret)>0:
        ret=ret.pivot(index='TradingDay',columns='BenchID',values='Yield')
        return ret

def get_bench_list_netvalue(bench_list, frequency):
    # table_name = 'BenchMarkYield'
    # res_df=get_public_data_dict(table_name)
    # if res_df is None:
    #     return
    # ret=res_df[(res_df.BenchID.isin(bench_list))&(res_df.Period==frequency)]
    dbhelper=get_db_helper()
    str_bench_list=str(bench_list)[1:-1]
    sql=f'select TradingDay,BenchID,ClosePrice from [DDerive]..benchmarkyield where Period={frequency} and BenchID in ({str_bench_list}) and yieldtype=1'
    ret=dbhelper.select(sql)
    if ret is not None and len(ret)>0:
        ret=ret.pivot(index='TradingDay',columns='BenchID',values='ClosePrice')
        return ret

def get_benchmark_port_yield(bench_list, frequency):
    # table_name = 'BenchmarkPortYield'
    # read_sql = '({}) & (Period=={}) & (YieldType==1) & (YieldSubType==1)'.format(' | '.join(['(BenchID=={})'.format(b) for b in bench_list]), frequency)
    # filename = '{}/DDerive/DDerive.h5'.format(base_path)
    # if not os.path.isfile(filename):
    #     logging.warning('filename:{} is not exist'.format(filename))
    #     return
    # res_df=None
    # hdf_api = Hdf5Api(filename, mode='r')
    # if table_name in hdf_api.tablename_list:
    #     res_df = hdf_api.read_rows(table_name, read_sql)
    # hdf_api.close()
    dbhelper=get_db_helper()
    str_bench_list=str(bench_list)[1:-1]
    sql=f'select * from [flare-public]..benchmarkportyield where Period={frequency} and BenchID in ({str_bench_list}) and yieldtype=1'
    ret=dbhelper.select(sql)
    if len(res_df)>0:
        res_df.TradingDay=res_df.TradingDay.astype('<M8[ns]')
        res_df.drop_duplicates(['TradingDay','BenchID'],keep='last',inplace=True)
        res_df=res_df.pivot(index='TradingDay',columns='BenchID',values='Yield')
    return res_df

def get_bench_yield(benchmark_type, bench_id, frequency):
    if benchmark_type==1:
        return get_bench_list_yield([bench_id],frequency=frequency)
    else:
        return get_benchmark_port_yield(bench_id,frequency)

def get_bench_list_closeprice1(bench_list, frequency):
    table_name = 'BenchMarkYield'
    read_sql = '({}) & (Period=={}) & (YieldType==1) & (YieldSubType==1)'.format(' | '.join(['(BenchID=={})'.format(b) for b in bench_list]), frequency)
    # filename = '{}/DDerive/DDerive.h5'.format(base_path)
    filename = '{}/DDerive/BenchMarkYield.h5'.format(base_path)
    if not os.path.isfile(filename):
        logging.warning('filename:{} is not exist'.format(filename))
        return
    res_df=None
    hdf_api = Hdf5Api(filename, mode='r')
    if table_name in hdf_api.tablename_list:
        res_df = hdf_api.read_rows(table_name, read_sql)
    hdf_api.close()
    if res_df is not None and len(res_df)>0:
        res_df.TradingDay=res_df.TradingDay.astype('<M8[ns]')
        res_df.drop_duplicates(['TradingDay','BenchID'],keep='last',inplace=True)
        res_df=res_df.pivot(index='TradingDay',columns='BenchID',values='ClosePrice')
    return res_df

def get_bench_list_closeprice(bench_list, frequency):
    return get_bench_list_netvalue(bench_list,frequency)
    # table_name = 'BenchMarkYield'
    # res_df=get_public_data_dict(table_name)
    # if res_df is None:
    #     return
    # ret=res_df[(res_df.BenchID.isin(bench_list))&(res_df.Period==frequency)]
    # if len(ret)>0:
    #     ret=ret.pivot('TradingDay','BenchID','ClosePrice')
    #     return ret

# def get_bench_asset_ratio1( bench_id, frequency):
#     table_name = 'BenchAssetRatio'
#     read_sql = '(BenchID=={}) & (Frequency=={})'.format(bench_id, frequency)
#     filename = '{}/DDerive/DDerive.h5'.format(base_path)
#     if not os.path.isfile(filename):
#         logging.warning('filename:{} is not exist'.format(filename))
#         return
#     res_df=None
#     hdf_api = Hdf5Api(filename, mode='r')
#     if table_name in hdf_api.tablename_list:
#         res_df = hdf_api.read_rows(table_name, read_sql)
#     hdf_api.close()
#     if res_df is not None and len(res_df)>0 :
#         res_df.TradingDay=res_df.TradingDay.astype('<M8[ns]')
#         res_df.drop_duplicates(['TradingDay','ExposureSubType'],keep='last',inplace=True)
#         res_df=res_df.pivot('TradingDay','ExposureSubType','Beta')
#     return res_df

def get_bench_asset_ratio( bench_id, frequency):
    # table_name = 'BenchAssetRatio'
    # res_df=get_public_data_dict(table_name)
    # if res_df is None:
    #     return
    # ret=res_df[(res_df.BenchID==bench_id)&(res_df.Frequency==frequency)]
    # if len(ret)>0:
    #     ret=ret.pivot('TradingDay','ExposureSubType','Beta')
    #     ret.rename(columns={10355:10024,10356:10025,10357:10026,10358:10027,10359:10028,10360:10075,10366:10024,10367:10025,10368:10026,10369:10027,10370:10028,10371:10075,10372:10024,10373:10025,10374:10026,10375:10027,10376:10028,10377:10075,},inplace=True)
    #     ret.rename(columns={10378:10016,10379:10017,10380:10018,10381:10019,10382:10018,10383:10019,10384:10016,10385:10017,10668:10016,10669:10017,},inplace=True)
    #     for exposure_sub_type in [10016, 10017, 10018, 10019, 10020, 10021, 10024, 10025, 10026, 10027, 10028, 10075]:
    #         if exposure_sub_type not in ret:
    #             ret[exposure_sub_type] = 0
    #     return ret
    dbhelper=get_db_helper()
    sql=f'select * from [flare-public]..BenchAssetRatio where Frequency={frequency} and BenchID={bench_id}'
    ret=dbhelper.select(sql)
    if ret is not None and len(ret)>0:
        ret=ret.drop_duplicates(['TradingDay','ExposureSubType'])
        ret=ret.pivot(index='TradingDay',columns='ExposureSubType',values='Beta')
        ret.rename(columns={10355:10024,10356:10025,10357:10026,10358:10027,10359:10028,10360:10075,10366:10024,10367:10025,10368:10026,10369:10027,10370:10028,10371:10075,10372:10024,10373:10025,10374:10026,10375:10027,10376:10028,10377:10075,},inplace=True)
        ret.rename(columns={10378:10016,10379:10017,10380:10018,10381:10019,10382:10018,10383:10019,10384:10016,10385:10017,10668:10016,10669:10017,},inplace=True)
        for exposure_sub_type in [10016, 10017, 10018, 10019, 10020, 10021, 10024, 10025, 10026, 10027, 10028, 10075]:
            if exposure_sub_type not in ret:
                ret[exposure_sub_type] = 0
        return ret

def get_bench_list_asset_ratio( bench_list, frequency):
    dbhelper=get_db_helper()
    str_bench_list=str(bench_list)[1:-1]
    sql=f'select * from [flare-public]..BenchAssetRatio where Frequency={frequency} and BenchID in ({str_bench_list})'
    ret=dbhelper.select(sql)
    if len(ret)>0:
        return ret

def get_scene_market_df( scene_type_pair_list,frequency):
    dbhelper=get_db_helper()
    str_scene_type_list=str(scene_type_pair_list)[1:-1]
    ret_list=[]
    for scene_type,bench_id in scene_type_pair_list:
        sql=f'select * from [flare-public]..SceneMarket where Frequency={frequency} and SceneType={scene_type} and benchid={bench_id}  '
        ret=dbhelper.select(sql)
        if ret is not None:
            ret_list.append(ret)
    if ret_list:
        ret=pd.concat(ret_list,sort=False)
        ret.sort_values(['TradingDay'],inplace=True)
        ret.index=ret.TradingDay
        return ret
    # table_name = 'SceneMarket'
    # res_df=get_public_data_dict(table_name)
    # if res_df is None:
    #     return
    # ret_list=[]
    # for scene_type,bench_id in scene_type_pair_list:
    #     ret=res_df[(res_df.BenchID==bench_id)&(res_df.Frequency==frequency)&(res_df.SceneType==scene_type)]
    #     if len(ret)>0:
    #         ret_list.append(ret)
    # if ret_list:
    #     ret=pd.concat(ret_list,sort=False)
    #     ret.sort_values(['TradingDay'],inplace=True)
    #     ret.index=ret.TradingDay
    #     return ret

def get_scene_market_df_by_type(scene_type_list,frequency):
    dbhelper=get_db_helper()
    str_scene_type_list=str(scene_type_list)[1:-1]
    sql=f'select * from [flare-public]..SceneMarket where Frequency={frequency} and SceneType in ({str_scene_type_list})'
    ret=dbhelper.select(sql)
    # table_name = 'SceneMarket'
    # res_df=get_public_data_dict(table_name)
    # if res_df is None:
    #     return
    # ret_list=[]
    # for scene_type in scene_type_list:
    #     ret=res_df[(res_df.Frequency==frequency)&(res_df.SceneType==scene_type)]
    #     if len(ret)>0:
    #         ret_list.append(ret)
    # if ret_list:
    if ret is not None:
        # ret=pd.concat(ret_list,sort=False)
        ret.sort_values(['TradingDay'],inplace=True)
        ret.index=ret.TradingDay
        return ret

def get_index_duration_df(bench_list,frequency):
    dbhelper=get_db_helper()
    str_bench_list=str(bench_list)[1:-1]
    sql=f'select * from [flare-public]..IndexDuration where Frequency={frequency} and BenchID in ({str_bench_list})'
    ret=dbhelper.select(sql)
    # table_name = 'IndexDuration'
    # res_df=get_public_data_dict(table_name)
    # if res_df is None:
    #     return
    # ret_list=[]
    # for bench_id in bench_list:
    #     ret=res_df[(res_df.BenchID==bench_id)&(res_df.Frequency==frequency)]
    #     if len(ret)>0:
    #         ret_list.append(ret)
    # if ret_list:
    #     ret=pd.concat(ret_list,sort=False)
    if ret is not None:
        ret.sort_values(['TradingDay'],inplace=True)
        ret.index=ret.TradingDay
        return ret

# def get_scene_market_df1( scene_type_pair_list,frequency):
#     table_name = 'SceneMarket'
#     scene_type_condition = ' | '.join(['((SceneType =={}) & (BenchID=={}))'.format(a[0],a[1]) for a in scene_type_pair_list])
#     read_sql = '({}) & (Frequency=={})'.format(scene_type_condition, frequency)
#     filename = '{}/DDerive/DDerive.h5'.format(base_path)
#     if not os.path.isfile(filename):
#         logging.warning('filename:{} is not exist'.format(filename))
#         return
#     res_df=None
#     hdf_api = Hdf5Api(filename, mode='r')
#     if table_name in hdf_api.tablename_list:
#         res_df = hdf_api.read_rows(table_name, read_sql)
#     hdf_api.close()
#     if res_df is not None and len(res_df)>0:
#         res_df.TradingDay=res_df.TradingDay.astype('<M8[ns]')
#         res_df.drop_duplicates(['TradingDay','SceneType','BenchID'],keep='last',inplace=True)
#         res_df.sort_values(['TradingDay'],inplace=True)
#         res_df.index=res_df.TradingDay
#         return res_df

def get_fund_index_corr(fund_list,frequency,dbname,cateinfo,corr_type_tuple_list=None,planid=None,start_date=None,end_date=None):
    table_name='FundIndexCorr'
    keylist=['TradingDay','YieldType','YieldSubType','BenchID']
    read_sql='(Frequency=={})'.format(frequency)
    if start_date is not None:
        start_num=np.datetime64(start_date,'ns').astype(np.longlong)
        read_sql+=' & (TradingDay>={}) '.format(start_num)
    if end_date is not None:
        end_num=np.datetime64(end_date,'ns').astype(np.longlong)
        read_sql+=' & (TradingDay<={}) '.format(end_num)

    if corr_type_tuple_list is not None:
        str_sql=' | '.join(['((YieldType=={}) & (YieldSubType=={}) & (BenchID=={})) '.format(yield_type,yield_sub_type,bench_id) for yield_type,yield_sub_type,bench_id in corr_type_tuple_list])
        read_sql+=' & ({}) '.format(str_sql)
    ret_dict=get_fund_data_from_hdf5(fund_list,cateinfo,dbname,table_name,read_sql,keylist=keylist,planid=planid)
    if ret_dict is None:
        return
    for v in ret_dict.values():
        v.TradingDay=v.TradingDay.astype('<M8[ns]')
        v.index=v.TradingDay
    return ret_dict
    # if yield_type_pair_list is None:
    #     return tmp_ret_dict
    # else:
    #     mark_list=[]
    #     for yield_type ,yield_sub_type in yield_type_pair_list:
    #         mark_list.append(yield_type*100+yield_sub_type)

    #     ret_dict={}
    #     for k,v in tmp_ret_dict.items():
    #         v.TradingDay=v.TradingDay.astype('<M8[ns]')
    #         v.index=v.TradingDay
    #         mark=v.YieldType*100+v.YieldSubType
    #         v=v[mark.isin(mark_list)]
    #         ret_dict[k]=v
    #     return ret_dict

def get_fund_statis(fund_list,frequency,dbname,cateinfo,yield_type_pair_list=None,plan_id=None,start_date=None,end_date=None):
    table_name='FundRollStatis4Rank'
    keylist=['TradingDay','YieldType','YieldSubType']
    read_sql='(Frequency=={})'.format(frequency)
    if start_date is not None:
        start_num=np.datetime64(start_date,'ns').astype(np.longlong)
        read_sql+=' & (TradingDay>={}) '.format(start_num)
    if end_date is not None:
        end_num=np.datetime64(end_date,'ns').astype(np.longlong)
        read_sql+=' & (TradingDay<={}) '.format(end_num)

    if yield_type_pair_list is not None :
        str_sql=' | '.join(['((YieldType=={}) & (YieldSubType=={})) '.format(yield_type,yield_sub_type) for yield_type,yield_sub_type in yield_type_pair_list])
        read_sql+=' & ({}) '.format(str_sql)
    ret_dict=get_fund_data_from_hdf5(fund_list,dbname,cateinfo,table_name,read_sql,keylist=keylist,plan_id=plan_id)
    if ret_dict is None:
        return
    for v in ret_dict.values():
        v.TradingDay=v.TradingDay.astype('<M8[ns]')
        v.index=v.TradingDay
    # else:
    #     tmp_ret_dict=get_fund_data_from_hdf5(fund_list,cateinfo,dbname,table_name,read_sql,keylist=keylist)
    #     if tmp_ret_dict is None:
    #         return
    #     if yield_type_pair_list is None:
    #         mark_list=[]
    #         for yield_type ,yield_sub_type in yield_type_pair_list:
    #             mark_list.append(yield_type*100+yield_sub_type)

    #         ret_dict={}
    #         for k,v in tmp_ret_dict.items():
    #             v.TradingDay=v.TradingDay.astype('<M8[ns]')
    #             v.index=v.TradingDay
    #             mark=v.YieldType*100+v.YieldSubType
    #             v=v[mark.isin(mark_list)]
    #             ret_dict[k]=v
    #     else:
    #         ret_dict= tmp_ret_dict
    return ret_dict

def get_fund_annual_statis(fund_list,frequency,dbname,cateinfo,yield_type_pair_list=None,plan_id=None,start_date=None,end_date=None):
    read_sql='(Frequency=={})'.format(frequency)
    if yield_type_pair_list is not None:
        str_sql=' | '.join(['((YieldType=={}) & (YieldSubType=={})) '.format(yield_type,yield_sub_type) for yield_type,yield_sub_type in yield_type_pair_list])
        read_sql+=' & ({}) '.format(str_sql)
    if start_date is not None:
        start_num=start_date.Year
        read_sql+=' & (Year>={}) '.format(start_num)
    if end_date is not None:
        end_num=end_date.Year
        read_sql+=' & (Year<={}) '.format(end_num)

    table_name='FundAnnualStatis4Rank'
    keylist=['Year','YieldType','YieldSubType']
    ret_dict=get_fund_data_from_hdf5(fund_list,dbname,cateinfo,table_name,read_sql,keylist,plan_id=plan_id)
    return ret_dict

def get_scene_market_statis(fund_list,frequency,dbname,cateinfo,scene_type_tuple_list=None,plan_id=None,start_date=None,end_date=None):
    read_sql='(Frequency=={})'.format(frequency)

    if start_date is not None:
        start_num=np.datetime64(start_date,'ns').astype(np.longlong)
        read_sql+=' & (TradingDay>={}) '.format(start_num)
    if end_date is not None:
        end_num=np.datetime64(end_date,'ns').astype(np.longlong)
        read_sql+=' & (TradingDay<={}) '.format(end_num)

    if scene_type_tuple_list is not None:
        str_sql=' | '.join(['((SceneType=={}) & (SceneValue=={}) & (BenchID=={})) '.format(scene_type,scene_value,bench_id) for scene_type,scene_value,bench_id in scene_type_tuple_list])
        read_sql+=' & ({}) '.format(str_sql)

    table_name='SceneMarketStatis'
    keylist=['TradingDay','SceneType','SceneValue','BenchID']
    ret_dict=get_fund_data_from_hdf5(fund_list,dbname,cateinfo,table_name,read_sql,keylist=keylist,plan_id=plan_id)
    for v in ret_dict.values():
        v.TradingDay=v.TradingDay.astype('<M8[ns]')
        v.index=v.TradingDay
    return ret_dict

def get_residual(fund_list,frequency,dbname,cateinfo,term_type_list,plan_id=None,start_date=None,end_date=None):
    read_sql='(Frequency=={})'.format(frequency)
    if start_date is not None:
        start_num=np.datetime64(start_date,'ns').astype(np.longlong)
        read_sql+=' & (TradingDay>={}) '.format(start_num)
    if end_date is not None:
        end_num=np.datetime64(end_date,'ns').astype(np.longlong)
        read_sql+=' & (TradingDay<={}) '.format(end_num)
    if term_type_list is not None:
        str_sql=' | '.join(['(TermType=={})'.format(term_type, residual_type) for term_type, residual_type in term_type_list])
        read_sql+=' & ({}) '.format(str_sql)
    table_name='Residual'
    keylist=['TradingDay','TermType']
    ret_dict=get_fund_data_from_hdf5(fund_list,dbname,cateinfo,table_name,read_sql,keylist=keylist,plan_id=plan_id)
    for v in ret_dict.values():
        v.TradingDay=v.TradingDay.astype('<M8[ns]')
        v.index=v.TradingDay
    return ret_dict

def get_fund_factor_exposure(fund_list,frequency,dbname,cateinfo,regression_pair_list=None,plan_id=None,start_date=None,end_date=None):
    read_sql='(Frequency=={}) & (FactorID==20000)'.format(frequency)
    if start_date is not None:
        start_num=np.datetime64(start_date,'ns').astype(np.longlong)
        read_sql+=' & (TradingDay>={}) '.format(start_num)
    if end_date is not None:
        end_num=np.datetime64(end_date,'ns').astype(np.longlong)
        read_sql+=' & (TradingDay<={}) '.format(end_num)

    if regression_pair_list is not None:
        str_sql=' | '.join(['(RegressionID=={}) & (RollNum=={}) '.format(regression_id,roll_num) for regression_id,roll_num in regression_pair_list])
        read_sql+=' & ({}) '.format(str_sql)

    table_name='FundFactorExposure'
    keylist=['TradingDay','RegressionID','RollNum']
    ret_dict=get_fund_data_from_hdf5(fund_list,dbname,cateinfo,table_name,read_sql,keylist=keylist,plan_id=plan_id)
    for v in ret_dict.values():
        v.TradingDay=v.TradingDay.astype('<M8[ns]')
        v.index=v.TradingDay
    return ret_dict

def get_fund_data_from_hdf5(fund_list,dbname,cateinfo,table_name,read_sql,keylist=None,plan_id=None):
    save_path='{}/{}/{}'.format(base_path,dbname,cateinfo)
    ret_dict={}
    t0=time.time()
    i=0
    j=len(fund_list)
    for fund_id in fund_list:
        i+=1
        if i%100==0:
            t1=time.time()
            print(i,j,t1-t0)
            t0=t1
        if plan_id is None:
            filename='{}/{}.h5'.format(save_path,fund_id)
        else:
            filename='{}/{}_{}.h5'.format(save_path,plan_id,fund_id)
        if not os.path.isfile(filename):
            continue
        hdf_api=Hdf5Api(filename,mode='r')
        if table_name  in hdf_api.tablename_list:
            ret=hdf_api.read_rows(table_name,read_sql)
            if ret is None or len(ret)==0:
                continue
            if keylist:
                ret.drop_duplicates(keylist,keep='last',inplace=True)
            ret_dict[fund_id]=ret
        hdf_api.close()
    return ret_dict

def get_rank_data(fund_list,frequency,dbname,cateinfo,rank_key_dict,start_date=None,end_date=None):
    data_info_dict={}
    for table_name,rank_key_list in rank_key_dict.items():
        read_sql='(Frequency=={})'.format(frequency)
        if start_date is not None:
            start_num=np.datetime64(start_date,'ns').astype(np.longlong)
            if 'Annual' in table_name:
                read_sql+=' & (Year>={}) '
            else:
                read_sql+=' & (TradingDay>={}) '.format(start_num)
        if end_date is not None:
            end_num=np.datetime64(end_date,'ns').astype(np.longlong)
            if 'Annual' in table_name:
                read_sql+=' & (Year>={}) '
            else:
                read_sql+=' & (TradingDay<={}) '.format(end_num)
        str_sql=''
        if table_name=='FundFactorExposure':
            keylist=['TradingDay','RegressionID','RollNum']
            read_sql+=' & (FactorID==20000)'.format(frequency)
            if rank_key_list is not None:
                str_sql=' | '.join(['(RegressionID=={}) & (RollNum=={}) '.format(regression_id,roll_num) for regression_id,roll_num in rank_key_list])
        elif table_name=='Residual':
            keylist=['TradingDay','TermType']
            if rank_key_list is not None:
                str_sql=' | '.join(['(TermType=={})'.format(term_type, residual_type) for term_type, residual_type in rank_key_list])
        elif table_name=='SceneMarketStatis':
            keylist=['TradingDay','SceneType','SceneValue','BenchID']
            if rank_key_list is not None:
                str_sql=' | '.join(['((SceneType=={}) & (SceneValue=={}) & (BenchID=={})) '.format(scene_type,scene_value,bench_id) for scene_type,scene_value,bench_id in rank_key_list])
        elif table_name=='FundAnnualStatis4Rank':
            keylist=['Year','YieldType','YieldSubType']
            if rank_key_list is not None:
                str_sql=' | '.join(['((YieldType=={}) & (YieldSubType=={})) '.format(yield_type,yield_sub_type) for yield_type,yield_sub_type in rank_key_list])
        elif table_name=='FundRollStatis4Rank':
            keylist=['TradingDay','YieldType','YieldSubType']
            if rank_key_list is not None:
                str_sql=' | '.join(['((YieldType=={}) & (YieldSubType=={})) '.format(yield_type,yield_sub_type) for yield_type,yield_sub_type in rank_key_list])
        elif table_name=='FundIndexCorr':
            keylist=['TradingDay','YieldType','YieldSubType','BenchID']
            if rank_key_list is not None:
                str_sql=' | '.join(['((YieldType=={}) & (YieldSubType=={}) & (BenchID=={})) '.format(yield_type,yield_sub_type,bench_id) for yield_type,yield_sub_type,bench_id in rank_key_list])
        if rank_key_list:
            read_sql+=' & ({}) '.format(str_sql)
        data_info_dict[table_name]=(read_sql,keylist)
    return get_fund_data_from_hdf52(fund_list,dbname,cateinfo,data_info_dict)

def get_fund_roll_statis_rank(dbname, fund_list, frequency, yield_type, yield_sub_type, statis_period):
    table_name = 'FundRollStatisRank'
    read_sql = '({0}) & (Frequency=={1}) & (YieldType=={2}) & (YieldSubType=={3}) & (StatisPeriod=={4})'.format(' | '.join(['(FundID=={})'.format(b) for b in fund_list]), frequency, yield_type, yield_sub_type, statis_period)
    filename = '{}/{}/{}.h5'.format(base_path,dbname)
    if not os.path.isfile(filename):
        logging.warning('filename:{} is not exist'.format(filename))
        return
    res_df=None
    hdf_api = Hdf5Api(filename, mode='r')
    if table_name in hdf_api.tablename_list:
        res_df = hdf_api.read_rows(table_name, read_sql)
    hdf_api.close()
    if res_df is not None and len(res_df)>0:
        res_df.TradingDay=res_df.TradingDay.astype('<M8[ns]')
        res_df.drop_duplicates(['TradingDay','FundID'],keep='last',inplace=True)
    return res_df

def get_fund_factor_exposure_rank(dbname, fund_list, frequency, regression_id):
    table_name = 'FundFactorExposureRank'
    read_sql = '({0}) & (Frequency=={1}) & (RegressionID=={2})'.format(' | '.join(['(FundID=={})'.format(b) for b in fund_list]), frequency, regression_id)
    filename = '{}/{}/{}.h5'.format(base_path,dbname)
    if not os.path.isfile(filename):
        logging.warning('filename:{} is not exist'.format(filename))
        return
    res_df=None
    hdf_api = Hdf5Api(filename, mode='r')
    if table_name in hdf_api.tablename_list:
        res_df = hdf_api.read_rows(table_name, read_sql)
    hdf_api.close()
    if res_df is not None and len(res_df)>0:
        res_df.TradingDay=res_df.TradingDay.astype('<M8[ns]')
        res_df.drop_duplicates(['TradingDay','FundID'],keep='last',inplace=True)
    return res_df

def get_scene_statis_rank(dbname, fund_list, frequency, scene_type_dict, folder_name):
    table_name = 'SceneStatisRank'
    # scene_type_condition = ' or '.join(['(scenetype ={} and scenevalue in ({}))'.format(a, str(scene_type_dict[a])[1:-1]) for a in scene_type_dict])
    # read_sql = '({0}) & (Frequency=={1}) & ({2})'.format(' | '.join(['(FundID=={})'.format(b) for b in fund_list]), frequency, scene_type_condition)

    path = '{}/{}/{}'.format(base_path,dbname, folder_name)

    res_list = []
    for file_name in os.listdir(path):
        key = 'Rank_SceneMarketStatis'
        if file_name.startswith(key):
            filename = path + '/' + file_name
            res_df=None
            hdf_api = Hdf5Api(filename, mode='r')
            if table_name in hdf_api.tablename_list:
                res_df = hdf_api.read_rows(table_name)
            hdf_api.close()
            if res_df is not None and len(res_df)>0:
                res_df.TradingDay=res_df.TradingDay.astype('<M8[ns]')
                res_df.drop_duplicates(['TradingDay','FundID', 'SceneType', 'SceneValue'],keep='last',inplace=True)
            res_list.append(res_df)

    if res_list:
        res_df = pd.concat(res_list)
        res_df = res_df[res_df.FundID.isin(fund_list)]
        res_df = res_df[res_df.SceneType.isin(list(scene_type_dict.keys()))]

        res_list = []
        for scene_type, scene_value_list in scene_type_dict.items():
            tmp = res_df[(res_df.SceneType == scene_type) & (res_df.SceneValue.isin(scene_value_list))]
            res_list.append(tmp)
        if res_list:
            return pd.concat(res_list)

def get_fund_cta_rank(dbname, fund_list, frequency, folder_name):
    table_name = 'FundCTARank'
    path = '{}/{}/{}'.format(base_path,dbname, folder_name)

    res_list = []
    for file_name in os.listdir(path):
        key = 'Rank_Residual'
        if file_name.startswith(key):
            filename = path + '/' + file_name
            res_df=None
            hdf_api = Hdf5Api(filename, mode='r')
            if table_name in hdf_api.tablename_list:
                res_df = hdf_api.read_rows(table_name)
            hdf_api.close()
            if res_df is not None and len(res_df)>0:
                res_df.TradingDay=res_df.TradingDay.astype('<M8[ns]')
                res_df.drop_duplicates(['TradingDay','FundID', 'TermType'],keep='last',inplace=True)
            res_list.append(res_df)

    if res_list:
        res_df = pd.concat(res_list)
        res_df = res_df[res_df.FundID.isin(fund_list)]
        return res_df


def get_fund_data_from_hdf52(fund_list,dbname,cateinfo,data_info_dict):
    save_path='{}/{}/{}'.format(base_path,dbname,cateinfo)
    ret_dict={}
    for k in data_info_dict.keys():
        ret_dict[k]={}
    t0=time.time()
    i=0
    j=len(fund_list)
    for fund_id in fund_list:
        i+=1
        if i%100==0:
            t1=time.time()
            print(i,j,t1-t0)
            t0=t1
        filename='{}/{}.h5'.format(save_path,fund_id)
        if not os.path.isfile(filename):
            continue
        hdf_api=Hdf5Api(filename,mode='r')

        for table_name ,query_info in data_info_dict.items():
            if table_name  in hdf_api.tablename_list:
                read_sql,keylist=query_info
                ret=hdf_api.read_rows(table_name,read_sql)
                if ret is None or len(ret)==0:
                    continue
                if keylist:
                    ret.drop_duplicates(keylist,keep='last',inplace=True)
                if 'TradingDay' in ret:
                    ret.TradingDay=ret.TradingDay.astype('<M8[ns]')
                    ret.index=ret.TradingDay
                ret_dict[table_name][fund_id]=ret
        hdf_api.close()
    return ret_dict

def get_data_from_hdf5(dbname,table_name,read_sql,keylist=None):
    save_path='{}/{}'.format(base_path,dbname)
    filename='{}/{}.h5'.format(save_path,dbname)
    if not os.path.isfile(filename):
        return
    hdf_api=Hdf5Api(filename,mode='r')
    if table_name  in hdf_api.tablename_list:
        ret=hdf_api.read_rows(table_name,read_sql)
        if keylist:
            ret.drop_duplicates(keylist,keep='last',inplace=True)
        hdf_api.close()
    return ret

def get_db_update_time(dbname, filename):
    file_path = '{}/{}/{}'.format(base_path,dbname, filename)
    return os.path.getmtime(file_path)

def get_public_db_update_time():
    return get_db_update_time('DDerive', 'DDerive.h5')


#############################################################################################################################
def get_hdf_rank_data(hdf_dir_path,hdf_path,source_table):
    '''
    :args
        hdf_path：具体的h5文件的路径
        source_table:hdf里面的表名
    '''

    h5_path = f'{hdf_dir_path}/{hdf_path}'
    hdf_operator = Hdf5Api(h5_path, mode='r')

    table_list = hdf_operator.get_table_name_list()
    first_table_name = table_list[0]

    hdf_df = hdf_operator.read_rows(first_table_name)
    hdf_df.TradingDay = hdf_df.TradingDay.astype('<M8[ns]',)
    hdf_operator.close()

    return hdf_df

def get_all_rank_data(hdf_dir_path,group_id,table_name_list,fund_list=None,end_date=None,frequency=None):
    dir_list  = os.listdir(hdf_dir_path)
    res_dict ={}
    for table_name in table_name_list:
        name = 'Rank_' + str(table_name)+'_'+'GroupID'+'_'+str(group_id)
        path_list = [h5_path for h5_path in dir_list if name in h5_path]
        if end_date:
            end_date_str = str(end_date.date())
            path_list = [h5_path for h5_path in path_list if end_date_str in h5_path]
        df_list=[]
        for h5_path in path_list:
            df = get_hdf_rank_data(hdf_dir_path,h5_path,table_name)
            if fund_list:
                df = df[df.FundID.isin(fund_list)]
            if frequency:
                df = df[df.Frequency == frequency]
            df_list.append(df)
        df_all=pd.concat(df_list)
        df_all.sortvalue()
        res_dict[table_name]=df_all
    return res_dict

def get_bond_factor_value(step_regression_params, alpha_map, frequency=1,start_date=None,end_date=None):
    global std_df_x_all
    import DBCfg
    try:
        import Config
    except:
        import PublicConfig as Config
    dbinfo = DBCfg.get_dbcfg(Config.public_db_address, 'EBTR')
    g = GetData(dbinfo)
    bench_list = step_regression_params['factor_list_group'][0]+[10029]
    factor_list = []
    for factors in step_regression_params['factor_list_group'][1:]:
        factor_list+=factors
    for style_factors in alpha_map.values():
        factor_list+=style_factors.keys()
    if start_date is None:
        start_date=datetime(2013,1,1)
    if bench_list:
        df_x1=g.get_df('''select BenchID,TradingDay,Yield from DDerive..benchmarkyield where benchid in ({}) and yieldtype=1 and yieldsubtype=1 and period={} and TradingDay>'{}'  order by TradingDay'''.format(str(bench_list)[1:-1],frequency,start_date.strftime('%Y-%m-%d')))
        df_x1=df_x1.pivot(index='TradingDay',columns='BenchID',values='Yield')
    if factor_list:
        if frequency==1:
            df_x2=g.get_df('''select * from BondfactorValue where factorid in ({})  and CalendarDay>'{}' and valuetype=5 order by factorid,CalendarDay'''.format(str(factor_list)[1:-1],start_date.strftime('%Y-%m-%d')))
        else:
            df_x2=g.get_df('''select * from BondfactorValue_frequency where frequency=2 and factorid in ({})  and CalendarDay>'{}' and valuetype=5 order by factorid,CalendarDay'''.format(str(factor_list)[1:-1],start_date.strftime('%Y-%m-%d')))
        df_x2=df_x2.pivot(index='CalendarDay',columns='FactorID',values='FactorValue')
        std_df_x_all = pd.concat([df_x1, df_x2],axis=1)
    else:
        std_df_x_all = df_x1
    std_df_x_all.index.name = 'TradingDay'
    std_df_x_all.fillna(0,inplace=True)

    return std_df_x_all
