import os,datetime as dt
import pandas as pd,numpy as np
import flare_cores as cores


bench_mark_yield_df=None
bench_mark_yield_df_weekly=None
bench_mark_closeprice_df=None


def calc_stock_asset_ratio(fund_yield_series,frequency,label=None,**kwargs):
    bench_id_list_all=[10006,10029,10196,22506,10024, 10025, 10026, 10027, 10028, 10075,10016, 10017, 10018, 10019, 10020, 10021,23188,23189,23190,23191,23192,23193,10000,10002,10004,22931,23628]
    bench_df_all=get_bench_mark_yield(bench_id_list_all,frequency=frequency)
    
    fund_yield_series,bench_df_all=cores.rematch(fund_yield_series,bench_df_all)
    if frequency==0: #原始净值,要先净值对齐,再求收益率
        fund_yield_series=fund_yield_series.pct_change().fillna(0)
        bench_df_all=bench_df_all.pct_change().fillna(0)
    roll_num=kwargs.get('roll_num')
    if roll_num is None:
        if frequency==0:
            roll_num=int(len(fund_yield_series)/3/((fund_yield_series.index[-1]-fund_yield_series.index[0]).days/365))
        else:
            roll_num=90 if frequency==1 else 18 
    weight_half_life=None #=4 if frequency==2 else 8 
    ret_list=[]
    if label in (1,2,3):
        if label==3:
            stock_beta=1.0
            cash_beta=0.0
        elif label==1:
            stock_beta=0.95
            cash_beta=0.05
        else:
            stock_beta=0.93
            cash_beta=0.07
        asset_weight=pd.DataFrame({10006:stock_beta,10029:0,10196:cash_beta},index=fund_yield_series.index)
    else:
        if label==4:
            bench_id_list_scale=[10000,10002,10004,22931,23628]
            bench_id_list_asset=[10006,10029,10196,22506]
            bench_id_list_industry=[10024,10025,10026, 10027, 10028, 10075,23188,23189,23190,23191,23192,23193]
            bench_id_list_style=[10016, 10017, 10018, 10019, 10020, 10021]
        else:
            bench_id_list_scale=[10000,10002,10004,22931,23628]
            bench_id_list_asset=[10006,10029,10196]
            bench_id_list_industry=[10024, 10025, 10026, 10027, 10028, 10075]
            bench_id_list_style=[10016, 10017, 10018, 10019, 10020, 10021]

        
        bench_df_asset=bench_df_all[bench_id_list_asset]
        asset_weight = cores.calc_fund_asset_ratio(fund_yield_series, bench_df_asset,roll_num,tol=1e-4)
        if asset_weight is None:
            return 
        if label==4:
            asset_weight[10006]=asset_weight[10006]+asset_weight[22506]
            asset_weight=asset_weight.drop(columns=[22506])
    ret_list.append(asset_weight)
    yield_stock_series=cores.calc_normalize_yield(asset_weight, bench_df_asset, fund_yield_series, 10006, 1)['Yield'].dropna()
    bench_df_industry=bench_df_all[bench_id_list_industry]
    sub_yield_stock_series,bench_df_industry = cores.rematch(yield_stock_series,bench_df_industry)
    industry_weight = cores.calc_fund_asset_ratio_new(sub_yield_stock_series, bench_df_industry,roll_num,tol=1e-8)
    if industry_weight is None:
        industry_weight=pd.DataFrame(columns=bench_id_list_industry,index=sub_yield_stock_series.index,dtype=np.float64)
    if label==4:
        industry_weight.loc[:,[10024, 10025, 10026, 10027, 10028, 10075]]+=industry_weight[[23188,23189,23190,23191,23192,23193]].values
        industry_weight=industry_weight.drop(columns=[23188,23189,23190,23191,23192,23193])
    ret_list.append(industry_weight)
    
    bench_df_style=bench_df_all[bench_id_list_style]
    sub_yield_stock_series,bench_df_style = cores.rematch(yield_stock_series,bench_df_style)
    style_weight = cores.calc_fund_asset_ratio_new(sub_yield_stock_series,bench_df_style,roll_num,tol=1e-8)
    if style_weight is None:
        style_weight=pd.DataFrame(columns=bench_id_list_style,index=sub_yield_stock_series.index,dtype=np.float64)
    ret_list.append(style_weight)

    bench_df_scale=bench_df_all[bench_id_list_scale]
    bench_df_scale=bench_df_scale.dropna(axis='columns',how='any')
    sub_yield_stock_series,bench_df_scale = cores.rematch(yield_stock_series,bench_df_scale)
    scale_weight = cores.calc_fund_asset_ratio_new(sub_yield_stock_series, bench_df_scale,roll_num,tol=1e-4)
    if scale_weight is not None:
        ret_list.append(scale_weight)
    else:
        scale_weight=pd.DataFrame(columns=bench_id_list_scale,index=sub_yield_stock_series.index,dtype=np.float64)
        
    if ret_list:
        ret_df=pd.concat(ret_list,axis=1)
        ret_df[ret_df.abs()<1e-5]=0
        return ret_df
    
def calc_neutral_asset_ratio(fund_yield_series,frequency,**kwargs):
    ret_list=[]
    bench_id_list_all=[10006,10024, 10025, 10026, 10027, 10028, 10075,10016, 10017, 10018, 10019, 10020, 10021,10000,10002,10004,22931,23628]
    bench_df_all=get_bench_mark_yield(bench_id_list_all,frequency=frequency)
    fund_yield_series,bench_df_all=cores.rematch(fund_yield_series,bench_df_all)
    if frequency==0: #原始净值,要先净值对齐,再求收益率
        fund_yield_series=fund_yield_series.pct_change().fillna(0)
        bench_df_all=bench_df_all.pct_change().fillna(0)
    roll_num=kwargs.get('roll_num')
    if roll_num is None:
        if frequency==0:
            roll_num=int(len(fund_yield_series)/3/((fund_yield_series.index[-1]-fund_yield_series.index[0]).days/365))
        else:
            roll_num=90 if frequency==1 else 18  
    bench_id_list_industry=[10024, 10025, 10026, 10027, 10028, 10075]
    bench_df_industry=bench_df_all[bench_id_list_industry].sub(bench_df_all[10006],axis=0) #超额
    industry_weight = cores.calc_fund_asset_ratio_new(fund_yield_series, bench_df_industry,roll_num,tol=1e-8)
    if industry_weight is None:
        return 
    ret_list.append(industry_weight)
    bench_id_list_style=[10016, 10017, 10018, 10019, 10020, 10021]
    bench_df_style=bench_df_all[bench_id_list_style].sub(bench_df_all[10006],axis=0)  #超额
    style_weight = cores.calc_fund_asset_ratio_new(fund_yield_series,bench_df_style,roll_num,tol=1e-8)
    if style_weight is None:
        style_weight=pd.DataFrame(columns=bench_id_list_style,index=fund_yield_series.index,dtype=np.float64)
    ret_list.append(style_weight)

    bench_id_list_scale=[10000,10002,10004,22931,23628]
    bench_df_scale=bench_df_all[bench_id_list_scale].sub(bench_df_all[10006],axis=0)

    bench_df_scale=bench_df_scale.dropna(axis='columns',how='any')
    fund_yield_series,bench_df_scale = cores.rematch(fund_yield_series,bench_df_scale)
    scale_weight = cores.calc_fund_asset_ratio_new(fund_yield_series, bench_df_scale,roll_num,tol=1e-4)
    if scale_weight is not None:
        ret_list.append(scale_weight)
    else:
        scale_weight=pd.DataFrame(columns=bench_id_list_scale,index=fund_yield_series.index,dtype=np.float64)

    if ret_list:
        ret_df=pd.concat(ret_list,axis=1)
        ret_df[ret_df.abs()<1e-5]=0
        ret_df[10006]=1.0
        ret_df[10029]=0.0
        ret_df[10196]=0.0
        return ret_df

def calc_cta_asset_ratio(fund_yield_series,frequency,**kwargs):
    bench_id_list_asset=[10054, 10055, 10056, 10196]
    bench_id_list_style=[10057, 10058, 10059, 10060]
    bench_id_list_asset1=[10925, 10930, 10924, 10923,10928,10166,10125]
    bench_id_list_style1=[23609,23610,23611,23612,23613,23614,23615,23616,23617,23618,23619,23620,23621,23622]
    ret_list=[]
    bench_df_all=get_bench_mark_yield(bench_id_list_asset+bench_id_list_style+bench_id_list_asset1+bench_id_list_style1,frequency=frequency)
    fund_yield_series,bench_df_all=cores.rematch(fund_yield_series,bench_df_all)
    if frequency==0: #原始净值,要先净值对齐,再求收益率
        fund_yield_series=fund_yield_series.pct_change().fillna(0)
        bench_df_all=bench_df_all.pct_change().fillna(0)
    roll_num=kwargs.get('roll_num')
    if roll_num is None:
        if frequency==0:
            roll_num=int(len(fund_yield_series)/3/((fund_yield_series.index[-1]-fund_yield_series.index[0]).days/365))
        else:
            roll_num=90 if frequency==1 else 18 
    bench_df_asset=bench_df_all[bench_id_list_asset]
    bounds=[(-10,10)]*3+[(0,1.0)]
    asset_weight = cores.calc_fund_asset_ratio(fund_yield_series, bench_df_asset,roll_num,bounds=bounds,tol=1e-8,constraints=0,abs_normalize=True)

    if asset_weight is None:
        return 
    ret_list.append(asset_weight)
    bench_df_style=bench_df_all[bench_id_list_style]
    bounds=[(-10,10)]*4
    style_weight = cores.calc_fund_asset_ratio_new(fund_yield_series,bench_df_style,roll_num,bounds=bounds,tol=1e-8,constraints=0,abs_normalize=True)
    if style_weight is None:
        style_weight=pd.DataFrame(columns=bench_id_list_style,index=fund_yield_series.index,dtype=np.float64)
    ret_list.append(style_weight)

    bench_df_asset1=bench_df_all[bench_id_list_asset1]
    
    asset_weight1 = bench_df_asset1.rolling(roll_num).corr(fund_yield_series)
    if asset_weight1 is None:
        asset_weight1=pd.DataFrame(columns=bench_id_list_asset1,index=fund_yield_series.index,dtype=np.float64)
    ret_list.append(asset_weight1)

    bench_df_style1=bench_df_all[bench_id_list_style1]
    style_weight1 = bench_df_style1.rolling(roll_num).corr(fund_yield_series)
    if style_weight1 is None:
        style_weight1=pd.DataFrame(columns=bench_id_list_style1,index=fund_yield_series.index,dtype=np.float64)
    ret_list.append(style_weight1)

    if ret_list:
        ret_df=pd.concat(ret_list,axis=1)
        ret_df[ret_df.abs()<1e-5]=0
        return ret_df
    
    
def calc_bond_asset_ratio(fund_yield_series,frequency,**kwargs):
    roll_num=63 if frequency==1 else 26
    p_thred=0.15
    thd_vif_resquared=0.9
    beta_sum_limit=10
    # [22117,22194]  [22218, 22251],[22299, 22301],[22298, 22304],     [22117, 22128],
    factor_list_group = [
                                    [10006, 10036],
                                    [22117, 22128],
                                    [22161, 22137],
                                    [22172, 22150],
                                    [22260, 22183],
                                    [22194, 22271],
                                    [22227, 22205],
                                    [22282, 22238],
                                    [22216, 22249],
                                    [22118, 22129],
                                    [22162, 22140],
                                    [22173, 22151],
                                    [22261, 22184],
                                    [22195, 22272],
                                    [22228, 22206],
                                    [22283, 22239],
                                    [22217, 22250],
                                    [22119, 22130],
                                    [22163, 22141],
                                    [22174, 22152],
                                    [22262, 22196],
                                    [22273, 22185],
                                    [22229, 22207],
                                    [22284, 22240],
                                    [22218, 22251],
                                    [22293, 22295],
                                    [22297, 22303],
                                    [22299, 22301],
                                    [22294, 22296],
                                    [22298, 22304],
                                    [22300, 22302],
                                ]
    exposure_map = {
                    5:{ 10126:[22312,22117],# 1年以下
                        10127:[ 22183,22161,22172,22282,22260,22271,22150,22128,22139,22249,22227,22238,22216,22194,22205],#1年
                        10128:[ 22184,22162,22163,22173,22283,22261,22262,22272,22151,22129,22140,22250,22228,22239,22118,22217],#2年
                        10129:[ 22137,22185,22174,22284,22273,22152,22130,22141,22251,22229,22240,22119,22218,22195,22196,22206,22207],#3年
                        10130:[ 22317,22297,22307,22323,22303,22313,22315,22295,22305,22321,22301,22311,22293,22319,22299,22309],#4-6年 10126,10127,10128,10129,10130,10131
                        10131:[ 22318,22298,22308,22324,22304,22314,22316,22296,22306,22322,22302,22312,22294,22320,22300,22310],#7年及以上
                        },
                    6:{ 10163:[ 22302,22172,22173,22174,22307,22308,22271,22272,22273,22313,22314,22139,22140,22141,22305,22306,22238,
                                22239,22240,22311,22312,22205,22206,22207,22309,22310,#]AA+
                            22161,22162,22163,22297,22298,22260,22261,22262,22303,22304,22128,22129,22130,22295,22296,22227,
                                22228,22229,22301,22032,22194,22195,22196,22299,22300],#AA
                        10164:[ 22137,22183,22184,22185,22317,22318,22282,22283,22284,22323,22324,22150,22151,22152,22315,22316,22249,
                                22250,22251,22321,22322,22216,22217,22218,22319,22320],#AA-
                        10162:[ 22117,22312,22118,22119,22293,22294]#Rate
                        },
                    38:{22390:[ 22117,22312,22118,22119,22293,22294],
                        22341:[22137,22183,22184,22185,22317,22318,22161,22162,22163,22297,22298,22172,22173,22174,22307,22308],
                        22344:[ 22282,22283,22284,22323,22324,22260,22261,22262,22303,22304,22271,22272,22273,22313,22314],
                        22342:[ 22216,22217,22218,22319,22320,22194,22195,22196,22299,22300,22205,22206,22207,22309,22310],
                        22340:[ 22150,22151,22152,22315,22316,22128,22129,22130,22295,22296,22139,22140,22141,22305,22306],
                        22343:[ 22249,22250,22251,22321,22322,22227,22228,22229,22301,22302,22238,22239,22240,22311,22312]
                        }} 
    bench_df_all=get_bench_mark_yield([f for fg in factor_list_group for f in fg],frequency=frequency)
    fund_yield_series,bench_df_all=cores.rematch(fund_yield_series,bench_df_all)
    if len(fund_yield_series)<2:
        return
    if frequency==0: #原始净值,要先净值对齐,再求收益率
        fund_yield_series=fund_yield_series.pct_change().fillna(0)
        bench_df_all=bench_df_all.pct_change().fillna(0)
    roll_num=kwargs.get('roll_num')
    if roll_num is None:
        if frequency==0 :
            roll_num=int(len(fund_yield_series)/2/((fund_yield_series.index[-1]-fund_yield_series.index[0]).days/365))
        else:
            roll_num=63 if frequency==1 else 26 
    ret = cores.double_step_regression(fund_yield_series, bench_df_all, factor_list_group, roll_num, beta_sum_limit,
                                               p_thred, thd_vif_resquared, True)
    if ret is None:
        return 
    ret.Beta[np.abs(ret.Beta)<1e-5]=0
    new_ret=ret.pivot(index='TradingDay',columns='ExposureSubType',values='Beta')
    new_ret.fillna(0,inplace=True)
    if 20000 in new_ret:
        new_ret.drop(columns=20000,inplace=True)
    asset_weight=pd.DataFrame(index=new_ret.index)
    for bid in [10006,10036]:
        if bid in new_ret:
            asset_weight[bid]=new_ret[bid]
            new_ret.drop(columns=bid,inplace=True)
        else:
            asset_weight[bid]=0.0
    asset_weight[10029]=new_ret.sum(axis=1)
    new_ret=new_ret.div(asset_weight[10029],axis=0) #归一化
    asset_weight[10196]=1-asset_weight[10006]-asset_weight[10029]-asset_weight[10036]
    asset_weight.loc[asset_weight[10196]<0,10196]=0.0
    asset_weight=asset_weight.div(asset_weight.sum(axis=1),axis=0)  #归一化
    for exposure_type ,map_dict in exposure_map.items():
        for k,v in map_dict.items():
            asset_weight[k]=new_ret[new_ret.columns[new_ret.columns.isin(v)]].sum(axis=1)

    return asset_weight

def calc_mix_asset_ratio(fund_yield_series,frequency,begin_date,end_date):
    ret_df = pd.DataFrame()
    stock_df = calc_stock_asset_ratio(fund_yield_series, frequency,begin_date,end_date)
    if len(stock_df) > 0:
        ret_df = stock_df[[i for i in stock_df.columns.tolist() if i not in [10006,10029,10196]]]
    ret_bond = calc_bond_asset_ratio(fund_yield_series, frequency,begin_date,end_date)
    if len(ret_df)>0:
        ret_df = pd.concat([ret_df,ret_bond],axis=1)
    else:
        ret_df = ret_bond
    return ret_df
        
def get_fund_asset_ratio(fund_net_value_series,strategy_type=None,frequency=1,begin_date=None,end_date=None,label=None,family_type=None):
    strategy2family_map={1:1,4:2,5:3,3:10}
    if family_type is None:
        family_type=strategy2family_map[strategy_type]
    if frequency>0:
        fund_yield_series=fund_net_value_series.pct_change()
    else:
        fund_yield_series=fund_net_value_series
    if begin_date is not None:
        fund_yield_series=fund_yield_series[fund_yield_series.index>=pd.to_datetime(begin_date)]
    if end_date is not None:
        fund_yield_series=fund_yield_series[fund_yield_series.index<=pd.to_datetime(end_date)]
    if family_type==1:
        ret=calc_stock_asset_ratio(fund_yield_series,frequency,label)
    elif family_type==10:
        ret=calc_neutral_asset_ratio(fund_yield_series,frequency)
    elif family_type==2:
        ret=calc_bond_asset_ratio(fund_yield_series,frequency)
    elif family_type==3:
        ret=calc_cta_asset_ratio(fund_yield_series,frequency)
    elif family_type == 4:
        ret = calc_mix_asset_ratio(fund_yield_series, frequency)
    else:
        raise Exception(f'strategy_type {strategy_type} wrong ')
    return ret
         
def get_bench_mark_yield(bench_id_list=None,frequency=1):
    if bench_mark_yield_df is None:
        __check__()
    if bench_id_list is None:
        if frequency==1:
            return bench_mark_yield_df
        elif frequency==2:
            return bench_mark_yield_df_weekly
        elif frequency==0:
            return bench_mark_closeprice_df
    else:
        blist=[b for b in bench_id_list if b in bench_mark_yield_df.columns]
        if frequency==1:
            tmp_df=bench_mark_yield_df[blist]
        elif frequency==2:
            tmp_df=bench_mark_yield_df_weekly[blist]
        elif frequency==0:
            tmp_df=bench_mark_closeprice_df[blist]
        return tmp_df

def fill_nan_after_first_valid(df,v=0):
    seen = df.notna().cumsum().gt(0)          # 每列从第一个非NaN开始为 True
    df2 = df.mask(seen & df.isna(), v)        # 只把这个区间内的 NaN 改成 0   
    return df2
     
def __check__():
    global bench_mark_yield_df,bench_mark_yield_df_weekly,bench_mark_closeprice_df
    bench_mark_closeprice_df=pd.read_csv('bench_mark_closeprice_df.csv') 
    bench_mark_closeprice_df['TradingDay']=pd.to_datetime(bench_mark_closeprice_df['TradingDay'])
    bench_mark_closeprice_df.set_index('TradingDay',inplace=True)
    bench_mark_closeprice_df.columns=bench_mark_closeprice_df.columns.astype(int)


    bench_mark_yield_df=pd.read_csv('bench_mark_yield_df.csv') 
    bench_mark_yield_df['TradingDay']=pd.to_datetime(bench_mark_yield_df['TradingDay'])
    bench_mark_yield_df.set_index('TradingDay',inplace=True)
    bench_mark_yield_df.columns=bench_mark_yield_df.columns.astype(int)

    bench_mark_yield_df_weekly=pd.read_csv('bench_mark_yield_df_weekly.csv') 
    bench_mark_yield_df_weekly['TradingDay']=pd.to_datetime(bench_mark_yield_df_weekly['TradingDay'])
    bench_mark_yield_df_weekly.set_index('TradingDay',inplace=True)
    bench_mark_yield_df_weekly.columns=bench_mark_yield_df_weekly.columns.astype(int)

    return {'bench_mark_closeprice_df':bench_mark_closeprice_df,
            'bench_mark_yield_df':bench_mark_yield_df,
            'bench_mark_yield_df_weekly':bench_mark_yield_df_weekly}

def update_globals():
    return __check__()

# __check__()




    