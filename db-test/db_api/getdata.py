# -*- coding: utf-8 -*-

import requests
from tables.path import join_path
import flare_cores as cores
from pycloud.dbsynchelper import dbsynchelper
import pandas as pd
from pycloud.LOG  import  LOG
from datetime import timedelta
import numpy as np
import time
import traceback
import json
import datetime
import pkg_resources
import asyncio


dbhelper_dict={}

def get_db_helper(db_address=None,db_name='Flare-PM'):
    global dbhelper_dict
    import DBCfg
    try:
        import Config
    except:
        import PublicConfig as Config
    if db_address is None:
        db_address=Config.source_db_address
    key=f'{db_address}_{db_name}'
    dbhelper=dbhelper_dict.get(key)
    if dbhelper is None: 
        dbinfo = DBCfg.get_dbcfg(db_address, db_name)
        dbhelper=dbsynchelper()
        dbhelper.SetDBCfg(dbinfo)
        dbhelper_dict[key]=dbhelper
    return dbhelper

class EurekaRequest(object):
    def __init__(self):
        from Task.real_time.cfg.apollo.get_apollo import get_apollo_config
        self.wisdomdb_url = get_apollo_config('eureka_wisdomdb_service_url')
        self.wisdomdb_app_name = get_apollo_config('eureka_wisdomdb_service_name')

    # 获取远程eureka服务数据通用接口
    def get_eukera_data(self, dic, server_url, app_name, url, is_df=True):
        url = '/' + app_name + url
        if isinstance(dic, str):
            dic = eval(dic)
        data = json.dumps(dic,cls=MyEncoder)

        headers = {"Content-Type": "application/json"}

        version = pkg_resources.get_distribution("py-eureka-client").version
        if version in set(['0.11.7', '0.11.8']):
            import nest_asyncio
            nest_asyncio.apply()
            # 运行协程
            res = asyncio.run(self.async_get_eukera_data(dic, server_url, app_name, url, is_df))
            return res
        else:
            from py_eureka_client.eureka_client import EurekaClient as eureka_client
            client = eureka_client(eureka_server=server_url, app_name=app_name, should_register=False)
            client.start()
            res = client.do_service(app_name=app_name, service=url,
                                    method='POST',
                                    data=data.encode(),
                                    headers=headers,
                                    timeout=500,
                                    return_type='json')
            client.stop()
            if res['code'] == 0:
                if is_df:
                    try:
                        df_res = pd.DataFrame(data=res['data'], columns=res['columns'])
                    except:
                        df_res = pd.DataFrame(res['data'])
                    # df_res['trading_day'] = pd.to_datetime(df_res['trading_day'])
                    # df_res[df_res.values == ''] = np.nan
                    return df_res
                else:
                    return res
            else:
                return None

    async def async_get_eukera_data(self, dic, server_url, app_name, url, is_df=True):
        try:

            if isinstance(dic, str):
                dic = eval(dic)
            data = json.dumps(dic,cls=MyEncoder)

            headers = {"Content-Type": "application/json"}
            from py_eureka_client.eureka_client import EurekaClient as eureka_client
            client = eureka_client(eureka_server=server_url, app_name=app_name,
                                   should_register=False)
            await client.start()

            res = await client.do_service(app_name=app_name, service=url,
                                          method='POST',
                                          data=data.encode(),
                                          headers=headers,
                                          timeout=500,
                                          return_type='json')
            await client.stop()

            if res['code'] == 0:
                if is_df:
                    try:
                        df_res = pd.DataFrame(data=res['data'], columns=res['columns'])
                    except:
                        df_res = pd.DataFrame(res['data'])
                    return df_res
                else:
                    return res
            else:
                return None

        except Exception as err:
            print(err)
    # 获取远程eureka服务数据通用接口
    def get_eukera_data_dict(self, server_url, app_name, dic, url, is_data=True, user_id='',x_mars_token=None):
        try:
            if isinstance(dic, str):
                dic = eval(dic)
            data = json.dumps(dic,cls=MyEncoder)
            if x_mars_token is not None:
                headers = {"Content-Type": "application/json", "x-wechat-openid": user_id,"x-mars-token": x_mars_token}
            else:
                headers = {"Content-Type": "application/json", "x-wechat-openid": user_id}

            version = pkg_resources.get_distribution("py-eureka-client").version
            if version in set(['0.11.7', '0.11.8']):
                import nest_asyncio
                nest_asyncio.apply()
                # 运行协程
                res = asyncio.run(self.async_get_eukera_data_dict(server_url, app_name, url, data, user_id=user_id,x_mars_token=x_mars_token,is_data=is_data))
                return res
            else:
                url = '/' + app_name + url
                from py_eureka_client.eureka_client import EurekaClient as eureka_client
                client = eureka_client(eureka_server=server_url, app_name=app_name, should_register=False)
                client.start()
                res = client.do_service(app_name=app_name, service=url,
                                        method='POST',
                                        data=data.encode(),
                                        headers=headers,
                                        timeout=500,
                                        return_type='json')
                client.stop()
                if res['code'] == 0:
                    if is_data:
                        res = res['data']
                        return res
                    else:
                        return res
                else:
                    return None
        except Exception as err:
            traceback.print_exc()
            # raise err

    async def async_get_eukera_data_dict(self, server_url, app_name, url, dic, is_data=True, user_id='',x_mars_token=None):
        try:

            url = '/' + app_name + url

            if x_mars_token is not None:
                headers = {"Content-Type": "application/json", "x-wechat-openid": user_id, "x-mars-token": x_mars_token}
            else:
                headers = {"Content-Type": "application/json", "x-wechat-openid": user_id}
            from py_eureka_client.eureka_client import EurekaClient as eureka_client
            client = eureka_client(eureka_server=server_url, app_name=app_name,
                                   should_register=False)
            await client.start()
            res = await client.do_service(app_name=app_name, service=url,
                                          method='POST',
                                          data=dic.encode(),
                                          headers=headers,
                                          timeout=500,
                                          return_type='json')
            await client.stop()
            if res['code'] == 0:
                if is_data:
                    res = res['data']
                    return res
                else:
                    return res
            else:
                return None
        except Exception as err:
            print('请求远程接口报错：{0}'.format((server_url, app_name, dic, url)))
            traceback.print_exc()

    def get_eukera_data_dict_get(self, server_url, app_name, url, params, code=False):
        headers = {"Content-Type": "application/json"}
        version = pkg_resources.get_distribution("py-eureka-client").version
        if version in set(['0.11.7', '0.11.8']):
            import nest_asyncio
            nest_asyncio.apply()
            # 运行协程
            res = asyncio.run(self.async_get_eukera_data_dict_get(server_url, app_name, url, params, code))
            return res
        else:
            url = '/' + app_name + url + params
            from py_eureka_client.eureka_client import EurekaClient as eureka_client
            client = eureka_client(eureka_server=server_url, app_name=app_name, should_register=False)
            client.start()
            res = client.do_service(app_name=app_name, service=url,
                                    method='POST',
                                    headers=headers,
                                    timeout=500,
                                    return_type='json')
            client.stop()
            if code:
                if res['code'] == 0:
                    return res
                else:
                    return None
            else:
                return res

    async def async_get_eukera_data_dict_get(self, server_url, app_name, url, params, code=False):
        try:
            url = '/' + app_name + url + params
            headers = {"Content-Type": "application/json",
                       'X-Mars-Token': '87819329-a80c-4a77-9132-5d87f50b6d88740'
                       }
            from py_eureka_client.eureka_client import EurekaClient as eureka_client
            client = eureka_client(eureka_server=server_url, app_name=app_name,
                                   should_register=False)
            await client.start()
            res = await client.do_service(app_name=app_name, service=url,
                                          headers=headers,
                                          timeout=500,
                                          return_type='json')
            await client.stop()
            if code:
                if res['code'] == 0:
                    return res
                else:
                    return None
            else:
                return res
        except Exception as err:
            print('请求远程接口报错：{0}'.format((server_url, app_name, url)))
            traceback.print_exc()

    def call_wd_api(self, cmd):
        headers = {"Content-Type": "application/json"}
        version = pkg_resources.get_distribution("py-eureka-client").version
        if version in set(['0.11.7', '0.11.8']):
            import nest_asyncio
            nest_asyncio.apply()
            # 运行协程
            res = asyncio.run(self.async_call_wd_api(cmd))
            return res
        else:
            from py_eureka_client.eureka_client import EurekaClient as eureka_client
            client = eureka_client(eureka_server=self.wisdomdb_url, app_name='getwddata', should_register=False)
            client.start()
            res = client.do_service(app_name='getwddata', service='/windExecutor',
                                    method='GET',
                                    data=cmd.encode(),
                                    timeout=500)
            client.stop()
            dic = json.loads(res)
            code = dic['CODE']
            if code == 0:
                data = dic['DATA']
                res_js = json.loads(data)
                return res_js

    async def async_call_wd_api(self, cmd):
        # if eureka_client.get_registry_client() is None:
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        client = eureka_client(eureka_server=self.wisdomdb_url, app_name='getwddata',
                               should_register=False)
        await client.start()
        res = await client.do_service(app_name='getwddata', service='/windExecutor',
                                      method='GET',
                                      data=cmd.encode(),
                                      timeout=500)

        await client.stop()

        dic = json.loads(res)
        code = dic['CODE']
        if code == 0:
            data = dic['DATA']
            res_js = json.loads(data)
            return res_js


class MyEncoder(json.JSONEncoder):
    '''重写json，使其能够序列化时间戳'''

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            print("MyEncoder-datetime.datetime")
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        if isinstance(obj, np.int64):
            return int(obj)
        else:
            return super(MyEncoder, self).default(obj)




url = 'http://10.168.20.62:8762/eureka' # 生产
# url = 'http://10.168.20.60:8763/eureka'  # 测试
# url = get_apollo_config('eureka_report_server_url')

def query_flag_name_by_fundid(fund_id_list):
    params = {
        'fund_id_list': fund_id_list
    }
    from Task.real_time.cfg.apollo.get_apollo import get_apollo_config
    eureka_svr = EurekaRequest()
    res_df = eureka_svr.get_eukera_data(params,get_apollo_config('eureka_report_server_url'),
                                            'zmfundserver',
                                            '/label/get_flag_name_by_id')



    return res_df



class GetData():
    def __init__(self, dbinfo):
        self.db_helper = dbsynchelper()
        self.db_helper.SetDBCfg(dbinfo)
        self.logging = LOG()

    def get_df(self, sql):
        df = self.db_helper.select(sql)
        return df

    def execute(self, sql, info=None):
        self.db_helper.execute(sql)
        if info is not None:
            print(info)

    def upsert_df(self, df, table_name,b_delete_old=True):
        self.db_helper.upsert_df(df, table_name,b_delete_old=b_delete_old)

    def upsert_dflist(self, dflist, table_name,b_delete_old=True):
        self.db_helper.upsert_dflist(dflist, table_name,b_delete_old=b_delete_old)

    def get_fund_name(self):
        return 'Flare-Fund'

    def get_public_name(self):
        return 'Flare-Public'
    
    def query_flag_name_by_fundid(self, fund_id_list):
        params = {
            'fund_id_list': fund_id_list
        }
        from Task.real_time.cfg.apollo.get_apollo import get_apollo_config
        url = get_apollo_config('zmfundsvr_url')
        # url = 'http://10.168.30.78:8888/zmfundserver'
        # url = 'http://10.168.70.113:8080/zmfundserver'
        url += '/label/get_flag_name_by_id'
        headers = {"Content-Type": "application/json"}
        try:
            start = time.time()
            response = requests.post(
                        url=url,
                        data=json.dumps(params),
                        headers=headers)
            end = time.time()
            print(f'end {round(end-start, 4)}', url)
            if response.status_code == 200:
                ret = response.json()

                code = ret['code']
                if code != 0:
                    msg = ret['msg']
                    raise Exception(msg)
            else:
                msg = response.text
                raise Exception(msg)
        except Exception as e:
            msg = str(e)
            print(msg)
            raise Exception(f'request {url} error')

        res_df = pd.DataFrame(data = ret['data'],columns=ret['columns'])
        return res_df

    def get_holddetail(self,pm_id, end_date=None):
        sql3=f'''select FundID,Date as TradingDay,FundName,AssetType,AssetDetailType,StrategyClass,SplitMethod,SecurityCode,Quantity,Price,UnitCost,Cost,MarketValue,Ratio,PL from (select Date,SecurityCode,AssetDetailType,Quantity,Price,UnitCost,Cost,MarketValue,Ratio,PL
                    from [FA-ODS].dbo.FundHoldDetail a where pmid={pm_id} and AssetDetailType != 30032001)a
                    inner join
                        (select FundID,SubjectCode,FundName,AssetType,StrategyClass,SplitMethod
                                from [flare-fund]..imsubfundinfo where pmid={pm_id} and StrategyClass<>7) b
                    on a.SecurityCode=b.SubjectCode
            '''
        if end_date is not None:
            sql3 += f"  WHERE Date<='{end_date}' "
        sql3 += " order by b.FundID,a.Date "
        hold_df=self.get_df(sql3)
        hold_df.TradingDay=pd.to_datetime(hold_df.TradingDay)
        hold_df.set_index(['FundID','TradingDay'],inplace=True)
        hold_df.sort_index(inplace=True)
        # hold_df.loc[hold_df.AssetDetailType==310205,'Quantity']=(hold_df.MarketValue/hold_df.Price)  #数量修正成手数*乘数
        # hold_df.loc[hold_df.AssetDetailType==310206,'MarketValue']=-1*np.abs(hold_df.MarketValue)  #做空的市值要置成负的
        # hold_df.loc[hold_df.AssetDetailType==310206,'Quantity']=hold_df.MarketValue/hold_df.Price  #数量也是负的
        # hold_df.loc[hold_df.AssetDetailType==310207,'Quantity']=(hold_df.MarketValue/hold_df.Price)  #数量修正成手数*乘数
        # hold_df.loc[hold_df.AssetDetailType==310208,'MarketValue']=-1*np.abs(hold_df.MarketValue)  #做空的市值要置成负的
        # hold_df.loc[hold_df.AssetDetailType==310208,'Quantity']=hold_df.MarketValue/hold_df.Price  #数量也是负的
        option_code_list=list(hold_df.loc[hold_df.AssetDetailType.isin([310207,310208]),'SecurityCode'].unique())

        if option_code_list:
        #     option_code_list=[name.replace('上证50股指','HO').replace('中证1000股指','MO').replace('沪深300股指','FO') for name in option_name_list]

            str_code_list=str(option_code_list)[1:-1]
            sql4=f'''SELECT 
        a.TradingDate,
        a.SettlePrice,
        b.TradingCode,
        c.ClosePrice as ULAClosePrice,
        b.StrikePrice,
        b.ExpirationDate,
        b.ULAName,
        CASE b.ContractType 
            WHEN 2 THEN '看涨' 
            ELSE '看跌' 
        END as ContractType
    FROM FINDB..Opt_DailyQuote a 
    INNER JOIN FINDB..Opt_OptionContract b ON a.InnerCode = b.InnerCode 
    INNER JOIN Findb..QT_DailyQuote c ON c.InnerCode = b.ULAInnerCode AND a.TradingDate = c.TradingDay
    INNER JOIN (
        SELECT DISTINCT StrikePrice, ExpirationDate 
        FROM FINDB..Opt_OptionContract 
        WHERE TradingCode IN ({str_code_list})
    ) d ON b.StrikePrice = d.StrikePrice AND b.ExpirationDate = d.ExpirationDate  order by b.ExpirationDate, b.StrikePrice
                
                '''
            db_helper=get_db_helper('10.168.20.22,1433','FinDB')
            option_quote_df=db_helper.select(sql4)
            option_quote_df.columns=['日期','结算价','TradingCode','标的收盘价','行权价','到期日','标的名称','期权类型']
            option_quote_df['剩余天数']=(option_quote_df['到期日']-option_quote_df['日期']).dt.days+0.001
            ret_list=[]
            for ula_name ,sub_option_df in option_quote_df.groupby('标的名称'):
                greek_df=calc_option_metrics(sub_option_df)
                greek_df.reset_index(inplace=True)
                greek_df['delta']=greek_df['delta']*greek_df['标的收盘价']
                ret_list.append(greek_df)
            greek_df_all=pd.concat(ret_list)
            hold_df.reset_index(inplace=True)
            hold_df=pd.merge(hold_df,greek_df_all[['日期','TradingCode','delta']],left_on=['TradingDay','SecurityCode'],right_on=['日期','TradingCode'],how='left')
 
            hold_df['all_delta']=hold_df['delta']*hold_df['Quantity']
            hold_df.set_index(['FundID','TradingDay'],inplace=True)
            hold_df.sort_index(inplace=True)
            hold_df['all_delta']=hold_df['MarketValue'] 
            hold_df.loc[hold_df.delta.notnull(),'all_delta']=hold_df['delta']*hold_df['Quantity']
        else:
            hold_df['all_delta']=hold_df['MarketValue'] 
        return hold_df

    def get_ledger(self,pm_id,end_date=None,unconfirmed_mark=None):
        ret_list=[]
        if unconfirmed_mark is None:
            sql1=f'''select FundID,ConfirmedDate as TradingDay,min(ApplicationDate) as ApplicationDate,AVG(UnitNAV) as PRNAV,
                         sum(case when recordtype=1 then ConfirmedAmount else 0 end) as PurchaseAmount,
                         sum(case when recordtype= 1 then ConfirmedShares else 0 end) as PurchaseShares ,
                         -sum(case when recordtype=2 then ConfirmedAmount else 0 end) as RedeemAmount ,
                         -sum(case when recordtype=2 then ConfirmedShares else 0 end) as RedeemShares
                       from [Flare-Fund].dbo.IMSubPurchaseRedeemRecord where pmid={pm_id} and ConfirmedDate is not null group by FundID, ConfirmedDate  ORDER by FundID, ConfirmedDate'''
        else:
            sql1=f'''select FundID,ConfirmedDate as TradingDay,min(ApplicationDate) as ApplicationDate,AVG(UnitNAV) as PRNAV,
                         sum(case when recordtype=1 then ConfirmedAmount else 0 end) as PurchaseAmount,
                         sum(case when recordtype= 1 then ConfirmedShares else 0 end) as PurchaseShares ,
                         -sum(case when recordtype=2 then ConfirmedAmount else 0 end) as RedeemAmount ,
                         -sum(case when recordtype=2 then ConfirmedShares else 0 end) as RedeemShares
                       from (select FundID,case when ConfirmedDate is null then ApplicationDate else ConfirmedDate end  as ConfirmedDate,ApplicationDate,UnitNAV,recordtype,ConfirmedShares,ConfirmedAmount from [Flare-Fund].dbo.IMSubPurchaseRedeemRecord where pmid={pm_id} ) t
                         group by FundID,ConfirmedDate ORDER by FundID, ConfirmedDate'''      
        pr_df=self.get_df(sql1)

        if pr_df is not None:
            if end_date is not None:
                pr_df=pr_df[pr_df.TradingDay<=end_date]
            pr_df.set_index(['FundID','TradingDay'],inplace=True)
            ret_list.append(pr_df)
        else:
            return 
        
        sql2_1=f'''select FundID,ExDividendDate as TradingDay,abs(ReallyGoldenBonus) as Bonus,abs(DividendInvestShares) as BonusShares
                 from [Flare-Fund].dbo.IMsubDividendRecord where pmid={pm_id}  order by RegistrationDate '''
        bonus_df=self.get_df(sql2_1)
        if bonus_df is not None:
            if end_date is not None:
                bonus_df=bonus_df[bonus_df.TradingDay<=end_date]
            bonus_df.set_index(['FundID','TradingDay'],inplace=True)
            ret_list.append(bonus_df)

        sql2_2=f'''select FundID,abs(DeductionsNum) as PayShares ,ConfirmedDate as TradingDay
                from [Flare-Fund].dbo.IMSubPayRecord where pmid={pm_id} '''
        pay_shares_df=self.get_df(sql2_2)
        if sql2_2 is not None:
            if end_date is not None:
                pay_shares_df=pay_shares_df[pay_shares_df.TradingDay<=end_date]
            pay_shares_df.set_index(['FundID','TradingDay'],inplace=True)
            ret_list.append(pay_shares_df)
        ledger_df=pd.concat(ret_list,axis=1)
        key_list=['PurchaseAmount','PurchaseShares','RedeemAmount','RedeemShares','Bonus','BonusShares','PayShares']
        ledger_df[key_list]=ledger_df[key_list].fillna(0)

        # sql4=f'''select FundID,MarketValue,Price,Quantity,AssetDetailType
        #             from [FA-ODS].dbo.FundHoldDetail a inner join [flare-fund]..imsubfundinfo b on a.SecurityCode=b.SubjectCode  where a.pmid={pm_id}  and b.pmid={pm_id} and AssetDetailType in (310205,310206,310207,310208) '''
        # qihuo_df=self.get_df(sql4)
        # if qihuo_df is not None and len(qihuo_df)>0:
        #     tmp_qihuo_df=qihuo_df.groupby('FundID').first(1)
        #     qihuo_multiplier=np.abs(tmp_qihuo_df['MarketValue']/tmp_qihuo_df['Price']/tmp_qihuo_df['Quantity'])
        #     qihuo_multiplier.name='Multiplier'
        #     ledger_df=ledger_df.join(qihuo_multiplier)
        #     ledger_df.loc[ledger_df.Multiplier.notnull(),['PurchaseShares','RedeemShares']]=ledger_df[['PurchaseShares','RedeemShares']].mul(ledger_df['Multiplier'],axis=0)
        #     ledger_df.loc[ledger_df.Multiplier.notnull(),'PRNAV']=ledger_df['PRNAV']/ledger_df['Multiplier']
        #     short_df=qihuo_df[qihuo_df.AssetDetailType.isin([310206,310208])].groupby('FundID').first(1)
        #     ledger_df.loc[ledger_df.index.get_level_values('FundID').isin(short_df.index),['PurchaseShares','PurchaseAmount','RedeemShares','RedeemAmount']]=-ledger_df.loc[ledger_df.index.get_level_values('FundID').isin(short_df.index),['RedeemShares','RedeemAmount','PurchaseShares','PurchaseAmount']].values


        sql3=f'select FundID,FundIDOld from [Flare-PM]..[VW_IMSubFundInfo_ApplicationDate] where pmid={pm_id}'
        fundid_df=self.get_df(sql3)
        fundid_df=fundid_df.drop_duplicates(subset=['FundID'])
        ledger_df=pd.merge(ledger_df.reset_index(),fundid_df,on='FundID')
        ledger_df.FundID=ledger_df.FundIDOld
        ledger_df.set_index(['FundID','TradingDay'],inplace=True)
        ledger_df.sort_index(inplace=True)
        return ledger_df

    def get_fund_proportion(self ,pm_id,calc_date=None):
        sql2 = f'''SELECT * FROM [Flare-Fund]..FundProportion Where PMID = {pm_id} '''
        if calc_date is not None:
            sql2+=f''' and TradingDay <='{calc_date}' '''

        sql2+=''' order by FundID,TradingDay'''
        df2 = self.get_df(sql2)
        df2["TradingDay"] = pd.to_datetime(df2["TradingDay"])
        df2.set_index(['FundID','TradingDay'],inplace=True)
        df2.fillna(0,inplace=True)
        return df2

    def get_sub_fund_asset_ratio_real(self,pm_id,calc_date=None):
        sql2 = f'''select FundID, PMID, TradingDay, MarketGrowth  [10016] , MarketValue as  [10017], MidcapGrowth [10018], MidcapValue [10019], SmallcapGrowth [10020], SmallcapValue [10021], UpstreamCycle [10024], MidstreamCycle [10025], DownstreamCycle [10026], FinancialSector [10027], Consumption [10028], TMT [10075],[10006],[10029],[10196] from [Flare-Public]..IMSubFundAssetRatio a  Where PMID  = {pm_id} '''
        if calc_date is not None:
            sql2+=f''' and TradingDay <='{calc_date}' '''

        sql2+=''' order by FundID,TradingDay'''
        df2 = self.get_df(sql2)
        df2["TradingDay"] = pd.to_datetime(df2["TradingDay"])
        df2.set_index(['FundID','TradingDay'],inplace=True)
        df2.fillna(0,inplace=True)
        df2 = df2.rename(
            columns={'10016': 10016, '10017': 10017, '10018': 10018, '10019': 10019, '10020': 10020, '10021': 10021,
                     '10024': 10024,'10025':10025, '10026':10026, '10027':10027, '10028':10028, '10075':10075, '10006':10006, '10029':10029, '10196':10196})
        return df2

    def get_pm_share_info(self,pm_id,end_date=None,unconfirmed_mark=None):
        # sql=f'select Date,FlareAssetName,MarketValue from  [FA-ODS]..FundAssetValue WHERE FlareAssetCode in (9010,9011) AND PMID = {pm_id}'
        # share_info_df= self.get_df(sql)
        # if share_info_df is not None:
        #     share_info_df.Date=pd.to_datetime(share_info_df.Date)
        #     return share_info_df.pivot('Date','FlareAssetName','MarketValue')
        sql1 = f'select * from VW_PMINFO where pmid={pm_id}'
        sql2 = f'''select ConfirmedDate as TradingDay,
                        sum(case  RecordType when 1 then abs(ConfirmedAmount) when 2 then -abs(ConfirmedAmount) else 0 end)  as NetAmount 
                        from [Flare-Fund].dbo.IMPMPurchaseRedeemRecord where pmid={pm_id}'''
        if end_date is not None:
            sql1+=f''' and TradingDay<='{end_date}' '''
            sql2+=f''' and ConfirmedDate<='{end_date}' group by ConfirmedDate '''
        sql1 += ' order by TradingDay'
        sql2 += ' order by ConfirmedDate'
        share_info_df= self.get_df(sql1)
        if unconfirmed_mark is not None:
            share_info_df['CashRatio']+=share_info_df['CashRatioNotConfirmed']
            share_info_df['Cash']+=share_info_df['CashNotConfirmed']
        pm_pr_df= self.get_df(sql2)
        if 'ProductNum' in share_info_df:
            share_info_df.drop(columns=['ProductNum'],inplace=True)
        if share_info_df is not None and pm_pr_df is not None:
            share_info_df.TradingDay=pd.to_datetime(share_info_df.TradingDay)
            share_info_df.index=share_info_df['TradingDay'].values
            pm_pr_df.TradingDay=pd.to_datetime(pm_pr_df.TradingDay)
            pm_pr_df.set_index('TradingDay',inplace=True)
            new_share_info_df=pd.concat([share_info_df ,pm_pr_df],axis=1)
            share_info_df['CumNetAmount']=new_share_info_df['NetAmount'].fillna(0).cumsum()
            share_info_df['TotalPL']=share_info_df['AssetNetValue']-share_info_df['CumNetAmount']
            return share_info_df

    def get_valuation_table(self,pm_id):
        sql=f'select PMID,Date,FundName,SecurityCode,SecurityName,Quantity,UnitCost,Cost,Price,ClosePrice,MarketValue,PL,Ratio from [FA-ODS].dbo.FundHoldDetail where pmid={pm_id}'
        return self.get_df(sql)

    def get_fund_information(self, fund_id,frequency,db_tag=None):
        if db_tag==None:
            if self.db_helper.dbinfo.database in ['Flare-Value-HF', 'Flare-Value-MF','Flare-Manage-MF']:
                db_tag = 'Flare-Value'
        if db_tag == 'Flare-Value':
            for db in ['Flare-Value-HF', 'Flare-Value-MF']:
                sql = "select * from [{}]..VW_FundCategoryList where FundID={} and frequency={}".format(db, fund_id,frequency)
                df = self.get_df(sql)
                if df is not None and len(df) > 0:
                    if 'BenchmarkPortType' in df.columns and 'BenchmarkPortID' in df.columns:
                        tmp_1 = df.BenchmarkPortType[0]
                        tmp_2 = df.BenchmarkPortID[0]
                    else:
                        tmp_1 = 1
                        tmp_2 = df.BenchID[0]
                    return df.FamilyType[0], df.FundType[0], df.SourceID[0], df.BenchID[0], tmp_1, tmp_2
        else:
            sql = "select FamilyType, FundType, SourceID, BenchID, BenchmarkPortType, BenchmarkPortID from VW_FundCategoryList where FundID={} and frequency={}".format(fund_id,frequency)
            df = self.get_df(sql)
            if df is not None and len(df) > 0:
                return df.FamilyType[0], df.FundType[0], df.SourceID[0], df.BenchID[0], df.BenchmarkPortType[0], df.BenchmarkPortID[0]
            else:
                raise Exception(f'产品基础信息缺失，no data in VW_FundCategoryList [{fund_id}]')

    def get_fund_list_information(self, fund_list, frequency_list=None ,db_tag=None):
        res_list = []
        if db_tag==None:
            if self.db_helper.dbinfo.database in ['Flare-Value-HF', 'Flare-Value-MF','Flare-Manage-MF']:
                db_tag = 'Flare-Value'
        if frequency_list is None:
            frequency_list = [None] * len(fund_list)
        for fund_id,frequency in zip(fund_list,frequency_list):
            if db_tag == 'Flare-Value':
                sql = "select * from VW_FundCategoryList where FundID ={} and frequency={}".format(fund_id,frequency)
                df = self.get_df(sql)
                if df is not None and len(df) > 0:
                    if 'BenchmarkPortType' not in  df.columns:
                        df['BenchmarkPortType']=1
                    if 'BenchmarkPortID' not in df.columns:
                        df['BenchmarkPortID']=df.BenchID
                    res_list.append(df)
            else:
                sql = "select FamilyType, FundType, SourceID, BenchID, BenchmarkPortType, BenchmarkPortID, FundID, Frequency from VW_FundCategoryList where FundID={} and frequency={}".format(fund_id,frequency)
                df = self.get_df(sql)
                if df is not None and len(df) > 0:
                    res_list.append(df)
        if res_list:
            return pd.concat(res_list)

    def get_subfund_info(self, pm_id):
        # sql= ''' select a.PMID,a.FundID,a.FamilyType,a.FundType,a.Frequency,a.BenchID,a.SourceID,a.BenchmarkPortType,a.BenchmarkPortID, isnull(a.ifposition,0) as IfPosition,count(1) as lenvalue  from vw_fundcategorylist a inner join fundnetvalue b on a.fundid=b.fundid and a.frequency=b.frequency where a.pmid={}
        #             group by a.pmid,a.FundID,a.FamilyType,a.FundType,a.Frequency,a.BenchID,a.SourceID,a.BenchmarkPortType,a.BenchmarkPortID, a.IfPosition '''.format(pm_id)
        
        sql = '''select distinct FundID, Frequency from VW_FundCategoryList where pmid={0} and FundID in 
                (select distinct FundID from PMFundWeight where TradingDay=(select max(TradingDay) from PMFundWeight where PMID={0}))'''.format(pm_id)
        df = self.get_df(sql)
        if df is None or len(df) == 0:
            raise Exception('PMFundWeight 子基金持仓数据为空') 
        return df

    def get_subfund_source_info(self, pm_id):
        sql= ''' SELECT DISTINCT a.PMID ,
                    b.FundIDOld AS FundID ,
                    a.Frequency
                    FROM   vw_fundcategorylist a
                    INNER JOIN [Flare-Fund].dbo.IMSubPurchaseRedeemDate b
                    ON a.pmid=b.PMID
                    AND a.FundID=b.FundIDOld
                    WHERE  a.pmid = {} '''.format(pm_id)
        df = self.get_df(sql)
        return df

    def get_pm_benchmark_info(self, pm_id):
        sql = '''select YieldType,YieldSubType,BenchmarkPortType,BenchmarkPortID from [{}]..IMFundBenchmarkPortMapping where pmid={}'''.format(self.get_fund_name(),pm_id)
        df = self.get_df(sql)
        infodict={}
        if df is not None and len(df)>0:
            for index, row in df.iterrows():
                infodict[(row['YieldType'],row['YieldSubType'])]=(row['BenchmarkPortType'],row['BenchmarkPortID'])
            return infodict

    def get_pm_frequency(self, pmid):
        sql = 'select frequency from [{}]..impmfundinfo where pmid={}'.format(self.get_fund_name(),pmid)
        df = self.get_df(sql)
        if df is not None and len(df)>0:
            return df.frequency.values[0]

    def get_max_trading_day(self):
        sql = 'select max(tradingday) as maxdate from fundnetvalue with(nolock)'
        df = self.get_df(sql)
        if df is not None:
            df.index=df.maxdate
            return df.index[-1]

    def get_trading_day_num(self, start_date, end_date, frequency):
        if frequency==1:
            table_name='[{}]..HD_TradingDay'.format(self.get_public_name())
            sql='''select count(1) as num from {} where iftradingday=1 and tradingday>='{}' and tradingday<='{}'
                '''.format(table_name,start_date,end_date)
        elif frequency==2:
            table_name='[{}]..HD_TradingWeek'.format(self.get_public_name())
            sql='''select count(1) as num from {} where iftradingweek=1 and tradingday>='{}' and tradingday<='{}'
                '''.format(table_name,start_date,end_date)
        else:
            return
        df=self.get_df(sql)
        if df is not None:
            return df.num.values[0]

    def get_fund_yield_real(self, fund_id, yield_type_pair, start_date=None, end_date=None,frequency=None ):
        # 获取指定基金的衍生净值及收益序列
        yield_type,yield_sub_type=yield_type_pair
        table_name = 'FundYield_Real'
        sql = """select FundID,TradingDay,NetValue,Yield,YieldType from {} WITH(NOLOCK)
                  where FundID = {}
                  and PartID = {}
                  and YieldType = {}
                  and YieldSubType = {}
                  and period = {}
                  """.format(table_name, fund_id, fund_id%20, yield_type,yield_sub_type,frequency)
        if start_date is not None:
            sql +=" and tradingday>='{}' ".format(start_date)
        if end_date is not None:
            sql +=" and tradingday<='{}' ".format(end_date)
        sql+=' order by TradingDay; '

        df = self.get_df(sql)
        if df is None or len(df) == 0:
            return
        df.index = df.TradingDay.values
        return df

    def get_pm_yield(self, pm_id, yield_type_pair, frequency):
        yield_type,yield_sub_type=yield_type_pair
        sql = '''select * from pmyield
                    where pmid = {}
                    and YieldType = {}
                    and YieldSubType = {}
                    and period={}
                    order by TradingDay asc'''.format(pm_id, yield_type, yield_sub_type,frequency)
        df = self.get_df(sql)
        if df is None:
            self.logging.error('No PM yield data for PMID: {}'.format(pm_id))
        else:
            df.index = df.TradingDay.values
            return df

    def get_pm_yield_real(self, pm_id, yield_type_pair, frequency):
        yield_type,yield_sub_type=yield_type_pair
        sql = '''select TradingDay,Yield from pmyield_real
                    where pmid = {}
                    and YieldType = {}
                    and YieldSubType = {}
                    and period={}
                    order by TradingDay asc'''.format(pm_id, yield_type, yield_sub_type,frequency)
        df = self.get_df(sql)
        if df is None:
            self.logging.error('No PM yield real data for PMID: {}'.format(pm_id))
        else:
            df.index = df.TradingDay.values
            return df

    def get_benchmark_port_yield(self,benchmark_port_pair,frequency=None):
        benchmark_port_type, benchmark_port_id = benchmark_port_pair
        if benchmark_port_type == 2:
            if frequency is None:
                sql = 'select TradingDay, BenchmarkPortID,Yield,NetValue, 1 as YieldType,1 as YieldSubType from [{}]..BenchMarkPortYield where benchmarkportid={}  order by tradingday'.format(
                    self.get_public_name(), benchmark_port_id)
            else:
                sql = 'select TradingDay, BenchmarkPortID,Yield,NetValue, 1 as YieldType,1 as YieldSubType from [{}]..BenchMarkPortYield where benchmarkportid={} and period={} order by tradingday'.format(
                    self.get_public_name(), benchmark_port_id, frequency)
        elif benchmark_port_type == 3:
            sql = 'SELECT a.TradingDay,a.LabelID BenchmarkPortID, a.Yield , a.NetValue , 1 as YieldType, 1 as YieldSubType FROM [Flare-Fund].[dbo].[LabelIndex] a WHERE a.LabelID={} ORDER BY a.TradingDay'.format(
                benchmark_port_id)
        else:
            if frequency is None:
                sql = 'select TradingDay,BenchID as BenchmarkPortID,Yield, ClosePrice as NetValue, YieldType, YieldSubType from [{}]..BenchMarkYield where benchid={}  and yieldtype=1 and yieldsubtype=1 order by tradingday'.format(
                    self.get_public_name(), benchmark_port_id)
            else:
                sql = 'select TradingDay,BenchID as BenchmarkPortID,Yield, ClosePrice as NetValue, YieldType, YieldSubType from [{}]..BenchMarkYield where benchid={} and period={} and yieldtype=1 and yieldsubtype=1 order by tradingday'.format(
                    self.get_public_name(), benchmark_port_id, frequency)
        df = self.get_df(sql)
        if df is not None and len(df) > 0:
            df = df.drop_duplicates(subset=['TradingDay'])
            df.index = df.TradingDay.values
            if benchmark_port_type == 3:
                df = df.resample('D').bfill()
                df.Yield = df.NetValue.pct_change()
                df.TradingDay = df.index.values
            return df

    def get_benchmark_yield_by_yield_type(self, yield_type_pair, frequency):
        yield_type, yield_sub_type = yield_type_pair
        sql = '''select TradingDay, BenchID, Yield, YieldType, YieldSubType from [{0}]..BenchMarkYield where YieldType={1} and YieldSubType={2} and Period={3} Order By TradingDay'''.format(self.get_public_name(), yield_type, yield_sub_type, frequency)
        df = self.get_df(sql)
        if df is not None and len(df) > 0:
            df.index = df.TradingDay.values
            return df

    def get_pm_super_yield(self, pm_id, yield_type_pair,frequency,benchmark_port_pair):
        yield_type,yield_sub_type=yield_type_pair
        benchmark_port_type,benchmark_port_id=benchmark_port_pair
        if benchmark_port_type==2:
            sql = '''select a.TradingDay ,(1+a.yield)/(1+b.yield)-1 as Yield from pmyield a
                            inner join [{0}]..benchmarkportyield b
                            on a.TradingDay=b.TradingDay
                        where a.pmid = {1}
                        and a.YieldType = {2}
                        and a.YieldSubType = {3}
                        and a.Period={4}
                        and b.Period={4}
                        and b.benchmarkportid={5}
                        order by a.TradingDay asc'''.format(self.get_public_name(),pm_id, yield_type, yield_sub_type,frequency,benchmark_port_id)
        else:
            sql = '''select a.TradingDay ,(1+a.yield)/(1+b.yield)-1 as Yield from pmyield a
                            inner join [{0}]..benchmarkyield b
                            on a.TradingDay=b.TradingDay
                        where a.pmid = {1}
                        and a.YieldType = {2}
                        and a.YieldSubType = {3}
                        and a.Period={4}
                        and b.Period={4}
                        and b.benchid={5}
                        and b.YieldType=1 and b.YieldSubType=1
                        order by a.TradingDay asc'''.format(self.get_public_name(),pm_id, yield_type, yield_sub_type,frequency,benchmark_port_id)

        df = self.get_df(sql)
        if df is None or len(df) == 0:
            self.logging.error('No PM yield data for PMID: {}'.format(pm_id))
        else:
            df.index = df.TradingDay.values
            return df

    def get_bench_yield(self, bench_id, frequency, bench_type=1):
        if bench_type==1:
            return self.get_bench_list_yield([bench_id],frequency=frequency)
        else:
            return self.get_benchmark_port_yield([bench_type, bench_id],frequency)

    def get_bench_list_yield(self, bench_list, start_date=None, end_date=None,frequency =None,yieldtype=1,yieldsubtype=1 ):
        '''
        :param bench_id:
        :return:
        '''
        table_name = '[{}]..BenchMarkYield'.format(self.get_public_name())
        sql_bench_list = ','.join(map(str, bench_list))
        sql = """select BenchID,TradingDay,ClosePrice,Yield from {} WITH(NOLOCK)
                  where BenchID in ({}) and yieldtype={} and yieldsubtype={}
                  and period = {}
                  """.format(table_name, sql_bench_list,yieldtype,yieldsubtype,frequency)
        if start_date is not None:
            sql +=" and tradingday>='{}' ".format(start_date)
        if end_date is not None:
            sql +=" and tradingday<='{}' ".format(end_date)
        sql+=' order by TradingDay; '
        res_df = self.get_df(sql)
        if res_df is None or len(res_df) == 0:
            self.logging.error('df is empty ! bench_list:{}'.format(bench_list))
            return None
        #res = pd.pivot(df, 'TradingDay', 'BenchID','Yield')
        res_df.index = res_df.TradingDay.values

        return res_df

    def get_fund_net_value(self, fund_id, frequency=1, start_date=None, end_date=None):
        '''
        :param fund_id:
        :return:
        '''
        table_name = '[Flare-Fund]..FundDeriveYield'
        sql = """select FundID,TradingDay,NetValue as SubscriptionAccuNAV from {0}  WITH(NOLOCK)
              where FundID = {1}
              and frequency = {2} and YieldType=1 and YieldSubType=1""".format(table_name, fund_id,frequency)

        if start_date is not None:
            sql += " and TradingDay>='{}' ".format(start_date)
        if end_date is not None:
            sql += " and TradingDay<='{}' ".format(end_date)

        sql += ''' order by TradingDay'''
        df = self.get_df(sql)
        if df is None or len(df) == 0:
            return
        df.index = df.TradingDay.values
        return df

    def get_fund_list_net_value(self, fund_list, frequency, start_date=None, end_date=None, deltanum=1000):
        table_name = '[Flare-Fund]..FundDeriveYield'
        len_fund = len(fund_list)
        res_list = []
        for i in range(0, len_fund, deltanum):
            fund_list_sub = fund_list[i: i+deltanum]
            sql = """select FundID,TradingDay,NetValue from {0}  WITH(NOLOCK)
                where FundID in ({1})
                and frequency = {2}
                 """.format(table_name, str(fund_list_sub)[1:-1], frequency)
            if start_date is not None:
                sql += " and TradingDay>='{}' ".format(start_date)
            if end_date is not None:
                sql += " and TradingDay<='{}' ".format(end_date)

            sql += ''' union all
                select PMID AS FundID,TradingDay,NetValue from [Flare-PM]..PMNetValue  WITH(NOLOCK)
                where PMID in ({0}) and Period = {1}'''.format(str(fund_list_sub)[1:-1], frequency)
            if start_date is not None:
                sql += " and TradingDay>='{}' ".format(start_date)
            if end_date is not None:
                sql += " and TradingDay<='{}' ".format(end_date)
            sql += ''' order by FundID, TradingDay'''

            df = self.get_df(sql)
            if df is None or len(df) == 0:
                continue
            df.index = df.TradingDay.values
            res_list.append(df)

        if res_list:
            res_df = pd.concat(res_list)
            return res_df

    def get_fund_list_derive_yield(self, fund_list, frequency, deltanum=1000, start_date=None, end_date=None):
        table_name = '[Flare-Fund]..FundDeriveYield'
        len_fund = len(fund_list)
        res_list = []
        for i in range(0, len_fund, deltanum):
            fund_list_sub = fund_list[i: i+deltanum]
            sql = """select FundID,TradingDay,Yield,NetValue from {0}  WITH(NOLOCK)
                where FundID in ({1})
                and frequency = {2} """.format(table_name, str(fund_list_sub)[1:-1], frequency)
            if start_date is not None:
                sql += " and TradingDay>='{}' ".format(start_date)
            if end_date is not None:
                sql += " and TradingDay<='{}' ".format(end_date)
            sql += ''' order by TradingDay'''
            df = self.get_df(sql)
            if df is None or len(df) == 0:
                continue
            df.index = df.TradingDay.values
            res_list.append(df)

        if res_list:
            res_df = pd.concat(res_list)
            return res_df

    def get_fund_list_derive_netvalue(self, fund_list, frequency, deltanum=1000, start_date=None, end_date=None):
        table_name = '[Flare-Fund]..FundDeriveYield'
        len_fund = len(fund_list)
        res_list = []
        for i in range(0, len_fund, deltanum):
            fund_list_sub = fund_list[i: i+deltanum]
            sql = """select FundID,TradingDay,Yield,NetValue from {0}  WITH(NOLOCK)
                where FundID in ({1})
                and frequency = {2} """.format(table_name, str(fund_list_sub)[1:-1], frequency)
            if start_date is not None:
                sql += " and TradingDay>='{}' ".format(start_date)
            if end_date is not None:
                sql += " and TradingDay<='{}' ".format(end_date)
            sql += ''' order by TradingDay'''
            df = self.get_df(sql)
            if df is None or len(df) == 0:
                continue
            df.index = df.TradingDay.values
            res_list.append(df)

        if res_list:
            res_df = pd.concat(res_list)
            return res_df

    def get_fund_factor_exposure(self, fund_list, frequency, deltanum=1000):
        res_list = []
        len_fund = len(fund_list)
        for i in range(0, len_fund, deltanum):
            fund_list_sub = fund_list[i: i+deltanum]
            for db in ['[Flare-Mix]']:
                table_name = db + '..FundFactorExposure'
                sql = """select a.FundID, FactorID, Coefficient from {0} a inner join
                    (select FundID, max(TradingDay) as TradingDay from {0} where RegressionID in (2,3) and RollNum=26 and Frequency={2}  and PlanID=-1 group by FundID) b
                    on a.FundID=b.FundID and a.TradingDay=b.TradingDay  
                    where a.RegressionID in (2,3) and a.RollNum=0 and FactorID<>20000 and a.FundID in ({1}) and Frequency={2} and a.PlanID=-1""".format(table_name, str(fund_list_sub)[1:-1], frequency)
                df = self.get_df(sql)
                if df is None or len(df) == 0:
                    continue
                df.drop_duplicates(subset=['FundID', 'FactorID'], keep='first', inplace=True)
                res_list.append(df)
        if res_list:
            df = pd.concat(res_list)
            return df

    def get_fund_list_net_value_real(self, fund_list, frequency=1, start_date=None, end_date=None):
        table_name = 'FundNetValue_Real'
        sql = """select FundID,TradingDay,UnitNAV,SubscriptionAccuNAV,SubscriptionAccuNAV as NetValue,Frequency from {0}  WITH(NOLOCK)
              where FundID in ({1})""".format(table_name, str(fund_list)[1:-1])

        if start_date is not None:
            sql += " and TradingDay>='{}' ".format(start_date)
        if end_date is not None:
            sql += " and TradingDay<='{}' ".format(end_date)

        sql += ''' order by TradingDay'''
        df = self.get_df(sql)
        if df is None or len(df) == 0:
            return
        df.index = df.TradingDay.values
        return df

    def get_fund_list_net_value_real2(self, fund_list, frequency=1, start_date=None, end_date=None):
        table_name = '[Flare-Fund]..FundOriginalNetValue'
        sql = """select FundID,TradingDay,UnitNAV,SubscriptionAccuNAV as NetValue, SubscriptionAccuNAV from {0}  WITH(NOLOCK)
              where FundID in ({1})""".format(table_name, str(fund_list)[1:-1])

        if start_date is not None:
            sql += " and TradingDay>='{}' ".format(start_date)
        if end_date is not None:
            sql += " and TradingDay<='{}' ".format(end_date)

        # sql += ''' order by FundID, TradingDay'''
        # df = self.get_df(sql)
        # df=self.db_helper.get_sql_data_by_bcp(sql)

        df=self.db_helper.get_sql_data(sql,bcp=False)
        if df is None or len(df) == 0:
            return
        df.sort_values(['FundID','TradingDay'],inplace=True)
        df.index = df.TradingDay.values
        # df['NetValue'] = df.groupby('FundID').apply(lambda x:cores.calc_netvalue_by_unitnav_and_accunav(x.NetValue,x.UnitNAV)).reset_index('FundID',drop=True)

        return df

    def get_fund_derive_yield_real_frequency(self, pm_id):
        sql = """select FundID, SubscriptionAccuNAV as NetValue, TradingDay from [Flare-Fund]..FundOriginalNetValue 
                where FundID in (select distinct FundID from VW_FundCategoryList where PMID={0}) order by FundID, TradingDay""".format(pm_id)
        df = self.get_df(sql)
        if df is None or len(df) == 0:
            return
        df['Yield'] = df.groupby('FundID').NetValue.pct_change()
        df.index = df.TradingDay.values
        return df

    def get_pm_fund_confirm_date(self, pm_id):
        sql = '''select distinct FundID, ApplicationDate as TradingDay, UnitNAV as NetValue from VW_IMSubFundInfo_ApplicationDate where PMID={} and ApplicationDate is not NULL'''.format(pm_id) # [Flare-Fund]..IMSubPurchaseRedeemRecord
        df = self.get_df(sql)
        return df

    def get_calc_tradingday(self, pm_id):
        sql = '''SELECT MAX(Date) AS CalDate FROM [FA-ODS].dbo.FundHoldDetail  WHERE PMID={}'''.format(pm_id)
        df = self.get_df(sql)
        if df is not None and len(df) > 0:
            df['CalDate'] = pd.to_datetime(df['CalDate'])
            tmp = df.iloc[0].CalDate
            if type(tmp) is pd._libs.tslibs.timestamps.Timestamp:
                return tmp

    def get_pm_bench_pair(self, pm_id):
        sql = '''select BenchmarkPortType, BenchmarkPortID from [Flare-Fund].dbo.IMPMFundInfo where PMID={}'''.format(pm_id)
        df = self.get_df(sql)
        if df is not None and len(df) > 0:
            return (df.iloc[0].BenchmarkPortType, df.iloc[0].BenchmarkPortID)

    def get_IM_benchID_list(self, bench_id):
        table_name = '[Flare-Fund]..IMBenchmarkPortDetail'
        sql = '''select BenchID from {0} WITH(NOLOCK) where BenchmarkPortID={1}'''.format(table_name, bench_id)
        res_df = self.get_df(sql)
        if res_df is None or len(res_df) == 0:
            self.logging.error('IMBenchmarkPortDetail is empty for {}!'.format(bench_id))
            return None
        return res_df.BenchID.values.tolist()

    def get_all_tradingday(self, start_date=None, end_date=None):
        sql = '''select TradingDay, IfNatureWeekEnd, IfWeekEnd from [Flare-Public]..HD_TradingDay where 1=1 '''
        if start_date is not None:
            sql += ''' and TradingDay>='{0}' '''.format(str(start_date))
        if end_date is not None:
            sql += ''' and TradingDay<='{0}' '''.format(str(end_date))
        res_df = self.get_df(sql)
        return res_df

    def get_pm_net_value(self, pm_id, end_date = None):
        sql = '''select TradingDay, NetValue, Period from PMNetValue where PMID={} and YieldType=1 and YieldSubType=1'''.format(pm_id)
        if end_date is not None:
            sql += f" and TradingDay <='{end_date}' "
        sql += " order by TradingDay"
        res_df = self.get_df(sql)
        if res_df is not None and len(res_df) > 0:
            res_df.index = res_df.TradingDay.values
            return res_df

    def get_pm_net_value_real(self, pm_id, end_date = None):     
        # sql = '''select TradingDay,   NetValue from [Flare-PM]..PMNetValue_real where PMID={}  '''.format(pm_id)
        # if end_date is not None:
        #     sql += f" and TradingDay <='{end_date}' "
        # sql += " order by TradingDay"
        # res_df = self.get_df(sql)
        # if res_df is not None and len(res_df) > 0:
        #     res_df.index = res_df.TradingDay.values
        #     return res_df
        # else:
        sql1='''SELECT TradingDay,AccuNAV AS NetValue FROM [Flare-Fund]..IMPMNetValuevaluation  where PMID={}  '''.format(pm_id)
        if end_date is not None:
            sql1 += f" and TradingDay <='{end_date}' " 
        sql1 += " order by TradingDay"
        res_df = self.get_df(sql1)
        if res_df is not None and len(res_df) > 0:
            res_df.index = res_df.TradingDay.values
            return res_df

    def get_pm_fund_weight(self, pm_id, end_date = None):
        sql = '''select FundID, TradingDay, WeightType, WeightMethod, Ratio from PMFundWeight where PMID={}'''.format(pm_id)
        if end_date is not None:
            sql += f" and TradingDay <='{end_date}' "
        sql += " order by TradingDay"
        res_df = self.get_df(sql)
        if res_df is not None and len(res_df) > 0:
            res_df.index = res_df.TradingDay.values
            return res_df

    def get_pm_fund_info(self, pm_id, fund_list=None):
        sql = '''select FundID, AssetType, StrategyClass, FamilyType, Frequency, BenchmarkPortType, BenchmarkPortID, BenchID, SourceID from VW_FundCategoryList where PMID={}'''.format(pm_id)
        if fund_list is not None and len(fund_list) > 0:
            sql += ' and FundID in ({})'.format(str(fund_list)[1:-1])
        res_df = self.get_df(sql)
        return res_df

    def get_fund_hold_asset(self, pm_id):
        try:
            raise Exception('FA-ODS..YEILD_FUND_ASSET_CHILD_1701')
            sql = '''exec [FA-ODS]..YEILD_FUND_ASSet_CHILD_1701 @i_pmid={}, @o_RetCode = 0, @o_RetMsg = N'' '''.format(pm_id)
            res_df = self.get_df(sql)
            if res_df is None or len(res_df)==0:
                raise Exception('FA-ODS..YEILD_FUND_ASSET_CHILD_1701 empty')
            res_df.sort_values(['Date','AssetType'],inplace=True) 
            if res_df is not None and len(res_df) > 0:
                res_df['TradingDay'] = pd.to_datetime(res_df.Date)
                res_df.index=res_df.Date.values
                sql = 'select TradingDay from PMNetValue where PMID={} order by TradingDay'.format(pm_id)
                tradingday_df = self.get_df(sql)
                tradingday_df.index=tradingday_df.TradingDay.values
                res_df = pd.merge(tradingday_df,res_df, on=['TradingDay'], how='outer')
                res_df['NetValue'] = res_df.groupby('AssetType')['NetValue'].fillna(method='ffill')
                res_df['Ratio'] = res_df.groupby('AssetType')['Ratio'].fillna(method='ffill')
                res_df = pd.merge(res_df, tradingday_df, on='TradingDay', how='inner')
                res_df['Yield'] = res_df.groupby('AssetType')['NetValue'].pct_change()
                res_df['Beta'] = res_df['Ratio']  #beta tradingday mean the close  beta
                res_df['SingleAssetYield'] = res_df.groupby('AssetType')['Ratio'].shift(1) * res_df['Yield']
                res_df['AssetOrientation'] = 1
                res_df['ExposureSubType'] = res_df['AssetType']
                return res_df
        except:
            sql = '''select assettype as ExposureSubType, Date as TradingDay, Ratio as Beta, 1 as AssetOrientation, 0 as SingleAssetYield from [FA-ODS]..FundHoldAsset where PMID={} order by Date'''.format(pm_id)
            res_df = self.get_df(sql)
            return res_df

    def get_pm_fund_list(self, pm_id):
        sql = '''select distinct FundID from PMFundWeight where PMID={0} and TradingDay=(select Max(TradingDay) from PMFundWeight where PMID={0})'''.format(pm_id)
        res_df = self.get_df(sql)
        if res_df is not None and len(res_df) > 0:
            fund_list = res_df.FundID.tolist()
            return fund_list

    def get_pm_label_info(self, fund_list):
        if fund_list is None or len(fund_list) == 0:
            return
        sql = 'SELECT FundID, LabelID, YieldType, YieldSubType FROM [Flare-Fund]..VW_IMSubFundInfo_LabelID where FundID in ({})'.format(str(fund_list)[1:-1])
        res_df = self.get_df(sql)
        return res_df

    def get_pm_label_info_tmp(self, fund_list,yield_type=None,yield_sub_type=None):
        if fund_list is None or len(fund_list) == 0:
            return
        if yield_type is None:
            sql = 'SELECT FundID, LabelID, YieldType, YieldSubType FROM [Flare-Fund]..VW_IMSubFundInfo_LabelID_TMP where FundID in ({}) '.format(str(fund_list)[1:-1])
        elif yield_type==12:
            sql = 'SELECT FundID, LabelID, YieldType, YieldSubType FROM [Flare-Fund]..VW_IMSubFundInfo_LabelID_TMP where FundID in ({}) and yieldtype={}'.format(str(fund_list)[1:-1],yield_type)
        elif yield_type==14:
            #####ZM1765
            sql=f'''
             with tab1 as (
                select a.FundID, 42202 as LabelID, 1 as YieldType, 1 as YieldSubType
                from [Flare-PM].dbo.VW_IMSubFundInfo_ApplicationDate a WITH (NOLOCK)
                where a.PMID in (10893,10090,10740,10103,10246,10306,10329,10358,10353)
                  and a.StrategyClass = 4
                  and a.FundID in ({str(fund_list) [1:-1]})
            ),
            tab2 as (
                     --短中长周期市场中性映射 市场跟踪池 的 三个labelid （不在市场跟踪池的manualfundraceinfo）
                     --中场周期趋势CTA 变 市场跟踪池的 的 1000000042
                     select a.FundID,case when a.Race =44 then 1000000644 when a.Race = 45 then 1000000645 when a.Race = 46 then 1000000646 when a.race = 52 then 1000000042  end as LabelID, 1 as YieldType, 1 as YieldSubType
                     from [Flare-PM].dbo.VW_IMSubFundInfo_ApplicationDate a with (nolock)
                    where a.FundID in ({str(fund_list) [1:-1]}) and a.Race in (44,45,46,52)
                 ),
			tab3 as (
                     --股票多头根据匹配上的市场跟踪池赛道数据
				select distinct a.FundID,case when a.Race = 20 then 10258 else LabelID end as LabelID
				, 1 as YieldType, 1 as YieldSubType
				FROM [Flare-PM].dbo.VW_IMSubFundInfo_ApplicationDate a WITH (NOLOCK)
				left JOIN (
				select Ifvalid, LabelName as CodeName,LabelID,ParentID
				,FilterCondition
				,SUBSTRING(FilterCondition,CHARINDEX('":',FilterCondition)+2,2) as StrategyCode
				from  [Flare-Fund].dbo.LabelBaseInfo B with(nolock) where B.ParentID in(42201)
				and Ifvalid=1  
				) b
				ON a.RaceName = b.CodeName  
				where a.StrategyClassName='股票多头' and  a.Race is not null  and  a.FundID in ({str(fund_list) [1:-1]})
                 )
            SELECT distinct a.FundID,case when a.Race = 20 then 10258 else coalesce(c.LabelID,c1.LabelID,c2.LabelID, b.code) end as LabelID, 1 as YieldType, 1 as YieldSubType
            FROM [Flare-PM].dbo.VW_IMSubFundInfo_ApplicationDate a WITH (NOLOCK)
                     left JOIN [Flare-Public].dbo.Dic_SystemCode b WITH (NOLOCK) ON a.RaceName= b.CodeName AND b.ClassEN = 'RaceCode'
                     left join tab1 c on a.FundID = c.FundID
                     left join tab2 c1 on a.FundID = c1.FundID
					 LEFT JOIN tab3 C2 ON a.FundID=C2.FundID
            where a.fundid in ({str(fund_list) [1:-1]})
            ORDER BY a.FundID
            '''
        elif yield_type==15:
            sql=f'''
            SELECT a.FundID, b.code as LabelID, 1 as YieldType, 1 as YieldSubType
            FROM [Flare-PM].dbo.VW_IMSubFundInfo_ApplicationDate a WITH (NOLOCK)
                     left JOIN [Flare-Public].dbo.Dic_SystemCode b ON CASE
                                                                                         WHEN a.RaceName = '300指增' THEN '私募300指增'
                                                                                         WHEN a.RaceName = '500指增' THEN '私募500指增'
                                                                                         WHEN a.RaceName = '800指增' THEN '私募800指增'
                                                                                         WHEN a.RaceName = '1000指增' THEN '私募1000指增'
                                                                                         else a.RaceName
                                                                                         END = b.CodeName AND b.ClassEN = 'RaceCode'
            where a.fundid in ({str(fund_list) [1:-1]})
            ORDER BY a.FundID
            '''
        res_df = self.get_df(sql)
        res_df.LabelID.fillna(0,inplace=True)
        return res_df

    def get_pm_label_info_by_label_list(self, label_list):
        if label_list is None or len(label_list) == 0:
            return
        res_dict = {}
        res_list = []
        for label_id in label_list:
            sql = 'SELECT distinct FundID FROM [Flare-Fund]..VW_IMSubFundInfo_LabelID where LabelID={}'.format(label_id)
            res_df = self.get_df(sql)
            if res_df is None or len(res_df) == 0:
                res_dict[label_id] = []
            else:
                res_dict[label_id] = res_df.FundID.tolist()
                res_list = list(set(res_list) | set(res_df.FundID.tolist()))
        return res_dict, res_list

    def get_fund_list_by_label_list(self, label_list):
        sql = "select distinct LabelID, DataID as FundID from [Flare-Fund]..FundLabelSystem where LabelID in ({})".format(str(label_list)[1:-1])
        df = self.get_df(sql)
        if df is not None and len(df) > 0:
            return df

    def get_same_feature_label_id(self, pm_id):
        sql = "SELECT distinct SameFeatureLabelID FROM [Flare-PM].[dbo].[VW_IMPMLabelID] where PMID={}".format(pm_id)
        df = self.get_df(sql)
        if df is not None and len(df) > 0:
            if df.SameFeatureLabelID.iloc[0] is not None:
                return int(df.SameFeatureLabelID.iloc[0])

    def get_similar_fof_list_dict(self, label_list, min_date, i_date):
        table_name = '[Flare-Fund]..FundDeriveYield'
        fof_list_dict = {}
        fof_yield_dict = {}
        for i_label_id in label_list:
            if i_label_id not in [1101050000000012, 1101050000000013, 1101050000000014]:
                min_date_str = 'NULL'
            else:
                min_date_str = "'{}'".format(str(min_date))
            sql = "EXEC [Flare-PM].dbo.p_QueryFOFFundList @i_LabelID={},@i_FundRegDate={}, @i_date='{}'".format(i_label_id, min_date_str, i_date)
            df = self.get_df(sql)
            if df is not None and len(df) > 0:
                if i_label_id not in [1101050000000012, 1101050000000013, 1101050000000014]:
                    max_enddate = df.EndDate.max()
                    df = df[df.EndDate == max_enddate]
                    max_enddate = df.BeginDate.iloc[0]
                fund_list = df.FundID.to_list()

                if len(fund_list) > 0:
                    sql = "select distinct FundID from (select FundID, min(TradingDay) as begindate from {0} where FundID in ({1})  group by FundID union all select PMID as FundID, min(TradingDay) as begindate from [Flare-Fund]..PMOriginalNetValue where PMID in ({1})  group by PMID) a ".format(table_name, str(fund_list)[1:-1])
                    if min_date_str != 'NULL':
                        sql += 'where a.begindate <= {}'.format(min_date_str)
                    df = self.get_df(sql)
                    fund_list = df.FundID.to_list()
                fof_list_dict[i_label_id] = fund_list
                if i_label_id not in [1101050000000012, 1101050000000013, 1101050000000014]:
                    fof_yield_dict[i_label_id] = self.get_fund_list_net_value(fund_list, 2, start_date=max_enddate-timedelta(days=365)) #Jira 6306 180改为365,2021/12/3 姗姗和郭泓一起过来口头说的
                else:
                    fof_yield_dict[i_label_id] = self.get_fund_list_net_value(fund_list, 2)
        return fof_list_dict, fof_yield_dict

    def get_similar_fund_dict(self, fund_info_df, group_type):
        type_df = fund_info_df[['FamilyType', 'SourceID']].drop_duplicates(subset=['FamilyType', 'SourceID'], keep='first', inplace=False)
        res_dict = {}
        res_list = []
        for index, row in type_df.iterrows():
            sql = "SELECT distinct RankConfigID FROM [Flare-Fund]..FundRankConfig where FamilyType={} and SourceID={} and GroupType='{}'".format(row.FamilyType, row.SourceID, group_type)
            df = self.get_df(sql)
            if df is not None and len(df) > 0:
                rank_config_id = df.RankConfigID.iloc[0]
                fund_list = self.get_queryFundList(rank_config_id)
                res_dict[row.FamilyType, row.SourceID] = fund_list
                res_list = list(set(res_list) | set(fund_list))
        return res_dict, res_list

    def get_company_similar_fund_df(self, pm_id):
        sql = 'SELECT * FROM [Flare-Fund]..SameCompanyFund WHERE pmid={}'.format(pm_id)
        df = self.get_df(sql)
        if df is not None and len(df) > 0:
            return df

    def get_label_fund_mapping(self, fund_list, label_list):
        sql = "select distinct DataID as FundID, LabelID from [Flare-Fund]..FundLabelSystem where DataID in ({0}) and LabelID in ({1})".format(str(fund_list)[1:-1], str(label_list)[1:-1])
        df = self.get_df(sql)
        return df

    def get_pm_fund_mapping(self, pm_id):
        sql = '''WITH tmp AS 
                (
                SELECT  a.PMID ,
                a.FundID AS FundID_Flare, --投后生成的fundid
                f.FundSysCode AS FundSyscode_Flare,
                f.AMACFundCode ,
                a.fundid,
                ISNULL(e.Num,0) AS Num_Flare
                FROM    [Flare-Fund].dbo.IMSubFundInfo a
                LEFT JOIN [Flare-Fund].dbo.IMFundInfo f
                ON	f.FundSysCode=a.FundSysCode	
                LEFT JOIN [Flare-Fund].dbo.FundNetValueStatis e
                ON	e.FundID=a.FundID
                WHERE	a.PMID={}
                )

                SELECT FundID,FundID_Flare as PMFundID
                FROM 
                (
                SELECT   b.PMID
                ,b.FundID_Flare--投后生成的fundid
                ,b.AMACFundCode
                ,a.FundSysCode
                ,ISNULL(c.FundID,b.FundID_Flare) AS FundID --映射后的fundid，取这个基金的净值。
                ,ROW_NUMBER()OVER(PARTITION BY b.FundID_Flare ORDER BY d.Num DESC) AS rn 
                FROM	[Flare-Fund].dbo.FundSysCodeInfo a --a做主表：因为1个b要对应多个a，用b做主表的话，只能随机返回1个值。
                JOIN	tmp b  --用jorn，不用left join：a表数据不全要，只看跟tmp有amacfundcode重复的部分。
                ON	b.AMACFundCode = a.AMACFundCode
                LEFT JOIN [Flare-Fund].dbo.FundMapSys2FundID c 
                ON	c.FundSysCode = a.FundSysCode
                AND	c.Method=1
                LEFT JOIN [Flare-Fund].dbo.FundNetValueStatis d
                ON	d.FundID=c.FundID
                AND	d.num>=b.Num_Flare --样本内净值要比样本外净值长度长，如果不长于样本外则不取样本的。
                WHERE	a.Type=1 --限制取样本内基金或者样本外基金本身的净值做对比。
                OR	a.FundSysCode=b.FundSyscode_Flare
                )t
                WHERE t.rn=1'''.format(pm_id)
        res_df = self.get_df(sql)
        return res_df

    def get_im_sub_fund_info(self, pm_id, asset_type_list):
        sql = '''select * from [Flare-Fund]..IMSubFundInfo where PMID={0} and AssetType in ({1})'''.format(pm_id, str(asset_type_list)[1:-1])
        res_df = self.get_df(sql)
        return res_df

    def get_im_sub_fund_info_tmp(self, pm_id, asset_type_list=None):
        sql = ''' select * 
                  FROM [Flare-PM]..VW_SubInfo_SubStrategy
                where PMID={0} '''.format(pm_id)
        
        if asset_type_list is not None:
            sql += ''' and AssetType in ({1})'''.format( str(asset_type_list)[1:-1])
        res_df = self.get_df(sql)
        return res_df


    def get_fund_list_yield(self, fund_list, frequency, yield_type_pair=(1,1)):
        yield_type, yield_sub_type = yield_type_pair
        sql = '''select FundID, TradingDay, Yield from FundYield where FundID in ({0}) and Period={1} and YieldType={2} and YieldSubType={3} order by FundID, TradingDay'''.format(str(fund_list)[1:-1], frequency, yield_type, yield_sub_type)
        res_df = self.get_df(sql)
        if res_df is not None and len(res_df) > 0:
            res_df.index = res_df.TradingDay.values
            return res_df

    def get_fund_list_yield_from_mix(self, fund_list, frequency, yield_type_pair=(1,1)):
        yield_type, yield_sub_type = yield_type_pair
        sql = '''select FundID, TradingDay, Yield, NetValue from [Flare-Mix]..FundYield where FundID in ({0}) and Period={1} and YieldType={2} and YieldSubType={3} order by FundID, TradingDay'''.format(str(fund_list)[1:-1], frequency, yield_type, yield_sub_type)
        res_df = self.get_df(sql)
        if res_df is not None and len(res_df) > 0:
            res_df.index = res_df.TradingDay.values
            res_df.drop_duplicates(subset=['FundID', 'TradingDay'], keep='first', inplace=True)
            return res_df

    def get_fund_list_yield_by_pmid(self, pm_id, frequency, yield_type_pair=(1,1)):
        yield_type, yield_sub_type = yield_type_pair
        sql = '''select FundID, TradingDay, Yield from FundYield where FundID in (select distinct FundID from VW_FundCategoryList where PMID={0}) and Period={1} and YieldType={2} and YieldSubType={3} order by FundID, TradingDay'''.format(pm_id, frequency, yield_type, yield_sub_type)
        res_df = self.get_df(sql)
        if res_df is not None and len(res_df) > 0:
            res_df.index = res_df.TradingDay.values
            return res_df

    def get_fund_list_asset_ratio(self, fund_list, frequency):
        sql = '''select FundID, TradingDay, ExposureType, ExposureSubType, RollNum, AssetOrientation, Beta, SingleAssetYield from FundAssetRatio where FundID in ({0}) and Frequency={1} order by FundID, TradingDay'''.format(str(fund_list)[1:-1], frequency)
        res_df = self.get_df(sql)
        if res_df is not None and len(res_df) > 0:
            res_df.index = res_df.TradingDay.values
            return res_df

    def get_fund_list_asset_ratio_by_pmid(self, pm_id, frequency):
        sql = '''select FundID, TradingDay, ExposureType, ExposureSubType, RollNum, AssetOrientation, Beta, SingleAssetYield from FundAssetRatio where FundID in (select distinct FundID from VW_FundCategoryList where PMID={0}) and Frequency={1} order by FundID, TradingDay'''.format(pm_id, frequency)
        res_df = self.get_df(sql)
        if res_df is not None and len(res_df) > 0:
            res_df.index = res_df.TradingDay.values
            return res_df

    def get_fund_list_asset_adjust(self, fund_list, frequency):
        sql = '''select FundID, AdjustDay, ExposureType, ExposureSubType, BetaDiff, AssetYield from FundAssetAdjust where FundID in ({0}) and Frequency={1} order by FundID, AdjustDay'''.format(str(fund_list)[1:-1], frequency)
        res_df = self.get_df(sql)
        if res_df is not None and len(res_df) > 0:
            return res_df

    def get_fund_list_asset_adjust_by_pmid(self, pm_id, frequency):
        sql = '''select FundID, AdjustDay, ExposureType, ExposureSubType, BetaDiff, AssetYield from FundAssetAdjust where FundID in (select distinct FundID from VW_FundCategoryList where PMID={0}) and Frequency={1} order by FundID, AdjustDay'''.format(pm_id, frequency)
        res_df = self.get_df(sql)
        if res_df is not None and len(res_df) > 0:
            return res_df

    def get_label_data_yield_dict(self, label_list, deltanum=1000):
        yield_type = 1
        yield_sub_type = 1
        frequency = 2

        label_data_yield_dict = {}
        sql = 'SELECT distinct DataID, LabelID FROM [Flare-Fund]..FundLabelSystem where LabelID in ({0}) and DataType=1'.format(str(label_list)[1:-1])
        df = self.get_df(sql)
        if df is None or len(df) == 0:
            return

        for label_id, label_df in df.groupby('LabelID'):
            fund_list = label_df.DataID.tolist()
            len_fund = len(fund_list)

            res_list = []
            for i in range(0, len_fund, deltanum):
                fund_list_sub = fund_list[i: i+deltanum]
                sql = '''select FundID,TradingDay,NetValue,Yield from [Flare-Fund].dbo.FundDeriveYield
                    where YieldType={0} and YieldSubType={1} and FundID in ({2}) and Frequency={3} order by FundID,TradingDay'''.format(yield_type, yield_sub_type, str(fund_list_sub)[1:-1], frequency)
                fund_yield_df = self.get_df(sql)
                if fund_yield_df is None or len(fund_yield_df) == 0:
                    continue
                fund_yield_df.index = fund_yield_df.TradingDay
                res_list.append(fund_yield_df)

            if res_list:
                res_df = pd.concat(res_list)
                label_data_yield_dict[label_id] = res_df

        return label_data_yield_dict

    def get_complete_fund_net_value(self, plan_id, pm_id):
        sql = 'select distinct DataID from [flare-fund]..FundLabelSystem where LabelID in (select distinct LabelID from [Flare-Mix]..LabelRankConfig where PlanID={}) and datatype=1'.format(plan_id)
        df = self.get_df(sql)
        if df is None or len(df) == 0:
            return
        fund_list = df.DataID.tolist()

        sql = 'select Frequency from [Flare-Mix]..LabelRankConfig where PlanID={}'.format(plan_id)
        df = self.get_df(sql)
        if df is None or len(df) == 0:
            return
        frequency = df.iloc[0].Frequency
        
        result = self.get_complete_fund_net_value_by_fund_list(fund_list, frequency, pm_id)
        return result

    def get_fund_list_yield_by_RankConfigID(self, pm_id, fund_list, frequency=2):
        config_list = []
        config_df = None
        for fund_id in fund_list:
            config_df = self.get_queryRankConfig(pm_id, fund_id)
            if config_df is not None and len(config_df) > 0:
                config_list.append(config_df)
        if config_list:
            config_df = pd.concat(config_list)
            rank_config_list = config_df.RankConfigID.unique().tolist()

        if config_df is not None:
            result_dict = {}
            for rank_config_id in rank_config_list:
                fund_list = self.get_queryFundList(rank_config_id)
                tmp = self.get_fund_list_derive_yield(fund_list, frequency)
                if tmp is not None and len(tmp) > 0:
                    result_dict[rank_config_id] = tmp
            return result_dict

    def get_complete_fund_net_value_by_fund_list(self, fund_list, frequency, pm_id):
        res_list = []
        for fund_id in fund_list:
            res_df = self.get_netvalue_concat(fund_id, frequency, pm_id)
            if res_df is not None and len(res_df) > 0:
                res_list.append(res_df)

        if res_list:
            result = pd.concat(res_list)
            return result

    def get_netvalue_concat(self, fundid, frequency, pmid):
        dic ={
            'fundid':str(fundid),'frequency':str(frequency),'pmid':str(pmid)
        }
        data = json.dumps(dic)
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            headers = {"Content-Type": "application/json"}
            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                                        app_name='WisdomDB',
                                        # service="/catalog",
                                        service="/wisdomdb/netvalue_concat",
                                        method='POST',
                                        headers=headers,
                                        data=data.encode(),
                                        timeout=2000)
            client.stop()
            res=json.loads(res)
            if 'data' not in res:
                return 

            df = pd.DataFrame(res['data'])
            df.TradingDay = pd.to_datetime(df.TradingDay)
            df.FundID = df.FundID.apply(lambda x: int(x))
            df.Frequency = df.Frequency.apply(lambda x: int(x))
            df.index = df.TradingDay.values
            return df
        except Exception as err:
            print(err)

    def get_pm_fund_list_api(self, pm_id):
        dic ={
            'pm_id':pm_id
        }
        data = json.dumps(dic)
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            headers = {"Content-Type": "application/json"}
            
            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                                        app_name='WisdomDB',
                                        service="/wisdomdb/pm/fundList",
                                        method='POST',
                                        headers=headers,
                                        data=data.encode(),
                                        timeout=2000)
            client.stop()
            df = pd.DataFrame(json.loads(res)['data'])
            fund_list = df.fund_id.tolist()
            return fund_list
        except Exception as err:
            print(err)

    def get_pm_fund_info_api(self, pm_id):
        dic ={
            'pm_id':pm_id
        }
        data = json.dumps(dic)
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            headers = {"Content-Type": "application/json"}
            
            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                                        app_name='WisdomDB',
                                        service="/wisdomdb/pm/fundList",
                                        method='POST',
                                        headers=headers,
                                        data=data.encode(),
                                        timeout=2000)
            client.stop()
            df = pd.DataFrame(json.loads(res)['data'])
            if df is not None and len(df) > 0:
                df = df[['fund_id', 'fund_name', 'fundtype_name', 'race_name', 'source_name', 'source_id', 'family_type']]
                df.rename(columns={'fund_id':'FundID', 'fund_name':'FundName', 'fundtype_name':'FundTypeName', 'race_name':'RaceName', 'source_name':'SourceName', 'source_id':'SourceID', 'family_type':'FamilyType'}, inplace=True)
                return df
        except Exception as err:
            print(err)

    def get_queryRankConfig(self, pm_id=None, fund_id=None):
        dic ={
            'pm_id':pm_id,'fund_id':fund_id
        }
        data = json.dumps(dic)
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            headers = {"Content-Type": "application/json"}
            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                                        app_name='WisdomDB',
                                        service="/wisdomdb/rank/queryRankConfig",
                                        method='POST',
                                        headers=headers,
                                        data=data.encode(),
                                        timeout=2000)
            client.stop()
            config_df = pd.DataFrame(json.loads(res)['data'])
            return config_df
        except Exception as err:
            print(err)

    def get_queryFundList(self, rank_config_id, num=5):
        dic ={
            'rank_config_id': str(rank_config_id)
        }
        data = json.dumps(dic)
        num -= 1
        if num == 0:
            return
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            headers = {"Content-Type": "application/json"}
            
            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                                        app_name='WisdomDB',
                                        service="/wisdomdb/rank/queryFundList",
                                        method='POST',
                                        headers=headers,
                                        data=data.encode(),
                                        timeout=2000)
            client.stop()
            df = pd.DataFrame(json.loads(res)['data'])
            if df is not None and len(df) > 0:
                fund_list = df.FundID.tolist()
            else:
                fund_list = []
            return fund_list
        except Exception as err:
            print(err)
            self.get_queryFundList(rank_config_id, num=num)

    def get_queryRankConfigById(self, rank_config_id):
        dic ={
            'rank_config_id':rank_config_id
        }
        data = json.dumps(dic)
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            headers = {"Content-Type": "application/json"}

            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                                        app_name='WisdomDB',
                                        service="/wisdomdb/rank/queryRankConfigById",
                                        method='POST',
                                        headers=headers,
                                        data=data.encode(),
                                        timeout=2000)
            client.stop()
            res_dict = json.loads(res)['data'][0]
            return res_dict
        except Exception as err:
            print(err)
            raise Exception(err)

    def get_calcScore(self, fund_id, rank_config_id, start_day=None, end_day=None):
        dic ={
            'fund_id':str(fund_id),'rank_config_id':str(rank_config_id),'start_day':start_day,'end_day':end_day
        }
        data = json.dumps(dic)
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            headers = {"Content-Type": "application/json"}
            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                                        app_name='WisdomDB',
                                        service="/wisdomdb/rank/calcScore",
                                        method='POST',
                                        headers=headers,
                                        data=data.encode(),
                                        timeout=2000)
            client.stop()
            resp = json.loads(res)
            code = resp['code']
            if 0 == code: # success
                res_dict = eval(resp['data'])
                norm_dict = res_dict['norm']

                res_dict = {}
                for key in ['total', 'performance_overall', 'asset_overall', 'scene_overall', 'alpha_overall', 'allocation_overall', 'commodity_allocation_overall']:
                    if key in norm_dict:
                        if norm_dict[key] is None or norm_dict[key] == 'NaN':
                            continue
                        res_dict[key] = norm_dict[key]
                return res_dict
            else: # error
                print(res)
        except Exception as err:
            print(err)

    def calcRankIndexValue(self, fund_id, frequency, rank_config_id, for_rank_data, start_day=None, end_day=None):
        params = {}
        for tabel_name, tmp_list in for_rank_data.items():
            df = pd.DataFrame(tmp_list)
            for_rank_data[tabel_name] = json.loads(df.to_json(orient='records', force_ascii=False))
        params['index_value_dict'] = for_rank_data
        params['rank_config_id'] = rank_config_id
        params['fund_id'] = fund_id
        params['frequency'] = frequency
        params['start_day'] = start_day
        params['end_day'] = end_day

        data = json.dumps(params)

        rank_table_dict = {
            'FundFactorExposure': 'FundFactorExposureRank',
            'FundRollStatis4Rank': 'FundRollStatisRank',
            'SceneMarketStatis': 'SceneStatisRank',
            'Residual': 'FundCTARank',
            'FundAnnualStatis4Rank': 'FundAnnualStatisRank'
        }

        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            # url = 'http://10.168.30.133:9700'
            # path = '/rank/calcRankIndexValue'
            # headers = {"Content-Type": "application/json"}
            # res = requests.post(url=url+path, data=json.dumps(params), headers=headers)
            # resp = json.loads(res.text)

            headers = {"Content-Type": "application/json"}
            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                app_name='WisdomDB',
                service="/wisdomdb/rank/calcRankIndexValue",
                method='POST',
                headers=headers,
                data=data.encode(),
                timeout=2000)
            client.stop()
            resp = json.loads(res)

            code = resp['code']
            if 0 == code: # success
                res_dict = resp['data']
                for table_name, sub_dict in res_dict.items():
                    res_df = pd.DataFrame(sub_dict)
                    self.upsert_df(res_df, rank_table_dict[table_name])
            else: # error
                print(res)
        except Exception as err:
            print(err)
            raise Exception(err)

    def get_calcPortfolioScore(self, fund_id_list, rank_config_id, start_day=None, end_day=None):
        fund_id_list=[int(fund_id) for fund_id in fund_id_list]
        rank_config_id=int(rank_config_id)
        dic ={
            'fund_id_list':fund_id_list,'rank_config_id':rank_config_id,'start_day':start_day,'end_day':end_day
        }
        data = json.dumps(dic)
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            headers = {"Content-Type": "application/json"}
            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                                        app_name='WisdomDB',
                                        service="/wisdomdb/rank/calcPortfolioScore",
                                        method='POST',
                                        headers=headers,
                                        data=data.encode(),
                                        timeout=2000)
            client.stop()
            resp = json.loads(res)
            code = resp['code']
            if 0 == code: # success
                res_dict = eval(resp['data'])
                norm_dict = res_dict['norm']

                res_dict = {}
                for key in ['total', 'performance_overall', 'asset_overall', 'scene_overall', 'alpha_overall', 'allocation_overall', 'commodity_allocation_overall']:
                    if key in norm_dict:
                        if norm_dict[key] is None or norm_dict[key] == 'NaN':
                            continue
                        res_dict[key] = norm_dict[key]
                return res_dict
            else: # error
                print(res)
        except Exception as err:
            print(err)

    def get_queryRankConfigList(self, frequency, group_type, source_id, analyze_config_id, pm_id=None):
        dic ={
            'group_type':group_type, 'source_id':source_id, 'frequency':frequency, 'analyze_config_id':str(analyze_config_id)[1:-1]
        }
        if pm_id is not None:
            dic['pm_id'] = pm_id
        data = json.dumps(dic)
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            headers = {"Content-Type": "application/json"}
            
            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                                        app_name='WisdomDB',
                                        service="/wisdomdb/rank/queryRankConfigListAfterInvestment",
                                        method='POST',
                                        headers=headers,
                                        data=data.encode(),
                                        timeout=2000)
            client.stop()
            data_list = json.loads(res)['data']
            rank_config_list = []
            for data in data_list:
                rank_config_list.append(int(data['RankConfigID']))
            return rank_config_list
        except Exception as err:
            print(err)

    def editQuantativeScoreRecordCalStatus(self, score_record_id, cal_status):
        dic ={
            'score_record_id':score_record_id, 'cal_status':cal_status
        }
        data = json.dumps(dic)
        from py_eureka_client.eureka_client import EurekaClient as eureka_client
        try:
            headers = {"Content-Type": "application/json"}

            client = eureka_client(eureka_server=url, app_name='WisdomDB', should_register=False)
            client.start()
            res = client.do_service(
                                        app_name='WisdomDB',
                                        service="/wisdomdb/rank/editQuantativeScoreRecordCalStatus",
                                        method='POST',
                                        headers=headers,
                                        data=data.encode(),
                                        timeout=2000)
            client.stop()
            msg = json.loads(res)['msg']
            return msg
        except Exception as err:
            print(err)