from pycloud.dbsynchelper import DataApiParams
# from Api.apollo.apollo_client import ApolloClient
# from Config import app_id,config_url

# client = ApolloClient(app_id=app_id,config_url=config_url)

import time

db_info_list=[
    {
        'data_api_ip':'10.168.30.159',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.30.50',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'F1are123456',
    },

    {
        'data_api_ip':'10.168.30.99',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.40.61',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.30.44',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'F1areDataC0ntro1',
    },

    {
        'data_api_ip':'10.168.30.50',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.20.84',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },


    {
        'data_api_ip':'10.168.30.70',
        'data_api_port':'1433',
        'data_api_user':'Itadmin',
        'data_api_pwd' : 'It123456',
    },

    {
        'data_api_ip':'10.168.30.22',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.40.81',
        'data_api_port':'1433',
        'data_api_user':'sas',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.40.81',
        'data_api_port':'14666',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.30.9',
        'data_api_port':'49233',
        'data_api_user':'sa',
        'data_api_pwd' : 'FlareTest123',
    },

    {
        'data_api_ip':'10.168.30.70',
        'data_api_port':'14555',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.30.70',
        'data_api_port':'14333',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.20.26',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.20.22',
        'data_api_port':'1433',
        'data_api_user':'kettle',
        'data_api_pwd' : '111222',
    },

    {
        'data_api_ip':'10.168.20.33',
        'data_api_port':'14333',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.30.80',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

     {
        'data_api_ip':'10.168.20.26',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    }, 

    {
        'data_api_ip':'10.168.20.25',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.20.87',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.40.11',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },

    {
        'data_api_ip':'10.168.30.83',
        'data_api_port':'1433',
        'data_api_user':'sa',
        'data_api_pwd' : 'Flare123456',
    },
]

db_cfg_dict={}
for db_info in db_info_list:
    data_api_ip=db_info['data_api_ip']
    data_api_port=db_info['data_api_port']

    DB_Address='{},{}'.format(data_api_ip,data_api_port)
    db_cfg_dict[DB_Address]=db_info

def get_value_from_apollo(key):
    value=client.get_value(key,None)
    if value is not None:
        value=eval(value)
        return value

def get_dbcfg(db_address,dbname):
    # db_info=get_value_from_apollo(db_address)
    # if db_info is None:
    #     if db_address not in db_cfg_dict:
    #         print('can not find {} db info'.format(db_address))
    #         time.sleep(5)
    #         return
    db_info=db_cfg_dict[db_address]
    dbcfg=DataApiParams(**db_info,
                        data_api_database=dbname)
    return dbcfg

def get_db_account(db_ip,db_port):
    db_address=f'{db_ip},{db_port}'
    if db_address not in db_cfg_dict:
        raise Exception(f'未找到数据库{db_address}账户信息')
    db_info=db_cfg_dict[db_address]
    driver=db_info.get('driver')
    if driver is None:
        driver='mssql'
    return db_info['data_api_user'],db_info['data_api_pwd'],driver
