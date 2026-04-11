
import os,sys,time
import datetime as dt

if '_main_' in __name__: 
    root_path =os.path.abspath(os.path.dirname(sys.argv[0]))+'/../..' 
    sys.path.append(root_path)
else:
    root_path='.'

from calc_asset_ratio import calc_asset_ratio


if __name__=='__main__':
    os.chdir(root_path)
    from pycloud import fast_example
    source_dir_list =['calc_asset_ratio'] 
    fast_example.register_service_singleton(calc_asset_ratio,'tcp://172.16.10.75:2233',source_list=source_dir_list,service_name='calc_asset_ratio') 