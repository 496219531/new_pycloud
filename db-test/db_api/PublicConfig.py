from multiprocessing import cpu_count
app_id='FlareAlgorithm'
config_url='http://10.168.30.223:8080'

#release
Queue_ZMQ_IP='10.168.20.16'
#debug
# Queue_ZMQ_IP='127.0.0.1'

Queue_ZMQ_PORT='5125'
Queue_ZMQ_ADDR_list=['tcp://{}:{}'.format(Queue_ZMQ_IP, Queue_ZMQ_PORT)]
Queue_ZMQ_ADDR=Queue_ZMQ_ADDR_list[0]

Queue_ZMQ_Server_PORT='5126'
Queue_ZMQ_Server_ADDR='tcp://{}:{}'.format(Queue_ZMQ_IP, Queue_ZMQ_Server_PORT)

HDF2DB_Address='tcp://127.0.0.1:31315'
Analyzer_Work_Num=min(cpu_count(),10)
white_worker_ip_list=None

data_api_ip='10.168.40.61'
data_api_port='1433'

rbq_data_api_ip = '10.168.30.81'
rbq_data_api_port = '5672'
rbq_data_api_user = 'guest'
rbq_data_api_pwd = 'guest'
rbq_data_api_database = 'RabbitMQ'
rbq_data_ap_host = rbq_data_api_ip + ':' + rbq_data_api_port

DB_Address_list=['{},{}'.format(data_api_ip,data_api_port)]
source_db_address=DB_Address_list[0]
# source_db_address='10.168.40.81,14666'
# source_db_address='10.168.30.159,1433'
public_db_address='10.168.20.22,1433'
# public_db_address='10.168.40.61,1433'
realtime_channel='touyan_realtime'
data_api_host=source_db_address
source_id_2_db_map={1:'Flare-Value-MF',2:'Flare-Value-HF',1.1:'Flare-Manage-MF',1.2:'Flare-Mix'}
