import os 
from sys import getsizeof
from  utility import download_and_extract_json as dej,write_json_data as wjd
import psutil
from socket import gaierror
from  requests.exceptions import ConnectionError as CE 
from datetime import datetime, timedelta

print_interval = timedelta(minutes=5)

working_dir = os.path.dirname(os.path.realpath(__file__))
master_file_list  = working_dir + "/dataset/MASTERFILELIST.TXT"
MAX_MEMORY = 13 * 1024 * 1024 * 1024  # 17 GB in bytes

with  open(master_file_list  ,'r') as  file :
    download_list  = file.read().split("\n")

output_dict = dict()
output_content  = list()
next_print = datetime.now()

for file in download_list:
    try:
        # Check current memory usage
        memory_usage = psutil.virtual_memory().used
        now = datetime.now()
        if now >= next_print:
                next_print = now + print_interval
                print("Memory Usage",(((memory_usage/1024)/1024)/1024))
        if memory_usage > MAX_MEMORY:
            print(f"Memory usage exceeded 11GB. Stopping execution.")
            break
        output_content.extend(dej(file))
    except CE as e:
        # Check if it's a NameResolutionError
        if isinstance(e.args[0], gaierror):
            print(f"Skipping file due to name resolution failure: {file}")
            continue  
    except Exception as error_msg:
        raise error_msg

output_dict["values"] =output_content
wjd(output_dict,"/root/docker_dataset/")

# 830