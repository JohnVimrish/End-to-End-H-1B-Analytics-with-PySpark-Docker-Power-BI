import  requests 
import  tarfile
import gzip
import json
from os import listdir
from os.path import isfile, join
import re 
import re

def match_file_type(file_name, pattern):
    regex = re.compile(pattern)
    if regex.match(file_name):
        return True
    return False


pattern = r"/(\d{6}\d+\.geg-gcnlapi\.json)"


def  download_extract_tar_file (url, extract_path) :
    with requests.get(url , stream=True) as  rx  , tarfile.open(fileobj=rx.raw  , mode="r:gz") as  tarobj  : 
            tarobj.extractall(path=extract_path) 

def download_and_extract_json(url):
    with requests.get(url, stream=True) as rx ,gzip.GzipFile(fileobj=rx.raw) as gz_file:
        file_content = gz_file.read()
        return  [json.loads(values)   for values in file_content.decode('utf-8').split("\n") if not values =='' ]
            # match = re.search(pattern, url)
            # file_name  =  match.group(1)  if match else 'data.json'

def  write_json_data (json_data,destination) :
    with open(f'{destination}/big_data.json', 'w') as out_file:
        json.dump(json_data, out_file)


def  return_files_list ( directory:str, file_type=None) :
   return   [f for f in listdir(directory) if (isfile(join(directory, f)))] if file_type is None else [f for f in listdir(directory) if (isfile(join(directory, f)) and match_file_type(f,file_type))]
