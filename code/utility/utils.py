import  requests 
import  tarfile
import gzip
import json
from os import listdir
from os.path import isfile, join
import re 
from  root.commonvariables import CommonVariables




class  Common_Utils_Operations  () :
    

    def match_file_type(file_name, pattern):
        regex = re.compile(pattern)
        if regex.match(file_name):
            return True
        return False

    def  download_extract_tar_file (url, extract_path) :
        with requests.get(url , stream=True) as  rx  , tarfile.open(fileobj=rx.raw  , mode="r:gz") as  tarobj  : 
                tarobj.extractall(path=extract_path) 

    def download_and_extract_json(url):
        with requests.get(url, stream=True) as rx ,gzip.GzipFile(fileobj=rx.raw) as gz_file:
            file_content = gz_file.read()
            return  [json.loads(values) for values in file_content.decode('utf-8').split("\n") if not values =='' ]
        
    def  return_files_list ( directory:str, file_type=None) :
        return   [f for f in listdir(directory) if (isfile(join(directory, f)))] if file_type is None else [f for f in listdir(directory) if (isfile(join(directory, f)) and match_file_type(f,file_type))]

    def check_is_not_empty_list (input_list :list) :
        return  True   if input_list else False

    def format_operation(inp_str, format_fillnvalues) :
        if type(format_fillnvalues) is str  :
            return inp_str.format(format_fillnvalues)
        elif type(format_fillnvalues) is dict  :
            return inp_str.format_map(format_fillnvalues)

    def derive_actual_directory (to_concat_path) :
        return CommonVariables.etl_project_directory + to_concat_path

    def derive_table_config_actual_directory (to_concat_path,json_file) :
        return CommonVariables.etl_project_directory  + to_concat_path + json_file
    

