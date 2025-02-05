from datetime import datetime as dt
import sys
import traceback
from os.path import dirname as directoryname, realpath
from root.commonvariables import CommonVariables
from jsonmanipulations.configparametervalue import ConfigParametersValue as CPV
from jsonmanipulations.jsontagvariables import JsonTagVariables as JsonTagVar
from jsonmanipulations.jsonvalueextract import JsonManupulatorNModifier as JsonMNM



def main_function():

    config_json_object                             = JsonMNM(sys.argv[1])
    spark_configuration                            = JsonMNM(sys.argv[2])
    database_configuration                         = JsonMNM.read_config_file(sys.argv[3])
    JsonTagVar.initialize_json_tag_variables()
    CPV.initialize_configuration_parameter(config_json_object, spark_configuration, database_configuration)               
    try:

        start_time = dt.now()
        print('ETL Start Time : {} .'.format(start_time))
        
            
    except Exception as error:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = list()
        for trace in trace_back:
           stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" %(trace[0], trace[1], trace[2], trace[3]))
        message = '\n'.join(["Exception type : %s " % ex_type.__name__,
                             "Exception message : %s" %ex_value ,
                             "Stack trace : %s" %stack_trace])
        raise message

if __name__ == "__main__":
    CommonVariables.etl_project_directory = directoryname(directoryname(directoryname(realpath(__file__))))
    main_function()