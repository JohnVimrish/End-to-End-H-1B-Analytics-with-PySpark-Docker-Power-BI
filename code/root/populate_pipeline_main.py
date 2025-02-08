from datetime import datetime as dt
import sys
import traceback
from os.path import dirname as directoryname, realpath
from root.commonvariables import CommonVariables
from jsonmanipulations.configparametervalue import ConfigurationParametersValue as CPV
from jsonmanipulations.jsontagvariables import JsonTagVariables as JsonTagVar
from jsonmanipulations.jsonvalueextract import JsonManupulatorNModifier as JsonMNM
from database.postgresconnector import PostgresConnection



def main_function():

    config_json_object                             = JsonMNM(sys.argv[1])
    spark_configuration                            = JsonMNM(sys.argv[2])
    database_configuration                         = JsonMNM.read_config_file(sys.argv[3])
    toprocess_tables_list                          = JsonMNM(sys.argv[4])
    JsonTagVar.initialize_json_tag_variables()
    CPV.initialize_configuration_parameter(config_json_object, spark_configuration, database_configuration,toprocess_tables_list)               
    try:
        start_time = dt.now()
        print('ETL Start Time : {} .'.format(start_time))
        print('ETL Process Started.')
        PostgresObj  = PostgresConnection()
        PostgresObj.acquire_connection()
        PostgresObj.execute_sql("Select * from information_schema.tables", "No Log Message")
        print (PostgresObj.get_full_write_records())
        PostgresObj.release_connection()
        PostgresObj.close_connection_pool()
        end_time = dt.now()
        print('ETL Process Completed.')
        print('ETL End Time : {} .'.format(end_time))
        print('Total ETL Process Time : {} .'.format(end_time - start_time))  
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