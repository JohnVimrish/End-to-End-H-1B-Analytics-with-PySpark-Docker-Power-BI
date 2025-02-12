from jsonmanipulations.configparametervalue import ConfigurationParametersValue as CPV
from core.spark_utilities import spark_utilities
from core.threadexecutor import ThreadExecution as THE
from core.spark_configuration import SparkConfiguration
from database.postgresconnector import PostgresConnection
import sys
import traceback


class trigger_pipeline:
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.PostgresObj  = PostgresConnection()
        self.spark_config_obj = SparkConfiguration()
        self.spark_util_obj = spark_utilities(self.spark_config_obj.fetch_spark_congiration_obj())

    def constructing_job_activities  (self,) :
        thread_executor_obj  = THE(CPV.dwh_tables_config_target_tables_groups,self.PostgresObj,self.spark_util_obj) 
        thread_executor_obj.process_etl(CPV.writing_to_db_no_of_pp)
    def stop_pipeline (self,) :
       self.PostgresObj.close_connection_pool()
       self.PostgresObj.close_connection_pool()

    def start_pipeline(self):
        print(f"Triggering the pipeline {self.pipeline_name}")
        try:
            self.constructing_job_activities()
        except Exception as e:
            self.handle_pipeline_exception("START PiPeLine", e)

    def handle_pipeline_exception(self, func_name, exception):
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))
        message = '\n'.join([
            f"Error in function {func_name}:",
            "Exception type : %s " % ex_type.__name__,
            "Exception message : %s" % ex_value,
            "Stack trace : %s" % stack_trace
        ])
        self.stop_pipeline()
        raise RuntimeError(message)
