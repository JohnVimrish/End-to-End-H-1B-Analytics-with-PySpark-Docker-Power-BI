from database.postgresconnector import PostgresConnection
from core.spark_utilities import spark_utilities
from utility.pandas_utilities import Pandas_Utilities
from jsonmanipulations.configparametervalue import ConfigurationParametersValue as CPV
from jsonmanipulations.jsontagvariables import JsonTagVariables as JsonTV
from root.commonvariables import CommonVariables as common_var
import sys
import traceback
import typing as t

class ETLJob:
    def __init__(self, spark_obj:spark_utilities, db_obj:PostgresConnection, table_properties:dict, table_name):
        self.table_name  = table_name 
        self.spark_util_obj = spark_obj
        self.db_obj = db_obj
        self.pandas_util = Pandas_Utilities()
        self.table_properties = table_properties
        self.table_name = table_name

    def handle_exception(self, func_name, exception):
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))
        message = '\n'.join([
            f"Error in function {func_name}:",
            "Exception type : %s " % ex_type.__name__,
            "Exception message : %s" % ex_value,
            "Stack trace : %s" % stack_trace,
            "Exception : %s" % exception
        ])
        raise RuntimeError(message)

    def process_csv_through_spark(self,) :
            def __process_inputs (csv_file:str):
                return self.spark_util_obj.func_csv_dataframe(csv_file)

            try  :
                self.spark_util_obj.func_set_read_options(self.spark_util_obj.func_set_infer_schema())
                self.spark_util_obj.func_set_read_options(self.spark_util_obj.func_set_header())
                
                if self.table_properties[JsonTV.dwh_tables_config_multiple_table_present]:

                    for csv_file in self.table_properties[JsonTV.dwh_tables_config_csv_input]:
                        if 'spark_dataframe' in locals()  :
                            spark_dataframe = spark_dataframe.union(__process_inputs(CPV.tables_input_folder_location+"/"+csv_file))
                        else:
                            spark_dataframe = __process_inputs(CPV.tables_input_folder_location+"/"+csv_file)
                else:
                    spark_dataframe =  __process_inputs(CPV.tables_input_folder_location+"/"+self.table_properties[JsonTV.dwh_tables_config_csv_input])
                spark_dataframe = self.spark_util_obj.func_repartion_dataframe(spark_dataframe, CPV.repartition_spark_dft)
                self.spark_util_obj.func_cache_dft_data(spark_dataframe, CPV.memory_cache)
                if self.table_properties[JsonTV.dwh_tables_config_alias_query] :
                    spark_dataframe = self.spark_util_obj.func_re_alias_column_names_with_query(spark_dataframe,self.table_properties[JsonTV.dwh_tables_config_alias_query])
                spark_dataframe_altered  = self.spark_util_obj.func_exclude_columns_from_dft(spark_dataframe,CPV.rmv_sprk_dft_col_starging_with)
                self.spark_util_obj.write_spark_dft_to_db(spark_dataframe_altered,CPV.target_db_url,CPV.target_db_properties,self.table_name, self.table_properties[JsonTV.dwh_tables_config_write_mode], CPV.target_db_driver)
            
            except Exception as e:
                 self.spark_util_obj.handle_spark_exception(self.process_csv_through_spark().__name__,e)


    def process_input_through_pandas(self, db_obj:PostgresConnection):

        def __extract_copy_command  (dataframe, table_name) :
             return   f"""COPY {table_name} ({", ".join([col for col in dataframe.columns])}) FROM STDIN WITH CSV"""
        
        def  __execute_copy_command_in_db(dataframe, table_name, alias_inputs:dict) :
            dft_renamed = self.pandas_util.rename_columns(alias_inputs,dataframe)
            db_obj.copy_command_executor(__extract_copy_command(dft_renamed,table_name),self.pandas_util.convert_to_inmemory_csv_obj(dft_renamed))

        try :
            excel_input   = CPV.tables_input_folder_location+"/"+self.table_properties[JsonTV.dwh_tables_config_xlsx_input]
            excel_use_cols = self.table_properties[JsonTV.dwh_tables_config_xlsx_input_usecols]
            sheet_name = self.table_properties[JsonTV.dwh_tables_config_xlsx_input_sheet]

            if self.table_properties[JsonTV.dwh_tables_config_multiple_table_present]:
                tables_nsheet = [table for table in self.table_properties[JsonTV.dwh_tables_config_multiple_table_input_present].values ()]
                dataframe_dict  = self.pandas_util.extract_multiple_tables(excel_input,sheet_name,tables_nsheet,excel_use_cols)
                for table_name, table_key in self.table_properties[JsonTV.dwh_tables_config_table_mapping].items():
                      if table_key in dataframe_dict:
                            __execute_copy_command_in_db(dataframe_dict[table_key],table_name,self.table_properties[JsonTV.dwh_tables_config_alias_query][table_name])
                      else  :
                          raise ( table_name,table_key, "Not present in the  code")
            else:
                dataframe = self.pandas_util.read_excel(excel_input, sheet_name, usecols=excel_use_cols) 
                table_name  = "".join([ table_name for table_name in self.table_properties[JsonTV.dwh_tables_config_table_mapping].keys()])
                __execute_copy_command_in_db(dataframe_dict[table_key],table_name,self.table_properties[JsonTV.dwh_tables_config_alias_query])          
        except Exception as e:
                 self.handle_exception(self.process_input_through_pandas().__name__,e)
        
    def process_etl(self,):
        
        try:
            
            self.db_obj.acquire_connection()
            # Execute prequeries if specified
            if  self.table_properties[JsonTV.dwh_tables_config_execute_prequery] :
                for query in t.Union(self.table_properties[JsonTV.dwh_tables_config_prequery],[]):
                    self.db_obj.execute_sql(query, "")

            # Process CSV or Excel files
            if self.table_properties [JsonTV.dwh_tables_config_use_dft_of] == common_var.spark_dft and self.table_properties [JsonTV.dwh_tables_config_input_type] == common_var.csv_file_type:
                self.process_csv_through_spark()

            elif self.table_properties [JsonTV.dwh_tables_config_use_dft_of] == common_var.pandas_dft and self.table_properties [JsonTV.dwh_tables_config_input_type] == common_var.excel_file_type:
                self.process_csv_through_pandas(self.db_obj)
   

            # Execute postqueries if specified
            if  self.table_properties[JsonTV.dwh_tables_config_execute_postquery] :
                for query in t.Union(self.table_properties[JsonTV.dwh_tables_config_postquery],[]):
                    self.db_obj.execute_sql(query, "")

            self.db_obj.release_connection()
        except Exception as e:
            self.handle_exception("process_etl", e)

