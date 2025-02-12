from concurrent.futures import ThreadPoolExecutor, as_completed
from root.commonvariables import CommonVariables as common_var
from core. etl_job import ETLJob as EJ 
from jsonmanipulations.jsontagvariables import JsonTagVariables as JTV


class ThreadExecution () :

    def __init__(self, etl_table_properties:list , db_obj,spark_obj ) :
        
        self.thread_name = 'ETL Thread'        
        self.table_etl_exec_array    =  list()
        mtp= 0 
        for each_item in etl_table_properties :

             if each_item [JTV.dwh_tables_config_multiple_table_present] :
                  table_name  = f"Multiple_Table_Components-{mtp}"
                  mtp +=1
             else :
                  for one_input  in  each_item [JTV.dwh_tables_config_table_mapping].keys() :
                        table_name =   one_input
             etl_job_obj  = EJ(spark_obj, db_obj, each_item, table_name)
             self.table_etl_exec_array.append(etl_job_obj)


    def process_etl(    self, 
                        max_threads_num):
        print (max_threads_num)
        try :
            with ThreadPoolExecutor(max_workers = max_threads_num, 
                                    thread_name_prefix = self.thread_name) as executor:

                #perform ETL for each table                 
                futures = [executor.submit(table_obj.process_etl)
                                                    for table_obj in self.table_etl_exec_array]
           
                # process each result as it is available
                for future_output in as_completed(futures):
                    try :
                        ## output of each table being processed
                        thread_output= future_output.result()                            
                    except Exception as exc:
                         raise (f'Thread enabling exception: {exc}')
                    quit()
        except  Exception as error  :
             raise error 