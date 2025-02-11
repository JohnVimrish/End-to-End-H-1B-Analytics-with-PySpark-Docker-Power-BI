from concurrent.futures import ThreadPoolExecutor, as_completed
from root.commonvariables import CommonVariables as common_var


class ThreadExecution () :

    def __init__(self,) :

        self.thread_name = 'ETL Thread'        
        self.table_etl_exec_dict   = dict() 
        self.process_table_list    = list()                 
        self.process_table_list.append(target_table)



    def process_etl(    self, 
                        max_threads_num,
                        db_obj):
            
            table_etl_status = list ()
            FAILED= common_var.status_failed
            SUCCESS=common_var.status_successfull

            with ThreadPoolExecutor(max_workers = max_threads_num, 
                                    thread_name_prefix = self.thread_name) as executor:

                #perform ETL for each table                 
                futures = [executor.submit(self.table_etl_exec_dict[table].perform_etl,
                                                    load_type,
                                                    db_obj,
                                                    group_prune_hour )
                                                    for table in self.process_table_list]
           
                # process each result as it is available
                for future_output in as_completed(futures):
                    log_statement  = dict (group_name = group_name,group_id=group_seq_int,load_type= load_type,failure_reason=None)
                    try :
                        ## output of each table being processed
                        thread_output= future_output.result()
                        for  table , op_info in thread_output.items() :
                            load_result    = op_info[common_var.result_of_load]
                            load_type      = op_info[common_var.load_type]
                            row_cnt        = op_info[common_var.num_of_rows_processed]
                            table_etl_status.append(load_result)
                            
                    except Exception as exc:
                         table_etl_status.append(FAILED)
                         raise (f'Thread enabling exception: {exc}')
            return FAILED if FAILED in  table_etl_status  else SUCCESS
