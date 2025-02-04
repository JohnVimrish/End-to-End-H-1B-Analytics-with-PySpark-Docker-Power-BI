from datetime import datetime as dt
import sys
import traceback
from os.path import dirname as directoryname, realpath
from database.dbinitialisers import DatabaseInitialiser
from jsonmanipulations.jsontagncommonvariables import JsonTagAndCommonVariables
from jsonmanipulations.jsonvalueextract import JsonValueExtractor
from root.fusioncommonvariables import CommonVariables
from jsonmanipulations.configparametervalue import ConfigParametersValue


def main_function():

    config_json_object                             = JsonValueExtractor(sys.argv[1])
    connection_json_object                         = JsonValueExtractor(sys.argv[2])
    storage_connection_object                      = JsonValueExtractor(sys.argv[3])
    fusion_jobs_object                             = JsonValueExtractor(sys.argv[4])
    log_level_object                               = JsonValueExtractor(sys.argv[5])
    table_groups_object                            = JsonValueExtractor(sys.argv[6])
    load_missing_viewobjects_json_object           = JsonValueExtractor(sys.argv[7])
    bipublisher_individual_report_configuration    = JsonValueExtractor(sys.argv[8])
    import_etl_dependent_csv_configuration         = JsonValueExtractor(sys.argv[9])
    job_scheduling_parameter_inputs_conf           = JsonValueExtractor(sys.argv[10])

    ConfigParametersValue(config_json_object, connection_json_object, storage_connection_object, 
                          fusion_jobs_object,
                          log_level_object,
                          table_groups_object,
                          load_missing_viewobjects_json_object,
                          bipublisher_individual_report_configuration,
                          import_etl_dependent_csv_configuration,
                          job_scheduling_parameter_inputs_conf)
                          

    try:
        main_logging_obj = LoggingUtil(
            ConfigParametersValue.log_base_directory
            )
        FusionMainLogger = main_logging_obj.setup_logger(
            CommonVariables.main_config, ConfigParametersValue.fusion_main_log_level)
        processid  = int (dt.now().strftime(ConfigParametersValue.main_log_file_name_format))
        main_log_file = f'{processid}.log'
        main_logfile = main_logging_obj.create_log_file(main_log_file)
        main_logging_obj.link_logger_filehandler(FusionMainLogger,main_logfile)
        start_time = dt.now()
        FusionMainLogger.info('ETL Start Time : {} .'.format(start_time))
        FusionMainLogger.info(f'Process ID : {processid}, shall be usefull to query logs in DB')
        main_logging_obj.init_program_process_id(processid)
        main_logging_obj.init_logging_queries(ConfigParametersValue.table_level_log_input_query ,
                                              ConfigParametersValue.download_obj_level_log_input_query, 
                                              ConfigParametersValue.overall_etl_log_insert_query,
                                              ConfigParametersValue.download_obj_requestid_fetch_query,
                                              ConfigParametersValue.download_obj_summary_query,
                                              ConfigParametersValue.table_etl_requestid_fetch_query,
                                              ConfigParametersValue.table_etl_summary_recovered_flow_query,
                                              ConfigParametersValue.table_etl_summary_normal_flow_query,
                                              ConfigParametersValue.overall_summary_query)

        ObjJSONnCommonVariables = JsonTagAndCommonVariables ()
        ObjJSONnCommonVariables.log_levels = ConfigParametersValue.all_packages_log_level
        FusionMainLogger.info("Connection Pool Establishment starts!")
        ObjDBInitialiser        = DatabaseInitialiser(ConfigParametersValue.dbconnectors,
                                                            main_logging_obj,
                                                            main_logfile,
                                                            ObjJSONnCommonVariables,
                                                            ConfigParametersValue.ora_instant_client_dir,
                                                            ConfigParametersValue.oracle_connection_retry_count)
        FusionMainLogger.info("Connection Pool Establishment ends!")
        logdbconnection = ObjDBInitialiser.init_db_connection(
                                                  ConfigParametersValue.log_db_connection_name,
                                                  FusionMainLogger,
                                                  main_logfile ,
                                                  main_logging_obj,
                                                  'Logger DB' )
        if logdbconnection is not None : 

            main_logging_obj.init_logdb_connection(logdbconnection)                                                                                       
            config_json_object.reinitialise_logger_object(
                main_logging_obj, CommonVariables.main_config, main_logfile, ConfigParametersValue.json_value_extc_log_level)
            connection_json_object.reinitialise_logger_object(
                main_logging_obj, CommonVariables.connection_param, main_logfile, ConfigParametersValue.json_value_extc_log_level)
            storage_connection_object.reinitialise_logger_object(
                main_logging_obj, CommonVariables.storage_conn, main_logfile, ConfigParametersValue.json_value_extc_log_level)
            fusion_jobs_object.reinitialise_logger_object(
                main_logging_obj, CommonVariables.fusion_job_config, main_logfile, ConfigParametersValue.json_value_extc_log_level)
            log_level_object.reinitialise_logger_object(
                main_logging_obj, CommonVariables.log_level_config, main_logfile, ConfigParametersValue.json_value_extc_log_level)
            table_groups_object.reinitialise_logger_object(
                main_logging_obj, CommonVariables.table_config, main_logfile, ConfigParametersValue.json_value_extc_log_level)
            load_start_mail_subject = ConfigParametersValue.email_subjectline.format('has Started .')
            # email_obj.send_load_start_viamail(load_start_mail_subject)
            ObjDownloadProcess = DownloadProcess(main_logging_obj, main_logfile)
            DownloadProcessStatus = ObjDownloadProcess.download_NProcess_jobs(ObjDBInitialiser,processid)
            end_time  = dt.now()
            FusionMainLogger.info('ETL End Time : {} .'.format(end_time))
            main_logging_obj.log_program_etl(dict(group_name = 'Complete Load' , group_id = -20, 
                                            status = DownloadProcessStatus , group_execution_start_time = start_time  ,
                                            group_execution_end_time  = end_time))
            Overall_summary_file_path, Attachments = main_logging_obj.generate_summary_log_files(ConfigParametersValue.log_summary_directory,
                                                        ConfigParametersValue.log_etl_summary_file,
                                                        ConfigParametersValue.log_download_summary_file,
                                                        ConfigParametersValue.log_table_summary_file)
            ObjDBInitialiser.release_connection(main_logging_obj.ObjLogDbConnection)
            ObjDBInitialiser.close_connections()
            
    except Exception as error:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = list()
        for trace in trace_back:
           stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" %(trace[0], trace[1], trace[2], trace[3]))
        email_message = '\n'.join(["Exception type : %s " % ex_type.__name__,
                             "Exception message : %s" %ex_value ,
                             "Stack trace : %s" %stack_trace])
        raise email_message

if __name__ == "__main__":
    CommonVariables.etl_project_directory = directoryname(directoryname(directoryname(realpath(__file__))))
    main_function()
