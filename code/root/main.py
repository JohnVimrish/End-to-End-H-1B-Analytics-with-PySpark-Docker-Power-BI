from core.jsongroupexecution import JSONGroupExecution
from util.fusionloggingutility import LoggingUtil
from util.fileutility import FileUtility
from jsoncustom.jsonvalueextract import JsonValueExtractor
from jsoncustom.jsontagncommonvariables import JsonTagAndCommonVariables
from core.recoverfailedexecutions import ExecutionRecovery
from importcsv.csvimport import CSVImportUsingDTF
from database.dbinitialisers import DatabaseInitialiser
from util.emailsender import EMailSummaryLogAttachments as mail_attachments
import sys
from datetime import datetime


def main_thread():

    config_json_filepath = sys.argv[1]
    connection_json_filepath = sys.argv[2]

    program_timer = dict()

    config_tags = JsonTagAndCommonVariables()
    config_vars = config_tags

    main_log_name = config_vars.main_log_name

    program_timer[config_vars.load_start_time] = datetime.now()

# Initialize Main configuration without logger
    config_obj = JsonValueExtractor(config_json_filepath, main_log_name)

## Initialize all the variables
    config_vars.log_levels = config_obj.json_val(config_vars.log_level_config)
    log_levels = config_vars.log_levels
    main_log_level = log_levels[config_vars.main_log_level]
    main_log_filepath = config_vars.main_log_file_path

    json_log_level = log_levels[config_vars.json_log_level]
    log_file_path = config_obj.json_val(config_vars.log_file_path)
    log_dir_name_format = config_obj.json_val(config_vars.log_folder_name_format)

## Initialize Logger
    main_log_obj = LoggingUtil(log_file_path)
    FileUtility.create_directory(datetime.now().strftime(log_dir_name_format))
    main_logger = main_log_obj.setup_logger(main_log_name, main_log_level)
    main_logger_file_handler = main_log_obj.create_log_file(main_log_filepath)

    main_log_obj.link_logger_filehandler(main_logger, main_logger_file_handler)

## Assign the logger to Main Configuration
    config_obj.reinitialise_logger_object( main_log_obj, 
                                           config_vars.main_config,
                                           main_logger_file_handler, 
                                           json_log_level)

# email class initialiser 
    load_start_mail_subject      = config_obj.json_val(config_vars.email_subject).format(f'has Started ')

    # email_obj      =  mail_attachments(
    #                     config_obj.json_val(config_vars.smtp_server_host),
    #                     config_obj.json_val(config_vars.smtp_server_port),
    #                     config_obj.json_val(config_vars.email_sender_id),
    #                     config_obj.json_val(config_vars.email_receiver_id),
    #                     config_obj.json_val(config_vars.email_subject)                        
    #                     )

    # email_obj.send_load_start_viamail (load_start_mail_subject)
## Initialize  variables  after logger is initialised
    ora_instant_client_dir = config_obj.json_val(config_vars.ora_instant_client_dir)

    csv_import_params = config_obj.json_val(config_vars.csv_import_parameters)
    csv_import_filepath = csv_import_params[config_vars.csv_import_file_info_path]
    csv_import_flag = csv_import_params[config_vars.csv_import_flg]

    prev_run_success_file = config_obj.json_val(config_vars.pre_sucessfull_run_tables_list)
    prev_run_full_file = config_obj.json_val(config_vars.pre_execution_complete_tbl_list)
    recovery_flag_config_file = config_obj.json_val(config_vars.recover_failed_exec_flg)

    curr_tbl_dict =  config_obj.json_val(config_vars.table_information_for_processing)
    load_type =  config_obj.json_val(config_vars.load_type)
    max_parallel_num = config_obj.json_val(config_vars.max_parallel_num)   

## Initialize Connection Configuration with logger
    conn_config_obj = JsonValueExtractor(   connection_json_filepath, 
                                            config_vars.connection_param,
                                            main_log_obj,
                                            main_logger_file_handler,
                                            json_log_level)

    connectors_dict = conn_config_obj.json_val(config_vars.connectors)

    main_logger.info('ETL Start Time : {} .'.format(datetime.now()))

    main_logger.info("Connection Pool Establishment starts!")

    db_obj = DatabaseInitialiser(   connectors_dict,
                                    main_log_obj,
                                    main_logger_file_handler, 
                                    config_vars, 
                                    ora_instant_client_dir,
                                    config_obj.json_val(config_vars.connection_retry_count))

    main_logger.info("Connection Pool Establishment ends!")

    if csv_import_flag == 'Y':
        csv_import_obj = CSVImportUsingDTF(csv_import_filepath,
                                    db_obj,
                                    main_log_obj,
                                    config_vars)
        csv_import_obj.load_csv_files_to_db()
    
    recover_prev_exec_obj = ExecutionRecovery ( prev_run_success_file,
                                                prev_run_full_file,
                                                recovery_flag_config_file)

    exec_obj = JSONGroupExecution(  config_vars, 
                                    curr_tbl_dict, 
                                    main_log_obj,
                                    main_logger_file_handler,
                                    db_obj)

    exec_obj.process_groups(recover_prev_exec_obj,config_obj.json_val(config_vars.table_config_file_path))

    table_log_summary, group_log_summary = exec_obj.process_tables(load_type, max_parallel_num)

    program_timer[config_vars.load_end_time] = datetime.now()

    current_run_log_directory   =  main_log_obj.summary_log_file ( config_vars,
                                    table_log_summary, 
                                    group_log_summary, 
                                    program_timer,
                                    config_obj.json_val(config_vars.common_summary_log_folder_name)
                                    )



    # email_obj.send_load_status_viamail(group_log_summary,
    #                                    config_obj.json_val(config_vars.email_attachments),
    #                                    current_run_log_directory)


if __name__ == "__main__":

    main_thread()
     
