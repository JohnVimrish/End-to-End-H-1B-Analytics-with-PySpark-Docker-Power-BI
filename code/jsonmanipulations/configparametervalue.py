from jsonmanipulations.jsontagvariables import JsonTagVariables as JsonTV

class ConfigurationParametersValue():
    
    @classmethod
    def initialize_configuration_parameter (  cls, 
                      main_config_obj,
                      spark_config_obj,
                      dbconnection_config_obj,
                      toprocess_tables_config_obj):
        cls.postgres_jdbc_loc  = main_config_obj.json_val(JsonTV.postgres_jdbc_loc)
        cls.files_to_process  = main_config_obj.json_val(JsonTV.files_to_process)
        cls.writing_to_db_no_of_pp  = main_config_obj.json_val(JsonTV.writing_to_db_no_of_pp)
        cls.memory_cache = main_config_obj.json_val(JsonTV.memory_cache)
        cls.rmv_sprk_dft_col_starging_with  = main_config_obj.json_val(JsonTV.rmv_sprk_dft_col_starging_with)
        cls.tables_input_folder_location  = main_config_obj.json_val(JsonTV.tables_input_folder_location)
        cls.target_db  = JsonTV.target_db
        cls.target_db_host  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_host]
        cls.target_db_port  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_port]
        cls.target_db_dbname  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_dbname]
        cls.target_db_user  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_user]
        cls.target_db_driver = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_driver]
        cls.target_db_database_from = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_database_from]
        cls.target_db_password  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_password]
        cls.target_db_max_open_connection_pool_number  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_max_open_connection_pool_number]
        cls.sp_conf_app_name    = spark_config_obj.json_val(JsonTV.sp_conf_app_name)
        cls.sp_conf_master      = spark_config_obj.json_val(JsonTV.sp_conf_master)
        cls.sp_conf_driver_memory    = spark_config_obj.json_val(JsonTV.sp_conf_driver_memory )
        cls.sp_conf_ui_port  = spark_config_obj.json_val(JsonTV.sp_conf_ui_port)
        cls.sp_conf_executor_memory   = spark_config_obj.json_val(JsonTV.sp_conf_executor_memory )
        cls.sp_conf_driver_cores    = spark_config_obj.json_val(JsonTV.sp_conf_driver_cores  )
        cls.sp_conf_executor_cores    = spark_config_obj.json_val(JsonTV.sp_conf_executor_cores  )
        cls.sp_conf_executor_instances    = spark_config_obj.json_val(JsonTV.sp_conf_executor_instances  )
        cls.sp_conf_parallelism    = spark_config_obj.json_val(JsonTV.sp_conf_parallelism  )
        cls.sp_conf_shuffle_partitions    = spark_config_obj.json_val(JsonTV.sp_conf_shuffle_partitions  )
        cls.sp_conf_task_cpus    = spark_config_obj.json_val(JsonTV.sp_conf_task_cpus  )
        cls.sp_conf_memory_fraction    = spark_config_obj.json_val(JsonTV.sp_conf_memory_fraction  )
        cls.sp_conf_memory_storage_fraction    = spark_config_obj.json_val(JsonTV.sp_conf_memory_storage_fraction  )
        cls.sp_conf_serializer    = spark_config_obj.json_val(JsonTV.sp_conf_serializer  )
        cls.sp_conf_kryo_registration    = spark_config_obj.json_val(JsonTV.sp_conf_kryo_registration  )
        cls.sp_conf_task_kryo_classes_to_register    = spark_config_obj.json_val(JsonTV.sp_conf_task_kryo_classes_to_register  )
        cls.sp_conf_eventlog_enabled    = spark_config_obj.json_val(JsonTV.sp_conf_eventlog_enabled  )
        cls.sp_conf_history_fs_logdirectory    = spark_config_obj.json_val(JsonTV.sp_conf_history_fs_logdirectory  )
        cls.sp_conf_memory_caching  = spark_config_obj.json_val(JsonTV.sp_conf_memory_caching)
        cls.sp_conf_memory_offheap_enabled  = spark_config_obj.json_val(JsonTV.sp_conf_memory_offheap_enabled)
        cls.dwh_tables_config_write_mode = toprocess_tables_config_obj.json_val(JsonTV.dwh_tables_config_write_mode)
        cls.dwh_tables_config_target_tables_groups =   toprocess_tables_config_obj.json_val(JsonTV.dwh_tables_config_target_tables_groups)