from jsonmanipulations.jsontagvariables import JsonTagVariables as JsonTV
from utility. utils import Common_Utils_Operations as  cuo

class ConfigParametersValue():
    
    @classmethod
    def initialize_configuration_parameter (  cls, 
                      main_config_obj,
                      spark_config_obj,
                      dbconnection_config_obj ):
        seperator  = JsonTV.json_value_separator
        cls.postgres_jdbc_loc  = main_config_obj.json_val(JsonTV.postgres_jdbc_loc,seperator)
        cls.files_to_process  = cuo.derive_actual_directory(main_config_obj.json_val(JsonTV.files_to_process,seperator))
        cls.writing_to_db_no_of_pp  = main_config_obj.json_val(JsonTV.writing_to_db_no_of_pp,seperator )
        cls.target_db_host  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_host]
        cls.target_db_port  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_port]
        cls.target_db_dbname  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_dbname]
        cls.target_db_user  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_user]
        cls.target_db_password  = dbconnection_config_obj [JsonTV.target_db][JsonTV.target_db_password]
        cls.sp_conf_app_name    = spark_config_obj.json_val(JsonTV.sp_conf_app_name,seperator )
        cls.sp_conf_master      = spark_config_obj.json_val(JsonTV.sp_conf_master,seperator )
        cls.sp_conf_driver_memory    = spark_config_obj.json_val(JsonTV.sp_conf_driver_memory ,seperator )
        cls.sp_conf_ui_port  = spark_config_obj.json_val(JsonTV.sp_conf_ui_port,seperator )
        cls.sp_conf_executor_memory   = spark_config_obj.json_val(JsonTV.sp_conf_executor_memory ,seperator )
        cls.sp_conf_driver_cores    = spark_config_obj.json_val(JsonTV.sp_conf_driver_cores  ,seperator )
        cls.sp_conf_executor_cores    = spark_config_obj.json_val(JsonTV.sp_conf_executor_cores  ,seperator )
        cls.sp_conf_executor_instances    = spark_config_obj.json_val(JsonTV.sp_conf_executor_instances  ,seperator )
        cls.sp_conf_parallelism    = spark_config_obj.json_val(JsonTV.sp_conf_parallelism  ,seperator )
        cls.sp_conf_shuffle_partitions    = spark_config_obj.json_val(JsonTV.sp_conf_shuffle_partitions  ,seperator )
        cls.sp_conf_task_cpus    = spark_config_obj.json_val(JsonTV.sp_conf_task_cpus  ,seperator )
        cls.sp_conf_memory_fraction    = spark_config_obj.json_val(JsonTV.sp_conf_memory_fraction  ,seperator )
        cls.sp_conf_memory_storage_fraction    = spark_config_obj.json_val(JsonTV.sp_conf_memory_storage_fraction  ,seperator )
        cls.sp_conf_serializer    = spark_config_obj.json_val(JsonTV.sp_conf_serializer  ,seperator )
        cls.sp_conf_kryo_registration    = spark_config_obj.json_val(JsonTV.sp_conf_kryo_registration  ,seperator )
        cls.sp_conf_task_kryo_classes_to_register    = spark_config_obj.json_val(JsonTV.sp_conf_task_kryo_classes_to_register  ,seperator )
        cls.sp_conf_eventlog_enabled    = spark_config_obj.json_val(JsonTV.sp_conf_eventlog_enabled  ,seperator )
        cls.sp_conf_history_fs_logdirectory    = spark_config_obj.json_val(JsonTV.sp_conf_history_fs_logdirectory  ,seperator )
        cls.sp_conf_memory_caching  = spark_config_obj.json_val(JsonTV.sp_conf_memory_caching,seperator )
        cls.sp_conf_memory_offheap_enabled  = spark_config_obj.json_val(JsonTV.sp_conf_memory_offheap_enabled,seperator )