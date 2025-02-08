from pyspark import SparkConf
from jsonmanipulations.configparametervalue import ConfigurationParametersValue as CPV

class SparkConf () :
    def __init__ (self,) :
        self.sparkconfiguration  = SparkConf()

    def __init_configuration_from_config_inputs  ( self,) :
            self.sparkconfiguration.set("spark.app.name",CPV.sp_conf_app_name) 
            self.sparkconfiguration.set("spark.master", CPV.sp_conf_master)                
            self.sparkconfiguration.set("spark.driver.memory", CPV.sp_conf_driver_memory)      
            self.sparkconfiguration.set("spark.driver.cores", CPV.sp_conf_driver_cores)                 
            self.sparkconfiguration.set("spark.ui.port", CPV.sp_conf_ui_port)                
            self.sparkconfiguration.set("spark.executor.memory", CPV.sp_conf_executor_memory)      
            self.sparkconfiguration.set("spark.executor.cores", CPV.sp_conf_executor_cores)               
            self.sparkconfiguration.set("spark.executor.instances", CPV.sp_conf_executor_instances)         
            self.sparkconfiguration.set("spark.default.parallelism",CPV.sp_conf_parallelism)         
            self.sparkconfiguration.set("spark.sql.shuffle.partitions", CPV.sp_conf_shuffle_partitions)       
            self.sparkconfiguration.set("spark.task.cpus", CPV.sp_conf_task_cpus)  
            self.sparkconfiguration.set("spark.memory.fraction",CPV.sp_conf_memory_fraction)            
            self.sparkconfiguration.set("spark.memory.storageFraction",CPV.sp_conf_memory_storage_fraction)     
            self.sparkconfiguration.set("spark.serializer", CPV.sp_conf_serializer)  
            self.sparkconfiguration.set("spark.kryo.registrationRequired",CPV.sp_conf_kryo_registration)  
            self.sparkconfiguration.set("spark.kryo.classesToRegister", CPV.sp_conf_task_kryo_classes_to_register)  
            self.sparkconfiguration.set("spark.eventLog.enabled", CPV.sp_conf_eventlog_enabled)           
            self.sparkconfiguration.set("spark.eventLog.dir", CPV.sp_conf_history_fs_logdirectory)  
            self.sparkconfiguration.set("spark.history.fs.logDirectory",CPV.sp_conf_history_fs_logdirectory)  
            self.sparkconfiguration.set("spark.jars", CPV.postgres_jdbc_loc)

    def  fetch_spark_congiration_obj  (self ):
        self.__init_configuration_from_config_inputs()
        return self.sparkconfiguration
         
