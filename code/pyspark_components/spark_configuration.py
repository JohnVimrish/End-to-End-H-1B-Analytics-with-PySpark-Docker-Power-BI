from pyspark import SparkConf

class SparkConf () :
    def __init__ (self,) :
        self.sparkconfiguration  = SparkConf()

    def __init_configuration_from_config_inputs  ( self,) :
            self.sparkconfiguration.set("spark.app.name", "ComprehensiveSparkJob") 
            self.sparkconfiguration.set("spark.master", "local[*]")                
            self.sparkconfiguration.set("spark.driver.memory", "4g")      
            self.sparkconfiguration.set("spark.driver.cores", "1")                 
            self.sparkconfiguration.set("spark.ui.port", "4040")                
            self.sparkconfiguration.set("spark.executor.memory", "2g")      
            self.sparkconfiguration.set("spark.executor.cores", "2")               
            self.sparkconfiguration.set("spark.executor.instances", "3")         
            self.sparkconfiguration.set("spark.default.parallelism", "6")         
            self.sparkconfiguration.set("spark.sql.shuffle.partitions", "6")       
            self.sparkconfiguration.set("spark.task.cpus", "1")  
            self.sparkconfiguration.set("spark.memory.fraction", "0.8")            
            self.sparkconfiguration.set("spark.memory.storageFraction", "0.5")     
            self.sparkconfiguration.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  
            self.sparkconfiguration.set("spark.kryo.registrationRequired", "true")  
            self.sparkconfiguration.set("spark.kryo.classesToRegister", "org.apache.spark.sql.Row")  
            self.sparkconfiguration.set("spark.eventLog.enabled", "true")           
            self.sparkconfiguration.set("spark.eventLog.dir", "/root/spark_log/spark-events/")  
            self.sparkconfiguration.set("spark.history.fs.logDirectory", "/root/spark_log/spark-history/")  

    def  fetch_spark_congiration_obj  (self ):
         return self.sparkconfiguration
         
