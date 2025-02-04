from pyspark.sql import SparkSession,functions as F 
from pyspark import StorageLevel

class spark_components () :
    
    def __init__ (self, ObjSparkConfig ) :
        def __init_spark_session () :
                return  SparkSession.builder.config(conf=ObjSparkConfig).getOrCreate()
        self.SparkSession_init  = __init_spark_session ()
      
    def func_cache_dft_data(self,dataframe) :
        """ we need to re write th  logic based on the code"""
        dataframe.persist(StorageLevel.MEMORY_AND_DISK)

    def func_json_dataframe (self,input_file:str) :
        return  self.SparkSession_init.read.option("multiline", "true").json(input_file)
    
    def func_repartion_dataframe(self,dataframe, repartion_count) :
        return dataframe.repartition(repartion_count)
    
    
    def func_explode_based_on_one_key (self,input_dataframe,explode_key:str , alias_of_column :str)  :
        return  input_dataframe.select(F.explode(F.col(explode_key)).alias(alias_of_column))
    
    def func_explode_based_on_depper_root_key (self,input_dataframe,explode_key:list  , drop_column :str)  :
        """ Always try to assemeble  he json key  based on the root level  in heirarchial order like root -->values-->payload-->commits then the explode_key   value should be [root,values,payload,commits]"""
        column_level_analysis = (".").join(explode_key)
        return  input_dataframe.select(f"{explode_key[0]}.*",F.explode_outer(F.col(column_level_analysis)).alias(column_level_analysis.replace(".","_"))).drop(drop_column)
    
    
    def func_get_rdd_num_partitions(self,dataframe) :
        return dataframe.rdd.getNumPartitions()
    
    def func_write_data_to_parquet (self,file_path, dataframe,write_mode) :
        dataframe.write.parquet(file_path,mode=write_mode)
    
    
    def func_read_parquet_file(self,file_path) :
        return self.SparkSession_init.read.parquet(file_path)
    
    def func_stop_spark (self,) :
            self.SparkSession_init.stop()
    
    
    def func_remane_column_names (self,dataframe) :
        column_list = [F.col(c).alias(c.replace('.', '_')) for c in dataframe.columns]
        for   values in column_list:
            print(values)
        return  dataframe.select([F.col(c).alias(c.replace('.', '_')) for c in dataframe.columns])
    
    
    def  func_limit_rows_dataframe (self,dataframe, limit_row_count) :
        return dataframe. limit(limit_row_count)
    
    def  finc_count_dft_rows(self,dataframe):
            return dataframe.count()
    