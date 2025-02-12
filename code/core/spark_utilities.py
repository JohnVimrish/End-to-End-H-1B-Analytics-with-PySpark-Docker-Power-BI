from pyspark.sql import SparkSession,functions as F 
from pyspark import StorageLevel
from pyspark.errors import PySparkException
from pyspark.sql.utils import AnalysisException
from root.commonvariables import CommonVariables as common_var

class spark_utilities () :
    
    def __init__ (self, ObjSparkConfig ) :
        def __init_spark_session () :
                return  SparkSession.builder.config(conf=ObjSparkConfig).getOrCreate()
        self.SparkSession_init  = __init_spark_session ()
        self.read_func  = None
      
    def func_cache_dft_data(self,dataframe, storage_mode :str ) :
        self.storage_mode = StorageLevel.MEMORY_AND_DISK if storage_mode == common_var.m_d else StorageLevel.MEMORY_ONLY if storage_mode == common_var.m_only else StorageLevel.DISK_ONLY
        dataframe.persist(self.storage_mode)
    
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
    
        
    def func_stop_spark (self,) :
            self.SparkSession_init.stop()
    
    def func_remane_column_names (self,dataframe) :
        return  dataframe.select([F.col(c).alias(c.replace('.', '_')) for c in dataframe.columns])
    
    def func_initiate_spark_read  (self) :
      self.read_func = self.SparkSession_init.read()

    def func_set_infer_schema(self, infer_schema=True):
        self.read_func = self.read_func.option(common_var.inferschema, str(infer_schema).lower())

    def func_set_header(self, header=True):
        self.read_func = self.read_func.option(common_var.header, str(header).lower())

    def func_read_multiline(self, multiline=True):
        self.read_func = self.read_func.option(common_var.multiline, str(multiline).lower())

    def func_set_read_options (self,func_type,argument_overide_bool= True) :
        try  :
            if self.read_func   is None : 
                  self.func_initiate_spark_read()
            if func_type == common_var.inferschema :
                self.func_set_infer_schema (argument_overide_bool)
            elif  func_type == common_var.header :
                self.func_set_header (argument_overide_bool)
            elif   func_type == common_var.multiline :
                self.func_read_multiline (argument_overide_bool)
        except (AnalysisException, PySparkException) as e:
            self.handle_spark_exception(func_type, e)

    def func_limit_rows_dataframe (self,dataframe, limit_row_count) :
        return dataframe. limit(limit_row_count)
    
    def  func_count_dft_rows(self,dataframe):
            return dataframe.count()
    
    def func_read_parquet_file(self,file_path) :
        return self.read_func.parquet(file_path)
    
    def func_json_dataframe (self,input_file:str) :
        return  self.read_func.json(input_file)
    
    def func_csv_dataframe (self,input_file:str) :
        return  self.read_func.csv(input_file)
    
    def func_write_data_to_parquet (self,file_path, dataframe,write_mode) :
        dataframe.write.parquet(file_path,mode=write_mode)
    
    def func_re_alias_column_names_with_query (self,dataframe,query:str) :
        return dataframe.selectExpr(query)

    def func_exclude_columns_from_dft(self,dataframe,exclude_columns:list) :
          selected_columns = [col for col in dataframe.columns if col not in exclude_columns]
          df_selected = dataframe.select(*selected_columns)
          return df_selected
    
    def write_spark_dft_to_db (self,dataframe,db_url:str,db_properties:dict,table_name:str,mode:str,driver) :
        dataframe.write.option(common_var.driver_attribute, driver).jdbc(url=db_url, table=table_name, properties=db_properties,mode = mode).save()
    
    def handle_spark_exception(self, func_name , e) :
        error_message = f"Error in function {func_name}: {str(e)}"
        raise RuntimeError(error_message) from e
    

    def func_stop_sparksession(self) :
        self.SparkSession_init.stop()