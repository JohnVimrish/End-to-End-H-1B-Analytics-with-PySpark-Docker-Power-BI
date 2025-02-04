from ast import Str
from logging import Logger
from jsoncustom.jsontagncommonvariables import JsonTagAndCommonVariables
from util.fusionloggingutility import LoggingUtil
from database.postgresconnectionpool import PostgresConnectionPool
from database.oracleconnectionpool import OracleConnectionPool
from database.oracleconnector import OracleConnection
from database.postgresconnector import PostgresConnection



class DatabaseInitialiser ():

    def __init__(self, 
                connectors_dict_list: list, 
                log_obj:LoggingUtil, 
                logger_filehandler, 
                config_vars:JsonTagAndCommonVariables, 
                oracle_instant_client_dir:str,
                oracle_connection_retry_count ):

        self.config_vars = config_vars
        self.connections = list()
        self.connection_pools = dict()
        
        enable_connection = config_vars.enable_connection
        db_type = config_vars.database_type
        connection_pool = config_vars.connection_pool
        db_type_alias = config_vars.database_prod
        ORACLE = config_vars.Oracle_Database
        POSTGRESQL = config_vars.Postgres_Database

        db_init_log_level = config_vars.log_levels[config_vars.db_initialiser_log_level]
        
        
        self.db_init_logger = log_obj.setup_logger ( config_vars.db_init, db_init_log_level )
        log_obj.link_logger_filehandler( self.db_init_logger, logger_filehandler)

        for connector_dict in connectors_dict_list:
            for db_connector, config_info in connector_dict.items():
                if config_info[enable_connection] == 'Y':
                    db_connection_dict = dict()

                    if config_info[db_type] == ORACLE:
                        connection_pool_obj = OracleConnectionPool (    db_connector,
                                                                        config_info, 
                                                                        config_vars, 
                                                                        log_obj, 
                                                                        logger_filehandler, 
                                                                        oracle_instant_client_dir,
                                                                        oracle_connection_retry_count)

                    elif config_info[db_type] == POSTGRESQL:
                        connection_pool_obj = PostgresConnectionPool(   db_connector,
                                                                        config_info,
                                                                        config_vars,
                                                                        log_obj,
                                                                        logger_filehandler)

                    db_connection_dict[connection_pool] = connection_pool_obj
                    db_connection_dict[db_type_alias] = config_info[db_type]
                    self.connections.append(db_connector)
                    self.connection_pools[db_connector] = db_connection_dict

            self.db_init_logger.info(f"Overall Enabled Connections {','.join(self.connections)}")

    def init_db_connection(self, 
                           db_connection_name:Str,
                           thread_logger:Logger, 
                           thread_logger_file, 
                           log_obj:LoggingUtil, 
                           table_name:Str):

            
        ORACLE = self.config_vars.Oracle_Database
        POSTGRESQL = self.config_vars.Postgres_Database  
        postgres_log_level = self.config_vars.log_levels[self.config_vars.postgres_connector_log_level]
        oracle_log_level = self.config_vars.log_levels[self.config_vars.ora_connector_log_level]
            
        if self.connection_pools.get(db_connection_name) is not None:
            try :

                connection_pool = self.connection_pools[db_connection_name][self.config_vars.connection_pool].get_connection_pool()     
                db_type         = self.connection_pools[db_connection_name][self.config_vars.database_prod]          

                if ( db_type == ORACLE):
                            
                    connection_obj = OracleConnection (  connection_pool, 
                                                         thread_logger_file,
                                                         log_obj, 
                                                         oracle_log_level, 
                                                         table_name, 
                                                         ORACLE)
                            
                elif ( db_type == POSTGRESQL):

                    connection_obj = PostgresConnection( connection_pool,
                                                         thread_logger_file, 
                                                         log_obj, 
                                                         postgres_log_level,
                                                         table_name,
                                                         POSTGRESQL)
                        
                connection_obj.acquire_connection()
                thread_logger.info(f'{db_connection_name} has connected .')

            except Exception as error  :
                        thread_logger.error(error,exc_info=True)
            return  connection_obj
        else:
            thread_logger.info(f'Either Connection Name specified in Table Config Json : {db_connection_name} not found in Connection Config Json')
            return None
                   
    def init_src_tgt_connections(self, 
                                src_connection_name:Str,
                                tgt_connection_name:Str,
                                thread_logger:Logger, 
                                thread_logger_file, 
                                log_obj:LoggingUtil, 
                                table_name:Str):

        connections =self.connections
        connection_pools = self.connection_pools
        config_vars = self.config_vars
        ORACLE = config_vars.Oracle_Database
        POSTGRESQL = config_vars.Postgres_Database  
        postgres_log_level = config_vars.log_levels[config_vars.postgres_connector_log_level]
        oracle_log_level = config_vars.log_levels[config_vars.ora_connector_log_level]
              

        if ((src_connection_name in connections) and (tgt_connection_name in connections)):

            for connection_name, connection_dict in connection_pools.items():
                
                connection_pool = connection_dict[config_vars.connection_pool].get_connection_pool()
                db_type = connection_dict[config_vars.database_prod]
                
                # Source DB Connection
                if (src_connection_name == connection_name ):
                    try :
                        if ( db_type == ORACLE):
                            
                            src_connection_obj = OracleConnection (  connection_pool, 
                                                                    thread_logger_file,
                                                                    log_obj, 
                                                                    oracle_log_level, 
                                                                    table_name, 
                                                                    ORACLE)
                            
                        elif ( db_type == POSTGRESQL):

                            src_connection_obj = PostgresConnection( connection_pool,
                                                                    thread_logger_file, 
                                                                    log_obj, 
                                                                    postgres_log_level,
                                                                    table_name,
                                                                    POSTGRESQL)
                        
                        src_connection_obj.acquire_connection()
                        src_connection_obj.source_db_type = db_type
                        thread_logger.info(f'{src_connection_name} has connected as Source .')

                    except Exception as error  :
                        thread_logger.error(error,exc_info=True)

                # Target DB Connection
                if (src_connection_name == tgt_connection_name == connection_name and db_type == POSTGRESQL ) :

                    tgt_connection_obj = src_connection_obj
                    tgt_connection_obj.target_db_type = db_type
                    thread_logger.info(f'{tgt_connection_name} has connected as Target ')

                elif ((src_connection_name != tgt_connection_name and tgt_connection_name == connection_name) and 
                        db_type == POSTGRESQL):
                    try :
                        tgt_connection_obj = PostgresConnection( connection_pool,
                                                                thread_logger_file, 
                                                                log_obj, 
                                                                postgres_log_level, 
                                                                table_name, 
                                                                POSTGRESQL)
                        tgt_connection_obj.acquire_connection()
                        tgt_connection_obj.target_db_type = db_type
                        thread_logger.info(f'{tgt_connection_name} has connected as Target')
                    except Exception as error  :
                        thread_logger.error(error,exc_info=True)

            return src_connection_obj, tgt_connection_obj
        else:
            thread_logger.info(f'Either Connection Name specified in Table Config Json :{src_connection_name}, {tgt_connection_name} not found in Connection Config Json,or Connections are not properly intialised  by the program which are listed in Connection configuration file. ')
            return None
        

    def release_connections(self,src_connection_obj, tgt_connection_obj):

        if src_connection_obj : 
            src_connection_obj.release_connection()
        
        if src_connection_obj is not tgt_connection_obj :
            tgt_connection_obj.release_connection()

    def release_connection(self,db_connection_obj):
              db_connection_obj.release_connection()

    def close_connections(self):
        for connection_name, connection_dict in self.connection_pools.items():
            connection_dict[self.config_vars.connection_pool].close_connection_pool(connection_name, '{}  has been closed !')
