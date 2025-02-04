import sys
from psycopg import  Error, DatabaseError as db_error, OperationalError
from psycopg_pool import ConnectionPool
from jsoncustom.jsontagncommonvariables import JsonTagAndCommonVariables
from util.fusionloggingutility import LoggingUtil


class PostgresConnectionPool():

    def __init__(self,
                 db_connector_name,
                 config_connect_vars: dict,
                 config_vars: JsonTagAndCommonVariables,
                 log_obj: LoggingUtil,
                 logger_file_handler):

        cv = config_vars
        # Logger Initialization
        log_level = cv.log_levels[cv.postgres_connection_pool_log_level]
        self.pg_cp_logger = log_obj.setup_logger(cv.postgres_con_pool, log_level)
        log_obj.link_logger_filehandler(self.pg_cp_logger, logger_file_handler)

        # Config Values Initializatoin
        user_id = config_connect_vars[cv.user_id]
        password = config_connect_vars[cv.password]
        port = config_connect_vars[cv.port]
        host = config_connect_vars[cv.host]
        min_connections = config_connect_vars[cv.min_connection_pools]
        max_connections = config_connect_vars[cv.max_connection_pools]
        database = config_connect_vars[cv.database]
        tns = config_connect_vars[cv.tns]
        db_type = config_connect_vars[cv.database_type]
        dsn = host+':'+port+'/' + database
        self.db_connector_name = db_connector_name

        database_config = f"user={user_id} password={password} host={host} port={port} dbname={database} keepalives={1} keepalives_idle={100} keepalives_interval={100} keepalives_count={5} tcp_user_timeout={100000}"
        try:
            # self.connection_pool = ConnectionPool(
            #     maxconn=max_connections,
            #     user=user_id,
            #     password=password,
            #     host=host,
            #     port=port,
            #     database=database,
            #     keepalives=1,
            #     keepalives_idle=100,
            #     keepalives_interval=100,
            #     keepalives_count=5,
            #     tcp_user_timeout=100000)
            self.connection_pool =ConnectionPool( conninfo =database_config,min_size=10,max_size=10 )
            
            str1 = f'{db_connector_name} Connector with  user_name:{user_id}, password : xxxx, '
            str2 = f'host :{host}, port :{port}, tns :{tns},database : {database}, '
            str3 = f'min_num of connections :{min_connections},'
            str4 = f'max_num of connections :{max_connections}'
            str5 = f'db_type :{db_type} , dsn:{dsn} has connected !'

            self.pg_cp_logger.info(str1 + str2 + str3 + str4 + str5)

        except (OperationalError, Exception, Error) as err:
            # passing exception to function
            self.pg_cp_logger.critical('{0} Connector  has failed to connect with error :{1}'.format(
                db_connector_name, err), exc_info=True)

    def get_connection_pool(self):
        return self.connection_pool

    def log_error(self, log_statement, error_message):
        self.pg_cp_logger.error(log_statement.format(
            self.log_pgdb_exception(error_message)), exc_info=True)

    def log_pgdb_exception(self, err_msg_obj):
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()
        # get the line number when exception occured
        line_n = traceback.tb_lineno
        # print the connect() error

        str1 = f' npsycopg2 ERROR:{err_msg_obj} on line number:{line_n}\npsycopg2 traceback:{traceback}'
        str2 = f' -- type:{err_type}\npgerror:{err_msg_obj.pgerror}\n pgcode: {err_msg_obj.pgcode}'
        return str1 + str2

    def close_connection_pool(self, connection_name, log_message):
        
        try  :
            try:
                self.connection_pool.close()
                self.pg_cp_logger.info(
                    log_message.format(connection_name))
            except (Error, db_error) as error:
                self.log_error(
                    'Unable to Close Connection pool , Error :{} .', error)
        except :
            self.pg_cp_logger.info('Unable to Close Connection pool as Postgres Connection pool is not initialised ')
        
