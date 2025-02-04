import sys
from datetime import datetime as dt
from psycopg import Error, DatabaseError as db_error




class PostgresConnection():

    def __init__ (self, config_connect_vars: dict) :

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


    def acquire_connection(self):
        try:
            self.new_connection = self.connection_pool.getconn()
            self.new_connection.autocommit = True
            self.cursor_object = self.new_connection.cursor()

        except db_error as error_message:
            self.pg_logger.critical(f'Database has failed to create a new connection  :{error_message}', exc_info=True)

    def log_and_close(self, log_statement, error_message):
        critical_error = ['server closed the connection unexpectedly']
        pg_exception_result = self.log_pgdb_exception(error_message)
        pg_error_msg   =pg_exception_result [1]
        if pg_error_msg in critical_error :
            self.pg_logger.critical(pg_exception_result [0], exc_info=True)
        self.pg_logger.error(log_statement.format(
           pg_exception_result [0]), exc_info=True)
        self.cursor_object.close()

    def execute_sql(self, sql, error_message,query_argments = None):
        try:
            if self.new_connection == None:
                self.acquire_connection()
            self.cursor_object.execute(sql,params=query_argments)
        except (Error, db_error) as error:
            self.log_and_close(error_message, error)
    
    def get_full_write_records(self) :
        return self.cursor_object.fetchall()

    def get_full_write_row_count(self):
        return self.write_row_count
    
    def get_full_records_asdict_list (self) :
        execution_query_description = self.cursor_object.description
        column_names = [column_name[0] for column_name in execution_query_description]
        return [dict(zip(column_names,each_row))   for each_row in self.get_full_write_records()]

    def get_curr_exec_rowcount(self):
        return self.cursor_object.rowcount

    def cursor_status(self):
        return self.cursor_object.closed

    def release_connection(self):
        try:
            self.connection_pool.putconn(self.new_connection)
        except (Error, db_error) as error:
            self.log_and_close(
                'Unable to Close Connection , Error :{} .', error)

    def log_pgdb_exception(self, err_msg_obj):
        err_type, err_obj, traceback = sys.exc_info()
        line_n = traceback.tb_lineno
        str1 = f' npsycopg2 ERROR:{err_msg_obj} on line number:{line_n}\npsycopg2 traceback:{traceback}'
        str2 = f' -- type:{err_type}\npgerror:{err_msg_obj.pgerror}\n pgcode: {err_msg_obj.pgcode}'
        return str1 + str2,err_msg_obj.pgerror