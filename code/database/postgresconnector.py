import sys
from datetime import datetime as dt
from psycopg import Error, DatabaseError as db_error,OperationalError
from jsonmanipulations.configparametervalue import ConfigurationParametersValue as CPV
from psycopg_pool import ConnectionPool

class PostgresConnection():

    def __init__ (self) :
            
        database_config = f'''user={CPV.target_db_user} password={CPV.target_db_password} host={CPV.target_db_host} port={CPV.target_db_port} dbname={CPV.target_db_dbname} keepalives={1} keepalives_idle={100} keepalives_interval={100} keepalives_count={5} tcp_user_timeout={100000}'''
        try:
            self.connection_pool =ConnectionPool( conninfo =database_config,min_size=10,max_size=10 )
        except (OperationalError, Exception, Error) as err:
            # passing exception to function
            raise ('{0} Connector  has failed to connect with error :{1}'.format(CPV.target_db, err))

    def acquire_connection(self):
        try:
            self.new_connection = self.connection_pool.getconn()
            self.new_connection.autocommit = True
            self.cursor_object = self.new_connection.cursor()

        except db_error as error_message:
            raise  (f'Database has failed to create a new connection  :{error_message}')

    def raise_and_close(self, log_statement, error_message):
        critical_error = ['server closed the connection unexpectedly']
        pg_exception_result = self.log_pgdb_exception(error_message)
        pg_error_msg   =pg_exception_result [1]
        if pg_error_msg in critical_error :
            raise pg_exception_result [0]
        self.cursor_object.close()
        raise log_statement.format(pg_exception_result [0])
       

    def execute_sql(self, sql, error_message,query_argments = None):
        try:
            if self.new_connection == None:
                self.acquire_connection()
            self.cursor_object.execute(sql,params=query_argments)
        except (Error, db_error) as error:
            self.raise_and_close(error_message, error)
    
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
    
    def copy_command_executor (self, query, input_data) :
        with self.cursor_object.copy(statement =query) as copy:
            copy.write(input_data.getvalue())
        self.new_connection.commit()

    def release_connection(self):
        try:
            self.connection_pool.putconn(self.new_connection)
        except (Error, db_error) as error:
            self.raise_and_close(
                'Unable to Close Connection , Error :{} .', error)

    def log_pgdb_exception(self, err_msg_obj):
        err_type, err_obj, traceback = sys.exc_info()
        line_n = traceback.tb_lineno
        str1 = f' npsycopg2 ERROR:{err_msg_obj} on line number:{line_n}\npsycopg2 traceback:{traceback}'
        str2 = f' -- type:{err_type}\npgerror:{err_msg_obj.pgerror}\n pgcode: {err_msg_obj.pgcode}'
        return str1 + str2,err_msg_obj.pgerror
    
    def close_connection_pool(self):
        self.connection_pool.close()

