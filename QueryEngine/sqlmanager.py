import io
import psycopg2 
from psycopg2 import extras #pool, 
from sqlalchemy.pool import QueuePool  
from sqlalchemy import URL, create_engine 
from urllib.parse import quote_plus
import pandas.io.sql as psql
from QueryEngine.idbmanager import IDBManager
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
from Utils.log_handler import Logger


#class PoolError(psycopg2.Error):
#    pass

class SqlManager(IDBManager):
    def __init__(self, settings):
        self.settings = settings
        #self.pool = pool.SimpleConnectionPool(minconn = 1, **settings)
        self.Schema = self.settings['options'].split("=")[-1]
        connection_string  = URL.create(
            "postgresql",
            username=self.settings['user'],
            password=self.settings['password'],
            host=self.settings['host'],
            port=self.settings['port'],
            database=self.settings['database'],
        )
        self.pool = QueuePool(creator = lambda:create_engine(connection_string, poolclass=QueuePool)\
			.raw_connection(), pool_size=5,   #Size of the connection pool 
			max_overflow=2)
        self.set_schemas(self.Schema)
        self.logger = Logger.setup_logger()
       

    def open_connection(self):
        conn = self.pool.connect() 
        conn.set_session(autocommit=True)
        return conn

    def release_connection(self, conn):
        if conn:# Return the connection to the pool without specifying a key
            conn.close()

    def close_connection(self):
        self.pool.closeall()

    def get_data(self, query):
        data = None
        conn = self.open_connection()
        try:
            data = psql.read_sql(query, con=conn)
        except Exception as ex:
            self.logger.error(f"Exception caught: {ex}", exc_info=True)
        finally:
            self.release_connection(conn)
            return data

    def set_schemas(self, schemas):
        self.Schema = schemas
        self.execute_query(f"set search_path to {schemas};")
 
    def execute_query(self, query):
        conn = self.open_connection()
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(query)
        except Exception as ex:
            self.logger.error(f"Exception caught: {ex}", exc_info=True)
        finally:
            cursor.close()
            self.release_connection(conn)

    def insert_data(self, data, table_name, schema_name = ''):
        conn = self.open_connection()
        cursor = None
        try:           
            cursor = conn.cursor()
            buffer = io.StringIO()
            data.to_csv(buffer, header=False, index = False, sep=';')
            buffer.seek(0)  # Required for rewinding the String object
            cursor.copy_from(buffer, table_name, sep=';')
        except Exception as ex:
            self.logger.error(f"Exception caught: {ex}", exc_info=True)
        finally:
            if cursor: 
                cursor.close() 
            self.release_connection(conn)

    def upsert_data(self, data, table_name, primary_key, schema):
        conn = self.open_connection()
        cursor = None
        try: 
            cursor = conn.cursor() 
            # Prepare the data as a list of tuples 
            data_values = [tuple(x) for x in data.to_numpy()] 
            
            # Calculate column names dynamically 
            columns = ', '.join(data.columns) 
            update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in data.columns if col != primary_key]) 
            
            # Generate the upsert query dynamically based on the DataFrame columns 
            query = f""" 
                INSERT INTO {table_name}({columns})   
                VALUES %s   
                ON CONFLICT ({', '.join(primary_key)}) DO UPDATE SET {update_columns} 
            """ 

            # Execute the upsert query with bulk data using execute_values 
            extras.execute_values(cursor, query, data_values) 
            
            # Commit the transaction and release the connection 
            conn.commit()            
        except Exception as ex:
            self.logger.error(f"Exception caught: {ex}", exc_info=True)
        finally: 
            if cursor: 
                cursor.close() 
            self.release_connection(conn)

