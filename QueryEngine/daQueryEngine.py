"""This module contains the main flow of the project.
"""
import datetime
import pandas as pd
import duckdb as psql
from Utils.params_utils import *
from QueryEngine.sqlmanager import SqlManager#, PoolError
from Utils.log_handler import Logger

class QueryEngine:

    def __init__(self, aGenConfig, allQueries):
		#basic properties initialization
        self.genconfig = aGenConfig
		#Create DB source object using defined connnection pool value
        self.dbmanager = SqlManager(aGenConfig['db_setting'])
        self.querylist = allQueries
        
        self.logger = Logger.setup_logger()
        self.logger.debug("QueryEngine object created...")

    def fetch_all_row_data(self):
        
        data = self.dbmanager.get_data(self.querylist["get_all_data"])
        
        self.logger.debug("All Raw data fetched successfully...")
  
        return data
		
	#temp: store interim probable list into db
    def log_data_into_table(self, node_probableList_df, module_option, stepname):
        squery = '''abc'''
        df1 = psql.query(squery).to_df()
        self.dbmanager.insert_data(df1, "table_name")
        del [df1]
			
    
    def update_batch_status(self, response = -1, req_obj=None):
        #update the status of batch in tb_di_batch_definition table:
        self.dbmanager.execute_query(f"update abc set sendresponse = {response} where batchid = {req_obj};")