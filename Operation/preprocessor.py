import pandas as pd
import numpy as np 
from Utils.log_handler import Logger
from Utils.params_utils import ParamsUtils
from QueryEngine.daQueryEngine import QueryEngine


class Preprocessing:
    
    def __init__(self):
        
        self.all_settings = ParamsUtils.get_configuration(file_name="GeneralConfig.yaml")
        self.all_queries = ParamsUtils.get_configuration(file_name="query.yaml")
        self.logger = Logger.setup_logger()
        
        self.processObj = QueryEngine(self.all_settings, self.all_queries)
        
        dataframe = self.processObj.fetch_all_row_data()
        
        print(dataframe.head())