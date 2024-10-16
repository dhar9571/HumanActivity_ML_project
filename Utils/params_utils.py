import yaml
import argparse
import os
from io import StringIO
import numpy as np
import pandas as pd
import pandas.io.sql as pndsql
import psycopg2
import psycopg2.extras as extras
# from Utils.common_dto import *
import datetime


class ParamsUtils:
    #returns a keyvaluepair list of the section name passed in the method
	#from the params.yaml file
    @classmethod
    def get_configuration(self, file_name = '', section = '',  sub_section = ''):
        __args = argparse.ArgumentParser()
        __path = os.getcwd() +"//Configuration//" + file_name
        __args.add_argument('--config', default = __path)
        __parsed_args = __args.parse_args()

        with open(__parsed_args.config) as yaml_file:
            config=yaml.safe_load(yaml_file)

        if(section == ''):
            return config
        elif(sub_section == ''):
            return config[section]
        else:
            return config[section][sub_section]			
				
    @classmethod
    def getAllConfiguration(self, adataframe):
        #Declare dictionary object for tenant config and tenant data query
        dicAllConfig = configDictionary()
		#Iterate key, value pair in a configuration file
        for index, row in adataframe.iterrows():
            if row["isenabled"] == 1:
                dicConfig = configDictionary()
                atConfig = ParamsUtils.get_configuration(row["tenantconfigfile"])
                atQuery = ParamsUtils.get_configuration(row["dataqueryfile"])
                dicConfig.add(0, row["source_schema"])
                dicConfig.add(1, row["prediction_schema"])
                dicConfig.add(2, atConfig)
                dicConfig.add(3, atQuery)
                dicAllConfig.add(int(row["tenantid"]), dicConfig)
			#End if tenant isenabled
		#end of for loop
        return dicAllConfig
				
    @classmethod
    def get_selected_tenantconfig(self, aConfiguration, aTagKey = None, aTagValue = None):
		#Iterate key, value pair in a configuration file to find a particular tag file
        for k, v in aConfiguration.items():
            if (isinstance(aConfiguration[k], dict)) == True:
                if str(aConfiguration[k][aTagKey]) == str(aTagValue):
                    return aConfiguration[k]
			    #endif search tag
            #Endif type
		#end for loop
		#Return none if no tag value found
        return None
		

    @classmethod
    def save_configuration(self, aConfiguration, file_name = ''):
        __args = argparse.ArgumentParser()
        __path = os.getcwd() +"//Configuration//" + file_name
        __args.add_argument('--config', default = __path)
        __parsed_args = __args.parse_args()

        with open(__parsed_args.config, "w") as yaml_file:
            yaml.dump(aConfiguration, yaml_file)

				
    @classmethod
    def add_columns_to_distinguish_different_conf(self, df,input_value_type,type_of_distance,\
        power_on_off_status,module_option = 0):
        if df is not None:
            df["input_value_type"] = input_value_type
            df["type_of_distance"] = type_of_distance
            df["power_on_off_status"] = power_on_off_status
            df["moduleoption"] = module_option 
        #End of if

        return df
    		
    @classmethod
    def get_week_of_month(self, date, end_date):
        # Calculate the week number (1-4) within the month
        aDate = pd.to_datetime(date)
        week_number = (aDate.day - 1) // 7 + 1
        # Check if end date falls into the next week
        if end_date.day > 28 and (week_number == 4 or week_number == 0):
            return 1  # Reset if we are in week 4 and end date is in next month
        return week_number if week_number <= 4 else 1