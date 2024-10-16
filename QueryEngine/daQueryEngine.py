"""This module contains the main flow of the project.
"""
import datetime
import pandas as pd
import duckdb as psql
from Utils.params_utils import *
from QueryEngine.sqlmanager import SqlManager#, PoolError
from Utils.log_handler import Logger

class QueryEngine:

    def __init__(self, aGenConfig):
		#basic properties initialization
        self.genconfig = aGenConfig
		#Create DB source object using defined connnection pool value
        self.dbmanager = SqlManager(aGenConfig['db_setting'])
        self.source_dbmanager = SqlManager(aGenConfig['source_db_setting'])
        self.logger = Logger.setup_logger()
        self.logger.debug("QueryEngine object created")
        
	#Initialization using requested tenant configuration
    def initializeTenantConfig(self, dictenantconfig):
        self.querylist = dictenantconfig.get(3)	#3 = tenant specific data queries
        self.logger.debug("QueryEngine initializeTenantConfig")

    def FetchFeederDTMasterNodes(self, aRequesterObj):
		#Execute yaml query to fetch master nodes
        input_df = {}
        input_df["node_master_df"] = self.getDTMaster(aRequesterObj.ModelType)
        input_df["parentnode_master_df"] = self.getFeederMaster()
        self.logger.debug("FetchFeederDTMasterNodes method executed...")
        return input_df
    	   
    def FetchFeederDTForChunk(self, lChunkDTO):
		#Execute yaml query to fetch incremental data
        input_df = {}
		
        input_df["node_master_df"] = self.getDTMaster(lChunkDTO.ModelType,lChunkDTO)
        input_df["parentnode_master_df"] = self.getFeederMaster(lChunkDTO)
        input_df["node_pnode_df"] = self.getNodePnodeChunkMapping(lChunkDTO)
        input_df["event_outage_df"] = self.getEventOutageData(lChunkDTO)
        input_df["load_survey_df"] = self.getLoadSurveyData(lChunkDTO)
        return input_df
	          
    def FetchFeederDTMasterOnlyForChunk(self, lChunkDTO):
		#Execute yaml query to fetch incremental data
        input_df = {}
		
        input_df["node_master_df"] = self.getDTMaster(lChunkDTO.ModelType)
        input_df["parentnode_master_df"] = self.getFeederMaster()
        input_df["node_pnode_df"] = self.getNodePnodeMapping(lChunkDTO)
        return input_df
		   
    #Execute yaml query to fetch Feeder Master
    def getFeederMaster(self, lChunkDTO = None):
        if lChunkDTO is None:
            dfFeederMaster = self.source_dbmanager.get_data(self.querylist["get_feeder_master"]\
						.format(-1,-1,-1))
        else:		
            dfFeederMaster = self.source_dbmanager.get_data(self.querylist["get_feeder_master"]\
						.format(lChunkDTO.ChunkStart, lChunkDTO.ChunkEnd, lChunkDTO.BatchID))
        self.logger.debug(f"dfFeederMaster {len(dfFeederMaster)}")
        return dfFeederMaster
		
    #Execute yaml query to fetch DT Master
    def getDTMaster(self, ModelType = -1, lChunkDTO = None):
        if lChunkDTO is None:
            dfDTMaster = self.source_dbmanager.get_data(self.querylist["get_dt_master"]\
						.format(-1,-1,-1,ModelType))
        else:
            dfDTMaster = self.source_dbmanager.get_data(self.querylist["get_dt_master"]\
						.format(lChunkDTO.ChunkStart, lChunkDTO.ChunkEnd, \
								lChunkDTO.BatchID, ModelType))
            self.logger.debug(f"dfDTMaster with modeltype: {len(dfDTMaster)} {ModelType} {lChunkDTO.ChunkStart} {lChunkDTO.ChunkEnd} {lChunkDTO.BatchID}")
        #Get selected field for DTConsumer model else as is #
		#As same dt master is used as node and pnode
        if ModelType == enmModelType.DTCONSUMER.value:
            squery = '''Select distinct on (node) node as pnode,meterserialno as pmeterserialno,
					nodename as pnodename,coordinates as pcoordinates,feeder_id,feeder_name,
					feeder_meter,substation_name,substation_id,gaaname,gaaid,nodetype
					from dfDTMaster'''
            dfDTMaster = psql.query(squery).to_df()
            self.logger.debug(f"dfDTMaster with modeltype: {len(dfDTMaster)} {ModelType}")
        self.logger.debug(f"dfDTMaster {len(dfDTMaster)}")
        return dfDTMaster
		
    #Execute yaml query to fetch Consumer Master
    def getConsumerMasterMinMax(self):
        dfNodeMinMax = self.source_dbmanager.get_data(self.querylist["get_consumer_master_minmax"])
        self.logger.debug(f"getConsumerMasterMinMax {len(dfNodeMinMax)}")
        return dfNodeMinMax
		
    #Execute yaml query to fetch Consumer Master
    def getConsumerMasterForChunk(self, lChunkDTO = None):
        # self.logger.debug(f"getConsumerMasterForChunk Start: {lChunkDTO.ChunkStart} End: {lChunkDTO.ChunkEnd}")
        dfConsChunk = self.source_dbmanager.get_data(self.querylist["get_consumer_master_chunk"]\
		    .format(lChunkDTO.ChunkStart,lChunkDTO.ChunkEnd, lChunkDTO.TenantID))
        # self.logger.debug(f"getConsumerMasterForChunk {len(dfConsChunk)}")
        return dfConsChunk

    #Execute yaml query to fetch incremental load survey data
    def getLoadSurveyData(self, lChunkDTO, lPNodeArr = None, lNodeArr = None):
        dfLoadSurveyData = self.source_dbmanager.get_data(self.querylist["get_load_survey_data"]\
						.format(lPNodeArr, lChunkDTO.FromDate, lChunkDTO.ToDate))
		#Append consumer data in same df for DTConsumer modeltype
        if lChunkDTO.ModelType == enmModelType.DTCONSUMER.value:
            dfConsLSData = self.source_dbmanager.get_data(self.querylist["get_load_survey_data_cons"]\
						.format(lNodeArr, lChunkDTO.FromDate, lChunkDTO.ToDate))
            squery = '''select * from dfLoadSurveyData
						union
						select * from dfConsLSData'''
            dfLoadSurveyData = psql.query(squery).to_df()
		#Endif ModelType
        if (dfLoadSurveyData is not None):
            dfLoadSurveyData.sort_values(by=["meterno","surveydate","sdatetime"],inplace=True)
            self.logger.debug(f"dfLoadSurveyData after sorting {len(dfLoadSurveyData)}")
        return dfLoadSurveyData

    #Execute yaml query to fetch incremental event outage data
    def getEventOutageData(self, lChunkDTO, lPNodeArr = None, lNodeArr = None):
        dfEventOutageData = self.source_dbmanager.get_data(self.querylist["get_event_data"]\
						.format(lPNodeArr, lChunkDTO.FromDate, lChunkDTO.ToDate))
		#Append consumer data in same df for DTConsumer modeltype
        if lChunkDTO.ModelType == enmModelType.DTCONSUMER.value:
            dfConsEventOutageData = self.source_dbmanager.get_data(self.querylist["get_event_data_cons"]\
						.format(lNodeArr, lChunkDTO.FromDate, lChunkDTO.ToDate))
            squery = '''select * from dfEventOutageData
						union
						select * from dfConsEventOutageData'''
            dfEventOutageData = psql.query(squery).to_df()
		#Endif ModelType
        if (dfEventOutageData is not None):
            dfEventOutageData.sort_values(by=["meterno","datetime"],inplace=True)
            self.logger.debug(f"dfEventOutageData after sorting {len(dfEventOutageData)}")
        return dfEventOutageData
		
    #Query to fetch tenant wise configured preciction schema and other details
    def getPredictionSchema(self):
		#DB query
        return self.dbmanager.get_data("select * from sp_getpredictionschema()")

    #Execute yaml query to fetch incremental event outage data
    def SaveDataFrameIntoDB(self, aDataFrame, aTableName = '', aSchemaName = ''):
		#Bulk insert data frame into db
        self.dbmanager.insert_data(aDataFrame, aTableName)
		
    # update and insert data frame into db
    def UpsertDataFrameIntoDB(self, aDataFrame, aTableName, aPrimaryKey, aSchemaName = None):
        self.dbmanager.upsert_data(aDataFrame, aTableName, aPrimaryKey, aSchemaName)
        self.logger.debug(f"Upserting successful...")

	#Create batch definition for requester job
    def CreateBatchDefinition(self, aRequesterObj):
		#Arrange columns data frame inot a required format
        squery = '''select * from sp_di_createbatchdefinition({},{},{},{},{},'{}','{}')'''
        df = self.dbmanager.get_data(squery.format(aRequesterObj.TenantID, \
					aRequesterObj.BatchID, aRequesterObj.TaskType,aRequesterObj.ModelType,\
					aRequesterObj.Status, aRequesterObj.FromDate,aRequesterObj.ToDate))
        self.logger.debug(f"CreateBatchDefinition method executed...")
        return df

	#Create batch definition for requester job
    def UpdateBatchStatus(self, aRequesterObj):
		#Arrange columns data frame inot a required format
        squery = '''select * from sp_di_updatebatchstatus({},{},{},{},{})'''
        self.dbmanager.get_data(squery.format(aRequesterObj.TenantID, \
					aRequesterObj.BatchID, aRequesterObj.Status, aRequesterObj.TaskType,
					aRequesterObj.ModelType))
        self.logger.debug(f"UpdateBatchStatus method executed...")
        #return aBatchID

	#Store chunk node list into db for further chunk wise processing
    def StoreChunkNodeList(self, aDataFrame):
		#Arrange columns data frame inot a required format
        squery = '''select batchid,tenantid,rank as chunkno,tasktype,node::bigint,
					-1 as nodetype,meterserialno,pnode::bigint as pnode,-1 as pnodetype,pmeterserialno,
					gis_distance,fromdate,todate,islastchunk,modeltype,current_timestamp
					from aDataFrame order by chunkno'''
        tmp_df = psql.query(squery).to_df()
		#Bulk insert data frame into db
        self.dbmanager.insert_data(tmp_df, "tb_di_batch_chunknodes")
        self.logger.debug(f"StoreChunkNodeList method executed...")
			
	#Store chunk node list into db for further chunk wise processing
    def StoreChunkNodePnodeList(self, aDataFrame):
		#Arrange columns data frame inot a required format
        squery = '''select distinct on (node, pnode) batchid,tenantid,
					chunkno,node::bigint,meterserialno,pnode::bigint,
					pmeterserialno,gis_distance,fromdate,todate,islastchunk,
					modeltype,cluster_segment from aDataFrame'''
        tmp_df = psql.query(squery).to_df()
		#Bulk insert data frame into db
        self.dbmanager.insert_data(tmp_df, "tb_di_batch_nodepnodelist")
        self.logger.debug(f"StoreChunkNodePnodeList method executed...")
			
	#temp: store interim probable list into db
    def log_probablelist_datewise(self, node_probableList_df, module_option, stepname):
        squery = '''select node,meterserialno,pnode,pmeterserialno,date,gis_distance::text
					from node_probableList_df'''
        df1 = psql.query(squery).to_df()
        df1["moduleoption"] = module_option
        df1["step"] = stepname
        self.dbmanager.insert_data(df1, "tb_tmp_node_pnode_list1")
        del [df1]
		
	#temp: store interim probable list into db
    def log_probablelist_uniquedatewise(self, node_probableList_df, module_option, stepname):
        squery = '''select node,cMeterserialno,cal_dtcode as pnode,null as pmeterserialno,
					Date,null as gis_distance
					from node_probableList_df'''
        df1 = psql.query(squery).to_df()
        df1["moduleoption"] = module_option
        df1["step"] = stepname
        self.dbmanager.insert_data(df1, "tb_tmp_node_pnode_list1")
        del [df1]
			
	#temp: store interim probable list into db
    def log_probablelist_datewise_01(self, consumer_rank_df, node_probableList_df, module_option, stepname):
        squery = '''select a2.node,a1.* from
					(select distinct on (dt_code,meterno,cMeterserialno,SURVEYDATE4) dt_code as pnode,
					cMeterserialno as meterserialno,meterno as pmeterserialno,SURVEYDATE4 as date,
					null as gis_distance from node_probableList_df) a1
					left join consumer_rank_df a2 on a1.meterserialno = a2.meterserialno'''
        df1 = psql.query(squery).to_df()
        df1["moduleoption"] = module_option
        df1["step"] = stepname
        self.dbmanager.insert_data(df1, "tb_tmp_node_pnode_list1")
        del [df1]

	#Function to fetch prediction result from database based on modeltype
    def fetch_results(self, aRequesterObj=None):

    	#Condition to check modeltype
        if aRequesterObj.ModelType == enmModelType.DTCONSUMER.value: 
            result = self.dbmanager.get_data(self.querylist["get_dtconsumer_result"].format(aRequesterObj.BatchID))
        elif aRequesterObj.ModelType == enmModelType.FEEDERDT.value:
            result = self.dbmanager.get_data(self.querylist["get_feederdt_result"].format(aRequesterObj.BatchID))
        
        return result
		
    #Method to manage partitions at schedule or every trigger time
    def ManagePredictionSchema(self):
		#Execute partman DB query
        return self.dbmanager.get_data("SELECT partman.run_maintenance();")

    def fetch_all_batchid(self):
        #fetch all batchids which have finished processing and are unpublished 
        return self.dbmanager.get_data("select distinct batchid from tb_di_batch_definition where status <> 0 and sendresponse = 0 order by batchid desc;")
    
    def update_batch_status(self, response = -1, aRequesterObj=None):
        #update the status of batch in tb_di_batch_definition table:
        self.dbmanager.execute_query(f"update tb_di_batch_definition set sendresponse = {response} where batchid = {aRequesterObj.BatchID};")