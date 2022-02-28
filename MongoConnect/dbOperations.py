# -*- coding: utf-8 -*-

import shutil
import pymongo
from datetime import datetime
from os import listdir
import json
import os
import sys
import pandas as pd

class dbOperations:
    def __init__(self, opPath, configPath):
        self.opPath = opPath
        self.config = configPath
        
        with open(self.config, "r",encoding="iso-8859-1") as file:
            configData = json.load(file)
            self.client = pymongo.MongoClient(host=configData.get("db_hostName"), port=configData.get("db_portNumber"))
            self.db = self.client[configData.get("databaseName")] 
        
        self.goodFilePath = "../Validation/Training_Raw_files/Good_Raw"
        self.badFilePath = "../Validation/Training_Raw_files/bad_Raw"
    
    def getCollectionData(self, query):
        try:
            dbCollection = self.db[query["collection_name"]]
            query.pop("collection_name")
            logdata = [log for log in dbCollection.find(query,{"_id":False})]
            return logdata
        
        except Exception as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print(error)
            return -1
    
    def insertCollectionData(self, query):
        try:
            if isinstance(query["logdata"], list):
                dbCollection = self.db[query["collection_name"]]
                dbCollection.insert_many(query["logdata"])
               
            if isinstance(query["logdata"], dict):
                dbCollection = self.db[query["collection_name"]]
                dbCollection.insert_one(query["logdata"])
            
            return 0

        except Exception as emsg:
            # self.catchDataError(query)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print(error)
            return -1
            
    # def catchDataError(self, data):
    #     with open("error.txt", "w") as file:
    #         file.write(str(data))
    
    def dataFromCollectionToCsv(self, query):
        self.dataPath = '../TrainingFileFromDb/'
        self.fileName = 'inputFile.csv'
        try:
            self.response = self.getCollectionData(query)
            if self.response != -1:
                if not os.path.isdir(self.dataPath):
                    os.makedirs(self.dataPath)
                
                with open(self.dataPath + self.fileName, "w", newline = '') as file:
                    data = pd.DataFrame(self.response)
                    data.to_csv(file)
            else:
                raise Exception("Exporting data from db failed!")
        
        except Exception as emsg:
            # self.catchDataError(query)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print(error)      
        
            