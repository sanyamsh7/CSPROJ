# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 15:09:20 2022

@author: SA20202305
"""

from datetime import datetime
from os import listdir
import os 
import re
import json 
import shutil
import pandas as pd
import sys

class ValidateRawData:
    
    def __init__(self, path):
        self.BatchDir = path
        self.config = "../config.json"
        
    def configValues(self):
        try:
            with open(self.config, 'r') as file:
                dic = json.load(file)
            
            pattern = dic['sample_file_name']
            dateStamplen = dic['dateStamplen']
            timeStampelen = dic['timeStampelen']
            colNames = dic['colNames']
            numberOfCol = dic['numberOfCol']
            
            print('dateStamplen: {} \t timeStampelen: {} \t numberOfCol: {}'.format(dateStamplen,timeStampelen,numberOfCol))
        except ValueError as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print('value not found inside validation config: {}'.format(error))
        
        except KeyError as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print('key not found inside validation config: {}'.format(error))
        
        except Exception as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print(error)
        
            
        return dateStamplen, timeStampelen, colNames, numberOfCol
            
    def regCreation(self):
        regex = "['CS_']+[\d_]+[\d]+\.csv"
        return regex
            
    def creatDirGoodBadRaw(self):
        try:
            print("Creating Good / Bad directories")
            path = os.path.join("Validation/Training_Raw_files/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            
            path = os.path.join("Validation/Training_Raw_files/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
        
        except OSError as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print(error)
            raise OSError
    
    def delExistingGoodDataDir(self):
        try:
            path = 'Validation/Training_Raw_files/'
            if os.path.isdir(path+ 'Good_Raw/'):
                shutil.rmtree(path+ 'Good_Raw/')
                print("Good_Raw directory deleted successfully")
            
        except OSError as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print(error)
            raise OSError
            
    def delExistingBadDataDir(self):
        try:
            path = 'Validation/Training_Raw_files/'
            if os.path.isdir(path+ 'Bad_Raw/'):
                shutil.rmtree(path+ 'Bad_Raw/')
                print("Bad_Raw directory deleted successfully")
            
        except OSError as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print(error)
            raise OSError
            
    def moveBadFilesToBadArchive(self):
        print("moving bad files to bad archives")
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            
            source = "Validation/Training_Raw_files/Bad_Raw/"
            if os.path.isdir(source):
                path = "BadTrainingArchives"
                if not os.path.isdir(path):
                    os.makedirs(path)
                    
                dest = "Validation/BadTrainingArchives/BadData_" + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source+f, dest)
                        print("bad files move to archive")
                
                self.delExistingBadDataDir()
        except Exception as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print(error)
            print("Error while moving bad files to archive folder")
            
    def validateRawFileName(self, regex, dateStamplen, timeStampelen):
        print("Validating raw file names")
        self.delExistingBadDataDir()
        self.delExistingGoodDataDir()

        files = [file for file in listdir(self.BatchDir)]
        try:
            self.creatDirGoodBadRaw() 
            # regex = "['CS_']+[\d_]+[\d]+\.csv"
            for filename in files:
                if (re.match(regex, filename)):
                    dotSplit = re.split('.csv', filename)
                    undSplit = re.split('_', dotSplit[0])
                    if len(undSplit[1]) == dateStamplen:
                        if len(undSplit[2]) == timeStampelen:
                            shutil.copy("Validation/Training_Batch_files/"+filename, "Validation/Training_Raw_files/Good_Raw")
                            print("valid file name !! file is moved to good raw folder: {}".format(filename))
                        else:
                            shutil.copy("Validation/Training_Batch_files/"+filename, "Validation/Training_Raw_files/Bad_Raw")
                            print("Invalid file name !! file is moved to Bad raw folder: {}".format(filename))
                    else:
                        shutil.copy("Validation/Training_Batch_files/"+filename, "Validation/Training_Raw_files/Bad_Raw")
                        print("Invalid file name !! file is moved to Bad raw folder: {}".format(filename))
            
                else:
                    shutil.copy("Validation/Training_Batch_files/"+filename, "Validation/Training_Raw_files/Bad_Raw")
                    print("Invalid file name !! file is moved to Bad raw folder: {}".format(filename))
        except Exception as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print(error)
            print("Error occcured while validating the filename")
            
    def validateColLen(self, numberOfCol):
        
        try:
            print("Validating columns")
            for file in listdir("Validation/Training_Raw_files/Good_Raw/"):
                csv = pd.read_csv("Training_Raw_files/Good_Raw/"+file)
                if csv.shape[1] == numberOfCol:
                    pass
                else:
                    shutil.move("Validation/Training_Raw_files/Good_Raw/"+file, "Validation/Training_Raw_files/Bad_Raw")
                    print("Invalid column length for the file ! file moved to bad raw folder : {}".format(file))
            
        except OSError as emsg:
           exc_type, exc_obj, exc_tb = sys.exc_info()
           error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
           print("error occured while moving the file :{} ".format(error))
           raise OSError
           
        except Exception as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print("Error occcured {}".format(error))
            
    def validateNullValInCol(self):
        try:
            print("Validating Null values in column")
            for file in listdir("Validation/Training_Raw_files/Good_Raw/"):
                csv = pd.read_csv("Validation/Training_Raw_files/Good_Raw/"+file)
                if any(csv.isnull().all()):
                    shutil.move("Validation/Training_Raw_files/Good_Raw/"+file, "Validation/Training_Raw_files/Bad_Raw")
                    print("Invalid column length for the file ! File moved to bad Raw folder: {}".format(file))
                else:
                    csv.rename(columns = {"Unnamed: 0": "Wafer"}, inplace = True)
                    csv.to_csv("Validation/Training_Raw_files/Good_Raw/"+file, index = None, header=True)
       
        except OSError as emsg:
           exc_type, exc_obj, exc_tb = sys.exc_info()
           error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
           print("error occured while moving the file :{} ".format(error))
           raise OSError
           
        except Exception as emsg:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error = str(exc_type) + '-' + str(emsg) +' - '+ str(exc_tb.tb_lineno)
            print("Error occcured {}".format(error))
        