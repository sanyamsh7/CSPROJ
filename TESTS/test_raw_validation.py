# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 16:37:18 2022

@author: Sanyam Sharma
"""
import sys
import os
wdir = os.getcwd()
sys.path.insert(0, os.path.join("\\".join(wdir.split("\\")[:-1]), "Validation\\"))
from Validation import raw_validation
# import pytest

# add scripts paths 
os.chdir("../")

validate = raw_validation.ValidateRawData("Validation/")
def test_raw_validation():
   
    dateStamplen, timeStampelen, colNames, numberOfCol = validate.configValues()
    validate.creatDirGoodBadRaw()
    validate.delExistingBadDataDir()
    validate.delExistingGoodDataDir()
    validate.moveBadFilesToBadArchive()
    validate.validateColLen()
    validate.validateNullValInCol()
    regex = validate.regCreation()
    validate.validateRawFileName(regex, dateStamplen, timeStampelen)


test_raw_validation()

