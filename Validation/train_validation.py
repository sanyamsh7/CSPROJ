# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 15:06:28 2022

@author: SA20202305
"""

from datetime import datetime
from raw_validation import ValidateRawData
from data_transform import DataTransform



class TrainValidation:
    def __init__(self, path):
        self.rawData = ValidateRawData(path)
        self.dataTransform = DataTransform()
        self.dfOperations = databaseOperations()