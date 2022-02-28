# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 21:41:38 2022

@author: Sanyam Sharma
"""
from datetime import datetime
from os import listdir
import pandas as pd

class DataTransform:
    def __init__(self):
        self.goodDataPath = "../Validation/Training_Raw_files/Good_Raw"
        
    def addQuotesToStringInCol(self):
        pass