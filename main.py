# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 14:44:42 2022

@author: Sanyam Sharma
"""

from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
from flask_cors import CORS, cross_origin 
import os


app = Flask(__name__)
# dashboard.bind(app)
CORS(app)

@app.route("/model/predict", method=['POST'])
@cross_origin()
def predRouteClient():
    try:
        if request.json is not None:
            path = request.json['filepath']
            
            validation = 