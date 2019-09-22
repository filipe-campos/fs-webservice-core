# -*- coding: utf-8 -*-
import mysql.connector
import json
import traceback
import inspect

from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from dateutil.relativedelta import *
from util import Util, Log, Constants, CodeReturn
from flask import Flask
from flask_mysqldb import MySQL
from db import DreDAO, BalanceDAO

log = Log('FileLayoutDAO')
constants = Constants()
codeReturn = CodeReturn()
util = Util()
mySQL = MySQL()
dreDAO = DreDAO()
balanceDAO = BalanceDAO()

mongo_client = MongoClient(
    'mongodb://fiscoserv:43208Fisco!1@200.98.145.91:27017/', connect=False)
mongo_db = mongo_client['fiscoserv']
coll_file_layout = mongo_db['file_layout']

app = Flask(__name__)

class FileLayoutDAO:
    def __init__(self):
        mySQL.init_app(app)

    def get_file_layout(self, import_type, company_id, user_id):
        try:
            file_layout_list= []

            for layout in coll_file_layout.find({
                "company_id":str(company_id), 
                "import_type":str(import_type)
                }):
                
                file_layout_list.append({
                    '_id': str(layout['_id']),
                    'file_type': str(layout['file_type']),
                    'name': str(layout['name']),
                    'fields': layout['fields']
                })

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(file_layout_list, sort_keys=True)
    
    def get_one_file_layout(self, layout_id, company_id, user_id):
        try:
            layout = coll_file_layout.find_one({"_id" : ObjectId(layout_id)})

            data_return = {
                    '_id': str(layout['_id']),
                    'file_type': str(layout['file_type']),
                    'name': str(layout['name']),
                    'fields': layout['fields']
                }

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(data_return)
    
    def insert_file_layout(self, data, company_id, user_id):
        try:
            layout_insert = {
              "company_id": str(company_id),
              "import_type": data['import_type'],
              "file_type": data['file_type'],
              "name": data['name'],
              "fields": data['fields']
            }

            coll_file_layout.insert_one(layout_insert)
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    def delete_file_layout(self, layout_id, user_id):
        try:
            coll_file_layout.delete_one({"_id":ObjectId(layout_id)})
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
    

    
