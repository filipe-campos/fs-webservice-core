# -*- coding: utf-8 -*-
import mysql.connector
import logging
import json
import traceback
import inspect

from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from dateutil.relativedelta import *
from util import Util, Log, Constants, CodeReturn
from flask import Flask

log = Log('NikoleDAO')
constants = Constants()
codeReturn = CodeReturn()
util = Util()

mongo_client = MongoClient('mongodb://'+str(configuration.MONGO_DB_FS)+':'
                           +str(configuration.MONGO_PASS)+'@'+str(configuration.MONGO_HOST)+':'
                           +str(configuration.MONGO_PORT)+'/', connect=False)

mongo_db = mongo_client[configuration.MONGO_DB_FS]
coll_nikole = mongo_db['nikole']

app = Flask(__name__)

class NikoleDAO:
    def __init__(self):
        pass

    def get_messages(self, companies_id, user_id):
        try:
            data_return = []

            for company_id in companies_id:
                for message in coll_nikole.find({
                    "company_id":str(company_id), 
                    "status":str(constants.NIKOLE_MSG_STATUS_NOT_READ)
                    }):
                    
                    data_return.append({
                        'id': str(message['_id']),
                        'status': str(message['status']),
                        'type': str(message['type']),
                        'message': message['message']
                    })

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(data_return)

    def read_message(self, msg_id, user_id):
        try:
            coll_nikole.find_one_and_update({
                                    "_id": ObjectId(msg_id)
                                }, 
                                {
                                    "$set": {"status": "1"}
                                })

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
    
    def send_msg_success(self, title, text, company_id):
        try:
            log_insert = {
                "date_msg": datetime.now().strftime("%d/%m/%Y %H:%M"),
                "company_id": company_id,
                "status": "0",
                "type": "0",
                "message": {
                    "title": title,
                    "text": text,
                    "buttons": [{
                        "text" : "Ok",
                        "class": "success",
                        "msg_return": "",
                        "action": "3"
                    }]
                }
            }
        
            coll_nikole.insert_one(log_insert)
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []


