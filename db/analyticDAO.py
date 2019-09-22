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
from flask_mysqldb import MySQL
from configuration import Configuration
from db import DreDAO, BalanceDAO

log = Log('AnalyticDAO')
constants = Constants()
codeReturn = CodeReturn()
configuration = Configuration()
util = Util()
mySQL = MySQL()
dreDAO = DreDAO()
balanceDAO = BalanceDAO()

mongo_client = MongoClient('mongodb://'+str(configuration.MONGO_DB_FS)+':'
                           +str(configuration.MONGO_PASS)+'@'+str(configuration.MONGO_HOST)+':'
                           +str(configuration.MONGO_PORT)+'/', connect=False)

mongo_db = mongo_client[configuration.MONGO_DB_FS_STORM]
coll_serie = mongo_db['serie']
coll_serie_type = mongo_db['serie-type']
coll_serie_data = mongo_db['serie-data']

app = Flask(__name__)

class AnalyticDAO:
    def __init__(self):
        mySQL.init_app(app)        

    # Date Format: mm/yyyy
    def get_acc_ref_mov_by_classification(self, classification_ref, date_start, date_end, companies_id, user_id):
        try:
            serie_data_list = []
            
            #Contas de Resultado devem ser calculadas separadas
            if(classification_ref in constants.CLASSIFICATION_ACC_RESULT):
                #Account 5
                acc3_list = json.loads(str(self.get_acc_ref_mov_by_classification('3', date_start, date_end, companies_id, user_id)).replace("'", '"'))
                acc4_list = json.loads(str(self.get_acc_ref_mov_by_classification('4', date_start, date_end, companies_id, user_id)).replace("'", '"'))
                acc5_list = []

                for i in range(len(acc3_list)):
                    try:
                        acc4_value = acc4_list[i]['value']
                    except:
                        acc4_value = 0
                    
                    acc5_list.append({
                        'date': acc3_list[i]['date'],
                        'value': acc3_list[i]['value'] - acc4_value
                    })
                
                if(classification_ref == '5'):
                    return acc5_list
                
                #Account 7
                acc6_list = json.loads(str(self.get_acc_ref_mov_by_classification('6', date_start, date_end, companies_id, user_id)).replace("'", '"'))
                acc7_list = []

                for i in range(len(acc5_list)):
                    try:
                        acc6_value = acc6_list[i]['value']
                    except:
                        acc6_value = 0

                    acc7_list.append({
                        'date': acc5_list[i]['date'],
                        'value': acc5_list[i]['value'] - acc6_value
                    })
                
                if(classification_ref == '7'):
                    return acc7_list

                #Account 9
                acc8_list = json.loads(str(self.get_acc_ref_mov_by_classification('8', date_start, date_end, companies_id, user_id)).replace("'", '"'))
                acc9_list = []

                for i in range(len(acc7_list)):
                    try:
                        acc8_value = acc8_list[i]['value']
                    except:
                        acc8_value = 0

                    acc9_list.append({
                        'date': acc7_list[i]['date'],
                        'value': acc7_list[i]['value'] - acc8_value
                    })
                
                if(classification_ref == '9'):
                    return acc9_list
                
                #Account 11
                acc10_list = json.loads(str(self.get_acc_ref_mov_by_classification('10', date_start, date_end, companies_id, user_id)).replace("'", '"'))
                acc11_list = []

                for i in range(len(acc9_list)):
                    try:
                        acc10_value = acc10_list[i]['value']
                    except:
                        acc10_value = 0

                    acc11_list.append({
                        'date': acc9_list[i]['date'],
                        'value': acc9_list[i]['value'] - acc10_value
                    })
                                 
                if(classification_ref == '11'):
                    return acc11_list
                
                #Account 13
                acc12_list = json.loads(str(self.get_acc_ref_mov_by_classification('12', date_start, date_end, companies_id, user_id)).replace("'", '"'))
                acc13_list = []

                for i in range(len(acc11_list)):
                    try:
                        acc12_value = acc12_list[i]['value']
                    except:
                        acc12_value = 0

                    acc13_list.append({
                        'date': acc11_list[i]['date'],
                        'value': acc11_list[i]['value'] - acc12_value
                    })
                
                if(classification_ref == '13'):
                    return acc13_list
                
                #Account 15
                acc14_list = json.loads(str(self.get_acc_ref_mov_by_classification('14', date_start, date_end, companies_id, user_id)).replace("'", '"'))
                acc15_list = []

                for i in range(len(acc13_list)):
                    try:
                        acc14_value = acc14_list[i]['value']
                    except:
                        acc14_value = 0

                    acc15_list.append({
                        'date': acc13_list[i]['date'],
                        'value': acc13_list[i]['value'] - acc14_value
                    })
                 
                return acc15_list

            companies_id_query = util.get_companies_id_query(companies_id)

            date_start = util.parse_date(date_start, constants.DATE_START_TYPE)
            date_end = util.parse_date(date_end, constants.DATE_END_TYPE)

            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("SELECT SUM(value_mov), date_mov "
                          "FROM vw_movimentation "
                          "WHERE classification_ref LIKE '"+str(classification_ref)+"%' "
                          "AND date_mov >= '"+date_start+"' AND date_mov <= '"+date_end+"' "
                          +str(companies_id_query)+
                          "AND cod_account_ref != 999 AND cod_account_ref != 998 "
                          "GROUP BY date_mov ORDER BY date_mov;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                value_mov = float(rowMySQL[0])
                date_mov = rowMySQL[1].strftime("%m/%Y")

                serie_data_list.append({
                    'date': date_mov,
                    'value': value_mov
                })

            cursorMySQL.close()

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE
         
        return json.dumps(serie_data_list)

    # Date Format: mm/yyyy
    def get_acc_ref_balance_by_classification(self, classification_ref, date_start, date_end, companies_id, user_id):
        try:
            serie_data_list = []

            date_start = datetime.strptime(date_start, "%m/%Y")
            date_end = datetime.strptime(date_end, "%m/%Y")

            while(date_start <= date_end):
                    date_start_search = str(date_start.month).rjust(2, '0')+'/'+str(date_start.year)
                    date_start = date_start + relativedelta(months=1)

                    date_end_search = str(date_start.month).rjust(2, '0')+'/'+str(date_start.year)

                    acc_list = json.loads(str(balanceDAO.get_balance_comparative(date_start_search, date_end_search, companies_id,user_id)).replace("'", '"'))
                    #print(str(acc_list))
                    for acc in acc_list:
                        if(acc['classification_ref'] == classification_ref):
                            serie_data_list.append({
                                'date': date_start_search,
                                'value': acc['value_date_start']
                            })

                            if(date_start <= date_end):
                                serie_data_list.append({
                                    'date': date_start_search,
                                    'value': acc['value_date_end']
                                })

                            break
                    
                    date_start = date_start + relativedelta(months=1)

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(serie_data_list)

    def list_serie(self, user_id):
        try:
            serie_list = []

            for serie in coll_serie.find():
                serie['serie_type'] = []

                for serie_type in coll_serie_type.find({"serie-id": ObjectId(serie['_id'])}):
                    serie['serie_type'].append({
                        '_id': str(serie_type['_id']),
                        'type': serie_type['type']
                    })

                serie['_id'] = str(serie['_id'])

                serie_list.append(serie)
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(serie_list)

    def list_serie_data(self, serie_type_id, date_start, date_end, user_id):
        try:
            data_return = []

            serie_data = coll_serie_data.find_one(
                {"serie-type-id": ObjectId(serie_type_id)})

            date_start = datetime.strptime(date_start, "%m/%Y")
            date_end = datetime.strptime(date_end, "%m/%Y")

            for data in serie_data['data']:
                data_date = datetime.strptime(data['date'], "%m/%Y")

                if(data_date >= date_start and data_date <= date_end):
                    data_return.append(data)

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(data_return)

    def get_roi(self, date_start, date_end, companies_id, user_id):
        try:
            serie_data_list = []
            
            result_liq_list = json.loads(str(self.get_acc_ref_mov_by_classification('15', date_start, date_end, companies_id, user_id)).replace("'", '"'))
            ativo_list = json.loads(str(self.get_acc_ref_balance_by_classification('1', date_start, date_end, companies_id, user_id)).replace("'", '"'))

            for i in range(len(result_liq_list)):
                roi_value = float("{0:.2f}".format((result_liq_list[i]['value'] / ativo_list[i]['value'])*100))
                
                serie_data_list.append({
                                        'date': result_liq_list[i]['date'],
                                        'value': roi_value
                                    })
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(serie_data_list)

   