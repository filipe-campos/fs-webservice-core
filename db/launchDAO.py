# -*- coding: utf-8 -*-
import mysql.connector
import datetime
import logging
import json
import traceback
import inspect

from dateutil.relativedelta import *
from datetime import datetime
from util import Util, Log, Constants, CodeReturn
from db import AccountDAO
from flask import Flask
from flask_mysqldb import MySQL

log = Log('LaunchDAO')
constants = Constants()
codeReturn = CodeReturn()
util = Util()
mySQL = MySQL()
accountDAO = AccountDAO()

app = Flask(__name__)

class LaunchDAO:
    def __init__(self):
        mySQL.init_app(app)

    #Date format received: mm/yyyy
    def list_launch(self, date, cod_account, companies_id, user_id):
        try:
            date = datetime.strptime(date, "%m/%Y")
            date = str(date.year)+'-'+str(date.month).rjust(2, '0')+'-%'

            list_launch = []
            cursorMySQL = mySQL.connection.cursor()

            companies_id_query = util.get_companies_id_query(companies_id)

            queryMySQL = ("SELECT date_launch, debit, credit, description_launch FROM vw_launch "
                          "WHERE date_launch LIKE '"+date+"' "
                          "AND cod_account = '"+cod_account+"' "
                          +str(companies_id_query)+
                          "ORDER BY date_launch;")

            cursorMySQL.execute(queryMySQL)

            
            for rowMySQL in cursorMySQL:
                value = 0
                launch_type = ''

                if(rowMySQL[2] > 0):
                    value = float(rowMySQL[2])
                    launch_type = 'C'
                else:
                    value = float(rowMySQL[1])
                    launch_type = 'D'

                list_launch.append({
                    'date_launch': rowMySQL[0].strftime("%d/%m/%Y"),
                    'value': value,
                    'launch_type': launch_type,
                    'description': rowMySQL[3]
                })

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(list_launch)
    
    #Date format received: mm/yyyy
    def list_launch_period(self, date_start, date_end, cod_account, companies_id, user_id):
        try:
            #Date Configuration
            date_start = util.parse_date(date_start, constants.DATE_START_TYPE)
            date_end = util.parse_date(date_end, constants.DATE_END_TYPE)

            launch_list = []
            cursorMySQL = mySQL.connection.cursor()

            companies_id_query = util.get_companies_id_query(companies_id)
            
            queryMySQL = ("SELECT date_launch, debit, credit, description_launch FROM vw_launch "
                          "WHERE date_launch >= '"+date_start+"' AND date_launch <= '"+date_end+"' "
                          "AND cod_account = '"+cod_account+"' "
                          +str(companies_id_query)+
                          "ORDER BY date_launch;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                value = 0
                launch_type = ''

                if(rowMySQL[2] > 0):
                    value = float(rowMySQL[2])
                    launch_type = 'C'
                else:
                    value = float(rowMySQL[1])
                    launch_type = 'D'

                launch_list.append({
                    'date_launch': rowMySQL[0].strftime("%d/%m/%Y"),
                    'value': value,
                    'launch_type': launch_type,
                    'description': rowMySQL[3]
                })

            cursorMySQL.close()

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, launch_list

    def insert_launch(self, launch, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            acc = accountDAO.get_acc_by_cod(launch['cod_account'], company_id)
            
            if(acc != codeReturn.OBJECT_NOT_FOUND_CODE):
                queryMySQL = ("INSERT INTO launch "
                              "(date_launch, debit, credit, lot, description, account_id) "
                              "VALUES ('"+str(launch['date_launch'])+"', "+str(launch['debit'])+", "+str(launch['credit'])+", "+str(launch['lot'])+", '"+str(launch['description'])+"', "+str(acc['id'])+");")

                cursorMySQL.execute(queryMySQL)
                mySQL.connection.commit()
                
            else:
                log.warning(inspect.getframeinfo(inspect.currentframe()).function, 
                            codeReturn.OBJECT_NOT_FOUND_MSG+' | cod_account: '+str(launch['cod_account']), 
                            user_id)
              
            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []      

    def delete_all_launch(self, company_id, user_id):
        try:    
            cursorMySQL = mySQL.connection.cursor()
            
            queryMySQL = ("DELETE launch FROM launch "
                          "INNER JOIN account ON account.id = launch.account_id "
                          "WHERE account.company_id = "+str(company_id)+";")

            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    #Date format: mm/yyyy
    def delete_launch_month(self, date, company_id, user_id):
        try:
            date = date[3:7] + '-' + date[0:2] + '-%'
            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("DELETE launch FROM launch "
                          "INNER JOIN account ON account.id = launch.account_id "
                          "WHERE date_launch LIKE '"+date+"' AND account.company_id = "+str(company_id)+";")
        
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

        

   