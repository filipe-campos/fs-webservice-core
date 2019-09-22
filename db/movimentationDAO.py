# -*- coding: utf-8 -*-
import mysql.connector
import datetime
import logging
import json
import traceback
import inspect

from dateutil.relativedelta import *
from util import Util, Log, Constants, CodeReturn
from flask import Flask
from flask_mysqldb import MySQL

log = Log('MovimentationDAO')
constants = Constants()
codeReturn = CodeReturn()
util = Util()
mySQL = MySQL()

app = Flask(__name__)

class MovimentationDAO:
    def __init__(self):
        mySQL.init_app(app)

    #Date format: mm/yyyy
    def calculate_movimentation(self, date, company_id, user_id):
        try:
            date_mov = util.parse_date(date, constants.DATE_START_TYPE)
            date_search = str(date[3:7]) + '-' + str(date[0:2]).rjust(2, '0') + '-%'

            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("SELECT SUM(debit), SUM(credit), cod_account_ref, description_ref, father_ref, account_id "
                          "FROM vw_launch "
                          "WHERE date_launch LIKE '"+str(date_search)+"' AND company_id = "+str(company_id)+" "
                          "GROUP BY cod_account;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                debit = float(rowMySQL[0])
                credit = float(rowMySQL[1])
                #cod_account_ref = str(rowMySQL[2])
                #description_ref = str(rowMySQL[3])
                #father = str(rowMySQL[4])
                acc_id = str(rowMySQL[5])

                value_mov = debit + credit

                #Insert Movimentation
                movimentation = {
                    'value_mov': value_mov,
                    'date_mov': date_mov,
                    'account_id': acc_id
                }

                if(acc_id != 0):
                    self.insert_movimentation(movimentation, company_id, user_id)
                else:
                    log.warning(inspect.getframeinfo(inspect.currentframe()).function, 
                                'Account ID = 0', 
                                user_id)
                    log.warning(inspect.getframeinfo(inspect.currentframe()).function, 
                                'QueryMySQL: '+ str(queryMySQL), 
                                user_id)
            
            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    def calculate_all_movimentation(self, company_id, user_id):
        try:
            del_code, del_msg, del_data = self.delete_all_movimentation(company_id, user_id)
            if(del_code == codeReturn.UNKNOW_ERROR_CODE):
                return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

            cursorMySQL = mySQL.connection.cursor()

            #Get the first date in Launch
            queryMySQL = ("SELECT date_launch FROM vw_launch "
                        "WHERE company_id = "+str(company_id)+" ORDER BY date_launch ASC LIMIT 5;")

        
            cursorMySQL.execute(queryMySQL)

            rowMySQL = cursorMySQL.fetchone()

            date_initial = rowMySQL[0]
        

            date = date_initial
            date_search = str(date.year) + '-' + str(date.month).rjust(2,'0') + '-%'
            date_mov = str(date.year) + '-' + str(date.month) + '-01'

            while(date <= datetime.date.today()):
                queryMySQL = ("SELECT SUM(debit), SUM(credit), cod_account_ref, description_ref, father_ref, account_id "
                              "FROM vw_launch "
                              "WHERE date_launch LIKE '"+str(date_search)+"' AND company_id = "+str(company_id)+" "
                              "GROUP BY cod_account;")

                cursorMySQL.execute(queryMySQL)

                for rowMySQL in cursorMySQL:
                    debit = float(rowMySQL[0])
                    credit = float(rowMySQL[1])
                    #cod_account_ref = str(rowMySQL[2])
                    #description_ref = str(rowMySQL[3])
                    #father = str(rowMySQL[4])
                    acc_id = str(rowMySQL[5])

                    value_mov = debit + credit

                    #Insert Movimentation
                    movimentation = {
                        'value_mov': value_mov,
                        'date_mov': date_mov,
                        'account_id': acc_id
                    }

                    if(acc_id != 0):
                        self.insert_movimentation(movimentation, company_id, user_id)
                    else:
                        log.warning(inspect.getframeinfo(inspect.currentframe()).function, 
                                'Account ID = 0', 
                                user_id)
                        log.warning(inspect.getframeinfo(inspect.currentframe()).function, 
                                'QueryMySQL: '+ str(queryMySQL), 
                                user_id)

                #Increment month
                date = date + relativedelta(months=+1)
                date_search = str(date.year) + '-' + str(date.month).rjust(2, '0') + '-%'
                date_mov = str(date.year) + '-' + str(date.month) + '-01'
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    def insert_movimentation(self, movimentation, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("INSERT INTO movimentation "
                        "(value_mov, date_mov, account_id) "
                        "VALUES ('" + str(movimentation['value_mov']) + "', '" + str(movimentation['date_mov']) + "', '" + str(movimentation['account_id']) + "')")

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
    def delete_month_movimentation(self, date, company_id, user_id):
        try:
            date_delete = str(date[3:7]) + '-' + str(date[0:2]).rjust(2, '0') + '-%'

            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("DELETE movimentation FROM movimentation "
                        "INNER JOIN account acc ON acc.id = movimentation.account_id "
                        "WHERE date_mov LIKE '"+str(date_delete)+"' AND acc.company_id = "+str(company_id)+";")
        
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    def delete_all_movimentation(self, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("DELETE movimentation FROM movimentation "
                        "INNER JOIN account acc ON acc.id = movimentation.account_id "
                        "WHERE acc.company_id = "+str(company_id)+";")

            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
        
        
