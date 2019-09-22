# -*- coding: utf-8 -*-
import mysql.connector
import datetime
import json
import traceback
import inspect

from dateutil.relativedelta import *
from datetime import datetime
from util import Util, Log, Constants, CodeReturn
from flask import Flask
from flask_mysqldb import MySQL

log = Log('AccountDAO')
constants = Constants()
codeReturn = CodeReturn()
util = Util()
mySQL = MySQL()

app = Flask(__name__)

class AccountDAO:
    def __init__(self):
        mySQL.init_app(app)

    def insert_account(self, account, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            code = codeReturn.UNKNOW_ERROR_CODE
            msg = codeReturn.UNKNOW_ERROR_MSG

            if('account_ref_id' not in account):
                account_ref_id = 999
            else:
                account_ref_id = account["account_ref_id"]

            queryMySQL = ("SELECT * FROM account "
                          "WHERE cod_account='"+str(account["cod_account"])+"' AND company_id="+str(company_id)+";")

            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                queryMySQL = ("UPDATE account SET "
                               "classification='"+str(account["classification"])+"', description ='"+str(account["description"])+"', "
                               "nature='"+str(account["nature"])+"', group_account='"+str(account["group_account"])+"' "
                               "WHERE cod_account='"+str(account["cod_account"])+"' AND company_id="+str(company_id)+";")

                code = codeReturn.INSERT_ALREADY_EXISTS_CODE
                msg = codeReturn.INSERT_ALREADY_EXISTS_MSG
            else:
                queryMySQL = ("INSERT INTO account "
                               "(cod_account, classification, description, nature, group_account, account_ref_id, company_id) "
                               "VALUES ('"+str(account["cod_account"])+"', '"+str(account["classification"])+"','"+str(account["description"])+"', "
                               "'"+str(account["nature"])+"', '"+str(account["group_account"])+"', "+str(account_ref_id)+", "+str(company_id)+");")
                
                code = codeReturn.SUCCESS_CODE
                msg = codeReturn.SUCCESS_MSG
                
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()

            return code, msg, []
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

    def get_acc_by_cod(self, cod_account, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            acc = {}

            queryMySQL = ("SELECT id, cod_account, classification, description, nature, group_account, account_ref_id "
                          "FROM account WHERE cod_account='"+str(cod_account)+"' AND company_id="+str(company_id)+";")

            cursorMySQL.execute(queryMySQL)

            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                acc = {
                    "id": rowMySQL[0],
                    "cod_account": rowMySQL[1],
                    "classification": rowMySQL[2],
                    "description": rowMySQL[3],
                    "nature": rowMySQL[4],
                    "group_account": rowMySQL[5],
                    "account_ref_id": rowMySQL[6]
                }
            else:
                return codeReturn.OBJECT_NOT_FOUND_CODE, codeReturn.OBJECT_NOT_FOUND_MSG, []

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(acc)

    def get_acc_ref_by_cod(self, cod_account_ref, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            acc_ref = {}

            queryMySQL = ("SELECT id, order_account, cod_account, classification, description, nature, level_account, father, group_account "
                          "FROM account_ref "
                          "WHERE cod_account = '"+str(cod_account_ref)+"';")

            cursorMySQL.execute(queryMySQL)

            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                acc_ref = {
                    "id": rowMySQL[0],
                    "order_account": rowMySQL[1],
                    "cod_account": rowMySQL[2],
                    "classification": rowMySQL[3],
                    "description": rowMySQL[4],
                    "nature": rowMySQL[5],
                    "level_account": rowMySQL[6],
                    "father": rowMySQL[7],
                    "group_account": rowMySQL[8]
                }
            else:
                return codeReturn.OBJECT_NOT_FOUND_CODE, codeReturn.OBJECT_NOT_FOUND_MSG, []

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, acc_ref
    
    def get_acc_ref_by_classification(self, classification_ref, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            acc_ref = {}

            queryMySQL = ("SELECT id, order_account, cod_account, classification, description, nature, level_account, father, group_account "
                          "FROM account_ref "
                          "WHERE classification = '"+str(classification_ref)+"';")

            cursorMySQL.execute(queryMySQL)

            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                acc_ref = {
                    "id": rowMySQL[0],
                    "order_account": rowMySQL[1],
                    "cod_account": rowMySQL[2],
                    "classification": rowMySQL[3],
                    "description": rowMySQL[4],
                    "nature": rowMySQL[5],
                    "level_account": rowMySQL[6],
                    "father": rowMySQL[7],
                    "group_account": rowMySQL[8]
                }
            else:
                return codeReturn.OBJECT_NOT_FOUND_CODE, codeReturn.OBJECT_NOT_FOUND_MSG, []

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, acc_ref

    def list_account(self, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            acc_list = []

            queryMySQL = ("SELECT id, cod_account, classification, description, nature, group_account, cod_account_ref, classification_ref, description_ref "
                          "FROM vw_account "
                          "WHERE company_id = "+str(company_id)+" ORDER BY cod_account;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                acc_list.append({
                    "cod_account": rowMySQL[1],
                    "classification": rowMySQL[2],
                    "description": rowMySQL[3],
                    "nature": rowMySQL[4],
                    "group_account": rowMySQL[5],
                    "cod_account_ref": rowMySQL[6],
                    "classification_ref": rowMySQL[7],
                    "description_ref": rowMySQL[8]
                })

            cursorMySQL.close()

            acc_list.sort(key=lambda x: x["classification"])
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(acc_list)

    def update_acc_relationship(self, cod_account, classification_ref, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            acc_ref = self.get_acc_ref_by_classification(classification_ref, user_id)

            queryMySQL = ("UPDATE account SET "
                          "account_ref_id="+str(acc_ref["id"])+" "
                          "WHERE cod_account='"+str(cod_account)+"' AND company_id="+str(company_id)+";")
        
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
   
    def clear_acc_relationship(self, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("UPDATE account SET "
                          "account_ref_id=999 "
                          "WHERE company_id="+str(company_id)+";")
        
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()
        
            cursorMySQL.close()

            log.warning(inspect.getframeinfo(inspect.currentframe()).function, 
                        "Limpando o relacionamento de todas as contas da empresa com ID: "+str(company_id), 
                        user_id)
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
    
    def delete_all_acc(self, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("DELETE FROM account "
                          "WHERE company_id="+str(company_id)+";")
        
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()

            log.warning(inspect.getframeinfo(inspect.currentframe()).function, 
                        "Deletando todas as contas da empresa com ID: "+str(company_id), 
                        user_id)
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
        
        