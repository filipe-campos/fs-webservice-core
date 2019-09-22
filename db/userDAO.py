# -*- coding: utf-8 -*-
import mysql.connector
import datetime
import json
import traceback
import inspect

from dateutil.relativedelta import *
from datetime import date
from util import Util, Log, Constants, CodeReturn
from controller import Controller
from flask import Flask
from flask_mysqldb import MySQL

log = Log('UserDAO')
constants = Constants()
util = Util()
codeReturn = CodeReturn()
controller = Controller()
mySQL = MySQL()

app = Flask(__name__)

class UserDAO:
    def __init__(self):
        mySQL.init_app(app)

    def insert_user(self, info):
        try:
            cursorMySQL = mySQL.connection.cursor()

            #Check if User exists
            queryMySQL = ("SELECT * FROM user WHERE user = '"+str(info['user'])+"';")
            
            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                return codeReturn.REGISTERED_USER_CODE, codeReturn.REGISTERED_USER_MSG, []

            #Check if Email exists
            queryMySQL = ("SELECT * FROM user WHERE email = '"+str(info['email'])+"';")
            
            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                return codeReturn.REGISTERED_EMAIL_CODE, codeReturn.REGISTERED_USER_MSG, []

            date_now = date.today().strftime("%Y-%m-%d")

            queryMySQL = ("INSERT INTO user "
                        "(name, user, pass, email, type_user, date_start, last_login, plan_id) "
                        "VALUES ('"+str(info['name'])+"', '"+str(info['user'])+"', "
                        "'"+str(info['pass'])+"', '"+str(info['email'])+"', "
                        "'0', '"+str(date_now)+"', "
                        "'"+str(date_now)+"', 1) ")

        
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      0)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
    
    def get_user_info(self, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            user_data = {}

            user_data['info'] = {
                "user": "",
                "name": "",
                "email": "",
                "type_user": "",
                "image": "",
                "date_start": "",
                "last_login": "",
                "plan_id": ""
            }

            # User Info
            queryMySQL = ("SELECT user, name, email, type_user, image, date_start, last_login, plan_id "
                          "FROM user "
                          "WHERE id="+str(user_id)+";")

            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                user_data['info']['user'] = rowMySQL[0]
                user_data['info']['name'] = rowMySQL[1]
                user_data['info']['email'] = rowMySQL[2]
                user_data['info']['type_user'] = rowMySQL[3]
                user_data['info']['image'] = rowMySQL[4]
                user_data['info']['date_start'] = rowMySQL[5].strftime("%d/%m/%Y")
                user_data['info']['last_login'] = rowMySQL[6].strftime("%d/%m/%Y")
                user_data['info']['plan_token'] = str(controller.encode_token(rowMySQL[7]).decode("utf-8")),
            else:
                cursorMySQL.close()

                return codeReturn.NOT_FOUND_USER_CODE, codeReturn.NOT_FOUND_USER_MSG, []
            
            cursorMySQL.close()

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(user_data, sort_keys=True)
    
    def update_user_info(self, info, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            #Check if User exists
            queryMySQL = ("SELECT * FROM user WHERE user = '"+str(info['user'])+"';")
            
            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                return codeReturn.REGISTERED_USER_CODE, codeReturn.REGISTERED_USER_MSG, []

            queryMySQL = ("UPDATE user SET "
                        "user='"+str(info['user'])+"', name='"+str(info['name'])+"', "
                        "email='"+str(info['email'])+"', type_user='"+str(info['type_user'])+"', "
                        "image='"+str(info['image'])+"',  "
                        "plan_id='"+str(controller.decode_token(info['plan_token']))+"' "
                        "WHERE id="+str(user_id)+";")

        
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit() 

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    def list_user_companies(self, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            company_list = []

            queryMySQL = ("SELECT name_company, cnpj, date_start, last_export, type_company, email, cnae, tax_regime, address, city, uf, country, cep, company_id "
                          "FROM vw_user_has_company "
                          "WHERE user_id="+str(user_id)+";")

            cursorMySQL.execute(queryMySQL)
            
            for rowMySQL in cursorMySQL:
                company_list.append({
                    'name_company': rowMySQL[0],
                    'cnpj': rowMySQL[1],
                    'date_start': rowMySQL[2].strftime("%d/%m/%Y"),
                    'last_export': rowMySQL[3].strftime("%d/%m/%Y"),
                    'type_company': str(controller.encode_token(rowMySQL[4]).decode("utf-8")),
                    'email': rowMySQL[5],
                    'cnae': rowMySQL[6],
                    'tax_regime': rowMySQL[7],
                    'address': rowMySQL[8],
                    'city': rowMySQL[9],
                    'uf': rowMySQL[10],
                    'country': rowMySQL[11],
                    'cep': rowMySQL[12],
                    'company_token': str(controller.encode_token(rowMySQL[13]).decode("utf-8"))
                })

            cursorMySQL.close()

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(company_list)

    def update_user_pass(self, old_password, new_password, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("SELECT * FROM user "
                          "WHERE id="+str(user_id)+" "
                          "AND pass='"+str(old_password)+"';")

            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                queryMySQL = ("UPDATE user SET "
                              "pass='"+str(new_password)+"' "
                              "WHERE id="+str(user_id)+";")
        
                cursorMySQL.execute(queryMySQL)
                mySQL.connection.commit()
            else:
                return codeReturn.WRONG_LOGIN_CODE, codeReturn.WRONG_LOGIN_MSG, []
            
            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []