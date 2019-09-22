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

log = Log('CompanyDAO')
constants = Constants()
controller = Controller()
codeReturn = CodeReturn()
util = Util()
mySQL = MySQL()

app = Flask(__name__)

class CompanyDAO:
    def __init__(self):
        mySQL.init_app(app)

    def get_company_info(self, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            company_data = {}

            company_data['qtt_launch'] = 0
            company_data['qtt_coa'] = 0
            company_data['info'] = {
                "name": "",
                "cnpj": "",
                "date_start": "",
                "last_export": "",
                "type_company": "",
                "email": "",
                "cnae": "",
                "tax_regime": "",
                "address": "",
                "city": "",
                "uf": "",
                "country": "",
                "cep": ""
            }

            # Qtt of Launchs
            queryMySQL = ("SELECT COUNT(*) FROM vw_launch WHERE company_id="+str(company_id)+";")

            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            company_data['qtt_launch'] = int(rowMySQL[0])

            # Qtt of Accounts
            queryMySQL = ("SELECT COUNT(*) FROM vw_account WHERE company_id="+str(company_id)+";")

            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            company_data['qtt_coa'] = int(rowMySQL[0])

            # Company Info
            queryMySQL = ("SELECT name, cnpj, date_start, last_export, type_company, email, cnae, tax_regime, address, city, uf, country, cep FROM company "
                          "WHERE id="+str(company_id)+";")

            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            company_data['info']['name'] = rowMySQL[0]
            company_data['info']['cnpj'] = rowMySQL[1]
            company_data['info']['date_start'] = rowMySQL[2].strftime("%d/%m/%Y")
            company_data['info']['last_export'] = rowMySQL[3].strftime("%d/%m/%Y")
            company_data['info']['type_company'] = str(controller.encode_token(rowMySQL[4]).decode("utf-8"))
            company_data['info']['email'] = rowMySQL[5]
            company_data['info']['cnae'] = rowMySQL[6]
            company_data['info']['tax_regime'] = rowMySQL[7]
            company_data['info']['address'] = rowMySQL[8]
            company_data['info']['city'] = rowMySQL[9]
            company_data['info']['uf'] = rowMySQL[10]
            company_data['info']['country'] = rowMySQL[11]
            company_data['info']['cep'] = rowMySQL[12]
            
            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(company_data, sort_keys=True)

    def update_company_info(self, info, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("UPDATE company SET "
                        "name='"+str(info['name'])+"', cnpj='"+str(info['cnpj'])+"', "
                        "email='"+str(info['email'])+"', cnae='"+str(info['cnae'])+"', tax_regime='"+str(info['tax_regime'])+"', "
                        "address='"+str(info['address'])+"', city='"+str(info['city'])+"', "
                        "uf='"+str(info['uf'])+"', country='"+str(info['country'])+"', "
                        "cep='"+str(info['cep'])+"' "
                        "WHERE id="+str(company_id)+";")

            
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    def insert_company(self, info, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            #Insert Company
            date_now = date.today().strftime("%Y-%m-%d")

            queryMySQL = ("INSERT INTO company "
                        "(name, cnpj, date_start, last_export, type_company, email, cnae, tax_regime, address, city, uf, country, cep) "
                        "VALUES ('"+str(info['name'])+"', '"+str(info['cnpj'])+"', '"+str(date_now)+"', "
                        "'"+str(date_now)+"', '1', "
                        "'"+str(info['email'])+"', '"+str(info['cnae'])+"', '"+str(info['tax_regime'])+"', "
                        "'"+str(info['address'])+"', '"+str(info['city'])+"', '"+str(info['uf'])+"', "
                        "'"+str(info['country'])+"', '"+str(info['cep'])+"') ")

            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            company_id = cursorMySQL.lastrowid

            #Insert user_has_company
            queryMySQL = ("INSERT INTO user_has_company "
                        "(user_id, company_id) "
                        "VALUES ("+str(user_id)+", "+str(company_id)+") ")

            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
