# -*- coding: utf-8 -*-
import coloredlogs
import logging
import json
import datetime
import jwt
import decimal
import traceback
import inspect

from .constants import Constants
from .code import CodeReturn
from configuration import Configuration
from celery import Celery

constants = Constants()
configuration = Configuration()
codeReturn = CodeReturn()

class Util:

    def setup_logging(self):
        coloredlogs.install(level='INFO',
                            fmt='%(asctime)s %(name)-8s[%(process)d] %(levelname)8s %(message)s',
                            datefmt='%d/%m/%Y %H:%M:%S')

    def round(self, n):
        return int(decimal.Decimal(n).quantize(0, decimal.ROUND_HALF_UP))

    def list_to_json(self, l):
        return json.dumps(l, default=lambda o: o.__dict__)

    def make_json(self, cod_return, message, data):
        if isinstance(data, list) or isinstance(data, dict):
            json_data = {
                'cod_return' : cod_return,
                'message' : message, 
                'data' : data
            }
        else:
            json_data = {
                'cod_return' : cod_return,
                'message' : message, 
                'data' : json.loads(data)
            }

        return json.dumps(json_data)


    def make_celery(self, app):
        celery = Celery(
            app.import_name,
            backend=app.config['CELERY_RESULT_BACKEND'],
            broker=app.config['CELERY_BROKER_URL']
        )

        class ContextTask(celery.Task):
            def __call__(self, *args, **kwargs):
                with app.app_context():
                    return self.run(*args, **kwargs)

        celery.Task = ContextTask

        return celery
    
    def detect_delimiter(self, csv_path):
        with open(csv_path, 'r', encoding="utf8") as csvfile:
            header = csvfile.readline()
            if header.find(";")!=-1:
                return ";"
            if header.find(",")!=-1:
                return ","

    def parse_cod_account(self, cod_account):
        try:
            if isinstance(cod_account, float):
                return str('%.0f'%float(cod_account))
            
            if (cod_account == None):
                return cod_account
        except:
            return str(cod_account).replace('nan', '')

        return str(cod_account).replace('nan', '')

    def parse_description(self, description):
        description = str(description).replace('nan', '')
        description = str(description).replace('"', '')
        description = str(description).replace("'", '')

        return description

    def parse_money(self, money):
        try:
            if isinstance(money, float):
                return "{0:.2f}".format(money)

            money = str(money)
            money = str(money).replace("nan","")
            money = str(money).replace("R$","")

            if(len(money) > 2):

                if money[len(money)-3] == '.':
                    money = str(money).replace(",","")

                    return money

                if money[len(money)-3] == ',':
                    money = str(money).replace(".","")
                    money = str(money).replace(",",".")

                    return money

            elif(len(money) > 1):
                if money[len(money)-2] == '.':
                    money = str(money).replace(",","")

                    return money

                if money[len(money)-2] == ',':
                    money = str(money).replace(".","")
                    money = str(money).replace(",",".")

                    return money
                
            money = str(money).replace(",",".")
            money = str("{0:.2f}".format(float(money)))
        except:
            return money

        return money
    
    def parse_date(self, date, type=None):
        try:
            date = str(date)
            date = date.replace('nan', '')
        
            #dd/mm/yyyy
            if(date[2] == '/' and date[5] == '/' or 
               date[2] == '-' and date[5] == '-' or
               date[2] == '.' and date[5] == '.'):
            
                date = str(date[6:10]) + '-' +str(date[3:5]).rjust(2,'0')+ '-' + str(date[0:2])
            
            #mm/yyyy
            elif(date[2] == '/' or 
               date[2] == '-' or
               date[2] == '.'):
            
                if(type == constants.DATE_START_TYPE):
                    date = str(date[3:7]) + '-' +str(date[0:2]).rjust(2,'0')+ '-01'
                elif(type == constants.DATE_END_TYPE):
                    date = str(date[3:7]) + '-' +str(date[0:2]).rjust(2,'0')+ '-31'
                else:
                    date = str(date[3:7]) + '-' +str(date[0:2]).rjust(2,'0')+ '-00'
                
        except:
            return date

        return date

    def companies_token_to_id(self, companies_token):
        try:
            companies_id = []

            for c in companies_token:
                companies_id.append(controller.decode_token(c))
            
            return companies_id
        except Exception as e:
            print(e)
            return None

    def get_companies_id_query(self, companies_id):
        try:
            companies_id_query = "AND ("

            for i in range(len(companies_id)-1):
                companies_id_query = companies_id_query + "company_id="+str(companies_id[i])+" OR "
            
            companies_id_query = companies_id_query + "company_id="+str(companies_id[len(companies_id)-1])+") "
            
            return companies_id_query
        except Exception as e:
            print(e)
            return None
        
