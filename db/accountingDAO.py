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
from db import AnalyticDAO
from flask import Flask
from flask_mysqldb import MySQL

log = Log('AccountingDAO')
constants = Constants()
codeReturn = CodeReturn()
util = Util()
analyticDAO = AnalyticDAO()
mySQL = MySQL()

app = Flask(__name__)

class AccountingDAO:
    def __init__(self):
        mySQL.init_app(app)

    def get_resume(self, date, companies_id, user_id):
        try:
            #ATIVO/PASSIVO
            ativo = json.loads(str(analyticDAO.get_acc_ref_balance_by_classification('1', date, date, companies_id, user_id)).replace("'", '"'))[0]['value']

            #ROI
            roi = json.loads(str(analyticDAO.get_roi(date, date, companies_id, user_id)).replace("'", '"'))[0]['value']

            #Margem de Contribuição
            lucro_bruto = json.loads(str(analyticDAO.get_acc_ref_mov_by_classification('7', date, date, companies_id, user_id)).replace("'", '"'))[0]['value']
            receita_bruta = json.loads(str(analyticDAO.get_acc_ref_mov_by_classification('3', date, date, companies_id, user_id)).replace("'", '"'))[0]['value']

            mg_contribuicao = float("{0:.2f}".format((lucro_bruto / receita_bruta)*100))
    
            #Ebitida
            ebitida = 0

            resume_data = {
                "ativo_passivo" : ativo,
                "roi" : roi,
                "margem_contribuicao" : mg_contribuicao,
                "ebitida" : ebitida
            }

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            resume_data = {
                "ativo_passivo" : 0,
                "roi" : 0,
                "margem_contribuicao" : 0,
                "ebitida" : 0
            }

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, json.dumps(resume_data)

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(resume_data)