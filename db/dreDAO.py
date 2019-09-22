# -*- coding: utf-8 -*-
import mysql.connector
import logging
import json
import traceback
import inspect

from dateutil.relativedelta import *
from datetime import datetime
from util import Util, Log, Constants, CodeReturn
from flask import Flask
from flask_mysqldb import MySQL

log = Log('DreDAO') 
constants = Constants()
codeReturn = CodeReturn()
util = Util()
mySQL = MySQL()

app = Flask(__name__)

class DreDAO:
    def __init__(self):
        mySQL.init_app(app)

    #Date format: mm/yyyy
    def get_dre_comparative(self, date_start, date_end, companies_id, user_id):
        try:
            list_dre = []

            date_start = datetime.strptime(date_start, "%m/%Y")
            date_end = datetime.strptime(date_end, "%m/%Y")

            cursorMySQL = mySQL.connection.cursor()

            companies_id_query = util.get_companies_id_query(companies_id)

            #Get Date Start
            queryMySQL = ("SELECT SUM(value_mov), date_mov, cod_account_ref, classification_ref, description_ref, father_ref, order_account "
                          "FROM vw_movimentation "
                          "WHERE (LEFT(classification_ref, 1) >= 3 OR (SUBSTRING(classification_ref, 2, 1) != '.' AND LEFT(cod_account_ref, 2) >= 3)) "
                          "AND MONTH(date_mov)='"+str(date_start.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)='"+str(date_start.year)+"' "
                          +str(companies_id_query)+
                          "AND cod_account_ref != 999 AND cod_account_ref != 998 "
                          "GROUP BY cod_account_ref ORDER BY order_account;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                cod_account_ref = rowMySQL[2]
                classification_ref = rowMySQL[3]
                description_ref = rowMySQL[4]
                value_date_start = float(rowMySQL[0])
                av_date_start = 0
                value_date_end = 0
                av_date_end = 0
                var_percent = 0
                var_value = 0
                father = rowMySQL[5]
                order_account = rowMySQL[6]

                if(value_date_start != 0):
                    accountDre = {
                        "cod_account_ref": cod_account_ref,
                        "classification_ref": classification_ref,
                        "description_ref": description_ref,
                        "value_date_start": value_date_start,
                        "av_date_start": av_date_start,
                        "value_date_end": value_date_end,
                        "av_date_end": av_date_end,
                        "var_percent": var_percent,
                        "var_value": var_value,
                        "father": father,
                        "order_account": order_account
                    }

                    list_dre.append(accountDre)

                    self.sum_acc_on_father_acc(accountDre, list_dre, father, constants.DATE_START_TYPE, user_id)

            #Get Date End
            queryMySQL = ("SELECT SUM(value_mov), date_mov, cod_account_ref, classification_ref, description_ref, father_ref, order_account "
                          "FROM vw_movimentation "
                          "WHERE (LEFT(classification_ref, 1) >= 3 OR (SUBSTRING(classification_ref, 2, 1) != '.' AND LEFT(classification_ref, 2) >= 3)) "
                          "AND MONTH(date_mov)='"+str(date_end.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)='"+str(date_end.year)+"' "
                          +str(companies_id_query)+
                          "AND cod_account_ref != 999 AND cod_account_ref != 998 "
                          "GROUP BY cod_account_ref ORDER BY order_account;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                cod_account_ref = rowMySQL[2]
                classification_ref = rowMySQL[3]
                description_ref = rowMySQL[4]
                value_date_start = 0
                av_date_start = 0
                value_date_end = float(rowMySQL[0])
                av_date_end = 0
                var_percent = 0
                var_value = 0
                father = rowMySQL[5]
                order_account = rowMySQL[6]

                if(value_date_end != 0):
                    accountDre = {
                        "cod_account_ref": cod_account_ref,
                        "classification_ref": classification_ref,
                        "description_ref": description_ref,
                        "value_date_start": value_date_start,
                        "av_date_start": av_date_start,
                        "value_date_end": value_date_end,
                        "av_date_end": av_date_end,
                        "var_percent": var_percent,
                        "var_value": var_value,
                        "father": father,
                        "order_account": order_account
                    }

                    self.add_acc_on_list(accountDre, list_dre, user_id)
                    self.sum_acc_on_father_acc(accountDre, list_dre, father, constants.DATE_END_TYPE, user_id)

            if(self.calculate_results(list_dre, user_id) == codeReturn.UNKNOW_ERROR_CODE):
                return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

            if(self.calculate_variation(list_dre, user_id) == codeReturn.UNKNOW_ERROR_CODE):
                return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

            if(self.clear_list_with_zeros(list_dre, user_id) == codeReturn.UNKNOW_ERROR_CODE):
                return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

            #list_dre.sort(key=lambda x: (int(x.cod_account_ref.split('.')[0]), x.cod_account_ref.replace('.','').ljust(8, '0')))
            list_dre.sort(key=lambda x: x["order_account"])

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(list_dre)
    
    def get_dre_month(self, date_start, date_end, companies_id, user_id):
        try:
            list_dre = []
            list_key = []

            date_start = datetime.strptime(date_start, "%m/%Y")
            date_end = datetime.strptime(date_end, "%m/%Y")

            cursorMySQL = mySQL.connection.cursor()

            companies_id_query = util.get_companies_id_query(companies_id)

            while(date_start <= date_end):

                #Get Date Start
                queryMySQL = ("SELECT SUM(value_mov), date_mov, cod_account_ref, classification_ref, description_ref, father_ref, order_account "
                              "FROM vw_movimentation "
                              "WHERE (LEFT(classification_ref, 1) >= 3 OR (SUBSTRING(classification_ref, 2, 1) != '.' AND LEFT(cod_account_ref, 2) >= 3)) "
                              "AND MONTH(date_mov)='"+str(date_start.month).rjust(2, '0')+"' "
                              "AND YEAR(date_mov)='"+str(date_start.year)+"' "
                              +str(companies_id_query)+
                              "AND cod_account_ref != 999 AND cod_account_ref != 998 "
                              "GROUP BY cod_account_ref ORDER BY order_account;")

                cursorMySQL.execute(queryMySQL)

                for rowMySQL in cursorMySQL:
                    cod_account_ref = rowMySQL[2]
                    classification_ref = rowMySQL[3]
                    description_ref = rowMySQL[4]
                    value = float(rowMySQL[0])
                    father = rowMySQL[5]
                    order_account = rowMySQL[6]

                    key = str(date_start.month).rjust(2, '0')+'/'+str(date_start.year)
                    
                    accountDre = {
                        "cod_account_ref": cod_account_ref,
                        "classification_ref": classification_ref,
                        "description_ref": description_ref,
                        "accumulated": 0,
                        key: value,
                        "father": father,
                        "order_account": order_account
                    }

                    self.add_acc_on_list(accountDre, list_dre, user_id, key)
                    self.sum_acc_on_father_acc_month(accountDre, list_dre, father, key, user_id)
                
                list_key.append(key)
                date_start = date_start + relativedelta(months=+1)

            for key in list_key:
                for acc in list_dre:
                    if not key in acc:
                        acc[key] = 0

            if(self.calculate_results_month(list_dre, list_key, user_id) == codeReturn.UNKNOW_ERROR_CODE):
                return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

            list_dre.sort(key=lambda x: x["order_account"])

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(list_dre, sort_keys=True)

    def add_acc_on_list(self, account, list_acc, user_id, key="value_date_end"):
        try:
            found_acc = False

            for i in range(len(list_acc)):
                if(list_acc[i]["cod_account_ref"] == account["cod_account_ref"]):
                    found_acc = True

                    list_acc[i][key] = account[key]

                    if('accumulated' in list_acc[i]):
                        list_acc[i]['accumulated'] = list_acc[i]['accumulated'] + account[key]

                    break

            if not(found_acc):
                list_acc.append(account)

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

    def sum_acc_on_father_acc(self, account, list_acc, father, date, user_id):
        try:
            found_acc = False
            cursorMySQL = mySQL.connection.cursor()

            for i in range(len(list_acc)):
                if(list_acc[i]["cod_account_ref"] == father):
                    found_acc = True

                    if(date == constants.DATE_START_TYPE):
                        list_acc[i]["value_date_start"] = list_acc[i]["value_date_start"] + account["value_date_start"]
                    else:
                        list_acc[i]["value_date_end"] = list_acc[i]["value_date_end"] + account["value_date_end"]

                    if(list_acc[i]["father"] != constants.ACC_REF_HAS_NO_FATHER):
                        self.sum_acc_on_father_acc(account, list_acc, list_acc[i]["father"], date, user_id)

                    break

            if not(found_acc):
                queryMySQL = ("SELECT cod_account, classification, description, father, order_account "
                              "FROM account_ref "
                              "WHERE cod_account = '"+account["father"]+"';")

                cursorMySQL.execute(queryMySQL)
                rowMySQL = cursorMySQL.fetchone()

                cod_account_ref = rowMySQL[0]
                classification_ref = rowMySQL[1]
                description_ref = rowMySQL[2]
                value_date_start = 0
                av_date_start = 0
                value_date_end = 0
                av_date_end = 0
                var_percent = 0
                var_value = 0
                father = rowMySQL[3]
                order_account = rowMySQL[4]

                if(date == constants.DATE_START_TYPE):
                    value_date_start = account["value_date_start"]
                else:
                    value_date_end = account["value_date_end"]
                
                accountDre = {
                    "cod_account_ref": cod_account_ref,
                    "classification_ref": classification_ref,
                    "description_ref": description_ref,
                    "value_date_start": value_date_start,
                    "av_date_start": av_date_start,
                    "value_date_end": value_date_end,
                    "av_date_end": av_date_end,
                    "var_percent": var_percent,
                    "var_value": var_value,
                    "father": father,
                    "order_account": order_account
                }    

                list_acc.append(accountDre)

                if(father != constants.ACC_REF_HAS_NO_FATHER):
                    self.sum_acc_on_father_acc(accountDre, list_acc, father, date, user_id)

            cursorMySQL.close()
        except Exception as e:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(e), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

    def sum_acc_on_father_acc_month(self, account, list_acc, father, date, user_id):
        try:
            found_acc = False
            cursorMySQL = mySQL.connection.cursor()

            for i in range(len(list_acc)):
                if(list_acc[i]["cod_account_ref"] == father):
                    found_acc = True

                    if date in list_acc[i]:
                        list_acc[i][date] = list_acc[i][date] + account[date]
                    else:
                        list_acc[i][date] = 0 + account[date]

                    list_acc[i]['accumulated'] = list_acc[i]['accumulated'] + account[date]

                    if(list_acc[i]["father"] != constants.ACC_REF_HAS_NO_FATHER):
                        self.sum_acc_on_father_acc(account, list_acc, list_acc[i]["father"], date, user_id)

                    break

            if not(found_acc):
                queryMySQL = ("SELECT cod_account, classification, description, father, order_account "
                              "FROM account_ref "
                              "WHERE cod_account = '"+account["father"]+"';")

                cursorMySQL.execute(queryMySQL)
                rowMySQL = cursorMySQL.fetchone()

                cod_account_ref = rowMySQL[0]
                classification_ref = rowMySQL[1]
                description_ref = rowMySQL[2]
                value = account[date]
                father = rowMySQL[3]
                order_account = rowMySQL[4]
                

                accountDre = {
                    "cod_account_ref": cod_account_ref,
                    "classification_ref": classification_ref,
                    "description_ref": description_ref,
                    date: value,
                    'accumulated': value,
                    "father": father,
                    "order_account": order_account
                }

                list_acc.append(accountDre)

                if(father != constants.ACC_REF_HAS_NO_FATHER):
                    self.sum_acc_on_father_acc(accountDre, list_acc, father, date, user_id)

            cursorMySQL.close()
        except Exception as e:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(e), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

    def calculate_variation(self, list_acc, user_id):
        try:
            #Get Values to calculate AV 
            acc_rec_liq = self.get_account_on_list('208', list_acc, user_id)

            if(acc_rec_liq != None):
                rec_liq_value_start = acc_rec_liq["value_date_start"]
                rec_liq_value_end = acc_rec_liq["value_date_end"]
            else:
                rec_liq_value_start = 0
                rec_liq_value_end = 0
            
            for i in range(len(list_acc)):
                #Format field 'value_date_start' and 'value_date_end'
                value_date_start = float(list_acc[i]["value_date_start"])
                value_date_end = float(list_acc[i]["value_date_end"])

                #Calculate AV
                if(list_acc[i]["cod_account_ref"] == '208'):
                    list_acc[i]["av_date_start"] = float(100)
                    list_acc[i]["av_date_end"] = float(100)

                elif(int(list_acc[i]["classification_ref"].split('.')[0]) >= 5):
                    if(rec_liq_value_start != 0):
                        list_acc[i]["av_date_start"] =  float("{0:.2f}".format((value_date_start / rec_liq_value_start)*100))
                    else:
                        list_acc[i]["av_date_start"] =  float("{0:.2f}".format((value_date_start / 1)*100))
                    
                    if(rec_liq_value_end != 0):
                        list_acc[i]["av_date_end"] =  float("{0:.2f}".format((value_date_end / rec_liq_value_end)*100))
                    else:
                        list_acc[i]["av_date_end"] =  float("{0:.2f}".format((value_date_end / 1)*100))
                else:
                    list_acc[i]["av_date_start"] = ''
                    list_acc[i]["av_date_end"] = ''

                #Calculate Value Variation
                list_acc[i]["var_value"] = float("{0:.2f}".format(value_date_end - value_date_start))

                #Calculate Percent Variation
                if(value_date_start != 0):
                    list_acc[i]["var_percent"] =  float("{0:.2f}".format(((value_date_end/value_date_start)-1)*100))
                else:
                    list_acc[i]["var_percent"] =  float("{0:.2f}".format(((value_date_end/1)-1)*100))

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    def calculate_results(self, list_acc, user_id):
        try:
            account3 = self.get_account_on_list('199', list_acc, user_id)
            account4 = self.get_account_on_list('204', list_acc, user_id)
            account5 = None
            account6 = None
            account7 = None
            account8 = None
            account9 = None
            account10 = None
            account11 = None
            account12 = None
            account13 = None
            account14 = None
            account15 = None

            #Account (=) RECEITA LIQUIDA
            if(account3 != None and account4 != None):
                value_date_start = account3["value_date_start"] + account4["value_date_start"]
                value_date_end = account3["value_date_end"] + account4["value_date_end"]

                account5 = {
                    "cod_account_ref": '208',
                    "classification_ref": '5',
                    "description_ref": '(=) RECEITA LIQUIDA',
                    "value_date_start": value_date_start,
                    "av_date_start": 0,
                    "value_date_end": value_date_end,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": 0,
                    "order_account": 109
                }    

                list_acc.append(account5)

            #Account (-) CUSTO DAS VENDAS E SERVIÇOS
            account6 = self.get_account_on_list('209', list_acc, user_id)

            if(account6 == None):
                account6 = {
                    "cod_account_ref": '209',
                    "classification_ref": '6',
                    "description_ref": '(-) CUSTO DAS VENDAS E SERVIÇOS',
                    "value_date_start": 0,
                    "av_date_start": 0,
                    "value_date_end": 0,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": 0,
                    "order_account": 110
                }    

            if(account5 != None):
                value_date_start = account5["value_date_start"] + account6["value_date_start"]
                value_date_end = account5["value_date_end"] + account6["value_date_end"]

                account7 = {
                    "cod_account_ref": '213',
                    "classification_ref": '7',
                    "description_ref": 'LUCRO BRUTO',
                    "value_date_start": value_date_start,
                    "av_date_start": 0,
                    "value_date_end": value_date_end,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": 0,
                    "order_account": 114
                } 

                list_acc.append(account7)

            #Account (=) RESULTADO OPERACIONAL LIQUIDO
            account8 = self.get_account_on_list('214', list_acc, user_id)

            if(account7 != None and account8 != None):
                value_date_start = account7["value_date_start"] + account8["value_date_start"]
                value_date_end = account7["value_date_end"] + account8["value_date_end"]

                account9 = {
                    "cod_account_ref": '230',
                    "classification_ref": '9',
                    "description_ref": '(=) RESULTADO OPERACIONAL LIQUIDO',
                    "value_date_start": value_date_start,
                    "av_date_start": 0,
                    "value_date_end": value_date_end,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": 0,
                    "order_account": 131
                } 

                list_acc.append(account9)

            #Account (+/-) RESULTADO NÃO OPERACIONAL
            account10 = self.get_account_on_list('231', list_acc, user_id)

            if(account10 == None):
                account10 = {
                    "cod_account_ref": '231',
                    "classification_ref": '10',
                    "description_ref": '(+/-) RESULTADO NÃO OPERACIONAL',
                    "value_date_start": 0,
                    "av_date_start": 0,
                    "value_date_end": 0,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": 0,
                    "order_account": 132
                }

            #Account (+/-) RESULTADO ANTES DO IMPOSTO DE RENDA
            if(account9 != None and account10 != None):
                value_date_start = account9["value_date_start"] + account10["value_date_start"]
                value_date_end = account9["value_date_end"] + account10["value_date_end"]

                account11 = {
                    "cod_account_ref": '235',
                    "classification_ref": '11',
                    "description_ref": '(+/-) RESULTADO ANTES DO IMPOSTO DE RENDA',
                    "value_date_start": value_date_start,
                    "av_date_start": 0,
                    "value_date_end": value_date_end,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": 0,
                    "order_account": 136
                }

                list_acc.append(account11)

            #Account (+/-) PROVISÃO PARA IRPJ E CSLL
            account12 = self.get_account_on_list('236', list_acc, user_id)

            if(account12 == None):
                account12 = {
                    "cod_account_ref": '236',
                    "classification_ref": '12',
                    "description_ref": '(+/-) PROVISÃO PARA IRPJ E CSLL',
                    "value_date_start": 0,
                    "av_date_start": 0,
                    "value_date_end": 0,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": 0,
                    "order_account": 137
                }

            #Account (=) RESULTADO ANTES DAS PARTICIPAÇÕES
            if(account11 != None and account12 != None):
                value_date_start = account11["value_date_start"] + account12["value_date_start"]
                value_date_end = account11["value_date_end"] + account12["value_date_end"]

                account13 = {
                    "cod_account_ref": '236',
                    "classification_ref": '13',
                    "description_ref": '(=) RESULTADO ANTES DAS PARTICIPAÇÕES',
                    "value_date_start": value_date_start,
                    "av_date_start": 0,
                    "value_date_end": value_date_end,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": 0,
                    "order_account": 142
                }

                list_acc.append(account13)

            #Account (-) PARTICIPAÇÕES
            account14 = self.get_account_on_list('242', list_acc, user_id)

            if(account14 == None):
                account14 = {
                    "cod_account_ref": '242',
                    "classification_ref": '14',
                    "description_ref": '(-) PARTICIPAÇÕES',
                    "value_date_start": 0,
                    "av_date_start": 0,
                    "value_date_end": 0,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": 0,
                    "order_account": 143
                }

            #Account (=) RESULTADO LÍQUIDO
            if(account13 != None and account14 != None):
                value_date_start = account13["value_date_start"] - account14["value_date_start"]
                value_date_end = account13["value_date_end"] - account14["value_date_end"]

                account15 = {
                    "cod_account_ref": '247',
                    "classification_ref": '15',
                    "description_ref": '(=) RESULTADO LÍQUIDO',
                    "value_date_start": value_date_start,
                    "av_date_start": 0,
                    "value_date_end": value_date_end,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": 0,
                    "order_account": 148
                }

                list_acc.append(account15)
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    def calculate_results_month(self, list_acc, list_key, user_id):
        try:
            account3 = self.get_account_on_list('199', list_acc, user_id)
            account4 = self.get_account_on_list('204', list_acc, user_id)
            account5 = None
            account6 = None
            account7 = None
            account8 = None
            account9 = None
            account10 = None
            account11 = None
            account12 = None
            account13 = None
            account14 = None
            account15 = None

            #Account (=) RECEITA LIQUIDA
            if(account3 == None ):
                account3 = {}

                for key in list_key:
                    account3[key] = 0

            if(account4 == None ):
                account4 = {}

                for key in list_key:
                    account4[key] = 0

            account5 = {
                "cod_account_ref": '208',
                "classification_ref": '5',
                "description_ref": '(=) RECEITA LIQUIDA',
                "accumulated": 0,
                "father": 0,
                "order_account": 109
            }    

            for key in list_key:
                account5[key] = account3[key] + account4[key]
                account5["accumulated"] = account5["accumulated"] + account5[key]

            list_acc.append(account5)

            #Account (-) CUSTO DAS VENDAS E SERVIÇOS
            account6 = self.get_account_on_list('209', list_acc, user_id)

            if(account6 == None):
                account6 = {
                    "cod_account_ref": '209',
                    "classification_ref": '6',
                    "description_ref": '(-) CUSTO DAS VENDAS E SERVIÇOS',
                    "accumulated": 0,
                    "father": 0,
                    "order_account": 110
                }    

                for key in list_key:
                    account6[key] = 0

            
            account7 = {
                "cod_account_ref": '213',
                "classification_ref": '7',
                "description_ref": 'LUCRO BRUTO',
                "accumulated": 0,
                "father": 0,
                "order_account": 114
            } 

            for key in list_key:
                account7[key] = account5[key] + account6[key]
                account7["accumulated"] = account7["accumulated"] + account7[key]

            list_acc.append(account7)

            #Account (=) RESULTADO OPERACIONAL LIQUIDO
            account8 = self.get_account_on_list('214', list_acc, user_id)


            account9 = {
                "cod_account_ref": '230',
                "classification_ref": '9',
                "description_ref": '(=) RESULTADO OPERACIONAL LIQUIDO',
                "accumulated": 0,
                "father": 0,
                "order_account": 131
            } 

            for key in list_key:
                account9[key] = account7[key] + account8[key]
                account9["accumulated"] = account9["accumulated"] + account9[key]

            list_acc.append(account9)

            #Account (+/-) RESULTADO NÃO OPERACIONAL
            account10 = self.get_account_on_list('231', list_acc, user_id)

            if(account10 == None):
                account10 = {
                    "cod_account_ref": '231',
                    "classification_ref": '10',
                    "description_ref": '(+/-) RESULTADO NÃO OPERACIONAL',
                    "accumulated": 0,
                    "father": 0,
                    "order_account": 132
                }

                for key in list_key:
                    account10[key] = 0

            #Account (+/-) RESULTADO ANTES DO IMPOSTO DE RENDA
            account11 = {
                "cod_account_ref": '235',
                "classification_ref": '11',
                "description_ref": '(+/-) RESULTADO ANTES DO IMPOSTO DE RENDA',
                "accumulated": 0,
                "father": 0,
                "order_account": 136
            }

            for key in list_key:
                account11[key] = account9[key] + account10[key]
                account11["accumulated"] = account11["accumulated"] + account11[key]

            list_acc.append(account11)

            #Account (+/-) PROVISÃO PARA IRPJ E CSLL
            account12 = self.get_account_on_list('236', list_acc, user_id)

            if(account12 == None):
                account12 = {
                    "cod_account_ref": '236',
                    "classification_ref": '12',
                    "description_ref": '(+/-) PROVISÃO PARA IRPJ E CSLL',
                    "accumulted": 0,
                    "father": 0,
                    "order_account": 137
                }

                for key in list_key:
                    account12[key] = 0

            #Account (=) RESULTADO ANTES DAS PARTICIPAÇÕES
            account13 = {
                "cod_account_ref": '236',
                "classification_ref": '13',
                "description_ref": '(=) RESULTADO ANTES DAS PARTICIPAÇÕES',
                "accumulated": 0,
                "father": 0,
                "order_account": 142
            }

            for key in list_key:
                account13[key] = account11[key] + account12[key]
                account13["accumulated"] = account13["accumulated"] + account13[key]

            list_acc.append(account13)

            #Account (-) PARTICIPAÇÕES
            account14 = self.get_account_on_list('242', list_acc, user_id)

            if(account14 == None):
                account14 = {
                    "cod_account_ref": '242',
                    "classification_ref": '14',
                    "description_ref": '(-) PARTICIPAÇÕES',
                    "accumulated": 0,
                    "father": 0,
                    "order_account": 143
                }

                for key in list_key:
                    account14[key] = 0

            #Account (=) RESULTADO LÍQUIDO
            account15 = {
                "cod_account_ref": '247',
                "classification_ref": '15',
                "description_ref": '(=) RESULTADO LÍQUIDO',
                "accumulated": 0,
                "father": 0,
                "order_account": 148
            }

            for key in list_key:
                account15[key] = account13[key] - account14[key]
                account15["accumulated"] = account15["accumulated"] + account15[key]

            list_acc.append(account15)
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    def get_account_on_list(self, cod_account_ref, list_acc, user_id):
        try:
            for acc in list_acc:
                if(str(acc["cod_account_ref"]) == str(cod_account_ref)):
                    return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, acc
            
            return codeReturn.OBJECT_NOT_FOUND_CODE, codeReturn.OBJECT_NOT_FOUND_MSG, None

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

    def clear_list_with_zeros(self, list_acc, user_id):
        try:   
            for i in range(len(list_acc)-1, -1, -1):
                #Clear values with 0
                if(list_acc[i]["value_date_start"] == 0 and list_acc[i]["value_date_end"] == 0): 
                    list_acc.remove(list_acc[i])
                else:
                    list_acc[i]["value_date_start"] = float("{0:.2f}".format(list_acc[i]["value_date_start"]))
                    list_acc[i]["value_date_end"] = float("{0:.2f}".format(list_acc[i]["value_date_end"]))

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    #Date format: mm/yyyy
    def get_dre_period(self, date_start1, date_end1, date_start2, date_end2, companies_id, user_id):
        try:
            list_dre = []
            cursorMySQL = mySQL.connection.cursor()

            #Date Configuration
            date_start1 = datetime.strptime(date_start1, "%m/%Y")
            date_end1 = datetime.strptime(date_end1, "%m/%Y")

            date_start2 = datetime.strptime(date_start2, "%m/%Y")
            date_end2 = datetime.strptime(date_end2, "%m/%Y")

            companies_id_query = util.get_companies_id_query(companies_id)

            #Get first period
            queryMySQL = ("SELECT SUM(value_mov), date_mov, cod_account_ref, classification_ref, description_ref, father_ref, order_account "
                          "FROM vw_movimentation "
                          "WHERE (LEFT(classification_ref, 1) >= 3 OR (SUBSTRING(classification_ref, 2, 1) != '.' AND LEFT(classification_ref, 2) >= 3)) "
                          "AND MONTH(date_mov)>='"+str(date_start1.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)>='"+str(date_start1.year)+"' "
                          "AND MONTH(date_mov)<='"+str(date_end1.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)<='"+str(date_end1.year)+"' "
                          +str(companies_id_query)+
                          "AND cod_account_ref != 999 AND cod_account_ref != 998 "
                          "GROUP BY cod_account_ref ORDER BY order_account;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                cod_account_ref = rowMySQL[2]
                classification_ref = rowMySQL[3]
                description_ref = rowMySQL[4]
                value_date_start = float(rowMySQL[0])
                av_date_start = 0
                value_date_end = 0
                av_date_end = 0
                var_percent = 0
                var_value = 0
                father = rowMySQL[5]
                order_account = rowMySQL[6]

                if(value_date_start != 0):
                    accountDre = {
                        "cod_account_ref": cod_account_ref,
                        "classification_ref": classification_ref,
                        "description_ref": description_ref,
                        "value_date_start": value_date_start,
                        "av_date_start": av_date_start,
                        "value_date_end": value_date_end,
                        "av_date_end": av_date_end,
                        "var_percent": var_percent,
                        "var_value": var_value,
                        "father": father,
                        "order_account": order_account
                    }

                    list_dre.append(accountDre)

                    self.sum_acc_on_father_acc(accountDre, list_dre, father, constants.DATE_START_TYPE, user_id)

            #Get second period
            queryMySQL = ("SELECT SUM(value_mov), date_mov, cod_account_ref, classification_ref, description_ref, father_ref, order_account "
                          "FROM vw_movimentation "
                          "WHERE (LEFT(classification_ref, 1) >= 3 OR (SUBSTRING(classification_ref, 2, 1) != '.' AND LEFT(classification_ref, 2) >= 3)) "
                          "AND MONTH(date_mov)>='"+str(date_start2.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)>='"+str(date_start2.year)+"' "
                          "AND MONTH(date_mov)<='"+str(date_end2.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)<='"+str(date_end2.year)+"' "
                          +str(companies_id_query)+
                          "AND cod_account_ref != 999 AND cod_account_ref != 998 "
                          "GROUP BY cod_account_ref ORDER BY order_account;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                cod_account_ref = rowMySQL[2]
                classification_ref = rowMySQL[3]
                description_ref = rowMySQL[4]
                value_date_start = 0
                av_date_start = 0
                value_date_end = float(rowMySQL[0])
                av_date_end = 0
                var_percent = 0
                var_value = 0
                father = rowMySQL[5]
                order_account = rowMySQL[6]

                if(value_date_end != 0):
                    accountDre = {
                        "cod_account_ref": cod_account_ref,
                        "classification_ref": classification_ref,
                        "description_ref": description_ref,
                        "value_date_start": value_date_start,
                        "av_date_start": av_date_start,
                        "value_date_end": value_date_end,
                        "av_date_end": av_date_end,
                        "var_percent": var_percent,
                        "var_value": var_value,
                        "father": father,
                        "order_account": order_account
                    }

                    self.add_acc_on_list(accountDre, list_dre, user_id)
                    self.sum_acc_on_father_acc(accountDre, list_dre, father, constants.DATE_END_TYPE, user_id)

            if(self.calculate_results(list_dre, user_id) == codeReturn.UNKNOW_ERROR_CODE):
                return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

            if(self.calculate_variation(list_dre, user_id) == codeReturn.UNKNOW_ERROR_CODE):
                return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

            if(self.clear_list_with_zeros(list_dre, user_id) == codeReturn.UNKNOW_ERROR_CODE):
                return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

            list_dre.sort(key=lambda x: x["order_account"])

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(list_dre)

    #Date format: mm/yyyy
    def list_acc_mov_from_acc_ref(self, date_start, date_end, cod_account_ref, companies_id, user_id):
        try:
            date_start = datetime.strptime(date_start, "%m/%Y")
            date_end = datetime.strptime(date_end, "%m/%Y")

            acc_list = []
            cursorMySQL = mySQL.connection.cursor()

            companies_id_query = util.get_companies_id_query(companies_id)

            #GET DATE START
            queryMySQL = ("SELECT SUM(value_mov), cod_account, classification, description "
                          "FROM vw_movimentation "
                          "WHERE MONTH(date_mov)='"+str(date_start.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)='"+str(date_start.year)+"' "
                          "AND cod_account_ref='"+str(cod_account_ref)+"' "
                          +str(companies_id_query)+
                          " GROUP BY cod_account;")


            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                acc_list.append({
                    "cod_account": rowMySQL[1],
                    "classification": rowMySQL[2],
                    "description": rowMySQL[3],
                    "value_date_start": float(rowMySQL[0]),
                    "value_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0
                })
 
            #GET DATE END
            queryMySQL = ("SELECT SUM(value_mov), cod_account, classification, description "
                          "FROM vw_movimentation "
                          "WHERE MONTH(date_mov)='"+str(date_end.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)='"+str(date_end.year)+"' "
                          "AND cod_account_ref='"+str(cod_account_ref)+"' "
                          +str(companies_id_query)+
                          " GROUP BY cod_account;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:

                acc = {
                    "cod_account": rowMySQL[1],
                    "classification": rowMySQL[2],
                    "description": rowMySQL[3],
                    "value_date_start": 0,
                    "value_date_end": float(rowMySQL[0]),
                    "var_percent": 0,
                    "var_value": 0
                }

                self.add_acc_on_acc_list(acc, acc_list, user_id)
        
            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(acc_list)

    def list_acc_mov_from_acc_ref_period(self, date_start1, date_end1, date_start2, date_end2, cod_account_ref, companies_id, user_id):
        try:
            #Date Configuration
            date_start1 = datetime.strptime(date_start1, "%m/%Y")
            date_end1 = datetime.strptime(date_end1, "%m/%Y")

            date_start2 = datetime.strptime(date_start2, "%m/%Y")
            date_end2 = datetime.strptime(date_end2, "%m/%Y")

            acc_list = []
            cursorMySQL = mySQL.connection.cursor()

            companies_id_query = util.get_companies_id_query(companies_id)

            #GET DATE START
            queryMySQL = ("SELECT SUM(value_mov), cod_account, classification, description "
                          "FROM vw_movimentation "
                          "WHERE MONTH(date_mov)>='"+str(date_start1.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)>='"+str(date_start1.year)+"' "
                          "AND MONTH(date_mov)<='"+str(date_end1.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)<='"+str(date_end1.year)+"' "
                          "AND cod_account_ref='"+str(cod_account_ref)+"' "
                          +str(companies_id_query)+
                          " GROUP BY cod_account;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                acc_list.append({
                    "cod_account": rowMySQL[1],
                    "classification": rowMySQL[2],
                    "description": rowMySQL[3],
                    "value_date_start": float(rowMySQL[0]),
                    "value_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0
                })

            #GET DATE END
            queryMySQL = ("SELECT SUM(value_mov), cod_account, classification, description "
                          "FROM vw_movimentation "
                          "WHERE MONTH(date_mov)>='"+str(date_start2.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)>='"+str(date_start2.year)+"' "
                          "AND MONTH(date_mov)<='"+str(date_end2.month).rjust(2, '0')+"' "
                          "AND YEAR(date_mov)<='"+str(date_end2.year)+"' "
                          "AND cod_account_ref='"+str(cod_account_ref)+"' "
                          +str(companies_id_query)+
                          " GROUP BY cod_account;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                acc = {
                    "cod_account": rowMySQL[1],
                    "classification": rowMySQL[2],
                    "description": rowMySQL[3],
                    "value_date_start": 0,
                    "value_date_end": float(rowMySQL[0]),
                    "var_percent": 0,
                    "var_value": 0
                }

                self.add_acc_on_acc_list(acc, acc_list, user_id)
        
            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(acc_list)

    #Check if ACC exists on List
    def add_acc_on_acc_list(self, account, list_acc, user_id):
        try:
            found_acc = False

            for acc in list_acc:

                if(acc["cod_account"] == account["cod_account"]):
                    found_acc = True
                    acc["value_date_end"] = account["value_date_end"]

                    #Calculate value variation
                    acc["var_value"] = float("{0:.2f}".format(acc["value_date_end"] - acc["value_date_start"]))

                    #Calculate percent variation
                    if(acc["value_date_start"] != 0):
                        acc["var_percent"] =  float("{0:.2f}".format(((acc["value_date_end"]/acc["value_date_start"])-1)*100))
                    else:
                        acc["var_percent"] =  float("{0:.2f}".format(((acc["value_date_end"]/1)-1)*100))

                    break

            if not (found_acc):
                #Calculate value variation
                account["var_value"] = float("{0:.2f}".format(account["value_date_end"] - account["value_date_start"]))

                #Calculate percent variation
                account["var_percent"] =  float("{0:.2f}".format(((account["value_date_end"]/1)-1)*100))

                list_acc.append(account)

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []