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
from db import AccountDAO, DreDAO
from flask import Flask
from flask_mysqldb import MySQL

log = Log('BalanceDAO')
constants = Constants()
codeReturn = CodeReturn()
util = Util()
mySQL = MySQL()
accountDAO = AccountDAO()
dreDAO = DreDAO()

app = Flask(__name__)

class BalanceDAO:
    def __init__(self):
        mySQL.init_app(app)

    #Date format: mm/yyyy
    def insert_balance_info(self, date, company_id, user_id):
        try:
            date = util.parse_date(date, constants.DATE_START_TYPE)

            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("SELECT * FROM balance_info WHERE company_id = "+str(company_id)+";")

            cursorMySQL.execute(queryMySQL)

            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                queryMySQL2 = ("UPDATE balance_info SET "
                               "date_initial='"+str(date)+"' "
                               "WHERE company_id="+str(company_id)+";")

            else:
                queryMySQL2 = ("INSERT INTO balance_info "
                               "(date_initial, company_id) "
                               "VALUES ('"+str(date)+"', "+str(company_id)+");")
                    
            cursorMySQL.execute(queryMySQL2)
            mySQL.connection.commit()

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
        
    def delete_balance_info(self, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("DELETE FROM balance_info "
                          "WHERE company_id = "+str(company_id)+";")
    
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()
        except Exception as e:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []

    def insert_balance(self, balance, company_id, user_id):
        try:
            acc = accountDAO.get_acc_by_cod(balance['cod_account'], company_id)
            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("SELECT * FROM balance "
                          "WHERE account_id="+str(acc['id'])+";")

            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                queryMySQL = ("UPDATE balance SET "
                              "value_balance="+str(balance["value_balance"])+" "
                              "WHERE account_id="+str(acc["id"])+";")

            else:
                queryMySQL = ("INSERT INTO balance "
                              "(value_balance, account_id) "
                              "VALUES ("+str(balance['value_balance'])+", "+str(acc['id'])+");")
            
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
        
    def delete_balance(self, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()

            queryMySQL = ("DELETE balance FROM balance "
                          "INNER JOIN account acc ON acc.id = balance.account_id "
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
        
    #Date format: mm/yyyy
    def get_balance_comparative(self, date_start, date_end, companies_id, user_id):
        try:
            date_start_search = date_start
            date_end_search = date_end

            list_balance = []
            cursorMySQL = mySQL.connection.cursor()

            date_start = util.parse_date(date_start, constants.DATE_END_TYPE)
            date_end = util.parse_date(date_end, constants.DATE_END_TYPE)

            companies_id_query = util.get_companies_id_query(companies_id)

            #Get Initial Balance Date
            queryMySQL = ("SELECT date_initial FROM balance_info "
                          "WHERE company_id > 0 "
                          +str(companies_id_query)+
                          "ORDER BY date_initial;")

            cursorMySQL.execute(queryMySQL)

            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL == None):
                return codeReturn.BALANCE_INFO_NOT_FOUND_CODE, codeReturn.BALANCE_INFO_NOT_FOUND_MSG, []
            
            year_balance_initial = int(str(rowMySQL[0])[0:4])
            date_balance_initial = str(rowMySQL[0])
    
            #Get Initial Balance
            queryMySQL = ("SELECT SUM(value_balance), cod_account_ref, classification_ref, description_ref, father_ref, order_account "
                          "FROM vw_balance "
                          "WHERE LEFT(classification_ref, 1) <= 2 "
                          "AND (SUBSTRING(classification_ref, 2, 1) = '.' OR SUBSTRING(classification_ref, 2, 1) = '') "
                          "AND cod_account_ref != 999 AND cod_account_ref != 998 "
                          +str(companies_id_query)+
                          "GROUP BY cod_account_ref ORDER BY order_account;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                accountBalance = {
                    "cod_account_ref": rowMySQL[1],
                    "classification_ref": rowMySQL[2],
                    "description_ref": rowMySQL[3],
                    "value_date_start": float(rowMySQL[0]),
                    "av_date_start": 0,
                    "value_date_end": float(rowMySQL[0]),
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": rowMySQL[4],
                    "order_account": rowMySQL[5]
                }

                list_balance.append(accountBalance)
        
            #Get Movimentations Date Start
            queryMySQL = ("SELECT SUM(value_mov), cod_account_ref, father_ref "
                          "FROM vw_movimentation "
                          "WHERE date_mov >= "+str(date_balance_initial)+" "
                          "AND date_mov <= '"+str(date_start)+"' "
                          "AND LEFT(classification_ref, 1) <= 2 "
                          "AND (SUBSTRING(classification_ref, 2, 1) = '.' OR SUBSTRING(classification_ref, 2, 1) = '') "
                          +str(companies_id_query)+
                          "AND cod_account_ref != 999 AND cod_account_ref != 998 "
                          "GROUP BY cod_account_ref;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                value_mov = float(rowMySQL[0])
                cod_account_ref = rowMySQL[1]
                father = rowMySQL[2]

                self.sum_acc_balance(value_mov, list_balance, cod_account_ref, constants.DATE_START_TYPE, user_id)
          
            #Get Movimentations Date End
            queryMySQL = ("SELECT SUM(value_mov), cod_account_ref, father_ref "
                          "FROM vw_movimentation "
                          "WHERE date_mov >= "+str(date_balance_initial)+" "
                          "AND date_mov <= '"+str(date_end)+"' "
                          "AND LEFT(classification_ref, 1) <= 2 "
                          "AND (SUBSTRING(classification_ref, 2, 1) = '.' OR SUBSTRING(classification_ref, 2, 1) = '') "
                          +str(companies_id_query)+
                          "AND cod_account_ref != 999 AND cod_account_ref != 998 "
                          "GROUP BY cod_account_ref;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                value_mov = float(rowMySQL[0])
                cod_account_ref = rowMySQL[1]
                father = rowMySQL[2]

                self.sum_acc_balance(value_mov, list_balance, cod_account_ref, constants.DATE_END_TYPE, user_id)

            cursorMySQL.close()
            
            #Create ACC Resultado do Período
            value_start_result = 0
            value_end_result = 0

            #Date Start Resultado do Período
            date_start_result = '01/'+date_start_search[3:7]
            list_acc_result = self.get_acc_ref_mov_by_classification('15', date_start_result, date_start_search, companies_id, user_id)

            for acc in list_acc_result:
                value_start_result = value_start_result + acc['value']


            #Date End Resultado do Período
            date_end_result = '01/'+date_end_search[3:7]
            list_acc_result = self.get_acc_ref_mov_by_classification('15', date_end_result, date_end_search, companies_id, user_id)

            for acc in list_acc_result:
                value_end_result = value_end_result + acc['value']

            accountBalance = {
                    "cod_account_ref": "198",
                    "classification_ref": "2.03.04.99",
                    "description_ref": "RESULTADO DO PERÍODO",
                    "value_date_start": value_start_result,
                    "av_date_start": 0,
                    "value_date_end": value_end_result,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": "195",
                    "order_account": 99
                }

            list_balance.append(accountBalance)
            
            #Calculando fechamento anual do balanço - Date Start
            if(int(date_start_search[3:7]) > year_balance_initial):
                value_start_closer = 0

                date_start_result = str(date_balance_initial[5:7])+'/'+str(date_balance_initial[0:4])
                date_end_result = '12/'+str(int(date_start_search[3:7])-1)

                list_acc_result = self.get_acc_ref_mov_by_classification('15', date_start_result, date_end_result, companies_id, user_id)

                for acc in list_acc_result:
                    value_start_closer = value_start_closer + acc['value']
                
                if(value_start_closer >= 0):
                    self.sum_acc_balance(value_start_closer, list_balance, '196', constants.DATE_START_TYPE, user_id)
                else:
                    self.sum_acc_balance(value_start_closer, list_balance, '197', constants.DATE_START_TYPE, user_id)

            #Calculando fechamento anual do balanço - Date End
            if(int(date_end_search[3:7]) > year_balance_initial):            
                value_end_closer = 0

                date_start_result = str(date_balance_initial[5:7])+'/'+str(date_balance_initial[0:4])
                date_end_result = '12/'+str(int(date_end_search[3:7])-1)

                list_acc_result = self.get_acc_ref_mov_by_classification('15', date_start_result, date_end_result, companies_id, user_id)

                for acc in list_acc_result:
                    value_end_closer = value_end_closer + acc['value']
                
                if(value_end_closer >= 0):
                    self.sum_acc_balance(value_end_closer, list_balance, '196', constants.DATE_END_TYPE, user_id)
                else:
                    self.sum_acc_balance(value_end_closer, list_balance, '197', constants.DATE_END_TYPE, user_id)
            

            self.update_father_acc_balance(list_balance, user_id)
            self.calculate_variation(list_balance, user_id)
            self.clear_balance(list_balance, user_id)
            
            list_balance.sort(key=lambda x: x['order_account'])
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(list_balance)

    def sum_acc_balance(self, value_mov, list_acc, cod_account_ref, date, user_id):
        try:
            found_acc = False

            cursorMySQL = mySQL.connection.cursor()

            for i in range(len(list_acc)):
                if(list_acc[i]["cod_account_ref"] == cod_account_ref):
                    found_acc = True
                    
                    if(date == constants.DATE_START_TYPE):
                        list_acc[i]["value_date_start"] = list_acc[i]["value_date_start"] + value_mov
                    else:
                        list_acc[i]["value_date_end"] = list_acc[i]["value_date_end"] + value_mov

                    break

            if not (found_acc):
                queryMySQL = ("SELECT cod_account, classification, description, father, order_account FROM account_ref "
                              "WHERE cod_account = '"+cod_account_ref+"';")

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
                    value_date_start = value_mov
                else:
                    value_date_end = value_mov

                accountBalance = {
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

                list_acc.append(accountBalance)

                cursorMySQL.close()

        except:
                log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                          str(traceback.format_exc()), 
                          user_id)
                return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

    def clear_balance(self, list_acc, user_id):
        try:
            for i in range(len(list_acc)-1, -1, -1):
                if(list_acc[i]['value_date_start'] == 0 and list_acc[i]['value_date_end'] == 0): 
                    list_acc.remove(list_acc[i])

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

    def update_father_acc_balance(self, list_acc, user_id):
        try:
            list_aux = list_acc.copy()

            for acc in list_aux:
                if(acc['father'] != constants.ACC_REF_HAS_NO_FATHER):
                    self.sum_father_acc_balance(acc, acc['value_date_start'], acc['value_date_end'], list_acc, user_id)

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

    def sum_father_acc_balance(self, acc, value_date_start, value_date_end, list_acc, user_id):
        try:
            found_father = False

            cursorMySQL = mySQL.connection.cursor()

            for i in range(len(list_acc)):
                if(list_acc[i]['cod_account_ref'] == acc['father']):
                    found_father = True
 
                    list_acc[i]['value_date_start'] = list_acc[i]['value_date_start'] + value_date_start
                    list_acc[i]['value_date_end'] = list_acc[i]['value_date_end'] + value_date_end

                    if(list_acc[i]['father'] != constants.ACC_REF_HAS_NO_FATHER):
                        self.sum_father_acc_balance(list_acc[i], value_date_start, value_date_end, list_acc, user_id)

                    break

            if not (found_father):
                queryMySQL = ("SELECT cod_account, classification, description, father, order_account "
                              "FROM account_ref "
                              "WHERE cod_account = '"+acc['father']+"';")

                cursorMySQL.execute(queryMySQL)
                rowMySQL = cursorMySQL.fetchone()

                accountBalance = {
                    "cod_account_ref": rowMySQL[0],
                    "classification_ref": rowMySQL[1],
                    "description_ref": rowMySQL[2],
                    "value_date_start": value_date_start,
                    "av_date_start": 0,
                    "value_date_end": value_date_end,
                    "av_date_end": 0,
                    "var_percent": 0,
                    "var_value": 0,
                    "father": rowMySQL[3],
                    "order_account": rowMySQL[4]
                }

                list_acc.append(accountBalance)

                if(str(accountBalance['father']) != constants.ACC_REF_HAS_NO_FATHER):
                    self.sum_father_acc_balance(accountBalance, value_date_start, value_date_end, list_acc, user_id)

            cursorMySQL.close()
        except Exception as e:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(e), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
    def calculate_variation(self, list_acc, user_id):
        try:
            #Get Values to calculate AV
            
            acc_ativo = self.get_account_on_list('100', list_acc, user_id)
            acc_passivo = self.get_account_on_list('158', list_acc, user_id)

            ativo_value_start = acc_ativo['value_date_start']
            ativo_value_end = acc_ativo['value_date_end']

            passivo_value_start = acc_passivo['value_date_start']
            passivo_value_end = acc_passivo['value_date_end']

            for i in range(len(list_acc)):
                #Format field 'value_date_start' and 'value_date_end'
                
                #Regulariza valores do Ativo
                if(str(list_acc[i]['classification_ref'].split('.')[0]) == '1'):
                    list_acc[i]['value_date_start'] = list_acc[i]['value_date_start'] * -1
                    list_acc[i]['value_date_end'] = list_acc[i]['value_date_end'] * -1

                value_date_start = list_acc[i]['value_date_start']
                value_date_end = list_acc[i]['value_date_end']

                #Calculate AV
                if(list_acc[i]['cod_account_ref'] == '100' or list_acc[i]['cod_account_ref'] == '158'):
                    list_acc[i]['av_date_start'] = float(100)
                    list_acc[i]['av_date_end'] = float(100)

                elif(list_acc[i]['classification_ref'][0] == '1'):
                    if(ativo_value_start != 0):
                        list_acc[i]['av_date_start'] = (value_date_start / ativo_value_start)*100
                    else:
                        list_acc[i]['av_date_start'] = (value_date_start / 1)*100

                    if(ativo_value_end != 0):
                        list_acc[i]['av_date_end'] = (value_date_end / ativo_value_end)*100
                    else:
                        list_acc[i]['av_date_end'] = (value_date_end / 1)*100

                elif(list_acc[i]['classification_ref'][0] == '2'):
                    if(passivo_value_start != 0):
                        list_acc[i]['av_date_start'] = (value_date_start / passivo_value_start)*100
                    else:
                        list_acc[i]['av_date_start'] = (value_date_start / 1)*100

                    if(passivo_value_end != 0):    
                        list_acc[i]['av_date_end'] = (value_date_end / passivo_value_end)*100
                    else:
                        list_acc[i]['av_date_end'] = (value_date_end / 1)*100

                
                list_acc[i]['av_date_start'] = float("{0:.2f}".format(list_acc[i]['av_date_start']))
                list_acc[i]['av_date_end'] = float("{0:.2f}".format(list_acc[i]['av_date_end']))

                #Calculate Value Variation
                list_acc[i]['var_value'] = float("{0:.2f}".format(value_date_end - value_date_start))

                #Calculate Percent Variation
                if(value_date_start != 0):
                    list_acc[i]['var_percent'] = float("{0:.2f}".format(((value_date_end/value_date_start)-1)*100))
                else:
                    list_acc[i]['var_percent'] = float("{0:.2f}".format(((value_date_end/1)-1)*100))

                list_acc[i]['value_date_start'] = float("{0:.2f}".format(list_acc[i]['value_date_start']))
                list_acc[i]['value_date_end'] = float("{0:.2f}".format(list_acc[i]['value_date_end']))

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

    def get_account_on_list(self, cod_account_ref, list_acc, user_id):
        try:
            for acc in list_acc:
                if(acc['cod_account_ref'] == cod_account_ref):
                    return acc

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

    # Date Format: mm/yyyy
    def get_acc_ref_mov_by_classification(self, classification_ref, date_start, date_end, companies_id, user_id):
        try:
            serie_data_list = []
            
            #Contas de Resultado tem que ser calculadas
            if(classification_ref in constants.CLASSIFICATION_ACC_RESULT):
                #Account 5
                acc3_list = self.get_acc_ref_mov_by_classification('3', date_start, date_end, companies_id, user_id)
                acc4_list = self.get_acc_ref_mov_by_classification('4', date_start, date_end, companies_id, user_id)
                acc5_list = []

                for i in range(len(acc3_list)):
                    try:
                        acc4_value = acc4_list[i]['value']
                    except:
                        acc4_value = 0
                    
                    acc5_list.append({
                        'date': acc3_list[i]['date'],
                        'value': acc3_list[i]['value'] + acc4_value
                    })
                
                if(classification_ref == '5'):
                    return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, acc5_list
                
                #Account 7
                acc6_list = self.get_acc_ref_mov_by_classification('6', date_start, date_end, companies_id, user_id)
                acc7_list = []

                for i in range(len(acc5_list)):
                    try:
                        acc6_value = acc6_list[i]['value']
                    except:
                        acc6_value = 0

                    acc7_list.append({
                        'date': acc5_list[i]['date'],
                        'value': acc5_list[i]['value'] + acc6_value
                    })
                
                if(classification_ref == '7'):
                    return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, acc7_list

                #Account 9
                acc8_list = self.get_acc_ref_mov_by_classification('8', date_start, date_end, companies_id, user_id)
                acc9_list = []

                for i in range(len(acc7_list)):
                    try:
                        acc8_value = acc8_list[i]['value']
                    except:
                        acc8_value = 0

                    acc9_list.append({
                        'date': acc7_list[i]['date'],
                        'value': acc7_list[i]['value'] + acc8_value
                    })
                
                if(classification_ref == '9'):
                    return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, acc9_list
                
                #Account 11
                acc10_list = self.get_acc_ref_mov_by_classification('10', date_start, date_end, companies_id, user_id)
                acc11_list = []

                for i in range(len(acc9_list)):
                    try:
                        acc10_value = acc10_list[i]['value']
                    except:
                        acc10_value = 0

                    acc11_list.append({
                        'date': acc9_list[i]['date'],
                        'value': acc9_list[i]['value'] + acc10_value
                    })
                                 
                if(classification_ref == '11'):
                    return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, acc11_list
                
                #Account 13
                acc12_list = self.get_acc_ref_mov_by_classification('12', date_start, date_end, companies_id, user_id)
                acc13_list = []

                for i in range(len(acc11_list)):
                    try:
                        acc12_value = acc12_list[i]['value']
                    except:
                        acc12_value = 0

                    acc13_list.append({
                        'date': acc11_list[i]['date'],
                        'value': acc11_list[i]['value'] + acc12_value
                    })
                
                if(classification_ref == '13'):
                    return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, acc13_list
                
                #Account 15
                acc14_list = self.get_acc_ref_mov_by_classification('14', date_start, date_end, companies_id, user_id)
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
                 
                return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, acc15_list

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
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
         
        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, serie_data_list

    #Date format: mm/yyyy
    def list_acc_balance_comparative(self, date_start, date_end, cod_account_ref, companies_id, user_id):
        try:
            list_acc_balance = []

            date_start = datetime.strptime(date_start, "%m/%Y")
            date_end = datetime.strptime(date_end, "%m/%Y")

            cursorMySQL = mySQL.connection.cursor()

            companies_id_query = util.get_companies_id_query(companies_id)
            
            #Get Initial Balance Date
            queryMySQL = ("SELECT date_initial FROM balance_info "
                          "WHERE company_id > 0 "
                          +str(companies_id_query)+
                          "ORDER BY date_initial;")

            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()
            print(queryMySQL)
            if(rowMySQL == None):
                return codeReturn.BALANCE_INFO_NOT_FOUND_CODE, codeReturn.BALANCE_INFO_NOT_FOUND_MSG, []
            
            year_balance_initial = int(str(rowMySQL[0])[0:4])
            date_balance_initial = str(rowMySQL[0])

            #Get Initial Balance
            queryMySQL = ("SELECT SUM(value_balance), cod_account, classification, description, cod_account_ref, classification_ref, description_ref "
                          "FROM vw_balance "
                          "WHERE cod_account_ref='"+str(cod_account_ref)+"' "
                          +str(companies_id_query)+
                          "GROUP BY cod_account;")

            cursorMySQL.execute(queryMySQL)
            print(queryMySQL)
            try:
                for rowMySQL in cursorMySQL:
                    if(rowMySQL != None):
                        list_acc_balance.append({
                            "cod_account": rowMySQL[1],
                            "classification": rowMySQL[2],
                            "description": rowMySQL[3],
                            "cod_account_ref": rowMySQL[4],
                            "classification_ref": rowMySQL[5],
                            "description_ref": rowMySQL[6],
                            "value_date_start": float(rowMySQL[0]),
                            "value_date_end": float(rowMySQL[0]),
                            "var_percent": 0.0,
                            "var_value": 0.0
                        })
            except:
                log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
                      
                return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

            #Get Movimentations Date Start
            queryMySQL = ("SELECT SUM(value_mov), date_mov, cod_account, classification, description, cod_account_ref, classification_ref, description_ref "
                          "FROM vw_movimentation "
                          "WHERE date_mov >= "+str(date_balance_initial)+" "
                          "AND date_mov <='"+str(util.parse_date(date_start, constants.DATE_END_TYPE))+"' "
                          "AND cod_account_ref = '"+str(cod_account_ref)+"' "
                          +str(companies_id_query)+
                          "GROUP BY cod_account;")
            

            cursorMySQL.execute(queryMySQL)
            print(queryMySQL)
            for rowMySQL in cursorMySQL:
                value_mov = float(rowMySQL[0])
                cod_account = str(rowMySQL[2])

                sum_return_code, sum_return_msg, sum_return_data = self.sum_mov_acc_balance(value_mov, list_acc_balance, cod_account, constants.DATE_START_TYPE, companies_id, user_id)
                
                if(sum_return_code == codeReturn.UNKNOW_ERROR_CODE):
                    return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
        
            #Get Movimentations Date End
            queryMySQL = ("SELECT SUM(value_mov), date_mov, cod_account, classification, description, cod_account_ref, classification_ref, description_ref "
                          "FROM vw_movimentation "
                          "WHERE date_mov >= "+str(date_balance_initial)+" "
                          "AND date_mov <='"+str(util.parse_date(date_end, constants.DATE_END_TYPE))+"' "
                          "AND cod_account_ref = '"+str(cod_account_ref)+"' "
                          +str(companies_id_query)+
                          "GROUP BY cod_account;")
            

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                value_mov = float(rowMySQL[0])
                cod_account = str(rowMySQL[2])

                sum_return_code, sum_return_msg, sum_return_data = self.sum_mov_acc_balance(value_mov, list_acc_balance, cod_account, constants.DATE_END_TYPE, companies_id, user_id)

                if(sum_return_code == codeReturn.UNKNOW_ERROR_CODE):
                    return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
            
            cursorMySQL.close()
        
            list_acc_balance.sort(key=lambda x: x["classification"])
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(list_acc_balance)
    
    def sum_mov_acc_balance(self, value_mov, list_acc, cod_account, date, companies_id, user_id):
        try:
            found_acc = False

            cursorMySQL = mySQL.connection.cursor()

            for i in range(len(list_acc)):
                if(list_acc[i]["cod_account"] == cod_account):
                    found_acc = True

                    if(date == constants.DATE_START_TYPE):
                        list_acc[i]["value_date_start"] = float(list_acc[i]["value_date_start"]) + float(value_mov)
                    else:
                        list_acc[i]["value_date_end"] = float(list_acc[i]["value_date_end"]) + float(value_mov)

                    #Calculate value variation
                    list_acc[i]["var_value"] = float("{0:.2f}".format(list_acc[i]["value_date_end"] - list_acc[i]["value_date_start"]))

                    #Calculate percent variation
                    if(list_acc[i]["value_date_start"] != 0):
                        list_acc[i]["var_percent"] =  float("{0:.2f}".format(((list_acc[i]["value_date_end"]/list_acc[i]["value_date_start"])-1)*100))
                    else:
                        list_acc[i]["var_percent"] =  float("{0:.2f}".format(((list_acc[i]["value_date_end"]/1)-1)*100))

                    break

            if not (found_acc):
                companies_id_query = util.get_companies_id_query(companies_id)

                queryMySQL = ("SELECT cod_account, classification, description, cod_account_ref, classification_ref, description_ref "
                              "FROM vw_account "
                              "WHERE cod_account = '"+cod_account+"' "
                              +str(companies_id_query)+";")
                
                cursorMySQL.execute(queryMySQL)
                rowMySQL = cursorMySQL.fetchone()

                value_date_start = 0.0
                value_date_end = 0.0

                if(date == constants.DATE_START_TYPE):
                    value_date_start = float(value_mov)
                else:
                    value_date_end = float(value_mov)

                 #Calculate value variation
                var_value = float("{0:.2f}".format(value_date_end - value_date_start))

                #Calculate percent variation
                var_percent =  float("{0:.2f}".format(((value_date_end/1)-1)*100))


                list_acc.append({
                    "cod_account": rowMySQL[0],
                    "classification": rowMySQL[1],
                    "description": rowMySQL[2],
                    "cod_account_ref": rowMySQL[3],
                    "classification_ref": rowMySQL[4],
                    "description_ref": rowMySQL[5],
                    "value_date_start": value_date_start,
                    "value_date_end": value_date_end,
                    "var_percent": var_percent,
                    "var_value": var_value
                })

                cursorMySQL.close()

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
    
