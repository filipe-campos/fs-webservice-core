# -*- coding: utf-8 -*-
import mysql.connector
import datetime
import logging
import json
import traceback
import inspect

from dateutil.relativedelta import *
from db import BalanceDAO, DreDAO
from util import Util, Log, Constants, CodeReturn
from flask import Flask
from flask_mysqldb import MySQL

log = Log('DfcDAO')
constants = Constants()
codeReturn = CodeReturn()
util = Util()
mySQL = MySQL()
balanceDAO = BalanceDAO()
dreDAO = DreDAO()

app = Flask(__name__)

class DfcDAO:
    def __init__(self):
        mySQL.init_app(app)

    #Date format: mm/yyyy
    def get_dfc(self, date_start, date_end, companies_id, user_id):
        try:
            is_mensal = False
            list_dfc = []
            companies_id_query = util.get_companies_id_query(companies_id)
            cursorMySQL = mySQL.connection.cursor()

            if(date_start == None):
                date = datetime.datetime(int(date_end[3:7]), int(date_end[0:2]), 1)

                #Decrement month
                date = date + relativedelta(months=-1)
                date_start = date.strftime("%m") + '/' + date.strftime("%Y")

                is_mensal = True
            else:
                date_start_profit = date_start

                date = datetime.datetime(int(date_start[3:7]), int(date_start[0:2]), 1)

                #Decrement month
                date = date + relativedelta(months=-1)
                date_start = date.strftime("%m") + '/' + date.strftime("%Y")

                list_dre_periodo = json.loads(str(dreDAO.get_dre_period(date_start, date_end, date_start, date_end, companies_id, user_id)).replace("'", '"'))


            date = util.parse_date(date_end, constants.DATE_START_TYPE)

            #Get Distributed Profit and Exercise Adjustment
            if(is_mensal):
                queryMySQL = ("SELECT value_profit, exercise_adjustment FROM dfc_info "
                              "WHERE date_dfc_info = '"+str(date)+"' "
                              +str(companies_id_query)+";")
            else:
                queryMySQL = ("SELECT SUM(value_profit), SUM(exercise_adjustment) FROM dfc_info "
                              "WHERE date_dfc_info >= '"+str(util.parse_date(date_start_profit, constants.DATE_START_TYPE))+"' AND date_dfc_info <= '"+str(util.parse_date(date_end, constants.DATE_START_TYPE))+"' "
                              +str(companies_id_query)+";")
            
            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                value_lucro = float(rowMySQL[0])
                value_exercicio = float(rowMySQL[1])
            else:
                value_lucro = 0
                value_exercicio = 0

            accLucrosDistribuidos = {
                'cod_account': '5.4', 
                'description': 'Lucros Distribuidos',
                'value': value_lucro
            }
            
            accExerciciosAnteriores = {
                'cod_account': '2.2', 
                'description': 'Ajustes de Exercícios Anteriores',
                'value': value_exercicio
            }

            accResultLucroLiq = {
                'cod_account': '2.9', 
                'description': 'Lucro Líquido Ajustado',
                'value': 0
            }
            
            accResultAtividadesOp = {
                'cod_account': '3.99', 
                'description': '(=) Caixa Líquido Gerado nas Atividades Operacionais',
                'value': 0
            }
            
            accResultAtividadesInv = {
                'cod_account': '4.6', 
                'description': '(=) Caixa Líquido Gerado nas Atividades de Investimento',
                'value': 0
            }
            
            accResultAtividadesFin = {
                'cod_account': '5.5', 
                'description': '(=) Caixa Líquido Gerado nas Atividades de Financiamento',
                'value': 0
            }
            
            accResultLiqDisp = {
                'cod_account': '5.9', 
                'description': 'Aumento Líquido nas Disponibilidades',
                'value': 0
            }
            
            accResultCaixa = {
                'cod_account': '6.3', 
                'description': 'Aumento de Caixa',
                'value': 0
            }

            #Get Initial Balance Date
            queryMySQL = ("SELECT date_initial FROM balance_info "
                          "WHERE company_id > 0 "
                          +str(companies_id_query)+
                          "ORDER BY date_initial;")

            cursorMySQL.execute(queryMySQL)

            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                date_balance_initial = str(rowMySQL[0])
                date_balance_initial = str(date_balance_initial[5:7])+'/'+str(date_balance_initial[0:4])
            else:
                return codeReturn.BALANCE_INFO_NOT_FOUND_CODE, codeReturn.BALANCE_INFO_NOT_FOUND_MSG, []

            list_balance_initial = json.loads(str(balanceDAO.get_balance_comparative(date_balance_initial, date_balance_initial, companies_id, user_id)).replace("'", '"'))
            list_balance = json.loads(str(balanceDAO.get_balance_comparative(date_start, date_end, companies_id, user_id)).replace("'", '"'))
            list_dre = json.loads(str(dreDAO.get_dre_comparative(date_start, date_end, companies_id, user_id)).replace("'", '"'))

            queryMySQL = ("SELECT id_account_ref, cod_account_ref, description_ref, id_dfc_cont, cod_account_dfc, description_dfc "
                          "FROM vw_acc_ref_dfc_cont;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                #id_cod_account_ref = rowMySQL[0]
                cod_account_ref = rowMySQL[1]
                description_ref = rowMySQL[2]
                #id_dfc_cont = rowMySQL[3]
                cod_account_dfc = rowMySQL[4]
                description_dfc = rowMySQL[5]

                if(cod_account_dfc == '6.01'):
                    value = self.get_value(cod_account_ref, list_balance, list_dre, constants.DATE_START_TYPE)
                elif(cod_account_dfc == '6.02'):
                    value = self.get_value(cod_account_ref, list_balance, list_dre, constants.DATE_END_TYPE)
                elif(cod_account_dfc == '1'):
                    if(is_mensal):
                        value = self.get_value(cod_account_ref, list_balance, list_dre, constants.DATE_END_TYPE)
                    else:
                        value = self.get_value(cod_account_ref, list_balance, list_dre_periodo, constants.DATE_END_TYPE)
                else:
                    value = self.get_dif(cod_account_ref, list_balance, list_dre)

                if(value != None):
                    self.check_account_dfc(cod_account_dfc, description_dfc, value, list_dfc)

                    if(cod_account_dfc[0] == '1' or cod_account_dfc[0] == '2' ):
                        accResultLucroLiq['value'] = accResultLucroLiq['value'] + value        
                    if(cod_account_dfc[0] == '3'):
                        accResultAtividadesOp['value'] = accResultAtividadesOp['value'] + value
                    if(cod_account_dfc[0] == '4'):
                        accResultAtividadesInv['value'] = accResultAtividadesInv['value'] + value
                    if(cod_account_dfc[0] == '5'):
                        accResultAtividadesFin['value'] = accResultAtividadesFin['value'] + value
                    #if(cod_account_dfc[0] == '6'):
                    #    accResultCaixa.value = accResultCaixa.value + value

            accResultAtividadesOp['value'] = accResultAtividadesOp['value'] + accResultLucroLiq['value']
            accResultAtividadesFin['value'] = accResultAtividadesFin['value'] + accLucrosDistribuidos['value']
            accResultLucroLiq['value'] = accResultLucroLiq['value'] + accExerciciosAnteriores['value']

            accResultLiqDisp['value'] = accResultAtividadesOp['value'] + accResultAtividadesInv['value'] + accResultAtividadesFin['value']

            accResultCaixa['value'] = self.get_account_dfc('6.02', list_dfc)['value'] - self.get_account_dfc('6.01', list_dfc)['value']

            #Change Description of Accounts
            if(accResultLucroLiq['value'] < 0):
                accResultLucroLiq['description'] = 'Prejuízo Líquido Ajustado'

            if(accResultAtividadesOp['value'] < 0):
                accResultAtividadesOp['description'] = '(=) Caixa Líquido Consumido nas Atividades Operacionais'

            if(accResultAtividadesInv['value'] < 0):
                accResultAtividadesInv['description'] = '(=) Caixa Líquido Consumido nas Atividades de Investimento'

            if(accResultAtividadesFin['value'] < 0):
                accResultAtividadesFin['description'] = '(=) Caixa Líquido Consumido nas Atividades de Financiamento'

            if(accResultLiqDisp['value'] < 0):
                accResultLiqDisp['description'] = 'Redução Líquida nas Disponibilidades'

            if(accResultCaixa['value'] < 0):
                accResultCaixa['description'] = 'Redução de Caixa'

            list_dfc.append(accLucrosDistribuidos)
            list_dfc.append(accExerciciosAnteriores)
            list_dfc.append(accResultLucroLiq)
            list_dfc.append(accResultAtividadesOp)
            list_dfc.append(accResultAtividadesInv)
            list_dfc.append(accResultAtividadesFin)
            list_dfc.append(accResultLiqDisp)
            list_dfc.append(accResultCaixa)

            list_dfc.sort(key=lambda x: x['cod_account'])

            self.parse_values(list_dfc)

            cursorMySQL.close()
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, list_dfc

    def check_account_dfc(self, cod_account_dfc, description_dfc, value, list_dfc):
        found_acc = False

        for i in range(len(list_dfc)):
            if(list_dfc[i]['cod_account'] == cod_account_dfc):
                found_acc = True
                list_dfc[i]['value'] = list_dfc[i]['value'] + value

                break

        if not(found_acc):
            list_dfc.append({
                'cod_account': cod_account_dfc, 
                'description': description_dfc, 
                'value': value
                })

    def get_account_dfc(self, cod_account_dfc, list_dfc):
        for i in range(len(list_dfc)):
            if(list_dfc[i]['cod_account'] == cod_account_dfc):
                return list_dfc[i]

    def get_dif(self, cod_account_ref, list_balance, list_dre):
        for b in list_balance:
            if (b['cod_account_ref'] == cod_account_ref):
                if(b['classification_ref'].split('.')[0] == '1'): 
                    return (b['value_date_end'] - b['value_date_start'])* -1
                else:
                    return (b['value_date_end'] - b['value_date_start'])
        
        for d in list_dre:
            if (d['cod_account_ref'] == cod_account_ref):
                return (d['value_date_end'] - d['value_date_start'])

    def get_value(self, cod_account_ref, l_balance, l_dre, date):
        for b in l_balance:
            if (b['cod_account_ref'] == cod_account_ref):
                if(b['classification_ref'].split('.')[0] == '1'):
                    if(date == constants.DATE_START_TYPE):
                        return b['value_date_start']
                    else:
                        return b['value_date_end']
                else:
                    return b['value_date_end']
        
        for d in l_dre:   
            if (d['cod_account_ref'] == cod_account_ref):
                if(date == constants.DATE_START_TYPE):
                    return d['value_date_start']
                else:
                    return d['value_date_end']

    def parse_values(self, list_dfc):
        for i in range(len(list_dfc)):
            list_dfc[i]['value'] = float("{0:.2f}".format(list_dfc[i]['value']))

    def insert_dfc_info_profit(self, info, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            date = util.parse_date(info['date'], constants.DATE_START_TYPE)
            value = util.parse_money(info['distributed_profit'])

            queryMySQL = ("SELECT * FROM dfc_info WHERE date_dfc_info = '"+str(date)+"' AND company_id = "+str(company_id)+";")
            
            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                queryMySQL = ("UPDATE dfc_info SET "
                              "distributed_profit="+str(value)+" "
                              "WHERE date_dfc_info='"+str(date)+"' AND company_id="+str(company_id)+";")
            else:
                queryMySQL = ("INSERT INTO dfc_info "
                              "(distributed_profit, exercise_adjustment, date_dfc_info, company_id) "
                              "VALUES ("+str(value)+", 0, '"+str(date)+"', "+str(company_id)+");")
            
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()
            
            return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)

            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
    
    def insert_dfc_info_exercise(self, info, company_id, user_id):
        try:
            cursorMySQL = mySQL.connection.cursor()
            date = util.parse_date(info['date'], constants.DATE_START_TYPE)
            value = util.parse_money(info['exercise_adjustment'])

            queryMySQL = ("SELECT * FROM dfc_info WHERE date_dfc_info = '"+str(date)+"' AND company_id = "+str(company_id)+";")
            
            cursorMySQL.execute(queryMySQL)
            rowMySQL = cursorMySQL.fetchone()

            if(rowMySQL != None):
                queryMySQL = ("UPDATE dfc_info SET "
                              "exercise_adjustment="+str(value)+" "
                              "WHERE date_dfc_info='"+str(date)+"' AND company_id="+str(company_id)+";")
            else:
                queryMySQL = ("INSERT INTO dfc_info "
                              "(distributed_profit, exercise_adjustment, date_dfc_info, company_id) "
                              "VALUES (0, "+str(value)+", '"+str(date)+"', "+str(company_id)+");")
            
            cursorMySQL.execute(queryMySQL)
            mySQL.connection.commit()

            return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, []
            
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []
    
    def list_dfc_info(self, companies_id, user_id):
        try:
            list_dfc_info = []
            cursorMySQL = mySQL.connection.cursor()

            companies_id_query = util.get_companies_id_query(companies_id)

            queryMySQL = ("SELECT value_profit, exercise_adjustment, date_dfc_info, company_name "
                          "FROM vw_dfc_info "
                          "WHERE company_id>0 "
                          +str(companies_id_query)+
                          "ORDER BY date_dfc_info DESC;")

            cursorMySQL.execute(queryMySQL)

            for rowMySQL in cursorMySQL:
                list_dfc_info.append({
                    "date" : str(rowMySQL[2].strftime("%m/%Y")),
                    "value_profit" : str(rowMySQL[0]),
                    "exercise_adjustment" : str(rowMySQL[1]),
                    "company_name" : str(rowMySQL[3])
                })

            return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, list_dfc_info
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
                      
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []