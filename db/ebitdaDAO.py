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
from db import DreDAO

constants = Constants()
codeReturn = CodeReturn()
dreDAO = DreDAO()
log = Log('EbitdaDAO') 
util = Util()
mySQL = MySQL()

app = Flask(__name__)

class EbitdaDAO:
    def __init__(self):
        mySQL.init_app(app)
    
    def get_ebitda_period(self, date_start, date_end, companies_id, user_id):
        try:
            list_ebitda = []

            dre_month_list = json.loads(str(dreDAO.get_dre_month(date_start, date_end, companies_id, user_id)).replace("'", '"'))

            date_start = datetime.strptime(date_start, "%m/%Y")
            date_end = datetime.strptime(date_end, "%m/%Y")

            cursorMySQL = mySQL.connection.cursor()

            acc_percent_ebitda = {
                "cod_account_ref": None,
                "classification_ref": None,
                "description_ref": '% DO EBITDA EM RELAÇÃO AO FATURAMENTO',
                "accumulated": 0,
                "father": None,
                "order_account": 0
            }

            acc_ebitda = {
                "cod_account_ref": None,
                "classification_ref": None,
                "description_ref": 'EBITDA',
                "accumulated": 0,
                "father": None,
                "order_account": 1
            }

            acc_desp_fin = {
                "cod_account_ref": 224,
                "classification_ref": '8.33',
                "description_ref": '(-) DESPESAS FINANCEIRAS',
                "accumulated": 0,
                "father": 214,
                "order_account": 125
            }

            acc_rec_fin = {
                "cod_account_ref": 227,
                "classification_ref": '8.45',
                "description_ref": '(+) RECEITAS FINANCEIRAS',
                "accumulated": 0,
                "father": 214,
                "order_account": 129
            }

            acc_jrs_fin = {
                "cod_account_ref": 226,
                "classification_ref": '8.41',
                "description_ref": '(-) JUROS SOBRE FINANCIAMENTOS E EMPRESTIMOS',
                "accumulated": 0,
                "father": 214,
                "order_account": 127
            }

            acc_jrs_desp_fin = {
                "cod_account_ref": 234,
                "classification_ref": '10.09',
                "description_ref": '(-) JUROS E DESPESAS FINANCEIRAS EXTRAORDINÁRIA',
                "accumulated": 0,
                "father": 231,
                "order_account": 135
            }

            acc_dms_rec = {
                "cod_account_ref": 232,
                "classification_ref": '10.01',
                "description_ref": '(+) DEMAIS RECEITAS NÃO OPERACIONAIS',
                "accumulated": 0,
                "father": 231,
                "order_account": 133
            }

            acc_dms_desp = {
                "cod_account_ref": 233,
                "classification_ref": '10.05',
                "description_ref": '(-) DEMAIS DESPESAS NÃO OPERACIONAIS',
                "accumulated": 0,
                "father": 231,
                "order_account": 134
            }

            acc_prov_irpj = {
                "cod_account_ref": 237,
                "classification_ref": '12.01',
                "description_ref": '(-) PROVISÃO P/ IRPJ',
                "accumulated": 0,
                "father": 236,
                "order_account": 138
            }

            acc_prov_csll = {
                "cod_account_ref": 238,
                "classification_ref": '12.03',
                "description_ref": '(-) PROVISÃO P/ CSLL',
                "accumulated": 0,
                "father": 236,
                "order_account": 139
            }

            acc_irpj_dif = {
                "cod_account_ref": 239,
                "classification_ref": '12.05',
                "description_ref": '(+) IRPJ DIFERIDO',
                "accumulated": 0,
                "father": 236,
                "order_account": 140
            }

            acc_csll_dif = {
                "cod_account_ref": 240,
                "classification_ref": '12.07',
                "description_ref": '(+) CSLL DIFERIDA',
                "accumulated": 0,
                "father": 236,
                "order_account": 141
            }

            acc_result_liq = {
                "cod_account_ref": 247,
                "classification_ref": '15',
                "description_ref": '(=) RESULTADO LÍQUIDO',
                "accumulated": 0,
                "father": 0,
                "order_account": 148
            }

            while(date_start <= date_end):

                key = str(date_start.month).rjust(2, '0')+'/'+str(date_start.year)

                #(-) DESPESAS FINANCEIRAS
                desp_fin = self.get_account_on_list(224, dre_month_list, user_id)

                if desp_fin != None and key in desp_fin:
                    acc_desp_fin[key] = desp_fin[key]
                    acc_desp_fin['accumulated'] = acc_desp_fin['accumulated'] + desp_fin[key]
                else:
                    acc_desp_fin[key] = 0

                
                #(+) RECEITAS FINANCEIRAS
                rec_fin = self.get_account_on_list(227, dre_month_list, user_id)

                if rec_fin != None and key in rec_fin:
                    acc_rec_fin[key] = rec_fin[key]
                    acc_rec_fin['accumulated'] = acc_rec_fin['accumulated'] + rec_fin[key]
                else:
                    acc_rec_fin[key] = 0

                #(-) JUROS SOBRE FINANCIAMENTOS E EMPRESTIMOS
                jrs_fin = self.get_account_on_list(226, dre_month_list, user_id)

                if jrs_fin != None and key in jrs_fin:
                    acc_jrs_fin[key] = jrs_fin[key]
                    acc_jrs_fin['accumulated'] = acc_jrs_fin['accumulated'] + jrs_fin[key]
                else:
                    acc_jrs_fin[key] = 0

                #(-) JUROS E DESPESAS FINANCEIRAS EXTRAORDINÁRIAS
                jrs_desp_fin = self.get_account_on_list(234, dre_month_list, user_id)

                if jrs_desp_fin != None and key in jrs_desp_fin:
                    acc_jrs_desp_fin[key] = jrs_desp_fin[key]
                    acc_jrs_desp_fin['accumulated'] = acc_jrs_desp_fin['accumulated'] + jrs_desp_fin[key]
                else:
                    acc_jrs_desp_fin[key] = 0

                #(+) DEMAIS RECEITAS NÃO OPERACIONAIS
                dms_rec = self.get_account_on_list(232, dre_month_list, user_id)

                if dms_rec != None and key in dms_rec:
                    acc_dms_rec[key] = dms_rec[key]
                    acc_dms_rec['accumulated'] = acc_dms_rec['accumulated'] + dms_rec[key]
                else:
                    acc_dms_rec[key] = 0

                #(-) DEMAIS DESPESAS NÃO OPERACIONAIS
                dms_desp = self.get_account_on_list(233, dre_month_list, user_id)

                if dms_desp != None and key in dms_desp:
                    acc_dms_desp[key] = dms_desp[key]
                    acc_dms_desp['accumulated'] = acc_dms_desp['accumulated'] + dms_desp[key]
                else:
                    acc_dms_desp[key] = 0

                #(-) PROVISÃO P/ IRPJ
                prov_irpj = self.get_account_on_list(237, dre_month_list, user_id)

                if prov_irpj != None and key in prov_irpj:
                    acc_prov_irpj[key] = prov_irpj[key]
                    acc_prov_irpj['accumulated'] = acc_prov_irpj['accumulated'] + prov_irpj[key]
                else:
                    acc_prov_irpj[key] = 0

                #(-) PROVISÃO P/ CSLL
                prov_csll = self.get_account_on_list(238, dre_month_list, user_id)

                if prov_csll != None and key in prov_csll:
                    acc_prov_csll[key] = prov_csll[key]
                    acc_prov_csll['accumulated'] = acc_prov_csll['accumulated'] + prov_csll[key]
                else:
                    acc_prov_csll[key] = 0

                #(+) IRPJ DIFERIDO
                irpf_dif = self.get_account_on_list(239, dre_month_list, user_id)

                if irpf_dif != None and key in irpf_dif:
                    acc_irpj_dif[key] = irpf_dif[key]
                    acc_irpj_dif['accumulated'] = acc_irpj_dif['accumulated'] + irpf_dif[key]
                else:
                    acc_irpj_dif[key] = 0

                #(+) CSLL DIFERIDO
                csll_dif = self.get_account_on_list(240, dre_month_list, user_id)

                if csll_dif != None and key in csll_dif:
                    acc_csll_dif[key] = csll_dif[key]
                    acc_csll_dif['accumulated'] = acc_csll_dif['accumulated'] + csll_dif[key]
                else:
                    acc_csll_dif[key] = 0
                
                #(=) RESULTADO LÍQUIDO
                result_liq = self.get_account_on_list(247, dre_month_list, user_id)

                if result_liq != None and key in result_liq:
                    acc_result_liq[key] = result_liq[key]
                    acc_result_liq['accumulated'] = acc_csll_dif['accumulated'] + result_liq[key]
                else:
                    acc_result_liq[key] = 0

                #EBITDA
                acc_ebitda[key] = (acc_result_liq[key] - (acc_desp_fin[key] + 
                                                          acc_rec_fin[key] + 
                                                          acc_jrs_fin[key] + 
                                                          acc_jrs_desp_fin[key] + 
                                                          acc_dms_rec[key] + 
                                                          acc_dms_desp[key] + 
                                                          acc_prov_irpj[key] + 
                                                          acc_prov_csll[key] + 
                                                          acc_irpj_dif[key] + 
                                                          acc_csll_dif[key])
                                   )
                
                acc_ebitda['accumulated'] = acc_ebitda['accumulated'] + acc_ebitda[key]
                
                #EBITDA PERCENT
                rct_liq = self.get_account_on_list('208', dre_month_list, user_id)
                
                if rct_liq != None:
                    acc_percent_ebitda[key] = (acc_ebitda[key]/rct_liq[key])*100
                else:
                    acc_percent_ebitda[key] = 0

                date_start = date_start + relativedelta(months=+1)

            list_ebitda.append(acc_percent_ebitda)
            list_ebitda.append(acc_ebitda)
            list_ebitda.append(acc_desp_fin)
            list_ebitda.append(acc_rec_fin)
            list_ebitda.append(acc_jrs_fin)
            list_ebitda.append(acc_jrs_desp_fin)
            list_ebitda.append(acc_dms_rec)
            list_ebitda.append(acc_dms_desp)
            list_ebitda.append(acc_prov_irpj)
            list_ebitda.append(acc_prov_csll)
            list_ebitda.append(acc_irpj_dif)
            list_ebitda.append(acc_csll_dif)
            list_ebitda.append(acc_result_liq)

            list_ebitda.sort(key=lambda x: x["order_account"])

        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(list_ebitda, sort_keys=True)


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

    