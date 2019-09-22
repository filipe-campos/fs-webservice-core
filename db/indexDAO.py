# -*- coding: utf-8 -*-
import mysql.connector
import datetime
import logging
import json
import traceback
import inspect

from dateutil.relativedelta import *
from db import DreDAO, BalanceDAO
from util import Util, Log, Constants, CodeReturn
from flask import Flask
from flask_mysqldb import MySQL

log = Log('IndexDAO')
constants = Constants()
codeReturn = CodeReturn()
util = Util()
mySQL = MySQL()
dreDAO = DreDAO()
balanceDAO = BalanceDAO()

app = Flask(__name__)

class IndexDAO:
    def __init__(self):
        mySQL.init_app(app)

    #Date format: mm/yyyy
    def get_index_comparative(self, date_start, date_end, companies_id, user_id):
        try:
            ativo_start = 0
            ativo_end = 0
            ativo_circulante_start = 0
            ativo_circulante_end = 0
            ativo_disponivel_start = 0
            ativo_disponivel_end = 0
            ativo_n_circulante_start = 0
            ativo_n_circulante_end = 0
            ativo_imobilizado_start = 0
            ativo_imobilizado_end = 0
            passivo_start = 0
            passivo_end = 0
            passivo_circulante_start = 0
            passivo_circulante_end = 0
            passivo_n_circulante_start = 0
            passivo_n_circulante_end = 0
            estoques_start = 0
            estoques_end = 0
            patrimonio_liquido_start = 0
            patrimonio_liquido_end = 0
            resultado_liquido_start = 0
            resultado_liquido_end = 0
            receita_liquida_start = 0
            receita_liquida_end = 0

            #Get Datas to Calculate Indices
            list_dre = json.loads(str(dreDAO.get_dre_comparative(date_start, date_end, companies_id, user_id)).replace("'", '"'))
            list_balance = json.loads(str(balanceDAO.get_balance_comparative(date_start, date_end, companies_id, user_id)).replace("'", '"'))

            for acc in list_balance:
                if(acc['description_ref'] == 'ATIVO'):
                    ativo_start = acc['value_date_start']
                    ativo_end = acc['value_date_end']

                if(acc['description_ref'] == 'ATIVO CIRCULANTE'):
                    ativo_circulante_start = acc['value_date_start']
                    ativo_circulante_end = acc['value_date_end']

                if(acc['description_ref'] == 'DISPONIBILIDADES'):
                    ativo_disponivel_start = acc['value_date_start']
                    ativo_disponivel_end = acc['value_date_end']

                if(acc['description_ref'] == 'ATIVO NÃO CIRCULANTE'):
                    ativo_n_circulante_start = acc['value_date_start']
                    ativo_n_circulante_end = acc['value_date_end']

                if(acc['description_ref'] == 'IMOBILIZADO'):
                    ativo_imobilizado_start = acc['value_date_start']
                    ativo_imobilizado_end = acc['value_date_end']

                if(acc['description_ref'] == 'PASSIVO'):
                    passivo_start = acc['value_date_start']
                    passivo_end = acc['value_date_end']

                if(acc['description_ref'] == 'PASSIVO CIRCULANTE'):
                    passivo_circulante_start = acc['value_date_start']
                    passivo_circulante_end = acc['value_date_end']

                if(acc['description_ref'] == 'PASSIVO NÃO-CIRCULANTE'):
                    passivo_n_circulante_start = acc['value_date_start']
                    passivo_n_circulante_end = acc['value_date_end']

                if(acc['description_ref'] == 'ESTOQUES'):
                    estoques_start = acc['value_date_start']
                    estoques_end = acc['value_date_end']

                if(acc['description_ref'] == 'PATRIMÔNIO LÍQUIDO'):
                    patrimonio_liquido_start = acc['value_date_start']
                    patrimonio_liquido_end = acc['value_date_end']

            for acc in list_dre:
                if(acc['description_ref'] == '(=) RESULTADO LÍQUIDO'):
                    resultado_liquido_start = acc['value_date_start']
                    resultado_liquido_end = acc['value_date_end']

                if(acc['description_ref'] == '(=) RECEITA LIQUIDA'):
                    receita_liquida_start = acc['value_date_start']
                    receita_liquida_end = acc['value_date_end']

            #Calculate
            json_data = {}

            #Liquidez
            liquidez_data = {}

            liquidez_date_start = {}
            liquidez_date_start['geral'] = "{0:.2f}".format((ativo_circulante_start + ativo_n_circulante_start) / (passivo_circulante_start + passivo_n_circulante_start))
            liquidez_date_start['corrente'] = "{0:.2f}".format((ativo_circulante_start / passivo_circulante_start))
            liquidez_date_start['seca'] = "{0:.2f}".format((ativo_circulante_start - estoques_start) / passivo_circulante_start)
            liquidez_date_start['imediata'] = "{0:.2f}".format(ativo_disponivel_start / passivo_circulante_start)

            liquidez_date_end = {}
            liquidez_date_end['geral'] = "{0:.2f}".format((ativo_circulante_end + ativo_n_circulante_end) / (passivo_circulante_end + passivo_n_circulante_end))
            liquidez_date_end['corrente'] = "{0:.2f}".format((ativo_circulante_end / passivo_circulante_end))
            liquidez_date_end['seca'] = "{0:.2f}".format((ativo_circulante_end - estoques_end) / passivo_circulante_end)
            liquidez_date_end['imediata'] = "{0:.2f}".format(ativo_disponivel_end / passivo_circulante_end)

            liquidez_data['date_start'] = liquidez_date_start
            liquidez_data['date_end'] = liquidez_date_end

            json_data['liquidez'] = liquidez_data

            #Endividamento
            endividamento_data = {}

            endividamento_date_start = {}
            endividamento_date_start['endividamento_geral'] = "{0:.2f}".format(((passivo_circulante_start + passivo_n_circulante_start) / ativo_start)*100)
            endividamento_date_start['comp_curto_prazo'] = "{0:.2f}".format((passivo_circulante_start / (passivo_circulante_start+passivo_n_circulante_start))*100)
            endividamento_date_start['comp_longo_prazo'] = "{0:.2f}".format((passivo_n_circulante_start / (passivo_circulante_start+passivo_n_circulante_start))*100)

            endividamento_date_end = {}
            endividamento_date_end['endividamento_geral'] = "{0:.2f}".format(((passivo_circulante_end + passivo_n_circulante_end) / ativo_end)*100)
            endividamento_date_end['comp_curto_prazo'] = "{0:.2f}".format((passivo_circulante_end / (passivo_circulante_end+passivo_n_circulante_end))*100)
            endividamento_date_end['comp_longo_prazo'] = "{0:.2f}".format((passivo_n_circulante_end / (passivo_circulante_end+passivo_n_circulante_end))*100)


            endividamento_data['date_start'] = endividamento_date_start
            endividamento_data['date_end'] = endividamento_date_end

            json_data['endividamento'] = endividamento_data

            #Rentabilidade
            rentabilidade_data = {}

            rentabilidade_date_start = {}
            rentabilidade_date_start['ret_sobre_ativo'] = "{0:.2f}".format((resultado_liquido_start / ativo_start)*100)
            rentabilidade_date_start['ret_capital_proprio'] = "{0:.2f}".format((resultado_liquido_start / patrimonio_liquido_start)*100)
            rentabilidade_date_start['giro_ativo'] = "{0:.2f}".format((receita_liquida_start / ativo_start)*100)
            rentabilidade_date_start['lucro_liquido'] = "{0:.2f}".format((resultado_liquido_start / receita_liquida_start)*100)

            rentabilidade_date_end = {}
            rentabilidade_date_end['ret_sobre_ativo'] = "{0:.2f}".format((resultado_liquido_end / ativo_end)*100)
            rentabilidade_date_end['ret_capital_proprio'] = "{0:.2f}".format((resultado_liquido_end / patrimonio_liquido_end)*100)
            rentabilidade_date_end['giro_ativo'] = "{0:.2f}".format((receita_liquida_end / ativo_end)*100)
            rentabilidade_date_end['lucro_liquido'] = "{0:.2f}".format((resultado_liquido_end / receita_liquida_end)*100)

            rentabilidade_data['date_start'] = rentabilidade_date_start
            rentabilidade_data['date_end'] = rentabilidade_date_end

            json_data['rentabilidade'] = rentabilidade_data


            #Estrutura do Capital
            capital_data = {}

            capital_date_start = {}
            capital_date_start['part_capital_terceiros'] = "{0:.1f}".format(((passivo_circulante_start + passivo_n_circulante_start) / patrimonio_liquido_start)*100)
            capital_date_start['imob_pl'] = "{0:.1f}".format((ativo_imobilizado_start / patrimonio_liquido_start)*100)
            capital_date_start['imob_rec_n_correntes'] = "{0:.1f}".format((ativo_imobilizado_start / (patrimonio_liquido_start + passivo_n_circulante_start))*100)

            capital_date_end = {}
            capital_date_end['part_capital_terceiros'] = "{0:.1f}".format(((passivo_circulante_end + passivo_n_circulante_end) / patrimonio_liquido_end)*100)
            capital_date_end['imob_pl'] = "{0:.1f}".format((ativo_imobilizado_end / patrimonio_liquido_end)*100)
            capital_date_end['imob_rec_n_correntes'] = "{0:.1f}".format((ativo_imobilizado_end / (patrimonio_liquido_end + passivo_n_circulante_end))*100)

            capital_data['date_start'] = capital_date_start
            capital_data['date_end'] = capital_date_end

            json_data['capital'] = capital_data
        
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      user_id)
            return codeReturn.UNKNOW_ERROR_CODE, codeReturn.UNKNOW_ERROR_MSG, []

        return codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, json.dumps(json_data)
