# -*- coding: utf-8 -*-
import os
import json
import traceback
import inspect

from db import BalanceDAO
from flask import Flask, request
from flask import send_file, send_from_directory
from werkzeug.utils import secure_filename
from util import Util, Constants, Log, CodeReturn
from controller import Controller

log = Log('BalanceRoute')
util = Util()
constants = Constants()
balanceDAO = BalanceDAO()
controller = Controller()
codeReturn = CodeReturn()

class BalanceRoute:
    
    def get_balance(self, request):
        try:
            header = request.headers

            #Get Token from Header
            token = str(header['token'])
            #Get datas from PARAMS
            data = json.loads(str(request.args.get('data')).replace("'", '"'))

            companies_token = data['companies_token']
            date_start = data['date_start']
            date_end = data['date_end']
        except:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

        #Authentication
        decode_auth_token = controller.decode_auth_token(token)

        if(decode_auth_token == codeReturn.EXPIRED_TOKEN_CODE):
            log.warning(inspect.getframeinfo(inspect.currentframe()).function, 
                        str(codeReturn.EXPIRED_TOKEN_MSG), 
                        0)

            return util.make_json(codeReturn.EXPIRED_TOKEN_CODE, codeReturn.EXPIRED_TOKEN_MSG, [])
        elif(decode_auth_token == codeReturn.INVALID_TOKEN_CODE):
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(codeReturn.INVALID_TOKEN_MSG), 
                      0)

            return util.make_json(codeReturn.INVALID_TOKEN_CODE, codeReturn.INVALID_TOKEN_MSG, [])
        else:
            companies_id = util.companies_token_to_id(companies_token)

            code, msg, data = balanceDAO.get_balance_comparative(date_start, date_end, companies_id, decode_auth_token)

            return util.make_json(code, msg, data)
    
    def delete_balance(self, request):
        if request.is_json:
            try:
                header = request.headers

                #Get Token from Header
                token = str(header['token'])
                #Get datas from PARAMS
                data = json.loads(str(request.args.get('data')).replace("'", '"'))

                company_token = data['company_token']
            except:
                return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

            #Authentication
            decode_auth_token = controller.decode_auth_token(token)

            if(decode_auth_token == codeReturn.EXPIRED_TOKEN_CODE):
                log.warning(inspect.getframeinfo(inspect.currentframe()).function, 
                            str(codeReturn.EXPIRED_TOKEN_MSG), 
                            0)

                return util.make_json(codeReturn.EXPIRED_TOKEN_CODE, codeReturn.EXPIRED_TOKEN_MSG, [])
            elif(decode_auth_token == codeReturn.INVALID_TOKEN_CODE):
                log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                        str(codeReturn.INVALID_TOKEN_MSG), 
                        0)

                return util.make_json(codeReturn.INVALID_TOKEN_CODE, codeReturn.INVALID_TOKEN_MSG, [])
            else:
                company_id = controller.decode_token(company_token)

                code, msg, data = balanceDAO.delete_balance(company_id, decode_auth_token)

                if(code == codeReturn.UNKNOW_ERROR_CODE):
                    return util.make_json(code, msg, data)

                code, msg, data = balanceDAO.delete_balance_info(company_id, decode_auth_token)

                if(code == codeReturn.UNKNOW_ERROR_CODE):
                    return util.make_json(code, msg, data)

                return util.make_json(code, msg, data)
        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

    def list_acc_balance_comparative(self, request):
        try:
            header = request.headers

            #Get Token from Header
            token = str(header['token'])
            #Get datas from JSON
            data = json.loads(str(request.args.get('data')).replace("'", '"'))

            companies_token = data['companies_token']
            date_start = data['date_start']
            date_end = data['date_end']
            cod_account_ref = data['cod_account_ref']
        except:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

        #Authentication
        decode_auth_token = controller.decode_auth_token(token)

        if(decode_auth_token == codeReturn.EXPIRED_TOKEN_CODE):
            log.warning(inspect.getframeinfo(inspect.currentframe()).function, 
                        str(codeReturn.EXPIRED_TOKEN_MSG), 
                        0)

            return util.make_json(codeReturn.EXPIRED_TOKEN_CODE, codeReturn.EXPIRED_TOKEN_MSG, [])
        elif(decode_auth_token == codeReturn.INVALID_TOKEN_CODE):
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(codeReturn.INVALID_TOKEN_MSG), 
                      0)

            return util.make_json(codeReturn.INVALID_TOKEN_CODE, codeReturn.INVALID_TOKEN_MSG, [])
        else:
            companies_id = util.companies_token_to_id(companies_token)

            code, msg, data = balanceDAO.list_acc_balance_comparative(date_start, date_end, cod_account_ref, companies_id, decode_auth_token)

            return util.make_json(code, msg, data)
