# -*- coding: utf-8 -*-
import logging
import os
import json
import traceback
import inspect


from db import AccountDAO, MovimentationDAO, LaunchDAO, BalanceDAO
from flask import Flask, request
from flask import send_file, send_from_directory
from werkzeug.utils import secure_filename
from util import Util, Constants, Log, CodeReturn
from controller import Controller

log = Log('AccountRoute')
util = Util()
constants = Constants()
accountDAO = AccountDAO()
launchDAO = LaunchDAO()
balanceDAO = BalanceDAO()
controller = Controller()
codeReturn = CodeReturn()


class AccountRoute:

    def list_account(self, request):
        try:
            content = request.get_json()
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

            code, msg, data = accountDAO.list_account(company_id, decode_auth_token)

            return util.make_json(code, msg, data)

    def insert_account(self, request):
        if request.is_json:
            try:
                content = request.get_json()
                header = request.headers

                #Get Token from Header
                token = str(header['token'])
                #Get datas from JSON
                data = json.loads(str(content['data']).replace("'",'"'))

                company_token = data['company_token']
                
                if ('account_list' not in data) and ('account' not in data):
                    return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])
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

                if 'account_list' in data:
                    for account in data['account_list']:
                        code, msg, data = accountDAO.insert_account(account, company_id, decode_auth_token)  
                elif 'account' in data:
                    code, msg, data = accountDAO.insert_account(data["account"], company_id, decode_auth_token)  

                return util.make_json(code, msg, data)

        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])


    def delete_all_account(self, request):
        if request.is_json:
            try:
                content = request.get_json()
                header = request.headers

                #Get Token from Header
                token = str(header['token'])
                #Get datas from JSON
                data = json.loads(str(content['data']).replace("'",'"'))

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

                code, msg, data = accountDAO.delete_all_acc(company_id, decode_auth_token)

                return util.make_json(code, msg, data)
        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])


    def update_acc_relationship(self, request):
        if (request.is_json):
            try:
                content = request.get_json()
                header = request.headers

                #Get Token from Header
                token = str(header['token'])
                #Get datas from JSON
                data = json.loads(str(content['data']).replace("'",'"'))

                company_token = data['company_token']
                cod_account = data['cod_account']
                classification_ref = data['classification_ref']
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

                code, msg, data = accountDAO.update_acc_relationship(cod_account, classification_ref, company_id, decode_auth_token)

                return util.make_json(code, msg, data)
        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

    def clear_acc_relationship(self, request):
        if (request.is_json):
            try:
                content = request.get_json()
                header = request.headers

                #Get Token from Header
                token = str(header['token'])
                #Get datas from JSON
                data = json.loads(str(content['data']).replace("'",'"'))

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

                code, msg, data = accountDAO.clear_acc_relationship(company_id, decode_auth_token)

                return util.make_json(code, msg, data)         
        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

    
