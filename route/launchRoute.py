# -*- coding: utf-8 -*-
import os
import json
import traceback
import inspect

from db import LaunchDAO, MovimentationDAO
from flask import Flask, request
from flask import send_file, send_from_directory
from werkzeug.utils import secure_filename
from util import Util, Constants, Log, CodeReturn
from controller import Controller

log = Log('LaunchRoute')
util = Util()
constants = Constants()
launchDAO = LaunchDAO()
movimentationDAO = MovimentationDAO()
controller = Controller()
codeReturn = CodeReturn()


class LaunchRoute:
    
    def list_launch(self, request):
        try:
            header = request.headers

            #Get Token from Header
            token = str(header['token'])
            #Get datas from PARAMS
            data = json.loads(str(request.args.get('data')).replace("'", '"'))

            companies_token = data['companies_token']
            date = data['date']
            cod_account = data['cod_account']
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

            code, msg, data = launchDAO.list_launch(date, cod_account, companies_id, decode_auth_token)

            return util.make_json(code, msg, data)

    def list_launch_period(self, request):
        try:
            header = request.headers

            #Get Token from Header
            token = str(header['token'])
            #Get datas from PARAMS
            data = json.loads(str(request.args.get('data')).replace("'", '"'))

            companies_token = data['companies_token']
            date_start = data['date_start']
            date_end = data['date_end']
            cod_account = data['cod_account']
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

            code, msg, data = launchDAO.list_launch_period(date_start, date_end, cod_account, companies_id, decode_auth_token)

            return util.make_json(code, msg, data)

    def insert_launch(self, request):
        if request.is_json:
            try:
                content = request.get_json()
                header = request.headers

                #Get Token from Header
                token = str(header['token'])
                #Get datas from JSON
                data = json.loads(str(content['data']).replace("'",'"'))

                company_token = data['company_token']

                launch_list = data['launch_list']
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

                for launch in launch_list:
                    insert_return = accountDAO.insert_account(account, company_id, decode_auth_token)  

                    code, msg, data = launchDAO.insert_launch(launch, company_id, decode_auth_token)
   
                return util.make_json(code, msg, data)
        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

    def delete_launch(self, request):
        try:
            date = None
            content = request.get_json()
            header = request.headers

            #Get Token from Header
            token = str(header['token'])
            
            company_token = data['company_token']

            if('date' in content):
                date = str(content['date'])
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

            if(date == None):
                #launchDAO.delete_all_launch(company_id, decode_auth_token)
                #movimentationDAO.delete_all_movimentation(company_id, decode_auth_token)
                pass
            else:
                #launchDAO.delete_launch_month(date, company_id, decode_auth_token)
                #movimentationDAO.delete_month_movimentation(date, company_id, decode_auth_token)
                pass

            return util.make_json(codeReturn.SUCCESS_CODE, codeReturn.SUCCESS_MSG, [])
