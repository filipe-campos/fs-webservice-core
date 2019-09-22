# -*- coding: utf-8 -*-
import logging
import os
import json
import traceback
import inspect

from db import NikoleDAO
from flask import Flask, request
from flask import send_file, send_from_directory
from werkzeug.utils import secure_filename
from util import Util, Constants, Log, CodeReturn
from controller import Controller

log = Log('NikoleRoute')
util = Util()
constants = Constants()
nikoleDAO = NikoleDAO()
controller = Controller()
codeReturn = CodeReturn()


class NikoleRoute:
    
    def list_message(self, request):
        try:
            header = request.headers

            #Get Token from Header
            token = str(header['token'])
            #Get datas from PARAMS
            data = json.loads(str(request.args.get('data')).replace("'", '"'))

            companies_token = data['companies_token']
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

            code, msg, data = nikoleDAO.get_messages(companies_id, decode_auth_token)

            return util.make_json(code, msg, data)


    def read_message(self, request):
        if (request.is_json):
            try:
                content = request.get_json()
                header = request.headers

                #Get Token from Header
                token = str(header['token'])
                #Get datas from PARAMS
                data = json.loads(str(content['data']).replace("'",'"'))

                msg_id = data['msg_id']
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
                code, msg, data = nikoleDAO.read_message(msg_id, decode_auth_token)

                return util.make_json(code, msg, data)

    