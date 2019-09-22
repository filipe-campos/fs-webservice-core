# -*- coding: utf-8 -*-
import logging
import os
import json
import traceback
import inspect


from flask import Flask, request
from util import Util, Constants, Log, CodeReturn
from controller import Controller
from db import UserDAO

log = Log('UserRoute')
util = Util()
constants = Constants()
controller = Controller()
codeReturn = CodeReturn()
userDAO = UserDAO()

class UserRoute:
    
    def insert_user(self, request):
        if (request.is_json):
            content = request.get_json()

            try:
                #Get datas from JSON 
                data = json.loads(str(content['data']).replace("'", '"'))

                info = data['info']
            except:
                return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])
 
            code, msg, data = userDAO.insert_user(info)

            return util.make_json(code, msg, data)
                    
        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

    def get_user_info(self, request):
        try:
            header = request.headers

            #Get Token from Header
            token = str(header['token'])
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
            code, msg, data = userDAO.get_user_info(decode_auth_token)

            return util.make_json(code, msg, data)

    def update_user_info(self, request):
        if (request.is_json):
            content = request.get_json()
            header = request.headers

            try:
                #Get Token from Header
                token = str(header['token'])
                #Get datas from JSON
                data = json.loads(str(content['data']).replace("'",'"'))

                info = data['info']
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
                code, msg, data = userDAO.update_user_info(info, decode_auth_token)
                
                return util.make_json(code, msg, data)
        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

    def update_user_pass(self, request):
        if (request.is_json):
            try:
                content = request.get_json()
                header = request.headers

                #Get Token from Header
                token = str(header['token'])
                #Get datas from JSON
                data = json.loads(str(content['data']).replace("'",'"'))

                old_password = data['old_password']
                new_password = data['new_password']
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
                code, msg, data = userDAO.update_user_pass(old_password, new_password, decode_auth_token)

                return util.make_json(code, msg, data)
        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

    def list_user_companies(self, request):
        try:
            header = request.headers

            #Get Token from Header
            token = str(header['token'])
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
            code, msg, data = userDAO.list_user_companies(decode_auth_token)

            return util.make_json(code, msg, data)
