# -*- coding: utf-8 -*-
import logging
import os
import json
import traceback
import inspect

from db import MovimentationDAO
from flask import Flask, request
from flask import send_file, send_from_directory
from werkzeug.utils import secure_filename
from util import Util, Constants, Log, CodeReturn
from controller import Controller

log = Log('MovimentationRoute')
util = Util()
constants = Constants()
movimentationDAO = MovimentationDAO()
controller = Controller()
codeReturn = CodeReturn()


class MovimentationRoute:
    
    def calc_movimentation(self, request):
        try:
            content = request.get_json()
            header = request.headers

            #Get Token from Header
            token = str(header['token'])
            #Get datas from JSON
            data = json.loads(str(content['data']).replace("'",'"'))

            company_token = data['company_token']
            date = data['date']
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

            code, msg, data = movimentationDAO.calculate_movimentation(date, company_id, decode_auth_token)

            return util.make_json(code, msg, data)