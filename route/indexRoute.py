# -*- coding: utf-8 -*-
import logging
import os
import json
import traceback
import inspect

from db import IndexDAO
from flask import Flask, request
from flask import send_file, send_from_directory
from werkzeug.utils import secure_filename
from util import Util, Constants, Log, CodeReturn
from controller import Controller

log = Log('IndexRoute')
util = Util()
constants = Constants()
indexDAO = IndexDAO()
controller = Controller()
codeReturn = CodeReturn()


class IndexRoute:
    
    def get_index_comparative(self, request):
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
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      0)
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

            code, msg, data = indexDAO.get_index_comparative(date_start, date_end, companies_id, decode_auth_token)
            
            return util.make_json(code, msg, data)
       