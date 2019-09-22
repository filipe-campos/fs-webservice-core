# -*- coding: utf-8 -*-
import logging
import os
import json
import traceback
import inspect


from db import CompanyDAO, FileLayoutDAO
from flask import Flask, request
from flask import send_file, send_from_directory
from werkzeug.utils import secure_filename
from util import Util, Constants, Log, CodeReturn
from controller import Controller

log = Log('CompanyRoute')
util = Util()
constants = Constants()
companyDAO = CompanyDAO()
fileLayoutDAO = FileLayoutDAO()
controller = Controller()
codeReturn = CodeReturn()


class CompanyRoute:

    def get_company_info(self, request):
        try:
            header = request.headers

            # Get Token from Header
            token = str(header['token'])
            # Get datas from PARAMS
            data = json.loads(str(request.args.get('data')).replace("'", '"'))

            company_token = data['company_token']
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function,
                      str(traceback.format_exc()),
                      0)
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

        # Authentication
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

            code, msg, data = companyDAO.get_company_info(company_id, decode_auth_token)

            return util.make_json(code, msg, data)

    def update_company_info(self, request):
        if (request.is_json):
            try:
                content = request.get_json()
                header = request.headers

                # Get Token from Header
                token = str(header['token'])
                # Get datas from JSON
                data = json.loads(str(content['data']).replace("'", '"'))

                company_token = data['company_token']
            except:
                return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

            # Authentication
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

                code, msg, data = companyDAO.update_company_info(data['info'], company_id, decode_auth_token)

                return util.make_json(code, msg, data)
        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

    def insert_company(self, request):
        if (request.is_json):
            try:
                content = request.get_json()
                header = request.headers

                # Get Token from Header
                token = str(header['token'])
                # Get datas from JSON
                data = json.loads(str(content['data']).replace("'", '"'))
                info = data['info']
            except:
                return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

            # Authentication
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
                code, msg, data = companyDAO.insert_company(info, decode_auth_token)

                return util.make_json(code, msg, data)
        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

    ## -- FILE LAYOUT -- ##
    def list_file_layout(self, request):
        try:
            header = request.headers

            # Get Token from Header
            token = str(header['token'])
            # Get datas from PARAMS
            data = json.loads(str(request.args.get('data')).replace("'", '"'))

            company_token = data['company_token']
            import_type = data['import_type']
        except:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

        # Authentication
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

            code, msg, data = fileLayoutDAO.get_file_layout(import_type, company_id, decode_auth_token)

            return util.make_json(code, msg, data)

    def insert_file_layout(self, request):
        if (request.is_json):
            try:
                content = request.get_json()
                header = request.headers

                # Get Token from Header
                token = str(header['token'])
                # Get datas from JSON
                data = json.loads(str(content['data']).replace("'", '"'))

                company_token = data['company_token']
            except:
                return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

            # Authentication
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

                code, msg, data = fileLayoutDAO.insert_file_layout(data, company_id, decode_auth_token)

                return util.make_json(code, msg, data)

        else:
            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

    def delete_file_layout(self, request):
        try:
            content = request.get_json()
            header = request.headers

            # Get Token from Header
            token = str(header['token'])
            # Get datas from JSON
            data = json.loads(str(content['data']).replace("'", '"'))

            layout_id = data['layout_id']
        except:
            log.error(inspect.getframeinfo(inspect.currentframe()).function, 
                      str(traceback.format_exc()), 
                      0)

            return util.make_json(codeReturn.BAD_REQUEST_CODE, codeReturn.BAD_REQUEST_MSG, [])

        # Authentication
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
            code, msg, data = fileLayoutDAO.delete_file_layout(layout_id, decode_auth_token)

            return util.make_json(code, msg, data)

