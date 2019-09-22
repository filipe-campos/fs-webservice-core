# -*- coding: utf-8 -*-

class CodeReturn:
    def __init__(self):
        #SUCCESS
        self.SUCCESS_CODE = 200
        self.SUCCESS_MSG = 'Requisição realizada com sucesso.'

        #ERROR
        self.BAD_REQUEST_CODE = 400
        self.BAD_REQUEST_MSG = 'Informações enviadas pelo cliente inválidas.'

        self.INVALID_TOKEN_CODE = 401
        self.INVALID_TOKEN_MSG = 'Token inválido.'

        self.EXPIRED_TOKEN_CODE = 402
        self.EXPIRED_TOKEN_MSG = 'Token expirado.'

        self.FORBIDEN_CODE = 403
        self.FODBIDEN_MSG = 'O cliente não tem permissão de acesso ao recurso solicitado.'

        self.WRONG_LOGIN_CODE = 405
        self.WRONG_LOGIN_MSG = 'Usuário ou Senha incorretos.'

        self.ERROR_DATA_BASE_CODE = 406
        self.ERROR_DATA_BASE_MSG = 'Erro ao executar comando MySQL.'

        self.REGISTERED_USER_CODE = 407
        self.REGISTERED_USER_MSG = 'Usuário já cadastrado.'

        self.REGISTERED_EMAIL_CODE = 408
        self.REGISTERED_EMAIL_MSG = 'E-mail já cadastrado.'

        self.NOT_FOUND_USER_CODE = 409
        self.NOT_FOUND_USER_MSG = 'Usuário já cadastrado.'

        self.OBJECT_NOT_FOUND_CODE = 410
        self.OBJECT_NOT_FOUND_MSG = 'Objeto não encontrado.'

        self.INSERT_ALREADY_EXISTS_CODE = 411
        self.INSERT_ALREADY_EXISTS_MSG = 'Objeto já existe. Update realizado com sucesso'

        self.BALANCE_INFO_NOT_FOUND_CODE = 412
        self.BALANCE_INFO_NOT_FOUND_MSG = 'Data do saldo inicial da empresa não encontrado.'

        self.UNKNOW_ERROR_CODE = 500
        self.UNKNOW_ERROR_MSG = 'Erro desconhecido no servidor.'

        self.UNAVALIABLE_SERVICE_CODE = 503
        self.UNAVALIABLE_SERVICE_MSG = 'Serviço fora do ar para manutenção.'
