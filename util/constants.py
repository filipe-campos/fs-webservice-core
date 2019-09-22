# -*- coding: utf-8 -*-

class Constants:
    def __init__(self):
        #Redis Configuration
        self.REDIS_HOST = 'localhost'
        self.REDIS_PORT = '6379'
        self.REDIS_URL = 'redis://'+self.REDIS_HOST+':'+str(self.REDIS_PORT)

        #Nikole Message Status
        self.NIKOLE_MSG_STATUS_NOT_READ = 0

        #Files Upload Configuration
        self.UPLOAD_FOLDER = 'files/upload/'
        self.UPLOAD_TEMP_FILE_FOLDER = self.UPLOAD_FOLDER+'temp_files/'

        #Company Image Configuration
        self.STATIC_FOLDER = 'static/'

        #Type of Dates
        self.DATE_START_TYPE = 'START'
        self.DATE_END_TYPE = 'END'
        self.DATE_ZERO_TYPE = 'ZERO'

        #Account Ref 
        self.ACC_REF_HAS_NO_FATHER = '0'
        self.CLASSIFICATION_ACC_RESULT = ['5', '7', '9', '11', '13', '15']

        #Auth URL
        self.AUTH_HOST = 'http://127.0.0.1'
        self.AUTH_PORT = '5001'
        self.AUTH_PATH = '/dashboard/api/v1.0/auth'
        self.AUTH_URL = self.AUTH_HOST+':'+str(self.AUTH_PORT) + self.AUTH_PATH

        self.AUTH_URL_CHECK_TOKEN = self.AUTH_URL + '/token/check'
