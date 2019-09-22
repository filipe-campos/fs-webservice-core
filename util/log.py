# -*- coding: utf-8 -*-
import coloredlogs
import logging
import datetime

from pymongo import MongoClient
from .constants import Constants
from configuration import Configuration
from util import Util

constants = Constants()
configuration = Configuration()
util = Util()

mongo_client = MongoClient('mongodb://'+str(configuration.MONGO_DB_FS)+':'
                          +str(configuration.MONGO_PASS)+'@'+str(configuration.MONGO_HOST)+':'
                          +str(configuration.MONGO_PORT)+'/', connect=False)

mongo_db = mongo_client[configuration.MONGO_DB_FS]
mongo_collection = mongo_db[configuration.MONGO_DB_FS_LOG]

class Log:

    def __init__(self, class_config):
        self.class_config = class_config

        #Log Configuration
        util.setup_logging()

        self.logger = logging.getLogger(class_config)
        logging.basicConfig()
        self.logger.setLevel(logging.INFO)

        self.DEBUG = True

    def info(self, method, msg, company_id):
        if self.DEBUG:
            self.logger.info('['+method+'] '+msg)

    def warning(self, method, msg, company_id):
        if self.DEBUG:
            self.logger.warning('['+method+'] '+msg)

        self.insert_collection("warning", method, msg, company_id)

    def error(self, method, msg, company_id):
        self.logger.error('['+method+'] '+msg)

        self.insert_collection("error", method, msg, company_id)

    def insert_collection(self, type, method, msg, user_id):
        log_insert = {
              "date_log": datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
              "type": type,
              "class": self.class_config,
              "method": method,
              "msg": msg,
              "user_id": user_id
        }

        try:
            mongo_collection.insert_one(log_insert)
        except Exception as e:
            self.logger.error('Erro ao inserir Log no MongoDB.')
            self.logger.error(e)
