# -*- coding: utf-8 -*-
import jwt
import datetime
import requests
import json

from util import Constants, Log, CodeReturn
from configuration import Configuration

constants = Constants()
configuration = Configuration()  
codeReturn = CodeReturn()
log = Log('Controller')


class Controller:

    def __init__(self):
        pass

    '''
        HIDDEN CODE
    '''