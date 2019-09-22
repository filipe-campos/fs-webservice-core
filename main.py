#!flask/bin/python
# -*- coding: utf-8 -*-
from route import *
from util import Constants, Util

from flask_cors import CORS
from flask import Flask, request, Response
from flask_mysqldb import MySQL

mySQL = MySQL()

constants = Constants()
util = Util()

#Routes
accountRoute = AccountRoute()
dreRoute = DRERoute()
launchRoute = LaunchRoute()
movimentationRoute = MovimentationRoute()
balanceRoute = BalanceRoute()
dfcRoute = DFCRoute()
ebitdaRoute = EbitdaRoute()
indexRoute = IndexRoute()
companyRoute = CompanyRoute()
analyticRoute = AnalyticRoute()
nikoleRoute = NikoleRoute()
accountingRoute = AccountingRoute()
userRoute = UserRoute()

#Flask Configuration
app = Flask(__name__)
CORS(app)

#MySQL Configuration
app.config['MYSQL_HOST'] = constants.MYSQL_HOST
app.config['MYSQL_PORT'] = constants.MYSQL_PORT
app.config['MYSQL_USER'] = constants.MYSQL_USER
app.config['MYSQL_PASSWORD'] = constants.MYSQL_PASS
app.config['MYSQL_DB'] = constants.MYSQL_DB

mySQL.init_app(app)

#Celery Configuration
app.config['CELERY_RESULT_BACKEND'] = constants.REDIS_URL
app.config['CELERY_BROKER_URL'] = constants.REDIS_URL

celery = util.make_celery(app)

## -- CONTAS -- ##
@app.route('/dashboard/api/v1.0/account/insert', methods=['POST'])
def insert_account():
    return Response(accountRoute.insert_account(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/account/list', methods=['GET'])
def list_account():
    return Response(accountRoute.list_account(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/account/delete/all', methods=['DELETE'])
def delete_all_acocunt():
    return Response(accountRoute.delete_all_account(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/account/relationship/update', methods=['PUT'])
def update_acc_relationship():
    return Response(accountRoute.update_acc_relationship(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/account/relationship/clear', methods=['PUT'])
def clear_acc_relationship():
    return Response(accountRoute.clear_acc_relationship(request), mimetype='application/json')




## -- MÓDULO CONTÁBIL -- ##

## -- RESUMO -- ##
@app.route('/dashboard/api/v1.0/accounting/resume', methods=['GET'])
def get_resume():
    return Response(accountingRoute.get_resume(request), mimetype='application/json')

## -- DRE -- ##
@app.route('/dashboard/api/v1.0/accounting/dre/comparative', methods=['GET'])
def dre_comparative():
    return Response(dreRoute.get_dre_comparative(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/dre/month', methods=['GET'])
def dre_month():
    return Response(dreRoute.get_dre_month(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/dre/period', methods=['GET'])
def dre_periodo():
    return Response(dreRoute.get_dre_period(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/dre/acc/mov', methods=['GET'])
def list_acc_mov_from_acc_ref():
    return Response(dreRoute.list_acc_mov_from_acc_ref(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/dre/acc/mov/period', methods=['GET'])
def list_acc_mov_from_acc_ref_period():
    return Response(dreRoute.list_acc_mov_from_acc_ref_period(request), mimetype='application/json')    



## -- BALANÇO -- ##
@app.route('/dashboard/api/v1.0/accounting/balance/comparative', methods=['GET'])
def get_balance():
    return Response(balanceRoute.get_balance(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/balance/initial/delete', methods=['DELETE'])
def delete_balance():
    return Response(balanceRoute.delete_balance(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/balance/acc', methods=['GET'])
def list_acc_balance():
    return Response(balanceRoute.list_acc_balance_comparative(request), mimetype='application/json')



##  -- LANÇAMENTOS -- ##
@app.route('/dashboard/api/v1.0/accounting/launch/list', methods=['GET'])
def list_launch():
    return Response(launchRoute.list_launch(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/launch/list/period', methods=['GET'])
def list_launch_period():
    return Response(launchRoute.list_launch_period(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/launch/insert', methods=['POST'])
def insert_launch():
    return Response(launchRoute.insert_launch(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/launch/delete', methods=['DELETE'])
def delete_launch():
    return Response(launchRoute.delete_launch(request), mimetype='application/json')



## -- DFC -- ##
@app.route('/dashboard/api/v1.0/accounting/dfc/month', methods=['GET'])
def dfc_mensal():
    return Response(dfcRoute.get_dfc(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/dfc/accumulated', methods=['GET'])
def dfc_acumulado():
    return Response(dfcRoute.get_dfc_accumulated(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/accounting/dfc/profit/insert', methods=['POST'])
def insert_dfc_profit():
    return dfcRoute.insert_dfc_profit(request)

@app.route('/dashboard/api/v1.0/accounting/dfc/exercise/insert', methods=['POST'])
def insert_dfc_exercise():
    return dfcRoute.insert_dfc_exercise(request)

@app.route('/dashboard/api/v1.0/accounting/dfc/info/list', methods=['GET'])
def list_dfc_info():
    return Response(dfcRoute.list_dfc_info(request), mimetype='application/json')


## -- EBITDA -- ##
@app.route('/dashboard/api/v1.0/accounting/ebitda/period', methods=['GET'])
def ebitda_period():
    return Response(ebitdaRoute.get_ebitda_month(request), mimetype='application/json')



## -- ÍNDICES -- ##
@app.route('/dashboard/api/v1.0/accounting/index/comparative', methods=['GET'])
def index():
    return Response(indexRoute.get_index_comparative(request), mimetype='application/json')



## -- MOVIMENTAÇÃO -- ##
@app.route('/dashboard/api/v1.0/movimentation/calc', methods=['POST'])
def calc_movimentation():
    return Response(movimentationRoute.calc_movimentation(request), mimetype='application/json')



## -- COMPANY -- ##
@app.route('/dashboard/api/v1.0/company/insert', methods=['POST'])
def insert_company():
    return Response(companyRoute.insert_company(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/company/info', methods=['GET'])
def get_company_info():
    return Response(companyRoute.get_company_info(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/company/info/update', methods=['PUT'])
def update_company_info():
    return Response(companyRoute.update_company_info(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/company/file_layout/list', methods=['GET'])
def list_file_layout():
    return Response(companyRoute.list_file_layout(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/company/file_layout/insert', methods=['POST'])
def insert_file_layout():
    return Response(companyRoute.insert_file_layout(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/company/file_layout/delete', methods=['DELETE'])
def delete_file_layout():
    return Response(companyRoute.delete_file_layout(request), mimetype='application/json')



## -- USER -- ##
@app.route('/dashboard/api/v1.0/user/insert', methods=['POST'])
def insert_user():
    return Response(userRoute.insert_user(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/user/info/get', methods=['GET'])
def get_user_info():
    return Response(userRoute.get_user_info(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/user/info/update', methods=['PUT'])
def update_user_info():
    return Response(userRoute.update_user_info(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/user/password/update', methods=['POST'])
def update_user_pass():
    return Response(userRoute.update_user_pass(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/user/companies/list', methods=['GET'])
def list_user_companies():
    return Response(userRoute.list_user_companies(request), mimetype='application/json')



## -- ANALYTICS -- ##
@app.route('/dashboard/api/v1.0/analytic/acc/ref/movimentation', methods=['GET'])
def list_acc_ref_movimentation():
    return Response(analyticRoute.list_acc_ref_movimentation(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/analytic/acc/ref/balance', methods=['GET'])
def list_acc_ref_balance():
    return Response(analyticRoute.list_acc_ref_balance(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/analytic/serie/list', methods=['GET'])
def list_serie():
    return Response(analyticRoute.list_serie(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/analytic/serie/data/list', methods=['GET'])
def list_serie_data():
    return Response(analyticRoute.list_serie_data(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/analytic/roi', methods=['GET'])
def get_roi():
    return Response(analyticRoute.get_roi(request), mimetype='application/json')



## -- NIKOLE -- ##
@app.route('/dashboard/api/v1.0/nikole/message/list', methods=['GET'])
def messages():
    return Response(nikoleRoute.list_message(request), mimetype='application/json')

@app.route('/dashboard/api/v1.0/nikole/message/read', methods=['POST'])
def read_msg():
    return Response(nikoleRoute.read_message(request), mimetype='application/json')


if __name__ == '__main__':
    #app.run(host='dashboard.fiscoserv.com.br', port=5002, ssl_context=('fullchain.pem', 'privkey.pem'), debug=False)
    app.run(host='127.0.0.1', port=5002)
    #app.run(host='10.0.0.187', port=5002)
