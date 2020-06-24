# -*- coding: utf-8 -*-
import datetime

import pandas as pd

import python.imports.utils as utils
from python.imports.init_params import *

_date_fmt1 = '%Y/%m/%d'
_date_fmt2 = '%Y-%m-%d'


def search_account(legal_name, bct_host, bct_token):
    return utils.call('clientAccountSearch', {
        'legalName': legal_name
    }, 'reference-data-service', bct_host, bct_token)


def search_bank(legal_name, bct_host, bct_token):
    return utils.call('refBankAccountSearch', {
        'legalName': legal_name
    }, 'reference-data-service', bct_host, bct_token)


def process_bank_cash(legal_name, cash_amount, transfer_date, bct_host, bct_token):
    account_info = search_account(legal_name, bct_host, bct_token)
    print(account_info[0])

    bank_info = search_bank(legal_name, bct_host, bct_token)
    print(bank_info[0])

    bank_account = bank_info[0]['bankAccount']
    print(bank_account)

    margin = account_info[0]['margin']  # 保证金
    print('margin: ' + str(margin))

    cash = account_info[0]['cash']  # 现金余额
    print('cash: ' + str(cash))

    debt = account_info[0]['debt']  # 负债
    print('debt: ' + str(debt))

    credit = account_info[0]['credit']  # 授信总额
    print('credit: ' + str(credit))

    credit_used = account_info[0]['creditUsed']  # 已用授信额度
    print('credit(used): ' + str(credit_used))

    if cash_amount > 0:
        try:
            account_info = utils.call('cliFundEventSave', {
                "clientId": legal_name,
                "bankAccount": bank_account,
                "paymentAmount": cash_amount,
                "paymentDate": transfer_date,
                "paymentDirection": "IN",
                "accountDirection": "PARTY"
            }, 'reference-data-service', bct_host, bct_token)
            print(account_info)
        except Exception as  e:
            print(repr(e))
    else:
        try:
            if abs(cash_amount) > cash:
                record_info = utils.call('clientSaveAccountOpRecord', {
                    'accountOpRecord': {
                        "accountId": legal_name + '0',
                        "legalName": legal_name,
                        "event": 'TRADE_CASH_FLOW',
                        "status": 'NORMAL',
                        "cashChange": abs(cash_amount + cash),
                        "debtChange": abs(cash_amount + cash)
                    }
                }, 'reference-data-service', bct_host, bct_token)
                print(record_info)
            account_info = utils.call('cliFundEventSave', {
                "clientId": legal_name,
                "bankAccount": bank_account,
                "paymentAmount": abs(cash_amount),
                "paymentDate": transfer_date,
                "paymentDirection": "OUT",
                "accountDirection": "PARTY"
            }, 'reference-data-service', bct_host, bct_token)
            print(account_info)
        except Exception as  e:
            print(repr(e))


def get_bct_bank_accounts(ip, headers):
    bank_account_result = utils.call_request(ip, 'reference-data-service', 'refBankAccountSearch', {}, headers)
    bank_accounts = {}
    for bankAccount in bank_account_result:
        bank_accounts[bankAccount['legalName']] = bankAccount['bankAccount']
    return bank_accounts


def get_bct_legal_name(ip, headers):
    parties = utils.call_request(ip, 'reference-data-service', 'refPartyList', {}, headers)
    party_names = {}
    for party in parties:
        party_names[party['legalName']] = party['legalName']
    return party_names


def create_bank_account(bank_name, legal_name, bank_account, bank_account_name, bank_account_status_enum,
                        payment_system_code, ip, headers):
    bank_account = {
        'bankName': bank_name,  ##开户行名称
        'legalName': legal_name,
        'bankAccount': bank_account,  ##银行账户
        'bankAccountName': bank_account_name,  ##银行账户名称
        'bankAccountStatusEnum': bank_account_status_enum,  ##启用/停用
        'paymentSystemCode': payment_system_code  ##支付系统行号
    }
    utils.call_request(ip, 'reference-data-service', 'refBankAccountSave', bank_account, headers)


def import_fund_record(ip, headers):
    print("出入金导入开始")
    xinhu_funds = pd.read_csv(import_fund_excel_file, encoding="gbk").to_dict(orient='records')
    legal_names = get_bct_legal_name(ip, headers)
    bank_accounts = get_bct_bank_accounts(ip, headers)
    out_fund = []
    in_fund = []
    for fund in xinhu_funds:
        xinhu_legal_name = fund['Client']
        legal_name = legal_names.get(xinhu_legal_name)
        if not legal_name:
            print("系统不存在:{xinhu_legal_name}".format(xinhu_legal_name=xinhu_legal_name))
            continue
        bank_account = bank_accounts.get(legal_name)
        if not bank_accounts.get(legal_name):
            create_bank_account(legal_name + '开户行名称', legal_name, legal_name + '银行账号', legal_name + '银行账户名称', 'NORMAL',
                                legal_name + '支付系统行号', ip, headers)
        payment_date = datetime.datetime.strptime(fund['Date'], _date_fmt1)
        payment_date = datetime.datetime.strftime(payment_date, _date_fmt2)
        in_payment_amount = fund['CashInFlow']
        out_payment_amount = fund['CashOutFlow']
        try:
            in_fund.append({
                'clientId': legal_name,
                'bankAccount': bank_account,
                'paymentAmount': abs(in_payment_amount),
                'paymentDate': payment_date,
                'paymentDirection': "IN",
                'accountDirection': 'PARTY'
            })
            out_fund.append({
                'clientId': legal_name,
                'bankAccount': bank_account,
                'paymentAmount': abs(out_payment_amount),
                'paymentDate': payment_date,
                'paymentDirection': "OUT",
                'accountDirection': 'PARTY'
            })
        except Exception as e:
            print('交易对手{legal_name},出入金未知异常  mes: {error}'.format(legal_name=legal_name, error=repr(e)))
    for in_item in in_fund:
        print(in_item)
        # save_fund_info(in_item, ip, headers)
        process_bank_cash(in_item['clientId'], in_item['paymentAmount'], in_item['paymentDate'], ip, headers)

    for out_item in out_fund:
        print(out_item)
        # save_fund_info(out_item, ip, headers)
        process_bank_cash(out_item['clientId'], -1 * out_item['paymentAmount'], out_item['paymentDate'], ip, headers)

    print("出入金导入结束")


if __name__ == '__main__':
    headers = utils.login(bct_login_ip, bct_login_body)
    import_fund_record(bct_login_ip, headers)
