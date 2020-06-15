# -*- coding: utf-8 -*-
import python.imports.utils as utils
from python.imports.init_params import *


def save_fund_info(fund_event, ip, headers):
    try:
        result = utils.call_request(ip, 'reference-data-service', 'cliFundEventSave', fund_event, headers)
        return result
    except Exception as  e:
        print(repr(e))


def get_bct_bank_accounts(ip, headers):
    bank_account_result = utils.call_request(ip, 'reference-data-service', 'refBankAccountSearch', {}, headers)[
        'result']
    bank_accounts = {}
    for bankAccount in bank_account_result:
        bank_accounts[bankAccount['legalName']] = bankAccount['bankAccount']
    return bank_accounts


def get_bct_legal_name(ip, headers):
    parties = utils.call_request(ip, 'reference-data-service', 'refPartyList', {}, headers)['result']
    party_names = {}
    for party in parties:
        party_names[party['clientNumber']] = party['legalName']
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
    xinhu_funds = None  ## TODO
    legal_names = get_bct_legal_name(ip, headers)
    bank_accounts = get_bct_bank_accounts(ip, headers)
    out_fund = []
    in_fund = []
    for fund in xinhu_funds:
        legal_name = fund[0]
        legal_name = legal_names.get(legal_name)
        if not legal_name:
            print("系统不存在{legal_name}".format(legal_name=legal_name))
            continue
        bank_account = bank_accounts.get(legal_name)
        if not bank_accounts.get(legal_name):
            create_bank_account(legal_name + '开户行名称', legal_name, legal_name + '银行账号', legal_name + '银行账户名称', 'NORMAL',
                                legal_name + '支付系统行号', ip, headers)
        payment_amount = None
        payment_date = None
        payment_direction = None
        try:
            fund_event = {
                'clientId': legal_name,
                'bankAccount': bank_account,
                'paymentAmount': abs(payment_amount),
                'paymentDate': payment_date,
                'paymentDirection': payment_direction,
                'accountDirection': 'PARTY'
            }
            if payment_direction == 'IN':
                in_fund.append(fund_event)
            else:
                out_fund.append(fund_event)
        except Exception as e:
            print('交易对手{legal_name},出入金未知异常  mes: {error}'.format(legal_name=legal_name, error=repr(e)))
    for in_item in in_fund:
        print(in_item)
        save_fund_info(in_item, ip, headers)
    for out_item in out_fund:
        print(out_item)
        save_fund_info(out_item, ip, headers)
    print("出入金导入结束")


if __name__ == '__main__':
    headers = utils.login(login_ip, login_body)
    import_fund_record(login_ip, headers)
