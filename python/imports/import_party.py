# -*- encoding: utf-8 -*-

import pandas as pd

import python.imports.utils as utils
from python.imports.init_params import *


def create_subsidiary(subsidiary_name, host, token):
    return utils.call('refSubsidiaryCreate', {
        'subsidiaryName': subsidiary_name,
    }, 'reference-data-service', host, token)


def create_branch(subsidiary_id, branch_name, host, token):
    return utils.call('refBranchCreate', {
        'subsidiaryId': subsidiary_id,
        'branchName': branch_name
    }, 'reference-data-service', host, token)


def create_sales(branch_id, sales_name, host, token):
    return utils.call('refSalesCreate', {
        'branchId': branch_id,
        'salesName': sales_name
    }, 'reference-data-service', host, token)


#
# def import_sales(subsidiary,branch,sale,ip,headers):
#     print('========== Creating sales =========')
#     subInfo = create_subsidiary(subsidiary, ip, headers)
#     branchInfo = create_branch(subInfo['subsidiaryId'], branch, ip, headers)
#     create_sales(branchInfo['branchId'], sale, ip, headers)


def list_to_sales_map(subsidiary, branch, sale, map):
    if map.get(subsidiary):
        subsidiary_map = map.get(subsidiary)
        if subsidiary_map.get(branch):
            branch_list = subsidiary_map.get(branch)
            if sale not in branch_list:
                branch_list.append(sale)
            subsidiary_map[branch] = branch_list
        else:
            subsidiary_map[branch] = [sale]
        map[subsidiary] = subsidiary_map
    else:
        map[subsidiary] = {}
        map[subsidiary][branch] = [sale]
    return map


def check_sale_exist(subsidiary, branch, sale, map):
    if map.get(subsidiary):
        subsidiary_map = map.get(subsidiary)
        if subsidiary_map.get(branch):
            branch_list = subsidiary_map.get(branch)
            if sale in branch_list:
                return True
    return False


def import_sheet_0(ip, headers):
    pd_data = pd.read_csv(party_excel_file, encoding="gbk", dtype={'code': str})
    xinhu_partys = pd_data.where(pd_data.notnull(), "空").to_dict(orient='records')

    print("create sales start")
    xinhu_sales_map={}
    for xinhu_party in xinhu_partys:
        subsidiary = xinhu_party['bctsubsidiaryName']
        branch = xinhu_party['bctbranchName']
        sale = xinhu_party['bctsalesName']
        list_to_sales_map(subsidiary,branch,sale,xinhu_sales_map)
    for subsidiary,branch_map in xinhu_sales_map.items():
        subInfo = create_subsidiary(subsidiary, ip, headers)
        for branch,sales_list in branch_map.items():
            branchInfo = create_branch(subInfo['subsidiaryId'], branch, ip, headers)
            for sale in sales_list:
                create_sales(branchInfo['branchId'], sale, ip, headers)
    print("create sales end")

    bct_sales_map = {}
    bct_sales = utils.call_request(ip, 'reference-data-service', 'refSalesList', {}, headers)
    for bct_sales in bct_sales:
        subsidiary = bct_sales['subsidiaryName']
        branch = bct_sales['branchName']
        sale = bct_sales['salesName']
        list_to_sales_map(subsidiary, branch, sale, bct_sales_map)

    print("create partys start")
    count = 0
    for xinhu_party in xinhu_partys:
        subsidiary_name = xinhu_party['bctsubsidiaryName']
        branch_name = xinhu_party['bctbranchName']
        sales_name = xinhu_party['bctsalesName']
        if not check_sale_exist(subsidiary_name, branch_name, sales_name, bct_sales_map):
            print("sale not exist sale info", subsidiary_name, branch_name, sales_name)
            continue
        try:
            legal_name = xinhu_party['bctlegalName']
            client_type = xinhu_party['bctClientType']
            legal_representative = xinhu_party['bctlegalRepresentative']
            client_level = xinhu_party['bctclientLevel']
            address = xinhu_party['bctaddress'] if xinhu_party['bctaddress'] and xinhu_party['bctaddress'].strip() else '空'
            contract = xinhu_party['bctcontract'] if xinhu_party['bctcontract'] else '空'
            trade_phone = xinhu_party['bcttradePhone']
            trade_email = xinhu_party['bcttradeEmail']
            master_agreementId = xinhu_party['bctmasterAgreementId'] if xinhu_party['bctmasterAgreementId'] and \
                                                                        xinhu_party['bctmasterAgreementId'] != '空' else \
            xinhu_party['bctlegalName'] + '主协议编号'
            investor_type = xinhu_party['bctinvestorType']
            bct_party = {
                "legalName": legal_name,
                "clientType": client_type,
                "clientLevel": client_level,
                "investorType": investor_type,
                "legalRepresentative": legal_representative,
                "address": address,
                "salesName": sales_name,
                "warrantor": None,
                "warrantorAddress": None,
                "contact": contract,
                "tradePhone": trade_phone,
                "tradeEmail": trade_email,
                "masterAgreementId": master_agreementId,
                "trustorEmail": None,
                "supplementalAgreementId": None,
                "authorizeExpiryDate": None,
                "signAuthorizerName": None,
                "signAuthorizerIdNumber": None,
                "signAuthorizerIdExpiryDate": None,
                "masterAgreementNoVersion": None,
                "masterAgreementSignDate": None,
                "businessLicense": None,
                "productName": None,
                "productCode": None,
                "productType": None,
                "recordNumber": None,
                "productFoundDate": None,
                "productExpiringDate": None,
                "fundManager": None,
                "tradingDirection": None,
                "tradingPermission": None,
                "tradingPermissionNote": None,
                "tradingUnderlyers": None,
                "marginDiscountRate": None,
                "subsidiaryName": subsidiary_name,
                "branchName": branch_name,
                "tradeAuthorizer": []
                # "tradeAuthorizer": [
                #     {
                #         "tradeAuthorizerName": "0002姓名",
                #         "tradeAuthorizerIdNumber": "411528000000000000",
                #         "tradeAuthorizerIdExpiryDate": "2020-06-08",
                #         "tradeAuthorizerPhone": "18800000000"
                #     }
                # ]
            }
            utils.call('refPartySave', bct_party, 'reference-data-service', ip, headers)
            count += 1
        except Exception as e:
            print("create party error", repr(e), 'party name', legal_name)
    print("create party end")
    print("create party success num", str(count))


if __name__ == '__main__':
    headers = utils.login(login_ip, login_body)
    import_sheet_0(login_ip, headers)
