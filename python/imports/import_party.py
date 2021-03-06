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


def get_all_legal_sales_dic(ip, headers):
    legal_sales_dic = {}
    try:
        legal_list = utils.call('refPartyList', {}, 'reference-data-service', ip,
                                headers)
        for item in legal_list:
            legal_sales_dic[item['legalName']] = item['salesName']
        return legal_sales_dic
    except Exception as e:
        print("failed get legal data for: " + ip + ",Exception:" + str(e))


def get_bct_sales_map(ip, headers):
    bct_sales_map = {}
    bct_sales = utils.call_request(ip, 'reference-data-service', 'refSalesList', {}, headers)
    for bct_sales in bct_sales:
        subsidiary = bct_sales['subsidiaryName']
        branch = bct_sales['branchName']
        sale = bct_sales['salesName']
        list_to_sales_map(subsidiary, branch, sale, bct_sales_map)
    return bct_sales_map


def check_sale_exist(subsidiary, branch, sale, map):
    if map.get(subsidiary):
        subsidiary_map = map.get(subsidiary)
        if subsidiary_map.get(branch):
            branch_list = subsidiary_map.get(branch)
            if sale in branch_list:
                return True
    return False


def get_bct_subsidiary_branch_map(ip, headers):
    bct_subsidiary_branch_map = {}
    bct_subsidiary_branchs = utils.call_request(ip, 'reference-data-service', 'refSubsidiaryBranchList', {}, headers)
    for item in bct_subsidiary_branchs:
        branchs = bct_subsidiary_branch_map.get(item['subsidiaryName'])
        if branchs:
            branchs.append(item)
        else:
            bct_subsidiary_branch_map[item['subsidiaryName']] = [item]
    return bct_subsidiary_branch_map


def import_sheet_0(ip, headers):
    pd_data = pd.read_csv(import_party_excel_file, encoding="gbk", dtype={'code': str})
    xinhu_partys = pd_data.where(pd_data.notnull(), "空").to_dict(orient='records')

    # 获取bct已经存在的分公司营业部信息
    bct_subsidiary_branch_map = get_bct_subsidiary_branch_map(ip, headers)
    bct_sales_map = get_bct_sales_map(ip, headers)

    print("create sales start")
    xinhu_sales_map = {}
    for xinhu_party in xinhu_partys:
        subsidiary = xinhu_party['bctsubsidiaryName']
        branch = xinhu_party['bctbranchName']
        sale = xinhu_party['bctsalesName']
        list_to_sales_map(subsidiary, branch, sale, xinhu_sales_map)
    for subsidiary, branch_map in xinhu_sales_map.items():
        bct_branchs = []
        if bct_subsidiary_branch_map.get(subsidiary):
            bct_branchs = bct_subsidiary_branch_map.get(subsidiary)
        if bct_branchs:
            subInfo = bct_branchs[0]
        else:
            subInfo = create_subsidiary(subsidiary, ip, headers)
        for branch, sales_list in branch_map.items():
            branchInfo = None
            for bct_branch in bct_branchs:
                if branch == bct_branch['branchName']:
                    branchInfo = bct_branch
                    break
            if not branchInfo:
                branchInfo = create_branch(subInfo['subsidiaryId'], branch, ip, headers)
            for sale in sales_list:
                if not check_sale_exist(subsidiary, branch, sale, bct_sales_map):
                    create_sales(branchInfo['branchId'], sale, ip, headers)
    print("create sales end")
    bct_sales_map = get_bct_sales_map(ip, headers)
    # 获取bct所有交易对手
    party_sales_dic = get_all_legal_sales_dic(ip, headers)
    party_names = list(party_sales_dic.keys())
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
            if legal_name in party_names:
                print("交易对手{legal_name}已存在".format(legal_name=legal_name))
                continue
            client_type = xinhu_party['bctClientType']
            legal_representative = xinhu_party['bctlegalRepresentative']
            client_level = xinhu_party['bctclientLevel']
            address = xinhu_party['bctaddress'] if xinhu_party['bctaddress'] and xinhu_party[
                'bctaddress'].strip() else '空'
            contract = xinhu_party['bctcontract'] if xinhu_party['bctcontract'] else '空'
            trade_phone = xinhu_party['bcttradePhone']
            trade_email = xinhu_party['bcttradeEmail']
            if not (xinhu_party['bctmasterAgreementId'] and xinhu_party['bctmasterAgreementId'] != '空'):
                print(legal_name, '主协议编号为空')

            master_agreementId = xinhu_party['bctmasterAgreementId'] if xinhu_party['bctmasterAgreementId'] and \
                                                                        xinhu_party['bctmasterAgreementId'] != '空' else \
                xinhu_party['bctlegalName'] + '主协议编号'
            investor_type = xinhu_party['bctinvestorType']
            if master_agreementId in ['JR-cwqq-190125', 'JR-cwqq-190129', 'JR-cwqq-190429', 'JR-cwqq-191113']:
                # 特殊数据 以上主协议编号有重复
                # print(legal_name,master_agreementId)
                master_agreementId = legal_name + '主协议编号'
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
            # error_msg="create party error"+repr(e)+'party name'+legal_name
            # if "已经存在主协议编号为" in error_msg:
            #     print(legal_name,master_agreementId)
            print("create party error", repr(e), 'party name', legal_name)
    print("create party end")
    print("create party success num", str(count))


if __name__ == '__main__':
    headers = utils.login(bct_login_ip, bct_login_body)
    import_sheet_0(bct_login_ip, headers)
