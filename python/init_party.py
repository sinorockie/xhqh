# -*- encoding: utf-8 -*-

import openpyxl
import re

from init_params import *

import utils


def import_sheet_0(ip, headers):
    workbook = openpyxl.load_workbook(party_excel_file)
    sheet0 = workbook['有交易']
    count = 0
    natures_normal = ['国企', '私企', '普通产业客户']
    natures_fin = ['保险公司', '券商', '同业', '私募', '资管', '银行']
    categories_normal = ['普通投资者']
    categories_pro = ['专业投资者']
    for r in sheet0.rows:
        count += 1
        if count < 4:
            continue
        row = [item.value for item in r]
        party = {
            'legalName': row[4],
            'legalRepresentative': '-',
            'address': '-',
            'contact': '-',
            'warrantor': '-',
            'warrantorAddress': '-',
            'tradePhone': '-',
            'tradeEmail': '-',
            'subsidiaryName': '-',
            'branchName': '-',
            'salesName': '-',
            'masterAgreementId': 'ROW_' + ('%05d' % count)
        }

        print('-----第 {i} 行-----'.format(i=count))
        if row[13] is None or (row[13] not in ['0', '1', 0, 1]):
            print('是否产品不正确', row[0], row[4], row[13])
            party['clientType'] = 'PRODUCT' if row[4].endswith('基金') else 'INSTITUTION'
        else:
            party['clientType'] = 'PRODUCT' if row[13] in ['1', 1] else 'INSTITUTION'

        has_category = True
        if row[14] is None or (row[14] not in categories_normal and row[14] not in categories_pro):
            has_category = False
            print('投资者类别不正确', row[0], row[4], row[14])

        has_nature = True
        if row[16] is None or (row[16] not in natures_normal and row[16] not in natures_fin):
            has_nature = False
            print('投资者性质不正确', row[0], row[4], row[16])

        if has_category and has_nature:
            if party['clientType'] == 'PRODUCT':
                party['investorType'] = 'FINANCIAL_PRODUCT'
            elif row[14] in categories_pro and row[16] in natures_fin:
                party['investorType'] = 'FINANCIAL_INSTITUTIONAL_INVESTOR'
            elif row[14] in categories_pro and row[16] not in natures_fin:
                party['investorType'] = 'PROFESSIONAL_INVESTOR'
            elif row[14] not in categories_pro and row[16] not in natures_fin:
                party['investorType'] = 'NON_PROFESSINAL_INVESTOR'
            elif party['legalName'].startswith('新湖-') \
                    or party['legalName'].find('保险') > 0 \
                    or party['legalName'].find('证券') > 0 \
                    or party['legalName'].find('银行') > 0 \
                    or party['legalName'].find('资产管理') > 0 \
                    or party['legalName'].find('风险管理') > 0 \
                    or party['legalName'].find('投资管理') > 0:
                party['investorType'] = 'FINANCIAL_INSTITUTIONAL_INVESTOR'
            else:
                print('* 请确定是否为一般机构投资者:', party['legalName'])
                party['investorType'] = 'NON_PROFESSINAL_INVESTOR'

        if row[17] is None or (re.search(r'C[1-5]?', row[17]) is None):
            print('评级不正确', row[0], row[4], row[17])
            party['clientLevel'] = 'LEVEL_C'
        else:
            party['clientLevel'] = 'LEVEL_C'

        print(party)
        utils.call_request(ip, 'reference-data-service', 'refPartySave', party, headers)
    print(count)
