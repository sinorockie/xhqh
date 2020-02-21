# -*- coding: utf-8 -*-
import openpyxl
import xlrd

from init_params import *
from init_trade import *

import utils


if __name__ == '__main__':
    # 登陆BCT，获取用户Token
    headers = utils.login(login_ip, login_body)
    print('BCT认证Token: {headers}'.format(headers=headers))
    # import_sheet_0(login_ip, headers)
    workbook = openpyxl.load_workbook(r'/Users/shilei/Downloads/对手方信息表有交易.xlsx')
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
        # print(row[4], '产品' if row[13] == 0 else '机构', row[14], row[15])
        print('-----第 {i} 行-----'.format(i=count))
        if row[0] is None:
            print('缺编号', '内部编号', row[0], row[4])

        if row[4] is None:
            print('缺交易对手方名称', '内部编号', row[0], row[4])

        if row[13] is None:
            print('缺是否产品', '内部编号', row[0], row[4], row[13])
        elif row[13] not in ['0', '1', 0, 1]:
            print('是否产品不正确', '内部编号', row[0], row[4], row[13])

        if row[14] is None:
            print('缺投资者类别', '内部编号', row[0], row[4], row[14])
        elif row[14] not in categories_normal and row[14] not in categories_pro:
            print('投资者类别不正确', '内部编号', row[0], row[4], row[14])

        if row[16] is None:
            print('缺投资者性质', '内部编号', row[0], row[4], row[16])
        elif row[16] not in natures_normal and row[16] not in natures_fin:
            print('投资者性质不正确', '内部编号', row[0], row[4], row[16])

        if row[17] is None:
            print('缺评级', '内部编号', row[0], row[4], row[17])
        elif re.search(r'C[1-5]?', row[17]) is None:
            print('评级不正确', '内部编号', row[0], row[4], row[17])
    print(count)
    sheet1 = workbook['无交易']
    count = 0
    for r in sheet1.rows:
        count += 1
        if count < 4:
            continue
        row = [item.value for item in r]
        # print(row[4], '产品' if row[13] == 0 else '机构', row[14], row[15])
        print('-----第 {i} 行-----'.format(i=count))
        if row[0] is None:
            print('缺编号', '内部编号', row[0], row[3])

        if row[3] is None:
            print('缺交易对手方名称', '内部编号', row[0], row[3])

        if row[11] is None:
            print('缺是否产品', '内部编号', row[0], row[4], row[11])
        elif row[11] not in ['0', '1', 0, 1]:
            print('是否产品不正确', '内部编号', row[0], row[4], row[11])

        if row[12] is None:
            print('缺投资者类别', '内部编号', row[0], row[4], row[12])
        elif row[12] not in categories_normal and row[12] not in categories_pro:
            print('投资者类别不正确', '内部编号', row[0], row[4], row[12])

        if row[13] is None:
            print('缺评级', '内部编号', row[0], row[4], row[13])
        elif re.search(r'C[1-5]?', row[13]) is None:
            print('评级不正确', '内部编号', row[0], row[4], row[13])
    print(count)
