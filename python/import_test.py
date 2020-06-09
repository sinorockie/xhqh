# -*- coding: utf-8 -*-

from init_params import *

import init_party
import init_trade
import utils

if __name__ == '__main__':
    # 登陆BCT，获取用户Token
    headers = utils.login(login_ip, login_body)
    print('BCT认证Token: {headers}'.format(headers=headers))
    # init_trade.import_sheet_0(login_ip, headers)
    init_party.import_sheet_0(login_ip, headers)
