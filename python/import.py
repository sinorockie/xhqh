# -*- coding: utf-8 -*-
from init_params import *
from init_trade import *

import utils


if __name__ == '__main__':
    # 登陆BCT，获取用户Token
    headers = utils.login(login_ip, login_body)
    print('BCT认证Token: {headers}'.format(headers=headers))
    import_sheet_0(login_ip, headers)


