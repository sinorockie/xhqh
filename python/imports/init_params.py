# -*- coding: utf-8 -*-

_date_short_fmt = '%Y%m%d'
_date_long_fmt = '%Y-%m-%d'

terminal_host = 'terminal-prod2.tongyu-quant.com'
terminal_port = 16016
terminal_username = 'admin'
terminal_password = '123456Aa'
terminal_client_id = 'terminal'
terminal_grant_type = 'password'



# bct_login_ip = '10.1.5.106'  # BCT IP地址
# bct_login_body = {
#     'username': 'admin',  # BCT 登陆用户名
#     'password': '123456a.'  # BCT 登陆密码
# }

bct_login_ip = '106.14.159.92'  # BCT IP地址
bct_login_body = {
    'username': 'admin',  # BCT 登陆用户名
    'password': '12345'  # BCT 登陆密码
}
#
book_name = 'test_xh_otc'  # BCT 交易簿

# 导入文件位置
import_trade_excel_file = 'D:/xinhu/BCT_format_trade_info.csv'  # 交易导入文件路径
import_exchange_trade_excel_file = 'D:/xinhu/bct_trades_input_20200619_.csv'  # 场内交易导入文件路径
import_party_excel_file = 'D:/xinhu/export_bct_client.csv'  # 客户导入文件路径
import_vol_file = 'D:/xinhu/hedgevol_20200616.xlsx'  # 波动率导入
##导出文件位置
export_trade_file = 'D:/xinhu/'  # 交易导出文件路径
export_terminal_trade_file = 'D:/xinhu/'  # 终端交易导出文件路径
export_vol_file = 'D:/xinhu/'  # 波动率导出文件路径
