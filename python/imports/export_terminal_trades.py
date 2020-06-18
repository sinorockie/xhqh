import datetime

import python.imports.terminal_utils as terminal_utils
from python.imports.init_params import *

_datetime_fmt = '%Y-%m-%d %H%M%S'


def downloadPositionExcel(trade_uuid_list, export_terminal_file, headers):
    terminal_utils.download(trade_uuid_list, export_terminal_file, 'downloadPositionExcel', headers)


def export_terminal_trades():
    headers = terminal_utils.login_terminal()
    resp = terminal_utils.call_terminal_request("terminal-service", "terTradeGroupPaged",
                                                {"productType": "VANILLA_EUROPEAN", "page": 0, "pageSize": 100000},
                                                headers)
    trade_list = resp['page']
    position_uuid_list = []
    for trade in trade_list:
        for position in trade:
            position_uuid_list.append(position['uuid'])
    out_path = export_terminal_trade_file + "terminal_trades" + datetime.datetime.now().strftime(
        _datetime_fmt) + '.xlsx'
    downloadPositionExcel(position_uuid_list, out_path, headers)


if __name__ == '__main__':
    export_terminal_trades()
