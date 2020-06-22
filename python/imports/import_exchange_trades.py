import datetime
import re

import pandas as pd

import python.imports.utils as utils
from python.imports.init_params import *

_datetime_fmt = '%Y-%m-%d-%H-%M-%S'
_datetime_fmt1 = '%Y-%m-%dT%H:%M:%S'


def instrument_wind_code(code, instrument_id_list):
    for instrument_id in instrument_id_list:
        if instrument_id.find(code.upper() + '.') == 0:
            return instrument_id
    if re.match(r'[A-Z][A-Z]?\d+', code):
        temp_code = re.sub(r'[A-Z][A-Z]?\d', re.search(r'[A-Z][A-Z]?', code).group(), code, 1)
        for instrument_id in instrument_id_list:
            if instrument_id.find(temp_code + '.') == 0:
                return instrument_id
    return code


def get_all_instrument_dic(ip, headers):
    instrument_dic = {}
    try:
        instrument_list = utils.call('mktInstrumentsListPaged', {}, 'market-data-service', ip,
                                     headers)['page']
        for item in instrument_list:
            instrument_dic[item['instrumentId']] = item
        return instrument_dic
    except Exception as e:
        print("failed update market data for: " + ip + ",Exception:" + str(e))


def import_exchange_trades(ip, headers):
    # 获取bct所有标的物
    instruments_dic = get_all_instrument_dic(ip, headers)
    xinhu_exchange_trades = pd.read_csv(import_exchange_trade_excel_file, encoding="gbk").to_dict(orient='records')
    records = []
    for trade in xinhu_exchange_trades:
        underlyer = instrument_wind_code(trade['code'], instruments_dic.keys())
        if trade['code'].find('.') < 0 and underlyer == trade['code']:
            print('BCT不存在合约代码 {underlyer} '.format(underlyer=str(underlyer)))
            continue
        if not instruments_dic.get(underlyer):
            print('BCT不存在合约代码 {underlyer} '.format(underlyer=str(underlyer)))
            continue
        multiplier = instruments_dic.get(underlyer)['multiplier'] if instruments_dic.get(underlyer)[
            'multiplier'] else 1
        direction = "BUYER" if trade['direction'] == 1 else "SELLER"
        open_close = "OPEN" if trade['direction'] == 1 else "CLOSE"  # 开平
        deal_price = trade['price']  # 交易价格
        deal_amount = trade['amount']  # 交易数量
        # 2020 - 06 - 18 - 20 - 59 - 00
        day_str = trade['dayStr']
        deal_time = datetime.datetime.strptime(day_str, _datetime_fmt).strftime(_datetime_fmt1)
        trade_id = str(trade['order_sys_id']) + "-" + str(trade['trade_id'])

        trade_account = '新湖场内交易账号'  # 交易账户
        exchange_trade = {
            "bookId": book_name,
            "instrumentId": underlyer,
            "direction": direction,
            "openClose": open_close,
            "dealPrice": deal_price,
            "dealAmount": deal_amount,
            "dealTime": deal_time,
            "tradeId": trade_id,
            "tradeAccount": trade_account,
            "multiplier": multiplier
        }
        records.append(exchange_trade)

    for record in records:
        try:
            utils.call('exeTradeRecordSave', record, 'exchange-service', ip, headers)
        except Exception as e:
            print("场内流水导入失败 {tradeId} msg:{error}".format(tradeId=record['tradeId'], error=repr(e)))


# exeTradeRecordSave
if __name__ == '__main__':
    headers = utils.login(bct_login_ip, bct_login_body)
    import_exchange_trades(bct_login_ip, headers)
