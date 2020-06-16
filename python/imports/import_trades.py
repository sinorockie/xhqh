# -*- encoding: utf-8 -*-

import re
from datetime import datetime

import pandas as pd

import python.imports.utils as utils
from python.imports.init_params import *
from python.imports.trade_templates import *

_DAYS_IN_YEAR = 365
_UNIT_LOT = 'LOT'
_UNIT_CNY = 'CNY'
_UNIT_PERCENT = 'PERCENT'
_DIRECTION_BUYER = 'BUYER'
_DIRECTION_SELLER = 'SELLER'
_OPTION_CALL = 'CALL'
_OPTION_PUT = 'PUT'
_KNOCK_UP = 'UP'
_KNOCK_DOWN = 'DOWN'
_REBATE_PAY_AT_EXPIRY = 'PAY_AT_EXPIRY'
_REBATE_PAY_WHEN_HIT = 'PAY_WHEN_HIT'
_OBSERVATION_DAILY = 'DAILY'
_OBSERVATION_CONTINUOUS = 'CONTINUOUS'
_OBSERVATION_TERMINAL = 'TERMINAL'
_EXCHANGE_OPEN = 'OPEN'
_EXCHANGE_CLOSE = 'CLOSE'
_datetime_fmt = '%Y-%m-%dT%H:%M:%S'
_date_fmt = '%Y%m%d'
_date_fmt2 = '%Y-%m-%d'


def add_to_white_list(ip, headers):
    instrument_list_response = utils.call_request(ip, "market-data-service", "mktInstrumentsListPaged", {}, headers)
    instrument_list = instrument_list_response['page']
    for instrument in instrument_list:
        params = {
            'venueCode': instrument['exchange'],
            'instrumentId': instrument['instrumentId'],
            'notionalLimit': 100000000000
        }
        utils.call_request(ip, 'market-data-service', 'mktInstrumentWhitelistSave', params, headers)


def create_trade(trade, valid_time, host, token):
    return utils.call('trdTradeCreate', {
        'trade': trade,
        'validTime': valid_time.strftime(_datetime_fmt)
    }, 'trade-service', host, token)


def unwind_trade(trade_event, ip, headers):
    return utils.call("trdTradeLCMEventProcess", trade_event, "trade-service", ip, headers)


def delete_trade(trade_id, host, token):
    return utils.call('trdTradeDelete', {
        'tradeId': trade_id
    }, 'trade-service', host, token)


def calc_premium(trade):
    """Return premium amount, round to 0.01."""
    asset = trade['positions'][0]['asset']
    direction = -1 if asset['direction'] == _DIRECTION_BUYER else 1
    if asset['premiumType'].upper() != _UNIT_PERCENT:
        return round(asset['premium'] * direction, 2)
    notional = asset['notionalAmount']
    if asset['notionalAmountType'] == _UNIT_LOT:
        notional *= asset['initialSpot'] * asset['underlyerMultiplier']
    if asset['annualized']:
        notional *= asset['term'] / asset['daysInYear']
    return round(notional * asset['premium'] * direction, 2)


def search_account(legal_name, host, token):
    res = utils.call('clientAccountSearch', {
        'legalName': legal_name
    }, 'reference-data-service', host, token)
    return res


def create_client_cash_flow(account_id, trade_id, cash_flow, margin_flow, host, token):
    trade = utils.call('trdTradeSearch', {
        'tradeId': trade_id
    }, 'trade-service', host, token)[0]
    position = trade['positions'][0]
    direction = position['asset']['direction']
    client = position['counterPartyCode']
    task = utils.call('cliTasksGenerateByTradeId', {
        'legalName': client,
        'tradeId': trade_id
    }, 'reference-data-service', host, token)[0]
    utils.call('clientChangePremium', {
        'tradeId': trade_id,
        'accountId': task['accountId'],
        'premium': task['premium'],
        'information': None
    }, 'reference-data-service', host, token)
    res = utils.call('clientSaveAccountOpRecord', {
        'accountOpRecord': {
            'accountId': task['accountId'],
            'cashChange': task['premium'] * -1,
            'counterPartyCreditBalanceChange': 0,
            'counterPartyFundChange': 0,
            'creditBalanceChange': 0,
            'debtChange': 0,
            'event': 'CHANGE_PREMIUM',
            'legalName': client,
            'premiumChange': task['premium'],
            'tradeId': trade_id
        }
    }, 'reference-data-service', host, token)
    return utils.call('cliMmarkTradeTaskProcessed', {
        'uuidList': [task['uuid']]
    }, 'reference-data-service', host, token)


def create_trade_and_client_cash_flow(trade, host, token):
    trade_id = trade['tradeId']
    # position = trade['positions'][0]
    # counter_party = position['counterPartyCode']
    # premium_cash = calc_premium(trade)
    # account_id = search_account(counter_party, host, token)[0]['accountId']
    # margin = 0 if position['asset']['direction'] == _DIRECTION_BUYER else 0
    create_trade(trade, datetime.now(), host, token)
    # create_client_cash_flow(account_id, trade_id, -premium_cash, -margin, host, token)
    print('Created: ' + trade_id)


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


def get_all_bct_trade_dic(ip, headers):
    bct_trades = utils.call('trdTradeSearch', {}, 'trade-service', ip, headers)
    bct_trade_dic = {}
    for bct_trade in bct_trades:
        bct_trade_dic[bct_trade['tradeId']] = bct_trade
    return bct_trade_dic


def convert_to_bct_trade_id(xinhu_trade_id):
    if '-' in xinhu_trade_id:
        return xinhu_trade_id[:xinhu_trade_id.rfind('-')]
    else:
        return xinhu_trade_id


def convert_to_bct_position_id(xinhu_trade_id):
    if '-' in xinhu_trade_id:
        position_index = str(int(xinhu_trade_id[xinhu_trade_id.rfind('-') + 1:]) - 1)
        trade_id = convert_to_bct_trade_id(xinhu_trade_id)
        return trade_id + '_' + position_index
    else:
        return xinhu_trade_id + '_0'


def import_open_trades(xinhu_trade_map, bct_trade_dic, party_sales_dic, instruments_dic, instrument_ids, ip, headers):
    print("create open trades start")
    bct_trades = []
    for trade_id, trades in xinhu_trade_map.items():
        bct_exist_trade = bct_trade_dic.get(trade_id)
        if bct_exist_trade:
            delete_trade(trade_id, ip, headers)
        positions = []
        trade_date = None
        sales_name = None
        for v in trades:
            party_name = v['partyName']
            sales_name = v['salesName']
            trade_date = v['tradeDate']
            if not party_sales_dic.get(party_name):
                print('BCT不存在该用户 {party_name} '.format(party_name=str(party_name)))
                break
            if not (party_sales_dic.get(party_name) == sales_name):
                print('BCT不存在该销售 {sales_name}'.format(sales_name=str(sales_name)))
                break
            sales_name = party_sales_dic.get(party_name)
            underlyer = instrument_wind_code(v['underlyerInstrumentId'], instrument_ids)  # TODO
            if v['underlyerInstrumentId'].find('.') < 0 and underlyer == v['underlyerInstrumentId']:
                print('BCT不存在合约代码 {underlyer} '.format(underlyer=str(underlyer)))
                break
            if not instruments_dic.get(underlyer):
                print('BCT不存在合约代码 {underlyer} '.format(underlyer=str(underlyer)))
                break
            trade_date = datetime.strptime(str(v['tradeDate']), _date_fmt2)
            option_type = _OPTION_CALL if v['optionType'].upper() == 'CALL' else _OPTION_PUT
            strike_type = v['strikeType']
            strike = v['strike']
            strike_low = v['strike_low']
            strike_high = v['strike_high']
            direction = _DIRECTION_SELLER if v['direction'].upper() == 'BUYER' else _DIRECTION_BUYER
            multiplier = instruments_dic.get(underlyer)['multiplier'] if instruments_dic.get(underlyer)[
                'multiplier'] else 1
            specified_price = 'close'  # v['specifiedPrice'] TODO
            init_spot = v['initialSpot']
            expiration_date = datetime.strptime(str(v['expirationDate']), _date_fmt2)
            notional_amount_type = v['notionalAmountType']
            notional = v['notionalAmount']
            participation_rate = v['participationRate']
            annualized = True if v['annualized'] and v['annualized'] == 1 else False
            effective_date = datetime.strptime(str(v['effectiveDate']), _date_fmt2)
            term = (datetime.strptime(str(v['expirationDate']), _date_fmt2) - datetime.strptime(str(v['effectiveDate']),
                                                                                                _date_fmt2)).days
            days_in_year = v['daysInYear']
            premium_type = v['premiumType']
            premium = v['premium']
            counter_party = party_name
            product_type = v['productType']
            position = None
            if product_type == 'Vanilla期权':
                position = vanilla_european_position(option_type, strike_type, strike,
                                                     direction, underlyer, multiplier, specified_price, init_spot,
                                                     expiration_date, notional_amount_type, notional,
                                                     participation_rate,
                                                     annualized, term, days_in_year, premium_type, premium,
                                                     effective_date,
                                                     counter_party)
            if product_type == 'Vanilla跨式':
                position = straddle_position(strike_type, strike_low, strike_high,
                                             direction, underlyer, multiplier, specified_price, init_spot,
                                             expiration_date, notional_amount_type, notional,
                                             participation_rate,
                                             annualized, term, days_in_year, premium_type, premium,
                                             effective_date,
                                             counter_party)
            if product_type == 'Vanilla价差期权':
                position = vertical_spread_position(option_type, strike_type, strike_low, strike_high,
                                                    direction, underlyer, multiplier, specified_price, init_spot,
                                                    expiration_date, notional_amount_type, notional, participation_rate,
                                                    annualized, term, days_in_year, premium_type, premium,
                                                    effective_date,
                                                    counter_party)
            positions.append(position)
        if not positions:
            continue
        trader = 'admin'
        trade = None
        if product_type == 'Vanilla期权':
            trade = vanilla_european_trade(positions, trade_id, book_name, trade_date, trader, sales_name)
        if product_type == 'Vanilla跨式':
            trade = straddle_trade(positions, trade_id, book_name, trade_date, trader, sales_name)
        if product_type == 'Vanilla价差期权':
            trade = vertical_spread_trade(positions, trade_id, book_name, trade_date, trader, sales_name)
        bct_trades.append(trade)
    for bct_trade in bct_trades:
        try:
            create_trade(bct_trade, datetime.now(), login_ip, headers)
        except Exception as e:
            print('导入开仓交易信息出错: {error} '.format(error=str(e)))
    print("create open trades end")


def import_unwind_trades(xinhu_trade_map, bct_trade_dic, ip, headers):
    print("create unwind trades start")
    for trade_id, trades in xinhu_trade_map.items():
        bct_exist_trade = bct_trade_dic.get(trade_id)
        if not bct_exist_trade:
            print('{trade_id}不存在,平仓失败'.format(trade_id=trade_id))
            continue
        for v in trades:
            xinhu_trade_id = v['tradeId']
            ##平仓名义本金
            un_wind_amount = v['notionalAmount']
            ##平仓金额
            un_wind_amount_value = v['unWindAmountValue']
            ##平仓日期
            payment_date = v['paymentDate']
            if v['lcmEventType'] == 1:  # 已了结
                position_id = convert_to_bct_position_id(xinhu_trade_id)
                event_detail = {
                    "unWindAmount": str(un_wind_amount),
                    "unWindAmountValue": str(un_wind_amount_value),
                    "paymentDate": payment_date
                }
                params = {
                    "positionId": position_id,
                    "tradeId": trade_id,
                    "eventType": "UNWIND",
                    "userLoginId": 'admin',
                    "eventDetail": event_detail
                }
                try:
                    unwind_trade(params, ip, headers)
                except Exception as e:
                    print('导入平仓交易信息出错: {error} '.format(error=str(e)))
    print("create unwind trades end")


def import_sheet_0(ip, headers):
    # 获取bct所有交易对手
    party_sales_dic = get_all_legal_sales_dic(ip, headers)
    print('BCT交易对手-销售名称字典: {party_sales_dic}'.format(party_sales_dic=party_sales_dic))
    # 获取bct所有标的物
    instruments_dic = get_all_instrument_dic(ip, headers)
    instrument_ids = instruments_dic.keys()
    print('BCT标的物列表: {instrument_ids}'.format(instrument_ids=instrument_ids))
    # 获取bct所有交易
    bct_trade_dic = get_all_bct_trade_dic(ip, headers)
    print('BCT交易ids: {bct_trade_ids}'.format(bct_trade_ids=bct_trade_dic.keys()))

    # for id in list(bct_trade_dic.keys()):
    #     delete_trade(id, ip, headers)

    xinhu_trades = pd.read_csv(trade_excel_file, encoding="gbk").to_dict(orient='records')
    xinhu_trade_map = {}
    for v in xinhu_trades:
        xinhu_trade_id = v['tradeId']
        trade_id = convert_to_bct_trade_id(xinhu_trade_id)
        if xinhu_trade_map.get(trade_id):
            trade_list = xinhu_trade_map.get(trade_id)
            trade_list.append(v)
        else:
            xinhu_trade_map[trade_id] = [v]
    print("新湖瑞丰 csv trades num:" + str(len(xinhu_trade_map.keys())))

    ##找出新湖交易id重复的交易数据
    for key in list(xinhu_trade_map.keys()):
        values = xinhu_trade_map[key]
        flag1 = False
        flag2 = False
        for v in values:
            tradeId = v['tradeId']
            if key == tradeId:
                flag1 = True
            else:
                flag2 = True
        if flag2 and flag1:
            print("交易id重复：{key}".format(key=key))
            del xinhu_trade_map[key]
            continue

    import_open_trades(xinhu_trade_map, bct_trade_dic, party_sales_dic, instruments_dic, instrument_ids, ip, headers)
    ## TODO 是否能自动到期
    import_unwind_trades(xinhu_trade_map, bct_trade_dic, ip, headers)


if __name__ == '__main__':
    headers = utils.login(login_ip, login_body)
    # add_to_white_list(login_ip,headers)
    import_sheet_0(login_ip, headers)
