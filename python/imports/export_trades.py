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


def get_all_bct_trade(ip, headers):
    bct_trades = utils.call('trdTradeSearch', {}, 'trade-service', ip, headers)

    return bct_trades


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
                    utils.call("trdTradeLCMEventProcess", params, "trade-service", ip, headers)
                except Exception as e:
                    print('导入平仓交易信息出错: {error} '.format(error=str(e)))
    print("create unwind trades end")


def export_trade(ip, headers):
    # 获取bct所有交易
    bct_trades = get_all_bct_trade(ip, headers)
    csv_data = []
    for trade in bct_trades:
        book_name = trade['bookName']
        trade_id = trade['tradeId']
        trader = trade['trader']
        trade_status = trade['tradeStatus']
        trade_date = trade['tradeDate']
        sales_name = trade['salesName']
        positions = trade['positions']
        trade_confirm_id = trade['tradeConfirmId']
        for position in positions:
            position_id = position['positionId']
            lcm_event_type = position['lcmEventType']
            product_type = position['productType']
            asset = position['asset']
            direction = asset['direction']
            exercise_type = asset.get('exerciseType')
            underlyer_instrument_id = asset['underlyerInstrumentId']
            initial_spot = asset['initialSpot']
            strike_type = asset['strikeType']

            strike = asset.get('strike')
            low_strike = asset.get('lowStrike')
            high_strike = asset.get('highStrike')

            specified_price = asset['specifiedPrice']
            settlement_date = asset['settlementDate']
            term = asset['term']
            annualized = asset['annualized']
            days_in_year = asset['daysInYear']

            participation_rate = asset.get('participationRate')
            low_participation_rate = asset.get('lowParticipationRate')
            high_participation_rate = asset.get('highParticipationRate')

            option_type = asset.get('optionType')  ##
            notional_amount = asset['notionalAmount']
            notional_amount_type = asset['notionalAmountType']
            underlyer_multiplier = asset['underlyerMultiplier']
            expiration_date = asset['expirationDate']
            effective_date = asset['effectiveDate']
            premium_type = asset['premiumType']
            premium = asset['premium']
            xinhu_trade = {
                "book_name": book_name,
                "trade_id": trade_id,
                "trader": trader,
                "trade_status": trade_status,
                "trade_date": trade_date,
                "sales_name": sales_name,
                "trade_confirm_id": trade_confirm_id,
                "position_id": position_id,
                "lcm_event_type": lcm_event_type,
                "product_type": product_type,
                "direction": direction,
                "exercise_type": exercise_type,
                "underlyer_instrument_id": underlyer_instrument_id,
                "initial_spot": initial_spot,
                "strike_type": strike_type,

                "strike": strike,  ##
                "low_strike": low_strike,  ##
                "high_strike": high_strike,  ##

                "specified_price": specified_price,
                "settlement_date": settlement_date,
                "term": term,
                "annualized": annualized,
                "days_in_year": days_in_year,

                "participation_rate": participation_rate,  ##
                "low_participation_rate": low_participation_rate,
                "high_participation_rate": high_participation_rate,

                "option_type": option_type,  ##
                "notional_amount": notional_amount,
                "notional_amount_type": notional_amount_type,
                "underlyer_multiplier": underlyer_multiplier,
                "expiration_date": expiration_date,
                "effective_date": effective_date,
                "premium_type": premium_type,
                "premium": premium
            }
            csv_data.append(xinhu_trade)
    print(csv_data)
    columns = ["book_name",
               "trade_id",
               "trader",
               "trade_status",
               "trade_date",
               "sales_name",
               "trade_confirm_id",
               "position_id",
               "lcm_event_type",
               "product_type",
               "direction",
               "exercise_type",
               "underlyer_instrument_id",
               "initial_spot",
               "strike_type",
               "strike",  ##
               "low_strike",  ##
               "high_strike",  ##
               "specified_price",
               "settlement_date",
               "term",
               "annualized",
               "days_in_year",
               "participation_rate",  ##
               "low_participation_rate",
               "high_participation_rate",
               "option_type",  ##
               "notional_amount",
               "notional_amount_type",
               "underlyer_multiplier",
               "expiration_date",
               "effective_date",
               "premium_type",
               "premium"]
    df = pd.DataFrame(columns=columns, data=csv_data)
    df.to_csv('D:/xinhu/data.csv', encoding='utf-8', index=False)


if __name__ == '__main__':
    headers = utils.login(login_ip, login_body)
    export_trade(login_ip, headers)
