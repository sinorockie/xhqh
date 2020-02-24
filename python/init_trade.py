# -*- encoding: utf-8 -*-

from datetime import datetime, timedelta, date

import logging
import pandas as pd
import re

from init_params import *

import trade_templates
import utils


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


def instrument_info(underlyer, host, token):
    return utils.call('mktInstrumentInfo', {
        'instrumentId': underlyer
    }, 'market-data-service', host, token)


def create_trade(trade, valid_time, host, token):
    return utils.call('trdTradeCreate', {
        'trade': trade,
        'validTime': valid_time.strftime(_datetime_fmt)
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


def import_sheet_0(ip, headers):
    # 获取所有交易对手列表
    party_names = \
        utils.call_request(ip, 'reference-data-service', 'refSimilarLegalNameList', {'similarLegalName': ''}, headers)
    print('BCT交易对手名称列表: {party_names}'.format(party_names=party_names))
    # 获取所有标的物列表
    instrument_ids = utils.call_request(ip, 'market-data-service', 'mktInstrumentIdsList', {}, headers)
    print('BCT标的物列表: {instrument_ids}'.format(instrument_ids=instrument_ids))

    df = pd.read_excel(trade_excel_file, header=1)
    for i, v in df.iterrows():
        if i == 0:
            continue
        underlyer = instrument_wind_code(v['undercode'], instrument_ids)
        if v['undercode'].find('.') < 0 and underlyer == v['undercode']:
            print('BCT不存在合约代码 {underlyer} '.format(underlyer=str(underlyer)))
            continue
        trade_id = v['internalID']
        book = book_name
        trade_date = datetime.strptime(str(v['T0']), _date_fmt)
        option_type = _OPTION_CALL if v['opt_base_type'].upper() == 'Call' else _OPTION_PUT
        strike_type = _UNIT_CNY
        strike = v['strike']
        direction = _DIRECTION_SELLER if v['side'] == '卖方' else _DIRECTION_BUYER
        multiplier = 1
        specified_price = 'close'
        init_spot = v['S0']
        expiration_date = datetime.strptime(str(v['T']), _date_fmt)
        notional_amount_type = _UNIT_CNY
        notional = v['notional']
        participation_rate = v['participation']
        annualized = False
        term = (datetime.strptime(str(v['T']), _date_fmt) - datetime.strptime(str(v['T0']), _date_fmt)).days
        days_in_year = _DAYS_IN_YEAR
        premium_type = _UNIT_CNY
        premium = v['totalPx']
        effective_date = datetime.strptime(str(v['T0']), _date_fmt)
        trader = ''
        counter_party = v['counterparty']
        sales = ''
        trade = trade_templates.vanilla_european(trade_id, book, trade_date, option_type, strike_type, strike,
                                                 direction, underlyer, multiplier, specified_price, init_spot,
                                                 expiration_date, notional_amount_type, notional, participation_rate,
                                                 annualized, term, days_in_year, premium_type, premium, effective_date,
                                                 trader, counter_party, sales)
        print('BCT交易信息: {trade} '.format(trade=str(trade)))
        try:
            create_trade_and_client_cash_flow(trade, login_ip, headers)
        except Exception as e:
            logging.info('导入交易信息出错: {error} '.format(error=str(e)))
