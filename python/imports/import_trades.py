# -*- encoding: utf-8 -*-

import re
from datetime import datetime

import pandas as pd
import vthread

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

_ACCOUNT_EVENT_UNWIND = 'UNWIND_TRADE'
_SETTLE_TRADE = 'SETTLE_TRADE'
_ACCOUNT_EVENT_START = 'START_TRADE'


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


def create_trade(trade, enrichment, valid_time, host, token):
    return utils.call('trdTradeCreate', {
        'trade': trade,
        'enrichment': enrichment,
        'validTime': valid_time.strftime(_datetime_fmt)
    }, 'trade-service', host, token)


def trade_lcm_process(trade_event, ip, headers):
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
        position_index = 0
        enrichment = []
        for v in trades:
            # 开仓波动率
            initial_vol = v['initialVol']
            position_id = trade_id + '_' + str(position_index)
            enrichment.append({'positionId': position_id, 'initialVol': initial_vol, 'trader': 'admin'})
            position_index += 1
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
            if strike_low and strike_high:
                if strike_low > strike_high:
                    tepm_strike = strike_low
                    strike_low = strike_high
                    strike_high = tepm_strike
            direction = _DIRECTION_BUYER if v['direction'].upper() == 'BUYER' else _DIRECTION_SELLER
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
        bct_trades.append({'trade': trade, 'enrichment': enrichment})
    for bct_trade in bct_trades:
        try:
            create_trade(bct_trade['trade'], bct_trade['enrichment'], datetime.now(), bct_login_ip, headers)
        except Exception as e:
            print('导入开仓交易信息出错: {error} '.format(error=str(e)))
    print("create open trades end")


def import_unwind_or_expiration_trades(xinhu_trade_map, bct_trade_dic, instruments_dic, instrument_ids, ip, headers):
    print("create unwind trades start")
    for trade_id, trades in xinhu_trade_map.items():
        bct_exist_trade = bct_trade_dic.get(trade_id)
        if not bct_exist_trade:
            print('{trade_id}不存在,平仓失败'.format(trade_id=trade_id))
            continue
        for v in trades:
            xinhu_trade_id = v['tradeId']
            if v['lcmEventType'] == 'UNWIND':  # 平仓
                ##平仓名义本金
                un_wind_amount = v['notionalAmount']
                ##平仓金额
                un_wind_amount_value = v['unWindAmountValue']
                ##平仓日期
                payment_date = v['paymentDate']
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
                    trade_lcm_process(params, ip, headers)
                except Exception as e:
                    print('xinhu_trade_id：{xinhu_trade_id}导入平仓交易信息出错: {error} '.format(xinhu_trade_id=xinhu_trade_id,
                                                                                       error=str(e)))
            elif v['lcmEventType'] == 'SETTLE':  # 行权
                position_id = convert_to_bct_position_id(xinhu_trade_id)
                underlyer_price = v['exerciseSpot']
                settle_amount = v['unWindAmountValue']
                notional_amount = v['notionalAmount']
                underlyer = instrument_wind_code(v['underlyerInstrumentId'], instrument_ids)
                if v['underlyerInstrumentId'].find('.') < 0 and underlyer == v['underlyerInstrumentId']:
                    print('BCT不存在合约代码 {underlyer} '.format(underlyer=str(underlyer)))
                    break
                if not instruments_dic.get(underlyer):
                    print('BCT不存在合约代码 {underlyer} '.format(underlyer=str(underlyer)))
                    break
                multiplier = instruments_dic.get(underlyer)['multiplier'] if instruments_dic.get(underlyer)[
                    'multiplier'] else 1
                numof_options = "%.13f" % (abs(notional_amount / v['initialSpot'] / multiplier))
                ##平仓日期
                payment_date = v['paymentDate']
                event_detail = {
                    "underlyerPrice": str(underlyer_price),
                    "settleAmount": str(settle_amount),
                    "notionalAmount": str(notional_amount),
                    "numOfOptions": str(numof_options),
                    "paymentDate": payment_date
                }
                params = {
                    "positionId": position_id,
                    "tradeId": trade_id,
                    "eventType": "EXERCISE",
                    "userLoginId": 'admin',
                    "eventDetail": event_detail
                }
                try:
                    trade_lcm_process(params, ip, headers)
                except Exception as e:
                    print('xinhu_trade_id：{xinhu_trade_id}导入行权交易信息出错: {error} '.format(xinhu_trade_id=xinhu_trade_id,
                                                                                       error=str(e)))
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

    xinhu_trades = pd.read_csv(import_trade_excel_file, encoding="gbk").to_dict(orient='records')
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

    import_unwind_or_expiration_trades(xinhu_trade_map, bct_trade_dic, instruments_dic, instrument_ids, ip, headers)
    # 交易台账
    process_cash(xinhu_trade_map, party_sales_dic, ip, headers)


def process_cash(xinhu_trade_map, party_sales_dic, ip, headers):
    for trade_id, trades in xinhu_trade_map.items():
        try:
            for v in trades:
                party_name = v['partyName']
                if not party_sales_dic.get(party_name):
                    print('BCT不存在该用户 {party_name} '.format(party_name=str(party_name)))
                    break
                process_trade_cash(trade_id, party_name, 'START', v['premium'], ip, headers)
                if v['lcmEventType'] == 'UNWIND' or v['lcmEventType'] == 'SETTLE':
                    process_trade_cash(trade_id, party_name, v['lcmEventType'], v['unWindAmountValue'], ip, headers)
        except Exception as e:
            print("交易资金录入未知异常" + repr(e))


@vthread.pool(10)
def process_trade_cash(trade_id, legal_name, event_name, cash_amount, bct_host, bct_token):
    try:
        account_info = search_account(legal_name, bct_host, bct_token)
        print(account_info[0])

        bank_info = search_bank(legal_name, bct_host, bct_token)
        if not bank_info:
            bank_dto = create_bank_account(legal_name + '开户行名称', legal_name, legal_name + '银行账号', legal_name + '银行账户名称',
                                           'NORMAL',
                                           legal_name + '支付系统行号', bct_host, bct_token)
            bank_info = [bank_dto]
        print(bank_info[0])

        bank_account = bank_info[0]['bankAccount']
        print(bank_account)

        margin = account_info[0]['margin']  # 保证金
        print('margin: ' + str(margin))

        cash = account_info[0]['cash']  # 现金余额
        print('cash: ' + str(cash))

        debt = account_info[0]['debt']  # 负债
        print('debt: ' + str(debt))

        credit = account_info[0]['credit']  # 授信总额
        print('credit: ' + str(credit))

        credit_used = account_info[0]['creditUsed']  # 已用授信额度
        print('credit(used): ' + str(credit_used))

        event = _ACCOUNT_EVENT_UNWIND
        if event_name == 'UNWIND':
            event = _ACCOUNT_EVENT_UNWIND
        if event_name == 'SETTLE':
            event = _SETTLE_TRADE
        if event_name == 'START':
            event = _ACCOUNT_EVENT_START

        if cash_amount > 0:
            account_info = utils.call('clientSaveAccountOpRecord', {
                'accountOpRecord': {
                    "tradeId": trade_id,
                    "accountId": legal_name + '0',
                    "legalName": legal_name,
                    "event": event,
                    "status": 'NORMAL',
                    "cashChange": cash_amount
                }
            }, 'reference-data-service', bct_host, bct_token)
            print(account_info)
        else:
            if abs(cash_amount) > cash:
                record_info = utils.call('clientSaveAccountOpRecord', {
                    'accountOpRecord': {
                        "accountId": legal_name + '0',
                        "legalName": legal_name,
                        "event": 'TRADE_CASH_FLOW',
                        "status": 'NORMAL',
                        "cashChange": abs(cash_amount + cash),
                        "debtChange": abs(cash_amount + cash)
                    }
                }, 'reference-data-service', bct_host, bct_token)
                print(record_info)
            account_info = utils.call('clientSaveAccountOpRecord', {
                'accountOpRecord': {
                    "tradeId": trade_id,
                    "accountId": legal_name + '0',
                    "legalName": legal_name,
                    "event": event,
                    "status": 'NORMAL',
                    "cashChange": cash_amount
                }
            }, 'reference-data-service', bct_host, bct_token)
            print(account_info)

    except Exception as e:
        print("交易资金录入未知异常process_trade_cash" + repr(e))


def search_account(legal_name, bct_host, bct_token):
    return utils.call('clientAccountSearch', {
        'legalName': legal_name
    }, 'reference-data-service', bct_host, bct_token)


def search_bank(legal_name, bct_host, bct_token):
    return utils.call('refBankAccountSearch', {
        'legalName': legal_name
    }, 'reference-data-service', bct_host, bct_token)


def create_bank_account(bank_name, legal_name, bank_account, bank_account_name, bank_account_status_enum,
                        payment_system_code, ip, headers):
    bank_account = {
        'bankName': bank_name,  ##开户行名称
        'legalName': legal_name,
        'bankAccount': bank_account,  ##银行账户
        'bankAccountName': bank_account_name,  ##银行账户名称
        'bankAccountStatusEnum': bank_account_status_enum,  ##启用/停用
        'paymentSystemCode': payment_system_code  ##支付系统行号
    }
    return utils.call_request(ip, 'reference-data-service', 'refBankAccountSave', bank_account, headers)


if __name__ == '__main__':
    headers = utils.login(bct_login_ip, bct_login_body)
    # add_to_white_list(login_ip,headers)
    import_sheet_0(bct_login_ip, headers)
