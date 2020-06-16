# -*- encoding: utf-8 -*-

import datetime
import re

import pandas as pd

import python.imports.utils as utils
from python.imports.init_params import *

_datetime_fmt = '%Y-%m-%d %H%M%S'


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


def get_all_bct_trade_dic(ip, headers):
    bct_trades = utils.call('trdTradeSearch', {}, 'trade-service', ip, headers)
    bct_trade_dic = {}
    for bct_trade in bct_trades:
        bct_trade_dic[bct_trade['tradeId']] = bct_trade
    return bct_trade_dic


def get_all_position_unwind_amount_dic(trade_ids, ip, headers):
    trade_ids_maps = []
    for trade_id in trade_ids:
        trade_ids_maps.append({"tradeId": trade_id})
    params = {"positionIds": trade_ids_maps}
    unwind_amount_list = utils.call('trdTradeLCMUnwindAmountGetAll', params, 'trade-service', ip, headers)
    position_unwind_amount_dic = {}
    for unwind_amount in unwind_amount_list:
        position_unwind_amount_dic[unwind_amount['positionId']] = unwind_amount
    return position_unwind_amount_dic


def export_trade(ip, headers):
    # 获取bct所有交易
    bct_trades_dic = get_all_bct_trade_dic(ip, headers)
    position_unwind_amount_dic = get_all_position_unwind_amount_dic(list(bct_trades_dic.keys()), ip, headers)
    csv_data = []
    for trade in bct_trades_dic.values():
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

            unwind_amount_info = position_unwind_amount_dic.get(position_id)
            initial_value = unwind_amount_info['initialValue']
            remain_value = unwind_amount_info['remainValue']

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
                "premium": premium,
                "initial_value": initial_value,
                "remain_value": remain_value
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
               "premium",
               "initial_value",
               "remain_value"]
    # columns = ["book_name(交易簿名称)",
    #            "trade_id（交易id）",
    #            "trader（交易员）",
    #            "trade_status（交易状态LIVE:存续期；CLOSE:结算）",
    #            "trade_date（交易日期）",
    #            "sales_name（销售）",
    #            "trade_confirm_id（交易确认书编号）",
    #            "position_id（多腿编号）",
    #            "lcm_event_type（仓位状态OPEN:开仓；UNWIND_PARTIAL:部分平仓;UNWIND:平仓）",
    #            "product_type（结构类型VANILLA_EUROPEAN:香草欧式;STRADDLE:跨式;SPREAD_EUROPEAN:价差欧式）",
    #            "direction(买卖方向)",
    #            "exercise_type(行权类型EUROPEAN:欧式;AMERICAN:美式)",
    #            "underlyer_instrument_id（标的物）",
    #            "initial_spot（期初价格）",
    #            "strike_type（行权类型CNY:人民币;PERCENT:百分比）",
    #            "strike（行权价）",  ##
    #            "low_strike（低行权价）",  ##
    #            "high_strike（高行权价）",  ##
    #            "specified_price（结算方式CLOSE:收盘价;OPEN:开盘价）",
    #            "settlement_date（结算日）",
    #            "term（期限）",
    #            "annualized（年化:TRUE/非年化:FALSE）",
    #            "days_in_year(年度计息天数)",
    #            "participation_rate(参与率)",  ##
    #            "low_participation_rate（低参与率）",
    #            "high_participation_rate（高参与率）",
    #            "option_type（CALL:看涨/PUT:看跌）",  ##
    #            "notional_amount（名义本金）",
    #            "notional_amount_type（名义本金类型CNY:人民币;PERCENT:百分比）",
    #            "underlyer_multiplier（合约乘数）",
    #            "expiration_date（到期日）",
    #            "effective_date（起始日）",
    #            "premium_type（期权费类型CNY:人民币;PERCENT:百分比）",
    #            "premium（期权费）",
    #            "initial_value（初始名义本金）",
    #            "remain_value（剩余名义本金）"]
    df = pd.DataFrame(columns=columns, data=csv_data)
    targe_path = 'D:/xinhu/data{datetime}.csv'.format(datetime=datetime.datetime.now().strftime(_datetime_fmt))
    df.to_csv(targe_path, encoding='utf-8', index=False)


if __name__ == '__main__':
    headers = utils.login(login_ip, login_body)
    export_trade(login_ip, headers)
