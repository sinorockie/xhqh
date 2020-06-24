# -*- encoding: utf-8 -*-

import datetime
import re

import pandas as pd

import python.imports.utils as utils
from python.imports.init_params import *

_datetime_fmt = '%Y-%m-%d-%H-%M-%S'


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


def get_all_lcm_events_dic(ip, headers):
    events = utils.call('trdTradeCashFlowListAll', {}, 'trade-service', ip, headers)
    lcm_event_dic = {}
    for event in events:
        position_events = lcm_event_dic.get(event['positionId'])
        if position_events:
            position_events.append(event)
            position_events.sort(key=lambda x: x['createdAt'], reverse=True)
        else:
            lcm_event_dic[event['positionId']] = [event]
    return lcm_event_dic


def get_all_position_enrichment_dic(position_ids, ip, headers):
    position_enrichments = utils.call('trdGetPositionEnrichment', {'positionIds': position_ids}, 'trade-service', ip,
                                      headers)
    position_enrichment_dic = {}
    for position_enrichment in position_enrichments:
        position_enrichment_dic[position_enrichment['positionId']] = position_enrichment
    return position_enrichment_dic


def export_trade(ip, headers):
    # 获取bct所有交易
    bct_trades_dic = get_all_bct_trade_dic(ip, headers)
    position_unwind_amount_dic = get_all_position_unwind_amount_dic(list(bct_trades_dic.keys()), ip, headers)
    lcm_event_dic = get_all_lcm_events_dic(ip, headers)

    position_ids = []
    for trade in bct_trades_dic.values():
        for position in trade['positions']:
            position_ids.append(position['positionId'])
    position_enrichment_dic = get_all_position_enrichment_dic(position_ids, ip, headers)
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
        index = 1
        for position in positions:
            counter_party_name = position['counterPartyName']
            if len(positions) > 1:
                xinhu_position_id = trade_id + '-' + str(index)
                index += 1
            else:
                xinhu_position_id = trade_id
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

            unwind_amount_info = position_unwind_amount_dic.get(position['positionId'])
            initial_value = unwind_amount_info['initialValue']
            remain_value = unwind_amount_info['remainValue']
            lcm_events = lcm_event_dic.get(position['positionId'])
            payment_date = None
            cash = None
            exercise_spot = None
            if lcm_events and lcm_events[0]['lcmEventType'] == 'UNWIND':
                payment_date = lcm_events[0]['paymentDate']
                cash = lcm_events[0]['cashFlow']
            if lcm_events and lcm_events[0]['lcmEventType'] == 'EXERCISE':
                payment_date = lcm_events[0]['paymentDate']
                cash = lcm_events[0]['cashFlow']
                if lcm_events[0]['eventDetail'] and lcm_events[0]['eventDetail']['underlyerPrice']:
                    exercise_spot = lcm_events[0]['eventDetail']['underlyerPrice']
            # 开仓波动率
            initial_vol = None
            position_enrichment = position_enrichment_dic.get(position['positionId'])
            if position_enrichment:
                initial_vol = position_enrichment['initialVol']
            xinhu_trade = {
                "counter_party_name": counter_party_name,
                "book_name": book_name,
                "trade_id": trade_id,
                "trader": trader,
                "trade_status": trade_status,
                "trade_date": trade_date,
                "sales_name": sales_name,
                "trade_confirm_id": trade_confirm_id,
                "position_id": xinhu_position_id,
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
                "remain_value": remain_value,
                "payment_date": payment_date,
                'cash': cash,
                'initial_vol': initial_vol,
                'exercise_spot': exercise_spot
            }
            csv_data.append(xinhu_trade)
    print(csv_data)
    columns = ["counter_party_name",
               "book_name",
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
               "remain_value",
               "payment_date",
               'cash',
               'initial_vol',
               'exercise_spot'
               ]
    df = pd.DataFrame(columns=columns, data=csv_data)
    targe_path = export_trade_file + 'bct_trades{datetime}.csv'.format(
        datetime=datetime.datetime.now().strftime(_datetime_fmt))
    df.to_csv(targe_path, encoding='utf-8', index=False)


if __name__ == '__main__':
    headers = utils.login(bct_login_ip, bct_login_body)
    export_trade(bct_login_ip, headers)
