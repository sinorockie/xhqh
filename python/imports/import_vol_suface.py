import re
from datetime import datetime

import pandas as pd

import python.imports.utils as utils
from python.imports.init_params import *

_date_fmt = '%Y-%m-%d'

spot = 20
days_in_year = 365
vol_names = ['TRADER_VOL']
tenors = ['1D', '1W', '2W', '3W', '1M', '3M', '6M', '9M', '1Y']
strikes = [spot * p for p in [0.8, 0.9, 0.95, 1, 1.05, 1.1, 1.2]]
labels = ['80% SPOT', '90% SPOT', '95% SPOT', '100% SPOT', '105% SPOT', '110% SPOT', '120% SPOT']


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


def instrument_wind_code1(code,instrument_id_list):
    code.split('.')


    pass

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


def create_vol(host, headers, name, underlyer, instance, spot, tenors, strikes,
               labels, vols, days_in_year, val_date, save=True):
    instruments = [{'tenor': tenors[i],
                    'vols': [{'strike': strikes[j], 'label': labels[j], 'quote': vols[i][j]}
                             for j in range(len(strikes))]}
                   for i in range(len(tenors))]
    params = {
        'underlyer': {
            'instrumentId': underlyer,
            'instance': instance,
            'field': 'close' if instance.lower() == 'close' else 'last',
            'quote': spot
        },
        'modelName': name,
        'instance': instance,
        'valuationDate': val_date,
        'instruments': instruments,
        'daysInYear': days_in_year,
        'save': save
    }
    res = utils.call_request(host, "model-service", "mdlVolSurfaceInterpolatedStrikeCreate", params, headers)
    return res


def import_vol_suface(ip, headers):
    # 获取bct所有标的物
    instruments_dic = get_all_instrument_dic(ip, headers)
    instrument_ids = instruments_dic.keys()
    xinhu_vol_list = pd.read_excel(import_vol_file, sheet_name='Sheet1').to_dict(orient='records')
    vol_date = datetime.now().strftime(_date_fmt)
    for vol_item in xinhu_vol_list:
        instrument_id = vol_item['code']
        underlyer = instrument_wind_code(instrument_id, instrument_ids)
        if instrument_id.find('.') < 0 and underlyer == instrument_id:
            print('BCT不存在合约代码 {underlyer} '.format(underlyer=str(underlyer)))
            continue
        if not instruments_dic.get(underlyer):
            print('BCT不存在合约代码 {underlyer} '.format(underlyer=str(underlyer)))
            continue
        instrument_id = underlyer
        vol_value = vol_item['iv']
        vols = [[vol_value for j in range(len(strikes))] for i in range(len(tenors))]
        for vol_name in vol_names:
            close_res = create_vol(ip, headers, vol_name, instrument_id, 'close', spot, tenors,
                                   strikes, labels, vols, days_in_year, vol_date)
            intraday_res = create_vol(ip, headers, vol_name, instrument_id, 'intraday', spot, tenors,
                                      strikes, labels, vols, days_in_year, vol_date)
            print(str(close_res) + str(intraday_res))


if __name__ == '__main__':
    headers = utils.login(bct_login_ip, bct_login_body)
    import_vol_suface(bct_login_ip, headers)
