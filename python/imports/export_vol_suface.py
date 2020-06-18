from datetime import datetime

import pandas as pd

import python.imports.utils as utils
from python.imports.init_params import *

_datetime_fmt = '%Y-%m-%d %H%M%S'
labels = {'80% SPOT': 0.8, '90% SPOT': 0.9, '95% SPOT': 0.95, '100% SPOT': 1, '105% SPOT': 1.05, '110% SPOT': 1.1,
          '120% SPOT': 1.2}


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


def mdl_model_data_get(model_type, underlyer, model_name, instance, ip, headers):
    try:
        params = {
            "modelType": model_type,
            "underlyer": underlyer,
            "modelName": model_name,
            "instance": instance
        }
        res = utils.call("mdlModelDataGet", params, "model-service", ip, headers)
        return res
    except Exception as e:
        print("mdl_model_data_get error:", repr(e))
        return None


def export_vol_suface(ip, headers):
    # 获取bct所有标的物
    instruments_dic = get_all_instrument_dic(ip, headers)
    export_csv_data = []
    for instrument_id in list(instruments_dic.keys()):
        vol_data = mdl_model_data_get("vol_surface", instrument_id, "TRADER_VOL", "INTRADAY", ip, headers)
        if vol_data:
            vol_grids = vol_data['modelInfo']['volGrid']
            for vol_grid in vol_grids:
                Tau = vol_grid['tenor']
                vols = vol_grid['vols']
                for vol in vols:
                    MK = labels.get(vol['label'])
                    IV = vol['quote']
                    to_csv_vol = {
                        "Code": instrument_id,
                        "Tau": Tau,
                        "MK": MK,
                        "IV": IV
                    }
                    export_csv_data.append(to_csv_vol)
    df = pd.DataFrame(columns=['Code', 'Tau', 'MK', 'IV'], data=export_csv_data)
    targe_path = export_vol_file+'vol_data{datetime}.csv'.format(datetime=datetime.now().strftime(_datetime_fmt))
    df.to_csv(targe_path, encoding='utf-8', index=False)


if __name__ == '__main__':
    headers = utils.login(bct_login_ip, bct_login_body)
    export_vol_suface(bct_login_ip, headers)
