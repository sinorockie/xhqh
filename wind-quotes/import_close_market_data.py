import logging
import time   as time_time
import math
from WindPy import *

import utils

ip = ''
login_body = {
    'userName': '',
    'password': ''
}
_date_fmt = '%Y-%m-%d'
_now_date=datetime.today()




def get(ticker, start, end):
    ts = time_time.time()
    data = w.wsd(ticker, reqs, start.isoformat(), end.isoformat())
    te = time_time.time()
    t = str(te - ts)
    logging.info("retrieving instrument " + ticker + " from Wind in " + t)
    # logging.info data
    ret = []
    for (t, v) in zip(data.Times, map(list, zip(*data.Data))):
        ret.append([ticker, t] + v)

    return convert(ret)


def convert(data):
    ret = []
    for row in data:
        quote = {}
        logging.info(row)
        if row[2] == None:
            continue
        if not math.isnan(row[2]):
            quote['open'] = row[2]
        if not math.isnan(row[3]):
            quote['high'] = row[3]
        if not math.isnan(row[4]):
            quote['low'] = row[4]
        if not math.isnan(row[5]):
            quote['close'] = row[5]
        if not math.isnan(row[6]):
            quote['settle'] = row[6]

        body = {
            'instrumentId': row[0],
            'instance': 'close',
            'valuationDate': row[1].isoformat() + 'T15:00:00',
            'quoteTimezone': 'Asia/Shanghai',
            'quote': quote
        }
        if len(quote) > 0:
            ret.append(body)
    return ret


def save(quote):
    params = quote
    try:
        ts = time_time.time()
        json = utils.call_request(ip, "market-data-service", "mktQuoteSave", params, headers)
        te = time_time.time()
        t = str(te - ts)
        logging.info("saving instrument " + quote['instrumentId'] + " into BCT in " + t)
        if 'error' in json:
            logging.info("failed to save quote "
                         + quote['instrumentId'] + " "
                         + quote['valuationDate']
                         + json['error']['message'])
        else:
            logging.info("success: saved quote "
                         + quote['instrumentId']
                         + " " + quote['valuationDate'])
    except Exception as e:
        logging.info("failed to save quote " + quote['instrumentId'] + ". network error?")






def import_wind_close_market_data(ip, headers, date_str):
    print('close_thread')
    w.start()
    start = datetime.strptime(date_str, '%Y-%m-%d').date()
    end = start
    instrument_list_response = utils.call_request(ip, "market-data-service", "mktInstrumentsListPaged", {}, headers)
    tickers = []
    if "error" not in instrument_list_response:
        for i in instrument_list_response['result']['page']:
            tickers.append(i['instrumentId'])
    for ticker in tickers:
        # ticker="abcdefg"
        data = get(ticker, start, end)
        check_and_save_market_data(data,ticker)
        for q in data:
            save(q)
    w.close()


def build_params(close_price,instrument_id):
    if not close_price:
        close_price=1;
    params = {
        "instance": "close",
        "instrumentId": instrument_id.upper(),
        "valuationDate": _now_date.strftime(_date_fmt),
        "quote": {
            "settle": close_price,
            "close": close_price
        }
    }
    return params


##wind没有取到数据默认 填入bct系统已存在的最新行情作为当日收盘行情
def check_and_save_market_data(data,instrument_id):
    if not data:
        quote = market_data_dict.get(instrument_id)
        if quote:
            utils.call_request(ip, "market-data-service", "mktQuoteSave", build_params(quote['close'],instrument_id), headers)
    else:
        result=data[0]
        quote=result['quote']
        if not quote.get('close'):
            quote = market_data_dict.get(instrument_id)
            if quote:
                utils.call_request(ip, "market-data-service", "mktQuoteSave", build_params(quote['close'],instrument_id), headers)


def get_last_market_data(ip, headers):
    params = {
        'instrumentIds': [],
        'valuationDate': None,
        'timezone': 'Asia/Shanghai',
        'page': None,
        'pageSize': None
    }
    quote_page_list = utils.call_request(ip, 'market-data-service', 'mktQuotesListPaged', params, headers)['result'][
        'page']
    market_dict = {}
    if not quote_page_list:
        return None
    else:
        for quote in quote_page_list:
            market_dict[quote['instrumentId']] = quote
        return market_dict


if __name__ == '__main__':

    # now = datetime.now()
    # strdate = now.strftime("%Y-%m-%d")
    # logging.basicConfig(level=logging.INFO,
    #                     format='%(asctime)s [line:%(lineno)d] %(message)s',
    #                     datefmt='%Y-%m-%d %H:%M:%S',
    #                     filename=('import_close_log\\' + strdate + '.log'),
    #                     filemode='a')
    # console = logging.StreamHandler()
    # logging.getLogger('').addHandler(console)
    #
    # fields = ['open', 'high', 'low', 'close', 'settle']
    # reqs = ','.join(fields)
    # headers = utils.login(ip, login_body)
    #
    # market_data_dict = get_last_market_data(ip, headers);
    # import_wind_close_market_data(ip, headers, _now_date.strftime(_date_fmt))
    trade_days = w.tdays("2017-01-01", "2019-12-31", "")
    print(trade_days)