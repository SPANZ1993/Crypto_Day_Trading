from API_Utils.API_Calls import _rest_api_call




def _get_exchange_info():
    return _rest_api_call('https://api.binance.us/api/v3/exchangeInfo')


def _get_klines(symbol, interval, start_time=None, end_time=None, limit=None):
    symbol_param_str = 'symbol=' + str(symbol)
    interval_param_str = 'interval=' + str(interval)
    if start_time is not None:
        start_time_param_str = 'startTime=' + str(start_time)
    else:
        start_time_param_str = None
    if end_time is not None:
        end_time_param_str = 'endTime=' + str(end_time)
    else:
        end_time_param_str = None
    if limit is not None:
        limit_param_str = 'limit=' + str(limit)
    else:
        limit_param_str = None

    full_param_str = '?' + symbol_param_str + '&' + interval_param_str
    for s in [start_time_param_str, end_time_param_str, limit_param_str]:
        if s is not None:
            full_param_str += '&' + s

    data_dict = _rest_api_call('https://api.binance.us/api/v3/klines' + full_param_str)
    return data_dict

if __name__ == '__main__':
    print(_get_klines(symbol='BTCUSD', interval='8h'))