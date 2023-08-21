# ADVANCED API CALLS
# THESE USE THE BASE LEVEL API CALLS TO PULL OUT INFO

import os
import sys
import requests
import json
import traceback
import datetime
import numpy as np
import pandas as pd
import time
import matplotlib.pyplot as plt
import sqlite3
from sqlite3 import Error

from Binance_API.base_api_calls import _rest_api_call, _get_klines, _get_exchange_info
from utils.times import cur_time, unix_to_datetime


def ping_binance():
    # Return True If You Get The Expected Response From Ping Else Return False
    data_dict = _rest_api_call('https://api.binance.us/api/v3/ping')
    if data_dict == {}:
        return True
    else:
        return False


def get_all_symbols():
    exchange_info = _get_exchange_info()
    symbol_data_list = exchange_info['symbols']
    symbols = []
    for d in symbol_data_list:
        symbols.append(d['symbol'])
    symbols = sorted(list(set(symbols)))
    return symbols


def get_all_base_assets():
    exchange_info = _get_exchange_info()
    symbol_data_list = exchange_info['symbols']
    base_assets = []
    for d in symbol_data_list:
        base_assets.append(d['baseAsset'])
    base_assets = sorted(list(set(base_assets)))
    return base_assets


def get_rate_limits():
    exchange_info = _get_exchange_info()
    rate_limits = exchange_info['rateLimits']
    return rate_limits


def get_klines(symbol='DOGEUSD', interval='1w', start_time=None, end_time=None, limit=None):
    print("TRYING TO GET KLINES")

    unformatted_data = _get_klines(symbol=symbol, interval=interval, start_time=start_time, end_time=end_time,
                                   limit=limit)
    open_time = []
    open = []
    high = []
    low = []
    close = []
    volume = []
    close_time = []
    quote_asset_volume = []
    number_of_trades = []
    taker_buy_base_asset_volume = []
    taker_buy_quote_asset_volume = []
    for candle in unformatted_data:
        open_time.append(candle[0])
        open.append(candle[1])
        high.append(candle[2])
        low.append(candle[3])
        close.append(candle[4])
        volume.append(candle[5])
        close_time.append(candle[6])
        quote_asset_volume.append(candle[7])
        number_of_trades.append(candle[8])
        taker_buy_base_asset_volume.append(candle[9])
        taker_buy_quote_asset_volume.append(candle[10])
    data = {
        'open_time': open_time,
        'open': open,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
        'close_time': close_time,
        'quote_asset_volume': quote_asset_volume,
        'number_of_trades': number_of_trades,
        'taker_buy_base_asset_volume': taker_buy_base_asset_volume,
        'taker_buy_quote_asset_volume': taker_buy_quote_asset_volume
    }
    assert (len(set([len(data[k]) for k in data.keys()])) == 1)
    print("GOT EM")
    return data


def find_oldest_kline_open_time_available(symbol='BTCUSD', interval='8h'):
    # Find the oldest possible timestamp for Kline data available on Binance for the symbol and interval

    order = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
    seconds_per = list(np.array(
        [60, 180, 300, 900, 1800, 3600, 7200, 14400, 21600, 28800, 43200, 86400, 259200, 604800, 2628000]).astype(np.int64) * 1000)
    usable_order = list(reversed(order[order.index(interval):]))
    usable_seconds_per = list(reversed(seconds_per[order.index(interval):]))
    current_time = cur_time()
    for cur_order, seconds in zip(usable_order, usable_seconds_per):
        current_time_tmp = current_time + 1
        while current_time != current_time_tmp:
            current_time_tmp = current_time
            start_time = max(current_time - (seconds * 900), 0)
            end_time = current_time + (seconds * 100)
            #time.sleep(20)
            klines = get_klines(symbol=symbol, interval=cur_order, start_time=start_time,
                                end_time=end_time, limit=1000)
            current_time = min(klines['open_time'])
            time.sleep(0.5)
    return current_time

if __name__ == '__main__':
    print(get_rate_limits())

    import datetime
    ct = cur_time()
    print(ct)
    print(unix_to_datetime(ct))
    # assert(False)
    #print(ping_binance())
    #print(get_klines('BTCUSD', '1m', start_time=ct-(3600*200), end_time=ct-(3600*1)))
    o = get_klines('BTCUSD', '1m', start_time=None, end_time=None)
    print('.......')
    #print(find_oldest_kline_open_time_available())
