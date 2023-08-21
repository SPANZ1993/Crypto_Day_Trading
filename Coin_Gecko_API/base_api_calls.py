# BASE LEVEL API CALLS
# THESE JUST CALL THE API

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

from API_Utils.API_Calls import _rest_api_call
from utils import times as T




def _get_coins_list():
    return _rest_api_call("https://api.coingecko.com/api/v3/coins/list")

def _get_current_market_data(page, coin_ids=[], page_size=250, precision=18, debug=False):
    if precision < 0 or precision > 18:
        raise Exception('Precision Must Be Between 1 and 18... Got {}'.format(precision))

    coins_part = "&ids=" + "%2C%20".join(coin_ids) if len(coin_ids) != 0 else ""
    api_str = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd{coins_part}&order=market_cap_desc&per_page={page_size}&page={page}&sparkline=false&locale=en&precision={precision}"

    if debug:
        print(api_str)

    return _rest_api_call(api_str)

def _get_market_chart_info(coin_id, start_time, end_time, precision=18, debug=False):
    if precision < 0 or precision > 18:
        raise Exception('Precision Must Be Between 1 and 18... Got {}'.format(precision))

    precision = str(int(precision))


    if isinstance(start_time, datetime.datetime):
        start_time = T.datetime_to_unix(start_time, timescale=T.TimeScale.SECONDS)
    if isinstance(end_time, datetime.datetime):
        end_time = T.datetime_to_unix(end_time, timescale=T.TimeScale.SECONDS)

    start_time = str(start_time)
    end_time = str(end_time)
    print(start_time)
    print(end_time)

    api_str = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range?vs_currency=usd&from={start_time}&to={end_time}&precision={precision}"

    if debug:
        print(api_str)

    return _rest_api_call(api_str)

if __name__ == '__main__':
    #coins = _get_coins_list()
    #print(coins)
    # chart_info = _get_market_chart_info('dogecoin',
    #                                     datetime.datetime(2022, 1, 1),
    #                                     datetime.datetime(2022, 3, 1),
    #                                     debug=True
    #                                     )
    # print(chart_info)
    #print(_api_call('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=400&sparkline=false&locale=en'))

    current_market_data = _get_current_market_data(1, debug=True)
    empty_market_data = _get_current_market_data(20000, debug=True)
    print(current_market_data)