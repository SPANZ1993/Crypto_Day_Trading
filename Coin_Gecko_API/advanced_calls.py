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

from Coin_Gecko_API.base_api_calls import _get_coins_list, _get_current_market_data
from API_Utils.Response_Exceptions import BaseResponseException, TooManyRequestsException
from utils.times import cur_time, unix_to_datetime


def get_ticker_to_coin_id():
    '''Pull a dictionary that maps tickers to CoinGecko coin ID's'''
    return {c['symbol']: c['id'] for c in _get_coins_list()}

def get_current_market_data(coin_ids=None, precision=18, page_size=250, default_sleep_time=5, debug=False):
    '''Get Current Volume, Market Cap, Price for all Coin Gecko Coins'''
    if coin_ids is None:
        coin_ids = []

    results = []
    page = 1
    sleep_time = default_sleep_time
    cur_results = None
    unhandled_response_exception = False
    while cur_results is None or len(cur_results) != 0:
        try:
            cur_results = _get_current_market_data(page=page,
                                               coin_ids=coin_ids,
                                               page_size=page_size,
                                               precision=precision,
                                               debug=debug
                                               )
            sleep_time = default_sleep_time
        except TooManyRequestsException as e:
            cur_results = None
            print(f"Too Many Requests to CoinGecko... Upping Sleep Time")
            sleep_time = int(sleep_time * 1.5)
        except BaseResponseException as e:
            cur_results = None
            print("Unhandled Response Exception... Breaking ")
            unhandled_response_exception = True
            raise e
        if cur_results is not None:
            for r in cur_results:
                results.append(r)
            print("CUR LENGTH OF RESULTS: ", len(results))
            page += 1

        print(f"SLEEPING FOR {sleep_time} SECONDS")
        time.sleep(sleep_time)
    return results
    
# TODO: Use _get_market_chart_info() to pull historic data


if __name__ == '__main__':
    #ticker_2_coinid = get_ticker_to_coin_id()
    #print(ticker_2_coinid)
    #current_market_data = get_current_market_data()
    #print(current_market_data)
    current_market_data = get_current_market_data(default_sleep_time=6.5)
    print(current_market_data)