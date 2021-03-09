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

from Data_Acquisition.HistoricalKlineDataLoader import HistoricalKlineDataLoader
import config as c



if __name__ == '__main__':


    symbol = 'BTCUSD'
    interval = '1m'
    db_path = os.path.join(c.database_folder_path, 'BTCUSD_1m.db')
    table_name = 'BTCUSD_1m'


    kline_loader = HistoricalKlineDataLoader(symbol=symbol, interval=interval, db_path=db_path, table_name=table_name)

    kline_loader.run()
