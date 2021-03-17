import numpy as np
import pandas as pd
import tsfresh as ts
import pickle as pk
import os
import copy
import time
import itertools
import matplotlib.pyplot as plt


from Database import create_connection, query_entire_table

from Data_Formatting import TSFresh_Data_Extractor
from Data_Formatting.Extract_TSFresh_Data import extract_data_z_norm_data_vs_end_of_window

from Model_Development.Data_Manip import drop_nan_cols

import config as c


def count_nans_per_column(df):
    df = df.replace([np.inf, -np.inf], np.nan)
    nan_count_dict = {"len": len(df)}
    for col in df.columns:
        nan_count_dict[col] = np.sum(np.isnan(df[col].values))
    return nan_count_dict





if __name__ == '__main__':
    # THIS WILL VARY FROM RUN TO RUN... WE ARE TRYING TO FIGURE OUT WHICH TSFRESH FEATURES TO UTILIZE TO PREVENT
    # NAN COLUMNS FROM SHOWING UP IN OUR TSFRESH FEATURES AND NEEDLESSLY BEING SAVED TO THE DB

    # THIS FILE WILL TAKE THE TSFRESH FEATURE DICTIONARIES DETERMINED IN determine_tsfresh_features_to_calculate.py
    # AND ENSURE THAT THEY DO NOT PRODUCE NAN VALUES FOR THE CORRESPONDING CANDLE FEATURE

    db_path = os.path.join(c.database_folder_path, 'BTCUSD_1m.db')
    data_table_name = 'BTCUSD_1m'
    n_samples_per_iteration = 1000
    n_iterations_to_run = 50


    tsfresh_settings_dicts_path = os.path.join('C:\\Users\\riggi\\Data\\Crypto_Day_Trading\\TSFresh_Feature_Calc_Settings')

    if not os.path.exists(tsfresh_settings_dicts_path):
        os.makedirs(tsfresh_settings_dicts_path)

    # DATA EXTRACTOR STUFF
    extract_data_func = extract_data_z_norm_data_vs_end_of_window
    prediction_cols = ['open', 'close', 'high', 'low'] # []
    time_col = 'open_time'
    window_size = 60
    timesteps_ahead = 30



    all_ts_features_dict = ts.feature_extraction.ComprehensiveFCParameters()


    all_features = ['open',
                      'high',
                      'low',
                      'close',
                      'volume',
                      'quote_asset_volume',
                      'number_of_trades',
                      'taker_buy_base_asset_volume',
                      'taker_buy_quote_asset_volume']



    tsfresh_feature_dicts = {}
    for feat in all_features:
        with open(os.path.join(tsfresh_settings_dicts_path, feat + '__tsfresh_settings.pkl'), 'rb') as f:
            cur_feature_dict = pk.load(f)
        tsfresh_feature_dicts[feat] = cur_feature_dict

    for feature in all_features:
        #print("FEATURE: ", feature)
        print(feature + '__tsfresh_feature_dict = {')
        for i, k in enumerate(tsfresh_feature_dicts[feature]):
            if i != len(tsfresh_feature_dicts[feature].keys())-1:
                print("'" + k + "': " + str(tsfresh_feature_dicts[feature][k]) + ',')
            else:
                print("'" + k + "': " + str(tsfresh_feature_dicts[feature][k]))
        print('}')
        print('')
        print('')
        print('')

    # nan_count_dicts = {}
    # for feature in all_features:
    #
    #     cur_extractor = TSFresh_Data_Extractor(
    #         extract_data_func=extract_data_func,
    #         feature_cols=[feature],
    #         prediction_cols=prediction_cols,
    #         time_col='open_time',
    #         window_size=window_size,
    #         timesteps_ahead=timesteps_ahead,
    #         tsfresh_fc_settings=tsfresh_feature_dicts[feature])
    #
    #
    #
    #
    #     with create_connection(db_path) as con:
    #         all_data = query_entire_table(con, data_table_name)
    #
    #
    #     possible_i_ranges = []
    #     start_i = 0
    #     end_i = start_i + n_samples_per_iteration
    #     while end_i <= len(all_data):
    #         end_i = start_i + n_samples_per_iteration
    #         possible_i_ranges.append([start_i, end_i])
    #         start_i = end_i
    #
    #
    #     i_ranges = [possible_i_ranges[i] for i in sorted(np.random.choice(list(range(len(possible_i_ranges))), size=n_iterations_to_run, replace=False))]
    #
    #
    #     feature_df = None
    #     for i_range in i_ranges:
    #         cur_start_i = i_range[0]
    #         cur_end_i = i_range[1]
    #
    #         cur_feature_df = cur_extractor.extract_data(all_data, cur_start_i, cur_end_i)
    #         if feature_df is None:
    #             feature_df = cur_feature_df
    #         else:
    #             feature_df = pd.concat([feature_df, cur_feature_df], ignore_index=True)
    #
    #     cur_nan_count_dict = count_nans_per_column(feature_df)
    #
    #     for _ in range(3):
    #         print('')
    #     print("#############################################################################")
    #     print("----------------- CUR FEATURE IS " + feature + "  len=" + str(cur_nan_count_dict['len'])+ " -----------------")
    #     for k in cur_nan_count_dict:
    #         if k != 'len' and cur_nan_count_dict[k] > 0:
    #             print(k, ": ", cur_nan_count_dict[k])
    #     print("#############################################################################")
    #     time.sleep(30)
    #     for _ in range(3):
    #         print('')
    #
    #
    #     nan_count_dicts[feature] = cur_nan_count_dict
    #
    #
    #
    #
    # with open(os.path.join(tsfresh_settings_dicts_path, 'nan_count_dicts.pkl'), 'wb') as f:
    #     pk.dump(nan_count_dicts, f)
    #
    #     print('')
