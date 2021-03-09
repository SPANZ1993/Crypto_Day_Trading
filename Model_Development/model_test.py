import numpy as np
import pandas as pd

import os

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import RobustScaler, MinMaxScaler
from sklearn.neural_network import MLPRegressor

import matplotlib.pyplot as plt
import seaborn as sns

import config as c
from utils import times
from Database import create_connection, query_entire_table
from Model_Development.Data_Manip import drop_nan_cols, drop_samples_outside_y_percentile_range, train_test_split_by_start_end, pull_out_nan_y_col_vals, replace_nan_str_with_nan, percent_change_vals_below_above_threshold
from Model_Development.Data_Vis import excalibur

if __name__ == '__main__':
    sns.set_style('darkgrid')

    db_path = os.path.join(c.database_folder_path, 'BTCUSD_1m.db')
    data_table_name = 'BTCUSD_1m'

    # all_features = ['open',
    #                   'high',
    #                   'low',
    #                   'close',
    #                   'volume',
    #                   'quote_asset_volume',
    #                   'number_of_trades',
    #                   'taker_buy_base_asset_volume',
    #                   'taker_buy_quote_asset_volume']

    all_features = ['open', 'high', 'low', 'close']

    tsfresh_data_table_name_base = 'BTCUSD_1m_tsfresh_features_z_normed_endpoint_minmax_scaled_X_60m_window_30m_forward_'

    ts_table_names = {}

    for feature in all_features:
        ts_table_names[feature] = tsfresh_data_table_name_base + feature


    for i, cur_feature in enumerate(all_features):
        with create_connection(db_path) as con:
            cur_df = query_entire_table(con, ts_table_names[cur_feature])

        for col in cur_df.columns:
            print(col)
        print("FEATURE WAS: ", cur_feature)


        # # if i == 0:
        # #     plt.figure()
        # plt.plot(cur_df['_open_true_value'].values)
        # start_time = times.unix_to_datetime(min(cur_df['open_time'].values))
        # end_time = times.unix_to_datetime(max(cur_df['open_time'].values))
        # plt.title(str(cur_feature).capitalize() + ' TSFRESH Feature Database Open')
        # plt.xlabel(str(start_time) + '---' + str(end_time))
        # plt.show()




        y_col = '_' + str(cur_feature) + '_normed_scaled_vs_mean'

        true_y_col = '_' + str(cur_feature) + '_true_value'
        window_mean_col = '_' + str(cur_feature) + '_window_mean'

        cur_df = replace_nan_str_with_nan(cur_df)
        cur_df = pull_out_nan_y_col_vals(cur_df, y_col)

        X_cols = [c for c in cur_df.columns if c[0] != '_' and c != 'open_time']

        X = cur_df[X_cols]
        y = cur_df[y_col].values
        y_percent_change = np.divide(cur_df[true_y_col].values, cur_df[window_mean_col].values)


        tmp_arr, y = drop_samples_outside_y_percentile_range([X, y_percent_change], y)
        X = tmp_arr[0]
        y_percent_change = tmp_arr[1]

        X = drop_nan_cols(X)


        print("LEN X: ", len(X))
        print("LEN y: ", len(y))
        print("LEN y_percent_change: ", len(y_percent_change))


        tr, te = train_test_split_by_start_end([X, y, y_percent_change], train_split = 0.75)
        X_train, y_train, y_percent_change_train = tr[0], tr[1], tr[2]
        X_test, y_test, y_percent_change_test = te[0], te[1], te[2]


        rs = RobustScaler()
        #nn = MLPRegressor([int(X_train.shape[1]/2), int(X_train.shape[1]/3)], activation='identity', early_stopping=True, max_iter=1000000)
        nn = MLPRegressor([int(X_train.shape[1]/2), int(X_train.shape[1]/3)], activation='identity', early_stopping=True, max_iter=10)

        X_train = rs.fit_transform(X_train)
        X_test = rs.transform(X_test)

        print("FITTING")
        nn.fit(X_train, y_train)

        print("PREDICTING")
        preds = nn.predict(X_test)


        excalibur(y_test, preds, title='MAE: ' + str(round(mean_absolute_error(y_test, preds), 4)), suptitle=cur_feature.capitalize() + ' 1/2 Hour Forward')

        for p in range(1,100):
            thresh = np.percentile(preds, p)
            cur_below_changes, cur_above_changes = percent_change_vals_below_above_threshold(preds, y_percent_change_test, thresh)
            print('#####################################')
            print("PERCENTILE: ", p)
            print('THRESHOLD: ', thresh)
            print('BELOW MEAN: ', np.mean(cur_below_changes))
            print('BELOW STD: ', np.std(cur_below_changes))
            print('ABOVE MEAN: ', np.mean(cur_above_changes))
            print('ABOVE STD: ', np.std(cur_above_changes))
            print('#####################################')

