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


def generate_tsfresh_settings_dict_for_non_nan_cols(df, col_prefix, ignore_cols=['open_time'], orig_tsfresh_settings=ts.feature_extraction.ComprehensiveFCParameters()):
    # df: The df of TSFresh Features that we extracted using orig_tsfresh_settings
    # col_prefix: The name of the timeseries data that we are extracting features from (TSFresh feature columns will,
    # begin with <col_prefix>__
    # ignore_cols: Columns that will be ignored when filtering tsfresh_settings dict (Non-Feature columns)
    # orig_tsfresh_settings: The original TSFresh settings dict that we used to calculate the feature DF. This dict
    # will be filtered down to produce a new dict which will only produce columns that do not contain nans when
    # calculated agains the feature denoted by col_prefix


    non_nan_df = drop_nan_cols(copy.deepcopy(df), return_df=True)

    all_cols = [c for c in df.columns]
    non_nan_cols = [c for c in non_nan_df.columns if c not in ignore_cols and c.startswith(col_prefix+'__')]


    assert(all([c.startswith(col_prefix+'__') for c in non_nan_cols]))

    new_tsfresh_settings = {}
    for k in orig_tsfresh_settings:
        # If this feature setting doesn't have any param dicts
        if orig_tsfresh_settings[k] is None:
            cur_feature_column_name = col_prefix + '__' + str(k)
            assert(cur_feature_column_name in all_cols)

            # If this column is non-nan, then add it to the new_tsfresh settings dict
            if cur_feature_column_name in non_nan_cols:
                new_tsfresh_settings[k] = None


        # If this feature does have a list of param dicts
        elif isinstance(orig_tsfresh_settings[k], list):
            # Process each param dict in order
            for param_dict in orig_tsfresh_settings[k]:
                # We don't know what order the params will appear in the column name
                # So just try them all until you get one
                found_correct_column_name = False
                all_param_orders = itertools.permutations([p for p in param_dict.keys()])
                # Format the parameter dict so that the keys are strings in the same form as they
                # show up in the column names
                formatted_param_dict = {}
                for pk in param_dict.keys():
                    assert(isinstance(pk, str))
                    if isinstance(param_dict[pk], str):
                        formatted_param_dict[pk] = '__' + pk + '_' + '"'+param_dict[pk]+'"'
                    else:
                        formatted_param_dict[pk] = '__' + pk + '_' + str(param_dict[pk])
                cur_potential_column_names = []
                for param_order in all_param_orders:
                    cur_potential_column_name = col_prefix + '__' + str(k)
                    for param in param_order:
                        cur_potential_column_name += formatted_param_dict[param]
                    cur_potential_column_names.append(cur_potential_column_name)
                # Cur potential column names is now all of the potential column names we could have
                # for the current feature and parameter dict (Trying all orders of the parameters in the col name)
                # One and only one of these should be in the columns of the original tsfresh features dataframe
                # Let's make sure our above assumptions are true and pull out that column name
                true_potential_column_name_mask = [cpcn in all_cols for cpcn in cur_potential_column_names]
                # Make sure we got either one hit or none on the column names we had
                assert(sum(true_potential_column_name_mask) <= 1)
                # If we have a column name in the features dataframe
                # Sometimes we don't... I think TSFresh just skips even trying some if the signal isn't long enough
                # This should be fine though.
                if sum(true_potential_column_name_mask) == 1:
                    true_potential_column_name_index = true_potential_column_name_mask.index(True)

                    # Finally after all that we've got the correct column name hopefully
                    cur_feature_column_name = cur_potential_column_names[true_potential_column_name_index]
                    assert(cur_feature_column_name in all_cols)

                    # If this column is non-nan, then add it to the new_tsfresh settings dict with the param dict
                    if cur_feature_column_name in non_nan_cols:
                        if k in new_tsfresh_settings.keys():
                            new_tsfresh_settings[k].append(param_dict)
                        else:
                            new_tsfresh_settings[k] = [param_dict]

        # Shouldn't get here....
        else:
            print("UNHANDLED FEATURE CONFIGURATION")
            assert(False)


    return new_tsfresh_settings




if __name__ == '__main__':
    # THIS WILL VARY FROM RUN TO RUN... WE ARE TRYING TO FIGURE OUT WHICH TSFRESH FEATURES TO UTILIZE TO PREVENT
    # NAN COLUMNS FROM SHOWING UP IN OUR TSFRESH FEATURES AND NEEDLESSLY BEING SAVED TO THE DB

    # THIS PRETTY MUCH SIMULATES WHAT HISTORICAL_TSFRESH_EXTRACTION_MANAGER DOES ON A SUBSET OF THE DATA AND LOOKS FOR
    # NAN COLS... THIS ALLOWS US TO CONSTRUCT A TSFRESH FEATURE CALCULATION DICT PER CANDLE FEATURE

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

    for feature in all_features:

        cur_extractor = TSFresh_Data_Extractor(
            extract_data_func=extract_data_func,
            feature_cols=[feature],
            prediction_cols=prediction_cols,
            time_col='open_time',
            window_size=window_size,
            timesteps_ahead=timesteps_ahead,
            tsfresh_fc_settings=ts.feature_extraction.ComprehensiveFCParameters())




        with create_connection(db_path) as con:
            all_data = query_entire_table(con, data_table_name)


        possible_i_ranges = []
        start_i = 0
        end_i = start_i + n_samples_per_iteration
        while end_i <= len(all_data):
            end_i = start_i + n_samples_per_iteration
            possible_i_ranges.append([start_i, end_i])
            start_i = end_i


        i_ranges = [possible_i_ranges[i] for i in sorted(np.random.choice(list(range(len(possible_i_ranges))), size=n_iterations_to_run, replace=False))]


        feature_df = None
        for i_range in i_ranges:
            cur_start_i = i_range[0]
            cur_end_i = i_range[1]

            cur_feature_df = cur_extractor.extract_data(all_data, cur_start_i, cur_end_i)
            if feature_df is None:
                feature_df = cur_feature_df
            else:
                feature_df = pd.concat([feature_df, cur_feature_df], ignore_index=True)


        new_tsfresh_settings = generate_tsfresh_settings_dict_for_non_nan_cols(feature_df, feature, orig_tsfresh_settings=all_ts_features_dict)

        for f in new_tsfresh_settings:
            print(f, ": ", new_tsfresh_settings[f])

        with open(os.path.join(tsfresh_settings_dicts_path, feature + '__tsfresh_settings.pkl'), 'wb') as f:
            pk.dump(new_tsfresh_settings, f)